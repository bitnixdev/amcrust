//! Client for a single Amcrest camera: digest-auth HTTP API, snapshots and
//! the `eventManager.cgi` AI event stream.

use chrono::{DateTime, Utc};
use digest_auth::AuthContext;
use log::{error, info, warn};
use reqwest::Client;
use tokio::sync::broadcast;
use tokio::time::{Duration, sleep};

/// AI detection event codes to subscribe to.
const EVENT_CODES: &str =
    "SmartMotionHuman,SmartMotionVehicle,CrossLineDetection,CrossRegionDetection";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct AmcrestClient {
    pub host: String,
    pub username: String,
    pub password: String,
    client: Client,
}

#[derive(Clone, Debug)]
pub struct CameraEvent {
    pub code: String,
    pub action: String,
    pub index: u32,
    pub data: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

impl AmcrestClient {
    pub fn new(host: String, username: String, password: String) -> Self {
        Self {
            host,
            username,
            password,
            client: Client::new(),
        }
    }

    /// RTSP URL for the given stream subtype (0 = main, 1/2 = sub streams).
    pub fn rtsp_url(&self, subtype: u8) -> String {
        format!(
            "rtsp://{}:{}@{}:554/cam/realmonitor?channel=1&subtype={}",
            self.username, self.password, self.host, subtype
        )
    }

    /// Performs a digest-authenticated GET for the given path + query.
    async fn get(
        &self,
        path_and_query: &str,
    ) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("http://{}{}", self.host, path_and_query);

        let resp = self.client.get(&url).send().await?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        if resp.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Err(
                format!("unexpected status {} for {}", resp.status(), path_and_query).into(),
            );
        }

        let www_authenticate = resp
            .headers()
            .get("www-authenticate")
            .ok_or("no WWW-Authenticate header")?
            .to_str()?
            .to_string();

        let context = AuthContext::new(&self.username, &self.password, path_and_query);
        let mut prompt = digest_auth::parse(&www_authenticate)?;
        let auth_header = prompt.respond(&context)?.to_header_string();

        let resp = self
            .client
            .get(&url)
            .header("Authorization", auth_header)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(format!(
                "auth failed with status {} for {}",
                resp.status(),
                path_and_query
            )
            .into());
        }
        Ok(resp)
    }

    /// Returns the camera's model designation, e.g. "IP8M-2796E-AI".
    pub async fn get_device_type(
        &self,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .get("/cgi-bin/magicBox.cgi?action=getDeviceType")
            .await?;
        let body = resp.text().await?;
        // Response format: "type=IP8M-2796E-AI"
        Ok(body
            .trim()
            .strip_prefix("type=")
            .unwrap_or(body.trim())
            .to_string())
    }

    /// Ensures the camera's AI detection (SmartMotionDetect) is enabled — it's
    /// the source of the person/vehicle events HomeKit motion sensors and
    /// recording triggers depend on.
    pub async fn ensure_smart_motion(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=SmartMotionDetect")
            .await?;
        let body = resp.text().await?;
        if body.contains("table.SmartMotionDetect[0].Enable=true") {
            return Ok(());
        }
        info!(
            "[{}] enabling SmartMotionDetect (AI person/vehicle events)",
            self.host
        );
        self.set_config("SmartMotionDetect%5B0%5D.Enable=true")
            .await
    }

    /// Applies encoder/config settings via configManager setConfig. `params`
    /// is the raw `Key=Value&Key=Value` query fragment.
    pub async fn set_config(
        &self,
        params: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = format!("/cgi-bin/configManager.cgi?action=setConfig&{params}");
        let resp = self.get(&path).await?;
        let body = resp.text().await?;
        if body.trim() != "OK" {
            return Err(format!("setConfig returned: {}", body.trim()).into());
        }
        Ok(())
    }

    /// Ensures the live-view substream is enabled and HomeKit-friendly:
    /// H.264, 1280x720, 15 fps with 1 s keyframes. Cameras ship with sub
    /// stream 2 disabled or misconfigured, which breaks live view.
    pub async fn ensure_live_substream(
        &self,
        subtype: u8,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if subtype == 0 {
            return Ok(()); // main stream is managed by the recording config
        }
        let idx = subtype - 1;
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=Encode")
            .await?;
        let body = resp.text().await?;

        let field = |name: &str| -> Option<String> {
            let prefix = format!("table.Encode[0].ExtraFormat[{idx}].{name}=");
            body.lines()
                .find(|l| l.starts_with(&prefix))
                .map(|l| l[prefix.len()..].trim().to_string())
        };

        let enabled = field("VideoEnable").as_deref() == Some("true");
        let h264 = field("Video.Compression").as_deref() == Some("H.264");
        let fps = field("Video.FPS")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let gop = field("Video.GOP")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let width = field("Video.Width")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let height = field("Video.Height")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let bitrate = field("Video.BitRate")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let bitrate_control = field("Video.BitRateControl").unwrap_or_default();
        let profile = field("Video.Profile").unwrap_or_default();
        let audio_enabled = field("AudioEnable").as_deref() == Some("true");
        let audio_codec = field("Audio.Compression").unwrap_or_default();
        let audio_frequency = field("Audio.Frequency")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let audio_bitrate = field("Audio.Bitrate")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);

        if enabled
            && h264
            && width == 1280
            && height == 720
            && fps == 15
            && gop == 15
            && bitrate == 1024
            && bitrate_control == "VBR"
            && profile == "Main"
            && audio_enabled
            && audio_codec == "AAC"
            && audio_frequency == 16000
            && audio_bitrate == 64
        {
            return Ok(());
        }

        info!(
            "[{}] configuring live substream {subtype} for HomeKit (was: enabled={enabled} h264={h264} {width}w {fps}fps gop {gop})",
            self.host
        );
        let params = format!(
            "Encode%5B0%5D.ExtraFormat%5B{idx}%5D.VideoEnable=true\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Compression=H.264\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.resolution=1280x720\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Width=1280\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Height=720\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.FPS=15\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.GOP=15\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.BitRate=1024\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.BitRateControl=VBR\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Profile=Main\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Quality=4\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.AudioEnable=true\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Compression=AAC\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Bitrate=64\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Depth=16\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Channels%5B0%5D=0\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Frequency=16000"
        );
        self.set_config(&params).await
    }

    /// Normalizes the camera microphone and the high-quality audio track used
    /// as the live HomeKit audio source.
    pub async fn ensure_audio_profile(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=All")
            .await?;
        let body = resp.text().await?;
        let desired = [
            "table.All.AudioInput[0].AudioSource=Mic",
            "table.All.AudioInputVolume[0]=100",
            "table.All.AudioInDenoise[0].enable=true",
            "table.All.AudioInDenoise[0].level=50",
            "table.All.Encode[0].MainFormat[0].AudioEnable=true",
            "table.All.Encode[0].MainFormat[0].Audio.Compression=AAC",
            "table.All.Encode[0].MainFormat[0].Audio.Frequency=48000",
            "table.All.Encode[0].MainFormat[0].Audio.Bitrate=64",
            "table.All.Encode[0].MainFormat[0].Audio.Depth=16",
        ];
        if desired.iter().all(|setting| body.contains(setting)) {
            return Ok(());
        }

        info!(
            "[{}] normalizing microphone and main audio track",
            self.host
        );
        self.set_config(
            "AudioInput%5B0%5D.AudioSource=Mic\
             &AudioInputVolume%5B0%5D=100\
             &AudioInDenoise%5B0%5D.enable=true\
             &AudioInDenoise%5B0%5D.level=50\
             &Encode%5B0%5D.MainFormat%5B0%5D.AudioEnable=true\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Compression=AAC\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Frequency=48000\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Bitrate=64\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Depth=16\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Channels%5B0%5D=0",
        )
        .await
    }

    /// Applies a consistent, minimal burned-in overlay to every camera: a
    /// white, bordered timestamp at the upper right with identical automatic
    /// font sizing on the main stream, substreams, and snapshots.
    pub async fn ensure_overlay_profile(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=VideoWidget")
            .await?;
        let body = resp.text().await?;
        let desired = [
            "table.VideoWidget[0].FontSize=0",
            "table.VideoWidget[0].FontSizeExtra1=0",
            "table.VideoWidget[0].FontSizeExtra2=0",
            "table.VideoWidget[0].FontSizeExtra3=0",
            "table.VideoWidget[0].FontSizeSnapshot=0",
            "table.VideoWidget[0].TimeTitle.EncodeBlend=true",
            "table.VideoWidget[0].TimeTitle.PreviewBlend=true",
            "table.VideoWidget[0].TimeTitle.ShowWeek=false",
            "table.VideoWidget[0].TimeTitle.FrontColor[0]=255",
            "table.VideoWidget[0].TimeTitle.FrontColor[1]=255",
            "table.VideoWidget[0].TimeTitle.FrontColor[2]=255",
            "table.VideoWidget[0].TimeTitle.FrontColor[3]=0",
            "table.VideoWidget[0].TimeTitle.BackColor[0]=0",
            "table.VideoWidget[0].TimeTitle.BackColor[1]=0",
            "table.VideoWidget[0].TimeTitle.BackColor[2]=0",
            "table.VideoWidget[0].TimeTitle.BackColor[3]=128",
            "table.VideoWidget[0].TimeTitle.Rect[0]=5319",
            "table.VideoWidget[0].TimeTitle.Rect[1]=352",
            "table.VideoWidget[0].TimeTitle.Rect[2]=7929",
            "table.VideoWidget[0].TimeTitle.Rect[3]=769",
            "table.VideoWidget[0].ChannelTitle.EncodeBlend=false",
            "table.VideoWidget[0].ChannelTitle.PreviewBlend=false",
            "table.VideoWidget[0].OSDMobileState.EncodeBlend=false",
            "table.VideoWidget[0].OSDMobileState.PreviewBlend=false",
        ];
        let scale_is_one = body.contains("table.VideoWidget[0].FontSizeScale=1\n")
            || body.contains("table.VideoWidget[0].FontSizeScale=1\r\n")
            || body.contains("table.VideoWidget[0].FontSizeScale=1.000000");
        if scale_is_one && desired.iter().all(|setting| body.contains(setting)) {
            return Ok(());
        }

        info!(
            "[{}] normalizing timestamp and overlay appearance",
            self.host
        );
        self.set_config(
            "VideoWidget%5B0%5D.FontSize=0\
             &VideoWidget%5B0%5D.FontSizeExtra1=0\
             &VideoWidget%5B0%5D.FontSizeExtra2=0\
             &VideoWidget%5B0%5D.FontSizeExtra3=0\
             &VideoWidget%5B0%5D.FontSizeSnapshot=0\
             &VideoWidget%5B0%5D.FontSizeScale=1\
             &VideoWidget%5B0%5D.TimeTitle.EncodeBlend=true\
             &VideoWidget%5B0%5D.TimeTitle.PreviewBlend=true\
             &VideoWidget%5B0%5D.TimeTitle.ShowWeek=false\
             &VideoWidget%5B0%5D.TimeTitle.FrontColor%5B0%5D=255\
             &VideoWidget%5B0%5D.TimeTitle.FrontColor%5B1%5D=255\
             &VideoWidget%5B0%5D.TimeTitle.FrontColor%5B2%5D=255\
             &VideoWidget%5B0%5D.TimeTitle.FrontColor%5B3%5D=0\
             &VideoWidget%5B0%5D.TimeTitle.BackColor%5B0%5D=0\
             &VideoWidget%5B0%5D.TimeTitle.BackColor%5B1%5D=0\
             &VideoWidget%5B0%5D.TimeTitle.BackColor%5B2%5D=0\
             &VideoWidget%5B0%5D.TimeTitle.BackColor%5B3%5D=128\
             &VideoWidget%5B0%5D.TimeTitle.Rect%5B0%5D=5319\
             &VideoWidget%5B0%5D.TimeTitle.Rect%5B1%5D=352\
             &VideoWidget%5B0%5D.TimeTitle.Rect%5B2%5D=7929\
             &VideoWidget%5B0%5D.TimeTitle.Rect%5B3%5D=769\
             &VideoWidget%5B0%5D.ChannelTitle.EncodeBlend=false\
             &VideoWidget%5B0%5D.ChannelTitle.PreviewBlend=false\
             &VideoWidget%5B0%5D.OSDMobileState.EncodeBlend=false\
             &VideoWidget%5B0%5D.OSDMobileState.PreviewBlend=false",
        )
        .await
    }

    /// Fetches a JPEG snapshot from the camera.
    pub async fn snapshot(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self.get("/cgi-bin/snapshot.cgi").await?;
        Ok(resp.bytes().await?.to_vec())
    }

    /// Runs the AI event stream forever, reconnecting on errors, publishing
    /// events to `tx`.
    pub async fn run_event_stream(&self, tx: broadcast::Sender<CameraEvent>) {
        loop {
            info!("[{}] connecting to event stream...", self.host);
            match self.stream_events(&tx).await {
                Ok(()) => warn!("[{}] event stream ended, reconnecting...", self.host),
                Err(e) => error!("[{}] event stream error: {e}, reconnecting...", self.host),
            }
            sleep(RECONNECT_DELAY).await;
        }
    }

    async fn stream_events(
        &self,
        tx: &broadcast::Sender<CameraEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = format!("/cgi-bin/eventManager.cgi?action=attach&codes=[{EVENT_CODES}]");
        let resp = self.get(&path).await?;

        info!("[{}] connected, streaming events...", self.host);

        let mut buffer = String::new();
        let mut stream = resp.bytes_stream();

        use futures::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(newline_pos) = buffer.find('\n') {
                let line = buffer[..newline_pos].trim().to_string();
                buffer = buffer[newline_pos + 1..].to_string();

                if line.starts_with("Code=") {
                    if let Some(event) = parse_event_line(&line) {
                        let _ = tx.send(event);
                    }
                }
            }
        }

        Ok(())
    }
}

/// Serves snapshots from memory, refreshed in the background, so HAP
/// `/resource` requests answer instantly instead of waiting on the camera.
#[derive(Clone)]
pub struct SnapshotCache {
    client: AmcrestClient,
    latest: std::sync::Arc<tokio::sync::RwLock<Option<(tokio::time::Instant, Vec<u8>)>>>,
    scaled: std::sync::Arc<
        tokio::sync::Mutex<std::collections::HashMap<(u32, u32), (tokio::time::Instant, Vec<u8>)>>,
    >,
}

const SNAPSHOT_REFRESH: Duration = Duration::from_secs(10);
/// Serve a stale snapshot for up to this long if the camera stops responding.
const SNAPSHOT_MAX_AGE: Duration = Duration::from_secs(120);

impl SnapshotCache {
    pub fn new(client: AmcrestClient) -> Self {
        Self {
            client,
            latest: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
            scaled: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Spawns the background refresh loop.
    pub fn spawn_refresher(&self) {
        let cache = self.clone();
        tokio::spawn(async move {
            loop {
                match cache.client.snapshot().await {
                    Ok(bytes) => {
                        *cache.latest.write().await = Some((tokio::time::Instant::now(), bytes));
                    }
                    Err(e) => warn!("[{}] snapshot refresh failed: {e}", cache.client.host),
                }
                sleep(SNAPSHOT_REFRESH).await;
            }
        });
    }

    /// Returns the most recent snapshot, falling back to a direct fetch if the
    /// cache is empty or too stale.
    pub async fn get(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some((at, bytes)) = self.latest.read().await.as_ref() {
            if at.elapsed() < SNAPSHOT_MAX_AGE {
                return Ok(bytes.clone());
            }
        }
        let bytes = self.client.snapshot().await?;
        *self.latest.write().await = Some((tokio::time::Instant::now(), bytes.clone()));
        Ok(bytes)
    }

    /// Returns the most recent snapshot scaled to the requested dimensions, as
    /// HAP controllers expect. Scaled variants are cached per size.
    pub async fn get_scaled(
        &self,
        width: u32,
        height: u32,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        {
            let scaled = self.scaled.lock().await;
            if let Some((at, bytes)) = scaled.get(&(width, height)) {
                if at.elapsed() < SNAPSHOT_REFRESH {
                    return Ok(bytes.clone());
                }
            }
        }

        let raw = self.get().await?;
        let bytes = scale_jpeg(raw, width, height).await?;
        self.scaled.lock().await.insert(
            (width, height),
            (tokio::time::Instant::now(), bytes.clone()),
        );
        Ok(bytes)
    }
}

/// Scales a JPEG to the given size with ffmpeg, preserving aspect ratio.
async fn scale_jpeg(
    input: Vec<u8>,
    width: u32,
    height: u32,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    use std::process::Stdio;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Fit within the requested box without distorting the aspect ratio.
    let filter =
        format!("scale='min({width},iw)':'min({height},ih)':force_original_aspect_ratio=decrease");

    let mut child = tokio::process::Command::new("ffmpeg")
        .args(["-hide_banner", "-loglevel", "error"])
        .args(["-f", "image2pipe", "-i", "pipe:0"])
        .args(["-frames:v", "1", "-vf", &filter])
        .args(["-f", "image2", "-c:v", "mjpeg", "-q:v", "4", "pipe:1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;

    let mut stdin = child.stdin.take().ok_or("no stdin")?;
    let mut stdout = child.stdout.take().ok_or("no stdout")?;

    let writer = async move {
        let _ = stdin.write_all(&input).await;
        drop(stdin);
    };
    let mut output = Vec::new();
    let reader = stdout.read_to_end(&mut output);

    let (_, read_result) = tokio::join!(writer, reader);
    read_result?;
    let status = child.wait().await?;
    if !status.success() || output.is_empty() {
        return Err(format!("ffmpeg snapshot scaling failed (status {status})").into());
    }
    Ok(output)
}

fn parse_event_line(line: &str) -> Option<CameraEvent> {
    // Format: Code=SmartMotionHuman;action=Start;index=0;data={...}
    let mut code = None;
    let mut action = None;
    let mut index = 0u32;
    let mut data = serde_json::Value::Null;

    for part in line.splitn(4, ';') {
        if let Some((key, value)) = part.split_once('=') {
            match key.trim() {
                "Code" => code = Some(value.to_string()),
                "action" => action = Some(value.to_string()),
                "index" => index = value.parse().unwrap_or(0),
                "data" => data = serde_json::from_str(value).unwrap_or(serde_json::Value::Null),
                _ => {}
            }
        }
    }

    Some(CameraEvent {
        code: code?,
        action: action?,
        index,
        data,
        timestamp: Utc::now(),
    })
}
