//! Client for a single Amcrest camera: digest-auth HTTP API, snapshots and
//! the `eventManager.cgi` AI event stream.

use chrono::{DateTime, Utc};
use digest_auth::AuthContext;
use log::{debug, error, info, warn};
use reqwest::Client;
use tokio::sync::broadcast;
use tokio::time::{Duration, sleep};

/// AI detection event codes to subscribe to.
const EVENT_CODES: &str =
    "SmartMotionHuman,SmartMotionVehicle,CrossLineDetection,CrossRegionDetection";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

fn encode_config_component(value: &str) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            _ => write!(&mut encoded, "%{byte:02X}").expect("writing to String cannot fail"),
        }
    }
    encoded
}

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

    async fn apply_supported_settings(
        &self,
        current: &str,
        desired: Vec<(String, String)>,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let updates: Vec<_> = desired
            .into_iter()
            .filter(|(key, value)| {
                let prefix = format!("table.{key}=");
                let expected = format!("table.{key}={value}");
                current.lines().any(|line| line.starts_with(&prefix))
                    && !current.lines().any(|line| line.trim() == expected)
            })
            .collect();
        for batch in updates.chunks(12) {
            let params = batch
                .iter()
                .map(|(key, value)| {
                    format!(
                        "{}={}",
                        encode_config_component(key),
                        encode_config_component(value)
                    )
                })
                .collect::<Vec<_>>()
                .join("&");
            self.set_config(&params).await?;
        }
        Ok(updates.len())
    }

    fn unapplied_supported_settings(current: &str, desired: &[(String, String)]) -> Vec<String> {
        desired
            .iter()
            .filter_map(|(key, value)| {
                let prefix = format!("table.{key}=");
                let expected = format!("table.{key}={value}");
                let supported = current.lines().any(|line| line.starts_with(&prefix));
                let applied = current.lines().any(|line| line.trim() == expected);
                (supported && !applied).then(|| key.clone())
            })
            .collect()
    }

    /// Applies the complete detection profile used by HomeKit motion sensors
    /// and recording triggers. Only settings reported by a camera model are
    /// written; the desired values themselves are never inherited defaults.
    pub async fn ensure_smart_motion(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let smart_profile: Vec<(String, String)> = [
            ("SmartMotionDetect[0].Enable", "true"),
            ("SmartMotionDetect[0].ObjectTypes.Human", "true"),
            ("SmartMotionDetect[0].ObjectTypes.Vehicle", "true"),
            ("SmartMotionDetect[0].Sensitivity", "Middle"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=SmartMotionDetect")
            .await?;
        let body = resp.text().await?;
        let smart_updates = self
            .apply_supported_settings(&body, smart_profile.clone())
            .await?;

        let mut motion = vec![
            ("MotionDetect[0].Enable".into(), "true".into()),
            ("MotionDetect[0].Level".into(), "3".into()),
            ("MotionDetect[0].EventHandler.Dejitter".into(), "5".into()),
            ("MotionDetect[0].EventHandler.Delay".into(), "0".into()),
            (
                "MotionDetect[0].EventHandler.AlarmOutEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.BeepEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.ExAlarmOutEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.FlashEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.LightingLink.Enable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.LogEnable".into(),
                "true".into(),
            ),
            (
                "MotionDetect[0].EventHandler.MailEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.MatrixEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.MessageEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.PtzLinkEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.RecordEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.SnapshotEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.TipEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.TourEnable".into(),
                "false".into(),
            ),
            (
                "MotionDetect[0].EventHandler.VoiceEnable".into(),
                "false".into(),
            ),
            ("MotionDetect[0].OsdTwinkleEnable".into(), "false".into()),
            ("MotionDetect[0].PirMotionLevel".into(), "3".into()),
            ("MotionDetect[0].PtzManualEnable".into(), "false".into()),
        ];
        for day in 0..7 {
            for period in 0..6 {
                motion.push((
                    format!("MotionDetect[0].EventHandler.TimeSection[{day}][{period}]"),
                    if period == 0 {
                        "1 00:00:00-23:59:59".into()
                    } else {
                        "0 00:00:00-23:59:59".into()
                    },
                ));
            }
        }
        for row in 0..18 {
            motion.push((format!("MotionDetect[0].Region[{row}]"), "4194303".into()));
            for window in 0..4 {
                motion.push((
                    format!("MotionDetect[0].MotionDetectWindow[{window}].Region[{row}]"),
                    if window == 0 { "4194303" } else { "0" }.into(),
                ));
            }
        }
        for window in 0..4 {
            motion.push((
                format!("MotionDetect[0].MotionDetectWindow[{window}].Sensitive"),
                "60".into(),
            ));
            motion.push((
                format!("MotionDetect[0].MotionDetectWindow[{window}].Threshold"),
                "5".into(),
            ));
            for coordinate in 0..4 {
                let value = if window == 0 && coordinate >= 2 {
                    "8191"
                } else {
                    "0"
                };
                motion.push((
                    format!("MotionDetect[0].MotionDetectWindow[{window}].Window[{coordinate}]"),
                    value.into(),
                ));
            }
        }
        // SmartMotion is the only analytics engine we use. Explicitly disable
        // any legacy face/IVS rule and its camera-side actions.
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=VideoAnalyseRule")
            .await?;
        let analyse_body = resp.text().await?;
        let analyse_updates = self
            .apply_supported_settings(
                &analyse_body,
                [
                    ("VideoAnalyseRule[0][0].Enable", "false"),
                    ("VideoAnalyseRule[0][0].TrackEnable", "false"),
                    ("VideoAnalyseRule[0][0].Config.FeatureEnable", "false"),
                    (
                        "VideoAnalyseRule[0][0].Config.FeatureExtractEnable",
                        "false",
                    ),
                    (
                        "VideoAnalyseRule[0][0].Config.DuplicateRemoval.Enable",
                        "false",
                    ),
                    (
                        "VideoAnalyseRule[0][0].Config.FaceBeautification.Enable",
                        "false",
                    ),
                    ("VideoAnalyseRule[0][0].Config.FilterUnAliveEnable", "false"),
                    ("VideoAnalyseRule[0][0].Config.snapObjRectEnable", "0"),
                    (
                        "VideoAnalyseRule[0][0].EventHandler.AlarmOutEnable",
                        "false",
                    ),
                    ("VideoAnalyseRule[0][0].EventHandler.BeepEnable", "false"),
                    (
                        "VideoAnalyseRule[0][0].EventHandler.ExAlarmOutEnable",
                        "false",
                    ),
                    (
                        "VideoAnalyseRule[0][0].EventHandler.LightingLink.Enable",
                        "false",
                    ),
                    ("VideoAnalyseRule[0][0].EventHandler.LogEnable", "false"),
                    ("VideoAnalyseRule[0][0].EventHandler.MMSEnable", "false"),
                    ("VideoAnalyseRule[0][0].EventHandler.MailEnable", "false"),
                    ("VideoAnalyseRule[0][0].EventHandler.MatrixEnable", "false"),
                    ("VideoAnalyseRule[0][0].EventHandler.MessageEnable", "false"),
                    ("VideoAnalyseRule[0][0].EventHandler.PtzLinkEnable", "false"),
                    ("VideoAnalyseRule[0][0].EventHandler.RecordEnable", "false"),
                    (
                        "VideoAnalyseRule[0][0].EventHandler.SnapshotEnable",
                        "false",
                    ),
                    (
                        "VideoAnalyseRule[0][0].EventHandler.SnapshotTitleEnable",
                        "false",
                    ),
                    ("VideoAnalyseRule[0][0].EventHandler.VoiceEnable", "false"),
                ]
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            )
            .await?;

        // Updating the legacy analytics engine resets SmartMotion/MotionDetect
        // on several firmware families. Read them back and apply both profiles
        // after VideoAnalyseRule, with MotionDetect deliberately last.
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=SmartMotionDetect")
            .await?;
        let smart_body = resp.text().await?;
        let final_smart_updates = self
            .apply_supported_settings(&smart_body, smart_profile.clone())
            .await?;
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=MotionDetect")
            .await?;
        let final_motion_body = resp.text().await?;
        let motion_updates = self
            .apply_supported_settings(&final_motion_body, motion.clone())
            .await?;
        let total_updates = smart_updates + analyse_updates + final_smart_updates + motion_updates;
        if total_updates > 0 {
            info!(
                "[{}] requested AI/motion normalization ({} reported settings differed)",
                self.host, total_updates
            );
        }

        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=SmartMotionDetect")
            .await?;
        let verified_smart = resp.text().await?;
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=MotionDetect")
            .await?;
        let verified_motion = resp.text().await?;
        let mut refused = Self::unapplied_supported_settings(&verified_smart, &smart_profile);
        refused.extend(Self::unapplied_supported_settings(
            &verified_motion,
            &motion,
        ));
        if !refused.is_empty() {
            warn!(
                "[{}] camera refused {} reported AI/motion settings after setConfig: {}",
                self.host,
                refused.len(),
                refused.join(", ")
            );
        } else {
            debug!("[{}] AI/motion detection profile verified", self.host);
        }
        Ok(())
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
        let quality = field("Video.Quality").and_then(|v| v.parse::<u32>().ok());
        let pack = field("Video.Pack").unwrap_or_default();
        let priority = field("Video.Priority").and_then(|v| v.parse::<u32>().ok());
        let svc_layers = field("Video.SVCTLayer").and_then(|v| v.parse::<u32>().ok());
        let ai_gop = field("Video.AiGOP").and_then(|v| v.parse::<u32>().ok());
        let audio_enabled = field("AudioEnable").as_deref() == Some("true");
        let audio_codec = field("Audio.Compression").unwrap_or_default();
        let audio_frequency = field("Audio.Frequency")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let audio_bitrate = field("Audio.Bitrate")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let audio_depth = field("Audio.Depth").and_then(|v| v.parse::<u32>().ok());
        let audio_channel = field("Audio.Channels[0]").and_then(|v| v.parse::<u32>().ok());
        let audio_pack = field("Audio.Pack").unwrap_or_default();

        if enabled
            && h264
            && width == 1280
            && height == 720
            && fps == 15
            && gop == 15
            && bitrate == 1024
            && bitrate_control == "VBR"
            && profile == "Main"
            && quality == Some(4)
            && pack == "DHAV"
            && priority == Some(0)
            && svc_layers == Some(1)
            && ai_gop.is_none_or(|value| value == 15)
            && audio_enabled
            && audio_codec == "AAC"
            && audio_frequency == 16000
            && audio_bitrate == 64
            && audio_depth == Some(16)
            && audio_channel == Some(0)
            && audio_pack == "DHAV"
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
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Pack=DHAV\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.Priority=0\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.SVCTLayer=1\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.AudioEnable=true\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Compression=AAC\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Bitrate=64\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Depth=16\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Channels%5B0%5D=0\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Pack=DHAV\
             &Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Audio.Frequency=16000"
        );
        self.set_config(&params).await?;
        if ai_gop.is_some() && ai_gop != Some(15) {
            self.set_config(&format!(
                "Encode%5B0%5D.ExtraFormat%5B{idx}%5D.Video.AiGOP=15"
            ))
            .await?;
        }
        Ok(())
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
            "table.All.SmartEncode[0].Enable=false",
            "table.All.VideoWaterMark[0].Enable=false",
            "table.All.Encode[0].MainFormat[0].AudioEnable=true",
            "table.All.Encode[0].MainFormat[0].Audio.Compression=AAC",
            "table.All.Encode[0].MainFormat[0].Audio.Frequency=48000",
            "table.All.Encode[0].MainFormat[0].Audio.Bitrate=64",
            "table.All.Encode[0].MainFormat[0].Audio.Depth=16",
            "table.All.Encode[0].MainFormat[0].Audio.Channels[0]=0",
            "table.All.Encode[0].MainFormat[0].Audio.Mode=0",
            "table.All.Encode[0].MainFormat[0].Audio.Pack=DHAV",
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
             &SmartEncode%5B0%5D.Enable=false\
             &VideoWaterMark%5B0%5D.Enable=false\
             &Encode%5B0%5D.MainFormat%5B0%5D.AudioEnable=true\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Compression=AAC\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Frequency=48000\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Bitrate=64\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Depth=16\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Channels%5B0%5D=0\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Mode=0\
             &Encode%5B0%5D.MainFormat%5B0%5D.Audio.Pack=DHAV",
        )
        .await
    }

    /// Applies a deterministic, minimal burned-in overlay to every camera.
    /// Font sizes are explicit and proportional to each stream's configured
    /// resolution, so the timestamp has the same apparent size after Home
    /// scales a 1080p snapshot or 720p/480p live stream into a tile.
    pub async fn ensure_overlay_profile(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let resp = self
            .get("/cgi-bin/configManager.cgi?action=getConfig&name=VideoWidget")
            .await?;
        let body = resp.text().await?;
        let week_position = if body.contains("table.VideoWidget[0].TimeTitle.WeekPosition=Right")
            || body.contains("table.VideoWidget[0].TimeTitle.WeekPosition=Left")
        {
            "Right"
        } else {
            "0"
        };
        let core_settings = [
            ("VideoWidget[0].FontBorder", "true"),
            ("VideoWidget[0].FontSize", "36"),
            ("VideoWidget[0].FontSizeExtra1", "16"),
            ("VideoWidget[0].FontSizeExtra2", "24"),
            ("VideoWidget[0].FontSizeExtra3", "16"),
            ("VideoWidget[0].FontSizeSnapshot", "36"),
            ("VideoWidget[0].FontSizeScale", "1"),
            ("VideoWidget[0].TimeTitle.EncodeBlend", "true"),
            ("VideoWidget[0].TimeTitle.PreviewBlend", "true"),
            ("VideoWidget[0].TimeTitle.ShowWeek", "false"),
            ("VideoWidget[0].TimeTitle.WeekPosition", week_position),
            ("VideoWidget[0].TimeTitle.FrontColor[0]", "255"),
            ("VideoWidget[0].TimeTitle.FrontColor[1]", "255"),
            ("VideoWidget[0].TimeTitle.FrontColor[2]", "255"),
            ("VideoWidget[0].TimeTitle.FrontColor[3]", "0"),
            ("VideoWidget[0].TimeTitle.BackColor[0]", "0"),
            ("VideoWidget[0].TimeTitle.BackColor[1]", "0"),
            ("VideoWidget[0].TimeTitle.BackColor[2]", "0"),
            ("VideoWidget[0].TimeTitle.BackColor[3]", "128"),
            ("VideoWidget[0].TimeTitle.Rect[0]", "5319"),
            ("VideoWidget[0].TimeTitle.Rect[1]", "352"),
            ("VideoWidget[0].TimeTitle.Rect[2]", "7929"),
            ("VideoWidget[0].TimeTitle.Rect[3]", "769"),
            ("VideoWidget[0].ChannelTitle.EncodeBlend", "false"),
            ("VideoWidget[0].ChannelTitle.PreviewBlend", "false"),
            ("VideoWidget[0].OSDMobileState.EncodeBlend", "false"),
            ("VideoWidget[0].OSDMobileState.PreviewBlend", "false"),
            ("VideoWidget[0].PictureTitle.EncodeBlend", "false"),
            ("VideoWidget[0].PictureTitle.PreviewBlend", "false"),
        ];
        let disabled_overlays = [
            "PTZCoordinates",
            "PTZDirection",
            "PTZOSDMenu",
            "PTZOSDMenuViaApp",
            "PTZPreset",
            "PTZZoom",
            "PtzPattern",
            "PtzRS485Detect",
            "Temperature",
            "VoltageStatus",
            "CustomTitle[0]",
            "CustomTitle[1]",
            "CustomTitle[2]",
            "CustomTitle[3]",
            "UserDefinedTitle[0]",
            "UserDefinedTitle[1]",
            "UserDefinedTitle[2]",
            "UserDefinedTitle[3]",
            "Covers[0]",
            "Covers[1]",
            "Covers[2]",
            "Covers[3]",
        ];
        let needs_core_update: Vec<_> = core_settings
            .iter()
            .filter(|(key, value)| {
                let prefix = format!("table.{key}=");
                if !body.lines().any(|line| line.starts_with(&prefix)) {
                    return false;
                }
                if *key == "VideoWidget[0].FontSizeScale" && *value == "1" {
                    return !(body.contains("table.VideoWidget[0].FontSizeScale=1\n")
                        || body.contains("table.VideoWidget[0].FontSizeScale=1\r\n")
                        || body.contains("table.VideoWidget[0].FontSizeScale=1.000000"));
                }
                !body.contains(&format!("table.{key}={value}"))
            })
            .collect();
        if !needs_core_update.is_empty() {
            info!(
                "[{}] normalizing timestamp and overlay appearance",
                self.host
            );
            let params = needs_core_update
                .into_iter()
                .map(|(key, value)| {
                    let encoded = key.replace('[', "%5B").replace(']', "%5D");
                    format!("{encoded}={value}")
                })
                .collect::<Vec<_>>()
                .join("&");
            self.set_config(&params).await?;
        }

        // Optional OSD elements vary by model. Disable only fields this
        // camera reports, using small requests because configManager rejects
        // the complete profile when its URL grows too large.
        for name in disabled_overlays {
            let encode_key = format!("table.VideoWidget[0].{name}.EncodeBlend");
            let preview_key = format!("table.VideoWidget[0].{name}.PreviewBlend");
            let supported = body.contains(&format!("{encode_key}="))
                && body.contains(&format!("{preview_key}="));
            let disabled = body.contains(&format!("{encode_key}=false"))
                && body.contains(&format!("{preview_key}=false"));
            if supported && !disabled {
                let encoded = name.replace('[', "%5B").replace(']', "%5D");
                self.set_config(&format!(
                    "VideoWidget%5B0%5D.{encoded}.EncodeBlend=false\
                     &VideoWidget%5B0%5D.{encoded}.PreviewBlend=false"
                ))
                .await?;
            }
        }
        for suffix in ["Extra1", "Extra2"] {
            let key = format!("table.VideoWidget[0].PTZOSDMenuViaApp.EncodeBlend{suffix}");
            if body.contains(&format!("{key}=")) && !body.contains(&format!("{key}=false")) {
                self.set_config(&format!(
                    "VideoWidget%5B0%5D.PTZOSDMenuViaApp.EncodeBlend{suffix}=false"
                ))
                .await?;
            }
        }
        Ok(())
    }

    /// Fetches a JPEG snapshot from the camera.
    pub async fn snapshot(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let resp = self.get("/cgi-bin/snapshot.cgi").await?;
        Ok(resp.bytes().await?.to_vec())
    }

    /// Runs the AI event stream forever, reconnecting on errors, publishing
    /// events to `tx`.
    pub async fn run_event_stream(
        &self,
        tx: broadcast::Sender<CameraEvent>,
        metrics: std::sync::Arc<crate::metrics::Metrics>,
    ) {
        loop {
            debug!("[{}] connecting to event stream...", self.host);
            match self.stream_events(&tx, &metrics).await {
                Ok(()) => warn!("[{}] event stream ended, reconnecting...", self.host),
                Err(e) => {
                    metrics.error(crate::metrics::ErrorSubsystem::EventStream);
                    error!("[{}] event stream error: {e}, reconnecting...", self.host);
                }
            }
            metrics.event_stream_connected(false);
            metrics.event_stream_reconnect();
            sleep(RECONNECT_DELAY).await;
        }
    }

    async fn stream_events(
        &self,
        tx: &broadcast::Sender<CameraEvent>,
        metrics: &crate::metrics::Metrics,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = format!("/cgi-bin/eventManager.cgi?action=attach&codes=[{EVENT_CODES}]");
        let resp = self.get(&path).await?;

        debug!("[{}] connected, streaming events...", self.host);
        metrics.event_stream_connected(true);

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
    latest: std::sync::Arc<tokio::sync::RwLock<Option<SnapshotFrame>>>,
    scaled:
        std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<(u32, u32), ScaledSnapshot>>>,
    refresh_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    next_generation: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Clone)]
struct SnapshotFrame {
    generation: u64,
    fetched_at: tokio::time::Instant,
    fingerprint: u64,
    bytes: Vec<u8>,
}

struct ScaledSnapshot {
    source_generation: u64,
    bytes: Vec<u8>,
}

pub struct SnapshotImage {
    pub bytes: Vec<u8>,
    pub source_generation: u64,
    pub source_age: Duration,
    pub source_fingerprint: u64,
    pub output_fingerprint: u64,
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
            refresh_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            next_generation: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Spawns the background refresh loop.
    pub fn spawn_refresher(&self) {
        let cache = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SNAPSHOT_REFRESH);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                match cache.refresh().await {
                    Ok(_) => {}
                    Err(e) => warn!("[{}] snapshot refresh failed: {e}", cache.client.host),
                }
            }
        });
    }

    async fn refresh(&self) -> Result<SnapshotFrame, Box<dyn std::error::Error + Send + Sync>> {
        use std::hash::{Hash, Hasher};
        use std::sync::atomic::Ordering;

        let _guard = self.refresh_lock.lock().await;
        let bytes = self.client.snapshot().await?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut hasher);
        let frame = SnapshotFrame {
            generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
            fetched_at: tokio::time::Instant::now(),
            fingerprint: hasher.finish(),
            bytes,
        };
        *self.latest.write().await = Some(frame.clone());
        Ok(frame)
    }

    /// Returns a recent camera frame. A failed refresh may fall back to the
    /// last good frame, but never for longer than SNAPSHOT_MAX_AGE.
    async fn get(&self) -> Result<SnapshotFrame, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(frame) = self.latest.read().await.clone() {
            if frame.fetched_at.elapsed() <= SNAPSHOT_REFRESH + Duration::from_secs(2) {
                return Ok(frame);
            }
        }

        match self.refresh().await {
            Ok(frame) => Ok(frame),
            Err(error) => {
                if let Some(frame) = self.latest.read().await.clone()
                    && frame.fetched_at.elapsed() <= SNAPSHOT_MAX_AGE
                {
                    warn!(
                        "[{}] serving snapshot source aged {:.1}s after refresh failure: {error}",
                        self.client.host,
                        frame.fetched_at.elapsed().as_secs_f32()
                    );
                    return Ok(frame);
                }
                Err(error)
            }
        }
    }

    /// Returns the most recent snapshot scaled to the requested dimensions, as
    /// HAP controllers expect. Scaled variants are cached per size.
    pub async fn get_scaled(
        &self,
        width: u32,
        height: u32,
    ) -> Result<SnapshotImage, Box<dyn std::error::Error + Send + Sync>> {
        let raw = self.get().await?;
        {
            let scaled = self.scaled.lock().await;
            if let Some(snapshot) = scaled.get(&(width, height)) {
                if snapshot.source_generation == raw.generation {
                    return Ok(SnapshotImage {
                        bytes: snapshot.bytes.clone(),
                        source_generation: raw.generation,
                        source_age: raw.fetched_at.elapsed(),
                        source_fingerprint: raw.fingerprint,
                        output_fingerprint: fingerprint(&snapshot.bytes),
                    });
                }
            }
        }

        let bytes = scale_jpeg(raw.bytes, width, height).await?;
        self.scaled.lock().await.insert(
            (width, height),
            ScaledSnapshot {
                source_generation: raw.generation,
                bytes: bytes.clone(),
            },
        );
        Ok(SnapshotImage {
            output_fingerprint: fingerprint(&bytes),
            bytes,
            source_generation: raw.generation,
            source_age: raw.fetched_at.elapsed(),
            source_fingerprint: raw.fingerprint,
        })
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
    if !output.starts_with(&[0xff, 0xd8]) || !output.ends_with(&[0xff, 0xd9]) {
        return Err("ffmpeg snapshot scaling produced an invalid JPEG".into());
    }
    Ok(output)
}

fn fingerprint(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
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
