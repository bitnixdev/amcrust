//! HomeKit camera RTP stream management: SetupEndpoints / SelectedRTPStreamConfiguration
//! TLV8 negotiation and the ffmpeg RTSP→SRTP media pipeline.

use log::{error, info, warn};
use rand::Rng;
use std::net::IpAddr;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::tlv8;

// SetupEndpoints request/response tags.
const SETUP_SESSION_ID: u8 = 0x01;
const SETUP_STATUS: u8 = 0x02;
const SETUP_ADDRESS: u8 = 0x03;
const SETUP_VIDEO_SRTP: u8 = 0x04;
const SETUP_AUDIO_SRTP: u8 = 0x05;
const SETUP_VIDEO_SSRC: u8 = 0x06;
const SETUP_AUDIO_SSRC: u8 = 0x07;

// Address tags.
const ADDR_IP_VERSION: u8 = 0x01;
const ADDR_IP: u8 = 0x02;
const ADDR_VIDEO_PORT: u8 = 0x03;
const ADDR_AUDIO_PORT: u8 = 0x04;

// SRTP parameter tags.
const SRTP_CRYPTO_SUITE: u8 = 0x01;
const SRTP_MASTER_KEY: u8 = 0x02;
const SRTP_MASTER_SALT: u8 = 0x03;

// SelectedRTPStreamConfiguration tags.
const SELECTED_SESSION_CONTROL: u8 = 0x01;
const SELECTED_VIDEO_PARAMS: u8 = 0x02;

const CONTROL_SESSION_ID: u8 = 0x01;
const CONTROL_COMMAND: u8 = 0x02;

const COMMAND_END: u8 = 0x00;
const COMMAND_START: u8 = 0x01;
const COMMAND_SUSPEND: u8 = 0x02;
const COMMAND_RESUME: u8 = 0x03;
const COMMAND_RECONFIGURE: u8 = 0x04;

// Selected video parameter tags.
const VIDEO_RTP_PARAMS: u8 = 0x04;

// Selected audio parameter tags (within SELECTED_AUDIO_PARAMS).
const SELECTED_AUDIO_PARAMS: u8 = 0x03;
const AUDIO_CODEC_PARAMS: u8 = 0x02;
const AUDIO_RTP_PARAMS: u8 = 0x03;
const AUDIO_PARAM_SAMPLE_RATE: u8 = 0x03;

// RTP parameter tags.
const RTP_PAYLOAD_TYPE: u8 = 0x01;
const RTP_MAX_MTU: u8 = 0x05;

#[derive(Debug, Clone)]
struct Session {
    id: Vec<u8>,
    controller_ip: String,
    video_port: u16,
    audio_port: u16,
    /// SRTP master key || master salt, as ffmpeg wants it.
    video_key: Vec<u8>,
    audio_key: Vec<u8>,
    video_ssrc: u32,
    audio_ssrc: u32,
}

#[derive(Default)]
struct Inner {
    /// Prepared sessions and running streams, keyed by HAP session id.
    sessions: std::collections::HashMap<Vec<u8>, Session>,
    children: std::collections::HashMap<Vec<u8>, Child>,
    /// Audio RTP proxy tasks, keyed by HAP session id.
    audio_proxies: std::collections::HashMap<Vec<u8>, tokio::task::JoinHandle<()>>,
    /// Response for the most recent SetupEndpoints write, read back by the
    /// controller immediately after writing.
    setup_response: Vec<u8>,
}

/// Manages the active HomeKit streams for this camera.
#[derive(Clone)]
pub struct StreamManager {
    inner: Arc<Mutex<Inner>>,
    rtsp_url: String,
    audio: bool,
    local_ip: IpAddr,
}

impl StreamManager {
    pub fn new(rtsp_url: String, audio: bool, local_ip: IpAddr) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            rtsp_url,
            audio,
            local_ip,
        }
    }

    pub fn audio_enabled(&self) -> bool {
        self.audio
    }

    /// Handles a controller write to SetupEndpoints and prepares the response
    /// that the controller will read back.
    pub async fn handle_setup_write(&self, payload: Vec<u8>) {
        let items = tlv8::parse(&payload);

        // hap-rs applies on_read results via set_value, which re-fires this
        // on_update handler with our own response. Requests never carry a
        // status tag — if one is present, this is that echo; ignore it.
        if tlv8::find(&items, SETUP_STATUS).is_some() {
            return;
        }

        let Some(session_id) = tlv8::find(&items, SETUP_SESSION_ID).map(|v| v.to_vec()) else {
            warn!("SetupEndpoints write without session id");
            return;
        };

        let addr_items = tlv8::find(&items, SETUP_ADDRESS)
            .map(|v| tlv8::parse(v))
            .unwrap_or_default();
        let controller_ip = tlv8::find(&addr_items, ADDR_IP)
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_default();
        let video_port = tlv8::find_u16(&addr_items, ADDR_VIDEO_PORT).unwrap_or(0);
        let audio_port = tlv8::find_u16(&addr_items, ADDR_AUDIO_PORT).unwrap_or(0);

        let video_srtp = tlv8::find(&items, SETUP_VIDEO_SRTP)
            .map(|v| v.to_vec())
            .unwrap_or_default();
        let audio_srtp = tlv8::find(&items, SETUP_AUDIO_SRTP)
            .map(|v| v.to_vec())
            .unwrap_or_default();

        let srtp_key = |raw: &[u8]| -> Vec<u8> {
            let items = tlv8::parse(raw);
            let mut key = tlv8::find(&items, SRTP_MASTER_KEY)
                .map(|v| v.to_vec())
                .unwrap_or_default();
            let salt = tlv8::find(&items, SRTP_MASTER_SALT)
                .map(|v| v.to_vec())
                .unwrap_or_default();
            key.extend_from_slice(&salt);
            key
        };

        let (video_ssrc, audio_ssrc) = {
            let mut rng = rand::thread_rng();
            (rng.gen_range(1..0x7fff_ffff), rng.gen_range(1..0x7fff_ffff))
        };
        let session = Session {
            id: session_id.clone(),
            controller_ip: controller_ip.clone(),
            video_port,
            audio_port,
            video_key: srtp_key(&video_srtp),
            audio_key: srtp_key(&audio_srtp),
            video_ssrc,
            audio_ssrc,
        };

        info!(
            "stream setup: controller {}:{} (video) :{} (audio)",
            controller_ip, video_port, audio_port
        );

        // Build the response: echo the session id and SRTP parameters, present
        // our own address and freshly chosen SSRCs.
        let ip_string = self.local_ip.to_string();
        let ip_version: u8 = if self.local_ip.is_ipv6() { 1 } else { 0 };

        let accessory_address = tlv8::Writer::new()
            .u8(ADDR_IP_VERSION, ip_version)
            .bytes(ADDR_IP, ip_string.as_bytes())
            .u16(ADDR_VIDEO_PORT, video_port)
            .u16(ADDR_AUDIO_PORT, audio_port)
            .build();

        let response = tlv8::Writer::new()
            .bytes(SETUP_SESSION_ID, &session_id)
            .u8(SETUP_STATUS, 0)
            .bytes(SETUP_ADDRESS, &accessory_address)
            .bytes(SETUP_VIDEO_SRTP, &video_srtp)
            .bytes(SETUP_AUDIO_SRTP, &audio_srtp)
            .u32(SETUP_VIDEO_SSRC, session.video_ssrc)
            .u32(SETUP_AUDIO_SSRC, session.audio_ssrc)
            .build();

        let mut inner = self.inner.lock().await;
        inner.sessions.insert(session_id, session);
        inner.setup_response = response;
    }

    /// The response to a controller read of SetupEndpoints.
    pub async fn setup_read(&self) -> Option<Vec<u8>> {
        let inner = self.inner.lock().await;
        if inner.setup_response.is_empty() {
            None
        } else {
            Some(inner.setup_response.clone())
        }
    }

    /// StreamingStatus TLV: always report available — we serve one stream per
    /// controller session concurrently.
    pub async fn streaming_status(&self) -> Vec<u8> {
        tlv8::Writer::new().u8(0x01, 0).build()
    }

    /// Handles a controller write to SelectedRTPStreamConfiguration.
    pub async fn handle_selected_write(&self, payload: Vec<u8>) {
        let items = tlv8::parse(&payload);

        let control = tlv8::find(&items, SELECTED_SESSION_CONTROL)
            .map(|v| tlv8::parse(v))
            .unwrap_or_default();
        let command = tlv8::find_u8(&control, CONTROL_COMMAND);
        let session_id = tlv8::find(&control, CONTROL_SESSION_ID).map(|v| v.to_vec());

        match command {
            Some(COMMAND_START) => {
                // Requested payload type & MTU live in the selected video params.
                let video_params = tlv8::find(&items, SELECTED_VIDEO_PARAMS)
                    .map(|v| tlv8::parse(v))
                    .unwrap_or_default();
                let rtp_params = tlv8::find(&video_params, VIDEO_RTP_PARAMS)
                    .map(|v| tlv8::parse(v))
                    .unwrap_or_default();
                let payload_type = tlv8::find_u8(&rtp_params, RTP_PAYLOAD_TYPE).unwrap_or(99);
                let max_mtu = tlv8::find_u16(&rtp_params, RTP_MAX_MTU).unwrap_or(1316);

                // Audio: negotiated Opus payload type & sample rate.
                let audio_params = tlv8::find(&items, SELECTED_AUDIO_PARAMS)
                    .map(|v| tlv8::parse(v))
                    .unwrap_or_default();
                let audio_rtp = tlv8::find(&audio_params, AUDIO_RTP_PARAMS)
                    .map(|v| tlv8::parse(v))
                    .unwrap_or_default();
                let audio_payload_type = tlv8::find_u8(&audio_rtp, RTP_PAYLOAD_TYPE).unwrap_or(110);
                let audio_codec_params = tlv8::find(&audio_params, AUDIO_CODEC_PARAMS)
                    .map(|v| tlv8::parse(v))
                    .unwrap_or_default();
                let audio_rate_hz =
                    match tlv8::find_u8(&audio_codec_params, AUDIO_PARAM_SAMPLE_RATE) {
                        Some(0) => 8000,
                        Some(2) => 24000,
                        _ => 16000,
                    };

                self.start_stream(
                    session_id,
                    payload_type,
                    max_mtu,
                    audio_payload_type,
                    audio_rate_hz,
                )
                .await;
            }
            Some(COMMAND_END) => {
                info!("stream end requested");
                self.end_session(session_id).await;
            }
            Some(COMMAND_SUSPEND) => {
                info!("stream suspend requested");
                self.end_session(session_id).await;
            }
            Some(COMMAND_RESUME) | Some(COMMAND_RECONFIGURE) => {
                info!("stream resume/reconfigure requested (ignored)");
            }
            other => warn!("unknown stream command: {other:?}"),
        }
    }

    async fn start_stream(
        &self,
        session_id: Option<Vec<u8>>,
        payload_type: u8,
        max_mtu: u16,
        audio_payload_type: u8,
        audio_rate_hz: u32,
    ) {
        let mut inner = self.inner.lock().await;

        let session = match &session_id {
            Some(sid) => inner.sessions.get(sid).cloned(),
            // No session id in the request: only unambiguous with one session.
            None if inner.sessions.len() == 1 => inner.sessions.values().next().cloned(),
            None => None,
        };
        let Some(session) = session else {
            warn!("start requested but no matching session prepared");
            return;
        };

        // Tear down any previous stream for this session first.
        if let Some(mut child) = inner.children.remove(&session.id) {
            let _ = child.start_kill();
        }
        if let Some(proxy) = inner.audio_proxies.remove(&session.id) {
            proxy.abort();
        }

        let pkt_size = max_mtu.clamp(188, 1378);
        let video_dest = format!(
            "srtp://{}:{}?rtcpport={}&pkt_size={}",
            session.controller_ip, session.video_port, session.video_port, pkt_size
        );

        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;

        let mut cmd = Command::new("ffmpeg");
        cmd.arg("-hide_banner")
            .args(["-loglevel", "warning"])
            .args(["-fflags", "+genpts+nobuffer"])
            .args(["-flags", "low_delay"])
            // The camera's own timestamps jump backwards (clock wobble); its
            // packet *delivery* is smooth, so arrival time is the better clock.
            .args(["-use_wallclock_as_timestamps", "1"])
            // H.264 parameters arrive via the RTSP SDP, so skip input probing —
            // it otherwise delays the first RTP packet by several seconds and
            // iOS gives up on the stream.
            .args(["-probesize", "65536"])
            .args(["-analyzeduration", "0"])
            .args(["-rtsp_transport", "tcp"])
            .args(["-i", &self.rtsp_url])
            .arg("-an")
            .args(["-c:v", "copy"])
            .args(["-payload_type", &payload_type.to_string()])
            .args(["-ssrc", &session.video_ssrc.to_string()])
            .args(["-f", "rtp"])
            .args(["-srtp_out_suite", "AES_CM_128_HMAC_SHA1_80"])
            .args(["-srtp_out_params", &b64.encode(&session.video_key)])
            .arg(&video_dest);

        // Audio goes through a local RTP proxy: ffmpeg stamps Opus with the
        // RFC 7587 48 kHz RTP clock, but HomeKit expects the negotiated sample
        // rate. The proxy rescales the timestamps and applies SRTP itself.
        let mut audio_proxy: Option<tokio::task::JoinHandle<()>> = None;
        if self.audio && !session.audio_key.is_empty() {
            match tokio::net::UdpSocket::bind("127.0.0.1:0").await {
                Ok(local_socket) => {
                    let local_port = local_socket.local_addr().map(|a| a.port()).unwrap_or(0);
                    // RTCP goes to local_port+1 (unbound, dropped) — the proxy
                    // only ever sees clean RTP on its own socket.
                    let audio_dest = format!("rtp://127.0.0.1:{local_port}");
                    cmd.arg("-vn")
                        .args(["-c:a", "libopus"])
                        .args(["-application", "lowdelay"])
                        .args(["-frame_duration", "20"])
                        // The camera under-delivers audio samples relative to
                        // wall time; async resampling fills gaps with silence
                        // so the output tracks real time.
                        .args([
                            "-af",
                            "aresample=async=1000:min_hard_comp=0.100:first_pts=0",
                        ])
                        .args(["-ar", &audio_rate_hz.to_string()])
                        .args(["-ac", "1"])
                        .args(["-b:a", "32k"])
                        .args(["-payload_type", &audio_payload_type.to_string()])
                        .args(["-ssrc", &session.audio_ssrc.to_string()])
                        .args(["-f", "rtp"])
                        .arg(&audio_dest);

                    audio_proxy = spawn_audio_proxy(
                        local_socket,
                        session.controller_ip.clone(),
                        session.audio_port,
                        session.audio_key.clone(),
                        48000 / audio_rate_hz.max(1),
                    );
                }
                Err(e) => warn!("could not bind audio proxy socket, audio disabled: {e}"),
            }
        }

        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        cmd.kill_on_drop(true);

        info!("starting ffmpeg → {video_dest}");
        match cmd.spawn() {
            Ok(mut child) => {
                if let Some(stderr) = child.stderr.take() {
                    tokio::spawn(async move {
                        let mut lines = BufReader::new(stderr).lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            warn!("ffmpeg: {line}");
                        }
                    });
                }
                inner.children.insert(session.id.clone(), child);
                if let Some(proxy) = audio_proxy {
                    inner.audio_proxies.insert(session.id.clone(), proxy);
                }
            }
            Err(e) => {
                error!("failed to spawn ffmpeg: {e}");
                if let Some(proxy) = audio_proxy {
                    proxy.abort();
                }
            }
        }
    }

    /// Ends the stream for one session (or all, if no id was given).
    async fn end_session(&self, session_id: Option<Vec<u8>>) {
        let mut inner = self.inner.lock().await;
        match session_id {
            Some(sid) => {
                if let Some(mut child) = inner.children.remove(&sid) {
                    let _ = child.start_kill();
                    info!("stream stopped");
                }
                if let Some(proxy) = inner.audio_proxies.remove(&sid) {
                    proxy.abort();
                }
                inner.sessions.remove(&sid);
            }
            None => {
                for (_, mut child) in inner.children.drain() {
                    let _ = child.start_kill();
                }
                for (_, proxy) in inner.audio_proxies.drain() {
                    proxy.abort();
                }
                inner.sessions.clear();
            }
        }
    }

    /// Stops all streams; used on shutdown.
    pub async fn stop_stream(&self) {
        self.end_session(None).await;
        let mut inner = self.inner.lock().await;
        inner.setup_response.clear();
    }
}

/// Receives plain RTP from ffmpeg on a local socket, rescales the Opus RTP
/// timestamps from 48 kHz to the HomeKit clock, applies SRTP, and forwards to
/// the controller.
fn spawn_audio_proxy(
    local_socket: tokio::net::UdpSocket,
    controller_ip: String,
    audio_port: u16,
    audio_key: Vec<u8>,
    ratio: u32,
) -> Option<tokio::task::JoinHandle<()>> {
    let mut sender = match crate::srtp::SrtpSender::new(&audio_key) {
        Some(sender) => sender,
        None => {
            warn!("invalid audio SRTP key material, audio disabled");
            return None;
        }
    };
    let mut rescaler = crate::srtp::TimestampRescaler::new(ratio);

    Some(tokio::spawn(async move {
        let out_socket = match tokio::net::UdpSocket::bind("0.0.0.0:0").await {
            Ok(s) => s,
            Err(e) => {
                warn!("audio proxy output socket failed: {e}");
                return;
            }
        };
        let dest = format!("{controller_ip}:{audio_port}");
        let mut buf = [0u8; 2048];
        let mut sent: u64 = 0;
        loop {
            let Ok(n) = local_socket.recv(&mut buf).await else {
                break;
            };
            let packet = &mut buf[..n];
            // Skip RTCP (payload types 200-204 appear where RTP has PT).
            if n >= 12 && packet[0] >> 6 == 2 && !(200..=204).contains(&packet[1]) {
                rescaler.rescale(packet);
                if let Some(protected) = sender.protect(packet) {
                    if out_socket.send_to(&protected, &dest).await.is_err() {
                        break;
                    }
                    sent += 1;
                    if sent == 1 {
                        info!("audio proxy: first packet → {dest}");
                    }
                }
            }
        }
    }))
}

// --- Supported configuration TLVs, advertised via the stream management service ---

/// SupportedVideoStreamConfiguration: H.264 (all common profiles/levels,
/// non-interleaved packetization) at a set of resolutions up to 720p.
pub fn supported_video_config() -> Vec<u8> {
    let mut params = tlv8::Writer::new();
    // Profiles: constrained baseline, main, high.
    params
        .u8(0x01, 0x00)
        .delimiter()
        .u8(0x01, 0x01)
        .delimiter()
        .u8(0x01, 0x02);
    // Levels: 3.1, 3.2, 4.0.
    params
        .u8(0x02, 0x00)
        .delimiter()
        .u8(0x02, 0x01)
        .delimiter()
        .u8(0x02, 0x02);
    // Packetization mode: non-interleaved.
    params.u8(0x03, 0x00);
    let params = params.build();

    let attributes: Vec<Vec<u8>> = [(1280u16, 720u16), (640, 360), (480, 270), (320, 240)]
        .iter()
        .map(|&(w, h)| {
            tlv8::Writer::new()
                .u16(0x01, w)
                .u16(0x02, h)
                .u8(0x03, 30)
                .build()
        })
        .collect();

    let mut codec_config = tlv8::Writer::new();
    codec_config.u8(0x01, 0x00); // codec type: H.264
    codec_config.bytes(0x02, &params);
    for (i, attr) in attributes.iter().enumerate() {
        if i > 0 {
            codec_config.delimiter();
        }
        codec_config.bytes(0x03, attr);
    }
    let codec_config = codec_config.build();

    tlv8::Writer::new().bytes(0x01, &codec_config).build()
}

/// SupportedAudioStreamConfiguration: Opus mono @ 16 kHz, no comfort noise.
pub fn supported_audio_config() -> Vec<u8> {
    let params = tlv8::Writer::new()
        .u8(0x01, 1) // channels
        .u8(0x02, 0x00) // variable bitrate
        .u8(0x03, 0x01) // 16 kHz
        .build();

    let codec_config = tlv8::Writer::new()
        .u8(0x01, 0x03) // codec type: Opus
        .bytes(0x02, &params)
        .build();

    tlv8::Writer::new()
        .bytes(0x01, &codec_config)
        .u8(0x02, 0x00) // comfort noise not supported
        .build()
}

/// SupportedRTPConfiguration: SRTP with AES_CM_128_HMAC_SHA1_80.
pub fn supported_rtp_config() -> Vec<u8> {
    tlv8::Writer::new().u8(0x02, 0x00).build()
}
