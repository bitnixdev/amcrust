//! HomeKit camera RTP stream management: SetupEndpoints / SelectedRTPStreamConfiguration
//! TLV8 negotiation and the ffmpeg RTSP→SRTP media pipeline.

use log::{debug, error, warn};
use rand::Rng;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::metrics::{ErrorSubsystem, Metrics};
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
const AUDIO_PARAM_PACKET_TIME: u8 = 0x04;
const AUDIO_PARAM_SAMPLE_RATE: u8 = 0x03;

// RTP parameter tags.
const RTP_PAYLOAD_TYPE: u8 = 0x01;
const RTP_MAX_BITRATE: u8 = 0x03;
const RTP_MIN_RTCP_INTERVAL: u8 = 0x04;
const RTP_MAX_MTU: u8 = 0x05;

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
    video_out_socket: Option<tokio::net::UdpSocket>,
    audio_out_socket: Option<tokio::net::UdpSocket>,
}

#[derive(Default)]
struct Inner {
    /// Prepared sessions and running streams, keyed by HAP session id.
    sessions: std::collections::HashMap<Vec<u8>, Session>,
    /// Video and audio use independent FFmpeg processes so an audio source or
    /// resampler stall can never throttle stream-copied video.
    children: std::collections::HashMap<Vec<u8>, Child>,
    audio_children: std::collections::HashMap<Vec<u8>, Child>,
    video_proxies: std::collections::HashMap<Vec<u8>, tokio::task::JoinHandle<()>>,
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
    audio_rtsp_url: String,
    audio: bool,
    local_ip: IpAddr,
    metrics: Arc<Metrics>,
}

impl StreamManager {
    pub fn new(
        rtsp_url: String,
        audio_rtsp_url: String,
        audio: bool,
        local_ip: IpAddr,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            rtsp_url,
            audio_rtsp_url,
            audio,
            local_ip,
            metrics,
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
        let bind_ip = unspecified_for(self.local_ip);
        let (video_out_socket, video_rtcp_reservation) = match bind_tokio_udp_pair(bind_ip).await {
            Ok(pair) => pair,
            Err(e) => {
                self.metrics.error(ErrorSubsystem::LiveStream);
                warn!("could not reserve video RTP/RTCP port: {e}");
                return;
            }
        };
        let video_local_port = video_out_socket.local_addr().map(|a| a.port()).unwrap_or(0);
        // HomeKit multiplexes video RTP and RTCP on the one advertised port.
        // Reserving the adjacent port while choosing the even port avoids
        // collisions with software that assumes an RTP/RTCP pair.
        drop(video_rtcp_reservation);
        let audio_out_socket = match tokio::net::UdpSocket::bind((bind_ip, 0)).await {
            Ok(socket) => socket,
            Err(e) => {
                self.metrics.error(ErrorSubsystem::LiveStream);
                warn!("could not reserve audio RTP/RTCP port: {e}");
                return;
            }
        };
        let audio_local_port = audio_out_socket.local_addr().map(|a| a.port()).unwrap_or(0);
        let session = Session {
            id: session_id.clone(),
            controller_ip: controller_ip.clone(),
            video_port,
            audio_port,
            video_key: srtp_key(&video_srtp),
            audio_key: srtp_key(&audio_srtp),
            video_ssrc,
            audio_ssrc,
            video_out_socket: Some(video_out_socket),
            audio_out_socket: Some(audio_out_socket),
        };

        debug!(
            "stream setup: controller {}:{} (video) :{} (audio); accessory :{} (video RTP/RTCP) :{} (audio RTP/RTCP)",
            controller_ip, video_port, audio_port, video_local_port, audio_local_port,
        );

        // Build the response: echo the session id and SRTP parameters, present
        // our own address and freshly chosen SSRCs.
        let ip_string = self.local_ip.to_string();
        let ip_version: u8 = if self.local_ip.is_ipv6() { 1 } else { 0 };

        let accessory_address = tlv8::Writer::new()
            .u8(ADDR_IP_VERSION, ip_version)
            .bytes(ADDR_IP, ip_string.as_bytes())
            .u16(ADDR_VIDEO_PORT, video_local_port)
            .u16(ADDR_AUDIO_PORT, audio_local_port)
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
                let audio_max_bitrate = tlv8::find_u16(&audio_rtp, RTP_MAX_BITRATE).unwrap_or(32);
                let audio_rtcp_interval = tlv8::find(&audio_rtp, RTP_MIN_RTCP_INTERVAL)
                    .filter(|v| v.len() >= 4)
                    .map(|v| f32::from_le_bytes([v[0], v[1], v[2], v[3]]))
                    .unwrap_or(0.5);
                let audio_codec_params = tlv8::find(&audio_params, AUDIO_CODEC_PARAMS)
                    .map(|v| tlv8::parse(v))
                    .unwrap_or_default();
                let audio_packet_time =
                    tlv8::find_u8(&audio_codec_params, AUDIO_PARAM_PACKET_TIME).unwrap_or(20);
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
                    audio_packet_time,
                    audio_max_bitrate,
                    audio_rtcp_interval,
                )
                .await;
            }
            Some(COMMAND_END) => {
                debug!("stream end requested");
                self.end_session(session_id).await;
            }
            Some(COMMAND_SUSPEND) => {
                debug!("stream suspend requested");
                self.end_session(session_id).await;
            }
            Some(COMMAND_RESUME) | Some(COMMAND_RECONFIGURE) => {
                debug!("stream resume/reconfigure requested (ignored)");
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
        audio_packet_time: u8,
        audio_max_bitrate: u16,
        audio_rtcp_interval: f32,
    ) {
        let mut inner = self.inner.lock().await;

        let session = match &session_id {
            Some(sid) => inner.sessions.remove(sid),
            // No session id in the request: only unambiguous with one session.
            None if inner.sessions.len() == 1 => {
                let sid = inner.sessions.keys().next().cloned();
                sid.and_then(|sid| inner.sessions.remove(&sid))
            }
            None => None,
        };
        let Some(mut session) = session else {
            warn!("start requested but no matching session prepared");
            return;
        };

        // Tear down any previous stream for this session first.
        if let Some(mut child) = inner.children.remove(&session.id) {
            let _ = child.start_kill();
        }
        if let Some(mut child) = inner.audio_children.remove(&session.id) {
            let _ = child.start_kill();
        }
        if let Some(proxy) = inner.audio_proxies.remove(&session.id) {
            proxy.abort();
        }
        if let Some(proxy) = inner.video_proxies.remove(&session.id) {
            proxy.abort();
        }
        self.metrics.set_live_connections(inner.children.len());

        debug!(
            "selected live audio: Opus mono {}Hz, {}ms packets, payload {}, max {}kbps, RTCP {:.3}s",
            audio_rate_hz,
            audio_packet_time,
            audio_payload_type,
            audio_max_bitrate,
            audio_rtcp_interval,
        );

        let pkt_size = max_mtu.clamp(188, 1378);
        let (video_local_rtp, video_local_rtcp) =
            match bind_tokio_udp_pair(IpAddr::V4(Ipv4Addr::LOCALHOST)).await {
                Ok(pair) => pair,
                Err(e) => {
                    self.metrics.error(ErrorSubsystem::LiveStream);
                    warn!("could not bind video proxy socket: {e}");
                    return;
                }
            };
        let video_local_rtp_port = video_local_rtp.local_addr().map(|a| a.port()).unwrap_or(0);
        let video_local_rtcp_port = video_local_rtcp.local_addr().map(|a| a.port()).unwrap_or(0);
        let video_dest = format!(
            "rtp://127.0.0.1:{video_local_rtp_port}?rtcpport={video_local_rtcp_port}&pkt_size={pkt_size}"
        );
        let Some(video_proxy) = spawn_media_proxy(
            "video",
            video_local_rtp,
            video_local_rtcp,
            session.video_out_socket.take().unwrap(),
            session.controller_ip.clone(),
            session.video_port,
            session.video_key.clone(),
            1,
        ) else {
            self.metrics.error(ErrorSubsystem::LiveStream);
            return;
        };

        let mut video_cmd = Command::new("ffmpeg");
        video_cmd
            .arg("-hide_banner")
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
            .args(["-map", "0:v:0"])
            .arg("-an")
            .args(["-c:v", "copy"])
            .args(["-payload_type", &payload_type.to_string()])
            .args(["-ssrc", &session.video_ssrc.to_string()])
            .args(["-f", "rtp"])
            .arg(&video_dest)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        video_cmd.kill_on_drop(true);

        let audio_enabled =
            self.audio && !session.audio_key.is_empty() && session.audio_out_socket.is_some();

        debug!("starting video ffmpeg → {video_dest}");
        match video_cmd.spawn() {
            Ok(mut child) => {
                if let Some(stderr) = child.stderr.take() {
                    tokio::spawn(async move {
                        let mut lines = BufReader::new(stderr).lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            warn!("video ffmpeg: {line}");
                        }
                    });
                }
                inner.children.insert(session.id.clone(), child);
                inner.video_proxies.insert(session.id.clone(), video_proxy);
                self.metrics.live_stream_started();
                self.metrics.set_live_connections(inner.children.len());
            }
            Err(e) => {
                video_proxy.abort();
                self.metrics.error(ErrorSubsystem::LiveStream);
                error!("failed to spawn video ffmpeg: {e}");
                return;
            }
        }

        // Audio has its own RTSP reader/encoder process and local RTP/RTCP
        // proxy. Nothing in this path can block or pace the video process.
        if audio_enabled {
            match bind_tokio_udp_pair(IpAddr::V4(Ipv4Addr::LOCALHOST)).await {
                Ok((local_rtp, local_rtcp)) => {
                    let local_rtp_port = local_rtp.local_addr().map(|a| a.port()).unwrap_or(0);
                    let local_rtcp_port = local_rtcp.local_addr().map(|a| a.port()).unwrap_or(0);
                    let audio_dest =
                        format!("rtp://127.0.0.1:{local_rtp_port}?rtcpport={local_rtcp_port}");
                    let packet_time = match audio_packet_time {
                        20 | 40 | 60 => audio_packet_time,
                        _ => 20,
                    };
                    let bitrate = format!("{}k", audio_max_bitrate.clamp(16, 64));
                    let mut audio_cmd = Command::new("ffmpeg");
                    audio_cmd
                        .arg("-hide_banner")
                        .args(["-loglevel", "warning"])
                        .args(["-fflags", "+genpts+nobuffer"])
                        .args(["-flags", "low_delay"])
                        .args(["-use_wallclock_as_timestamps", "1"])
                        .args(["-rtsp_transport", "tcp"])
                        .args(["-allowed_media_types", "audio"])
                        .args(["-i", &self.audio_rtsp_url])
                        .args(["-map", "0:a:0"])
                        .arg("-vn")
                        .args(["-c:a", "libopus"])
                        .args(["-application:a", "voip"])
                        .args(["-vbr:a", "on"])
                        .args(["-packet_loss:a", "5"])
                        .args(["-fec:a", "1"])
                        .args(["-frame_duration:a", &packet_time.to_string()])
                        .args([
                            "-af",
                            "aresample=async=1000:min_hard_comp=0.100:first_pts=0",
                        ])
                        .args(["-ar", &audio_rate_hz.to_string()])
                        .args(["-ac", "1"])
                        .args(["-b:a", &bitrate])
                        .args(["-payload_type", &audio_payload_type.to_string()])
                        .args(["-ssrc", &session.audio_ssrc.to_string()])
                        .args(["-f", "rtp"])
                        .arg(&audio_dest)
                        .stdin(Stdio::null())
                        .stdout(Stdio::null())
                        .stderr(Stdio::piped());
                    audio_cmd.kill_on_drop(true);

                    debug!("starting audio ffmpeg → {audio_dest}");
                    match audio_cmd.spawn() {
                        Ok(mut child) => {
                            if let Some(stderr) = child.stderr.take() {
                                tokio::spawn(async move {
                                    let mut lines = BufReader::new(stderr).lines();
                                    while let Ok(Some(line)) = lines.next_line().await {
                                        warn!("audio ffmpeg: {line}");
                                    }
                                });
                            }
                            let proxy = spawn_media_proxy(
                                "audio",
                                local_rtp,
                                local_rtcp,
                                session.audio_out_socket.take().unwrap(),
                                session.controller_ip.clone(),
                                session.audio_port,
                                session.audio_key.clone(),
                                48000 / audio_rate_hz.max(1),
                            );
                            inner.audio_children.insert(session.id.clone(), child);
                            if let Some(proxy) = proxy {
                                inner.audio_proxies.insert(session.id.clone(), proxy);
                            }
                        }
                        Err(e) => {
                            self.metrics.error(ErrorSubsystem::LiveStream);
                            warn!("failed to spawn audio ffmpeg, video remains active: {e}");
                        }
                    }
                }
                Err(e) => {
                    self.metrics.error(ErrorSubsystem::LiveStream);
                    warn!("could not bind audio proxy socket, audio disabled: {e}");
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
                    debug!("stream stopped");
                }
                if let Some(mut child) = inner.audio_children.remove(&sid) {
                    let _ = child.start_kill();
                }
                if let Some(proxy) = inner.audio_proxies.remove(&sid) {
                    proxy.abort();
                }
                if let Some(proxy) = inner.video_proxies.remove(&sid) {
                    proxy.abort();
                }
                inner.sessions.remove(&sid);
            }
            None => {
                for (_, mut child) in inner.children.drain() {
                    let _ = child.start_kill();
                }
                for (_, mut child) in inner.audio_children.drain() {
                    let _ = child.start_kill();
                }
                for (_, proxy) in inner.audio_proxies.drain() {
                    proxy.abort();
                }
                for (_, proxy) in inner.video_proxies.drain() {
                    proxy.abort();
                }
                inner.sessions.clear();
            }
        }
        self.metrics.set_live_connections(inner.children.len());
    }

    /// Stops all streams; used on shutdown.
    pub async fn stop_stream(&self) {
        self.end_session(None).await;
        let mut inner = self.inner.lock().await;
        inner.setup_response.clear();
    }
}

fn unspecified_for(ip: IpAddr) -> IpAddr {
    if ip.is_ipv6() {
        IpAddr::V6(Ipv6Addr::UNSPECIFIED)
    } else {
        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    }
}

async fn bind_tokio_udp_pair(
    ip: IpAddr,
) -> std::io::Result<(tokio::net::UdpSocket, tokio::net::UdpSocket)> {
    for _ in 0..64 {
        let rtp = tokio::net::UdpSocket::bind(SocketAddr::new(ip, 0)).await?;
        let port = rtp.local_addr()?.port();
        if port % 2 != 0 || port == u16::MAX {
            continue;
        }
        if let Ok(rtcp) = tokio::net::UdpSocket::bind(SocketAddr::new(ip, port + 1)).await {
            return Ok((rtp, rtcp));
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AddrNotAvailable,
        "could not bind an even RTP/odd RTCP port pair",
    ))
}

/// Proxies FFmpeg's plain RTP and RTCP to the controller as SRTP/SRTCP on one
/// multiplexed port, and returns controller SRTCP feedback to FFmpeg.
fn spawn_media_proxy(
    label: &'static str,
    local_rtp: tokio::net::UdpSocket,
    local_rtcp: tokio::net::UdpSocket,
    out_socket: tokio::net::UdpSocket,
    controller_ip: String,
    controller_port: u16,
    key: Vec<u8>,
    ratio: u32,
) -> Option<tokio::task::JoinHandle<()>> {
    let mut srtp = match crate::srtp::SrtpSession::new(&key) {
        Some(srtp) => srtp,
        None => {
            warn!("invalid {label} SRTP key material, stream disabled");
            return None;
        }
    };
    let mut rescaler = crate::srtp::TimestampRescaler::new(ratio);

    Some(tokio::spawn(async move {
        let dest = format!("{controller_ip}:{controller_port}");
        if let Err(e) = out_socket.connect(&dest).await {
            warn!("{label} proxy could not connect to {dest}: {e}");
            return;
        }
        let mut rtp_buf = [0u8; 2048];
        let mut rtcp_buf = [0u8; 2048];
        let mut feedback_buf = [0u8; 2048];
        let mut ffmpeg_rtcp_peer = None;
        let mut sent_rtp = 0u64;
        let mut sent_rtcp = 0u64;
        let mut received_feedback = 0u64;
        let mut logged_keyframe = false;
        loop {
            tokio::select! {
                result = local_rtp.recv(&mut rtp_buf) => {
                    let Ok(n) = result else { break };
                    let packet = &mut rtp_buf[..n];
                    if n >= 12 && packet[0] >> 6 == 2 {
                        rescaler.rescale_rtp(packet);
                        if let Some(protected) = srtp.protect_rtp(packet) {
                            if out_socket.send(&protected).await.is_err() {
                                break;
                            }
                            sent_rtp += 1;
                            if sent_rtp == 1 {
                                debug!("{label} proxy: first RTP packet → {dest}");
                            }
                            if label == "video" && !logged_keyframe && h264_contains_idr(packet) {
                                debug!("video proxy: first H.264 keyframe packet → {dest}");
                                logged_keyframe = true;
                            }
                        }
                    }
                }
                result = local_rtcp.recv_from(&mut rtcp_buf) => {
                    let Ok((n, peer)) = result else { break };
                    ffmpeg_rtcp_peer = Some(peer);
                    let packet = &mut rtcp_buf[..n];
                    rescaler.rescale_rtcp(packet);
                    if let Some(protected) = srtp.protect_rtcp(packet) {
                        if out_socket.send(&protected).await.is_err() {
                            break;
                        }
                        sent_rtcp += 1;
                        if sent_rtcp == 1 {
                            debug!("{label} proxy: first SRTCP sender report → {dest}");
                        }
                    }
                }
                result = out_socket.recv(&mut feedback_buf) => {
                    let Ok(n) = result else { break };
                    if let Some(packet) = srtp.unprotect_rtcp(&feedback_buf[..n]) {
                        if let Some(peer) = ffmpeg_rtcp_peer {
                            let _ = local_rtcp.send_to(&packet, peer).await;
                        }
                        received_feedback += 1;
                        if received_feedback == 1 {
                            debug!("{label} proxy: first controller SRTCP feedback received");
                        }
                    }
                }
            }
        }
    }))
}

fn h264_contains_idr(packet: &[u8]) -> bool {
    if packet.len() < 13 || packet[0] >> 6 != 2 {
        return false;
    }
    let mut offset = 12 + usize::from(packet[0] & 0x0f) * 4;
    if packet[0] & 0x10 != 0 {
        if offset + 4 > packet.len() {
            return false;
        }
        let extension_words = u16::from_be_bytes([packet[offset + 2], packet[offset + 3]]);
        offset += 4 + usize::from(extension_words) * 4;
    }
    let Some(payload) = packet.get(offset..) else {
        return false;
    };
    let Some(&nal_header) = payload.first() else {
        return false;
    };
    match nal_header & 0x1f {
        5 => true,
        // FU-A: the second byte carries Start and the original NAL type.
        28 if payload.len() >= 2 => payload[1] & 0x80 != 0 && payload[1] & 0x1f == 5,
        // STAP-A: walk the length-prefixed aggregated NAL units.
        24 => {
            let mut index = 1;
            while index + 2 <= payload.len() {
                let length = usize::from(u16::from_be_bytes([payload[index], payload[index + 1]]));
                index += 2;
                if length == 0 || index + length > payload.len() {
                    return false;
                }
                if payload[index] & 0x1f == 5 {
                    return true;
                }
                index += length;
            }
            false
        }
        _ => false,
    }
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

#[cfg(test)]
mod tests {
    use super::h264_contains_idr;

    #[test]
    fn detects_single_and_fragmented_h264_idr_packets() {
        let mut single = vec![0x80, 99, 0, 1, 0, 0, 0, 1, 0, 0, 0, 42];
        single.extend_from_slice(&[0x65, 1, 2, 3]);
        assert!(h264_contains_idr(&single));

        let mut fua = vec![0x80, 99, 0, 2, 0, 0, 0, 2, 0, 0, 0, 42];
        fua.extend_from_slice(&[0x7c, 0x85, 1, 2, 3]);
        assert!(h264_contains_idr(&fua));
        fua[13] = 0x05;
        assert!(!h264_contains_idr(&fua));
    }
}
