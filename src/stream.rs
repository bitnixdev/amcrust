//! HomeKit camera RTP stream management: SetupEndpoints / SelectedRTPStreamConfiguration
//! TLV8 negotiation, shared Rust video and the remaining native audio adapter.

use log::{debug, warn};
use rand::Rng;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::amcrest::VideoProfile;
use crate::metrics::{ErrorSubsystem, Metrics};
use crate::tlv8;
use amcrust::media::rtcp;
use amcrust::media::transport::{Error as TransportError, RTP_OVERHEAD, Transport};

mod shared;

struct MediaProxy {
    transport: shared::Control,
    metrics: Arc<Metrics>,
    task: Option<tokio::task::JoinHandle<()>>,
    #[cfg(test)]
    producer_id: Option<u32>,
}
impl MediaProxy {
    /// Temporary native-producer adapter. Transport/session lifetime survives its
    /// replacement by Rust producers; no separate, unjoined child ownership.
    fn with_producer(mut self, mut child: Child) -> Self {
        #[cfg(test)]
        {
            self.producer_id = child.id();
        }
        let mut media = self.task.take().expect("media task");
        let transport = self.transport.clone();
        let stderr = child.stderr.take();
        let metrics = self.metrics.clone();
        self.task = Some(tokio::spawn(async move {
            let diagnostics = tokio::spawn(async move {
                if let Some(mut stderr) = stderr {
                    // Bounded drain: do not retain/log native URLs or credentials.
                    let mut buffer = [0; 1024];
                    while let Ok(n) = stderr.read(&mut buffer).await {
                        if n == 0 {
                            break;
                        }
                    }
                }
            });
            let media_finished = tokio::select! {
                _ = &mut media => true,
                status = child.wait() => {
                    if !transport.is_closed() {
                        metrics.error(ErrorSubsystem::LiveStream);
                        warn!("native media producer ended unexpectedly: {status:?}");
                    }
                    false
                },
            };
            transport.revoke();
            if !media_finished {
                media.abort();
                let _ = media.await;
            }
            if child.id().is_some() {
                let _ = child.start_kill();
            }
            let _ = child.wait().await;
            diagnostics.abort();
            let _ = diagnostics.await;
        }));
        self
    }
    async fn stop(mut self) {
        self.transport.revoke();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for MediaProxy {
    fn drop(&mut self) {
        // Wake media and its supervisor. Aborting the supervisor would lose the
        // explicit wait/reap; Drop signals, stop() additionally joins.
        self.transport.revoke();
    }
}

struct LiveConnection(Arc<Metrics>);
impl Drop for LiveConnection {
    fn drop(&mut self) {
        self.0.live_connection_closed();
    }
}

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
    rtcp_identity: [u8; 16],
    video_out_socket: Option<tokio::net::UdpSocket>,
    audio_out_socket: Option<tokio::net::UdpSocket>,
}

#[derive(Default)]
struct Inner {
    /// Prepared sessions and running streams, keyed by HAP session id.
    sessions: std::collections::HashMap<Vec<u8>, Session>,
    video_proxies: std::collections::HashMap<Vec<u8>, MediaProxy>,
    /// Audio RTP proxy tasks, keyed by HAP session id.
    audio_proxies: std::collections::HashMap<Vec<u8>, MediaProxy>,
    /// Response for the most recent SetupEndpoints write, read back by the
    /// controller immediately after writing.
    setup_response: Vec<u8>,
}

impl Inner {
    fn revoke_transports(&self, session: Option<&[u8]>) {
        for (id, proxy) in self.video_proxies.iter().chain(&self.audio_proxies) {
            if session.is_none_or(|session| session == id.as_slice()) {
                proxy.transport.revoke();
            }
        }
    }
}

#[derive(Clone)]
pub struct VideoSource {
    pub source: amcrust::source::Source,
    pub profile: VideoProfile,
}

/// Manages the active HomeKit streams for this camera.
#[derive(Clone)]
pub struct StreamManager {
    inner: Arc<Mutex<Inner>>,
    video_sources: Vec<VideoSource>,
    audio_rtsp_url: String,
    audio: bool,
    local_ip: IpAddr,
    metrics: Arc<Metrics>,
}

impl StreamManager {
    pub fn new(
        video_sources: Vec<VideoSource>,
        audio_rtsp_url: String,
        audio: bool,
        local_ip: IpAddr,
        metrics: Arc<Metrics>,
    ) -> Self {
        assert!(
            !video_sources.is_empty(),
            "at least one live source is required"
        );
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            video_sources,
            audio_rtsp_url,
            audio,
            local_ip,
            metrics,
        }
    }

    pub fn audio_enabled(&self) -> bool {
        self.audio
    }

    pub fn supported_video_config(&self) -> Vec<u8> {
        supported_video_config(
            &self
                .video_sources
                .iter()
                .map(|source| source.profile)
                .collect::<Vec<_>>(),
        )
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
            rtcp_identity: rand::random(),
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
                let video_attributes = tlv8::find(&video_params, 0x03)
                    .map(|v| tlv8::parse(v))
                    .unwrap_or_default();
                let selected_width = tlv8::find_u16(&video_attributes, 0x01);
                let selected_height = tlv8::find_u16(&video_attributes, 0x02);
                let selected_fps = tlv8::find_u8(&video_attributes, 0x03);
                let source =
                    select_video_source(&self.video_sources, selected_width, selected_height)
                        .clone();
                let rtp_params = tlv8::find(&video_params, VIDEO_RTP_PARAMS)
                    .map(|v| tlv8::parse(v))
                    .unwrap_or_default();
                let payload_type = tlv8::find_u8(&rtp_params, RTP_PAYLOAD_TYPE).unwrap_or(99);
                let max_mtu = tlv8::find_u16(&rtp_params, RTP_MAX_MTU).unwrap_or(1316);
                let video_rtcp_interval = tlv8::find(&rtp_params, RTP_MIN_RTCP_INTERVAL)
                    .filter(|v| v.len() == 4)
                    .map(|v| f32::from_le_bytes([v[0], v[1], v[2], v[3]]))
                    .unwrap_or(0.5);
                let max_bitrate = tlv8::find_u16(&rtp_params, RTP_MAX_BITRATE).unwrap_or_default();
                debug!(
                    "selected live video {:?}x{:?}@{:?}: native source {}x{}@{} {}kbps, controller max {}kbps",
                    selected_width,
                    selected_height,
                    selected_fps,
                    source.profile.width,
                    source.profile.height,
                    source.profile.fps,
                    source.profile.bitrate_kbps,
                    max_bitrate,
                );
                // Audio: negotiated Opus payload type & sample rate.
                let audio_params = tlv8::find(&items, SELECTED_AUDIO_PARAMS)
                    .map(|v| tlv8::parse(v))
                    .unwrap_or_default();
                let audio_rtp = tlv8::find(&audio_params, AUDIO_RTP_PARAMS)
                    .map(|v| tlv8::parse(v))
                    .unwrap_or_default();
                let audio_payload_type = tlv8::find_u8(&audio_rtp, RTP_PAYLOAD_TYPE).unwrap_or(110);
                let audio_max_bitrate = tlv8::find_u16(&audio_rtp, RTP_MAX_BITRATE).unwrap_or(32);
                let audio_max_mtu = tlv8::find_u16(&audio_rtp, RTP_MAX_MTU).unwrap_or(1378);
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
                    source,
                    payload_type,
                    max_mtu,
                    audio_payload_type,
                    audio_rate_hz,
                    audio_packet_time,
                    audio_max_bitrate,
                    audio_rtcp_interval,
                    audio_max_mtu,
                    video_rtcp_interval,
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
        video_source: VideoSource,
        payload_type: u8,
        max_mtu: u16,
        audio_payload_type: u8,
        audio_rate_hz: u32,
        audio_packet_time: u8,
        audio_max_bitrate: u16,
        audio_rtcp_interval: f32,
        audio_max_mtu: u16,
        video_rtcp_interval: f32,
    ) {
        if !video_rtcp_interval.is_finite() || !(0.0..=60.0).contains(&video_rtcp_interval) {
            self.metrics.error(ErrorSubsystem::LiveStream);
            return;
        }
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

        // Fence both tracks before the first awaited cleanup.
        inner.revoke_transports(Some(&session.id));
        // Tear down any previous stream for this session first.
        if let Some(proxy) = inner.audio_proxies.remove(&session.id) {
            proxy.stop().await;
        }
        if let Some(proxy) = inner.video_proxies.remove(&session.id) {
            proxy.stop().await;
        }
        debug!(
            "selected live audio: Opus mono {}Hz, {}ms packets, payload {}, max {}kbps, RTCP {:.3}s",
            audio_rate_hz,
            audio_packet_time,
            audio_payload_type,
            audio_max_bitrate,
            audio_rtcp_interval,
        );

        let wire_mtu = usize::from(max_mtu.min(1378));
        if wire_mtu < 188 {
            self.metrics.error(ErrorSubsystem::LiveStream);
            warn!("unsupported video MTU");
            return;
        }
        let Ok(controller_ip) = session.controller_ip.parse() else {
            self.metrics.error(ErrorSubsystem::LiveStream);
            return;
        };
        let video_proxy = shared::spawn(
            shared::Video {
                source: video_source.source,
                profile: video_source.profile,
                socket: session.video_out_socket.take().unwrap(),
                peer: SocketAddr::new(controller_ip, session.video_port),
                key: session.video_key.clone(),
                ssrc: session.video_ssrc,
                identity: session.rtcp_identity,
                mtu: wire_mtu,
                payload: payload_type,
                interval: std::time::Duration::from_secs_f32(video_rtcp_interval.max(0.5)),
            },
            self.metrics.clone(),
        );

        let audio_wire_mtu = usize::from(audio_max_mtu.min(1378));
        if self.audio && audio_wire_mtu < 188 {
            self.metrics.error(ErrorSubsystem::LiveStream);
            warn!("unsupported audio MTU, audio disabled");
        }
        let audio_enabled = self.audio
            && !session.audio_key.is_empty()
            && session.audio_out_socket.is_some()
            && audio_wire_mtu >= 188;

        inner.video_proxies.insert(session.id.clone(), video_proxy);
        self.metrics.live_stream_started();

        // Audio has its own RTSP reader/encoder process and local RTP/RTCP
        // proxy. Nothing in this path can block or pace the video process.
        if audio_enabled {
            match bind_tokio_udp_pair(IpAddr::V4(Ipv4Addr::LOCALHOST)).await {
                Ok((local_rtp, local_rtcp)) => {
                    let local_rtp_port = local_rtp.local_addr().map(|a| a.port()).unwrap_or(0);
                    let local_rtcp_port = local_rtcp.local_addr().map(|a| a.port()).unwrap_or(0);
                    let audio_pkt_size = audio_wire_mtu - RTP_OVERHEAD;
                    let audio_dest = format!(
                        "rtp://127.0.0.1:{local_rtp_port}?rtcpport={local_rtcp_port}&pkt_size={audio_pkt_size}"
                    );
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
                            let proxy = spawn_media_proxy(
                                "audio",
                                local_rtp,
                                local_rtcp,
                                session.audio_out_socket.take().unwrap(),
                                session.controller_ip.clone(),
                                session.audio_port,
                                session.audio_key.clone(),
                                session.audio_ssrc,
                                session.rtcp_identity,
                                audio_wire_mtu,
                                48000 / audio_rate_hz.max(1),
                                self.metrics.clone(),
                            );
                            if let Some(proxy) = proxy {
                                inner
                                    .audio_proxies
                                    .insert(session.id.clone(), proxy.with_producer(child));
                            } else {
                                let _ = child.start_kill();
                                let _ = child.wait().await;
                                self.metrics.error(ErrorSubsystem::LiveStream);
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
        inner.revoke_transports(session_id.as_deref());
        match session_id {
            Some(sid) => {
                if let Some(proxy) = inner.audio_proxies.remove(&sid) {
                    proxy.stop().await;
                }
                if let Some(proxy) = inner.video_proxies.remove(&sid) {
                    proxy.stop().await;
                }
                inner.sessions.remove(&sid);
            }
            None => {
                for (_, proxy) in inner.audio_proxies.drain() {
                    proxy.stop().await;
                }
                for (_, proxy) in inner.video_proxies.drain() {
                    proxy.stop().await;
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
/// multiplexed port. Authenticated controller feedback is consumed only by Rust.
fn spawn_media_proxy(
    label: &'static str,
    local_rtp: tokio::net::UdpSocket,
    local_rtcp: tokio::net::UdpSocket,
    out_socket: tokio::net::UdpSocket,
    controller_ip: String,
    controller_port: u16,
    key: Vec<u8>,
    ssrc: u32,
    rtcp_identity: [u8; 16],
    wire_mtu: usize,
    ratio: u32,
    metrics: Arc<Metrics>,
) -> Option<MediaProxy> {
    let started = std::time::Instant::now();
    let dest = SocketAddr::new(controller_ip.parse().ok()?, controller_port);
    let transport = Arc::new(Transport::new(out_socket, dest, &key, ssrc, wire_mtu)?);
    // Legacy FFmpeg adapter only. Rust packetizers must pass their clock unchanged.
    let mut rescaler = amcrust::srtp::TimestampRescaler::new(ratio);
    let sender = transport.clone();
    let reports = rtcp::Sender::new(ssrc, rtcp_identity);
    let connection = if label == "video" {
        metrics.live_connection_opened();
        Some(LiveConnection(metrics.clone()))
    } else {
        None
    };
    let owner_metrics = metrics.clone();
    let task = tokio::spawn(async move {
        let _connection = connection;
        let mut receiver = rtcp::Receiver::new(ssrc);
        let mut rtp_buf = [0u8; 2048];
        let mut rtcp_buf = [0u8; 2048];
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
                        if let Err(error) = sender.send_rtp(packet, || true).await {
                            if !matches!(error, TransportError::Closed) {
                                metrics.error(ErrorSubsystem::LiveStream);
                                warn!("{label} RTP transport stopped: {error:?}");
                            }
                            break;
                        }
                        sent_rtp += 1;
                        if sent_rtp == 1 {
                            if label == "video" {
                                metrics.live_first_packet(false, started.elapsed());
                            }
                            debug!("{label} proxy: first RTP packet → {dest}");
                        }
                        if label == "video" && !logged_keyframe && h264_contains_idr(packet) {
                            debug!("video proxy: first H.264 keyframe packet → {dest}");
                            logged_keyframe = true;
                            metrics.live_first_packet(true, started.elapsed());
                        }
                    }
                }
                result = local_rtcp.recv(&mut rtcp_buf) => {
                    let Ok(n) = result else { break };
                    let packet = &mut rtcp_buf[..n];
                    rescaler.rescale_rtcp(packet);
                    let control = match rtcp::local_control(packet, ssrc) {
                        Ok(control) => control,
                        Err(error) => {
                            metrics.error(ErrorSubsystem::LiveStream);
                            warn!("{label} producer RTCP refused: {error}");
                            break;
                        }
                    };
                    if let Some(clock) = control.clock {
                        // Preserve only the explicit legacy clock pair. Counters
                        // describe accepted encrypted writes, and CNAME is ours.
                        let mut compound = [0u8; 80];
                        compound[..72].copy_from_slice(&reports.report(clock, sender.rtp_counts()));
                        let size = if control.bye {
                            compound[72..].copy_from_slice(&reports.bye());
                            80
                        } else { 72 };
                        if let Err(error) = sender.send_rtcp(&compound[..size], || true).await {
                            if !matches!(error, TransportError::Closed) {
                                metrics.error(ErrorSubsystem::LiveStream);
                                warn!("{label} RTCP transport stopped: {error:?}");
                            }
                            break;
                        }
                        sent_rtcp += 1;
                        if sent_rtcp == 1 {
                            debug!("{label} proxy: first Rust SRTCP sender report → {dest}");
                        }
                    }
                    // A bare BYE terminates locally; do not invent a clock to
                    // build a new compound report or emit unnegotiated reduced RTCP.
                    if control.bye { break; }
                }
                result = sender.feedback() => {
                    match result {
                        Err(TransportError::Authentication | TransportError::TooLarge) => continue,
                        Err(TransportError::Closed) => break,
                        Err(error) => {
                            metrics.error(ErrorSubsystem::LiveStream);
                            warn!("{label} feedback transport stopped: {error:?}");
                            break;
                        }
                        Ok(packet) => {
                            let feedback = match receiver.receive(&packet) {
                                Ok(feedback) => feedback,
                                Err(_) => {
                                    metrics.feedback_refused();
                                    continue;
                                }
                            };
                            metrics.receiver_feedback(feedback);
                            received_feedback += 1;
                            if received_feedback == 1 {
                                debug!("{label} proxy: first validated controller feedback received");
                            }
                            // PLI/FIR/NACK remain observable unsupported requests:
                            // no camera write, retransmission or clock reset is implied.
                            if feedback.bye { break; }
                        }
                    }
                }
            }
        }
        sender.revoke();
        debug!("{label} receiver feedback summary: {:?}", receiver.stats());
    });
    Some(MediaProxy {
        transport: shared::Control::Ready(transport),
        metrics: owner_metrics,
        task: Some(task),
        #[cfg(test)]
        producer_id: None,
    })
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

fn select_video_source(
    sources: &[VideoSource],
    width: Option<u16>,
    height: Option<u16>,
) -> &VideoSource {
    sources
        .iter()
        .find(|source| Some(source.profile.width) == width && Some(source.profile.height) == height)
        .unwrap_or(&sources[0])
}

// --- Supported configuration TLVs, advertised via the stream management service ---

/// SupportedVideoStreamConfiguration for the exact packet-copy source.
pub fn supported_video_config(profiles: &[VideoProfile]) -> Vec<u8> {
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

    let mut unique_profiles = Vec::new();
    for profile in profiles {
        if !unique_profiles.iter().any(|current: &VideoProfile| {
            current.width == profile.width
                && current.height == profile.height
                && current.fps == profile.fps
        }) {
            unique_profiles.push(*profile);
        }
    }
    let attributes = unique_profiles
        .into_iter()
        .map(|profile| {
            tlv8::Writer::new()
                .u16(0x01, profile.width)
                .u16(0x02, profile.height)
                .u8(0x03, profile.fps)
                .build()
        })
        .collect::<Vec<_>>();

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
    use super::*;
    fn registry() -> amcrust::source::Registry {
        amcrust::source::Registry::new(amcrust::source::Limits {
            timing: amcrust::source::Timing::Presentation,
            ..Default::default()
        })
        .unwrap()
    }
    fn register(
        registry: &mut amcrust::source::Registry,
        camera: u64,
        url: String,
    ) -> amcrust::source::Source {
        registry
            .register(
                amcrust::source::SourceKey {
                    camera,
                    encoder: amcrust::source::Encoder::Main,
                },
                amcrust::ingest::CameraUrl::new(url).unwrap(),
            )
            .unwrap()
    }

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

    #[test]
    #[ignore = "internal subprocess fixture; invoked only by lifecycle tests"]
    fn media_child_fixture() {
        if std::env::var("AMCRUST_MEDIA_CHILD_FIXTURE").as_deref() == Ok("wait") {
            std::thread::sleep(std::time::Duration::from_secs(60));
        }
    }
    fn fixture_child(mode: &str) -> Child {
        Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "stream::tests::media_child_fixture"])
            .env("AMCRUST_MEDIA_CHILD_FIXTURE", mode)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    }
    fn assert_reaped(pid: u32) {
        assert!(is_reaped(pid), "producer was not reaped by its supervisor");
    }
    fn is_reaped(pid: u32) -> bool {
        // Public POSIX API on the supported Darwin/Linux targets. WNOHANG=1;
        // ECHILD=10 means our supervisor, not this assertion, already reaped it.
        unsafe extern "C" {
            fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
        }
        let mut status = 0;
        let result = unsafe { waitpid(pid.try_into().unwrap(), &mut status, 1) };
        let error = std::io::Error::last_os_error().raw_os_error();
        result == -1 && error == Some(10)
    }

    #[tokio::test]
    async fn producer_exit_revokes_transport_and_releases_media_tasks() {
        let (rtp, rtcp) = bind_tokio_udp_pair(Ipv4Addr::LOCALHOST.into())
            .await
            .unwrap();
        let out = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let controller = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let metrics = Metrics::new();
        let mut proxy = spawn_media_proxy(
            "video",
            rtp,
            rtcp,
            out,
            "127.0.0.1".into(),
            controller.local_addr().unwrap().port(),
            vec![7; 30],
            42,
            [9; 16],
            188,
            1,
            metrics.clone(),
        )
        .unwrap()
        .with_producer(fixture_child("exit"));
        let pid = proxy.producer_id.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            proxy.task.take().unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_reaped(pid);
        assert!(
            metrics
                .render(false)
                .contains("amcrust_connections_open{type=\"live_stream\"} 0")
        );
        assert!(matches!(
            proxy.transport.send_rtp(&[0; 12], || true).await,
            Err(amcrust::media::transport::Error::Closed)
        ));
        proxy.stop().await;
    }

    #[tokio::test]
    async fn legacy_adapter_uses_secure_transport_and_joins_stop() {
        use std::time::Duration;
        use tokio::net::UdpSocket;
        use webrtc_srtp::{context::Context, protection_profile::ProtectionProfile};
        async fn receive(socket: &UdpSocket) -> Vec<u8> {
            let mut bytes = [0; 2048];
            let n = tokio::time::timeout(Duration::from_secs(1), socket.recv(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            bytes[..n].to_vec()
        }
        let (rtp, rtcp) = bind_tokio_udp_pair(Ipv4Addr::LOCALHOST.into())
            .await
            .unwrap();
        let rtp_address = rtp.local_addr().unwrap();
        let rtcp_address = rtcp.local_addr().unwrap();
        let out = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let out_address = out.local_addr().unwrap();
        let controller = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let producer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let metrics = Metrics::new();
        let proxy = spawn_media_proxy(
            "audio",
            rtp,
            rtcp,
            out,
            "127.0.0.1".into(),
            controller.local_addr().unwrap().port(),
            vec![7; 30],
            42,
            [9; 16],
            188,
            3,
            metrics.clone(),
        )
        .unwrap();
        let proxy = proxy.with_producer(fixture_child("wait"));
        let pid = proxy.producer_id.unwrap();
        let fence = proxy.transport.clone();
        let mut crypto = Context::new(
            &[7; 16],
            &[7; 14],
            ProtectionProfile::Aes128CmHmacSha1_80,
            None,
            None,
        )
        .unwrap();
        let mut packet = vec![0x80, 110, 0, 1, 0, 0, 3, 0xe8, 0, 0, 0, 42, 0x55];
        producer.send_to(&packet, rtp_address).await.unwrap();
        let first = crypto.decrypt_rtp(&receive(&controller).await).unwrap();
        let origin = u32::from_be_bytes(first[4..8].try_into().unwrap());
        packet[3] = 2;
        packet[4..8].copy_from_slice(&1960u32.to_be_bytes());
        producer.send_to(&packet, rtp_address).await.unwrap();
        let second = crypto.decrypt_rtp(&receive(&controller).await).unwrap();
        assert_eq!(
            u32::from_be_bytes(second[4..8].try_into().unwrap()),
            origin.wrapping_add(320)
        );
        let mut report = vec![0x80, 200, 0, 6, 0, 0, 0, 42];
        report.extend_from_slice(&[0; 8]);
        report.extend_from_slice(&1960u32.to_be_bytes());
        report.extend_from_slice(&[0; 8]);
        producer.send_to(&report, rtcp_address).await.unwrap();
        let control = crypto.decrypt_rtcp(&receive(&controller).await).unwrap();
        assert_eq!(&control[16..20], &second[4..8]);
        assert_eq!(control.len(), 72);
        assert_eq!(&control[8..16], &report[8..16]);
        assert_eq!(&control[20..28], &[0, 0, 0, 2, 0, 0, 0, 2]);
        assert_eq!(&control[38..70], b"09090909090909090909090909090909");
        let feedback = [0x80, 201, 0, 1, 0, 0, 0, 99];
        let encrypted = crypto.encrypt_rtcp(&feedback).unwrap();
        controller.send_to(&encrypted, out_address).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !metrics.render(false).contains("kind=\"datagram\"} 1") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), producer.recv(&mut [0; 2048]))
                .await
                .is_err()
        );
        let malformed = [0x81, 201, 0, 1, 0, 0, 0, 99];
        controller
            .send_to(&crypto.encrypt_rtcp(&malformed).unwrap(), out_address)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !metrics.render(false).contains("kind=\"refused\"} 1") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut registry = registry();
        let manager = StreamManager::new(
            vec![VideoSource {
                source: register(&mut registry, 1, "rtsp://unused.invalid".into()),
                profile: VideoProfile::LIVE_720P,
            }],
            "rtsp://unused.invalid".into(),
            true,
            Ipv4Addr::LOCALHOST.into(),
            metrics,
        );
        manager
            .inner
            .lock()
            .await
            .audio_proxies
            .insert(vec![1], proxy);
        manager.end_session(Some(vec![1])).await;
        manager.stop_stream().await;
        registry.shutdown().await.unwrap();
        assert_reaped(pid);
        assert!(matches!(
            fence.send_rtp(&packet, || true).await,
            Err(amcrust::media::transport::Error::Closed)
        ));
        drop(fence);
        // Explicit stop joins before returning, releasing all three owned sockets.
        let _rtp = UdpSocket::bind(rtp_address).await.unwrap();
        let _rtcp = UdpSocket::bind(rtcp_address).await.unwrap();
        let _out = UdpSocket::bind(out_address).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "real north-driveway camera, serial read-only access; production video control/teardown, not playback"]
    async fn real_production_feedback_and_joined_teardown() {
        use webrtc_srtp::{context::Context, protection_profile::ProtectionProfile};
        dotenvy::dotenv().ok();
        let user = std::env::var("AMCREST_USERNAME").unwrap();
        let password = std::env::var("AMCREST_PASSWORD").unwrap();
        let url =
            format!("rtsp://{user}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype=0");
        let metrics = Metrics::new();
        let mut registry = registry();
        let source = VideoSource {
            source: register(&mut registry, 1, url.clone()),
            profile: VideoProfile::LIVE_1080P,
        };
        let manager = StreamManager::new(
            vec![source.clone()],
            url,
            false,
            Ipv4Addr::LOCALHOST.into(),
            metrics.clone(),
        );
        let controller = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let out = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let destination = out.local_addr().unwrap();
        let id = vec![1; 16];
        manager.inner.lock().await.sessions.insert(
            id.clone(),
            Session {
                id: id.clone(),
                controller_ip: "127.0.0.1".into(),
                video_port: controller.local_addr().unwrap().port(),
                audio_port: 0,
                video_key: vec![7; 30],
                audio_key: vec![],
                video_ssrc: 42,
                audio_ssrc: 43,
                rtcp_identity: [9; 16],
                video_out_socket: Some(out),
                audio_out_socket: None,
            },
        );
        manager
            .start_stream(
                Some(id.clone()),
                source,
                99,
                1200,
                110,
                48000,
                20,
                24,
                1.0,
                1200,
                0.5,
            )
            .await;
        let pid = manager
            .inner
            .lock()
            .await
            .video_proxies
            .get(&id)
            .and_then(|p| p.producer_id);
        let mut crypto = Context::new(
            &[7; 16],
            &[7; 14],
            ProtectionProfile::Aes128CmHmacSha1_80,
            None,
            None,
        )
        .unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(20), async {
            let mut buffer = [0; 2048];
            let mut packets = 0u64;
            let mut report = false;
            while packets < 100 || !report {
                let size = controller.recv(&mut buffer).await.map_err(|e| e.to_string())?;
                if (192..=223).contains(&buffer[1]) {
                    let decoded = crypto.decrypt_rtcp(&buffer[..size]).map_err(|e| e.to_string())?;
                    report |= decoded.len() == 72 && decoded[1] == 200;
                } else {
                    crypto.decrypt_rtp(&buffer[..size]).map_err(|e| e.to_string())?;
                    packets += 1;
                }
            }
            let rr = [0x80, 201, 0, 1, 0, 0, 0, 99];
            let pli = [0x81, 206, 0, 2, 0, 0, 0, 99, 0, 0, 0, 42];
            let bye = [0x81, 203, 0, 1, 0, 0, 0, 99];
            for bytes in [rr.as_slice(), pli.as_slice(), bye.as_slice()] {
                controller.send_to(&crypto.encrypt_rtcp(bytes).map_err(|e| e.to_string())?, destination).await.map_err(|e| e.to_string())?;
            }
            loop {
                let inner = manager.inner.lock().await;
                if inner.video_proxies.get(&id).is_some_and(|proxy| proxy.task.as_ref().is_some_and(|t| t.is_finished())) { break; }
                drop(inner);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            let task = manager.inner.lock().await.video_proxies.get_mut(&id)
                .and_then(|p| p.task.take()).ok_or("missing media owner")?;
            task.await.map_err(|e| e.to_string())?;
            if pid.is_some() {
                return Err("shared video unexpectedly spawned a native producer".into());
            }
            if !metrics.render(false).contains("kind=\"bye\"} 1") || !metrics.render(false).contains("kind=\"pli\"} 1") {
                return Err("receiver feedback was not handled locally".into());
            }
            eprintln!("Production video control: decrypted_rtp={packets} sender_report={report}; RR/PLI/BYE accepted; Rust producer ended without a HAP stop request; no video child process");
            Ok::<_, String>(())
        }).await;
        manager.stop_stream().await;
        registry.shutdown().await.unwrap();
        if let Some(pid) = pid {
            assert_reaped(pid);
        }
        assert!(
            metrics
                .render(false)
                .contains("amcrust_connections_open{type=\"live_stream\"} 0")
        );
        result
            .expect("production control deadline")
            .expect("production control");
    }

    #[test]
    fn advertises_all_native_live_profiles() {
        let raw = supported_video_config(&[VideoProfile::LIVE_720P, VideoProfile::LIVE_1080P]);
        let root = tlv8::parse(&raw);
        let codec = tlv8::parse(tlv8::find(&root, 0x01).unwrap());
        let attributes = tlv8::parse(tlv8::find(&codec, 0x03).unwrap());
        assert_eq!(tlv8::find_u16(&attributes, 0x01), Some(1280));
        assert_eq!(tlv8::find_u16(&attributes, 0x02), Some(720));
        assert_eq!(tlv8::find_u8(&attributes, 0x03), Some(15));
        assert_eq!(codec.iter().filter(|item| item.tag == 0x03).count(), 2);
    }

    #[tokio::test]
    async fn selects_native_source_by_controller_dimensions() {
        let mut registry = registry();
        let sources = [
            VideoSource {
                source: register(&mut registry, 1, "rtsp://camera/subtype=2".into()),
                profile: VideoProfile::LIVE_720P,
            },
            VideoSource {
                source: register(&mut registry, 2, "rtsp://camera/subtype=0".into()),
                profile: VideoProfile::LIVE_1080P,
            },
        ];
        assert_eq!(
            select_video_source(&sources, Some(1280), Some(720)).profile,
            VideoProfile::LIVE_720P
        );
        assert_eq!(
            select_video_source(&sources, Some(1920), Some(1080)).profile,
            VideoProfile::LIVE_1080P
        );
        registry.shutdown().await.unwrap();
    }
}
