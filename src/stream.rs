//! HomeKit camera RTP stream management: SetupEndpoints / SelectedRTPStreamConfiguration
//! TLV8 negotiation and shared Rust video/audio producers.

use log::{debug, warn};
use rand::Rng;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::amcrest::VideoProfile;
use crate::metrics::{ErrorSubsystem, Metrics};
use crate::tlv8;
use amcrust::media::rtcp;
use amcrust::media::transport::{Error as TransportError, Transport};

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod privacy_tests;
mod shared;

struct MediaProxy {
    source: amcrust::source::Source,
    transport: shared::Control,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl MediaProxy {
    async fn join(&mut self) {
        self.transport.revoke();
        if let Some(task) = self.task.as_mut() {
            let _ = task.await;
        }
        self.task = None;
    }

    #[cfg(test)]
    async fn stop(mut self) {
        self.join().await;
    }
}
impl Drop for MediaProxy {
    fn drop(&mut self) {
        // Wake media and its supervisor. Aborting the supervisor would lose the
        // explicit join; Drop signals, owned teardown additionally joins.
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

pub(crate) const MAX_PREPARED_SESSIONS: usize = 16;
pub(crate) const MAX_LIVE_SESSIONS: usize = 8;
pub(crate) const PREPARED_LIFETIME: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_SETUP_BYTES: usize = 1024;

struct Session {
    prepared_at: std::time::Instant,
    id: Vec<u8>,
    controller_ip: String,
    video_port: u16,
    audio_port: u16,
    /// SRTP master key || master salt for this track's independent crypto state.
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
    disabled: bool,
    live_disabled: bool,
    main_blocked: bool,
    shutdown: bool,
    /// Prepared sessions and running streams, keyed by HAP session id.
    sessions: std::collections::HashMap<Vec<u8>, Session>,
    video_proxies: std::collections::HashMap<Vec<u8>, MediaProxy>,
    /// Audio RTP proxy tasks, keyed by HAP session id.
    audio_proxies: std::collections::HashMap<Vec<u8>, MediaProxy>,
    /// Response for the most recent SetupEndpoints write, read back by the
    /// controller immediately after writing.
    setup_response: Vec<u8>,
    setup_busy: u64,
    start_busy: u64,
    expired_prepared: u64,
}

impl Inner {
    fn live_sessions(&self) -> usize {
        self.video_proxies.len()
            + self
                .audio_proxies
                .keys()
                .filter(|id| !self.video_proxies.contains_key(*id))
                .count()
    }
    fn owns_session(&self, id: &[u8]) -> bool {
        self.video_proxies.contains_key(id) || self.audio_proxies.contains_key(id)
    }
    fn expire_prepared(&mut self) {
        let now = std::time::Instant::now();
        let response = tlv8::parse(&self.setup_response);
        let response_id = tlv8::find(&response, SETUP_SESSION_ID);
        let mut clear_response = false;
        let before = self.sessions.len();
        self.sessions.retain(|id, session| {
            let keep = now.duration_since(session.prepared_at) < PREPARED_LIFETIME;
            clear_response |=
                !keep && response_id.is_some_and(|response_id| response_id == id.as_slice());
            keep
        });
        self.expired_prepared = self
            .expired_prepared
            .saturating_add((before - self.sessions.len()) as u64);
        if clear_response {
            self.setup_response.clear();
        }
    }
    async fn reap_finished(&mut self) {
        for owner in self
            .video_proxies
            .values_mut()
            .chain(self.audio_proxies.values_mut())
        {
            if owner.task.as_ref().is_none_or(|task| task.is_finished()) {
                owner.join().await;
            }
        }
        self.video_proxies.retain(|_, owner| owner.task.is_some());
        self.audio_proxies.retain(|_, owner| owner.task.is_some());
    }
    // Keep owners in the maps across awaits: cancellation must not detach a join.
    async fn stop(&mut self, session: Option<&[u8]>) {
        self.revoke_transports(session);
        for (id, owner) in self.audio_proxies.iter_mut().chain(&mut self.video_proxies) {
            if session.is_none_or(|session| session == id.as_slice()) {
                owner.join().await;
            }
        }
        self.audio_proxies
            .retain(|id, _| session.is_some_and(|session| session != id.as_slice()));
        self.video_proxies
            .retain(|id, _| session.is_some_and(|session| session != id.as_slice()));
        self.sessions
            .retain(|id, _| session.is_some_and(|session| session != id.as_slice()));
    }

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
    main_profile: Arc<std::sync::RwLock<Option<VideoProfile>>>,
    audio_source: amcrust::source::Source,
    audio_pool: amcrust::media::audio::Pool,
    audio: bool,
    local_ip: IpAddr,
    metrics: Arc<Metrics>,
}

#[derive(Debug, serde::Serialize)]
pub struct StreamResources {
    pub main: amcrust::source::Snapshot,
    pub sub_stream2: Option<amcrust::source::Snapshot>,
    pub live_opus: amcrust::media::PoolSnapshot,
    pub owners: Option<LiveOwners>,
    pub limits: LiveAdmissionLimits,
}
#[derive(Debug, serde::Serialize)]
pub struct LiveAdmissionLimits {
    pub prepared_sessions: usize,
    pub live_sessions: usize,
    pub prepared_lifetime_seconds: u64,
    pub setup_bytes: usize,
}
impl Default for LiveAdmissionLimits {
    fn default() -> Self {
        Self {
            prepared_sessions: MAX_PREPARED_SESSIONS,
            live_sessions: MAX_LIVE_SESSIONS,
            prepared_lifetime_seconds: PREPARED_LIFETIME.as_secs(),
            setup_bytes: MAX_SETUP_BYTES,
        }
    }
}
#[derive(Debug, serde::Serialize)]
pub struct LiveOwners {
    pub prepared_sessions: usize,
    pub video: usize,
    pub audio: usize,
    pub live_sessions: usize,
    pub setup_busy: u64,
    pub start_busy: u64,
    pub expired_prepared: u64,
}

impl StreamManager {
    /// Nonblocking scrape: unavailable owner counts are explicit, not fabricated zeroes.
    pub fn resources(&self) -> StreamResources {
        StreamResources {
            main: self.audio_source.snapshot(),
            sub_stream2: self
                .video_sources
                .iter()
                .find(|s| !s.source.same_owner(&self.audio_source))
                .map(|s| s.source.snapshot()),
            live_opus: self.audio_pool.snapshot(),
            limits: LiveAdmissionLimits::default(),
            owners: self.inner.try_lock().ok().map(|inner| LiveOwners {
                prepared_sessions: inner.sessions.len(),
                video: inner.video_proxies.len(),
                audio: inner.audio_proxies.len(),
                live_sessions: inner.live_sessions(),
                setup_busy: inner.setup_busy,
                start_busy: inner.start_busy,
                expired_prepared: inner.expired_prepared,
            }),
        }
    }
    pub fn new(
        video_sources: Vec<VideoSource>,
        audio_source: amcrust::source::Source,
        audio: bool,
        local_ip: IpAddr,
        metrics: Arc<Metrics>,
    ) -> Self {
        assert!(
            !video_sources.is_empty(),
            "at least one live source is required"
        );
        let main_profile = video_sources
            .iter()
            .find(|v| v.source.same_owner(&audio_source))
            .map(|v| v.profile);
        Self {
            main_profile: Arc::new(std::sync::RwLock::new(main_profile)),
            inner: Arc::new(Mutex::new(Inner::default())),
            video_sources,
            audio_source,
            audio_pool: amcrust::media::audio::Pool::new(8, 16 * 1024 * 1024)
                .expect("fixed audio budget"),
            audio,
            local_ip,
            metrics,
        }
    }

    pub fn audio_enabled(&self) -> bool {
        self.audio
    }

    fn available_video_sources(&self) -> Vec<VideoSource> {
        let mut sources: Vec<_> = self
            .video_sources
            .iter()
            .filter(|v| !v.source.same_owner(&self.audio_source))
            .cloned()
            .collect();
        if let Some(profile) = *self.main_profile.read().unwrap() {
            sources.push(VideoSource {
                source: self.audio_source.clone(),
                profile,
            });
        }
        sources
    }

    pub fn supported_video_config(&self) -> Vec<u8> {
        supported_video_config(
            &self
                .available_video_sources()
                .iter()
                .map(|source| source.profile)
                .collect::<Vec<_>>(),
        )
    }

    /// Handles a controller write to SetupEndpoints and prepares the response
    /// that the controller will read back.
    pub async fn handle_setup_write(&self, payload: Vec<u8>) {
        if payload.len() > MAX_SETUP_BYTES {
            self.inner.lock().await.setup_response =
                tlv8::Writer::new().u8(SETUP_STATUS, 2).build();
            return;
        }
        let items = tlv8::parse(&payload);

        // Status is a response-only tag, not a valid setup request.
        if tlv8::find(&items, SETUP_STATUS).is_some() {
            return;
        }

        let Some(session_id) = tlv8::find(&items, SETUP_SESSION_ID).map(|v| v.to_vec()) else {
            warn!("SetupEndpoints write without session id");
            return;
        };

        // Serialize admission, socket reservation and publication with privacy.
        let mut inner = self.inner.lock().await;
        if session_id.len() != 16 {
            inner.setup_response = tlv8::Writer::new().u8(SETUP_STATUS, 2).build();
            return;
        }
        inner.expire_prepared();
        inner.reap_finished().await;
        if inner.disabled
            || inner.live_disabled
            || inner.shutdown
            || self.available_video_sources().is_empty()
        {
            inner.setup_response = tlv8::Writer::new()
                .bytes(SETUP_SESSION_ID, &session_id)
                .u8(SETUP_STATUS, 2)
                .build();
            return;
        }
        if (inner.sessions.len() >= MAX_PREPARED_SESSIONS
            && !inner.sessions.contains_key(&session_id))
            || (inner.live_sessions() >= MAX_LIVE_SESSIONS && !inner.owns_session(&session_id))
        {
            inner.setup_busy = inner.setup_busy.saturating_add(1);
            inner.setup_response = tlv8::Writer::new()
                .bytes(SETUP_SESSION_ID, &session_id)
                .u8(SETUP_STATUS, 1)
                .build();
            return;
        }
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
            prepared_at: std::time::Instant::now(),
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

        inner.sessions.insert(session_id, session);
        inner.setup_response = response;
    }

    /// The response to a controller read of SetupEndpoints.
    pub async fn setup_read(&self) -> Option<Vec<u8>> {
        let mut inner = self.inner.lock().await;
        inner.expire_prepared();
        if inner.setup_response.is_empty() {
            None
        } else {
            Some(inner.setup_response.clone())
        }
    }

    /// Report unavailable while privacy or shutdown closes admission.
    pub async fn streaming_status(&self) -> Vec<u8> {
        let mut inner = self.inner.lock().await;
        inner.expire_prepared();
        inner.reap_finished().await;
        tlv8::Writer::new()
            .u8(
                0x01,
                if inner.disabled
                    || inner.live_disabled
                    || inner.shutdown
                    || self.available_video_sources().is_empty()
                {
                    2
                } else if inner.live_sessions() >= MAX_LIVE_SESSIONS
                    || inner.sessions.len() >= MAX_PREPARED_SESSIONS
                {
                    1
                } else {
                    0
                },
            )
            .build()
    }

    /// Handles a controller write to SelectedRTPStreamConfiguration.
    pub async fn handle_selected_write(
        &self,
        payload: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
                let sources = self.available_video_sources();
                let source =
                    select_video_source(&sources, selected_width, selected_height, selected_fps)
                        .cloned()
                        .ok_or("selected live format has no current packet-copy source")?;
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

                if !self
                    .start_stream(
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
                    .await
                {
                    return Err("live stream unavailable or unsupported".into());
                }
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
                return Err("resume/reconfigure requires fresh stream setup".into());
            }
            _ => return Err("unknown stream command".into()),
        }
        Ok(())
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
    ) -> bool {
        if !video_rtcp_interval.is_finite() || !(0.0..=60.0).contains(&video_rtcp_interval) {
            self.metrics.error(ErrorSubsystem::LiveStream);
            return false;
        }
        let mut inner = self.inner.lock().await;

        if inner.disabled || inner.live_disabled || inner.shutdown {
            return false;
        }
        if video_source.source.same_owner(&self.audio_source)
            && (inner.main_blocked
                || *self.main_profile.read().unwrap() != Some(video_source.profile))
        {
            return false;
        }
        inner.expire_prepared();
        inner.reap_finished().await;
        let requested = session_id.as_deref().or_else(|| {
            (inner.sessions.len() == 1).then(|| inner.sessions.keys().next().unwrap().as_slice())
        });
        if inner.live_sessions() >= MAX_LIVE_SESSIONS
            && !requested.is_some_and(|id| inner.owns_session(id))
        {
            inner.start_busy = inner.start_busy.saturating_add(1);
            return false;
        }
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
            return false;
        };

        // Fence both tracks and retain their joins before replacing the session.
        inner.stop(Some(&session.id)).await;
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
            return false;
        }
        let Ok(controller_ip) = session.controller_ip.parse() else {
            self.metrics.error(ErrorSubsystem::LiveStream);
            return false;
        };
        let clock = amcrust::media::presentation::PresentationClock::new();
        let video_proxy = shared::spawn(
            shared::Video {
                clock,
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
            && !inner.main_blocked
            && !session.audio_key.is_empty()
            && session.audio_out_socket.is_some()
            && audio_wire_mtu >= 188;

        inner.video_proxies.insert(session.id.clone(), video_proxy);
        self.metrics.live_stream_started();

        if audio_enabled
            && audio_rtcp_interval.is_finite()
            && (0.0..=60.0).contains(&audio_rtcp_interval)
        {
            let proxy = shared::spawn_audio(
                shared::Audio {
                    clock,
                    source: self.audio_source.clone(),
                    socket: session.audio_out_socket.take().unwrap(),
                    peer: SocketAddr::new(controller_ip, session.audio_port),
                    key: session.audio_key.clone(),
                    ssrc: session.audio_ssrc,
                    identity: session.rtcp_identity,
                    mtu: audio_wire_mtu,
                    payload: audio_payload_type,
                    interval: std::time::Duration::from_secs_f32(audio_rtcp_interval.max(0.5)),
                    config: amcrust::media::audio::Config {
                        rate: audio_rate_hz,
                        frame_ms: audio_packet_time,
                        bitrate: u32::from(audio_max_bitrate.clamp(16, 64)) * 1000,
                        ..Default::default()
                    },
                    pool: self.audio_pool.clone(),
                },
                self.metrics.clone(),
            );
            inner.audio_proxies.insert(session.id.clone(), proxy);
        }
        true
    }

    /// Ends the stream for one session (or all, if no id was given).
    async fn end_session(&self, session_id: Option<Vec<u8>>) {
        let mut inner = self.inner.lock().await;
        inner.stop(session_id.as_deref()).await;
    }

    /// RTP-service Active is independent of camera/source and recording permission.
    /// Leave shared acquisition running for non-live consumers; never revive sessions.
    pub async fn set_live_enabled(&self, enabled: bool) -> Result<(), amcrust::source::Terminal> {
        let mut inner = self.inner.lock().await;
        if enabled && inner.shutdown {
            return Err(amcrust::source::Terminal::Shutdown);
        }
        if enabled && !inner.live_disabled {
            return Ok(());
        }
        inner.live_disabled = true;
        inner.sessions.clear();
        inner.setup_response.clear();
        // Also finish an interrupted disable before reopening admission.
        inner.stop(None).await;
        inner.live_disabled = !enabled;
        Ok(())
    }

    /// Camera-wide RTSP privacy. Sources retire recording as well as live grants.
    /// Re-enable permits fresh demand only; old sessions/keys are never revived.
    pub async fn set_enabled(&self, enabled: bool) -> Result<(), amcrust::source::Terminal> {
        let mut inner = self.inner.lock().await;
        if enabled && inner.shutdown {
            return Err(amcrust::source::Terminal::Shutdown);
        }
        if enabled && !inner.disabled {
            return Ok(());
        }
        self.reconcile_enabled(&mut inner, enabled).await
    }

    async fn reconcile_enabled(
        &self,
        inner: &mut Inner,
        enabled: bool,
    ) -> Result<(), amcrust::source::Terminal> {
        inner.disabled = true;
        inner.revoke_transports(None);
        inner.sessions.clear();
        inner.setup_response.clear();
        let mut failure = None;
        for source in self
            .video_sources
            .iter()
            .map(|v| &v.source)
            .chain(std::iter::once(&self.audio_source))
        {
            let source_enabled =
                enabled && !(inner.main_blocked && source.same_owner(&self.audio_source));
            if let Err(error) = source.set_enabled(source_enabled).await {
                failure = Some(error);
            }
        }
        if enabled && failure.is_some() {
            // Fail closed if only part of the source set could be enabled.
            for source in self
                .video_sources
                .iter()
                .map(|v| &v.source)
                .chain(std::iter::once(&self.audio_source))
            {
                let _ = source.set_enabled(false).await;
            }
        }
        inner.stop(None).await;
        if let Some(error) = failure {
            return Err(error);
        }
        inner.disabled = !enabled;
        Ok(())
    }

    /// Fail-closed main-encoder transition. Unrelated video remains usable.
    pub async fn begin_main_reconfiguration(&self) -> Result<(), amcrust::source::Terminal> {
        let mut inner = self.inner.lock().await;
        if inner.shutdown {
            return Err(amcrust::source::Terminal::Shutdown);
        }
        inner.main_blocked = true;
        *self.main_profile.write().unwrap() = None;
        inner.sessions.clear();
        inner.setup_response.clear();
        for owner in inner
            .video_proxies
            .values()
            .chain(inner.audio_proxies.values())
        {
            if owner.source.same_owner(&self.audio_source) {
                owner.transport.revoke();
            }
        }
        self.audio_source.set_enabled(false).await?;
        for owner in inner.video_proxies.values_mut() {
            if owner.source.same_owner(&self.audio_source) {
                owner.join().await;
            }
        }
        for owner in inner.audio_proxies.values_mut() {
            if owner.source.same_owner(&self.audio_source) {
                owner.join().await;
            }
        }
        inner
            .video_proxies
            .retain(|_, p| !p.source.same_owner(&self.audio_source));
        inner
            .audio_proxies
            .retain(|_, p| !p.source.same_owner(&self.audio_source));
        Ok(())
    }

    pub async fn finish_main_reconfiguration(
        &self,
        profile: Option<VideoProfile>,
    ) -> Result<(), amcrust::source::Terminal> {
        let mut inner = self.inner.lock().await;
        if inner.shutdown {
            return Err(amcrust::source::Terminal::Shutdown);
        }
        // Confirmed settings define a fresh attempt, not the failed old policy's cooldown.
        self.audio_source.reset().await?;
        self.audio_source.set_enabled(!inner.disabled).await?;
        *self.main_profile.write().unwrap() = profile;
        inner.main_blocked = false;
        Ok(())
    }

    /// Permanently close admission before joining shutdown work.
    pub async fn stop_stream(&self) {
        let mut inner = self.inner.lock().await;
        inner.shutdown = true;
        let _ = self.reconcile_enabled(&mut inner, false).await;
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
    fps: Option<u8>,
) -> Option<&VideoSource> {
    match (width, height) {
        (None, None) => sources
            .iter()
            .find(|source| fps.is_none_or(|fps| source.profile.fps == fps)),
        (Some(width), Some(height)) => sources.iter().find(|source| {
            source.profile.width == width
                && source.profile.height == height
                && fps.is_none_or(|fps| source.profile.fps == fps)
        }),
        _ => None,
    }
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
    pub(super) fn registry() -> amcrust::source::Registry {
        amcrust::source::Registry::new(amcrust::source::Limits {
            timing: amcrust::source::Timing::Presentation,
            ..Default::default()
        })
        .unwrap()
    }
    pub(super) fn register(
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
            source.source.clone(),
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
                prepared_at: std::time::Instant::now(),
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
            if !metrics.render(false).contains("kind=\"bye\"} 1") || !metrics.render(false).contains("kind=\"pli\"} 1") {
                return Err("receiver feedback was not handled locally".into());
            }
            eprintln!("Production video control: decrypted_rtp={packets} sender_report={report}; RR/PLI/BYE accepted; Rust producer ended without a HAP stop request; no video child process");
            Ok::<_, String>(())
        }).await;
        manager.stop_stream().await;
        registry.shutdown().await.unwrap();
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
            select_video_source(&sources, Some(1280), Some(720), None)
                .unwrap()
                .profile,
            VideoProfile::LIVE_720P
        );
        assert_eq!(
            select_video_source(&sources, Some(1920), Some(1080), None)
                .unwrap()
                .profile,
            VideoProfile::LIVE_1080P
        );
        let mut faster = sources[1].clone();
        faster.profile.fps = 30;
        let same_dimensions = [sources[1].clone(), faster];
        assert_eq!(
            select_video_source(&same_dimensions, Some(1920), Some(1080), Some(30))
                .unwrap()
                .profile
                .fps,
            30
        );
        assert!(select_video_source(&same_dimensions, Some(1920), Some(1080), Some(60)).is_none());
        assert!(select_video_source(&sources, Some(640), Some(480), None).is_none());
        assert!(select_video_source(&sources, Some(1920), None, None).is_none());
        assert!(select_video_source(&[], None, None, None).is_none());
        registry.shutdown().await.unwrap();
    }
}
