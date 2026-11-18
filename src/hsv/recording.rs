//! HSV recording state: supported/selected recording configuration TLVs,
//! persistence and shared-source recording configuration. Video stays encoded;
//! recording audio uses bounded recovered resampling and AAC encoding.

use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;

use crate::amcrest::{AmcrestClient, EncoderCapabilities, VideoProfile};
use crate::hsv::fmp4::{Recorder, RecorderConfig};
use crate::metrics::Metrics;
use crate::tlv8;

#[cfg(test)]
#[path = "configuration_tests.rs"]
mod configuration_tests;

pub const PREBUFFER_MS: u32 = 4000;
pub const FRAGMENT_MS: u32 = 4000;

/// The parsed SelectedCameraRecordingConfiguration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SelectedConfig {
    pub raw: Vec<u8>,
    pub prebuffer_ms: u32,
    pub fragment_ms: u32,
    pub width: u16,
    pub height: u16,
    pub fps: u8,
    pub video_bitrate_kbps: u32,
    pub iframe_interval_ms: u32,
    pub audio_channels: u8,
    pub audio_sample_rate: u8,
    pub audio_bitrate_kbps: u32,
}

#[derive(Default, Serialize, Deserialize)]
struct PersistedState {
    selected_b64: Option<String>,
    recording_active: bool,
    audio_active: bool,
    event_snapshots: bool,
    periodic_snapshots: bool,
    homekit_active: bool,
}

pub struct HsvState {
    camera_name: String,
    pub recording_active: AtomicBool,
    pub audio_active: AtomicBool,
    pub event_snapshots: AtomicBool,
    pub periodic_snapshots: AtomicBool,
    pub homekit_active: AtomicBool,
    pub selected: Mutex<Option<SelectedConfig>>,
    pub recorder: Recorder,
    pub snapshots: crate::amcrest::SnapshotCache,
    pub motion_active: Arc<AtomicBool>,
    camera: AmcrestClient,
    path: PathBuf,
    configuration_lock: Mutex<()>,
    recorder_lock: Mutex<()>,
    camera_lock: Mutex<()>,
    stopped: AtomicBool,
    configuration_ready: AtomicBool,
    encoder_dirty: AtomicBool,
    capabilities: EncoderCapabilities,
}

impl HsvState {
    pub fn load(
        camera_name: String,
        data_dir: &str,
        camera: AmcrestClient,
        motion_active: Arc<AtomicBool>,
        metrics: Arc<Metrics>,
        capabilities: EncoderCapabilities,
        source: amcrust::source::Source,
    ) -> Arc<Self> {
        let path = PathBuf::from(data_dir).join("hsv.json");
        let persisted: PersistedState = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or(PersistedState {
                selected_b64: None,
                recording_active: false,
                audio_active: true,
                event_snapshots: true,
                periodic_snapshots: true,
                homekit_active: true,
            });

        let selected = persisted
            .selected_b64
            .as_ref()
            .and_then(|b64| {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .ok()
                    .and_then(|raw| parse_selected(&raw))
            })
            .filter(|selected| {
                supported_selection(selected)
                    && capabilities.recording_attributes().contains(&(
                        selected.width,
                        selected.height,
                        selected.fps,
                    ))
            });

        let snapshots = crate::amcrest::SnapshotCache::new(camera.clone());
        snapshots.invalidate(
            persisted.homekit_active && (persisted.event_snapshots || persisted.periodic_snapshots),
        );
        Arc::new(Self {
            snapshots,
            camera_name,
            recording_active: AtomicBool::new(persisted.recording_active),
            audio_active: AtomicBool::new(persisted.audio_active),
            event_snapshots: AtomicBool::new(persisted.event_snapshots),
            periodic_snapshots: AtomicBool::new(persisted.periodic_snapshots),
            homekit_active: AtomicBool::new(persisted.homekit_active),
            selected: Mutex::new(selected),
            recorder: Recorder::new(source, metrics),
            motion_active,
            camera,
            path,
            configuration_lock: Mutex::new(()),
            recorder_lock: Mutex::new(()),
            camera_lock: Mutex::new(()),
            stopped: AtomicBool::new(false),
            configuration_ready: AtomicBool::new(true),
            encoder_dirty: AtomicBool::new(false),
            capabilities,
        })
    }

    pub fn recording_attributes(&self) -> Vec<(u16, u16, u8)> {
        self.capabilities.recording_attributes()
    }

    /// Native main-stream profile available to live view without transcoding.
    pub async fn selected_main_profile(&self) -> Option<VideoProfile> {
        self.selected
            .lock()
            .await
            .as_ref()
            .map(selected_video_profile)
    }

    pub async fn persist(&self) {
        use base64::Engine;
        let selected_b64 = self
            .selected
            .lock()
            .await
            .as_ref()
            .map(|s| base64::engine::general_purpose::STANDARD.encode(&s.raw));
        let state = PersistedState {
            selected_b64,
            recording_active: self.recording_active.load(Ordering::SeqCst),
            audio_active: self.audio_active.load(Ordering::SeqCst),
            event_snapshots: self.event_snapshots.load(Ordering::SeqCst),
            periodic_snapshots: self.periodic_snapshots.load(Ordering::SeqCst),
            homekit_active: self.homekit_active.load(Ordering::SeqCst),
        };
        if let Ok(bytes) = serde_json::to_vec_pretty(&state) {
            if let Err(e) = std::fs::write(&self.path, bytes) {
                warn!("failed to persist HSV state: {e}");
            }
        }
    }

    /// Reconcile external settings only after all affected media has retired.
    pub async fn handle_selected_write(
        self: &Arc<Self>,
        streams: &crate::stream::StreamManager,
        raw: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = parse_selected(&raw).ok_or("invalid recording configuration")?;
        if !supported_selection(&config)
            || !self.capabilities.recording_attributes().contains(&(
                config.width,
                config.height,
                config.fps,
            ))
        {
            return Err("unsupported recording configuration".into());
        }
        self.reconcile_configuration(streams, Some(config), false, |config| async move {
            self.apply_configuration(config.as_ref()).await
        })
        .await
    }

    pub async fn resume_recorder(
        self: &Arc<Self>,
        streams: &crate::stream::StreamManager,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let selected = self.selected.lock().await.clone();
        self.reconcile_configuration(streams, selected, true, |config| async move {
            self.apply_configuration(config.as_ref()).await
        })
        .await
    }

    // Injecting the settings operation keeps tests read-only: the production operation
    // below is the sole camera-mutating path through this transaction.
    async fn reconcile_configuration<F, Fut>(
        self: &Arc<Self>,
        streams: &crate::stream::StreamManager,
        next: Option<SelectedConfig>,
        force: bool,
        apply: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce(Option<SelectedConfig>) -> Fut,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    {
        let _configuration = self.configuration_lock.lock().await;
        let _camera = self.camera_lock.lock().await;
        if self.stopped.load(Ordering::SeqCst) {
            return Err("camera shut down".into());
        }
        let current = self.selected.lock().await.clone();
        let ready = self.configuration_ready.load(Ordering::SeqCst);
        if !force && ready && current.as_ref().map(|c| &c.raw) == next.as_ref().map(|c| &c.raw) {
            debug!("ignoring duplicate selected recording configuration");
            return Ok(());
        }
        let encoder_change = force
            || self.encoder_dirty.load(Ordering::SeqCst)
            || match (&current, &next) {
                (Some(current), Some(next)) => !same_encoder_settings(current, next),
                _ => true,
            };
        let recorder_guard = self.recorder_lock.lock().await;
        // Sticky until the whole operation succeeds, including across cancelled waits.
        self.configuration_ready.store(false, Ordering::SeqCst);
        if encoder_change {
            self.encoder_dirty.store(true, Ordering::SeqCst);
            self.snapshots.invalidate(false);
            streams.begin_main_reconfiguration().await?;
        }
        self.recorder.stop().await;
        if encoder_change {
            apply(next.clone()).await?;
            streams
                .finish_main_reconfiguration(next.as_ref().map(selected_video_profile))
                .await?;
            self.encoder_dirty.store(false, Ordering::SeqCst);
        }
        *self.selected.lock().await = next;
        self.configuration_ready.store(true, Ordering::SeqCst);
        if encoder_change {
            self.snapshots.invalidate(self.snapshots_enabled());
        }
        self.persist().await;
        drop(recorder_guard);
        self.sync_recorder().await;
        Ok(())
    }

    async fn apply_configuration(
        &self,
        selected: Option<&SelectedConfig>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(config) = selected {
            self.apply_camera_settings(config).await?;
        }
        self.restore_dependent_camera_profiles(selected.map(|c| (c.width, c.height)))
            .await
    }

    fn snapshots_enabled(&self) -> bool {
        self.homekit_active.load(Ordering::SeqCst)
            && !self.encoder_dirty.load(Ordering::SeqCst)
            && !self.stopped.load(Ordering::SeqCst)
            && (self.periodic_snapshots.load(Ordering::SeqCst)
                || self.event_snapshots.load(Ordering::SeqCst))
    }

    /// The value returned for reads of SelectedCameraRecordingConfiguration.
    pub async fn selected_read(&self) -> Option<Vec<u8>> {
        self.selected.lock().await.as_ref().map(|s| s.raw.clone())
    }

    /// Adjusts the camera's main-stream encoder to exactly match the selected
    /// video configuration. Keep acquisition AAC at 48 kHz; the Rust recording
    /// encoder owns the independently negotiated 32/48-kHz output rate.
    async fn apply_camera_settings(
        &self,
        config: &SelectedConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let gop = config.fps as u32 * config.iframe_interval_ms / 1000;
        let bitrate = self
            .capabilities
            .main_bitrate_range
            .map_or(config.video_bitrate_kbps, |(_, max)| {
                config.video_bitrate_kbps.min(max)
            });
        self.camera
            .ensure_recording_encoder(config.width, config.height, config.fps, gop, bitrate, 48000)
            .await?;
        info!(
            "[{}] camera main stream set to {}x{}@{} GOP {} for recording",
            self.camera_name, config.width, config.height, config.fps, gop
        );
        Ok(())
    }

    /// Motion remains last because firmware may reset it on other profile writes.
    async fn restore_dependent_camera_profiles(
        &self,
        main_resolution: Option<(u16, u16)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.camera
            .ensure_snapshot_profile(&self.capabilities, main_resolution)
            .await?;
        self.camera.ensure_smart_motion().await?;
        Ok(())
    }

    /// Starts or stops the recording pipeline according to the current state.
    /// Serialize camera mode changes across live acquisition and recording.
    /// A successful return means both source retirement and recorder joins completed.
    pub async fn set_camera_active(
        self: &Arc<Self>,
        streams: &crate::stream::StreamManager,
        active: bool,
    ) -> Result<(), amcrust::source::Terminal> {
        let _guard = self.camera_lock.lock().await;
        if self.stopped.load(Ordering::SeqCst) {
            return Err(amcrust::source::Terminal::Shutdown);
        }
        self.homekit_active.store(active, Ordering::SeqCst);
        self.snapshots.invalidate(self.snapshots_enabled());
        let result = streams.set_enabled(active).await;
        if result.is_err() {
            self.homekit_active.store(false, Ordering::SeqCst);
            self.snapshots.invalidate(false);
        }
        self.sync_recorder().await;
        self.persist().await;
        result
    }

    pub async fn set_snapshot_active(&self, periodic: bool, active: bool) {
        let _guard = self.camera_lock.lock().await;
        if periodic {
            self.periodic_snapshots.store(active, Ordering::SeqCst);
        } else {
            self.event_snapshots.store(active, Ordering::SeqCst);
        }
        self.snapshots.invalidate(self.snapshots_enabled());
        self.persist().await;
    }

    pub async fn shutdown(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        let _camera = self.camera_lock.lock().await;
        self.snapshots.shutdown().await;
        let _guard = self.recorder_lock.lock().await;
        self.recorder.stop().await;
    }

    pub async fn sync_recorder(self: &Arc<Self>) {
        // Read desired state AFTER acquiring reconciliation ownership. An older
        // callback must never restart recording after a disable acknowledgement.
        let _guard = self.recorder_lock.lock().await;
        let selected = self.selected.lock().await.clone();
        let active = self.configuration_ready.load(Ordering::SeqCst)
            && !self.stopped.load(Ordering::SeqCst)
            && self.recording_active.load(Ordering::SeqCst)
            && self.homekit_active.load(Ordering::SeqCst);
        match (active, selected) {
            (true, Some(config)) => {
                let audio_sample_rate_hz = match config.audio_sample_rate {
                    0 => 8000,
                    1 => 16000,
                    2 => 24000,
                    3 => 32000,
                    4 => 44100,
                    _ => 48000,
                };
                self.recorder
                    .start(RecorderConfig {
                        width: config.width,
                        height: config.height,
                        audio_bitrate: config.audio_bitrate_kbps.saturating_mul(1000),
                        audio: self.audio_active.load(Ordering::SeqCst),
                        fps: config.fps,
                        audio_sample_rate_hz,
                        fragment_ms: config.fragment_ms,
                        prebuffer_ms: config.prebuffer_ms,
                    })
                    .await;
            }
            _ => self.recorder.stop().await,
        }
    }

    /// Whether a dataSend open for `ipcamera.recording` is currently allowed.
    pub async fn recording_allowed(&self) -> Result<(), u8> {
        if !self.configuration_ready.load(Ordering::SeqCst) || self.selected.lock().await.is_none()
        {
            return Err(9); // INVALID_CONFIGURATION
        }
        if self.stopped.load(Ordering::SeqCst)
            || !self.recording_active.load(Ordering::SeqCst)
            || !self.homekit_active.load(Ordering::SeqCst)
        {
            return Err(1); // NOT_ALLOWED
        }
        Ok(())
    }
}

fn selected_video_profile(config: &SelectedConfig) -> VideoProfile {
    VideoProfile {
        width: config.width,
        height: config.height,
        fps: config.fps,
        bitrate_kbps: config.video_bitrate_kbps,
    }
}

fn supported_selection(config: &SelectedConfig) -> bool {
    config.prebuffer_ms <= PREBUFFER_MS
        && (1..=FRAGMENT_MS).contains(&config.fragment_ms)
        && (1..=FRAGMENT_MS).contains(&config.iframe_interval_ms)
        && u32::from(config.fps) * config.iframe_interval_ms >= 1000
        && config.audio_channels == 1
        && matches!(config.audio_sample_rate, 3 | 5)
        && (16..=192).contains(&config.audio_bitrate_kbps)
}
fn same_encoder_settings(left: &SelectedConfig, right: &SelectedConfig) -> bool {
    left.width == right.width
        && left.height == right.height
        && left.fps == right.fps
        && left.video_bitrate_kbps == right.video_bitrate_kbps
        && left.iframe_interval_ms == right.iframe_interval_ms
}

// --- Supported configuration TLVs ---

pub fn supported_camera_recording_config() -> Vec<u8> {
    let container_params = tlv8::Writer::new().u32(0x01, FRAGMENT_MS).build();
    let container = tlv8::Writer::new()
        .u8(0x01, 0) // fragmented MP4
        .bytes(0x02, &container_params)
        .build();

    // Event trigger options: 8-byte field, motion bit in the low word.
    let triggers: u64 = 0x01;

    tlv8::Writer::new()
        .u32(0x01, PREBUFFER_MS)
        .bytes(0x02, &triggers.to_le_bytes())
        .bytes(0x03, &container)
        .build()
}

pub fn supported_video_recording_config(attributes: &[(u16, u16, u8)]) -> Vec<u8> {
    let mut params = tlv8::Writer::new();
    params
        .u8(0x01, 0x00)
        .delimiter()
        .u8(0x01, 0x01)
        .delimiter()
        .u8(0x01, 0x02); // profiles
    params
        .u8(0x02, 0x00)
        .delimiter()
        .u8(0x02, 0x01)
        .delimiter()
        .u8(0x02, 0x02); // levels
    let params = params.build();

    let mut codec_config = tlv8::Writer::new();
    codec_config.u8(0x01, 0x00); // H.264
    codec_config.bytes(0x02, &params);
    for (i, &(w, h, fps)) in attributes.iter().enumerate() {
        if i > 0 {
            codec_config.delimiter();
        }
        let attr = tlv8::Writer::new()
            .u16(0x01, w)
            .u16(0x02, h)
            .u8(0x03, fps)
            .build();
        codec_config.bytes(0x03, &attr);
    }
    let codec_config = codec_config.build();

    tlv8::Writer::new().bytes(0x01, &codec_config).build()
}

pub fn supported_audio_recording_config() -> Vec<u8> {
    // AAC-LC mono; Rust emits either rate from the shared 48-kHz acquisition.
    let params = tlv8::Writer::new()
        .u8(0x01, 1) // channels
        .u8(0x02, 0) // variable bitrate
        .u8(0x03, 3) // 32 kHz
        .delimiter()
        .u8(0x03, 5) // 48 kHz
        .build();
    let codec_config = tlv8::Writer::new()
        .u8(0x01, 0) // AAC-LC
        .bytes(0x02, &params)
        .build();
    tlv8::Writer::new().bytes(0x01, &codec_config).build()
}

fn read_u32(items: &[tlv8::Item], tag: u8) -> Option<u32> {
    tlv8::find(items, tag).and_then(|v| {
        let mut bytes = [0u8; 4];
        let n = v.len().min(4);
        bytes[..n].copy_from_slice(&v[..n]);
        Some(u32::from_le_bytes(bytes))
    })
}

pub fn parse_selected(raw: &[u8]) -> Option<SelectedConfig> {
    let items = tlv8::parse(raw);

    let general = tlv8::parse(tlv8::find(&items, 0x01)?);
    let prebuffer_ms = read_u32(&general, 0x01).unwrap_or(PREBUFFER_MS);
    let container = tlv8::parse(tlv8::find(&general, 0x03).unwrap_or(&[]));
    let container_params = tlv8::parse(tlv8::find(&container, 0x02).unwrap_or(&[]));
    let fragment_ms = read_u32(&container_params, 0x01).unwrap_or(FRAGMENT_MS);

    let video = tlv8::parse(tlv8::find(&items, 0x02)?);
    let video_params = tlv8::parse(tlv8::find(&video, 0x02).unwrap_or(&[]));
    let video_bitrate_kbps = read_u32(&video_params, 0x03).unwrap_or(2000);
    let iframe_interval_ms = read_u32(&video_params, 0x04).unwrap_or(fragment_ms);
    let attributes = tlv8::parse(tlv8::find(&video, 0x03).unwrap_or(&[]));
    let width = tlv8::find_u16(&attributes, 0x01).unwrap_or(1920);
    let height = tlv8::find_u16(&attributes, 0x02).unwrap_or(1080);
    let fps = tlv8::find_u8(&attributes, 0x03).unwrap_or(15);

    let audio = tlv8::parse(tlv8::find(&items, 0x03).unwrap_or(&[]));
    let audio_config = tlv8::parse(tlv8::find(&audio, 0x01).unwrap_or(&[]));
    let audio_params = tlv8::parse(tlv8::find(&audio_config, 0x02).unwrap_or(&[]));
    let audio_channels = tlv8::find_u8(&audio_params, 0x01).unwrap_or(1);
    let audio_sample_rate = tlv8::find_u8(&audio_params, 0x03).unwrap_or(5);
    let audio_bitrate_kbps = read_u32(&audio_params, 0x04).unwrap_or(64);

    Some(SelectedConfig {
        raw: raw.to_vec(),
        prebuffer_ms,
        fragment_ms,
        width,
        height,
        fps,
        video_bitrate_kbps,
        iframe_interval_ms,
        audio_channels,
        audio_sample_rate,
        audio_bitrate_kbps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_config_roundtrip() {
        // Build a selected config the way a controller would.
        let container_params = tlv8::Writer::new().u32(0x01, 4000).build();
        let container = tlv8::Writer::new()
            .u8(0x01, 0)
            .bytes(0x02, &container_params)
            .build();
        let general = tlv8::Writer::new()
            .u32(0x01, 8000)
            .bytes(0x02, &1u64.to_le_bytes())
            .bytes(0x03, &container)
            .build();

        let vparams = tlv8::Writer::new()
            .u8(0x01, 1)
            .u8(0x02, 2)
            .u32(0x03, 2000)
            .u32(0x04, 4000)
            .build();
        let vattrs = tlv8::Writer::new()
            .u16(0x01, 1920)
            .u16(0x02, 1080)
            .u8(0x03, 15)
            .build();
        let video = tlv8::Writer::new()
            .u8(0x01, 0)
            .bytes(0x02, &vparams)
            .bytes(0x03, &vattrs)
            .build();

        let aparams = tlv8::Writer::new()
            .u8(0x01, 1)
            .u8(0x02, 0)
            .u8(0x03, 5)
            .u32(0x04, 96)
            .build();
        let acodec = tlv8::Writer::new()
            .u8(0x01, 0)
            .bytes(0x02, &aparams)
            .build();
        let audio = tlv8::Writer::new().bytes(0x01, &acodec).build();

        let raw = tlv8::Writer::new()
            .bytes(0x01, &general)
            .bytes(0x02, &video)
            .bytes(0x03, &audio)
            .build();

        let parsed = parse_selected(&raw).unwrap();
        assert_eq!(parsed.prebuffer_ms, 8000);
        assert_eq!(parsed.fragment_ms, 4000);
        assert_eq!(parsed.width, 1920);
        assert_eq!(parsed.height, 1080);
        assert_eq!(parsed.fps, 15);
        assert_eq!(parsed.video_bitrate_kbps, 2000);
        assert_eq!(parsed.iframe_interval_ms, 4000);
        assert_eq!(parsed.audio_sample_rate, 5);
        assert_eq!(parsed.audio_bitrate_kbps, 96);
        assert!(!supported_selection(&parsed)); // Parsed is not the same as admitted.
        let mut supported = parsed.clone();
        supported.prebuffer_ms = PREBUFFER_MS;
        assert!(supported_selection(&supported));
        let mut changed = supported.clone();
        changed.audio_sample_rate = 3;
        assert!(supported_selection(&changed));
        assert!(same_encoder_settings(&supported, &changed)); // Output-only change.
        changed.audio_channels = 2;
        assert!(!supported_selection(&changed));
    }

    #[test]
    fn supported_configs_are_well_formed() {
        let rec = tlv8::parse(&supported_camera_recording_config());
        assert!(tlv8::find(&rec, 0x01).is_some());
        assert_eq!(tlv8::find(&rec, 0x02).map(|v| v.len()), Some(8));

        let video = tlv8::parse(&supported_video_recording_config(&[
            (3840, 2160, 15),
            (1920, 1080, 15),
        ]));
        let codec = tlv8::parse(tlv8::find(&video, 0x01).unwrap());
        assert_eq!(tlv8::find_u8(&codec, 0x01), Some(0));

        let audio = tlv8::parse(&supported_audio_recording_config());
        assert!(tlv8::find(&audio, 0x01).is_some());
    }
}
