//! HSV recording state: supported/selected recording configuration TLVs,
//! persistence, and camera encoder adjustment so recordings are stream-copied.

use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;

use crate::amcrest::AmcrestClient;
use crate::hsv::fmp4::{Recorder, RecorderConfig};
use crate::metrics::Metrics;
use crate::tlv8;

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
    pub recording_active: AtomicBool,
    pub audio_active: AtomicBool,
    pub event_snapshots: AtomicBool,
    pub periodic_snapshots: AtomicBool,
    pub homekit_active: AtomicBool,
    pub selected: Mutex<Option<SelectedConfig>>,
    pub recorder: Recorder,
    pub motion_active: Arc<AtomicBool>,
    camera: AmcrestClient,
    path: PathBuf,
}

impl HsvState {
    pub fn load(
        data_dir: &str,
        camera: AmcrestClient,
        motion_active: Arc<AtomicBool>,
        metrics: Arc<Metrics>,
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

        let selected = persisted.selected_b64.as_ref().and_then(|b64| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(b64)
                .ok()
                .and_then(|raw| parse_selected(&raw))
        });

        Arc::new(Self {
            recording_active: AtomicBool::new(persisted.recording_active),
            audio_active: AtomicBool::new(persisted.audio_active),
            event_snapshots: AtomicBool::new(persisted.event_snapshots),
            periodic_snapshots: AtomicBool::new(persisted.periodic_snapshots),
            homekit_active: AtomicBool::new(persisted.homekit_active),
            selected: Mutex::new(selected),
            recorder: Recorder::new(metrics),
            motion_active,
            camera,
            path,
        })
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

    /// Handles a SelectedCameraRecordingConfiguration write: parse, persist,
    /// reconfigure the camera encoder, restart the recording pipeline.
    pub async fn handle_selected_write(self: &Arc<Self>, raw: Vec<u8>) {
        let Some(config) = parse_selected(&raw) else {
            warn!("could not parse selected recording configuration");
            return;
        };
        info!(
            "selected recording config: {}x{}@{} {}kbps, iframe {}ms, fragment {}ms, prebuffer {}ms, audio rate code {}",
            config.width,
            config.height,
            config.fps,
            config.video_bitrate_kbps,
            config.iframe_interval_ms,
            config.fragment_ms,
            config.prebuffer_ms,
            config.audio_sample_rate,
        );

        // The camera may reset its RTSP encoder when these settings change.
        // Stop ffmpeg first, then restart it against the configured stream.
        self.recorder.stop().await;
        self.apply_camera_settings(&config).await;
        *self.selected.lock().await = Some(config);
        self.persist().await;
        self.sync_recorder().await;
    }

    /// Restores persisted camera encoder settings before resuming recording.
    pub async fn resume_recorder(self: &Arc<Self>) {
        if let Some(config) = self.selected.lock().await.clone() {
            self.apply_camera_settings(&config).await;
        }
        self.sync_recorder().await;
    }

    /// The value returned for reads of SelectedCameraRecordingConfiguration.
    pub async fn selected_read(&self) -> Option<Vec<u8>> {
        self.selected.lock().await.as_ref().map(|s| s.raw.clone())
    }

    /// Adjusts the camera's main-stream encoder to exactly match the selected
    /// recording configuration, so ffmpeg can stream-copy.
    async fn apply_camera_settings(&self, config: &SelectedConfig) {
        let gop = config.fps as u32 * config.iframe_interval_ms / 1000;
        let audio_hz: u32 = match config.audio_sample_rate {
            0 => 8000,
            1 => 16000,
            2 => 24000,
            3 => 32000,
            4 => 44100,
            _ => 48000,
        };
        let params = format!(
            "Encode[0].MainFormat[0].VideoEnable=true\
             &Encode[0].MainFormat[0].Video.Compression=H.264\
             &Encode[0].MainFormat[0].Video.resolution={w}x{h}\
             &Encode[0].MainFormat[0].Video.Width={w}\
             &Encode[0].MainFormat[0].Video.Height={h}\
             &Encode[0].MainFormat[0].Video.FPS={fps}\
             &Encode[0].MainFormat[0].Video.GOP={gop}\
             &Encode[0].MainFormat[0].Video.BitRate={bitrate}\
             &Encode[0].MainFormat[0].Video.BitRateControl=VBR\
             &Encode[0].MainFormat[0].Video.Profile=Main\
             &Encode[0].MainFormat[0].Video.Quality=4\
             &Encode[0].MainFormat[0].Video.Pack=DHAV\
             &Encode[0].MainFormat[0].Video.Priority=0\
             &Encode[0].MainFormat[0].Video.SVCTLayer=1\
             &Encode[0].MainFormat[0].Video.encodeType=0\
             &Encode[0].MainFormat[0].Audio.Compression=AAC\
             &Encode[0].MainFormat[0].Audio.Frequency={audio_hz}\
             &Encode[0].MainFormat[0].Audio.Bitrate=64\
             &Encode[0].MainFormat[0].Audio.Depth=16\
             &Encode[0].MainFormat[0].Audio.Channels[0]=0\
             &Encode[0].MainFormat[0].Audio.Mode=0\
             &Encode[0].MainFormat[0].Audio.Pack=DHAV\
             &Encode[0].MainFormat[0].AudioEnable=true",
            w = config.width,
            h = config.height,
            fps = config.fps,
            gop = gop,
            bitrate = config.video_bitrate_kbps,
        );
        match self.camera.set_config(&params).await {
            Ok(()) => {
                info!(
                    "camera main stream set to {}x{}@{} GOP {} for recording",
                    config.width, config.height, config.fps, gop
                );
                // Several Amcrest firmware families reset MotionDetect event
                // actions when the main encoder is written. Detection must be
                // the final camera profile applied, including on later HomeKit
                // recording-configuration changes.
                if let Err(e) = self.camera.ensure_smart_motion().await {
                    warn!("failed to restore AI/motion profile after encoder config: {e}");
                }
            }
            Err(e) => warn!("failed to set camera encoder config: {e}"),
        }
    }

    /// Starts or stops the recording pipeline according to the current state.
    pub async fn sync_recorder(self: &Arc<Self>) {
        let active = self.recording_active.load(Ordering::SeqCst)
            && self.homekit_active.load(Ordering::SeqCst);
        let selected = self.selected.lock().await.clone();
        match (active, selected) {
            (true, Some(config)) => {
                self.recorder
                    .start(RecorderConfig {
                        rtsp_url: self.camera.rtsp_url(0),
                        audio: self.audio_active.load(Ordering::SeqCst),
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
        if self.selected.lock().await.is_none() {
            return Err(9); // INVALID_CONFIGURATION
        }
        if !self.recording_active.load(Ordering::SeqCst)
            || !self.homekit_active.load(Ordering::SeqCst)
        {
            return Err(1); // NOT_ALLOWED
        }
        Ok(())
    }
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

/// Recording resolutions we can configure the camera's main stream to.
/// (fps 15: the 4K sensors on these cameras top out at 15 fps.)
const RECORDING_ATTRIBUTES: &[(u16, u16, u8)] = &[
    (1280, 720, 15),
    (1920, 1080, 15),
    (2688, 1520, 15),
    (3840, 2160, 15),
];

pub fn supported_video_recording_config() -> Vec<u8> {
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
    for (i, &(w, h, fps)) in RECORDING_ATTRIBUTES.iter().enumerate() {
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
    // AAC-LC mono; the camera can be set to 32 or 48 kHz.
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
    }

    #[test]
    fn supported_configs_are_well_formed() {
        let rec = tlv8::parse(&supported_camera_recording_config());
        assert!(tlv8::find(&rec, 0x01).is_some());
        assert_eq!(tlv8::find(&rec, 0x02).map(|v| v.len()), Some(8));

        let video = tlv8::parse(&supported_video_recording_config());
        let codec = tlv8::parse(tlv8::find(&video, 0x01).unwrap());
        assert_eq!(tlv8::find_u8(&codec, 0x01), Some(0));

        let audio = tlv8::parse(&supported_audio_recording_config());
        assert!(tlv8::find(&audio, 0x01).is_some());
    }
}
