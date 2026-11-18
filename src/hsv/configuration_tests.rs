use super::*;
use crate::stream::{StreamManager, VideoSource};
use amcrust::source::{
    Encoder, Limits, Registry, Source, SourceKey, SubscriptionLimits, Terminal, Timing, Tracks,
};
use std::time::Duration;

struct Fixture {
    registry: Registry,
    main: Source,
    other: Source,
    streams: StreamManager,
    hsv: Arc<HsvState>,
    directory: PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let mut registry = Registry::new(Limits {
            timing: Timing::Presentation,
            ..Default::default()
        })
        .unwrap();
        let mut register = |encoder| {
            registry
                .register(
                    SourceKey { camera: 1, encoder },
                    amcrust::ingest::CameraUrl::new("rtsp://127.0.0.1:9/unused".into()).unwrap(),
                )
                .unwrap()
        };
        let main = register(Encoder::Main);
        let other = register(Encoder::SubStream2);
        let metrics = Metrics::new();
        let streams = StreamManager::new(
            vec![
                VideoSource {
                    source: other.clone(),
                    profile: VideoProfile::LIVE_720P,
                },
                VideoSource {
                    source: main.clone(),
                    profile: VideoProfile::LIVE_1080P,
                },
            ],
            main.clone(),
            true,
            "127.0.0.1".parse().unwrap(),
            metrics.clone(),
        );
        let directory =
            std::env::temp_dir().join(format!("amcrust-config-{:016x}", rand::random::<u64>()));
        std::fs::create_dir(&directory).unwrap();
        let hsv = HsvState::load(
            "configuration-test".into(),
            directory.to_str().unwrap(),
            AmcrestClient::new("127.0.0.1:9".into(), "unused".into(), "unused".into()),
            Arc::new(AtomicBool::new(false)),
            metrics,
            EncoderCapabilities::default(),
            main.clone(),
        );
        *hsv.selected.lock().await = Some(config(1));
        Self {
            registry,
            main,
            other,
            streams,
            hsv,
            directory,
        }
    }
    async fn close(mut self) {
        self.streams.stop_stream().await;
        self.hsv.shutdown().await;
        self.registry.shutdown().await.unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.directory.join("hsv.json"));
        let _ = std::fs::remove_dir(&self.directory);
    }
}
fn config(id: u8) -> SelectedConfig {
    SelectedConfig {
        raw: vec![id],
        prebuffer_ms: 4000,
        fragment_ms: 4000,
        width: 1920,
        height: 1080,
        fps: 15,
        video_bitrate_kbps: 2000,
        iframe_interval_ms: 4000,
        audio_channels: 1,
        audio_sample_rate: 5,
        audio_bitrate_kbps: 64,
    }
}
async fn disabled(source: &Source) {
    assert_eq!(
        source
            .subscribe(Tracks::Video, SubscriptionLimits::default())
            .await
            .err(),
        Some(Terminal::Disabled)
    );
}

#[tokio::test]
async fn failed_settings_retire_before_write_and_same_selection_can_repair() {
    let f = Fixture::new().await;
    let old_snapshot = f.hsv.snapshots.permission();
    let mut next = config(2);
    next.width = 1280;
    next.height = 720;
    let result = f
        .hsv
        .reconcile_configuration(&f.streams, Some(next), false, |_| async {
            disabled(&f.main).await;
            assert!(!old_snapshot.current());
            assert!(!f.hsv.recorder.is_running().await);
            Err("injected camera settings failure".into())
        })
        .await;
    assert!(result.is_err());
    assert_eq!(f.hsv.selected_read().await, Some(vec![1]));
    assert_eq!(f.hsv.recording_allowed().await, Err(9));
    f.hsv.recording_active.store(true, Ordering::SeqCst);
    f.hsv.sync_recorder().await;
    assert!(!f.hsv.recorder.is_running().await);
    f.hsv.set_camera_active(&f.streams, false).await.unwrap();
    f.hsv.set_camera_active(&f.streams, true).await.unwrap();
    disabled(&f.main).await;
    assert!(!f.hsv.snapshots.permission().current());
    let unrelated = f
        .other
        .subscribe(Tracks::Video, SubscriptionLimits::default())
        .await
        .unwrap();
    drop(unrelated);
    // Repeating the previously accepted selection must not be discarded as an echo.
    let called = AtomicBool::new(false);
    f.hsv.recording_active.store(false, Ordering::SeqCst);
    f.hsv
        .reconcile_configuration(&f.streams, Some(config(1)), false, |_| async {
            called.store(true, Ordering::SeqCst);
            disabled(&f.main).await;
            Ok(())
        })
        .await
        .unwrap();
    assert!(called.load(Ordering::SeqCst));
    assert!(f.hsv.configuration_ready.load(Ordering::SeqCst));
    assert!(f.hsv.snapshots.permission().current());
    assert!(!old_snapshot.current());
    f.close().await;
}

#[tokio::test]
async fn recording_audio_change_does_not_mutate_or_retire_the_camera() {
    let f = Fixture::new().await;
    let permission = f.hsv.snapshots.permission();
    let advertised = f.streams.supported_video_config();
    let mut next = config(2);
    next.audio_sample_rate = 3;
    next.audio_bitrate_kbps = 96;
    f.hsv
        .reconcile_configuration(&f.streams, Some(next), false, |_| async {
            panic!("output-only recording change must not write camera settings");
            #[allow(unreachable_code)]
            Ok(())
        })
        .await
        .unwrap();
    assert!(permission.current());
    assert_eq!(f.streams.supported_video_config(), advertised);
    assert_eq!(f.main.snapshot().open_attempts, 0);
    assert_eq!(f.hsv.selected_read().await, Some(vec![2]));
    // A cancelled output-only reconciliation is not an uncertain encoder write.
    f.hsv.configuration_ready.store(false, Ordering::SeqCst);
    let same = f.hsv.selected.lock().await.clone();
    f.hsv
        .reconcile_configuration(&f.streams, same, false, |_| async {
            panic!("output-only retry must not reconfigure live acquisition");
            #[allow(unreachable_code)]
            Ok(())
        })
        .await
        .unwrap();
    assert!(permission.current());
    assert_eq!(f.main.snapshot().open_attempts, 0);
    f.close().await;
}

#[tokio::test]
async fn cancelled_configuration_stays_blocked_and_shutdown_cannot_reopen_it() {
    let f = Fixture::new().await;
    let mut next = config(2);
    next.fps = 30;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            f.hsv
                .reconcile_configuration(&f.streams, Some(next.clone()), false, |_| async {
                    std::future::pending::<Result<(), Box<dyn std::error::Error + Send + Sync>>>()
                        .await
                })
        )
        .await
        .is_err()
    );
    disabled(&f.main).await;
    assert!(!f.hsv.configuration_ready.load(Ordering::SeqCst));
    assert_eq!(f.hsv.selected_read().await, Some(vec![1]));
    let result = f
        .hsv
        .reconcile_configuration(&f.streams, Some(next), false, |_| async {
            f.streams.stop_stream().await;
            Ok(())
        })
        .await;
    assert!(result.is_err());
    assert_eq!(f.hsv.selected_read().await, Some(vec![1]));
    assert!(!f.hsv.configuration_ready.load(Ordering::SeqCst));
    f.close().await;
}

#[tokio::test]
async fn successful_transition_publishes_current_profile_without_reviving_snapshot_copies() {
    let f = Fixture::new().await;
    let old = f.streams.supported_video_config();
    let permission = f.hsv.snapshots.permission();
    let mut next = config(2);
    next.width = 1280;
    next.height = 720;
    f.hsv
        .reconcile_configuration(&f.streams, Some(next), false, |_| async {
            disabled(&f.main).await;
            assert_ne!(f.streams.supported_video_config(), old);
            assert!(!permission.current());
            Ok(())
        })
        .await
        .unwrap();
    assert_ne!(f.streams.supported_video_config(), old);
    assert_eq!(f.hsv.selected_main_profile().await.unwrap().width, 1280);
    assert!(!permission.current());
    assert!(f.hsv.snapshots.permission().current());
    f.close().await;
}
