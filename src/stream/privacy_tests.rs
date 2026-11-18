//! Local privacy/lifetime checks; never connect to a real camera or change settings.
use super::*;
use crate::hsv::recording::{HsvState, SelectedConfig};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

fn setup(id: &[u8]) -> Vec<u8> {
    setup_at(id, 9, 9)
}
fn setup_at(id: &[u8], video_port: u16, audio_port: u16) -> Vec<u8> {
    let address = tlv8::Writer::new()
        .bytes(ADDR_IP, b"127.0.0.1")
        .u16(ADDR_VIDEO_PORT, video_port)
        .u16(ADDR_AUDIO_PORT, audio_port)
        .build();
    let crypto = tlv8::Writer::new()
        .bytes(SRTP_MASTER_KEY, &[7; 16])
        .bytes(SRTP_MASTER_SALT, &[8; 14])
        .build();
    tlv8::Writer::new()
        .bytes(SETUP_SESSION_ID, id)
        .bytes(SETUP_ADDRESS, &address)
        .bytes(SETUP_VIDEO_SRTP, &crypto)
        .bytes(SETUP_AUDIO_SRTP, &crypto)
        .build()
}
fn start(id: &[u8]) -> Vec<u8> {
    let control = tlv8::Writer::new()
        .bytes(CONTROL_SESSION_ID, id)
        .u8(CONTROL_COMMAND, COMMAND_START)
        .build();
    tlv8::Writer::new()
        .bytes(SELECTED_SESSION_CONTROL, &control)
        .build()
}
async fn status(manager: &StreamManager) -> u8 {
    tlv8::find_u8(&tlv8::parse(&manager.streaming_status().await), 1).unwrap()
}
struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("amcrust-privacy-{:016x}", rand::random::<u64>()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0.join("hsv.json"));
        let _ = std::fs::remove_dir(&self.0);
    }
}
fn state(
    source: amcrust::source::Source,
    directory: &Directory,
    metrics: Arc<Metrics>,
) -> Arc<HsvState> {
    HsvState::load(
        "privacy-test".into(),
        directory.0.to_str().unwrap(),
        crate::amcrest::AmcrestClient::new("127.0.0.1".into(), "unused".into(), "unused".into()),
        Arc::new(AtomicBool::new(false)),
        metrics,
        crate::amcrest::EncoderCapabilities::default(),
        source,
    )
}
fn selected() -> SelectedConfig {
    SelectedConfig {
        raw: vec![],
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn camera_off_joins_all_encoders_and_recording_then_requires_fresh_setup() {
    use amcrust::source::{SubscriptionLimits, Terminal, Tracks};
    let main_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let other_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut registry = super::tests::registry();
    let main = super::tests::register(
        &mut registry,
        1,
        format!("rtsp://{}/stalled", main_listener.local_addr().unwrap()),
    );
    let other = super::tests::register(
        &mut registry,
        2,
        format!("rtsp://{}/stalled", other_listener.local_addr().unwrap()),
    );
    let metrics = Metrics::new();
    let manager = StreamManager::new(
        vec![
            VideoSource {
                source: main.clone(),
                profile: VideoProfile::LIVE_1080P,
            },
            VideoSource {
                source: other.clone(),
                profile: VideoProfile::LIVE_720P,
            },
        ],
        main.clone(),
        true,
        Ipv4Addr::LOCALHOST.into(),
        metrics.clone(),
    );
    let directory = Directory::new();
    let hsv = state(main.clone(), &directory, metrics.clone());
    *hsv.selected.lock().await = Some(selected());
    hsv.recording_active.store(true, Ordering::SeqCst);
    hsv.sync_recorder().await;
    let (main_peer, _) = tokio::time::timeout(Duration::from_secs(2), main_listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut other_subscription = other
        .subscribe(Tracks::Video, SubscriptionLimits::default())
        .await
        .unwrap();
    let (other_peer, _) = tokio::time::timeout(Duration::from_secs(2), other_listener.accept())
        .await
        .unwrap()
        .unwrap();
    manager.handle_setup_write(setup(&[1; 16])).await;
    manager.handle_selected_write(start(&[1; 16])).await;
    manager.handle_setup_write(setup(&[2; 16])).await;
    assert!(hsv.recorder.is_running().await);
    {
        let inner = manager.inner.lock().await;
        assert_eq!(inner.video_proxies.len(), 1);
        assert_eq!(inner.audio_proxies.len(), 1);
        assert_eq!(inner.sessions.len(), 1);
    }
    tokio::time::timeout(
        Duration::from_secs(3),
        hsv.set_camera_active(&manager, false),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(status(&manager).await, 2);
    assert!(!hsv.recorder.is_running().await);
    assert!(manager.setup_read().await.is_none());
    assert_eq!(
        other_subscription.recv().await.err(),
        Some(Terminal::Disabled)
    );
    for source in [&main, &other] {
        assert!(!source.snapshot().active);
        assert_eq!(source.snapshot().charged_bytes, 0);
        assert_eq!(
            source
                .subscribe(Tracks::Video, SubscriptionLimits::default())
                .await
                .err(),
            Some(Terminal::Disabled)
        );
    }
    {
        let inner = manager.inner.lock().await;
        assert!(
            inner.sessions.is_empty()
                && inner.video_proxies.is_empty()
                && inner.audio_proxies.is_empty()
        );
    }
    manager.handle_setup_write(setup(&[3; 16])).await;
    assert_eq!(
        tlv8::find_u8(
            &tlv8::parse(&manager.setup_read().await.unwrap()),
            SETUP_STATUS
        ),
        Some(2)
    );
    let loaded = state(main.clone(), &directory, metrics.clone());
    assert!(!loaded.homekit_active.load(Ordering::SeqCst));

    // Re-enable without recording demand. No old prepared or running session revives.
    hsv.recording_active.store(false, Ordering::SeqCst);
    hsv.set_camera_active(&manager, true).await.unwrap();
    assert_eq!(status(&manager).await, 0);
    for id in [1, 2, 3] {
        manager.handle_selected_write(start(&[id; 16])).await;
    }
    assert_eq!(main.snapshot().open_attempts, 1);
    assert_eq!(other.snapshot().open_attempts, 1);
    manager.handle_setup_write(setup(&[4; 16])).await;
    manager.handle_selected_write(start(&[4; 16])).await;
    let (new_peer, _) = tokio::time::timeout(Duration::from_secs(2), main_listener.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(main.snapshot().open_attempts, 2);
    manager.stop_stream().await;
    assert_eq!(manager.set_enabled(true).await, Err(Terminal::Shutdown));
    manager.handle_setup_write(setup(&[5; 16])).await;
    assert!(manager.inner.lock().await.sessions.is_empty());
    hsv.shutdown().await;
    assert_eq!(
        hsv.set_camera_active(&manager, true).await,
        Err(Terminal::Shutdown)
    );
    registry.shutdown().await.unwrap();
    drop((main_peer, other_peer, new_peer, other_subscription));
    assert_eq!(manager.audio_pool.reserved_bytes(), 0);
    assert!(
        metrics
            .render(false)
            .contains("amcrust_connections_open{type=\"live_stream\"} 0")
    );
}

#[tokio::test]
async fn snapshot_mode_changes_never_revive_pending_permissions() {
    let mut registry = super::tests::registry();
    let source = super::tests::register(&mut registry, 1, "rtsp://127.0.0.1:9/unused".into());
    let directory = Directory::new();
    let hsv = state(source, &directory, Metrics::new());
    let pending = hsv.snapshots.permission();
    hsv.set_snapshot_active(true, false).await;
    assert!(!pending.current());
    assert!(hsv.snapshots.permission().current()); // events still allowed
    let event = hsv.snapshots.permission();
    hsv.set_snapshot_active(false, false).await;
    assert!(!event.current());
    assert!(!hsv.snapshots.permission().current());
    hsv.set_snapshot_active(true, true).await;
    assert!(hsv.snapshots.permission().current());
    assert!(!pending.current() && !event.current());
    hsv.shutdown().await;
    hsv.set_snapshot_active(false, true).await;
    assert!(!hsv.snapshots.permission().current());
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_teardown_keeps_join_ownership_and_shutdown_admission_closed() {
    let mut registry = super::tests::registry();
    let source = super::tests::register(&mut registry, 1, "rtsp://127.0.0.1:9/unused".into());
    let manager = StreamManager::new(
        vec![VideoSource {
            source: source.clone(),
            profile: VideoProfile::LIVE_1080P,
        }],
        source,
        false,
        Ipv4Addr::LOCALHOST.into(),
        Metrics::new(),
    );
    let (release, wait) = tokio::sync::oneshot::channel();
    let control = shared::Control::pending();
    let owner = MediaProxy {
        transport: control.clone(),
        task: Some(tokio::spawn(async move {
            let _ = wait.await;
        })),
    };
    manager
        .inner
        .lock()
        .await
        .video_proxies
        .insert(vec![1], owner);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), manager.stop_stream())
            .await
            .is_err()
    );
    assert!(control.is_closed());
    {
        let inner = manager.inner.lock().await;
        assert!(inner.shutdown && inner.disabled);
        assert!(inner.video_proxies.get(&vec![1]).unwrap().task.is_some());
    }
    assert_eq!(
        manager.set_enabled(true).await,
        Err(amcrust::source::Terminal::Shutdown)
    );
    release.send(()).unwrap();
    manager.stop_stream().await;
    assert!(manager.inner.lock().await.video_proxies.is_empty());
    registry.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real north-driveway camera: serial read-only shared privacy off/on; no encoder writes or captures"]
async fn real_camera_privacy_roundtrip_requires_fresh_media() {
    use crate::hsv::fmp4::Message;
    use webrtc_srtp::{context::Context, protection_profile::ProtectionProfile};
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").unwrap();
    let password = std::env::var("AMCREST_PASSWORD").unwrap();
    let mut registry = super::tests::registry();
    let source = super::tests::register(
        &mut registry,
        1,
        format!("rtsp://{user}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype=0"),
    );
    let metrics = Metrics::new();
    let manager = StreamManager::new(
        vec![VideoSource {
            source: source.clone(),
            profile: VideoProfile::LIVE_1080P,
        }],
        source.clone(),
        true,
        Ipv4Addr::LOCALHOST.into(),
        metrics.clone(),
    );
    let directory = Directory::new();
    let hsv = state(source.clone(), &directory, metrics);
    *hsv.selected.lock().await = Some(selected());
    hsv.recording_active.store(true, Ordering::SeqCst);
    let result = tokio::time::timeout(Duration::from_secs(75), async {
        let mut previous_generation = 0;
        for cycle in 1..=2u8 {
            hsv.set_camera_active(&manager, true).await.unwrap();
            let video = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let audio = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            manager.handle_setup_write(setup_at(&[cycle; 16], video.local_addr().unwrap().port(), audio.local_addr().unwrap().port())).await;
            manager.handle_selected_write(start(&[cycle; 16])).await;
            let crypto = || Context::new(&[7; 16], &[8; 14], ProtectionProfile::Aes128CmHmacSha1_80, None, None).unwrap();
            let (mut video_crypto, mut audio_crypto) = (crypto(), crypto());
            let (mut video_packets, mut audio_packets) = (0, 0);
            let (mut video_buffer, mut audio_buffer) = ([0; 2048], [0; 2048]);
            while video_packets < 100 || audio_packets < 25 {
                tokio::select! {
                    n = video.recv(&mut video_buffer) => {
                        let n = n.unwrap();
                        if !(192..=223).contains(&video_buffer[1]) {
                            video_crypto.decrypt_rtp(&video_buffer[..n]).unwrap();
                            video_packets += 1;
                        }
                    },
                    n = audio.recv(&mut audio_buffer) => {
                        let n = n.unwrap();
                        if !(192..=223).contains(&audio_buffer[1]) {
                            audio_crypto.decrypt_rtp(&audio_buffer[..n]).unwrap();
                            audio_packets += 1;
                        }
                    }
                }
            }
            let mut clip = loop {
                if let Ok(clip) = hsv.recorder.open().await { break clip; }
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            let retained = loop {
                match clip.messages.recv().await {
                    Some(Message::Media(fragment)) => break fragment,
                    Some(Message::Init(_)) => {},
                    _ => panic!("recording ended before first media fragment"),
                }
            };
            assert!(retained.lease.current());
            let before = source.snapshot();
            assert_eq!(before.open_attempts, u64::from(cycle));
            assert!(before.generation > previous_generation);
            previous_generation = before.generation;
            hsv.set_camera_active(&manager, false).await.unwrap();
            assert_eq!(status(&manager).await, 2);
            assert!(!hsv.recorder.is_running().await);
            assert!(!source.snapshot().active);
            assert!(retained.lease.write(|| ()).is_err());
            assert_eq!(manager.audio_pool.reserved_bytes(), 0);
            clip.stop().await;
            let bytes = retained.data.len();
            drop((retained, clip));
            assert_eq!(source.snapshot().charged_bytes, 0);
            // A prepared response/session from before privacy is not reusable.
            manager.handle_selected_write(start(&[cycle; 16])).await;
            assert!(manager.inner.lock().await.video_proxies.is_empty());
            eprintln!("Privacy cycle {cycle}: {video_packets} authenticated video / {audio_packets} audio RTP; retained recording fragment {bytes} bytes refused; acquisition generation {previous_generation} joined, zero released charges");
        }
    }).await;
    manager.stop_stream().await;
    hsv.shutdown().await;
    registry.shutdown().await.unwrap();
    result.expect("camera privacy roundtrip deadline");
}
