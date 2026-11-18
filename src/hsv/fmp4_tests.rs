use super::*;

#[tokio::test]
async fn resource_snapshot_reports_contention_without_waiting_or_zeroing_history() {
    let mut registry = amcrust::source::Registry::new(Default::default()).unwrap();
    let source = registry
        .register(
            amcrust::source::SourceKey {
                camera: 1,
                encoder: amcrust::source::Encoder::Main,
            },
            amcrust::ingest::CameraUrl::new("rtsp://127.0.0.1:9/unused".into()).unwrap(),
        )
        .unwrap();
    let recorder = Recorder::new(source, Metrics::new());
    let guard = recorder.state.lock().await;
    let busy = recorder.resources();
    assert!(busy.history.is_none());
    assert_eq!(busy.aac.reserved_jobs, 0);
    assert_eq!(busy.mux.job_limit, 1);
    drop(guard);
    let idle = recorder.resources();
    assert_eq!(idle.history.unwrap().entries, 0);
    recorder.stop().await;
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_join_keeps_native_owner_reachable() {
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let producer = ProducerOwner(Mutex::new(Some(tokio::spawn(async move {
        wait.await.unwrap();
    }))));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), producer.join())
            .await
            .is_err()
    );
    assert!(producer.0.lock().await.is_some());
    release.send(()).unwrap();
    producer.join().await;
    assert!(producer.0.lock().await.is_none());

    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let clip = ClipOwner {
        lease: Lease::new(),
        task: Mutex::new(Some(tokio::spawn(async move {
            wait.await.unwrap();
            Ok::<_, String>(())
        }))),
        finished: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(10), clip.stop())
            .await
            .is_err()
    );
    assert!(!clip.lease.current());
    assert!(clip.task.lock().await.is_some());
    release.send(()).unwrap();
    clip.stop().await;
    assert!(clip.task.lock().await.is_none());
}
fn mp4_box(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
    let mut bytes = ((body.len() + 8) as u32).to_be_bytes().to_vec();
    bytes.extend_from_slice(kind);
    bytes.extend_from_slice(body);
    bytes
}
#[test]
fn incremental_boxes_preserve_init_media_and_completion() {
    let lease = Lease::new();
    let bytes = [
        mp4_box(b"ftyp", b"isom"),
        mp4_box(b"moov", &[]),
        mp4_box(b"moof", &[1, 2]),
        mp4_box(b"mdat", &[3, 4]),
        mp4_box(b"mfra", &[]),
    ]
    .concat();
    for stride in [1, 7, 8, 13, 1024] {
        let mut parser = Boxes::new();
        let mut messages = Vec::new();
        for chunk in bytes.chunks(stride) {
            messages.extend(parser.feed(chunk, &lease).unwrap());
        }
        assert!(parser.complete());
        assert_eq!(messages.len(), 2);
        assert!(matches!(&messages[0], Message::Init(fragment) if fragment.data.len() == 20));
        assert!(matches!(&messages[1], Message::Media(fragment) if fragment.data.len() == 20));
    }
}
#[test]
fn malformed_and_incomplete_boxes_are_not_completed_media() {
    let lease = Lease::new();
    for bytes in [
        mp4_box(b"mdat", &[]),
        [0; 8].to_vec(),
        [
            1u32.to_be_bytes().as_slice(),
            b"moof",
            u64::MAX.to_be_bytes().as_slice(),
        ]
        .concat(),
        [
            mp4_box(b"ftyp", &[]),
            mp4_box(b"moof", &[]),
            mp4_box(b"moof", &[]),
        ]
        .concat(),
    ] {
        assert!(Boxes::new().feed(&bytes, &lease).is_err());
    }
    let mut parser = Boxes::new();
    parser
        .feed(
            &[mp4_box(b"ftyp", &[]), mp4_box(b"moof", &[])].concat(),
            &lease,
        )
        .unwrap();
    assert!(!parser.complete());
}
fn config() -> RecorderConfig {
    RecorderConfig {
        width: 1920,
        height: 1080,
        fps: 15,
        audio: false,
        audio_sample_rate_hz: 48000,
        audio_bitrate: 64000,
        fragment_ms: 4000,
        prebuffer_ms: 4000,
    }
}
async fn stalled() -> (amcrust::source::Registry, Source, tokio::net::TcpListener) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut registry = amcrust::source::Registry::new(amcrust::source::Limits {
        timing: amcrust::source::Timing::Presentation,
        ..Default::default()
    })
    .unwrap();
    let source = registry
        .register(
            amcrust::source::SourceKey {
                camera: 1,
                encoder: amcrust::source::Encoder::Main,
            },
            amcrust::ingest::CameraUrl::new(format!(
                "rtsp://{}/main",
                listener.local_addr().unwrap()
            ))
            .unwrap(),
        )
        .unwrap();
    (registry, source, listener)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_joins_shared_recording_with_stalled_acquisition() {
    let (mut registry, source, listener) = stalled().await;
    let recorder = Recorder::new(source.clone(), Metrics::new());
    recorder.start(config()).await;
    let (_peer, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), recorder.stop())
        .await
        .unwrap();
    assert!(!recorder.is_running().await);
    assert!(recorder.open().await.is_err());
    tokio::time::timeout(Duration::from_secs(2), registry.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.snapshot().charged_bytes, 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn last_handle_drop_signals_shared_recording_owner() {
    let (mut registry, source, listener) = stalled().await;
    let recorder = Recorder::new(source.clone(), Metrics::new());
    recorder.start(config()).await;
    let (_peer, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let state = recorder.state.clone();
    drop(recorder);
    let owner = state.lock().await.owner.take().unwrap();
    tokio::time::timeout(Duration::from_secs(1), owner.join())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), registry.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.snapshot().charged_bytes, 0);
}
