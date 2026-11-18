//! Admission/lifetime checks with local UDP sockets and controlled owners; no camera.
use super::privacy_tests::{setup, start, status};
use super::*;

fn fixture() -> (amcrust::source::Registry, StreamManager) {
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
    (registry, manager)
}
async fn setup_status(manager: &StreamManager) -> u8 {
    tlv8::find_u8(
        &tlv8::parse(&manager.setup_read().await.unwrap()),
        SETUP_STATUS,
    )
    .unwrap()
}

#[tokio::test]
async fn concurrent_preparations_are_bounded_and_expired_keys_cannot_start() {
    let (mut registry, manager) = fixture();
    let mut tasks = tokio::task::JoinSet::new();
    for id in 0..32 {
        let manager = manager.clone();
        tasks.spawn(async move {
            manager.handle_setup_write(setup(&[id; 16])).await;
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let counts = manager.resources().owners.unwrap();
    assert_eq!(counts.prepared_sessions, MAX_PREPARED_SESSIONS);
    assert_eq!(counts.setup_busy, 16);
    assert_eq!(status(&manager).await, 1);
    let id = manager
        .inner
        .lock()
        .await
        .sessions
        .keys()
        .next()
        .unwrap()
        .clone();
    manager.handle_setup_write(setup(&id)).await; // replace without needing another slot
    assert_eq!(setup_status(&manager).await, 0);
    assert_eq!(
        manager.resources().owners.unwrap().prepared_sessions,
        MAX_PREPARED_SESSIONS
    );
    let ports = {
        let mut inner = manager.inner.lock().await;
        let session = inner.sessions.get_mut(&id).unwrap();
        session.prepared_at = std::time::Instant::now() - PREPARED_LIFETIME;
        [
            session
                .video_out_socket
                .as_ref()
                .unwrap()
                .local_addr()
                .unwrap(),
            session
                .audio_out_socket
                .as_ref()
                .unwrap()
                .local_addr()
                .unwrap(),
        ]
    };
    assert!(manager.setup_read().await.is_none()); // stale success/keys cleared
    assert!(manager.handle_selected_write(start(&id)).await.is_err());
    for port in ports {
        drop(tokio::net::UdpSocket::bind(port).await.unwrap());
    }
    assert_eq!(manager.resources().owners.unwrap().expired_prepared, 1);
    assert_eq!(status(&manager).await, 0);
    manager.handle_setup_write(setup(&[99; 16])).await;
    assert_eq!(setup_status(&manager).await, 0);
    // Start itself must enforce expiry, not depend on a preceding read/status call.
    manager
        .inner
        .lock()
        .await
        .sessions
        .get_mut(&vec![99; 16])
        .unwrap()
        .prepared_at = std::time::Instant::now() - PREPARED_LIFETIME;
    assert!(
        manager
            .handle_selected_write(start(&[99; 16]))
            .await
            .is_err()
    );
    assert_eq!(manager.audio_source.snapshot().open_attempts, 0);
    manager.stop_stream().await;
    assert_eq!(manager.resources().owners.unwrap().prepared_sessions, 0);
    registry.shutdown().await.unwrap();
}

fn owner(
    source: &amcrust::source::Source,
) -> (
    MediaProxy,
    tokio::sync::oneshot::Sender<()>,
    shared::Control,
) {
    let (release, wait) = tokio::sync::oneshot::channel();
    let control = shared::Control::pending();
    (
        MediaProxy {
            source: source.clone(),
            transport: control.clone(),
            task: Some(tokio::spawn(async move {
                let _ = wait.await;
            })),
        },
        release,
        control,
    )
}

#[tokio::test]
async fn live_budget_counts_both_tracks_once_and_reclaims_only_completed_owners() {
    let (mut registry, manager) = fixture();
    manager.handle_setup_write(setup(&[99; 16])).await;
    let mut releases = Vec::new();
    let mut controls = Vec::new();
    {
        let mut inner = manager.inner.lock().await;
        for id in 0..MAX_LIVE_SESSIONS {
            let (owner, release, control) = owner(&manager.audio_source);
            inner.video_proxies.insert(vec![id as u8; 16], owner);
            releases.push(release);
            controls.push(control);
        }
    }
    let (audio, audio_release, audio_control) = owner(&manager.audio_source);
    manager
        .inner
        .lock()
        .await
        .audio_proxies
        .insert(vec![0; 16], audio);
    assert_eq!(
        manager.resources().owners.unwrap().live_sessions,
        MAX_LIVE_SESSIONS
    );
    assert_eq!(status(&manager).await, 1);
    assert!(
        manager
            .handle_selected_write(start(&[99; 16]))
            .await
            .is_err()
    );
    assert_eq!(manager.resources().owners.unwrap().start_busy, 1);
    manager.handle_setup_write(setup(&[100; 16])).await;
    assert_eq!(setup_status(&manager).await, 1);
    // Same-ID preparations do not revoke the running owner or consume a live slot.
    manager.handle_setup_write(setup(&[0; 16])).await;
    assert_eq!(setup_status(&manager).await, 0);
    assert!(controls.iter().all(|control| !control.is_closed()));
    assert!(!audio_control.is_closed());
    releases.remove(0).send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            assert_eq!(status(&manager).await, 1); // audio still owns this session
            if manager.resources().owners.unwrap().video == MAX_LIVE_SESSIONS - 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(controls[0].is_closed());
    assert!(!audio_control.is_closed());
    audio_release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while status(&manager).await != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(audio_control.is_closed());
    assert_eq!(
        manager.resources().owners.unwrap().live_sessions,
        MAX_LIVE_SESSIONS - 1
    );
    manager.handle_setup_write(setup(&[100; 16])).await;
    assert_eq!(setup_status(&manager).await, 0);
    assert_eq!(manager.audio_source.snapshot().open_attempts, 0);
    for release in releases {
        release.send(()).unwrap();
    }
    manager.stop_stream().await;
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_or_oversized_setup_cannot_retain_endpoints() {
    let (mut registry, manager) = fixture();
    for payload in [
        setup(&[1; 15]),
        setup(&[1; 17]),
        vec![0; MAX_SETUP_BYTES + 1],
    ] {
        manager.handle_setup_write(payload).await;
        assert_eq!(setup_status(&manager).await, 2);
        assert_eq!(manager.resources().owners.unwrap().prepared_sessions, 0);
    }
    assert_eq!(manager.audio_source.snapshot().open_attempts, 0);
    manager.stop_stream().await;
    registry.shutdown().await.unwrap();
}
