//! Deterministic native acquisition failures, not simulated media. No camera access.
use amcrust::{
    ingest::CameraUrl,
    source::{
        Encoder, FaultKind, Limits, RecoveryPhase, Registry, SourceKey, SubscriptionLimits,
        Terminal, Tracks,
    },
};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn circuit_cools_down_without_reviving_old_subscriptions_or_opening_idle_sources() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket); // A real connection reset during RTSP setup.
        }
    });
    let mut registry = Registry::new(Limits {
        retry_initial: Duration::from_millis(5),
        retry_max: Duration::from_millis(10),
        retry_cooldown: Duration::from_millis(250),
        failures_before_reset: 1,
        ..Default::default()
    })
    .unwrap();
    let source = registry
        .register(
            SourceKey {
                camera: 1,
                encoder: Encoder::Main,
            },
            CameraUrl::new(format!("rtsp://{address}/offline")).unwrap(),
        )
        .unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(10), async {
        let mut old = Vec::new();
        for attempt in 1..=3 {
            let mut subscription = source
                .subscribe(Tracks::Video, SubscriptionLimits::default())
                .await
                .unwrap();
            let reason = subscription
                .recv()
                .await
                .err()
                .expect("offline source must fail");
            assert!(matches!(
                reason,
                Terminal::Source(_) | Terminal::WorkerFailed
            ));
            assert!(matches!(
                source
                    .subscribe(Tracks::Video, SubscriptionLimits::default())
                    .await,
                Err(Terminal::RetryExhausted)
            ));
            let recovery = source.snapshot().recovery;
            assert_eq!(recovery.phase, RecoveryPhase::Cooldown);
            assert_eq!(recovery.consecutive_failures, 1);
            assert!(recovery.retry_remaining_ms > 0 && recovery.retry_remaining_ms <= 250);
            assert_eq!(recovery.cooldowns, attempt);
            assert_eq!(recovery.cooldown_resets, attempt - 1);
            assert!(matches!(
                recovery.last_fault,
                Some(FaultKind::Native | FaultKind::Worker)
            ));
            old.push((subscription, reason));
            assert_eq!(source.snapshot().open_attempts, attempt);
            if attempt == 3 {
                source.set_enabled(false).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(325)).await;
            assert_eq!(
                source.snapshot().open_attempts,
                attempt,
                "cooldown must not acquire without fresh demand"
            );
            let recovery = source.snapshot().recovery;
            assert_eq!(
                recovery.phase,
                if attempt == 3 {
                    RecoveryPhase::Disabled
                } else {
                    RecoveryPhase::Idle
                }
            );
            assert_eq!(recovery.consecutive_failures, 0);
            assert_eq!(recovery.retry_remaining_ms, 0);
            assert_eq!(recovery.cooldown_resets, attempt);
            assert!(recovery.last_fault.is_some()); // historical fault is not erased by cooldown
            for (subscription, reason) in &mut old {
                assert_eq!(subscription.terminal(), Some(*reason));
                assert!(matches!(subscription.recv().await, Err(e) if e == *reason));
            }
        }
        source.set_enabled(false).await.unwrap();
        assert_eq!(source.snapshot().recovery.phase, RecoveryPhase::Disabled);
        tokio::time::sleep(Duration::from_millis(325)).await;
        assert!(matches!(
            source
                .subscribe(Tracks::Video, SubscriptionLimits::default())
                .await,
            Err(Terminal::Disabled)
        ));
        assert_eq!(source.snapshot().open_attempts, 3);
        drop(old);
    })
    .await;
    registry.shutdown().await.unwrap();
    peer.abort();
    let _ = peer.await;
    outcome.expect("bounded repeated recovery");
    assert_eq!(source.snapshot().charged_bytes, 0);
    assert_eq!(source.snapshot().recovery.phase, RecoveryPhase::Shutdown);
}
