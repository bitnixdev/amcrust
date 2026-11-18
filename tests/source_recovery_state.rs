//! Recovery telemetry from real local RTSP setup failure/stall, without camera access.
use amcrust::{
    ingest::CameraUrl,
    source::{
        Encoder, Limits, RecoveryPhase, Registry, SourceKey, SubscriptionLimits, Terminal, Tracks,
    },
};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opening_backoff_reset_and_privacy_are_distinct_observations() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut registry = Registry::new(Limits {
        retry_initial: Duration::from_secs(2),
        retry_max: Duration::from_secs(2),
        retry_cooldown: Duration::from_secs(2),
        failures_before_reset: 2,
        ..Default::default()
    })
    .unwrap();
    let source = registry
        .register(
            SourceKey {
                camera: 1,
                encoder: Encoder::Main,
            },
            CameraUrl::new(format!("rtsp://{}/offline", listener.local_addr().unwrap())).unwrap(),
        )
        .unwrap();
    assert_eq!(source.snapshot().recovery.phase, RecoveryPhase::Idle);
    let outcome = tokio::time::timeout(Duration::from_secs(10), async {
        let mut failed = source
            .subscribe(Tracks::Video, SubscriptionLimits::default())
            .await
            .unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        assert_eq!(source.snapshot().recovery.phase, RecoveryPhase::Opening);
        drop(peer);
        assert!(matches!(
            failed.recv().await,
            Err(Terminal::Source(_) | Terminal::WorkerFailed)
        ));
        // Publication follows the joined fault, not the earlier permission revocation.
        while source.snapshot().recovery.phase != RecoveryPhase::Backoff {
            tokio::task::yield_now().await;
        }
        let recovery = source.snapshot().recovery;
        assert_eq!(recovery.consecutive_failures, 1);
        assert_eq!(recovery.cooldowns, 0);
        assert!(recovery.retry_remaining_ms > 0);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(source.snapshot().recovery.retry_remaining_ms < recovery.retry_remaining_ms);
        source.set_enabled(false).await.unwrap();
        assert_eq!(source.snapshot().recovery.phase, RecoveryPhase::Disabled);
        source.reset().await.unwrap();
        let reset = source.snapshot().recovery;
        assert_eq!(reset.phase, RecoveryPhase::Disabled);
        assert_eq!(reset.consecutive_failures, 0);
        assert_eq!(reset.retry_remaining_ms, 0);
        assert_eq!(reset.last_fault, recovery.last_fault);
        assert_eq!(reset.cooldown_resets, 0); // administrative reset is not a timed reset
        source.set_enabled(true).await.unwrap();
        assert_eq!(source.snapshot().recovery.phase, RecoveryPhase::Idle);
        assert_eq!(source.snapshot().open_attempts, 1);
        assert!(failed.terminal().is_some());
        let mut fresh = source
            .subscribe(Tracks::Video, SubscriptionLimits::default())
            .await
            .unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        assert_eq!(source.snapshot().recovery.phase, RecoveryPhase::Opening);
        source.set_enabled(false).await.unwrap(); // cancel and join a genuinely stalled native open
        assert_eq!(fresh.recv().await.err(), Some(Terminal::Disabled));
        let disabled = source.snapshot();
        assert_eq!(disabled.recovery.phase, RecoveryPhase::Disabled);
        assert!(!disabled.active);
        assert_eq!(disabled.faults, 1); // privacy cancellation is not an acquisition fault
        assert_eq!(disabled.open_attempts, 2);
        drop(peer);
    })
    .await;
    registry.shutdown().await.unwrap();
    outcome.expect("bounded local failure/retirement");
    assert_eq!(source.snapshot().recovery.phase, RecoveryPhase::Shutdown);
    assert_eq!(source.snapshot().recovery.retry_remaining_ms, 0);
}
