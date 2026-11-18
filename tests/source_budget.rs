//! Aggregate/per-source admission pressure using only loopback stalled RTSP opens.
use amcrust::{
    ingest::CameraUrl,
    source::{Encoder, Limits, Registry, SourceKey, SubscriptionLimits, Terminal, Tracks},
};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_retired_subscriptions_remain_charged_across_encoders_and_shutdown() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut registry = Registry::new(Limits {
        subscribers: 2,
        subscribers_per_source: 1,
        ..Default::default()
    })
    .unwrap();
    let observer = registry.budget_observer();
    let mut sources = Vec::new();
    for camera in 1..=3 {
        sources.push(
            registry
                .register(
                    SourceKey {
                        camera,
                        encoder: Encoder::Main,
                    },
                    CameraUrl::new(format!("rtsp://{}/stalled", listener.local_addr().unwrap()))
                        .unwrap(),
                )
                .unwrap(),
        );
    }
    let outcome = tokio::time::timeout(Duration::from_secs(10), async {
        let first = sources[0]
            .subscribe(Tracks::Video, SubscriptionLimits::default())
            .await
            .unwrap();
        let (first_peer, _) = listener.accept().await.unwrap();
        assert!(matches!(
            sources[0]
                .subscribe(Tracks::Video, SubscriptionLimits::default())
                .await,
            Err(Terminal::Budget)
        ));
        assert_eq!(sources[0].snapshot().subscription_budget_refusals, 1);
        let second = sources[1]
            .subscribe(Tracks::Video, SubscriptionLimits::default())
            .await
            .unwrap();
        let (second_peer, _) = listener.accept().await.unwrap();
        assert_eq!(observer.snapshot().reserved_subscribers, 2);
        sources[0].set_enabled(false).await.unwrap();
        assert_eq!(first.terminal(), Some(Terminal::Disabled));
        assert_eq!(sources[0].snapshot().subscribers, 0);
        assert_eq!(sources[0].snapshot().reserved_subscribers, 1);
        assert!(matches!(
            sources[2]
                .subscribe(Tracks::Video, SubscriptionLimits::default())
                .await,
            Err(Terminal::Budget)
        ));
        assert_eq!(sources[2].snapshot().reserved_subscribers, 0); // partial local reservation rolled back
        assert_eq!(sources[2].snapshot().subscription_budget_refusals, 1);
        assert_eq!(observer.snapshot().subscription_budget_refusals, 2);
        assert!(second.terminal().is_none()); // budget refusal did not retire another owner
        assert!(sources[1].snapshot().active);
        assert_eq!(sources[2].snapshot().open_attempts, 0);
        drop(first);
        assert_eq!(sources[0].snapshot().reserved_subscribers, 0);
        assert_eq!(observer.snapshot().reserved_subscribers, 1);
        let third = sources[2]
            .subscribe(Tracks::Video, SubscriptionLimits::default())
            .await
            .unwrap();
        let (third_peer, _) = listener.accept().await.unwrap();
        registry.shutdown().await.unwrap();
        assert_eq!(second.terminal(), Some(Terminal::Shutdown));
        assert_eq!(third.terminal(), Some(Terminal::Shutdown));
        assert_eq!(observer.snapshot().reserved_subscribers, 2);
        drop((second, third, first_peer, second_peer, third_peer));
        assert_eq!(observer.snapshot().reserved_subscribers, 0);
        assert_eq!(observer.snapshot().charged_bytes, 0);
    })
    .await;
    registry.shutdown().await.unwrap();
    outcome.expect("bounded local budget pressure");
    drop((sources, registry));
    assert_eq!(observer.snapshot().reserved_subscribers, 0);
    assert_eq!(observer.snapshot().subscriber_limit, 2);
    assert_eq!(observer.snapshot().subscription_budget_refusals, 2);
}
