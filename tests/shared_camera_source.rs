//! Opt-in real-camera lifecycle/fanout check. No fixture publisher, encoder writes,
//! captured media, or HomeKit acceptance claim.
use amcrust::{
    ingest::CameraUrl,
    source::{Encoder, Limits, Registry, SourceKey, SubscriptionLimits, Terminal, Tracks},
};
use std::time::Duration;

fn camera_url(subtype: u8) -> CameraUrl {
    dotenvy::dotenv().ok();
    let username = std::env::var("AMCREST_USERNAME").expect("AMCREST_USERNAME required");
    let password = std::env::var("AMCREST_PASSWORD").expect("AMCREST_PASSWORD required");
    CameraUrl::new(format!(
        "rtsp://{username}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype={subtype}"
    ))
    .unwrap()
}

// Inspect only this test process's actual established RTSP sockets. No command
// lines, environments, or media contents enter the evidence.
#[cfg(target_os = "macos")]
fn rtsp_connections() -> usize {
    let output = std::process::Command::new("/usr/sbin/lsof")
        .args([
            "-nP",
            "-a",
            "-p",
            &std::process::id().to_string(),
            "-iTCP@10.76.14.10:554",
            "-sTCP:ESTABLISHED",
            "-Fn",
        ])
        .output()
        .expect("lsof required for real socket count");
    assert!(output.status.success() || output.status.code() == Some(1));
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with('n') && line.ends_with("->10.76.14.10:554"))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires coordinated access to authorized camera and existing credentials"]
async fn real_shared_source() {
    tokio::time::timeout(Duration::from_secs(45), exercise())
        .await
        .expect("bounded real-camera check");
}
async fn exercise() {
    let mut registry = Registry::new(Limits {
        subscribers_per_source: 4,
        ..Default::default()
    })
    .unwrap();
    let key = SourceKey {
        camera: 1,
        encoder: Encoder::Main,
    };
    let main = registry.register(key, camera_url(0)).unwrap();
    assert!(registry.register(key, camera_url(0)).is_err());
    let handle = registry.source(key).unwrap();
    let limits = SubscriptionLimits {
        residence: Duration::from_secs(10),
        media_span: Duration::from_secs(10),
        ..Default::default()
    };
    let mut first = main.subscribe(Tracks::Video, limits).await.unwrap();
    let mut second = handle.subscribe(Tracks::Video, limits).await.unwrap();
    let mut slow = main
        .subscribe(
            Tracks::Video,
            SubscriptionLimits {
                packets: 1,
                ..limits
            },
        )
        .await
        .unwrap();
    let mut audio = main.subscribe(Tracks::Audio, limits).await.unwrap();
    assert!(matches!(
        main.subscribe(Tracks::Audio, limits).await,
        Err(Terminal::Budget)
    ));
    let audio_delivery = audio.recv().await.unwrap();
    assert!(audio_delivery.packet().unwrap().pts().is_some());
    drop(audio_delivery);
    drop(audio);
    let (a, b, held) = tokio::join!(first.recv(), second.recv(), slow.recv());
    let (a, b, held) = (a.unwrap(), b.unwrap(), held.unwrap());
    assert!(std::ptr::eq(a.packet().unwrap(), b.packet().unwrap()));
    assert_eq!(a.packet().unwrap().generation(), 1);
    assert!(a.packet().unwrap().pts().is_some());
    assert!(a.packet().unwrap().sequence() > 0); // initial unclocked IDR not admitted
    assert_eq!(main.snapshot().open_attempts, 1);
    #[cfg(target_os = "macos")]
    assert_eq!(rtsp_connections(), 1);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(slow.terminal(), Some(Terminal::Budget));
    assert!(matches!(held.packet(), Err(Terminal::Budget)));
    assert!(slow.charged_bytes() > 0); // in-flight owner still charged after retirement
    assert!(
        first.recv().await.unwrap().packet().unwrap().sequence() > a.packet().unwrap().sequence()
    );
    assert!(second.recv().await.is_ok());

    let mut overdue = main
        .subscribe(
            Tracks::Audio,
            SubscriptionLimits {
                residence: Duration::from_millis(100),
                ..limits
            },
        )
        .await
        .unwrap();
    let overdue_delivery = overdue.recv().await.unwrap();
    tokio::time::sleep(Duration::from_millis(160)).await;
    assert_eq!(overdue.terminal(), Some(Terminal::Residence));
    assert!(matches!(
        overdue_delivery.packet(),
        Err(Terminal::Residence)
    ));
    assert!(first.recv().await.is_ok()); // slow/expired subscribers did not kill source

    main.set_enabled(false).await.unwrap();
    assert!(matches!(a.packet(), Err(Terminal::Disabled)));
    assert!(matches!(first.recv().await, Err(Terminal::Disabled)));
    assert_eq!(slow.terminal(), Some(Terminal::Budget)); // preserve first cause
    assert!(matches!(
        main.subscribe(Tracks::Video, limits).await,
        Err(Terminal::Disabled)
    ));
    assert!(!main.snapshot().active);
    drop((a, b, held, overdue_delivery, first, second, slow, overdue));
    assert_eq!(registry.charged_bytes(), 0);
    #[cfg(target_os = "macos")]
    assert_eq!(rtsp_connections(), 0);

    main.set_enabled(true).await.unwrap();
    let mut resumed = main.subscribe(Tracks::Video, limits).await.unwrap();
    let resumed_delivery = resumed.recv().await.unwrap();
    assert_eq!(resumed_delivery.packet().unwrap().generation(), 2);
    assert_eq!(main.snapshot().open_attempts, 2);
    let sub = registry
        .register(
            SourceKey {
                camera: 1,
                encoder: Encoder::SubStream2,
            },
            camera_url(2),
        )
        .unwrap();
    let mut separate = sub.subscribe(Tracks::Video, limits).await.unwrap();
    let sub_delivery = separate.recv().await.unwrap();
    assert_eq!(sub.snapshot().open_attempts, 1);
    #[cfg(target_os = "macos")]
    assert_eq!(rtsp_connections(), 2);
    registry.configure(key, camera_url(0)).await.unwrap();
    assert!(sub_delivery.packet().is_ok());
    assert!(separate.recv().await.is_ok());
    assert!(sub.snapshot().active);
    assert!(matches!(
        resumed_delivery.packet(),
        Err(Terminal::Reconfigured)
    ));
    drop((resumed_delivery, resumed, sub_delivery));
    assert_eq!(main.snapshot().charged_bytes, 0);

    let mut idle = main.subscribe(Tracks::Audio, limits).await.unwrap();
    drop(idle.recv().await.unwrap());
    drop(idle);
    for _ in 0..100 {
        if !main.snapshot().active {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(!main.snapshot().active);
    main.reset().await.unwrap();
    // Keep Sub Stream 2 active while joining, then repeat shutdown. Cancelling
    // the first wait (if it takes >1 ms) must not detach its join ownership.
    let _ = tokio::time::timeout(Duration::from_millis(1), registry.shutdown()).await;
    registry.shutdown().await.unwrap();
    assert!(matches!(separate.recv().await, Err(Terminal::Shutdown)));
    drop(separate);
    assert!(matches!(
        main.subscribe(Tracks::Video, limits).await,
        Err(Terminal::Shutdown)
    ));
    assert_eq!(registry.charged_bytes(), 0);
    eprintln!(
        "Rust shared main source: one RTSP socket for main video/audio consumers, two with Sub Stream 2; shared packet identity; subscriber cap; local overflow/residence failures; charged held deliveries; disable/re-enable; main reset leaves Sub Stream 2 alive; idle stop and joined shutdown passed. Snapshot: {:?}",
        main.snapshot()
    );
}
