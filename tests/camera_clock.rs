//! Opt-in metadata-only real-camera characterization. One shared reader, no
//! encoder writes, captures, replacement clock, transport, or playback claim.
use amcrust::{
    ingest::{CameraUrl, Cancellation},
    media::audio::{Config, Pool, Transcoder},
    source::{
        Encoder, Limits, Registry, Source, SourceKey, SubscriptionLimits, Tracks, clock::Snapshot,
    },
};
use serde::Serialize;
use std::time::Duration;

#[derive(Serialize)]
struct Sample {
    clock: Snapshot,
    decoded_aac_packets: u64,
    decoded_aac_samples: u64,
    sample_clock_contradictions: u64,
    opus_timestamp_refusals: u64,
}
async fn sample(source: &Source, seconds: u64) -> Result<Sample, String> {
    let mut subscription = source
        .subscribe(Tracks::Audio, SubscriptionLimits::default())
        .await
        .map_err(|e| format!("subscribe: {e:?}"))?;
    let first = subscription
        .recv()
        .await
        .map_err(|e| format!("first packet: {e:?}"))?;
    let pool = Pool::new(1, 4 * 1024 * 1024).map_err(|e| format!("pool: {e:?}"))?;
    let mut encoder = Transcoder::new(
        &first,
        Config {
            rate: 48000,
            ..Default::default()
        },
        &pool,
        Cancellation::default(),
    )
    .map_err(|e| format!("decoder: {e:?}"))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    let mut next = Some(first);
    let mut refused = 0;
    loop {
        let delivery = if let Some(first) = next.take() {
            first
        } else {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => break,
                packet = subscription.recv() => packet.map_err(|e| format!("receive: {e:?}"))?,
            }
        };
        for packet in encoder
            .push(&delivery)
            .map_err(|e| format!("decode: {e:?}"))?
        {
            refused += u64::from(packet.source_timestamp_48k().is_err());
        }
    }
    for packet in encoder.finish().map_err(|e| format!("finish: {e:?}"))? {
        refused += u64::from(packet.source_timestamp_48k().is_err());
    }
    let stats = encoder.stats();
    let result = Sample {
        clock: source.clock_snapshot().ok_or("no clock observations")?,
        decoded_aac_packets: stats.input_packets,
        decoded_aac_samples: stats.decoded_samples,
        sample_clock_contradictions: stats.clock_discontinuities,
        opus_timestamp_refusals: refused,
    };
    drop(encoder);
    if pool.reserved_bytes() != 0 {
        return Err("audio reservation not released".into());
    }
    Ok(result)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires coordinated authorized camera access and existing credentials; takes ~80 seconds"]
async fn real_sender_clocks() {
    dotenvy::dotenv().ok();
    let username = std::env::var("AMCREST_USERNAME").expect("AMCREST_USERNAME required");
    let password = std::env::var("AMCREST_PASSWORD").expect("AMCREST_PASSWORD required");
    let url = CameraUrl::new(format!(
        "rtsp://{username}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype=0"
    ))
    .unwrap();
    let mut registry = Registry::new(Limits::default()).unwrap();
    let source = registry
        .register(
            SourceKey {
                camera: 1,
                encoder: Encoder::Main,
            },
            url,
        )
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(105), async {
        let first = sample(&source, 60).await?;
        source
            .set_enabled(false)
            .await
            .map_err(|e| format!("disable: {e:?}"))?;
        if !source
            .clock_snapshot()
            .is_some_and(|s| s.retired && s.generation == first.clock.generation)
        {
            return Err("clock observations not retired".into());
        }
        source
            .set_enabled(true)
            .await
            .map_err(|e| format!("enable: {e:?}"))?;
        let second = sample(&source, 20).await?;
        Ok::<_, String>((first, second))
    })
    .await;
    // Join the reader even on a returned error/timeout, before assertions.
    registry.shutdown().await.unwrap();
    let (first, second) = result
        .expect("bounded camera observation")
        .expect("camera clock observation");
    assert!(source.clock_snapshot().unwrap().retired);
    assert_eq!(source.snapshot().open_attempts, 2);
    assert_eq!(second.clock.generation, first.clock.generation + 1);
    for sample in [&first, &second] {
        eprintln!(
            "Rust source clocks {}",
            serde_json::to_string(sample).unwrap()
        );
        assert!(!sample.clock.retired);
        assert!(sample.clock.fault.is_none());
        assert_eq!(sample.clock.tracks.len(), 2);
        for track in &sample.clock.tracks {
            assert!(
                track.reports.distinct >= 2,
                "insufficient real SR intervals"
            );
            assert!(track.producer_references > 0);
            assert!(track.pts_epoch_spread_ns.is_some());
        }
        assert_eq!(
            sample.decoded_aac_samples,
            sample.decoded_aac_packets * 1024
        );
        if sample.sample_clock_contradictions > 0 {
            assert!(sample.opus_timestamp_refusals > 0);
        }
    }
    assert!(second.decoded_aac_packets < first.decoded_aac_packets);
}
