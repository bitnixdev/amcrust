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
#[ignore = "real camera, serial access; up to 300s awaiting oversized audio mapping; metadata only"]
async fn real_audio_quarantine_preserves_video() {
    use amcrust::source::Terminal;
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").unwrap();
    let password = std::env::var("AMCREST_PASSWORD").unwrap();
    let url = CameraUrl::new(format!(
        "rtsp://{user}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype=0"
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
    let result = tokio::time::timeout(Duration::from_secs(300), async {
        let mut audio = source.subscribe(Tracks::AudioSampleClock, SubscriptionLimits::default()).await.map_err(|e| e.to_string())?;
        let mut video = source.subscribe(Tracks::Video, SubscriptionLimits::default()).await.map_err(|e| e.to_string())?;
        let mut held_audio = None;
        let mut audio_live = true;
        let mut quarantine = None;
        let mut frames_after = 0;
        loop {
            tokio::select! {
                delivery = audio.recv(), if audio_live => match delivery {
                    Ok(delivery) => held_audio = Some(delivery),
                    Err(Terminal::Clock) => {
                        let snapshot = source.clock_snapshot().ok_or("missing clock snapshot")?;
                        let fault = snapshot.tracks.iter().find_map(|t| t.quarantine).ok_or("audio failed without quarantine")?;
                        if snapshot.retired || snapshot.fault.is_some() { return Err("quarantine retired acquisition".into()); }
                        eprintln!("Real audio quarantine: {snapshot:?}");
                        quarantine = Some(fault);
                        audio_live = false;
                    },
                    Err(reason) => return Err(reason.to_string()),
                },
                delivery = video.recv() => {
                    let delivery = delivery.map_err(|e| e.to_string())?;
                    if quarantine.is_some_and(|q| delivery.packet().is_ok_and(|p| p.sequence() > q.sequence)) {
                        frames_after += 1;
                    }
                },
            }
            if frames_after >= 30 { break; }
        }
        if !held_audio.is_some_and(|p| matches!(p.packet(), Err(Terminal::Clock))) {
            return Err("held audio was not revoked".into());
        }
        for tracks in [Tracks::Audio, Tracks::AudioSampleClock, Tracks::AudioVideo] {
            if !matches!(source.subscribe(tracks, SubscriptionLimits::default()).await, Err(Terminal::Clock)) {
                return Err("quarantined audio admitted a new consumer".into());
            }
        }
        if source.snapshot().open_attempts != 1 || !source.snapshot().active || source.snapshot().faults != 0 {
            return Err("video required acquisition restart or source fault".into());
        }
        // Reset is explicit owner action, never an automatic reanchor/reconnect.
        source.reset().await.map_err(|e| e.to_string())?;
        if video.terminal() != Some(Terminal::Reconfigured) { return Err("reset did not retire old video".into()); }
        drop((audio, video));
        let mut fresh = source.subscribe(Tracks::AudioSampleClock, SubscriptionLimits::default()).await.map_err(|e| e.to_string())?;
        let first = fresh.recv().await.map_err(|e| e.to_string())?;
        first.packet().map_err(|e| e.to_string())?;
        let snapshot = source.clock_snapshot().ok_or("missing fresh clock snapshot")?;
        if snapshot.generation != 2 || snapshot.tracks.iter().any(|t| t.quarantine.is_some()) || source.snapshot().open_attempts != 2 {
            return Err("quarantine was not cleared by a fresh generation".into());
        }
        eprintln!("Audio quarantine recovery: video_packets_after={frames_after} fresh_generation={}", snapshot.generation);
        Ok::<_, String>(())
    }).await;
    registry.shutdown().await.unwrap();
    eprintln!(
        "Audio quarantine outcome: {result:?} source={:?} clocks={:?}",
        source.snapshot(),
        source.clock_snapshot()
    );
    assert_eq!(source.snapshot().charged_bytes, 0);
    result
        .expect("quarantine/recovery check exceeded 300s; see event log")
        .expect("audio quarantine preserves video");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real camera, serial access; up to 300s until a bounded mapping step; metadata only"]
async fn real_bounded_audio_remapping() {
    use amcrust::source::Terminal;
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").unwrap();
    let password = std::env::var("AMCREST_PASSWORD").unwrap();
    let url = CameraUrl::new(format!(
        "rtsp://{user}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype=0"
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
    let result = tokio::time::timeout(Duration::from_secs(300), async {
        let mut strict = source
            .subscribe(Tracks::Audio, SubscriptionLimits::default())
            .await?;
        let mut video = source
            .subscribe(Tracks::Video, SubscriptionLimits::default())
            .await?;
        let mut candidate = source
            .subscribe(Tracks::AudioSampleClock, SubscriptionLimits::default())
            .await?;
        let mut previous = None;
        let mut held_strict = None;
        let mut strict_stopped = false;
        let mut step_sequence = None;
        let mut video_after = false;
        let mut audio_after = false;
        loop {
            tokio::select! {
                packet = strict.recv(), if !strict_stopped => match packet {
                    Ok(packet) => held_strict = Some(packet),
                    Err(Terminal::Clock) => strict_stopped = true,
                    Err(reason) => return Err(reason),
                },
                packet = video.recv() => {
                    let delivery = packet?;
                    let p = delivery.packet()?;
                    video_after |= step_sequence.is_some_and(|seq| p.sequence() > seq);
                },
                packet = candidate.recv() => {
                    let delivery = packet?;
                    let p = delivery.packet()?;
                    let dts = p.dts().ok_or(Terminal::Clock)?;
                    audio_after |= step_sequence.is_some_and(|seq| p.sequence() > seq);
                    if step_sequence.is_none() && previous.is_some_and(|last| dts <= last) {
                        let clocks = source.clock_snapshot().unwrap();
                        let step = clocks.tracks[p.stream_index()].first_mapping_step.unwrap();
                        assert!(step.bounded_remapping);
                        assert_eq!(step.current.sequence, p.sequence());
                        assert_eq!(step.current.dts, dts); // Original rollback, not clamped.
                        eprintln!("Real bounded AAC remapping: {step:?}");
                        step_sequence = Some(p.sequence());
                    }
                    previous = Some(dts);
                },
            }
            if strict_stopped && video_after && audio_after {
                assert!(held_strict.is_some_and(|p| matches!(p.packet(), Err(Terminal::Clock))));
                assert_eq!(source.snapshot().faults, 0);
                return Ok::<_, Terminal>(());
            }
        }
    })
    .await;
    registry.shutdown().await.unwrap();
    eprintln!(
        "Bounded remapping outcome: {result:?} source={:?} clocks={:?}",
        source.snapshot(),
        source.clock_snapshot()
    );
    assert_eq!(source.snapshot().open_attempts, 1);
    assert_eq!(source.snapshot().charged_bytes, 0);
    result
        .expect("no bounded remapping observed within 300s")
        .expect("bounded source remapping");
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
