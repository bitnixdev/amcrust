//! Opt-in real AAC -> Opus check. No synthetic media, retained capture, camera
//! writes, external worker or HomeKit acceptance. The explicit candidate clock
//! check is separate from strict mode; run ignored checks serially.
use amcrust::{
    ingest::{CameraUrl, Cancellation},
    media::{
        Error,
        audio::{ClockPolicy, Config, DecoderSkip, OpusPacket, Pool, Transcoder},
    },
    source::{Encoder, Limits, Registry, SourceKey, SubscriptionLimits, Tracks},
};
use ffmpeg_sys_next as av;
use std::{ptr, time::Duration};

// Independent downstream FFmpeg Opus decoder. Raw pointers stay private and are
// freed on every path. This is test-only decode validation, not another camera reader.
struct Decoder {
    context: *mut av::AVCodecContext,
    samples: u64,
    frames: u64,
}
struct Frame(*mut av::AVFrame);
impl Drop for Frame {
    fn drop(&mut self) {
        unsafe {
            av::av_frame_free(&mut self.0);
        }
    }
}
struct Packet(*mut av::AVPacket);
impl Drop for Packet {
    fn drop(&mut self) {
        unsafe {
            av::av_packet_free(&mut self.0);
        }
    }
}
impl Drop for Decoder {
    fn drop(&mut self) {
        unsafe {
            av::avcodec_free_context(&mut self.context);
        }
    }
}
impl Decoder {
    fn new(first: &OpusPacket) -> Self {
        Self::new_with_trim(first, None)
    }
    fn new_with_trim(first: &OpusPacket, trim: Option<DecoderSkip>) -> Self {
        // SAFETY: codec is checked; context owns a padded private extradata copy.
        unsafe {
            // The native FFmpeg SILK decoder buffers resampling output across
            // callbacks. Use the reference libopus decoder for per-packet trim
            // checks; retain the native decoder for legacy whole-window checks.
            let codec = if trim.is_some() {
                av::avcodec_find_decoder_by_name(c"libopus".as_ptr())
            } else {
                av::avcodec_find_decoder(av::AVCodecID::AV_CODEC_ID_OPUS)
            };
            assert!(!codec.is_null());
            let mut result = Self {
                context: av::avcodec_alloc_context3(codec),
                samples: 0,
                frames: 0,
            };
            assert!(!result.context.is_null());
            let header = first.header().unwrap();
            (*result.context).extradata =
                av::av_mallocz(header.len() + av::AV_INPUT_BUFFER_PADDING_SIZE as usize).cast();
            assert!(!(*result.context).extradata.is_null());
            ptr::copy_nonoverlapping(header.as_ptr(), (*result.context).extradata, header.len());
            (*result.context).extradata_size = header.len() as i32;
            (*result.context).pkt_timebase = av::AVRational {
                num: 1,
                den: first.config().rate as i32,
            };
            (*result.context).thread_count = 1;
            (*result.context).sample_rate = 48000;
            assert_eq!(av::avcodec_open2(result.context, codec, ptr::null_mut()), 0);
            result.accept_with_trim(first, trim);
            result
        }
    }
    fn accept(&mut self, encoded: &OpusPacket) {
        self.accept_with_trim(encoded, None);
    }
    fn accept_with_trim(&mut self, encoded: &OpusPacket, trim: Option<DecoderSkip>) {
        // SAFETY: all native allocations are guarded, payload/side data are deep
        // copied, and send_packet retains its own reference while decoding.
        unsafe {
            let packet = Packet(av::av_packet_alloc());
            assert!(!packet.0.is_null());
            let data = encoded.data().unwrap();
            assert_eq!(av::av_new_packet(packet.0, data.len() as i32), 0);
            ptr::copy_nonoverlapping(data.as_ptr(), (*packet.0).data, data.len());
            let (pts, duration) = encoded.codec_position().unwrap();
            (*packet.0).pts = pts;
            (*packet.0).dts = pts;
            (*packet.0).duration = duration;
            for (kind, bytes) in encoded.side_data().unwrap() {
                if kind == av::AVPacketSideDataType::AV_PKT_DATA_SKIP_SAMPLES && trim.is_some() {
                    continue;
                }
                let target = av::av_packet_new_side_data(packet.0, kind, bytes.len());
                assert!(!target.is_null());
                if !bytes.is_empty() {
                    ptr::copy_nonoverlapping(bytes.as_ptr(), target, bytes.len());
                }
                if kind == av::AVPacketSideDataType::AV_PKT_DATA_SKIP_SAMPLES {
                    // The native encoder's padding counts are at config.rate;
                    // this independent decoder produces fixed-rate 48-kHz PCM.
                    let skip = encoded.decoder_skip().unwrap().unwrap();
                    let data = std::slice::from_raw_parts_mut(target, 10);
                    data[..4].copy_from_slice(&skip.start.to_le_bytes());
                    data[4..8].copy_from_slice(&skip.end.to_le_bytes());
                    data[8] = skip.start_reason;
                    data[9] = skip.end_reason;
                }
            }
            if let Some(skip) = trim {
                let target = av::av_packet_new_side_data(
                    packet.0,
                    av::AVPacketSideDataType::AV_PKT_DATA_SKIP_SAMPLES,
                    10,
                );
                assert!(!target.is_null());
                let data = std::slice::from_raw_parts_mut(target, 10);
                data[..4].copy_from_slice(&skip.start.to_le_bytes());
                data[4..8].copy_from_slice(&skip.end.to_le_bytes());
                data[8] = skip.start_reason;
                data[9] = skip.end_reason;
            }
            assert_eq!(av::avcodec_send_packet(self.context, packet.0), 0);
            self.drain(false);
        }
    }
    fn drain(&mut self, finish: bool) {
        loop {
            unsafe {
                let frame = Frame(av::av_frame_alloc());
                assert!(!frame.0.is_null());
                let code = av::avcodec_receive_frame(self.context, frame.0);
                if code == av::AVERROR_EOF {
                    assert!(finish);
                    break;
                }
                if code < 0
                    && std::io::Error::from_raw_os_error(-code).kind()
                        == std::io::ErrorKind::WouldBlock
                {
                    assert!(!finish);
                    break;
                }
                assert_eq!(code, 0, "Opus decode error");
                assert_eq!((*frame.0).sample_rate, 48000);
                assert!((*frame.0).nb_samples > 0 && (*frame.0).nb_samples <= 5760);
                self.samples += (*frame.0).nb_samples as u64;
                self.frames += 1;
            }
        }
    }
    fn finish(&mut self) {
        unsafe {
            assert_eq!(av::avcodec_send_packet(self.context, ptr::null()), 0);
        }
        self.drain(true);
    }
}
fn consume(packet: &OpusPacket, decoder: &mut Option<Decoder>, refused: &mut u64) {
    match packet.source_timestamp_48k() {
        Ok(_) => (),
        Err(Error::Clock | Error::Incomplete) => *refused += 1,
        Err(error) => panic!("unexpected timestamp result: {error}"),
    }
    if let Some(decoder) = decoder {
        decoder.accept(packet);
    } else {
        *decoder = Some(Decoder::new(packet));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires coordinated authorized camera access and existing credentials"]
async fn real_aac_opus() {
    tokio::time::timeout(Duration::from_secs(30), exercise())
        .await
        .expect("bounded audio check");
}
async fn exercise() {
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").expect("AMCREST_USERNAME required");
    let password = std::env::var("AMCREST_PASSWORD").expect("AMCREST_PASSWORD required");
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
    let mut subscription = source
        .subscribe(Tracks::Audio, SubscriptionLimits::default())
        .await
        .unwrap();
    let first = subscription.recv().await.unwrap();
    let pool = Pool::new(5, 12 * 1024 * 1024).unwrap();
    let configs = [
        Config {
            rate: 48000,
            bitrate: 32000,
            fec: false,
            expected_loss_percent: 0,
            ..Default::default()
        },
        Config::default(),
        Config {
            rate: 24000,
            frame_ms: 60,
            bitrate: 32000,
            ..Default::default()
        },
        Config {
            rate: 8000,
            frame_ms: 40,
            bitrate: 16000,
            vbr: false,
            fec: false,
            expected_loss_percent: 0,
            ..Default::default()
        },
    ];
    assert_ne!(configs[0], configs[1]);
    let cancellations: Vec<_> = configs.iter().map(|_| Cancellation::default()).collect();
    let mut encoders: Vec<_> = configs
        .iter()
        .zip(&cancellations)
        .map(|(&config, cancel)| Transcoder::new(&first, config, &pool, cancel.clone()).unwrap())
        .collect();
    let mut failing =
        Some(Transcoder::new(&first, configs[0], &pool, Cancellation::default()).unwrap());
    assert!(matches!(
        Transcoder::new(&first, configs[0], &pool, Cancellation::default()),
        Err(Error::Limit)
    ));
    let mut held = Vec::new();
    let mut fault_seen = false;
    let mut decoders: Vec<Option<Decoder>> = (0..configs.len()).map(|_| None).collect();
    let mut refused = vec![0; configs.len()];
    let mut delivery = first;
    for ordinal in 0..200 {
        let original = (
            delivery.packet().unwrap().pts(),
            delivery.packet().unwrap().dts(),
        );
        for (index, encoder) in encoders.iter_mut().enumerate() {
            for packet in encoder.push(&delivery).unwrap() {
                consume(&packet, &mut decoders[index], &mut refused[index]);
            }
        }
        if let Some(encoder) = &mut failing {
            match encoder.push(&delivery) {
                Ok(packets) => held.extend(packets),
                Err(Error::Limit) => {
                    fault_seen = true;
                    assert!(
                        held.iter()
                            .all(|packet| matches!(packet.data(), Err(Error::Limit)))
                    );
                    failing.take();
                    let reserved = pool.reserved_bytes();
                    held.clear();
                    assert!(pool.reserved_bytes() < reserved);
                }
                Err(error) => panic!("unexpected local encoder failure: {error}"),
            }
        }
        assert_eq!(
            original,
            (
                delivery.packet().unwrap().pts(),
                delivery.packet().unwrap().dts()
            )
        );
        drop(delivery);
        if ordinal != 199 {
            delivery = subscription.recv().await.unwrap();
        } else {
            break;
        }
    }
    assert!(fault_seen);
    let mut tails = Vec::new();
    for (index, encoder) in encoders.iter_mut().enumerate() {
        let packets = encoder.finish().unwrap();
        for packet in &packets {
            consume(packet, &mut decoders[index], &mut refused[index]);
        }
        let stats = encoder.stats();
        let decoder = decoders[index].as_mut().unwrap();
        decoder.finish();
        assert_eq!(
            decoder.samples,
            stats.resampled_samples * (48000 / configs[index].rate) as u64,
            "decoded Opus samples must account for actual resampler output and codec padding"
        );
        assert_eq!(stats.input_packets, 200);
        assert_eq!(stats.decoded_samples, 200 * 1024);
        if stats.clock_discontinuities > 0 {
            assert!(refused[index] > 0);
        }
        eprintln!(
            "Rust AAC/Opus rate={} frame_ms={} decoded_pcm={} converted_pcm={} opus_packets={} decoded_opus_48k={} clock_discontinuities={} first_mismatch={:?} step_error_range={:?}..{:?} timestamp_refusals={} SR_packets={}",
            configs[index].rate,
            configs[index].frame_ms,
            stats.decoded_samples,
            stats.resampled_samples,
            stats.encoded_packets,
            decoder.samples,
            stats.clock_discontinuities,
            stats.first_mismatch,
            stats.minimum_step_error,
            stats.maximum_step_error,
            refused[index],
            stats.sender_reports
        );
        assert!(!packets.is_empty());
        tails.push(packets);
    }
    drop(encoders);
    assert!(pool.reserved_bytes() > 0);
    cancellations[0].cancel();
    cancellations[0].cancel();
    assert!(
        tails[0]
            .iter()
            .all(|p| matches!(p.data(), Err(Error::Cancelled)))
    );
    assert!(tails[1].iter().all(|p| p.data().is_ok()));
    assert_eq!(source.snapshot().open_attempts, 1);
    source.set_enabled(false).await.unwrap();
    assert!(
        tails[1]
            .iter()
            .all(|p| matches!(p.data(), Err(Error::Source(_))))
    );
    drop(tails);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(subscription);
    registry.shutdown().await.unwrap();
    eprintln!(
        "Audio codec output decoded; source clock contradictions were refused, not repaired. Local encoder failure, repeated cancellation, source revocation and retained reservation release passed; no A/V or HomeKit acceptance claim."
    );
}

fn consume_candidate(
    packet: &OpusPacket,
    decoder: &mut Option<Decoder>,
    end: &mut Option<i64>,
) -> Result<(), Error> {
    let native_skip = packet.decoder_skip()?;
    let native_position = packet.codec_position()?;
    let presentation = packet.presentation_48k().inspect_err(|error| {
        eprintln!(
            "candidate presentation refusal={error:?} position={:?} skip={:?}",
            packet.codec_position(),
            packet.decoder_skip()
        );
    })?;
    if let Some(previous) = *end {
        assert_eq!(presentation.audible_timestamp_48k, previous);
    }
    *end = Some(presentation.audible_timestamp_48k + presentation.audible_samples as i64);
    let before = decoder.as_ref().map_or(0, |d| d.samples);
    if let Some(decoder) = decoder {
        decoder.accept_with_trim(packet, Some(presentation.decoder_skip));
    } else {
        *decoder = Some(Decoder::new_with_trim(
            packet,
            Some(presentation.decoder_skip),
        ));
    }
    assert_eq!(
        decoder.as_ref().unwrap().samples - before,
        presentation.audible_samples as u64
    );
    if presentation.decoder_skip.start != 0 || presentation.decoder_skip.end != 0 {
        assert_eq!(packet.source_timestamp_48k(), Err(Error::Incomplete));
    } else {
        assert_eq!(
            packet.source_timestamp_48k()?,
            presentation.packet_timestamp_48k
        );
    }
    assert_eq!(packet.decoder_skip()?, native_skip);
    assert_eq!(packet.codec_position()?, native_position);
    Ok(())
}

// Constant-space diagnostic regression: phase in milliseconds against nominal
// decoded-input seconds. This is NOT a measured oscillator rate or a clock grant.
#[derive(Default)]
struct PhaseTrend {
    n: u64,
    mean_x: f64,
    mean_y: f64,
    xx: f64,
    xy: f64,
}
impl PhaseTrend {
    fn observe(&mut self, sample_position: u64, phase_samples: i128) {
        let x = sample_position as f64 / 48000.0;
        let y = phase_samples as f64 / 48.0;
        self.n += 1;
        let dx = x - self.mean_x;
        let dy = y - self.mean_y;
        self.mean_x += dx / self.n as f64;
        self.mean_y += dy / self.n as f64;
        self.xx += dx * (x - self.mean_x);
        self.xy += dx * (y - self.mean_y);
    }
    fn ms_per_second(&self) -> Option<f64> {
        (self.n >= 2 && self.xx > 0.0).then(|| self.xy / self.xx)
    }
}
#[test]
fn phase_trend_is_diagnostic_ms_per_nominal_second() {
    let mut trend = PhaseTrend::default();
    assert_eq!(trend.ms_per_second(), None);
    for second in 10..=20 {
        trend.observe(second * 48000, second as i128 * 48);
    }
    assert_eq!(trend.ms_per_second(), Some(1.0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires coordinated camera access; default 60s, AMCRUST_CLOCK_SECONDS=1800 qualifies until first refusal; run serially"]
async fn real_anchored_audio_clock() {
    // Parse before dotenv: the window is an explicit invocation choice, not a
    // credential-file setting. Never run indefinitely or silently shorten it.
    let seconds = match std::env::var("AMCRUST_CLOCK_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .expect("AMCRUST_CLOCK_SECONDS must be an integer"),
        Err(std::env::VarError::NotPresent) => 60,
        Err(_) => panic!("AMCRUST_CLOCK_SECONDS must be valid text"),
    };
    assert!(
        (60..=1800).contains(&seconds),
        "clock window must be 60..=1800 seconds"
    );
    let qualification = seconds == 1800;
    eprintln!(
        "Candidate clock window: requested_seconds={seconds} qualification={qualification}; no reanchor or budget increase"
    );
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").expect("AMCREST_USERNAME required");
    let password = std::env::var("AMCREST_PASSWORD").expect("AMCREST_PASSWORD required");
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
    let mut last_observation = None;
    let mut monitors = Vec::new();
    let result = tokio::time::timeout(Duration::from_secs(seconds + 25), async {
        for tracks in [Tracks::Audio, Tracks::Video] {
            let mut subscription = source.subscribe(tracks, SubscriptionLimits::default()).await.map_err(Error::from)?;
            monitors.push((tracks, tokio::spawn(async move {
                let mut count = 0_u64;
                let mut last = None;
                let mut last_sequence = None;
                loop {
                    match subscription.recv().await {
                        Ok(delivery) => {
                            if let Ok(packet) = delivery.packet() { last_sequence = Some(packet.sequence()); }
                            count += 1;
                            last = Some(delivery);
                        }
                        Err(reason) => return (count, last_sequence, reason, last.is_none_or(|p| p.packet().is_err())),
                    }
                }
            })));
        }
        let mut input = source.subscribe(Tracks::AudioSampleClock, SubscriptionLimits::default()).await.map_err(Error::from)?;
        let first = loop {
            let packet = input.recv().await.map_err(Error::from)?;
            if packet.packet()?.sender_report().is_some() { break packet; }
        };
        let config = Config { rate: 48000, ..Default::default() };
        let pool = Pool::new(3, 6 * 1024 * 1024)?;
        let mut candidate = Transcoder::new_with_clock_policy(&first, config, ClockPolicy::candidate(), &pool, Cancellation::default())?;
        let mut strict = Transcoder::new(&first, config, &pool, Cancellation::default())?;
        let mut tight = Some(Transcoder::new_with_clock_policy(&first, config,
            ClockPolicy::AnchoredSamples { max_phase_samples: 1, report_timeout: Duration::from_secs(15) }, &pool, Cancellation::default())?);
        let mut held_tight = Vec::new();
        let mut next = Some(first);
        let mut decoder = None;
        let mut end = None;
        let mut strict_refused = 0;
        let mut last = None;
        let mut budget_trip = false;
        let started = tokio::time::Instant::now();
        let until = started + Duration::from_secs(seconds);
        let mut trend = PhaseTrend::default();
        let mut next_trace = Duration::ZERO;
        loop {
            let delivery = if let Some(first) = next.take() { first } else {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(until) => break,
                    packet = input.recv() => packet.map_err(Error::from)?,
                }
            };
            let original = (delivery.packet()?.pts(), delivery.packet()?.dts());
            for packet in strict.push(&delivery)? {
                strict_refused += u64::from(packet.source_timestamp_48k().is_err());
            }
            if let Some(encoder) = &mut tight {
                match encoder.push(&delivery) {
                    Ok(packets) => held_tight.extend(packets),
                    Err(Error::Clock) => {
                        assert!(held_tight.iter().all(|p| p.data() == Err(Error::Clock)));
                        tight.take(); held_tight.clear();
                    }
                    Err(error) => return Err(error),
                }
            }
            let position = candidate.clock_status().decoded_samples;
            let report_boundary = delivery.packet()?.sender_report().is_some();
            match candidate.push(&delivery) {
                Ok(packets) => {
                    trend.observe(position, candidate.clock_status().latest_phase_samples);
                    for packet in packets {
                        consume_candidate(&packet, &mut decoder, &mut end)?;
                        last = Some(packet);
                    }
                },
                Err(Error::Clock) => {
                    let status = candidate.clock_status();
                    let phase_trip = status.maximum_absolute_phase_samples > 4800;
                    let stale_report = status.last_report_age.is_some_and(|age| age >= Duration::from_secs(15));
                    eprintln!("Candidate clock REFUSED: elapsed_seconds={:.3} phase_trip={phase_trip} stale_report={stale_report} status={status:?}", started.elapsed().as_secs_f64());
                    assert!(phase_trip || stale_report, "unclassified clock refusal");
                    if phase_trip { trend.observe(position, status.latest_phase_samples); }
                    assert!(last.as_ref().is_some_and(|p| p.data() == Err(Error::Clock)));
                    assert!(matches!(candidate.finish(), Err(Error::Clock)));
                    budget_trip = true;
                }
                Err(error) => {
                    eprintln!("Candidate processing refused: elapsed_seconds={:.3} error={error:?} status={:?}", started.elapsed().as_secs_f64(), candidate.clock_status());
                    return Err(error);
                }
            }
            let elapsed = started.elapsed();
            last_observation = Some((elapsed.as_secs_f64(), candidate.clock_status(), trend.ms_per_second()));
            if report_boundary || budget_trip || elapsed >= next_trace {
                let status = candidate.clock_status();
                eprintln!("Candidate phase: elapsed_seconds={:.3} sample_position={position} report_boundary={report_boundary} phase_samples={} peak_samples={} report_age_ms={:?} ols_ms_per_second={:?}",
                    elapsed.as_secs_f64(), status.latest_phase_samples, status.maximum_absolute_phase_samples,
                    status.last_report_age.map(|age| age.as_millis()), trend.ms_per_second());
                next_trace = elapsed + Duration::from_secs(5);
            }
            assert_eq!(original, (delivery.packet()?.pts(), delivery.packet()?.dts()));
            if budget_trip { break; }
        }
        if !budget_trip {
            for packet in candidate.finish()? {
                consume_candidate(&packet, &mut decoder, &mut end)?;
                last = Some(packet);
            }
            decoder.as_mut().unwrap().finish();
            assert_eq!(decoder.as_ref().unwrap().samples, candidate.stats().decoded_samples);
        }
        assert!(strict_refused > 0);
        assert!(tight.is_none());
        eprintln!("Candidate anchored clock: requested_seconds={seconds} elapsed_seconds={:.3} budget_trip={budget_trip} ols_ms_per_second={:?} status={:?} input={:?} decoded_opus={} strict_refusals={strict_refused}",
            started.elapsed().as_secs_f64(), trend.ms_per_second(), candidate.clock_status(), candidate.stats(), decoder.as_ref().unwrap().samples);
        assert_eq!(source.snapshot().open_attempts, 1);
        eprintln!("Candidate window source clocks: {:?}", source.clock_snapshot());
        source.set_enabled(false).await.map_err(Error::from)?;
        assert!(last.as_ref().unwrap().data().is_err());
        drop((candidate, strict, last, held_tight));
        assert_eq!(pool.reserved_bytes(), 0);
        drop(input);
        if qualification {
            // One acquisition, no second window after a refusal. Cleanup still
            // runs, but Cargo must not label the qualification as successful.
            return if budget_trip { Err(Error::Clock) } else { Ok(()) };
        }
        source.set_enabled(true).await.map_err(Error::from)?;
        let mut input = source.subscribe(Tracks::Audio, SubscriptionLimits::default()).await.map_err(Error::from)?;
        let first = loop {
            let delivery = input.recv().await.map_err(Error::from)?;
            if delivery.packet()?.sender_report().is_some() { break delivery; }
        };
        let mut fresh = Transcoder::new_with_clock_policy(&first, config, ClockPolicy::candidate(), &pool, Cancellation::default())?;
        assert!(fresh.clock_status().anchored_pts.is_none());
        assert!(!fresh.clock_status().sender_report_seen);
        let output = fresh.push(&first)?;
        assert_eq!(fresh.clock_status().decoded_samples, 1024);
        assert_eq!(fresh.clock_status().latest_phase_samples, 0);
        assert_eq!(source.snapshot().open_attempts, 2);
        source.set_enabled(false).await.map_err(Error::from)?;
        assert!(output.iter().all(|p| p.data().is_err()));
        drop((fresh, output));
        assert_eq!(pool.reserved_bytes(), 0);
        Ok::<_, Error>(())
    }).await;
    registry.shutdown().await.unwrap();
    let bounded_steps = source.clock_snapshot().map_or(0, |s| {
        s.tracks
            .iter()
            .map(|t| t.bounded_mapping_steps)
            .sum::<u64>()
    });
    for (tracks, monitor) in monitors {
        let outcome = monitor.await.expect("joined subscription observer");
        eprintln!(
            "Candidate sibling: tracks={tracks:?} outcome={outcome:?} bounded_mapping_steps={bounded_steps}"
        );
        assert!(outcome.3, "terminal sibling must revoke held delivery");
        if qualification && bounded_steps > 0 && matches!(tracks, Tracks::Audio) {
            assert_eq!(outcome.2, amcrust::source::Terminal::Clock);
        }
    }
    eprintln!(
        "Candidate final outcome: requested_seconds={seconds} result={result:?} last_observation={last_observation:?} source={:?} clocks={:?}",
        source.snapshot(),
        source.clock_snapshot()
    );
    if qualification {
        assert_eq!(
            source.snapshot().open_attempts,
            1,
            "qualification must not restart acquisition"
        );
    }
    result
        .expect("bounded candidate window")
        .expect("candidate clock/lifecycle check; a qualification refusal is not acceptance");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires coordinated camera access; negotiated-rate boundary checks; run serially"]
async fn real_negotiated_audio_clocks() {
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").expect("AMCREST_USERNAME required");
    let password = std::env::var("AMCREST_PASSWORD").expect("AMCREST_PASSWORD required");
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
    let result = tokio::time::timeout(Duration::from_secs(25), async {
        let mut input = source.subscribe(Tracks::Audio, SubscriptionLimits::default()).await.map_err(Error::from)?;
        let first = loop {
            let delivery = input.recv().await.map_err(Error::from)?;
            if delivery.packet()?.sender_report().is_some() { break delivery; }
        };
        struct Case {
            config: Config,
            target: u64,
            encoder: Option<Transcoder>,
            decoder: Option<Decoder>,
            end: Option<i64>,
            tail: u64,
            last: Option<OpusPacket>,
        }
        let pool = Pool::new(13, 26 * 1024 * 1024)?;
        assert!(matches!(Transcoder::new_with_clock_policy(&first, Config { rate: 32000, ..Default::default() },
            ClockPolicy::candidate(), &pool, Cancellation::default()), Err(Error::Configuration)));
        assert_eq!(pool.reserved_bytes(), 0);
        let mut cases = Vec::new();
        for rate in [8000, 16000, 24000, 48000] {
            for (index, frame_ms) in [20, 40, 60].into_iter().enumerate() {
                let config = Config { rate, frame_ms, ..Default::default() };
                cases.push(Case {
                    config, target: 199 + index as u64,
                    encoder: Some(Transcoder::new_with_clock_policy(&first, config, ClockPolicy::candidate(), &pool, Cancellation::default())?),
                    decoder: None, end: None, tail: 0, last: None,
                });
            }
        }
        // Also finish the common 16-kHz/20-ms mode at a covered endpoint; the
        // 199-AU shortfall is an endpoint refusal, not a refusal of this mode.
        let config = Config::default();
        cases.push(Case { config, target: 200,
            encoder: Some(Transcoder::new_with_clock_policy(&first, config, ClockPolicy::candidate(), &pool, Cancellation::default())?),
            decoder: None, end: None, tail: 0, last: None });
        let mut next = Some(first);
        for count in 1..=201 {
            let delivery = if let Some(first) = next.take() { first } else { input.recv().await.map_err(Error::from)? };
            for case in &mut cases {
                let Some(encoder) = &mut case.encoder else { continue; };
                for packet in encoder.push(&delivery)? {
                    assert_eq!(packet.presentation_48k()?.resampler_tail_discard_48k, 0, "live output cannot receive finite endpoint trims");
                    consume_candidate(&packet, &mut case.decoder, &mut case.end)?;
                    case.last = Some(packet);
                }
                if count == case.target {
                    let finished = match encoder.finish() {
                        Ok(packets) => packets,
                        Err(Error::Incomplete) => {
                            let stats = encoder.stats();
                            let factor = (48000 / case.config.rate) as u64;
                            let converted = stats.resampled_samples * factor;
                            assert!(converted < stats.decoded_samples);
                            assert!(stats.decoded_samples - converted < factor);
                            assert_eq!(case.last.as_ref().unwrap().data(), Err(Error::Incomplete));
                            eprintln!("Negotiated candidate rate={} frame_ms={} AAC={} finite_finish=refused uncovered_48k={}", case.config.rate, case.config.frame_ms, stats.input_packets, stats.decoded_samples - converted);
                            case.last.take();
                            case.encoder.take();
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    for packet in finished {
                        case.tail += packet.presentation_48k().inspect_err(|e| eprintln!("tail refusal rate={} frame={} error={e:?} position={:?} skip={:?}", case.config.rate, case.config.frame_ms, packet.codec_position(), packet.decoder_skip()))?.resampler_tail_discard_48k as u64;
                        consume_candidate(&packet, &mut case.decoder, &mut case.end)?;
                    }
                    let stats = encoder.stats();
                    let status = encoder.clock_status();
                    case.decoder.as_mut().unwrap().finish();
                    let decoded = case.decoder.as_ref().unwrap().samples;
                    assert_eq!(stats.decoded_samples, case.target * 1024);
                    assert_eq!(decoded, stats.decoded_samples);
                    assert_eq!(case.end, Some(status.anchored_pts.unwrap() + decoded as i64));
                    assert_eq!(case.tail, stats.resampled_samples * (48000 / case.config.rate) as u64 - decoded);
                    assert!(case.tail < (48000 / case.config.rate) as u64);
                    eprintln!("Negotiated candidate rate={} frame_ms={} AAC={} decoded={} resampled={} extra_tail_48k={}", case.config.rate, case.config.frame_ms, stats.input_packets, decoded, stats.resampled_samples, case.tail);
                    case.last.take();
                    case.encoder.take();
                }
            }
        }
        assert!(cases.iter().all(|c| c.encoder.is_none()));
        assert_eq!(source.snapshot().open_attempts, 1);
        assert_eq!(pool.reserved_bytes(), 0);
        Ok::<_, Error>(())
    }).await;
    registry.shutdown().await.unwrap();
    result
        .expect("bounded negotiated-rate check")
        .expect("negotiated-rate presentation");
}
