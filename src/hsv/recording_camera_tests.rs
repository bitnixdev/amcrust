use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn inspect(tool: &str, arguments: &[&str], bytes: &[u8]) -> std::process::Output {
    let mut child = tokio::process::Command::new(tool)
        .args(arguments)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let bytes = bytes.to_vec();
    let input = tokio::spawn(async move { input.write_all(&bytes).await });
    let output = child.stdout.take().unwrap();
    let errors = child.stderr.take().unwrap();
    let output = tokio::spawn(async move {
        let mut bytes = Vec::new();
        output
            .take(2 * 1024 * 1024)
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        bytes
    });
    let errors = tokio::spawn(async move {
        let mut bytes = Vec::new();
        errors.take(65536).read_to_end(&mut bytes).await.unwrap();
        bytes
    });
    let status = match tokio::time::timeout(Duration::from_secs(15), child.wait()).await {
        Ok(status) => status.unwrap(),
        Err(_) => {
            child.kill().await.unwrap();
            child.wait().await.unwrap()
        }
    };
    let result = std::process::Output {
        status,
        stdout: output.await.unwrap(),
        stderr: errors.await.unwrap(),
    };
    input.await.unwrap().unwrap();
    assert!(
        status.success() && result.stderr.is_empty(),
        "{tool}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real north-driveway; serial read-only shared recording/decode matrix"]
async fn real_recovered_recording_clips_decode_and_cover_tails() {
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").unwrap();
    let password = std::env::var("AMCREST_PASSWORD").unwrap();
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
                "rtsp://{user}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype=0"
            ))
            .unwrap(),
        )
        .unwrap();
    let mut viewer = source
        .subscribe(Tracks::Video, Default::default())
        .await
        .unwrap();
    let first = viewer.recv().await.unwrap();
    let (width, height) = first.configuration().unwrap()[first.packet().unwrap().stream_index()]
        .parameters
        .dimensions();
    drop(first);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let viewer = tokio::spawn(async move {
        tokio::pin!(stopped);
        let mut count = 0;
        loop {
            tokio::select! { _ = &mut stopped => break, packet = viewer.recv() => { if packet.is_err() { break; } count += 1; } }
        }
        count
    });
    let recorder = Recorder::new(source.clone(), Metrics::new());
    for rate in [Some(32000), Some(48000), None] {
        recorder
            .start(RecorderConfig {
                width: width.try_into().unwrap(),
                height: height.try_into().unwrap(),
                fps: 15,
                audio: rate.is_some(),
                audio_sample_rate_hz: rate.unwrap_or(48000),
                audio_bitrate: 64000,
                fragment_ms: 4000,
                prebuffer_ms: 4000,
            })
            .await;
        for clip_index in 0..2 {
            let mut clip = tokio::time::timeout(Duration::from_secs(25), async {
                loop {
                    if let Ok(clip) = recorder.open().await {
                        break clip;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            let finish_at = tokio::time::Instant::now() + Duration::from_secs(2);
            let deadline = finish_at + Duration::from_secs(5);
            let mut finishing = false;
            let mut bytes = Vec::new();
            let mut fragments = 0;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => panic!("clip completion deadline at {rate:?}"),
                    _ = tokio::time::sleep_until(finish_at), if !finishing => { finishing = true; clip.finish(); },
                    message = clip.messages.recv() => match message {
                        Some(Message::Init(fragment)) => { assert!(bytes.is_empty()); bytes.extend_from_slice(&fragment.data); },
                        Some(Message::Media(fragment)) => { assert!(!bytes.is_empty()); fragments += 1; bytes.extend_from_slice(&fragment.data); },
                        Some(Message::End) => break,
                        None => panic!("incomplete recording at {rate:?}"),
                    },
                }
            }
            clip.stop().await;
            drop(clip);
            assert!(fragments >= 2);
            assert!(source.snapshot().active);
            assert_eq!(source.snapshot().open_attempts, 1);
            let metadata = inspect("ffprobe", &["-v", "error", "-i", "pipe:0", "-show_packets", "-show_streams", "-show_entries", "packet=codec_type,pts,dts,duration,flags:stream=codec_type,codec_name,profile,sample_rate,channels,width,height,time_base", "-of", "json"], &bytes).await;
            let metadata: serde_json::Value = serde_json::from_slice(&metadata.stdout).unwrap();
            let packets = metadata["packets"].as_array().unwrap();
            let mut video_end = None;
            let mut audio_end = None;
            let mut video_count = 0;
            let mut audio_count = 0;
            for packet in packets {
                let pts = packet["pts"].as_i64().unwrap();
                let dts = packet["dts"].as_i64().unwrap();
                let duration = packet["duration"].as_i64().unwrap();
                assert_eq!(pts, dts);
                assert!(duration > 0);
                if packet["codec_type"] == "video" {
                    if let Some(end) = video_end {
                        assert_eq!(pts, end, "video duration continuity");
                    } else {
                        assert_eq!(pts, 0);
                        assert!(packet["flags"].as_str().unwrap().contains('K'));
                    }
                    video_end = Some(pts + duration);
                    video_count += 1;
                } else {
                    if let Some(end) = audio_end {
                        assert_eq!(pts, end, "AAC duration continuity");
                    } else {
                        assert!(pts < 0, "AAC preroll is missing");
                    }
                    assert_eq!(duration, 1024);
                    audio_end = Some(pts + duration);
                    audio_count += 1;
                }
            }
            assert!(video_count >= 60);
            if let Some(rate) = rate {
                assert!(audio_count > 100);
                let extra = i128::from(audio_end.unwrap()) * 90000
                    - i128::from(video_end.unwrap()) * i128::from(rate);
                assert!(
                    (0..=1025 * 90000).contains(&extra),
                    "AAC tail coverage: {extra}"
                );
                let audio = metadata["streams"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|stream| stream["codec_type"] == "audio")
                    .unwrap();
                assert_eq!(audio["codec_name"], "aac");
                assert_eq!(audio["profile"], "LC");
                assert_eq!(audio["channels"], 1);
                assert_eq!(audio["sample_rate"], rate.to_string());
            } else {
                assert_eq!(audio_count, 0);
            }
            inspect(
                "ffmpeg",
                &[
                    "-v", "error", "-i", "pipe:0", "-map", "0:v", "-map", "0:a?", "-f", "null", "-",
                ],
                &bytes,
            )
            .await;
            eprintln!(
                "shared recording {rate:?} clip {clip_index}: {video_count} video/{audio_count} AAC packets, {fragments} fragments, {} bytes; decoded, continuous durations, covered tail; main acquisition still shared",
                bytes.len()
            );
        }
        recorder.stop().await;
    }
    let _ = stop.send(());
    assert!(viewer.await.unwrap() > 0);
    registry.shutdown().await.unwrap();
    assert_eq!(source.snapshot().charged_bytes, 0);
    assert_eq!(recorder.audio_pool.reserved_bytes(), 0);
    assert_eq!(recorder.mux_pool.reserved_bytes(), 0);
}
