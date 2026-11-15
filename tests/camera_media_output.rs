//! Opt-in real-camera media output check. FFmpeg CLI is a downstream decoder only;
//! acquisition/mux/packetization use Rust library code. No captured media is saved.
use amcrust::{
    ingest::{CameraUrl, Cancellation, Codec},
    media::{
        Error,
        fmp4::{Limits as MuxLimits, Muxer, Pool},
        rtp::{Config, H264},
    },
    source::{Encoder, Limits, Registry, SourceKey, Subscription, SubscriptionLimits, Tracks},
};
use sha2::{Digest, Sha256};
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    process::{Output, Stdio},
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    process::{ChildStdin, Command},
    task::JoinHandle,
};

struct Decoder {
    input: Option<ChildStdin>,
    task: Option<JoinHandle<Output>>,
}
impl Drop for Decoder {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
impl Decoder {
    fn start(program: &str, args: &[&str]) -> Self {
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take();
        let task = Some(tokio::spawn(async move {
            child.wait_with_output().await.unwrap()
        }));
        Self { input, task }
    }
    async fn write(&mut self, bytes: &[u8]) {
        self.input.as_mut().unwrap().write_all(bytes).await.unwrap();
    }
    async fn finish(&mut self) -> Output {
        self.input.take();
        let output = self.task.as_mut().unwrap().await.unwrap();
        self.task.take();
        assert!(
            output.status.success(),
            "decoder failed (decoded frames={}): {}",
            decoded_frames(&output),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stderr.is_empty(),
            "decoder diagnostics: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }
}
fn decoded_frames(output: &Output) -> usize {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.strip_prefix("frame=")?.trim().parse::<usize>().ok())
        .max()
        .unwrap_or(0)
}
fn camera_url() -> CameraUrl {
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").expect("AMCREST_USERNAME required");
    let password = std::env::var("AMCREST_PASSWORD").expect("AMCREST_PASSWORD required");
    CameraUrl::new(format!(
        "rtsp://{user}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype=0"
    ))
    .unwrap()
}
struct Task<T>(JoinHandle<T>);
impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn rtp_viewer(mut subscription: Subscription, config: Config) -> (usize, u64) {
    // Reserve a local RTP/RTCP pair before giving it to the diagnostic decoder.
    let (reservation, control) = (0..32)
        .find_map(|_| {
            let socket = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).ok()?;
            let port = socket.local_addr().ok()?.port();
            let control =
                std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, port.checked_add(1)?)).ok()?;
            Some((socket, control))
        })
        .expect("local decoder ports");
    let port = reservation.local_addr().unwrap().port();
    let sdp = format!(
        "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=Rust camera output check\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=video {port} RTP/AVP {}\r\na=rtpmap:{} H264/90000\r\na=fmtp:{} packetization-mode=1\r\n",
        config.payload_type, config.payload_type, config.payload_type
    );
    drop((reservation, control));
    let mut decoder = Decoder::start(
        "ffmpeg",
        &[
            "-hide_banner",
            "-loglevel",
            "error",
            "-xerror",
            "-protocol_whitelist",
            "pipe,udp,rtp",
            "-analyzeduration",
            "0",
            "-probesize",
            "32",
            "-f",
            "sdp",
            "-i",
            "pipe:0",
            "-frames:v",
            "30",
            "-fps_mode:v",
            "passthrough",
            "-enc_time_base:v",
            "demux",
            "-f",
            "null",
            "-",
            "-progress",
            "pipe:1",
            "-nostats",
        ],
    );
    decoder.write(sdp.as_bytes()).await;
    decoder.input.take();
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let destination = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    let first = subscription.recv().await.unwrap();
    let mut packetizer = H264::new(&first, config).unwrap();
    let mut delivery = first;
    let mut buffer = vec![0; config.packet_bytes];
    let mut last_sequence = None;
    let mut source_frames = 0;
    loop {
        {
            let mut unit = packetizer.begin(&delivery).unwrap();
            let mut markers = 0;
            while let Some(size) = unit.next_packet(&mut buffer).unwrap() {
                assert!(size <= config.packet_bytes);
                assert_eq!(buffer[1] & 0x7f, config.payload_type);
                assert_eq!(
                    u32::from_be_bytes(buffer[8..12].try_into().unwrap()),
                    config.ssrc
                );
                let sequence = u16::from_be_bytes(buffer[2..4].try_into().unwrap());
                if let Some(last) = last_sequence {
                    assert_eq!(sequence, u16::wrapping_add(last, 1));
                }
                last_sequence = Some(sequence);
                markers += usize::from(buffer[1] & 0x80 != 0);
                socket.send_to(&buffer[..size], destination).await.unwrap();
            }
            assert_eq!(markers, 1);
        }
        source_frames += 1;
        drop(delivery);
        // Continue through a second real IDR; the SDP receiver may spend the
        // beginning of the first GOP discovering codec parameters.
        if source_frames >= 100 {
            break;
        }
        delivery = subscription.recv().await.unwrap();
    }
    let output = decoder.finish().await;
    let decoded = decoded_frames(&output);
    assert_eq!(decoded, 30);
    (decoded, packetizer.packetized())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires coordinated access to authorized camera, credentials and downstream FFmpeg"]
async fn real_camera_media_output() {
    tokio::time::timeout(Duration::from_secs(45), exercise())
        .await
        .expect("bounded camera output check");
}
async fn exercise() {
    let mut registry = Registry::new(Limits::default()).unwrap();
    let source = registry
        .register(
            SourceKey {
                camera: 1,
                encoder: Encoder::Main,
            },
            camera_url(),
        )
        .unwrap();
    let limits = SubscriptionLimits::default();
    let mut recording = source.subscribe(Tracks::AudioVideo, limits).await.unwrap();
    let one = source.subscribe(Tracks::Video, limits).await.unwrap();
    let two = source.subscribe(Tracks::Video, limits).await.unwrap();
    let mut other_subscription = source.subscribe(Tracks::Video, limits).await.unwrap();
    let mut viewer_one = Task(tokio::spawn(rtp_viewer(
        one,
        Config {
            ssrc: 0x12345678,
            payload_type: 96,
            packet_bytes: 400,
        },
    )));
    let mut viewer_two = Task(tokio::spawn(rtp_viewer(
        two,
        Config {
            ssrc: 0x87654321,
            payload_type: 97,
            packet_bytes: 1200,
        },
    )));
    let first = recording.recv().await.unwrap();
    let foreign = other_subscription.recv().await.unwrap();
    let pool = Pool::new(2, 160 * 1024 * 1024).unwrap();
    let cancellation = Cancellation::default();
    // Exercise failure paths with actual admitted camera packets, not fixtures.
    let mut partial = H264::new(
        &first,
        Config {
            ssrc: 7,
            payload_type: 98,
            packet_bytes: 400,
        },
    )
    .unwrap();
    {
        let mut unit = partial.begin(&first).unwrap();
        assert!(unit.next_packet(&mut [0; 400]).unwrap().is_some());
    }
    assert!(matches!(partial.begin(&first), Err(Error::Incomplete)));
    drop(partial);
    let mut mismatched = Muxer::new(
        &first,
        false,
        &pool,
        MuxLimits::default(),
        Cancellation::default(),
    )
    .unwrap();
    assert!(matches!(
        mismatched.push(&foreign),
        Err(Error::Source(amcrust::source::Terminal::Reconfigured))
    ));
    drop((mismatched, foreign, other_subscription));
    let mut limited = Muxer::new(
        &first,
        false,
        &pool,
        MuxLimits {
            input_bytes: 1,
            ..Default::default()
        },
        Cancellation::default(),
    )
    .unwrap();
    assert!(matches!(limited.push(&first), Err(Error::Limit)));
    assert!(matches!(limited.drain(), Err(Error::Limit)));
    drop(limited);
    assert_eq!(pool.reserved_bytes(), 0);
    let mut muxer = Muxer::new(
        &first,
        true,
        &pool,
        MuxLimits::default(),
        cancellation.clone(),
    )
    .unwrap();
    let mut video_only = Muxer::new(
        &first,
        false,
        &pool,
        MuxLimits::default(),
        Cancellation::default(),
    )
    .unwrap();
    assert!(matches!(
        Muxer::new(
            &first,
            false,
            &pool,
            MuxLimits::default(),
            Cancellation::default()
        ),
        Err(Error::Limit)
    ));
    let mut decoder = Decoder::start(
        "ffmpeg",
        &[
            "-hide_banner",
            "-loglevel",
            "error",
            "-xerror",
            "-i",
            "pipe:0",
            "-map",
            "0:v:0",
            "-map",
            "0:a:0",
            "-fps_mode:v",
            "passthrough",
            "-enc_time_base:v",
            "demux",
            "-f",
            "null",
            "-",
            "-progress",
            "pipe:1",
            "-nostats",
        ],
    );
    let mut video_decoder = Decoder::start(
        "ffmpeg",
        &[
            "-hide_banner",
            "-loglevel",
            "error",
            "-xerror",
            "-i",
            "pipe:0",
            "-map",
            "0:v:0",
            "-an",
            "-fps_mode:v",
            "passthrough",
            "-enc_time_base:v",
            "demux",
            "-f",
            "null",
            "-",
            "-progress",
            "pipe:1",
            "-nostats",
        ],
    );
    let mut probe = Decoder::start(
        "ffprobe",
        &[
            "-v",
            "error",
            "-show_packets",
            "-show_entries",
            "packet=stream_index,pts,dts,duration",
            "-of",
            "json",
            "-i",
            "pipe:0",
        ],
    );
    let mut delivery = first;
    let mut expected = vec![Vec::new(), Vec::new()];
    let origin = delivery.packet().unwrap().pts().unwrap();
    let video_clock =
        delivery.configuration().unwrap()[delivery.packet().unwrap().stream_index()].time_base;
    let start = Instant::now();
    loop {
        let packet = delivery.packet().unwrap();
        let stream = &delivery.configuration().unwrap()[packet.stream_index()];
        let output_index = if stream.codec == Codec::H264 { 0 } else { 1 };
        let numerator =
            origin as i128 * video_clock.numerator as i128 * stream.time_base.denominator as i128;
        let denominator = video_clock.denominator as i128 * stream.time_base.numerator as i128;
        assert_eq!(numerator % denominator, 0);
        let shift = (numerator / denominator) as i64;
        expected[output_index].push((
            packet.pts().unwrap() - shift,
            packet.dts().unwrap() - shift,
            packet.duration(),
        ));
        let before = (
            packet.pts(),
            packet.dts(),
            packet.flags(),
            Sha256::digest(packet.data()),
        );
        muxer.push(&delivery).unwrap();
        video_only.push(&delivery).unwrap();
        assert_eq!(
            before,
            (
                packet.pts(),
                packet.dts(),
                packet.flags(),
                Sha256::digest(packet.data())
            )
        );
        drop(delivery);
        let chunk = muxer.drain().unwrap();
        decoder.write(chunk.bytes().unwrap()).await;
        probe.write(chunk.bytes().unwrap()).await;
        let video_chunk = video_only.drain().unwrap();
        video_decoder.write(video_chunk.bytes().unwrap()).await;
        if start.elapsed() >= Duration::from_secs(6) {
            break;
        }
        delivery = recording.recv().await.unwrap();
    }
    let tail = muxer.finish().unwrap();
    let video_tail = video_only.finish().unwrap();
    video_decoder.write(video_tail.bytes().unwrap()).await;
    drop(video_only);
    let accepted = muxer.accepted_bytes();
    decoder.write(tail.bytes().unwrap()).await;
    probe.write(tail.bytes().unwrap()).await;
    assert!(accepted > 0);
    drop(muxer);
    assert!(pool.reserved_bytes() > 0); // copied tail keeps the job's reservation
    let output = decoder.finish().await;
    let decoded = decoded_frames(&output);
    assert_eq!(decoded, expected[0].len());
    let video_output = video_decoder.finish().await;
    assert_eq!(decoded_frames(&video_output), decoded);
    let packet_output = probe.finish().await;
    let report: serde_json::Value = serde_json::from_slice(&packet_output.stdout).unwrap();
    let mut actual = vec![Vec::new(), Vec::new()];
    for packet in report["packets"].as_array().unwrap() {
        let index = packet["stream_index"].as_u64().unwrap() as usize;
        actual[index].push((
            packet["pts"].as_i64().unwrap(),
            packet["dts"].as_i64().unwrap(),
            packet["duration"].as_i64().unwrap(),
        ));
    }
    let mut duration_changes = [0usize; 2];
    for track in 0..2 {
        assert_eq!(actual[track].len(), expected[track].len());
        for (actual, expected) in actual[track].iter().zip(&expected[track]) {
            assert_eq!(
                (actual.0, actual.1),
                (expected.0, expected.1),
                "mux must preserve exact common-origin PTS/DTS"
            );
            duration_changes[track] += usize::from(actual.2 != expected.2);
        }
    }
    let (one, two) = tokio::join!(&mut viewer_one.0, &mut viewer_two.0);
    let (one, two) = (one.unwrap(), two.unwrap());
    assert!(one.1 > two.1); // smaller MTU fragments the same source into more packets
    assert_eq!(source.snapshot().open_attempts, 1);
    cancellation.cancel();
    assert!(matches!(tail.bytes(), Err(Error::Cancelled)));
    cancellation.cancel(); // repeated cancellation is harmless
    drop(tail);
    assert!(pool.reserved_bytes() > 0);
    source.set_enabled(false).await.unwrap();
    assert!(matches!(video_tail.bytes(), Err(Error::Source(_))));
    drop(video_tail);
    assert_eq!(pool.reserved_bytes(), 0);
    drop(recording);
    registry.shutdown().await.unwrap();
    eprintln!(
        "Real Rust outputs: fMP4 decoded={decoded} video packets={} AAC packets={} accepted_bytes={accepted}; exact shifted PTS/DTS preserved; MP4 duration differences video={} AAC={} (presentation policy unresolved); video-only mux decoded same count; RTP viewers decoded={}+{} packetized={}+{}; partial-AU refusal, job/input limits, copied output cancellation/source revocation and reservation release passed",
        expected[0].len(),
        expected[1].len(),
        duration_changes[0],
        duration_changes[1],
        one.0,
        two.0,
        one.1,
        two.1
    );
}
