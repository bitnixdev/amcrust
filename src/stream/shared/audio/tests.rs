use super::*;
use amcrust::{
    ingest::CameraUrl,
    source::{Encoder, Limits, Registry, SourceKey, Timing},
};
use ffmpeg_sys_next as av;
use webrtc_srtp::{context::Context, protection_profile::ProtectionProfile};

#[derive(Default)]
struct ReportClock {
    previous: Option<(u64, u32)>,
    count: usize,
}
impl ReportClock {
    fn observe(&mut self, packet: &[u8], rate: u32) {
        assert!(packet.len() >= 28);
        assert_eq!(packet[1], 200);
        let ntp = u64::from_be_bytes(packet[8..16].try_into().unwrap());
        let rtp = u32::from_be_bytes(packet[16..20].try_into().unwrap());
        if let Some((old_ntp, old_rtp)) = self.previous {
            let wall = i128::from(ntp - old_ntp) * i128::from(rate);
            let ticks = i128::from(rtp.wrapping_sub(old_rtp)) * (1i128 << 32);
            // Only host UTC/monotonic sampling jitter, not a media phase waiver.
            assert!(
                (ticks - wall).abs() <= i128::from(rate) * (1i128 << 32) / 100,
                "RTP and RTCP wire rates disagree"
            );
        }
        self.previous = Some((ntp, rtp));
        self.count += 1;
    }
}

#[test]
fn hap_clock_preserves_offsets_durations_and_wrap_without_reanchoring() {
    for rate in [8000, 16000, 24000, 48000] {
        let clock = HapClock::new(PresentationClock::new(), u32::MAX - 40, rate).unwrap();
        assert_eq!(clock.timestamp(24000).wrapping_sub(clock.origin), rate / 2);
        for ms in [20, 40, 60] {
            for start in [-100000i64, -313, -1, 0, 1, 1i64 << 32, 180 * 86400 * 48000] {
                assert_eq!(
                    clock
                        .timestamp(start + ms * 48)
                        .wrapping_sub(clock.timestamp(start)),
                    rate * ms as u32 / 1000
                );
            }
        }
        let before = std::time::Instant::now();
        let report = clock.report().unwrap();
        let after = std::time::Instant::now();
        let delta = report.rtp.wrapping_sub(clock.origin) as i128;
        assert!(delta >= clock.presentation.ticks(before, rate));
        assert!(delta <= clock.presentation.ticks(after, rate));
    }
    assert!(HapClock::new(PresentationClock::new(), 0, 44100).is_err());
}

struct Decoder(*mut av::AVCodecContext);
impl Drop for Decoder {
    fn drop(&mut self) {
        unsafe {
            av::avcodec_free_context(&mut self.0);
        }
    }
}
impl Decoder {
    fn new() -> Self {
        unsafe {
            let codec = av::avcodec_find_decoder_by_name(c"libopus".as_ptr());
            assert!(!codec.is_null());
            let result = Self(av::avcodec_alloc_context3(codec));
            assert!(!result.0.is_null());
            (*result.0).sample_rate = 48000;
            av::av_channel_layout_default(&mut (*result.0).ch_layout, 1);
            // RTP negotiation supplies channels/rate, not container pre-skip.
            let header = b"OpusHead\x01\x01\x00\x00\x80\xbb\x00\x00\x00\x00\x00";
            (*result.0).extradata =
                av::av_mallocz(header.len() + av::AV_INPUT_BUFFER_PADDING_SIZE as usize).cast();
            assert!(!(*result.0).extradata.is_null());
            std::ptr::copy_nonoverlapping(header.as_ptr(), (*result.0).extradata, header.len());
            (*result.0).extradata_size = header.len() as i32;
            assert_eq!(av::avcodec_open2(result.0, codec, std::ptr::null_mut()), 0);
            result
        }
    }
    fn decode(&mut self, bytes: &[u8]) -> usize {
        unsafe {
            let mut packet = av::av_packet_alloc();
            assert!(!packet.is_null());
            assert_eq!(av::av_new_packet(packet, bytes.len() as i32), 0);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), (*packet).data, bytes.len());
            let result = av::avcodec_send_packet(self.0, packet);
            av::av_packet_free(&mut packet);
            assert_eq!(result, 0);
            let mut frame = av::av_frame_alloc();
            assert!(!frame.is_null());
            let result = av::avcodec_receive_frame(self.0, frame);
            let samples = (*frame).nb_samples;
            av::av_frame_free(&mut frame);
            assert_eq!(result, 0);
            samples as usize
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real camera; serial read-only production A/V, not Apple playback"]
async fn real_production_audio_and_video_share_source() {
    let _ = env_logger::Builder::new()
        .is_test(true)
        .filter_level(log::LevelFilter::Warn)
        .try_init();
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").unwrap();
    let password = std::env::var("AMCREST_PASSWORD").unwrap();
    let mut registry = Registry::new(Limits {
        timing: Timing::Presentation,
        ..Default::default()
    })
    .unwrap();
    let source = registry
        .register(
            SourceKey {
                camera: 1,
                encoder: Encoder::Main,
            },
            CameraUrl::new(format!(
                "rtsp://{user}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype=0"
            ))
            .unwrap(),
        )
        .unwrap();
    let metrics = Metrics::new();
    let video_source = VideoSource {
        source: source.clone(),
        profile: VideoProfile::LIVE_1080P,
    };
    let manager = StreamManager::new(
        vec![video_source.clone()],
        source.clone(),
        true,
        Ipv4Addr::LOCALHOST.into(),
        metrics.clone(),
    );
    let video = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let audio = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let output_audio = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let feedback_address = output_audio.local_addr().unwrap();
    let id = vec![5; 16];
    manager.inner.lock().await.sessions.insert(
        id.clone(),
        Session {
            id: id.clone(),
            controller_ip: "127.0.0.1".into(),
            video_port: video.local_addr().unwrap().port(),
            audio_port: audio.local_addr().unwrap().port(),
            video_key: vec![7; 30],
            audio_key: vec![8; 30],
            video_ssrc: 42,
            audio_ssrc: 43,
            rtcp_identity: [1; 16],
            video_out_socket: Some(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap()),
            audio_out_socket: Some(output_audio),
        },
    );
    manager
        .start_stream(
            Some(id.clone()),
            video_source,
            99,
            1200,
            110,
            16000,
            20,
            24,
            0.5,
            1200,
            0.5,
        )
        .await;
    let seconds: u64 = std::env::var("AMCRUST_LIVE_AUDIO_SECONDS")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(2);
    assert!((2..=1800).contains(&seconds));
    let result = tokio::time::timeout(Duration::from_secs(seconds + 25), async {
        let started = std::time::Instant::now();
        let mut v = Context::new(&[7; 16], &[7; 14], ProtectionProfile::Aes128CmHmacSha1_80, None, None).unwrap();
        let mut a = Context::new(&[8; 16], &[8; 14], ProtectionProfile::Aes128CmHmacSha1_80, None, None).unwrap();
        let mut decoder = Decoder::new();
        let mut vb = [0; 2048]; let mut ab = [0; 2048];
        let (mut videos, mut audios, mut samples) = (0usize, 0usize, 0usize);
        let (mut vr, mut ar) = (false, false);
        let mut reports = ReportClock::default();
        let mut previous: Option<u32> = None;
        while audios < 100 || videos < 100 || !vr || !ar || reports.count < 2 || started.elapsed().as_secs() < seconds {
            tokio::select! {
                n = video.recv(&mut vb) => {
                    let n = n.unwrap();
                    if (192..=223).contains(&vb[1]) { v.decrypt_rtcp(&vb[..n]).unwrap(); vr = true; }
                    else { v.decrypt_rtp(&vb[..n]).unwrap(); videos += 1; }
                },
                n = audio.recv(&mut ab) => {
                    let n = n.unwrap();
                    if (192..=223).contains(&ab[1]) { let report = a.decrypt_rtcp(&ab[..n]).unwrap(); reports.observe(&report, 16000); ar = true; }
                    else {
                        let packet = a.decrypt_rtp(&ab[..n]).unwrap();
                        let timestamp = u32::from_be_bytes(packet[4..8].try_into().unwrap());
                        if let Some(previous) = previous { assert_eq!(timestamp.wrapping_sub(previous), 320); }
                        previous = Some(timestamp);
                        assert_eq!(decoder.decode(&packet[12..]), 960);
                        samples += 960; audios += 1;
                    }
                },
                _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("missing production media"),
            }
            assert!(!manager.inner.lock().await.audio_proxies.get(&id).unwrap().transport.is_closed(), "audio producer failed: {}", metrics.render(false));
        }
        assert_eq!(source.snapshot().open_attempts, 1);
        let bye = [0x81, 203, 0, 1, 0, 0, 0, 99];
        audio.send_to(&a.encrypt_rtcp(&bye).unwrap(), feedback_address).await.unwrap();
        let task = manager.inner.lock().await.audio_proxies.get_mut(&id).unwrap().task.take().unwrap();
        task.await.unwrap();
        assert_eq!(manager.audio_pool.reserved_bytes(), 0);
        assert!(!manager.inner.lock().await.video_proxies.get(&id).unwrap().transport.is_closed());
        assert!(source.snapshot().active);
        eprintln!("Production shared A/V: RTP video={videos} audio={audios}, decoded untrimmed RTP Opus={samples} samples at 48k; encoder/RTP=16k/20ms; one acquisition; audio BYE joined worker/released pool while video stayed active");
    }).await;
    manager.stop_stream().await;
    registry.shutdown().await.unwrap();
    assert_eq!(manager.audio_pool.reserved_bytes(), 0);
    assert_eq!(source.snapshot().charged_bytes, 0);
    result.expect("production A/V deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real camera; serial read-only negotiated audio matrix"]
async fn real_presented_audio_matrix() {
    let _ = env_logger::Builder::new()
        .is_test(true)
        .filter_level(log::LevelFilter::Warn)
        .try_init();
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").unwrap();
    let password = std::env::var("AMCREST_PASSWORD").unwrap();
    let mut registry = Registry::new(Limits {
        timing: Timing::Presentation,
        ..Default::default()
    })
    .unwrap();
    let source = registry
        .register(
            SourceKey {
                camera: 1,
                encoder: Encoder::Main,
            },
            CameraUrl::new(format!(
                "rtsp://{user}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype=0"
            ))
            .unwrap(),
        )
        .unwrap();
    let mut keep = source
        .subscribe(Tracks::Video, SubscriptionLimits::default())
        .await
        .unwrap();
    let keeper = tokio::spawn(async move { while keep.recv().await.is_ok() {} });
    let pool = Pool::new(2, 4 * 1024 * 1024).unwrap();
    let mut index = 10u8;
    for rate in [8000, 16000, 24000, 48000] {
        for frame_ms in [20, 40, 60] {
            index += 1;
            let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let proxy = spawn(
                Audio {
                    clock: PresentationClock::new(),
                    source: source.clone(),
                    socket: tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                    peer: receiver.local_addr().unwrap(),
                    key: vec![index; 30],
                    ssrc: u32::from(index),
                    identity: [index; 16],
                    mtu: 1200,
                    payload: 110,
                    interval: Duration::from_millis(500),
                    config: Config {
                        rate,
                        frame_ms,
                        ..Default::default()
                    },
                    pool: pool.clone(),
                },
                Metrics::new(),
            );
            let result = tokio::time::timeout(Duration::from_secs(10), async {
                let mut crypto = Context::new(&[index; 16], &[index; 14], ProtectionProfile::Aes128CmHmacSha1_80, None, None).unwrap();
                let mut decoder = Decoder::new();
                let mut previous: Option<u32> = None;
                let mut buffer = [0; 2048];
                let mut count = 0;
                let mut reports = ReportClock::default();
                while count < 25 || reports.count < 2 {
                    let n = receiver.recv(&mut buffer).await.unwrap();
                    if (192..=223).contains(&buffer[1]) { let report = crypto.decrypt_rtcp(&buffer[..n]).unwrap(); reports.observe(&report, rate); continue; }
                    let packet = crypto.decrypt_rtp(&buffer[..n]).unwrap();
                    let timestamp = u32::from_be_bytes(packet[4..8].try_into().unwrap());
                    let samples = u32::from(frame_ms) * 48;
                    if let Some(previous) = previous { assert_eq!(timestamp.wrapping_sub(previous), rate * u32::from(frame_ms) / 1000); }
                    previous = Some(timestamp);
                    assert_eq!(decoder.decode(&packet[12..]), samples as usize);
                    count += 1;
                }
                eprintln!("Presented live audio: {rate} Hz/{frame_ms} ms, {count} authenticated/decoded packets, negotiated-rate HomeKit RTP/RTCP, 48-kHz decoded samples");
            }).await;
            proxy.stop().await;
            assert_eq!(pool.reserved_bytes(), 0);
            result.expect("negotiated audio deadline");
            assert_eq!(source.snapshot().open_attempts, 1);
        }
    }
    registry.shutdown().await.unwrap();
    keeper.await.unwrap();
    assert_eq!(source.snapshot().charged_bytes, 0);
}
