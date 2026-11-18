//! Shared-reader live video. No child producer, local RTP relay, or camera URL.
use super::*;
mod audio;
use amcrust::{
    media::{
        presentation::PresentationClock,
        rtp::{Config, H264},
    },
    source::{Source, SubscriptionLimits, Tracks},
};
pub(super) use audio::{Audio, Encoders, spawn as spawn_audio};
use std::sync::Mutex as SyncMutex;
use std::time::Duration;

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn stop_joins_pending_camera_startup_without_waiting_for_media() {
        use amcrust::{
            ingest::CameraUrl,
            source::{Encoder, Limits, Registry, SourceKey, Timing},
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
                CameraUrl::new(format!("rtsp://{}/stalled", listener.local_addr().unwrap()))
                    .unwrap(),
            )
            .unwrap();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let metrics = Metrics::new();
        let proxy = spawn(
            Video {
                clock: PresentationClock::new(),
                source: source.clone(),
                profile: VideoProfile::LIVE_1080P,
                socket,
                peer: "127.0.0.1:9".parse().unwrap(),
                key: vec![7; 30],
                ssrc: 42,
                identity: [0; 16],
                mtu: 1200,
                payload: 99,
                interval: Duration::from_secs(1),
            },
            metrics.clone(),
        );
        let (stalled, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let audio_pool = amcrust::media::audio::Pool::new(1, 2 * 1024 * 1024).unwrap();
        let audio_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let audio_address = audio_socket.local_addr().unwrap();
        let audio = spawn_audio(
            Audio {
                clock: PresentationClock::new(),
                source: source.clone(),
                socket: audio_socket,
                peer: "127.0.0.1:9".parse().unwrap(),
                key: vec![8; 30],
                ssrc: 43,
                identity: [0; 16],
                mtu: 1200,
                payload: 110,
                interval: Duration::from_secs(1),
                config: Default::default(),
                pool: audio_pool.clone(),
                encoders: Default::default(),
            },
            metrics.clone(),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::time::timeout(Duration::from_secs(1), audio.stop())
            .await
            .unwrap();
        let _audio_released = tokio::net::UdpSocket::bind(audio_address).await.unwrap();
        assert_eq!(audio_pool.reserved_bytes(), 0);
        tokio::time::timeout(Duration::from_secs(1), proxy.stop())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), registry.shutdown())
            .await
            .unwrap()
            .unwrap();
        drop(stalled);
        let _released = tokio::net::UdpSocket::bind(address).await.unwrap();
        assert_eq!(source.snapshot().charged_bytes, 0);
        assert!(!source.snapshot().active);
        assert!(
            metrics
                .render(false)
                .contains("amcrust_connections_open{type=\"live_stream\"} 0")
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "real camera; serial read-only production video sharing and independent stop"]
    async fn real_shared_video_owners() {
        use amcrust::{
            ingest::CameraUrl,
            source::{Encoder, Limits, Registry, SourceKey, Timing},
        };
        use webrtc_srtp::{context::Context, protection_profile::ProtectionProfile};
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
        let a = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let b = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let make = |socket, peer, ssrc| Video {
            clock: PresentationClock::new(),
            source: source.clone(),
            profile: VideoProfile::LIVE_1080P,
            socket,
            peer,
            key: vec![7; 30],
            ssrc,
            identity: [1; 16],
            mtu: 1200,
            payload: 99,
            interval: Duration::from_millis(500),
        };
        let owner_a = spawn(
            make(
                tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                a.local_addr().unwrap(),
                42,
            ),
            metrics.clone(),
        );
        let owner_b = spawn(
            make(
                tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                b.local_addr().unwrap(),
                43,
            ),
            metrics.clone(),
        );
        let mut ca = Context::new(
            &[7; 16],
            &[7; 14],
            ProtectionProfile::Aes128CmHmacSha1_80,
            None,
            None,
        )
        .unwrap();
        let mut cb = Context::new(
            &[7; 16],
            &[7; 14],
            ProtectionProfile::Aes128CmHmacSha1_80,
            None,
            None,
        )
        .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(20), async {
            let mut bytes = [0; 2048];
            let mut counts = [0usize; 2];
            while counts[0] < 100 {
                let n = a.recv(&mut bytes).await.unwrap();
                // Drain A first; B is drained below, well within the source queue budget.
                if (192..=223).contains(&bytes[1]) { ca.decrypt_rtcp(&bytes[..n]).unwrap(); }
                else { ca.decrypt_rtp(&bytes[..n]).unwrap(); counts[0] += 1; }
                if counts[0] >= 100 { break; }
            }
            assert_eq!(source.snapshot().open_attempts, 1);
            owner_a.stop().await;
            let before = source.clock_snapshot().unwrap().tracks[0].packets;
            while source.clock_snapshot().unwrap().tracks[0].packets < before + 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            // Fresh source input advanced after A stopped; B is still consuming it.
            let mut timestamps = std::collections::BTreeSet::new();
            while timestamps.len() < 3 {
                let n = b.recv(&mut bytes).await.unwrap();
                if (192..=223).contains(&bytes[1]) { cb.decrypt_rtcp(&bytes[..n]).unwrap(); }
                else {
                    let packet = cb.decrypt_rtp(&bytes[..n]).unwrap();
                    timestamps.insert(u32::from_be_bytes(packet[4..8].try_into().unwrap()));
                    counts[1] += 1;
                }
            }
            assert!(source.snapshot().active);
            assert_eq!(source.snapshot().open_attempts, 1);
            eprintln!("Production shared video: authenticated RTP={counts:?}, B timestamps={}, one acquisition, A stop did not stop B", timestamps.len());
            owner_b.stop().await;
        }).await;
        registry.shutdown().await.unwrap();
        result.expect("shared video deadline");
        assert_eq!(source.snapshot().charged_bytes, 0);
    }

    #[tokio::test]
    async fn finished_owner_does_not_pin_transport_resources() {
        let control = Control::pending();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let transport = Arc::new(
            Transport::new(socket, "127.0.0.1:9".parse().unwrap(), &[7; 30], 42, 1200).unwrap(),
        );
        let weak = Arc::downgrade(&transport);
        control.install(transport).unwrap();
        assert!(weak.upgrade().is_some());
        control.revoke();
        assert!(weak.upgrade().is_none());
        // Concurrent native-child startup can briefly inherit a CLOEXEC descriptor
        // between fork/spawn and exec. Ownership is gone above; allow bounded OS
        // descriptor-close scheduling rather than assuming immediate port reuse.
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match tokio::net::UdpSocket::bind(address).await {
                    Ok(socket) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Err(error) => panic!("released transport socket: {error}"),
                }
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn pending_stop_cannot_install_a_late_transport() {
        let control = Control::pending();
        control.revoke();
        let transport = Arc::new(
            Transport::new(
                tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                "127.0.0.1:9".parse().unwrap(),
                &[7; 30],
                42,
                1200,
            )
            .unwrap(),
        );
        assert!(matches!(
            control.install(transport.clone()),
            Err(TransportError::Closed)
        ));
        assert!(transport.is_closed());
        tokio::time::timeout(Duration::from_millis(50), control.cancelled())
            .await
            .unwrap();
    }
}

#[derive(Clone)]
pub(super) struct Control(Arc<Pending>);
struct Pending {
    state: SyncMutex<(bool, Option<Arc<Transport>>)>,
    closed: tokio::sync::watch::Sender<bool>,
}
impl Control {
    pub(super) fn pending() -> Self {
        Self(Arc::new(Pending {
            state: SyncMutex::new((false, None)),
            closed: tokio::sync::watch::channel(false).0,
        }))
    }
    pub(super) fn revoke(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.0 = true;
        if let Some(transport) = state.1.take() {
            transport.revoke();
        }
        self.0.closed.send_replace(true);
    }
    pub(super) fn is_closed(&self) -> bool {
        *self.0.closed.borrow()
    }
    fn install(&self, transport: Arc<Transport>) -> Result<(), TransportError> {
        let mut state = self.0.state.lock().unwrap();
        if state.0 || state.1.is_some() {
            transport.revoke();
            return Err(TransportError::Closed);
        }
        state.1 = Some(transport);
        Ok(())
    }
    async fn cancelled(&self) {
        let mut closed = self.0.closed.subscribe();
        if *closed.borrow() {
            return;
        }
        let _ = closed.changed().await;
    }
}
struct Finish(Control);
impl Drop for Finish {
    fn drop(&mut self) {
        self.0.revoke();
    }
}

pub(super) struct Video {
    pub clock: PresentationClock,
    pub source: Source,
    pub profile: VideoProfile,
    pub socket: tokio::net::UdpSocket,
    pub peer: SocketAddr,
    pub key: Vec<u8>,
    pub ssrc: u32,
    pub identity: [u8; 16],
    pub mtu: usize,
    pub payload: u8,
    pub interval: Duration,
}
pub(super) fn spawn(video: Video, metrics: Arc<Metrics>) -> MediaProxy {
    let source = video.source.clone();
    let control = Control::pending();
    let owner = control.clone();
    let task_metrics = metrics.clone();
    metrics.live_connection_opened();
    let connection = LiveConnection(metrics.clone());
    let task = tokio::spawn(async move {
        let _connection = connection;
        let _finish = Finish(owner.clone());
        let result = tokio::select! {
            biased;
            _ = owner.cancelled() => Ok(()),
            result = run(video, &owner, &task_metrics) => result,
        };
        if let Err(error) = result {
            if !owner.is_closed() {
                task_metrics.error(ErrorSubsystem::LiveStream);
                warn!("shared video stopped: {error}");
            }
        }
    });
    MediaProxy {
        source,
        transport: control,
        task: Some(task),
    }
}
type Failure = Box<dyn std::error::Error + Send + Sync>;
async fn run(video: Video, control: &Control, metrics: &Metrics) -> Result<(), Failure> {
    let started = std::time::Instant::now();
    let clock = video.clock;
    let mut subscription = video
        .source
        .subscribe(Tracks::Video, SubscriptionLimits::default())
        .await?;
    let first = subscription.recv().await?;
    let stream = &first.configuration()?[first.packet()?.stream_index()];
    if stream.parameters.dimensions()
        != (
            i32::from(video.profile.width),
            i32::from(video.profile.height),
        )
    {
        return Err("source dimensions no longer match the negotiated native profile".into());
    }
    let transport = Arc::new(Transport::new_source_bound(
        video.socket,
        video.peer,
        &video.key,
        video.ssrc,
        video.mtu,
        &first,
    )?);
    control.install(transport.clone())?;
    let mut packetizer = H264::new_presented(
        &first,
        Config {
            ssrc: video.ssrc,
            payload_type: video.payload,
            packet_bytes: transport.rtp_capacity(),
        },
        clock,
    )?;
    let reports = rtcp::Sender::new(video.ssrc, video.identity);
    let mut receiver = rtcp::Receiver::new(video.ssrc);
    let mut interval = tokio::time::interval(video.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut delivery = first;
    let mut bytes = vec![0; transport.rtp_capacity()];
    let mut first_packet = true;
    let mut first_idr = true;
    loop {
        {
            let mut unit = packetizer.begin(&delivery)?;
            while let Some(size) = unit.next_packet(&mut bytes)? {
                transport
                    .send_rtp(&bytes[..size], || delivery.packet().is_ok())
                    .await?;
                if first_packet {
                    first_packet = false;
                    metrics.live_first_packet(false, started.elapsed());
                }
                if first_idr && h264_contains_idr(&bytes[..size]) {
                    first_idr = false;
                    metrics.live_first_packet(true, started.elapsed());
                }
            }
        }
        drop(delivery);
        loop {
            tokio::select! {
                next = subscription.recv() => { delivery = next?; break; },
                _ = interval.tick() => {
                    transport.send_rtcp(&reports.report(packetizer.sender_clock()?, transport.rtp_counts()), || true).await?;
                },
                feedback = transport.feedback() => {
                    let packet = match feedback {
                        Ok(packet) => packet,
                        Err(TransportError::Authentication | TransportError::TooLarge) => { metrics.feedback_refused(); continue; },
                        Err(error) => return Err(error.into()),
                    };
                    match receiver.receive(&packet) {
                        Ok(feedback) => { metrics.receiver_feedback(feedback); if feedback.bye { return Ok(()); } },
                        Err(_) => metrics.feedback_refused(),
                    }
                }
            }
        }
    }
}
