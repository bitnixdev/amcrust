//! Real shared-source retirement against actual secure UDP writes. No encoder
//! writes, retained media, invented RTCP clocks, or playback acceptance claim.
use amcrust::{
    ingest::CameraUrl,
    media::{
        rtp::{Config, H264},
        transport::{Error, Transport},
    },
    source::{Delivery, Encoder, Limits, Registry, SourceKey, SubscriptionLimits, Tracks},
};
use std::{sync::Arc, time::Duration};
use tokio::net::UdpSocket;
use webrtc_srtp::{context::Context, protection_profile::ProtectionProfile};

type Failure = Box<dyn std::error::Error + Send + Sync>;

/// Fail two native setup attempts, then relay the same endpoint to the camera.
/// No source reset/reconfiguration and no media or credential logging in the relay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real north-driveway camera; coordinated serial read-only access"]
async fn real_cooldown_recovers_media_without_operator_reset() {
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").unwrap();
    let password = std::env::var("AMCREST_PASSWORD").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut relay = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await?;
            drop(socket);
        }
        let (mut local, _) = listener.accept().await?;
        let mut camera = tokio::net::TcpStream::connect("10.76.14.10:554").await?;
        tokio::io::copy_bidirectional(&mut local, &mut camera).await?;
        Ok::<_, std::io::Error>(())
    });
    let mut registry = Registry::new(Limits {
        retry_initial: Duration::from_millis(5),
        retry_max: Duration::from_millis(10),
        retry_cooldown: Duration::from_millis(250),
        failures_before_reset: 1,
        ..Default::default()
    })
    .unwrap();
    let url = CameraUrl::new(format!(
        "rtsp://{user}:{password}@{address}/cam/realmonitor?channel=1&subtype=0"
    ))
    .unwrap();
    let source = registry
        .register(
            SourceKey {
                camera: 1,
                encoder: Encoder::Main,
            },
            url,
        )
        .unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(30), async {
        let mut old = Vec::new();
        for _ in 0..2 {
            let mut subscription = source.subscribe(Tracks::Video, SubscriptionLimits::default()).await?;
            if subscription.recv().await.is_ok() { return Err("injected setup failure unexpectedly delivered media".into()); }
            old.push(subscription);
            if !matches!(source.subscribe(Tracks::Video, SubscriptionLimits::default()).await, Err(amcrust::source::Terminal::RetryExhausted)) {
                return Err("missing retry cooldown".into());
            }
            tokio::time::sleep(Duration::from_millis(325)).await;
        }
        let mut fresh = source.subscribe(Tracks::Video, SubscriptionLimits::default()).await?;
        let first = fresh.recv().await?;
        if first.packet()?.generation() != 3 || source.snapshot().open_attempts != 3 {
            return Err("recovery did not create the third acquisition generation".into());
        }
        let (transport, peer) = bound(&first, 45).await?;
        let copied = access_unit(&first, &transport, &peer, 45).await?;
        drop(first);
        for mut subscription in old {
            if subscription.recv().await.is_ok() { return Err("old subscription revived during recovery".into()); }
        }
        drop(fresh);
        refused(&transport, &peer, &copied).await?;
        eprintln!("Recovered real media after two setup failures without operator reset: authenticated RTP packets={}", transport.rtp_counts().packets);
        Ok::<_, Failure>(())
    }).await;
    registry.shutdown().await.unwrap();
    let relay_result = tokio::time::timeout(Duration::from_secs(2), &mut relay).await;
    if relay_result.is_err() {
        relay.abort();
        let _ = relay.await;
    }
    eprintln!(
        "Cooldown-to-media outcome: {outcome:?} {:?}",
        source.snapshot()
    );
    assert_eq!(source.snapshot().charged_bytes, 0);
    outcome
        .expect("recovery deadline")
        .expect("real media recovery");
    // libavformat may close with unread interleaved media, producing TCP RST.
    // This is acceptable ONLY after recovery succeeded and registry shutdown joined.
    if let Err(error) = relay_result
        .expect("relay shutdown deadline")
        .expect("relay task")
    {
        assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ),
            "unexpected relay shutdown: {error}"
        );
    }
}

async fn bound(first: &Delivery, ssrc: u32) -> Result<(Arc<Transport>, UdpSocket), Failure> {
    let peer = UdpSocket::bind("127.0.0.1:0").await?;
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    Ok((
        Arc::new(Transport::new_source_bound(
            socket,
            peer.local_addr()?,
            &[7; 30],
            ssrc,
            1200,
            first,
        )?),
        peer,
    ))
}
async fn access_unit(
    first: &Delivery,
    transport: &Transport,
    peer: &UdpSocket,
    ssrc: u32,
) -> Result<Vec<u8>, Failure> {
    let mut packetizer = H264::new(
        first,
        Config {
            ssrc,
            payload_type: 99,
            packet_bytes: transport.rtp_capacity(),
        },
    )?;
    let mut decoder = Context::new(
        &[7; 16],
        &[7; 14],
        ProtectionProfile::Aes128CmHmacSha1_80,
        None,
        None,
    )?;
    let mut unit = packetizer.begin(first)?;
    let mut output = [0; 1190];
    let mut wire = [0; 2048];
    let mut copied = Vec::new();
    while let Some(size) = unit.next_packet(&mut output)? {
        transport
            .send_rtp(&output[..size], || first.packet().is_ok())
            .await?;
        let size_on_wire = peer.recv(&mut wire).await?;
        let plain = decoder.decrypt_rtp(&wire[..size_on_wire])?;
        if plain.as_ref() != &output[..size] {
            return Err("SRTP round trip differed".into());
        }
        copied.clear();
        copied.extend_from_slice(&output[..size]);
    }
    if copied.is_empty() {
        return Err("empty IDR".into());
    }
    Ok(copied)
}
async fn refused(transport: &Transport, peer: &UdpSocket, copied: &[u8]) -> Result<(), Failure> {
    let counts = transport.rtp_counts();
    if !transport.is_closed()
        || !matches!(
            transport.send_rtp(copied, || true).await,
            Err(Error::Closed)
        )
    {
        return Err("copied RTP survived retirement".into());
    }
    let bye = amcrust::media::rtcp::Sender::new(42, [0; 16]).bye();
    if !matches!(transport.send_rtcp(&bye, || true).await, Err(Error::Closed)) {
        return Err("RTCP survived retirement".into());
    }
    if counts != transport.rtp_counts() {
        return Err("accepted-write count changed after retirement".into());
    }
    if tokio::time::timeout(Duration::from_millis(30), peer.recv(&mut [0; 2048]))
        .await
        .is_ok()
    {
        return Err("unexpected post-retirement datagram".into());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real north-driveway camera; coordinated serial read-only access"]
async fn real_source_transport_fences() {
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
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let limits = SubscriptionLimits { residence: Duration::from_secs(10), media_span: Duration::from_secs(10), ..Default::default() };
        let mut one = source.subscribe(Tracks::Video, limits).await?;
        let mut two = source.subscribe(Tracks::Video, limits).await?;
        let (first, second) = tokio::join!(one.recv(), two.recv());
        let (first, second) = (first?, second?);
        let (a, peer_a) = bound(&first, 42).await?;
        let (b, peer_b) = bound(&second, 43).await?;
        let copied_a = access_unit(&first, &a, &peer_a, 42).await?;
        drop(first);
        let pending = a.feedback();
        tokio::pin!(pending);
        // Poll receive before retirement, rather than only testing an already closed call.
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut pending).await.is_err());
        drop(one);
        if !matches!(pending.await, Err(Error::Closed)) { return Err("pending feedback survived cancellation".into()); }
        refused(&a, &peer_a, &copied_a).await?;
        let copied_b = access_unit(&second, &b, &peer_b, 43).await?;
        drop(second);
        if source.snapshot().open_attempts != 1 { return Err("viewer isolation reopened acquisition".into()); }
        source.set_enabled(false).await?;
        refused(&b, &peer_b, &copied_b).await?;
        drop(two);
        source.set_enabled(true).await?;
        let mut fresh = source.subscribe(Tracks::Video, limits).await?;
        let first = fresh.recv().await?;
        if first.packet()?.generation() != 2 { return Err("missing fresh generation".into()); }
        let (c, peer_c) = bound(&first, 44).await?;
        let copied_c = access_unit(&first, &c, &peer_c, 44).await?;
        drop(first);
        refused(&b, &peer_b, &copied_b).await?; // A new generation never revives an old transport.
        drop(fresh);
        refused(&c, &peer_c, &copied_c).await?;
        eprintln!("Real source fences: authenticated RTP packets per transport={}/{}/{}; two viewers shared one acquisition; cancellation/disable/copy fencing and fresh-generation isolation passed", a.rtp_counts().packets, b.rtp_counts().packets, c.rtp_counts().packets);
        Ok::<_, Failure>(())
    }).await;
    registry.shutdown().await.unwrap();
    eprintln!("Source fence outcome: {result:?} {:?}", source.snapshot());
    assert_eq!(source.snapshot().charged_bytes, 0);
    result
        .expect("source fence deadline")
        .expect("source transport fences");
}
