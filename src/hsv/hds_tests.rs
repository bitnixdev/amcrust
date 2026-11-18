use super::*;
fn recording_source(url: String) -> (amcrust::source::Registry, amcrust::source::Source) {
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
            amcrust::ingest::CameraUrl::new(url).unwrap(),
        )
        .unwrap();
    (registry, source)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real north-driveway; serial read-only recording/HDS fence, not Apple acceptance"]
async fn real_recording_bytes_are_fenced_after_stop() {
    use crate::hsv::fmp4::{Recorder, RecorderConfig};
    dotenvy::dotenv().ok();
    let user = std::env::var("AMCREST_USERNAME").unwrap();
    let password = std::env::var("AMCREST_PASSWORD").unwrap();
    let (mut registry, source) = recording_source(format!(
        "rtsp://{user}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype=0"
    ));
    let mut viewer = source
        .subscribe(amcrust::source::Tracks::Video, Default::default())
        .await
        .unwrap();
    let first = viewer.recv().await.unwrap();
    let (width, height) = first.configuration().unwrap()[first.packet().unwrap().stream_index()]
        .parameters
        .dimensions();
    drop(first);
    let (stop_viewer, stopped) = tokio::sync::oneshot::channel::<()>();
    let viewer = tokio::spawn(async move {
        tokio::pin!(stopped);
        let mut count = 0;
        loop {
            tokio::select! { _ = &mut stopped => break, packet = viewer.recv() => { if packet.is_err() { break; } count += 1; } }
        }
        count
    });
    let recorder = Recorder::new(source.clone(), Metrics::new());
    recorder
        .start(RecorderConfig {
            width: width.try_into().unwrap(),
            height: height.try_into().unwrap(),
            audio: true,
            fps: 15,
            audio_sample_rate_hz: 48000,
            audio_bitrate: 64000,
            fragment_ms: 4000,
            prebuffer_ms: 4000,
        })
        .await;
    let mut clip = timeout(Duration::from_secs(25), async {
        loop {
            if let Ok(clip) = recorder.open().await {
                break clip;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let (init, fragment) = timeout(Duration::from_secs(5), async {
        let init = match clip.messages.recv().await.unwrap() {
            crate::hsv::fmp4::Message::Init(init) => init,
            _ => panic!("missing init"),
        };
        let fragment = match clip.messages.recv().await.unwrap() {
            crate::hsv::fmp4::Message::Media(fragment) => fragment,
            _ => panic!("missing media"),
        };
        (init, fragment)
    })
    .await
    .unwrap();
    assert_eq!(source.snapshot().open_attempts, 1);
    let (writer, mut peer) = pair().await;
    let writer = Arc::new(Mutex::new(writer));
    let expected = init.data.len() + fragment.data.len();
    let receive = tokio::spawn(async move {
        let mut counter = 0;
        let mut received = 0;
        while received < expected {
            let mut bytes = vec![0; 4];
            peer.read_exact(&mut bytes).await.unwrap();
            let length =
                ((bytes[1] as usize) << 16) | ((bytes[2] as usize) << 8) | bytes[3] as usize;
            bytes.resize(4 + length + 16, 0);
            peer.read_exact(&mut bytes[4..]).await.unwrap();
            let plain = frame::decrypt_frame(&[7; 32], &mut counter, &bytes).unwrap();
            let message = codec::Decoder::new(&plain[1 + plain[0] as usize..])
                .decode()
                .unwrap()
                .unwrap();
            let Some(Value::Array(packets)) = message.get("packets") else {
                panic!("missing packet metadata");
            };
            for packet in packets {
                let Some(Value::Data(data)) = packet.get("data") else {
                    panic!("missing media bytes");
                };
                received += data.len();
            }
        }
        (received, counter, peer)
    });
    let session = Lease::new();
    let result = async {
        send_data(
            &writer,
            1,
            &init.data,
            "mediaInitialization",
            1,
            false,
            (&init.lease, &session),
        )
        .await?;
        send_data(
            &writer,
            1,
            &fragment.data,
            "mediaFragment",
            2,
            false,
            (&init.lease, &session),
        )
        .await?;
        Ok::<_, String>(())
    }
    .await;
    source.set_enabled(false).await.unwrap();
    recorder.stop().await;
    clip.stop().await;
    let _ = stop_viewer.send(());
    assert!(viewer.await.unwrap() > 0);
    registry.shutdown().await.unwrap();
    result.unwrap();
    let (received, frames, mut peer) = timeout(Duration::from_secs(3), receive)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received, expected);
    assert!(!init.lease.current());
    assert!(!fragment.lease.current());
    assert!(
        send_data(
            &writer,
            1,
            &fragment.data,
            "mediaFragment",
            3,
            false,
            (&init.lease, &session)
        )
        .await
        .is_err()
    );
    assert!(
        timeout(Duration::from_millis(30), peer.read_u8())
            .await
            .is_err()
    );
    drop(init);
    drop(fragment);
    drop(clip);
    assert_eq!(source.snapshot().charged_bytes, 0);
    eprintln!(
        "Recording HDS: {received} actual fMP4 bytes in {frames} authenticated frames; one acquisition; joined stop; retained old fragment refused after source retirement; zero released-output charges"
    );
}

async fn pair() -> (FrameWriter, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    let (_, write_half) = socket.into_split();
    (
        FrameWriter {
            write_half,
            key: [7; 32],
            counter: 0,
            poisoned: false,
        },
        peer,
    )
}

#[tokio::test]
async fn authenticated_frame_then_retired_copy_cannot_write() {
    let (mut writer, mut peer) = pair().await;
    let epoch = Lease::new();
    let session = Lease::new();
    let header = Value::dict(vec![("protocol", Value::String("dataSend".into()))]);
    let message = Value::Data(vec![5; 100]);
    writer
        .send_fenced(&header, &message, Some((&epoch, &session)))
        .await
        .unwrap();
    let mut encrypted = vec![0; 4];
    peer.read_exact(&mut encrypted).await.unwrap();
    let size =
        ((encrypted[1] as usize) << 16) | ((encrypted[2] as usize) << 8) | encrypted[3] as usize;
    encrypted.resize(4 + size + 16, 0);
    peer.read_exact(&mut encrypted[4..]).await.unwrap();
    let mut counter = 0;
    let plain = frame::decrypt_frame(&[7; 32], &mut counter, &encrypted).unwrap();
    assert_eq!(counter, 1);
    assert!(!plain.is_empty());
    epoch.revoke();
    assert!(
        writer
            .send_fenced(&header, &message, Some((&epoch, &session)))
            .await
            .is_err()
    );
    assert!(writer.poisoned);
    assert!(writer.send(&header, &message).await.is_err());
    assert!(
        timeout(Duration::from_millis(30), peer.read_u8())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revocation_wakes_slow_tcp_writer_and_poisons_partial_frame() {
    let (mut writer, _unread_peer) = pair().await;
    let epoch = Lease::new();
    let session = Lease::new();
    let copy = epoch.clone();
    let task = tokio::spawn(async move {
        let header = Value::Dict(vec![]);
        let message = Value::Data(vec![0; MAX_CHUNK]);
        loop {
            if writer
                .send_fenced(&header, &message, Some((&copy, &session)))
                .await
                .is_err()
            {
                break;
            }
        }
        writer
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    epoch.revoke();
    let mut writer = timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(writer.poisoned);
    assert!(writer.counter > 0);
    assert!(
        writer
            .send(&Value::Dict(vec![]), &Value::Dict(vec![]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn shutdown_joins_unidentified_connections_and_closes_admission() {
    use crate::amcrest::{AmcrestClient, EncoderCapabilities};
    let metrics = Metrics::new();
    let path = std::env::temp_dir().join(format!("amcrust-unused-hds-{}", rand::random::<u64>()));
    let (mut registry, source) = recording_source("rtsp://127.0.0.1:9/main".into());
    let state = HsvState::load(
        "test".into(),
        path.to_str().unwrap(),
        AmcrestClient::new("127.0.0.1".into(), "unused".into(), "unused".into()),
        Arc::new(AtomicBool::new(false)),
        metrics.clone(),
        EncoderCapabilities::default(),
        source,
    );
    let server = HdsServer::new("test".into(), 0, state, metrics.clone());
    let (port, _) = server.setup(&[3; 32], &[4; 32]).await.unwrap();
    let mut peer = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Delay completion after the real connection has drained; cancel one waiter.
    let (joined, observed) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    {
        let mut inner = server.inner.lock().await;
        let mut listener = inner.listener.take().unwrap();
        let original = listener.task;
        listener.task = tokio::spawn(async move {
            original.await.unwrap();
            joined.send(()).unwrap();
            wait.await.unwrap();
        });
        inner.listener = Some(listener);
    }
    let first = tokio::spawn({
        let s = server.clone();
        async move { s.shutdown().await }
    });
    timeout(Duration::from_secs(1), observed)
        .await
        .unwrap()
        .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(server.setup(&[3; 32], &[4; 32]).await.is_err());
    assert!(
        timeout(Duration::from_millis(20), server.shutdown())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    timeout(Duration::from_secs(1), server.shutdown())
        .await
        .unwrap();
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    assert!(server.setup(&[3; 32], &[4; 32]).await.is_err());
    server.shutdown().await;
    registry.shutdown().await.unwrap();
    assert!(
        metrics
            .render(false)
            .contains("amcrust_connections_open{type=\"data_stream\"} 0")
    );
}
