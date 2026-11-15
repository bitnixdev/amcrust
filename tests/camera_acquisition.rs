//! Opt-in real-device check. No fixtures, encoder writes, playback claim or capture.
use amcrust::ingest::{CameraUrl, Cancellation, Codec, Error, Limits, Reader};
use std::time::{Duration, Instant};

#[test]
#[ignore = "requires coordinated access to the authorized camera and existing credentials"]
fn real_camera_acquisition() {
    dotenvy::dotenv().ok();
    let username = std::env::var("AMCREST_USERNAME").expect("AMCREST_USERNAME required");
    let password = std::env::var("AMCREST_PASSWORD").expect("AMCREST_PASSWORD required");
    // Deliberately fixed authorized target. Never print the constructed URL.
    let url = CameraUrl::new(format!(
        "rtsp://{username}:{password}@10.76.14.10:554/cam/realmonitor?channel=1&subtype=0"
    ))
    .unwrap();
    let cancellation = Cancellation::default();
    let mut reader = Reader::open(url, 1, cancellation.clone(), Limits::default()).unwrap();
    assert!(reader.streams().iter().any(|s| s.codec == Codec::H264));
    assert!(reader.streams().iter().any(|s| s.codec == Codec::Aac));
    for stream in reader.streams() {
        eprintln!(
            "stream={} codec={:?} time_base={}/{} dimensions={:?} audio={:?} configuration_bytes={}",
            stream.index,
            stream.codec,
            stream.time_base.numerator,
            stream.time_base.denominator,
            stream.parameters.dimensions(),
            stream.parameters.audio_format(),
            stream.parameters.extradata().len()
        );
    }
    let start = Instant::now();
    let (mut packets, mut missing_pts, mut missing_dts, mut reports) = (0, 0, 0, 0);
    let mut first = None;
    while start.elapsed() < Duration::from_secs(5) {
        let packet = reader.read().unwrap();
        packets += 1;
        missing_pts += usize::from(packet.pts().is_none());
        missing_dts += usize::from(packet.dts().is_none());
        reports += usize::from(packet.sender_report().is_some());
        if first.is_none() {
            first = Some(packet);
        }
    }
    assert!(packets > 0);
    let cancel_start = Instant::now();
    let cancel_thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        cancellation.cancel();
    });
    loop {
        match reader.read() {
            Ok(_) => (),
            Err(Error::Cancelled) => break,
            Err(error) => panic!("unexpected cancellation outcome: {error}"),
        }
    }
    cancel_thread.join().unwrap();
    assert!(cancel_start.elapsed() < Duration::from_secs(2));
    assert!(matches!(reader.read(), Err(Error::Cancelled)));
    let configuration = reader.streams().to_vec();
    drop(reader);
    // Both owners must remain valid after the input has been closed.
    assert!(!first.unwrap().data().is_empty());
    assert!(!configuration[0].parameters.extradata().is_empty());
    eprintln!(
        "Rust libavformat: packets={packets} missing_pts={missing_pts} missing_dts={missing_dts} packets_with_sr={reports}; cancellation and post-close ownership checked"
    );
}
