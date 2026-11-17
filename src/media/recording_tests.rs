use super::*;

#[test]
fn recovered_video_keeps_every_admitted_frame_through_drift_and_idr_bursts() {
    for ppm in [-1000i128, 1000] {
        let clock = PresentationClock::new();
        let start = Instant::now();
        let mut timing = VideoClock::new(start + Duration::from_millis(145), 15, clock);
        let mut previous_arrival = start;
        let mut previous_end = None;
        let mut count = 0;
        for frame in 0..(6 * 3600 * 15u64) {
            let ns = i128::from(frame) * 1_000_000_000 * (1_000_000 + ppm) / (15 * 1_000_000);
            let idr = frame % 60 == 0;
            let arrival = (start
                + Duration::from_nanos(ns as u64)
                + Duration::from_millis(if idr { 145 } else { 2 }))
            .max(previous_arrival + Duration::from_micros(1));
            previous_arrival = arrival;
            if let Some((pts, duration)) = timing.observe(arrival, idr).unwrap() {
                if let Some(previous) = previous_end {
                    assert_eq!(pts, previous);
                } else {
                    assert!(idr);
                }
                assert!((5970..=6030).contains(&duration));
                previous_end = Some(pts + duration);
                count += 1;
            }
        }
        assert_eq!(count, 6 * 3600 * 15 - 60);
        assert!(timing.recovery.as_ref().unwrap().status().phase_48k.abs() <= 4800);
        assert_eq!(timing.warm_frames, 61);
    }
}
#[test]
fn video_admission_and_unrecoverable_gaps_fail_without_reanchoring() {
    let start = Instant::now();
    let clock = PresentationClock::new();
    let mut timing = VideoClock::new(start, 15, clock);
    assert_eq!(timing.observe(start, true).unwrap(), None);
    for n in 1..30 {
        assert_eq!(
            timing
                .observe(start + Duration::from_nanos(n * 1_000_000_000 / 15), false)
                .unwrap(),
            None
        );
    }
    let first = timing
        .observe(start + Duration::from_secs(2), true)
        .unwrap()
        .unwrap();
    assert_eq!(first.1, 6000);
    let origin = timing.origin;
    assert_eq!(
        timing.observe(start + Duration::from_secs(3), false),
        Err(Error::Clock)
    );
    assert_eq!(timing.origin, origin);
    let mut absent = VideoClock::new(start, 15, clock);
    assert_eq!(
        absent.observe(start + Duration::from_secs(16), true),
        Err(Error::Deadline)
    );
}
