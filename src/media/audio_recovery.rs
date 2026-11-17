//! Bounded read-time audio rate recovery; not camera/capture-clock reconstruction.
use super::Error;
use std::time::{Duration, Instant};

/// Bounded pre-admission AAC history/probe backlog. Once armed this never
/// discards another decoded frame or changes the chosen presentation anchor.
pub(super) struct Startup {
    first: Instant,
    previous: Option<Instant>,
    skipped: u64,
    armed: bool,
}
impl Startup {
    pub fn new(first: Instant) -> Self {
        Self {
            first,
            previous: None,
            skipped: 0,
            armed: false,
        }
    }
    pub fn admit(&mut self, at: Instant, rate: u32, samples: u32) -> Result<bool, Error> {
        if self.armed {
            return Ok(true);
        }
        if at < self.first || self.previous.is_some_and(|last| at < last) {
            return Err(Error::Clock);
        }
        if at.duration_since(self.first) >= Duration::from_secs(2) {
            return Err(Error::Incomplete);
        }
        let paced = self.previous.is_some_and(|last| {
            let ns = at.duration_since(last).as_nanos();
            ns * rate as u128 >= samples as u128 * 500_000_000
                && ns <= 100_000_000u128.max(samples as u128 * 2_000_000_000 / rate as u128)
        });
        self.previous = Some(at);
        if paced {
            self.armed = true;
            return Ok(true);
        }
        self.skipped += u64::from(samples);
        if self.skipped > u64::from(rate) * 2 {
            return Err(Error::Incomplete);
        }
        Ok(false)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RecoveryStatus {
    pub phase_48k: i128,
    pub peak_phase_48k: u128,
    pub correction_ppm: i32,
    pub frames: u64,
    pub late_observations: u64,
}
pub(super) struct Recovery {
    origin: Instant,
    last: Instant,
    filtered: i128,
    late_since: Option<Instant>,
    status: RecoveryStatus,
}
impl Recovery {
    pub fn new(origin: Instant) -> Self {
        Self {
            origin,
            last: origin,
            filtered: 0,
            late_since: None,
            status: RecoveryStatus::default(),
        }
    }
    pub fn status(&self) -> RecoveryStatus {
        self.status
    }
    /// Proportional rate correction over a ten-second horizon, with a 1/16
    /// low-pass and a hard +/-5000 ppm cap. Never insert silence, drop samples,
    /// move the anchor, or use a camera sender report as the control signal.
    pub fn step(&mut self, at: Instant, projected_48k: i128, rate: u32) -> Result<i32, Error> {
        if at < self.last {
            return Err(Error::Clock);
        }
        let target = at.duration_since(self.origin).as_nanos() as i128 * 48000 / 1_000_000_000;
        self.last = at;
        let phase = target - projected_48k;
        self.status.phase_48k = phase;
        self.status.peak_phase_48k = self.status.peak_phase_48k.max(phase.unsigned_abs());
        self.status.frames = self.status.frames.checked_add(1).ok_or(Error::Limit)?;
        if !(-4800..=24000).contains(&phase) {
            return Err(Error::Clock);
        }
        if phase > 4800 {
            // A delayed read followed by a catch-up burst is not missing PCM.
            // Hold the rate; preserve every frame and the original anchor. A
            // sustained offset cannot be hidden as a succession of new anchors.
            let since = *self.late_since.get_or_insert(at);
            if at.duration_since(since) >= Duration::from_millis(500) {
                return Err(Error::Clock);
            }
            self.status.late_observations = self
                .status
                .late_observations
                .checked_add(1)
                .ok_or(Error::Limit)?;
            return Ok(
                (i64::from(self.status.correction_ppm) * i64::from(rate) / 1_000_000) as i32,
            );
        }
        self.late_since = None;
        // Fixed point retains sub-sample corrections across noisy observations.
        self.filtered += (phase * 65536 - self.filtered) / 16;
        let ppm = (self.filtered * 1_000_000 / (65536 * 48000 * 10)).clamp(-5000, 5000) as i32;
        self.status.correction_ppm = ppm;
        Ok((i64::from(ppm) * i64::from(rate) / 1_000_000) as i32)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn startup_is_bounded_and_never_discards_after_admission() {
        let first = Instant::now();
        let mut startup = Startup::new(first);
        assert_eq!(startup.admit(first, 48000, 1024), Ok(false));
        assert_eq!(
            startup.admit(first + Duration::from_micros(10), 48000, 1024),
            Ok(false)
        );
        assert_eq!(
            startup.admit(first + Duration::from_millis(21), 48000, 1024),
            Ok(true)
        );
        assert_eq!(
            startup.admit(first + Duration::from_secs(3), 48000, 1024),
            Ok(true)
        ); // recovery must refuse this, not startup discard
        let mut stalled = Startup::new(first);
        assert_eq!(stalled.admit(first, 48000, 1024), Ok(false));
        assert_eq!(
            stalled.admit(first + Duration::from_secs(2), 48000, 1024),
            Err(Error::Incomplete)
        );
        let mut backlog = Startup::new(first);
        for _ in 0..93 {
            assert_eq!(backlog.admit(first, 48000, 1024), Ok(false));
        }
        assert_eq!(backlog.admit(first, 48000, 1024), Err(Error::Incomplete));
    }
    #[test]
    fn bounded_soft_correction_does_not_reanchor_or_hide_a_gap() {
        let start = Instant::now();
        let mut recovery = Recovery::new(start);
        assert_eq!(recovery.step(start, 0, 48000), Ok(0));
        for i in 1..100 {
            let delta = recovery
                .step(
                    start + Duration::from_secs(i),
                    i as i128 * 48000 - 4000,
                    48000,
                )
                .unwrap();
            assert!((0..=240).contains(&delta));
        }
        assert_eq!(recovery.status().correction_ppm, 5000);
        assert_eq!(
            recovery.step(start + Duration::from_secs(100), 100 * 48000 - 24001, 48000),
            Err(Error::Clock)
        );
    }
    #[test]
    fn late_burst_recovers_without_moving_anchor_but_sustained_gap_fails() {
        let start = Instant::now();
        let mut recovery = Recovery::new(start);
        recovery.step(start, 0, 48000).unwrap();
        assert_eq!(
            recovery.step(start + Duration::from_millis(200), 1024, 48000),
            Ok(0)
        );
        assert_eq!(recovery.status().late_observations, 1);
        recovery
            .step(start + Duration::from_millis(220), 10240, 48000)
            .unwrap();
        assert_eq!(recovery.status().phase_48k, 320);
        recovery
            .step(start + Duration::from_secs(1), 40000, 48000)
            .unwrap();
        assert_eq!(
            recovery.step(start + Duration::from_millis(1500), 64000, 48000),
            Err(Error::Clock)
        );
    }
    #[test]
    fn both_drift_signs_remain_bounded_over_six_hours() {
        for drift in [-0.001, 0.001] {
            let start = Instant::now();
            let mut recovery = Recovery::new(start);
            let mut position = 0.0;
            let mut correction = 0;
            for frame in 0..(6 * 3600 * 50) {
                let elapsed = frame as f64 * 0.020 * (1.0 + drift);
                correction = recovery
                    .step(
                        start + Duration::from_secs_f64(elapsed),
                        position as i128,
                        48000,
                    )
                    .unwrap();
                position += 960.0 * (1.0 + correction as f64 / 48000.0);
            }
            assert!(recovery.status().peak_phase_48k < 1000);
            assert_eq!(correction.signum(), if drift < 0.0 { -1 } else { 1 });
        }
    }
}
