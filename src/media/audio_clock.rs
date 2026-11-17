//! Explicit candidate live-audio policy; not a capture-clock proof or HSV policy.
use super::Error;
use std::time::{Duration, Instant};

/// Part of processor compatibility, alongside codec configuration and generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ClockPolicy {
    #[default]
    Strict,
    /// Host-monotonic presentation with bounded continuous resampling. Construct
    /// through Transcoder::new_presented, not the source-clock candidate API.
    Presentation,
    /// Anchor once to the first decoded source PTS, then count actual 48-kHz PCM
    /// samples. Never chase later SR phase steps. Limits are operational refusal
    /// budgets, NOT measured perceptual tolerances. Requires 48-kHz AAC input;
    /// output supports Config's rates/frames with explicit finite endpoint trims.
    AnchoredSamples {
        max_phase_samples: u32,
        report_timeout: Duration,
    },
}
impl ClockPolicy {
    /// Explicit opt-in starting point: 100 ms phase budget, 15 s fresh-SR budget.
    /// Approximately three observed SR periods, aged from SR-bearing packet read
    /// time (not raw RTCP arrival). Revisit using sustained playback;
    /// do not increase these simply to conceal a failing camera clock.
    pub const fn candidate() -> Self {
        Self::AnchoredSamples {
            max_phase_samples: 4800,
            report_timeout: Duration::from_secs(15),
        }
    }
    pub(super) fn valid(self) -> bool {
        match self {
            Self::Strict | Self::Presentation => true,
            Self::AnchoredSamples {
                max_phase_samples,
                report_timeout,
            } => {
                (1..=4800).contains(&max_phase_samples)
                    && !report_timeout.is_zero()
                    && report_timeout <= Duration::from_secs(15)
            }
        }
    }
}
#[derive(Debug, Clone, Copy, Default)]
pub struct ClockStatus {
    /// Decoder input-rate units (48 kHz for the candidate, not necessarily strict).
    pub anchored_pts: Option<i64>,
    pub decoded_samples: u64,
    pub latest_phase_samples: i128,
    pub maximum_absolute_phase_samples: u128,
    pub sender_report_seen: bool,
    pub last_report_age: Option<Duration>,
}
pub(super) struct SampleClock {
    pub policy: ClockPolicy,
    pub status: ClockStatus,
    started: Instant,
    last_report: Option<Instant>,
}
impl SampleClock {
    pub fn new(policy: ClockPolicy, now: Instant) -> Self {
        Self {
            policy,
            status: ClockStatus::default(),
            started: now,
            last_report: None,
        }
    }
    pub fn snapshot(&self, now: Instant) -> ClockStatus {
        ClockStatus {
            last_report_age: self.last_report.map(|at| now.saturating_duration_since(at)),
            ..self.status
        }
    }
    pub fn report(&mut self, at: Instant) {
        self.last_report = Some(at);
        self.status.sender_report_seen = true;
    }
    pub fn check(&self, now: Instant) -> Result<(), Error> {
        if let ClockPolicy::AnchoredSamples { report_timeout, .. } = self.policy
            && now.saturating_duration_since(self.last_report.unwrap_or(self.started))
                >= report_timeout
        {
            return Err(Error::Clock);
        }
        Ok(())
    }
    pub fn ready(&self, now: Instant) -> Result<(), Error> {
        self.check(now)?;
        if matches!(self.policy, ClockPolicy::AnchoredSamples { .. }) && self.last_report.is_none()
        {
            return Err(Error::Incomplete);
        }
        Ok(())
    }
    pub fn frame(&mut self, pts: i64, samples: u32) -> Result<(), Error> {
        let anchor = *self.status.anchored_pts.get_or_insert(pts);
        let phase = pts as i128 - (anchor as i128 + self.status.decoded_samples as i128);
        self.status.latest_phase_samples = phase;
        self.status.maximum_absolute_phase_samples = self
            .status
            .maximum_absolute_phase_samples
            .max(phase.unsigned_abs());
        if let ClockPolicy::AnchoredSamples {
            max_phase_samples, ..
        } = self.policy
            && phase.unsigned_abs() > max_phase_samples as u128
        {
            return Err(Error::Clock);
        }
        self.status.decoded_samples = self
            .status
            .decoded_samples
            .checked_add(samples as u64)
            .ok_or(Error::Clock)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_phase_is_not_reanchored_and_excess_is_refused() {
        let now = Instant::now();
        let mut c = SampleClock::new(ClockPolicy::candidate(), now);
        c.report(now);
        c.frame(1000, 1024).unwrap();
        c.frame(2008, 1024).unwrap();
        c.report(now + Duration::from_secs(5)); // freshness only, never a new origin
        assert_eq!(c.status.anchored_pts, Some(1000));
        assert_eq!(c.status.latest_phase_samples, -16);
        c.frame(3048 + 4800, 1024).unwrap();
        assert_eq!(c.frame(4072 + 4801, 1024), Err(Error::Clock));
        assert_eq!(c.status.decoded_samples, 3072);
    }
    #[test]
    fn mapping_step_can_cross_phase_window_without_reanchoring() {
        let mut c = SampleClock::new(ClockPolicy::candidate(), Instant::now());
        c.frame(0, 1024).unwrap();
        c.frame(1024 + 4560, 1024).unwrap(); // +95 ms
        c.frame(2048 - 2736, 1024).unwrap(); // -57 ms: 152-ms mapping step
        assert_eq!(c.status.anchored_pts, Some(0));
        assert_eq!(c.status.decoded_samples, 3072);
        assert_eq!(c.status.latest_phase_samples, -2736);
        assert_eq!(c.frame(3072 - 4801, 1024), Err(Error::Clock));
        assert_eq!(c.status.decoded_samples, 3072);
    }

    #[test]
    fn report_is_required_and_deadline_is_not_refreshed_by_media() {
        let now = Instant::now();
        let mut c = SampleClock::new(ClockPolicy::candidate(), now);
        assert_eq!(c.ready(now), Err(Error::Incomplete));
        c.report(now);
        c.frame(0, 1024).unwrap();
        assert!(c.ready(now + Duration::from_secs(14)).is_ok());
        assert_eq!(c.ready(now + Duration::from_secs(15)), Err(Error::Clock));
    }
    #[test]
    fn fresh_generation_has_no_anchor_or_report() {
        let c = SampleClock::new(ClockPolicy::candidate(), Instant::now());
        assert!(c.status.anchored_pts.is_none());
        assert!(!c.status.sender_report_seen);
        assert!(
            !ClockPolicy::AnchoredSamples {
                max_phase_samples: 4801,
                report_timeout: Duration::from_secs(15)
            }
            .valid()
        );
    }
}
