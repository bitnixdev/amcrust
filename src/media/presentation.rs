//! Explicit host-monotonic presentation domain. Read time is not capture time.
//! Original camera timestamps are never modified. Sender reports sample host UTC
//! against this same monotonic clock; they do not import camera NTP jumps.
use super::{Error, rtcp::ClockPair};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy)]
pub struct PresentationClock {
    origin: Instant,
}
impl Default for PresentationClock {
    fn default() -> Self {
        Self::new()
    }
}
impl PresentationClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
    pub fn ticks(&self, at: Instant, rate: u32) -> i128 {
        let ns = match at.checked_duration_since(self.origin) {
            Some(delta) => delta.as_nanos() as i128,
            None => -(self.origin.duration_since(at).as_nanos() as i128),
        };
        ns * i128::from(rate) / 1_000_000_000
    }
    pub fn report(&self, origin_rtp: u32, rate: u32) -> Result<ClockPair, Error> {
        self.pair(Instant::now(), SystemTime::now(), origin_rtp, rate)
    }
    fn pair(
        &self,
        at: Instant,
        wall: SystemTime,
        origin_rtp: u32,
        rate: u32,
    ) -> Result<ClockPair, Error> {
        let unix = wall.duration_since(UNIX_EPOCH).map_err(|_| Error::Clock)?;
        let seconds = unix.as_secs().wrapping_add(2_208_988_800) as u32;
        let fraction = (u64::from(unix.subsec_nanos()) << 32) / 1_000_000_000;
        Ok(ClockPair {
            ntp: (u64::from(seconds) << 32) | fraction,
            rtp: origin_rtp.wrapping_add(self.ticks(at, rate) as u32),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn common_domain_preserves_offsets_wraps_and_long_runtime() {
        let clock = PresentationClock::new();
        let at = clock.origin + Duration::from_secs(180 * 24 * 3600);
        assert_eq!(clock.ticks(at, 90000) * 8, clock.ticks(at, 48000) * 15);
        assert_eq!(
            clock.ticks(clock.origin - Duration::from_millis(20), 48000),
            -960
        );
        let wall = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let pair = clock.pair(at, wall, u32::MAX, 90000).unwrap();
        assert_eq!(
            pair.rtp,
            u32::MAX.wrapping_add(clock.ticks(at, 90000) as u32)
        );
        // UTC correction affects NTP, never reanchors the monotonic media clock.
        let corrected = clock
            .pair(at, wall - Duration::from_secs(2), u32::MAX, 90000)
            .unwrap();
        assert_eq!(pair.rtp, corrected.rtp);
        assert_eq!(pair.ntp - corrected.ntp, 2u64 << 32);
    }
}
