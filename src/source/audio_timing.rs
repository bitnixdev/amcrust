//! Bounded evidence for AAC SR-remapping steps, not an independent capture clock.
//! Pinned FFmpeg 9.0.2 rtpdec.c derives PRFT from the latest public SR plus the
//! signed RTP delta. Invert that integer operation only when it round-trips exactly.
use crate::ingest::{Packet, ProducerReference, SenderReport};
use serde::Serialize;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Point {
    pub sequence: u64,
    pub pts: i64,
    pub dts: i64,
    pub duration: i64,
    pub flags: i32,
    pub reference: ProducerReference,
    pub report: SenderReport,
    /// Reconstructed from public metadata; NOT an independently observed wire timestamp.
    pub reconstructed_rtp: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MappingStep {
    pub previous: Point,
    pub current: Point,
    pub mapped_step: i128,
    pub reconstructed_rtp_step: i32,
    pub report_mapping_shift: i128,
    pub mapping_residual: i128,
    pub previous_report_age_ms: u128,
    pub bounded_remapping: bool,
}
#[derive(Default)]
pub(super) struct Tracker {
    report: Option<(SenderReport, Instant)>,
    previous: Option<(Point, Instant)>,
}
fn nearest(n: i128, d: i128) -> i128 {
    n.signum() * ((n.abs() + d / 2) / d)
}
fn reconstruct(report: SenderReport, reference: ProducerReference) -> Option<u32> {
    if reference.flags != 24 || report.ntp >> 32 < 2_208_988_800 {
        return None; // No guessed NTP era or unsupported PRFT provenance.
    }
    // Deliberately match ff_parse_ntp_time (denominator UINT32_MAX, not 2^32).
    let micros = ((report.ntp >> 32) as i128 - 2_208_988_800) * 1_000_000
        + (report.ntp as u32 as i128 * 1_000_000 / u32::MAX as i128);
    let delta_us = reference.unix_micros as i128 - micros;
    let ticks = nearest(delta_us * 48000, 1_000_000);
    if ticks.abs() > 15 * 48000 || nearest(ticks * 1_000_000, 48000) != delta_us {
        return None;
    }
    Some(report.rtp.wrapping_add(ticks as i32 as u32))
}
impl Tracker {
    pub(super) fn observe(&mut self, packet: &Packet) -> Option<MappingStep> {
        self.sample(
            packet.sequence(),
            packet.pts(),
            packet.dts(),
            packet.duration(),
            packet.flags(),
            packet.sender_report(),
            packet.producer_reference(),
            packet.received_at(),
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn sample(
        &mut self,
        sequence: u64,
        pts: Option<i64>,
        dts: Option<i64>,
        duration: i64,
        flags: i32,
        report: Option<SenderReport>,
        reference: Option<ProducerReference>,
        at: Instant,
    ) -> Option<MappingStep> {
        if let Some(report) = report
            && self.report.is_none_or(|(old, _)| old != report)
        {
            self.report = Some((report, at));
        }
        let previous = self.previous.take();
        let (report, report_at) = self.report?;
        let reference = reference?;
        let point = Point {
            sequence,
            pts: pts?,
            dts: dts?,
            duration,
            flags,
            reference,
            report,
            reconstructed_rtp: reconstruct(report, reference)?,
        };
        self.previous = Some((point, report_at));
        let (old, old_report_at) = previous?;
        if point.dts > old.dts {
            return None;
        }
        let raw_step = point.reconstructed_rtp.wrapping_sub(old.reconstructed_rtp) as i32;
        let mapped_step = point.dts as i128 - old.dts as i128;
        let ntp_step = point.report.ntp.wrapping_sub(old.report.ntp) as i64;
        let sr_rtp_step = point.report.rtp.wrapping_sub(old.report.rtp) as i32;
        let shift = nearest(ntp_step as i128 * 48000, 1_i128 << 32) - sr_rtp_step as i128;
        let residual = mapped_step - raw_step as i128 - shift;
        let age = at.saturating_duration_since(old_report_at);
        let bounded = old.report.ssrc == point.report.ssrc
            && ntp_step > 0
            && sr_rtp_step > 0
            && age < Duration::from_secs(15)
            && at.saturating_duration_since(report_at) < Duration::from_secs(15)
            && old.pts == old.dts
            && point.pts == point.dts
            && old.duration == 1024
            && point.duration == 1024
            && (old.flags | point.flags) & 2 == 0
            && (1..=4800).contains(&raw_step)
            // Two valid endpoints can lie at opposite ends of the existing
            // +/-100ms phase window. This is its diameter, NOT a larger phase
            // budget: the consuming SampleClock still checks every endpoint.
            && shift.abs() <= 2 * 4800
            && residual.abs() <= 1;
        Some(MappingStep {
            previous: old,
            current: point,
            mapped_step,
            reconstructed_rtp_step: raw_step,
            report_mapping_shift: shift,
            mapping_residual: residual,
            previous_report_age_ms: age.as_millis(),
            bounded_remapping: bounded,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sr() -> SenderReport {
        SenderReport {
            ssrc: 1,
            ntp: 3_900_000_000_u64 << 32,
            rtp: u32::MAX - 500,
            packets: 1,
            bytes: 10,
        }
    }
    fn prft(sr: SenderReport, delta: i32) -> ProducerReference {
        ProducerReference {
            flags: 24,
            unix_micros: (((sr.ntp >> 32) as i128 - 2_208_988_800) * 1_000_000
                + sr.ntp as u32 as i128 * 1_000_000 / u32::MAX as i128
                + nearest(delta as i128 * 1_000_000, 48000)) as i64,
        }
    }
    #[test]
    fn inverse_round_trips_signed_ticks_fraction_and_wrap() {
        for ntp_fraction in [0, 1, 0x87654321, u32::MAX] {
            let s = SenderReport {
                ntp: sr().ntp | ntp_fraction as u64,
                ..sr()
            };
            for delta in [-720000, -1024, -1, 0, 1, 1008, 1024, 720000] {
                assert_eq!(
                    reconstruct(s, prft(s, delta)),
                    Some(s.rtp.wrapping_add(delta as u32))
                );
            }
        }
        let mut p = prft(sr(), 0);
        p.unix_micros += 1;
        assert!(reconstruct(sr(), p).is_none());
        p.flags = 0;
        assert!(reconstruct(sr(), p).is_none());
    }
    fn step(raw_step: i32, shift: i32, age: Duration, same_ssrc: bool) -> MappingStep {
        let now = Instant::now();
        let first = sr();
        let next = SenderReport {
            ntp: first.ntp + (5_u64 << 32),
            rtp: first.rtp.wrapping_add((240000 - shift) as u32),
            ssrc: if same_ssrc { 1 } else { 2 },
            ..first
        };
        let mut t = Tracker::default();
        assert!(
            t.sample(
                1,
                Some(0),
                Some(0),
                1024,
                1,
                Some(first),
                Some(prft(first, 239000)),
                now
            )
            .is_none()
        );
        let mapped = raw_step + shift;
        t.sample(
            3,
            Some(mapped as i64),
            Some(mapped as i64),
            1024,
            1,
            Some(next),
            Some(prft(next, 239000 + raw_step - (240000 - shift))),
            now + age,
        )
        .unwrap()
    }
    #[test]
    fn captured_camera_metadata_identifies_a_152ms_remapping() {
        // Exact public metadata from the real camera, not retained media. This
        // regression is not a substitute for live delivery/decoder acceptance.
        let old = SenderReport {
            ssrc: 1089859174,
            ntp: 17196202496549929877,
            rtp: 1258655754,
            packets: 13983,
            bytes: 4006210,
        };
        let new = SenderReport {
            ntp: 17196202517328981655,
            rtp: 1258895274,
            packets: 14451,
            bytes: 4140485,
            ..old
        };
        let now = Instant::now();
        let mut t = Tracker {
            report: Some((old, now)),
            ..Default::default()
        };
        t.sample(
            9534,
            Some(7401792),
            Some(7401792),
            1024,
            1,
            None,
            Some(ProducerReference {
                unix_micros: 1794814100640000,
                flags: 24,
            }),
            now + Duration::from_millis(4960),
        );
        let step = t
            .sample(
                9536,
                Some(7395648),
                Some(7395648),
                1024,
                1,
                Some(new),
                Some(ProducerReference {
                    unix_micros: 1794814100511999,
                    flags: 24,
                }),
                now + Duration::from_millis(4987),
            )
            .unwrap();
        assert_eq!(step.mapped_step, -6144);
        assert_eq!(step.reconstructed_rtp_step, 1152);
        assert_eq!(step.report_mapping_shift, -7296);
        assert_eq!(step.mapping_residual, 0);
        assert!(step.bounded_remapping);
    }

    #[test]
    fn duplicate_reports_do_not_refresh_age_and_missing_metadata_breaks_evidence() {
        let now = Instant::now();
        let old = sr();
        let mut t = Tracker::default();
        t.sample(
            1,
            Some(0),
            Some(0),
            1024,
            1,
            Some(old),
            Some(prft(old, 0)),
            now,
        );
        t.sample(
            2,
            Some(1024),
            Some(1024),
            1024,
            1,
            Some(old),
            Some(prft(old, 1024)),
            now + Duration::from_secs(14),
        );
        assert_eq!(t.report.unwrap().1, now);
        t.sample(
            3,
            Some(2048),
            Some(2048),
            1024,
            1,
            None,
            None,
            now + Duration::from_secs(14),
        );
        assert!(t.previous.is_none());
        assert!(
            t.sample(
                4,
                Some(1000),
                Some(1000),
                1024,
                1,
                None,
                Some(prft(old, 1000)),
                now + Duration::from_secs(14)
            )
            .is_none()
        );
    }

    #[test]
    fn remapping_is_distinct_from_raw_rollback_and_stale_or_changed_sources() {
        let s = step(1024, -1120, Duration::from_secs(5), true);
        assert_eq!(s.mapped_step, -96);
        assert_eq!(s.reconstructed_rtp_step, 1024);
        assert_eq!(s.mapping_residual, 0);
        assert!(s.bounded_remapping);
        assert!(!step(-96, 0, Duration::from_secs(5), true).bounded_remapping);
        assert!(!step(0, -1120, Duration::from_secs(5), true).bounded_remapping);
        assert!(step(1024, -9600, Duration::from_secs(5), true).bounded_remapping);
        assert!(!step(1024, -1120, Duration::from_secs(15), true).bounded_remapping);
        assert!(!step(1024, -1120, Duration::from_secs(5), false).bounded_remapping);
        assert!(!step(1024, -9601, Duration::from_secs(5), true).bounded_remapping);
    }
}
