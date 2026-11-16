//! Bounded, generation-scoped observations of PUBLIC FFmpeg clock metadata.
//! This is NOT a clock repairer or a transport admission grant. In particular an
//! SR is attached to a following media packet, not necessarily a packet sampled at
//! the SR's RTP/NTP instant. PRFT in the pinned RTSP demuxer is derived from the SR
//! and the packet RTP timestamp; it is not an independent camera capture clock.
use super::audio_timing::Tracker;
pub use super::audio_timing::{MappingStep, Point, Progress};
use crate::ingest::{Codec, Packet, SenderReport, StreamConfiguration, TimeBase};
use serde::Serialize;
use std::time::{Duration, Instant};

const NTP_SECOND: i128 = 1i128 << 32;
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Range {
    pub min: i128,
    pub max: i128,
}
fn include(range: &mut Option<Range>, value: i128) {
    match range {
        Some(range) => {
            range.min = range.min.min(value);
            range.max = range.max.max(value);
        }
        None => {
            *range = Some(Range {
                min: value,
                max: value,
            })
        }
    }
}
fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Fault {
    PacketIdentity,
    ReportSourceChanged,
    ReportNtpNotAdvancing,
    ReportRtpNotAdvancing,
    ReportIntervalAmbiguous,
    UnsupportedProducerReference,
    MissingMediaTimestamp,
    NonIncreasingDts,
}
/// First offending packet metadata only: bounded, no payloads or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Failure {
    pub kind: Fault,
    pub stream_index: usize,
    pub sequence: u64,
    pub pts: Option<i64>,
    pub dts: Option<i64>,
    /// Previous observed DTS for metadata faults; last admitted DTS for admission faults.
    pub previous_dts: Option<i64>,
    pub duration: i64,
    pub flags: i32,
    pub packet_bytes: usize,
    pub sender_report: Option<SenderReport>,
    pub producer_reference: Option<crate::ingest::ProducerReference>,
}
impl Failure {
    fn at(kind: Fault, packet: &Packet, previous_dts: Option<i64>) -> Self {
        Self {
            kind,
            stream_index: packet.stream_index(),
            sequence: packet.sequence(),
            pts: packet.pts(),
            dts: packet.dts(),
            previous_dts,
            duration: packet.duration(),
            flags: packet.flags(),
            packet_bytes: packet.data().len(),
            sender_report: packet.sender_report(),
            producer_reference: packet.producer_reference(),
        }
    }
}
#[derive(Debug, Clone, Default, Serialize)]
pub struct Reports {
    pub attached: u64,
    pub duplicates: u64,
    pub distinct: u64,
    pub first_after_ms: Option<u64>,
    pub last_age_ms: Option<u64>,
    /// Sender-clock intervals (NTP fixed-point converted for display only).
    pub ntp_interval_us: Option<Range>,
    /// Gaps between packets carrying SR metadata, NOT network SR receive times.
    pub attachment_interval_us: Option<Range>,
    pub rtp_interval_ticks: Option<Range>,
    /// RTP elapsed time minus NTP elapsed time, at the advertised clock rate.
    /// A measurement, not a rate/phase correction or an accepted tolerance.
    pub rtp_minus_ntp_ns: Option<Range>,
    /// Modular counters, not proof of packet delivery/loss or of one RTP packet/AU.
    pub sender_packet_delta: Option<Range>,
    pub sender_byte_delta: Option<Range>,
    pub media_packets_between_reports: Option<Range>,
}
#[derive(Debug, Clone, Serialize)]
pub struct TrackSnapshot {
    pub index: usize,
    pub codec: Codec,
    pub time_base: TimeBase,
    pub packets: u64,
    pub missing_pts: u64,
    pub missing_dts: u64,
    pub pts_ne_dts: u64,
    pub dts_steps: Option<Range>,
    pub duration_mismatches: u64,
    pub step_minus_declared_duration: Option<Range>,
    pub duration_error_on_report_packet: Option<Range>,
    pub duration_error_between_report_packets: Option<Range>,
    /// Accumulated DTS elapsed minus sum of declared durations in packet order.
    /// This does not assert that packet duration is a proven decoded sample count.
    pub duration_debt_ticks: Option<Range>,
    pub final_duration_debt_ticks: i128,
    pub dts_step_gcd: u64,
    pub referenced_dts_step_gcd: u64,
    pub referenced_duration_debt_ticks: Option<Range>,
    pub final_referenced_duration_debt_ticks: i128,
    pub producer_references: u64,
    pub first_reference_after_ms: Option<u64>,
    pub last_reference_age_ms: Option<u64>,
    /// Spread of (PRFT UTC - packet PTS). Display nanoseconds only; PRFT itself
    /// has microsecond precision. Never used to rewrite source clocks.
    pub pts_epoch_spread_ns: Option<i128>,
    pub first_epoch_difference_ns: Option<i128>,
    pub reports: Reports,
    pub audio_progress: Option<Progress>,
    pub mapping_steps: u64,
    pub bounded_mapping_steps: u64,
    pub first_mapping_step: Option<MappingStep>,
    pub last_mapping_step: Option<MappingStep>,
}
#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub generation: u64,
    pub retired: bool,
    pub fault: Option<Fault>,
    pub failure: Option<Failure>,
    pub epoch_reference_stream: Option<usize>,
    pub tracks: Vec<TrackSnapshot>,
}
struct Track {
    snapshot: TrackSnapshot,
    last_dts: Option<(i64, i64, bool)>,
    debt: i128,
    referenced_debt: i128,
    last_reference: Option<Instant>,
    first_epoch_ns: Option<i128>,
    epoch_ns: Option<Range>,
    last_report: Option<(SenderReport, Instant, u64)>,
    audio_timing: Tracker,
}
impl Track {
    fn new(index: usize, codec: Codec, time_base: TimeBase) -> Self {
        Self {
            snapshot: TrackSnapshot {
                index,
                codec,
                time_base,
                packets: 0,
                missing_pts: 0,
                missing_dts: 0,
                pts_ne_dts: 0,
                dts_steps: None,
                duration_mismatches: 0,
                step_minus_declared_duration: None,
                duration_error_on_report_packet: None,
                duration_error_between_report_packets: None,
                duration_debt_ticks: None,
                final_duration_debt_ticks: 0,
                dts_step_gcd: 0,
                referenced_dts_step_gcd: 0,
                referenced_duration_debt_ticks: None,
                final_referenced_duration_debt_ticks: 0,
                producer_references: 0,
                first_reference_after_ms: None,
                last_reference_age_ms: None,
                pts_epoch_spread_ns: None,
                first_epoch_difference_ns: None,
                reports: Reports::default(),
                audio_progress: None,
                mapping_steps: 0,
                bounded_mapping_steps: 0,
                first_mapping_step: None,
                last_mapping_step: None,
            },
            last_dts: None,
            debt: 0,
            referenced_debt: 0,
            last_reference: None,
            first_epoch_ns: None,
            epoch_ns: None,
            last_report: None,
            audio_timing: Tracker::default(),
        }
    }
    fn record_dts(
        &mut self,
        dts: Option<i64>,
        packet_duration: i64,
        referenced: bool,
        report: bool,
    ) {
        let s = &mut self.snapshot;
        if let Some(dts) = dts {
            if let Some((previous, duration, previous_reference)) = self.last_dts {
                let step = dts as i128 - previous as i128;
                include(&mut s.dts_steps, step);
                s.dts_step_gcd = gcd(s.dts_step_gcd, step.unsigned_abs() as u64);
                if duration > 0 {
                    let error = step - duration as i128;
                    s.duration_mismatches += u64::from(error != 0);
                    include(&mut s.step_minus_declared_duration, error);
                    include(
                        if report {
                            &mut s.duration_error_on_report_packet
                        } else {
                            &mut s.duration_error_between_report_packets
                        },
                        error,
                    );
                    self.debt += error;
                    include(&mut s.duration_debt_ticks, 0);
                    include(&mut s.duration_debt_ticks, self.debt);
                    if previous_reference && referenced {
                        self.referenced_debt += error;
                        include(&mut s.referenced_duration_debt_ticks, 0);
                        include(&mut s.referenced_duration_debt_ticks, self.referenced_debt);
                    } else {
                        self.referenced_debt = 0;
                    }
                } else {
                    // Unknown duration breaks the accumulated-duration comparison.
                    self.debt = 0;
                    self.referenced_debt = 0;
                }
                if previous_reference && referenced {
                    s.referenced_dts_step_gcd =
                        gcd(s.referenced_dts_step_gcd, step.unsigned_abs() as u64);
                }
            }
            self.last_dts = Some((dts, packet_duration, referenced));
        } else {
            self.last_dts = None;
            self.debt = 0;
            self.referenced_debt = 0;
        }
    }
    fn observe(&mut self, packet: &Packet, born: Instant) -> Result<(), Fault> {
        let reference = packet.producer_reference();
        let report = packet.sender_report();
        if self.snapshot.codec == Codec::Aac
            && self.snapshot.time_base.numerator == 1
            && self.snapshot.time_base.denominator == 48000
        {
            if let Some(step) = self.audio_timing.observe(packet) {
                self.snapshot.mapping_steps += 1;
                self.snapshot.bounded_mapping_steps += u64::from(step.bounded_remapping);
                self.snapshot.first_mapping_step.get_or_insert(step);
                self.snapshot.last_mapping_step = Some(step);
            }
            self.snapshot.audio_progress = self.audio_timing.progress;
        }
        self.record_dts(
            packet.dts(),
            packet.duration(),
            reference.is_some(),
            report.is_some(),
        );
        let s = &mut self.snapshot;
        let at = packet.received_at();
        s.packets += 1;
        s.missing_pts += u64::from(packet.pts().is_none());
        s.missing_dts += u64::from(packet.dts().is_none());
        s.pts_ne_dts += u64::from(packet.pts().zip(packet.dts()).is_some_and(|(p, d)| p != d));
        if let Some(reference) = reference {
            s.producer_references += 1;
            s.first_reference_after_ms
                .get_or_insert(millis(at.saturating_duration_since(born)));
            self.last_reference = Some(at);
            // Pinned FFmpeg 9 RTSP sets flags=24 in rtp_set_prft. Other producers
            // are not silently treated as sender-clock evidence.
            if reference.flags != 24 {
                return Err(Fault::UnsupportedProducerReference);
            }
            if let Some(pts) = packet.pts() {
                let pts_ns = pts as i128 * s.time_base.numerator as i128 * 1_000_000_000
                    / s.time_base.denominator as i128;
                let epoch_ns = reference.unix_micros as i128 * 1000 - pts_ns;
                self.first_epoch_ns.get_or_insert(epoch_ns);
                include(&mut self.epoch_ns, epoch_ns);
            }
        }
        if let Some(report) = report {
            self.report(report, at, born)?;
        }
        Ok(())
    }
    fn report(&mut self, current: SenderReport, at: Instant, born: Instant) -> Result<(), Fault> {
        let s = &mut self.snapshot;
        let reports = &mut s.reports;
        reports.attached += 1;
        if let Some((previous, previous_at, previous_packets)) = self.last_report {
            if previous == current {
                reports.duplicates += 1;
                return Ok(());
            }
            if previous.ssrc != current.ssrc {
                return Err(Fault::ReportSourceChanged);
            }
            // Serial-number differences handle wrap, refusing half-range ambiguity.
            // This is NOT an affine fit of camera clocks to host arrival times.
            let ntp = current.ntp.wrapping_sub(previous.ntp) as i64;
            if ntp <= 0 {
                return Err(Fault::ReportNtpNotAdvancing);
            }
            let rtp = current.rtp.wrapping_sub(previous.rtp) as i32;
            if rtp <= 0 {
                return Err(Fault::ReportRtpNotAdvancing);
            }
            if ntp as i128 * s.time_base.denominator as i128
                >= i32::MAX as i128 * NTP_SECOND * s.time_base.numerator as i128
            {
                return Err(Fault::ReportIntervalAmbiguous);
            }
            include(
                &mut reports.ntp_interval_us,
                ntp as i128 * 1_000_000 / NTP_SECOND,
            );
            include(
                &mut reports.attachment_interval_us,
                at.saturating_duration_since(previous_at).as_micros() as i128,
            );
            include(&mut reports.rtp_interval_ticks, rtp as i128);
            let rtp_ns = rtp as i128 * s.time_base.numerator as i128 * 1_000_000_000
                / s.time_base.denominator as i128;
            let ntp_ns = ntp as i128 * 1_000_000_000 / NTP_SECOND;
            include(&mut reports.rtp_minus_ntp_ns, rtp_ns - ntp_ns);
            include(
                &mut reports.sender_packet_delta,
                current.packets.wrapping_sub(previous.packets) as i128,
            );
            include(
                &mut reports.sender_byte_delta,
                current.bytes.wrapping_sub(previous.bytes) as i128,
            );
            include(
                &mut reports.media_packets_between_reports,
                (s.packets - previous_packets) as i128,
            );
        }
        reports.distinct += 1;
        reports
            .first_after_ms
            .get_or_insert(millis(at.saturating_duration_since(born)));
        self.last_report = Some((current, at, s.packets));
        Ok(())
    }
}
fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

pub(super) struct Observations {
    generation: u64,
    next_sequence: u64,
    born: Instant,
    retired: bool,
    fault: Option<Fault>,
    failure: Option<Failure>,
    tracks: Vec<Track>,
}
impl Observations {
    pub(super) fn new(generation: u64, born: Instant, streams: &[StreamConfiguration]) -> Self {
        Self {
            generation,
            next_sequence: 0,
            born,
            retired: false,
            fault: None,
            failure: None,
            tracks: streams
                .iter()
                .map(|s| Track::new(s.index, s.codec, s.time_base))
                .collect(),
        }
    }
    pub(super) fn observe(&mut self, packet: &Packet) -> Result<(), Fault> {
        if let Some(fault) = self.fault {
            return Err(fault);
        }
        let previous_dts = self
            .tracks
            .get(packet.stream_index())
            .and_then(|track| track.last_dts.map(|value| value.0));
        let result = if self.retired
            || packet.generation() != self.generation
            || packet.sequence() != self.next_sequence
            || packet.stream_index() >= self.tracks.len()
            || self.next_sequence == u64::MAX
        {
            Err(Fault::PacketIdentity)
        } else {
            self.next_sequence += 1;
            self.tracks[packet.stream_index()].observe(packet, self.born)
        };
        if let Err(fault) = result {
            self.record_failure(Failure::at(fault, packet, previous_dts));
        }
        result
    }
    pub(super) fn bounded_audio_mapping_step(&self, packet: &Packet) -> bool {
        self.tracks
            .get(packet.stream_index())
            .and_then(|track| track.snapshot.last_mapping_step)
            .is_some_and(|step| {
                step.current.sequence == packet.sequence() && step.bounded_remapping
            })
    }
    fn record_failure(&mut self, failure: Failure) {
        if self.failure.is_none() {
            self.fault = Some(failure.kind);
            self.failure = Some(failure);
        }
    }
    /// Admission already refused this packet. Record its reason without changing
    /// the admission rule or replacing any original timestamp.
    pub(super) fn refused_admission(&mut self, packet: &Packet, previous_dts: Option<i64>) {
        let kind = if packet.pts().is_none() || packet.dts().is_none() {
            Fault::MissingMediaTimestamp
        } else {
            Fault::NonIncreasingDts
        };
        self.record_failure(Failure::at(kind, packet, previous_dts));
    }
    pub(super) fn retire(&mut self) {
        self.retired = true;
    }
    pub(super) fn snapshot(&self) -> Snapshot {
        let now = Instant::now();
        let reference = self
            .tracks
            .iter()
            .find_map(|t| t.first_epoch_ns.map(|e| (t.snapshot.index, e)));
        let tracks = self
            .tracks
            .iter()
            .map(|track| {
                let mut s = track.snapshot.clone();
                s.final_duration_debt_ticks = track.debt;
                s.final_referenced_duration_debt_ticks = track.referenced_debt;
                s.last_reference_age_ms = track
                    .last_reference
                    .map(|at| millis(now.saturating_duration_since(at)));
                s.reports.last_age_ms = track
                    .last_report
                    .map(|(_, at, _)| millis(now.saturating_duration_since(at)));
                s.pts_epoch_spread_ns = track.epoch_ns.map(|r| r.max - r.min);
                s.first_epoch_difference_ns =
                    track.first_epoch_ns.zip(reference).map(|(e, (_, r))| e - r);
                s
            })
            .collect();
        Snapshot {
            generation: self.generation,
            retired: self.retired,
            fault: self.fault,
            failure: self.failure,
            epoch_reference_stream: reference.map(|(index, _)| index),
            tracks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn track() -> Track {
        Track::new(
            0,
            Codec::H264,
            TimeBase {
                numerator: 1,
                denominator: 90000,
            },
        )
    }
    fn report() -> SenderReport {
        SenderReport {
            ssrc: 7,
            ntp: 10u64 << 32,
            rtp: 1000,
            packets: 100,
            bytes: 10000,
        }
    }

    #[test]
    fn first_failure_survives_retirement_but_not_a_new_generation() {
        let now = Instant::now();
        let mut observations = Observations::new(1, now, &[]);
        let first = Failure {
            kind: Fault::NonIncreasingDts,
            stream_index: 1,
            sequence: 10,
            pts: Some(900),
            dts: Some(900),
            previous_dts: Some(1000),
            duration: 1024,
            flags: 0,
            packet_bytes: 128,
            sender_report: None,
            producer_reference: None,
        };
        observations.record_failure(first);
        observations.record_failure(Failure {
            kind: Fault::MissingMediaTimestamp,
            pts: None,
            ..first
        });
        observations.retire();
        let snapshot = observations.snapshot();
        assert!(snapshot.retired);
        assert_eq!(snapshot.fault, Some(first.kind));
        assert_eq!(snapshot.failure, Some(first));
        assert!(Observations::new(2, now, &[]).snapshot().failure.is_none());
    }

    #[test]
    fn duration_debt_measures_quantization_without_repairing_it() {
        let mut t = track();
        for dts in [1000, 2008, 3064, 4072] {
            t.record_dts(Some(dts), 1024, true, false);
        }
        assert_eq!(t.snapshot.duration_mismatches, 3);
        assert_eq!(t.snapshot.referenced_dts_step_gcd, 48);
        assert_eq!(t.debt, 0);
        assert_eq!(t.referenced_debt, 0);
        let range = t.snapshot.referenced_duration_debt_ticks.unwrap();
        assert_eq!((range.min, range.max), (-16, 16));
        // Unknown packet duration and missing DTS break accumulated comparisons.
        t.record_dts(Some(5100), 0, true, true);
        assert_eq!(t.snapshot.duration_error_on_report_packet.unwrap().max, 4);
        assert_eq!(
            t.snapshot
                .duration_error_between_report_packets
                .unwrap()
                .max,
            32
        );
        assert_eq!(t.debt, 4);
        t.record_dts(Some(7000), 1024, true, false);
        assert_eq!(t.debt, 0);
        t.record_dts(None, 1024, true, false);
        t.record_dts(Some(9000), 1024, true, false);
        assert_eq!(t.referenced_debt, 0);
    }
    #[test]
    fn signed_extreme_timestamps_do_not_overflow() {
        let mut t = track();
        t.record_dts(Some(i64::MIN), i64::MAX, true, false);
        t.record_dts(Some(i64::MAX), 1024, true, false);
        assert_eq!(t.snapshot.dts_steps.unwrap().max, u64::MAX as i128);
        assert_eq!(t.debt, i64::MAX as i128 + 1);
    }
    #[test]
    fn serial_wrap_preserves_sender_intervals() {
        let born = Instant::now();
        let mut t = track();
        let first = SenderReport {
            ntp: u64::MAX - (1u64 << 32) + 1,
            rtp: u32::MAX - 89999,
            packets: u32::MAX - 20,
            bytes: u32::MAX - 20,
            ..report()
        };
        t.report(first, born, born).unwrap();
        t.snapshot.packets = 65;
        t.report(
            SenderReport {
                ntp: 1u64 << 32,
                rtp: 90000,
                packets: 44,
                bytes: 44,
                ..first
            },
            born + Duration::from_secs(2),
            born,
        )
        .unwrap();
        let r = t.snapshot.reports;
        assert_eq!(r.distinct, 2);
        assert_eq!(r.ntp_interval_us.unwrap().min, 2_000_000);
        assert_eq!(r.rtp_interval_ticks.unwrap().min, 180000);
        assert_eq!(r.rtp_minus_ntp_ns.unwrap().min, 0);
        assert_eq!(r.sender_packet_delta.unwrap().min, 65);
        assert_eq!(r.sender_byte_delta.unwrap().min, 65);
        assert_eq!(r.media_packets_between_reports.unwrap().min, 65);
    }
    #[test]
    fn duplicate_is_not_a_fresh_report() {
        let born = Instant::now();
        let mut t = track();
        t.report(report(), born, born).unwrap();
        t.report(report(), born + Duration::from_secs(10), born)
            .unwrap();
        assert_eq!(t.snapshot.reports.attached, 2);
        assert_eq!(t.snapshot.reports.distinct, 1);
        assert_eq!(t.snapshot.reports.duplicates, 1);
        assert_eq!(t.last_report.unwrap().1, born);
    }
    #[test]
    fn reset_and_ambiguous_reports_are_refused_without_replacing_anchor() {
        let born = Instant::now();
        let first = report();
        let good = SenderReport {
            ntp: first.ntp + (1u64 << 32),
            rtp: first.rtp + 90000,
            packets: first.packets + 47,
            bytes: first.bytes + 3000,
            ..first
        };
        for (bad, fault) in [
            (SenderReport { ssrc: 8, ..good }, Fault::ReportSourceChanged),
            (
                SenderReport {
                    ntp: first.ntp,
                    ..good
                },
                Fault::ReportNtpNotAdvancing,
            ),
            (
                SenderReport {
                    ntp: first.ntp - 1,
                    ..good
                },
                Fault::ReportNtpNotAdvancing,
            ),
            (
                SenderReport {
                    rtp: first.rtp,
                    ..good
                },
                Fault::ReportRtpNotAdvancing,
            ),
            (
                SenderReport {
                    rtp: first.rtp - 1,
                    ..good
                },
                Fault::ReportRtpNotAdvancing,
            ),
            (
                SenderReport {
                    rtp: first.rtp.wrapping_add(1 << 31),
                    ..good
                },
                Fault::ReportRtpNotAdvancing,
            ),
            (
                SenderReport {
                    ntp: first.ntp + (i32::MAX as u64 * (1u64 << 32) / 90000) + 2,
                    ..good
                },
                Fault::ReportIntervalAmbiguous,
            ),
        ] {
            let mut t = track();
            t.report(first, born, born).unwrap();
            assert_eq!(
                t.report(bad, born + Duration::from_secs(1), born),
                Err(fault)
            );
            assert_eq!(t.last_report.unwrap().0, first);
            assert_eq!(t.snapshot.reports.distinct, 1);
        }
    }
    #[test]
    fn retired_clock_is_not_reused_by_new_generation() {
        let born = Instant::now();
        let mut old = Observations::new(1, born, &[]);
        let mut t = track();
        t.report(report(), born, born).unwrap();
        t.first_epoch_ns = Some(10000);
        t.epoch_ns = Some(Range {
            min: 10000,
            max: 10667,
        });
        old.tracks.push(t);
        old.retire();
        let snapshot = old.snapshot();
        assert!(snapshot.retired);
        assert_eq!(snapshot.tracks[0].pts_epoch_spread_ns, Some(667));
        let new = Observations::new(2, born, &[]).snapshot();
        assert_eq!(new.generation, 2);
        assert!(!new.retired);
        assert!(new.epoch_reference_stream.is_none());
        assert!(new.tracks.is_empty());
    }
}
