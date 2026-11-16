//! Authenticated receiver feedback only. Authentication/replay/peer checks belong
//! to Transport. Parsing is bounded and transactional; reports do not prove playback.
use super::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceptionReport {
    pub fraction_lost: u8,
    pub cumulative_lost: i32,
    pub highest_sequence: u32,
    /// In this sender's RTP clock units, not milliseconds.
    pub jitter: u32,
    pub last_sender_report: u32,
    /// Units of 1/65536 second; insufficient by itself to calculate RTT.
    pub delay_since_report: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Feedback {
    pub report_packets: u32,
    pub report: Option<ReceptionReport>,
    pub pli: u32,
    pub fir: u32,
    /// Requested packet occurrences, not unique losses or retransmissions.
    pub nack_packets: u32,
    pub unsupported: u32,
    pub bye: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FeedbackStats {
    pub datagrams: u64,
    pub report_packets: u64,
    pub last_report: Option<ReceptionReport>,
    pub pli: u64,
    pub fir: u64,
    pub nack_packets: u64,
    pub unsupported: u64,
    pub departed: bool,
}

/// One local sender and one authenticated remote participant, with no SSRC map
/// or retained packet/CNAME/reason strings. PLI/FIR/NACK are observations, not
/// permission to write camera settings, retransmit, or reset a media clock.
pub struct Receiver {
    local: u32,
    peer: Option<u32>,
    last_fir: Option<u8>,
    stats: FeedbackStats,
}
impl Receiver {
    pub fn new(local: u32) -> Self {
        Self {
            local,
            peer: None,
            last_fir: None,
            stats: FeedbackStats::default(),
        }
    }
    pub fn stats(&self) -> FeedbackStats {
        self.stats
    }
    pub fn receive(&mut self, bytes: &[u8]) -> Result<Feedback, Error> {
        if bytes.len() < 8 || bytes.len() > 2048 || block_size(bytes) < 8 {
            return Err(Error::Malformed);
        }
        let peer = word(&bytes[4..8]);
        if self.peer.is_some_and(|old| old != peer) {
            return Err(Error::WrongSsrc);
        }
        let mut result = Feedback::default();
        let mut fir_sequence = None;
        let mut rest = bytes;
        while !rest.is_empty() {
            if rest.len() < 4 || rest[0] >> 6 != 2 {
                return Err(Error::Malformed);
            }
            let size = block_size(rest);
            if size > rest.len() {
                return Err(Error::Malformed);
            }
            let mut block = &rest[..size];
            if block[0] & 0x20 != 0 {
                let padding = usize::from(block[size - 1]);
                if size != rest.len() || padding == 0 || padding > size - 4 {
                    return Err(Error::Malformed);
                }
                block = &block[..size - padding];
            }
            if rest.len() == bytes.len() && block.len() < 8 {
                return Err(Error::Malformed);
            }
            let count = usize::from(block[0] & 31);
            match block[1] {
                200 | 201 => {
                    let base = if block[1] == 200 { 28 } else { 8 };
                    if block.len() != base + count * 24 {
                        return Err(Error::Malformed);
                    }
                    sender(block, peer)?;
                    result.report_packets += 1;
                    for report in block[base..].as_chunks::<24>().0 {
                        if word(report) != self.local {
                            continue;
                        }
                        if result.report.is_some() {
                            return Err(Error::DuplicateReport);
                        }
                        let lost = i32::from_be_bytes([report[5], report[6], report[7], 0]) >> 8;
                        result.report = Some(ReceptionReport {
                            fraction_lost: report[4],
                            cumulative_lost: lost,
                            highest_sequence: word(&report[8..]),
                            jitter: word(&report[12..]),
                            last_sender_report: word(&report[16..]),
                            delay_since_report: word(&report[20..]),
                        });
                    }
                }
                202 => validate_sdes(block, count)?,
                203 => {
                    let end = 4 + count * 4;
                    if count == 0 || block.len() < end {
                        return Err(Error::Malformed);
                    }
                    if block.len() > end {
                        let reason_end = end + 1 + usize::from(block[end]);
                        if reason_end > block.len()
                            || block.len() - reason_end > 3
                            || block[reason_end..].iter().any(|b| *b != 0)
                        {
                            return Err(Error::Malformed);
                        }
                    }
                    result.bye |= block[4..end]
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .any(|id| word(id) == peer);
                }
                205 | 206 => {
                    if block.len() < 12 {
                        return Err(Error::Malformed);
                    }
                    sender(block, peer)?;
                    let media = word(&block[8..]);
                    match (block[1], count) {
                        (205, 1) => {
                            if block.len() < 16 || !(block.len() - 12).is_multiple_of(4) {
                                return Err(Error::Malformed);
                            }
                            if media == self.local {
                                for entry in block[12..].as_chunks::<4>().0 {
                                    result.nack_packets +=
                                        1 + u16::from_be_bytes([entry[2], entry[3]]).count_ones();
                                }
                            }
                        }
                        (206, 1) => {
                            if block.len() != 12 {
                                return Err(Error::Malformed);
                            }
                            if media == self.local {
                                result.pli += 1;
                            }
                        }
                        (206, 4) => {
                            if media != 0
                                || block.len() < 20
                                || !(block.len() - 12).is_multiple_of(8)
                            {
                                return Err(Error::Malformed);
                            }
                            for entry in block[12..].as_chunks::<8>().0 {
                                if word(entry) == self.local {
                                    if fir_sequence.is_some() {
                                        return Err(Error::DuplicateReport);
                                    }
                                    fir_sequence = Some(entry[4]);
                                    result.fir = u32::from(self.last_fir != fir_sequence);
                                }
                            }
                        }
                        _ => result.unsupported += 1,
                    }
                }
                _ => result.unsupported += 1,
            }
            rest = &rest[size..];
        }
        // Commit only after the entire compound datagram has validated.
        self.peer = Some(peer);
        if fir_sequence.is_some() {
            self.last_fir = fir_sequence;
        }
        self.stats.datagrams = self.stats.datagrams.saturating_add(1);
        self.stats.report_packets = self
            .stats
            .report_packets
            .saturating_add(result.report_packets.into());
        if result.report.is_some() {
            self.stats.last_report = result.report;
        }
        self.stats.pli = self.stats.pli.saturating_add(result.pli.into());
        self.stats.fir = self.stats.fir.saturating_add(result.fir.into());
        self.stats.nack_packets = self
            .stats
            .nack_packets
            .saturating_add(result.nack_packets.into());
        self.stats.unsupported = self
            .stats
            .unsupported
            .saturating_add(result.unsupported.into());
        self.stats.departed |= result.bye;
        Ok(result)
    }
}
fn word(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes[..4].try_into().unwrap())
}
fn block_size(bytes: &[u8]) -> usize {
    (usize::from(u16::from_be_bytes([bytes[2], bytes[3]])) + 1) * 4
}
fn sender(block: &[u8], peer: u32) -> Result<(), Error> {
    if word(&block[4..]) != peer {
        Err(Error::WrongSsrc)
    } else {
        Ok(())
    }
}
fn validate_sdes(block: &[u8], count: usize) -> Result<(), Error> {
    let mut at = 4;
    for _ in 0..count {
        if at + 4 > block.len() {
            return Err(Error::Malformed);
        }
        at += 4; // No identifiers or CNAME strings retained.
        loop {
            let kind = *block.get(at).ok_or(Error::Malformed)?;
            at += 1;
            if kind == 0 {
                while at % 4 != 0 {
                    if block.get(at) != Some(&0) {
                        return Err(Error::Malformed);
                    }
                    at += 1;
                }
                break;
            }
            let size = usize::from(*block.get(at).ok_or(Error::Malformed)?);
            at += 1 + size;
            if at > block.len() {
                return Err(Error::Malformed);
            }
        }
    }
    if at != block.len() {
        return Err(Error::Malformed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rr() -> Vec<u8> {
        let mut b = vec![
            0x81, 201, 0, 7, 0, 0, 0, 99, 0, 0, 0, 42, 128, 255, 255, 254,
        ];
        for word in [0x10001u32, 960, 0x12345678, 65536] {
            b.extend_from_slice(&word.to_be_bytes());
        }
        b
    }
    fn request(pt: u8, fmt: u8, media: u32, body: &[u8]) -> Vec<u8> {
        let mut b = vec![
            0x80 | fmt,
            pt,
            0,
            ((12 + body.len()) / 4 - 1) as u8,
            0,
            0,
            0,
            99,
        ];
        b.extend_from_slice(&media.to_be_bytes());
        b.extend_from_slice(body);
        b
    }
    #[test]
    fn report_fields_and_transactional_refusal() {
        let mut r = Receiver::new(42);
        let report = r.receive(&rr()).unwrap().report.unwrap();
        assert_eq!(
            report,
            ReceptionReport {
                fraction_lost: 128,
                cumulative_lost: -2,
                highest_sequence: 0x10001,
                jitter: 960,
                last_sender_report: 0x12345678,
                delay_since_report: 65536
            }
        );
        let old = r.stats();
        for bad in [
            [rr(), vec![0x80, 201, 0, 1]].concat(),
            [rr(), rr()].concat(),
        ] {
            assert!(r.receive(&bad).is_err());
            assert_eq!(r.stats(), old);
        }
        let mut other = rr();
        other[7] = 100;
        assert_eq!(r.receive(&other), Err(Error::WrongSsrc));
        let mut fresh = Receiver::new(42);
        assert!(fresh.receive(&[rr(), vec![0]].concat()).is_err());
        assert!(fresh.receive(&other).is_ok()); // Malformed input did not pin identity.
        let mut foreign = rr();
        foreign[11] = 43;
        assert!(r.receive(&foreign).unwrap().report.is_none());
        assert_eq!(r.stats().last_report, Some(report));
    }
    #[test]
    fn requests_bye_padding_and_fir_deduplication() {
        let mut r = Receiver::new(42);
        assert_eq!(r.receive(&request(206, 1, 42, &[])).unwrap().pli, 1);
        assert_eq!(r.receive(&request(206, 1, 43, &[])).unwrap().pli, 0);
        assert_eq!(
            r.receive(&request(205, 1, 42, &[255, 255, 0, 3]))
                .unwrap()
                .nack_packets,
            3
        );
        let mut fir = request(206, 4, 0, &[0, 0, 0, 42, 255, 0, 0, 0]);
        assert_eq!(r.receive(&fir).unwrap().fir, 1);
        assert_eq!(r.receive(&fir).unwrap().fir, 0);
        fir[16] = 0;
        assert_eq!(r.receive(&fir).unwrap().fir, 1);
        let bye = [0xa1, 203, 0, 2, 0, 0, 0, 99, 0, 0, 0, 4];
        assert!(r.receive(&bye).unwrap().bye);
        assert!(r.stats().departed);
        assert!(
            r.receive(&[bye.as_slice(), rr().as_slice()].concat())
                .is_err()
        );
        assert!(r.receive(&request(206, 1, 42, &[0; 4])).is_err());
        assert!(r.receive(&request(205, 1, 42, &[])).is_err());
    }
    #[test]
    fn rejected_compound_cannot_consume_fir_or_terminate() {
        let mut r = Receiver::new(42);
        r.receive(&rr()).unwrap();
        let before = r.stats();
        let fir = request(206, 4, 0, &[0, 0, 0, 42, 7, 0, 0, 0]);
        let bye = vec![0x81, 203, 0, 1, 0, 0, 0, 99];
        assert!(r.receive(&[fir.clone(), bye, vec![0]].concat()).is_err());
        assert_eq!(r.stats(), before);
        assert_eq!(r.receive(&fir).unwrap().fir, 1);
        let mut foreign = request(206, 1, 42, &[]);
        foreign[7] = 100;
        assert_eq!(r.receive(&[rr(), foreign].concat()), Err(Error::WrongSsrc));
        assert!(!r.stats().departed);
    }

    #[test]
    fn malformed_inputs_never_panic_and_sdes_is_bounded() {
        let compound = [
            rr(),
            vec![
                0x81, 202, 0, 3, 0, 0, 0, 99, 1, 3, b'a', b'b', b'c', 0, 0, 0,
            ],
        ]
        .concat();
        assert!(Receiver::new(42).receive(&compound).is_ok());
        for end in 0..compound.len() {
            let _ = Receiver::new(42).receive(&compound[..end]);
        }
        for at in 0..compound.len() {
            for byte in [0, 1, 31, 128, 255] {
                let mut changed = compound.clone();
                changed[at] = byte;
                let _ = Receiver::new(42).receive(&changed);
            }
        }
        assert!(Receiver::new(42).receive(&vec![0; 2049]).is_err());
    }
}
