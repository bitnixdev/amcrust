//! Bounded outbound RTCP formatting, independent of acquisition and clock policy.
//! A caller supplies an explicit sender-clock pair; this module never substitutes
//! packet arrival time or guesses a source-to-NTP mapping.

mod feedback;
pub use feedback::{Feedback, FeedbackStats, Receiver, ReceptionReport};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// RFC 3550 counters wrap modulo 2^32. These count kernel-accepted RTP only.
    pub packets: u32,
    /// RTP payload bytes, excluding headers, extensions, padding and SRTP overhead.
    pub octets: u32,
}
impl Counts {
    pub(crate) fn accepted(&mut self, octets: u32) {
        self.packets = self.packets.wrapping_add(1);
        self.octets = self.octets.wrapping_add(octets);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockPair {
    /// NTP fixed-point timestamp, including its original fractional bits.
    pub ntp: u64,
    /// This session's RTP clock at that NTP instant, not simply the last packet.
    pub rtp: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Malformed,
    WrongSsrc,
    DuplicateReport,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RTCP: {self:?}")
    }
}
impl std::error::Error for Error {}

/// One track's report formatter. Pass the SAME random identity to both tracks of
/// a viewer session, and a fresh identity to each new session. No host name, camera
/// address, credentials or HAP session ID are embedded in the CNAME.
pub struct Sender {
    ssrc: u32,
    cname: [u8; 32],
}
impl Sender {
    pub fn new(ssrc: u32, identity: [u8; 16]) -> Self {
        let mut cname = [0; 32];
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for (i, byte) in identity.into_iter().enumerate() {
            cname[2 * i] = HEX[usize::from(byte >> 4)];
            cname[2 * i + 1] = HEX[usize::from(byte & 15)];
        }
        Self { ssrc, cname }
    }
    /// Compound SR + SDES/CNAME. Counts must describe accepted RTP writes, not
    /// attempted packets or protected datagram bytes. Clock qualification belongs
    /// to the producer; formatting is not evidence of capture-clock accuracy.
    pub fn report(&self, clock: ClockPair, counts: Counts) -> [u8; 72] {
        let mut bytes = [0; 72];
        bytes[..4].copy_from_slice(&[0x80, 200, 0, 6]);
        bytes[4..8].copy_from_slice(&self.ssrc.to_be_bytes());
        bytes[8..16].copy_from_slice(&clock.ntp.to_be_bytes());
        bytes[16..20].copy_from_slice(&clock.rtp.to_be_bytes());
        bytes[20..24].copy_from_slice(&counts.packets.to_be_bytes());
        bytes[24..28].copy_from_slice(&counts.octets.to_be_bytes());
        bytes[28..32].copy_from_slice(&[0x81, 202, 0, 10]);
        bytes[32..36].copy_from_slice(&self.ssrc.to_be_bytes());
        bytes[36..38].copy_from_slice(&[1, 32]);
        bytes[38..70].copy_from_slice(&self.cname);
        // END item plus zero alignment byte are already initialized.
        bytes
    }
    /// BYE block to append to a compound report; not permission to send standalone
    /// reduced-size RTCP when that mode has not been negotiated.
    pub fn bye(&self) -> [u8; 8] {
        let mut bytes = [0x81, 203, 0, 1, 0, 0, 0, 0];
        bytes[4..].copy_from_slice(&self.ssrc.to_be_bytes());
        bytes
    }
}

/// RFC 3550 payload accounting, also refusing malformed CSRC/extension/padding
/// lengths before encryption. The caller separately bounds total datagram size.
pub(crate) fn payload_octets(bytes: &[u8]) -> Result<u32, Error> {
    if bytes.len() < 12 || bytes[0] >> 6 != 2 {
        return Err(Error::Malformed);
    }
    let mut start = 12 + usize::from(bytes[0] & 15) * 4;
    if start > bytes.len() {
        return Err(Error::Malformed);
    }
    if bytes[0] & 0x10 != 0 {
        if bytes.len() - start < 4 {
            return Err(Error::Malformed);
        }
        let words = usize::from(u16::from_be_bytes([bytes[start + 2], bytes[start + 3]]));
        start += 4 + words * 4;
        if start > bytes.len() {
            return Err(Error::Malformed);
        }
    }
    let mut payload = bytes.len() - start;
    if bytes[0] & 0x20 != 0 {
        let padding = usize::from(bytes[bytes.len() - 1]);
        if padding == 0 || padding > payload {
            return Err(Error::Malformed);
        }
        payload -= padding;
    }
    u32::try_from(payload).map_err(|_| Error::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reports_preserve_explicit_clock_and_session_identity() {
        let clock = ClockPair {
            ntp: 0xffff_ffff_1234_5678,
            rtp: 0xffff_ff00,
        };
        let counts = Counts {
            packets: 17,
            octets: 2300,
        };
        let video = Sender::new(42, [7; 16]).report(clock, counts);
        assert_eq!(&video[..8], &[0x80, 200, 0, 6, 0, 0, 0, 42]);
        assert_eq!(&video[8..16], &clock.ntp.to_be_bytes());
        assert_eq!(&video[16..20], &clock.rtp.to_be_bytes());
        assert_eq!(&video[20..28], &[0, 0, 0, 17, 0, 0, 8, 252]);
        assert_eq!(&video[28..38], &[0x81, 202, 0, 10, 0, 0, 0, 42, 1, 32]);
        assert_eq!(&video[70..], &[0, 0]);
        let audio = Sender::new(43, [7; 16]).report(clock, counts);
        assert_eq!(&video[38..70], &audio[38..70]);
        assert_ne!(
            &video[38..70],
            &Sender::new(42, [8; 16]).report(clock, counts)[38..70]
        );
        let compound = [video.as_slice(), &Sender::new(42, [7; 16]).bye()].concat();
        assert!(Receiver::new(42).receive(&compound).unwrap().bye);
    }
    #[test]
    fn payload_counts_exclude_csrc_extensions_and_padding() {
        let mut packet = vec![0xb1, 96, 0, 1, 0, 0, 0, 0, 0, 0, 0, 42];
        packet.extend_from_slice(&[0, 0, 0, 43]); // CSRC
        packet.extend_from_slice(&[0, 1, 0, 1, 1, 2, 3, 4]); // one extension word
        packet.extend_from_slice(&[5, 6, 7, 0, 0, 0, 4]); // payload, four padding bytes
        assert_eq!(payload_octets(&packet), Ok(3));
        *packet.last_mut().unwrap() = 0;
        assert!(payload_octets(&packet).is_err());
        packet[18..20].copy_from_slice(&u16::MAX.to_be_bytes());
        assert!(payload_octets(&packet).is_err());
        let mut counts = Counts {
            packets: u32::MAX,
            octets: u32::MAX,
        };
        counts.accepted(3);
        assert_eq!(
            counts,
            Counts {
                packets: 0,
                octets: 2
            }
        );
    }
}
