//! SRTP/SRTCP protection and HomeKit's negotiated Opus timestamp clock.

use webrtc_srtp::context::Context;
use webrtc_srtp::protection_profile::ProtectionProfile;

/// Independent inbound and outbound cryptographic contexts for one HomeKit
/// audio session. RFC 3711 requires distinct state for the two directions.
pub struct SrtpSession {
    outbound: Context,
    inbound: Context,
}

impl SrtpSession {
    /// `master` is the HomeKit SRTP material: 16-byte key || 14-byte salt.
    pub fn new(master: &[u8]) -> Option<Self> {
        if master.len() != 30 {
            return None;
        }
        let create = || {
            Context::new(
                &master[..16],
                &master[16..],
                ProtectionProfile::Aes128CmHmacSha1_80,
                None,
                None,
            )
            .ok()
        };
        Some(Self {
            outbound: create()?,
            inbound: create()?,
        })
    }

    pub fn protect_rtp(&mut self, packet: &[u8]) -> Option<Vec<u8>> {
        self.outbound.encrypt_rtp(packet).ok().map(|v| v.to_vec())
    }

    pub fn protect_rtcp(&mut self, packet: &[u8]) -> Option<Vec<u8>> {
        self.outbound.encrypt_rtcp(packet).ok().map(|v| v.to_vec())
    }

    pub fn unprotect_rtcp(&mut self, packet: &[u8]) -> Option<Vec<u8>> {
        self.inbound.decrypt_rtcp(packet).ok().map(|v| v.to_vec())
    }
}

/// Rewrites FFmpeg's 48 kHz Opus timestamps to the clock selected by HomeKit.
/// Keeping a stable random output base also hides discontinuities in the
/// camera's source timestamps.
pub struct TimestampRescaler {
    ratio: u32,
    base_in: Option<u32>,
    base_out: u32,
}

impl TimestampRescaler {
    pub fn new(ratio: u32) -> Self {
        Self {
            ratio: ratio.max(1),
            base_in: None,
            base_out: rand::random::<u16>() as u32,
        }
    }

    pub fn rescale_timestamp(&mut self, timestamp: u32) -> u32 {
        let base_in = *self.base_in.get_or_insert(timestamp);
        self.base_out
            .wrapping_add(timestamp.wrapping_sub(base_in) / self.ratio)
    }

    pub fn rescale_rtp(&mut self, packet: &mut [u8]) {
        if packet.len() < 12 {
            return;
        }
        let timestamp = u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]);
        let scaled = self.rescale_timestamp(timestamp);
        packet[4..8].copy_from_slice(&scaled.to_be_bytes());
    }

    /// Rewrites the RTP timestamp in each Sender Report of a compound RTCP
    /// packet so its clock agrees with the rewritten RTP packets.
    pub fn rescale_rtcp(&mut self, packet: &mut [u8]) {
        let mut offset = 0;
        while offset + 4 <= packet.len() {
            let words = u16::from_be_bytes([packet[offset + 2], packet[offset + 3]]) as usize + 1;
            let length = words * 4;
            if length < 4 || offset + length > packet.len() {
                break;
            }
            if packet[offset + 1] == 200 && length >= 20 {
                let timestamp = u32::from_be_bytes([
                    packet[offset + 16],
                    packet[offset + 17],
                    packet[offset + 18],
                    packet[offset + 19],
                ]);
                let scaled = self.rescale_timestamp(timestamp);
                packet[offset + 16..offset + 20].copy_from_slice(&scaled.to_be_bytes());
            }
            offset += length;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protects_rtp_and_rtcp() {
        let master = [7u8; 30];
        let mut session = SrtpSession::new(&master).unwrap();

        let mut rtp = vec![0x80, 110, 0, 1, 0, 0, 3, 0xE8, 0, 0, 0, 42];
        rtp.extend_from_slice(b"opusdata");
        let protected_rtp = session.protect_rtp(&rtp).unwrap();
        assert_eq!(protected_rtp.len(), rtp.len() + 10);
        assert_eq!(&protected_rtp[..12], &rtp[..12]);

        let mut rtcp = vec![0x80, 200, 0, 6];
        rtcp.extend_from_slice(&42u32.to_be_bytes());
        rtcp.extend_from_slice(&[0u8; 20]);
        let protected_rtcp = session.protect_rtcp(&rtcp).unwrap();
        assert_eq!(protected_rtcp.len(), rtcp.len() + 14);
    }

    #[test]
    fn rescaler_keeps_rtp_and_sender_report_clocks_aligned() {
        let mut rescaler = TimestampRescaler::new(3);
        let mut rtp = vec![0u8; 12];
        rtp[4..8].copy_from_slice(&9960u32.to_be_bytes());
        rescaler.rescale_rtp(&mut rtp);
        let rtp_timestamp = u32::from_be_bytes(rtp[4..8].try_into().unwrap());

        let mut rtcp = vec![0x80, 200, 0, 6];
        rtcp.extend_from_slice(&42u32.to_be_bytes());
        rtcp.extend_from_slice(&[0u8; 8]);
        rtcp.extend_from_slice(&9960u32.to_be_bytes());
        rtcp.extend_from_slice(&[0u8; 8]);
        rescaler.rescale_rtcp(&mut rtcp);
        let rtcp_timestamp = u32::from_be_bytes(rtcp[16..20].try_into().unwrap());

        assert_eq!(rtcp_timestamp, rtp_timestamp);
    }
}
