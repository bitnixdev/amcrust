//! SRTP/SRTCP primitives for source-bound Rust media transports.

use webrtc_srtp::context::Context;
use webrtc_srtp::protection_profile::ProtectionProfile;

/// Independent inbound and outbound cryptographic contexts for one HomeKit
/// media session. RFC 3711 requires distinct state for the two directions.
/// Feedback pins the first authenticated sender SSRC and uses a 128-packet replay
/// window; another sender requires a new session. Use media::transport for bounded
/// outgoing SSRCs, datagram sizes and actual socket-write ownership.
pub struct SrtpSession {
    outbound: Context,
    // Pin only the first authenticated feedback SSRC. Unauthenticated packets
    // must not grow the upstream context's per-SSRC replay-state map.
    inbound: Option<(u32, Context)>,
    master: [u8; 30],
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
            inbound: None,
            master: master.try_into().ok()?,
        })
    }

    pub fn protect_rtp(&mut self, packet: &[u8]) -> Option<Vec<u8>> {
        self.outbound.encrypt_rtp(packet).ok().map(|v| v.to_vec())
    }

    pub fn protect_rtcp(&mut self, packet: &[u8]) -> Option<Vec<u8>> {
        self.outbound.encrypt_rtcp(packet).ok().map(|v| v.to_vec())
    }

    pub fn unprotect_rtcp(&mut self, packet: &[u8]) -> Option<Vec<u8>> {
        if packet.len() < 22 {
            return None;
        }
        let ssrc = u32::from_be_bytes(packet[4..8].try_into().ok()?);
        if let Some((expected, context)) = &mut self.inbound {
            if *expected != ssrc {
                return None;
            }
            return context.decrypt_rtcp(packet).ok().map(|v| v.to_vec());
        }
        let mut context = Context::new(
            &self.master[..16],
            &self.master[16..],
            ProtectionProfile::Aes128CmHmacSha1_80,
            None,
            Some(webrtc_srtp::option::srtcp_replay_protection(128)),
        )
        .ok()?;
        let plain = context.decrypt_rtcp(packet).ok()?.to_vec();
        self.inbound = Some((ssrc, context));
        Some(plain)
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
}
