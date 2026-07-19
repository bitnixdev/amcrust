//! Minimal SRTP sender (RFC 3711, AES_CM_128_HMAC_SHA1_80) for the audio RTP
//! path, where we must rewrite RTP timestamps before encryption (HomeKit
//! expects the Opus RTP clock at the negotiated sample rate, not RFC 7587's
//! 48 kHz — so ffmpeg's SRTP output can't be used directly for audio).

use aes::Aes128;
use aes::cipher::{BlockEncrypt, KeyInit, KeyIvInit, StreamCipher, generic_array::GenericArray};
use hmac::{Hmac, Mac};
use sha1::Sha1;

type Aes128Ctr = ctr::Ctr128BE<Aes128>;
type HmacSha1 = Hmac<Sha1>;

const AUTH_TAG_LEN: usize = 10; // 80 bits

pub struct SrtpSender {
    session_key: [u8; 16],
    session_salt: [u8; 14],
    auth_key: [u8; 20],
    /// Rollover counter, incremented when the RTP sequence number wraps.
    roc: u32,
    last_seq: Option<u16>,
}

impl SrtpSender {
    /// `master` is the HomeKit SRTP material: 16-byte key || 14-byte salt.
    pub fn new(master: &[u8]) -> Option<Self> {
        if master.len() < 30 {
            return None;
        }
        let master_key: [u8; 16] = master[..16].try_into().ok()?;
        let master_salt: [u8; 14] = master[16..30].try_into().ok()?;

        let mut session_key = [0u8; 16];
        derive(&master_key, &master_salt, 0x00, &mut session_key);
        let mut auth_key = [0u8; 20];
        derive(&master_key, &master_salt, 0x01, &mut auth_key);
        let mut session_salt_full = [0u8; 16];
        derive(&master_key, &master_salt, 0x02, &mut session_salt_full);
        let session_salt: [u8; 14] = session_salt_full[..14].try_into().ok()?;

        Some(Self {
            session_key,
            session_salt,
            auth_key,
            roc: 0,
            last_seq: None,
        })
    }

    /// Encrypts an RTP packet in place (payload only), appends the auth tag,
    /// and returns the SRTP packet.
    pub fn protect(&mut self, packet: &[u8]) -> Option<Vec<u8>> {
        if packet.len() < 12 || packet[0] >> 6 != 2 {
            return None;
        }
        let seq = u16::from_be_bytes([packet[2], packet[3]]);
        let ssrc = u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]);

        // Track sequence rollover.
        if let Some(last) = self.last_seq {
            if seq < 0x1000 && last > 0xF000 {
                self.roc = self.roc.wrapping_add(1);
            }
        }
        self.last_seq = Some(seq);

        let header_len = 12 + 4 * (packet[0] & 0x0F) as usize;
        if packet.len() < header_len {
            return None;
        }

        let mut out = packet.to_vec();

        // AES-CM keystream IV per RFC 3711 §4.1.1:
        // IV = (salt << 16) XOR (ssrc << 64) XOR (index << 16), index = ROC || SEQ.
        let index: u64 = ((self.roc as u64) << 16) | seq as u64;
        let mut iv = [0u8; 16];
        iv[..14].copy_from_slice(&self.session_salt);
        for (i, b) in ssrc.to_be_bytes().iter().enumerate() {
            iv[4 + i] ^= b;
        }
        for (i, b) in index.to_be_bytes()[2..].iter().enumerate() {
            iv[8 + i] ^= b;
        }

        let mut cipher = Aes128Ctr::new(
            GenericArray::from_slice(&self.session_key),
            GenericArray::from_slice(&iv),
        );
        cipher.apply_keystream(&mut out[header_len..]);

        // Auth tag: HMAC-SHA1(packet || ROC), truncated to 80 bits.
        let mut mac = <HmacSha1 as Mac>::new_from_slice(&self.auth_key).ok()?;
        mac.update(&out);
        mac.update(&self.roc.to_be_bytes());
        let tag = mac.finalize().into_bytes();
        out.extend_from_slice(&tag[..AUTH_TAG_LEN]);

        Some(out)
    }
}

/// RFC 3711 §4.3 AES-CM key derivation (key derivation rate 0).
fn derive(master_key: &[u8; 16], master_salt: &[u8; 14], label: u8, out: &mut [u8]) {
    // x = key_id XOR master_salt, where key_id = label || 0^48 placed in the
    // low 7 bytes of the 112-bit salt space.
    let mut iv = [0u8; 16];
    iv[..14].copy_from_slice(master_salt);
    iv[7] ^= label;
    // IV = x * 2^16 → the last two bytes stay zero (block counter).

    let aes = Aes128::new(GenericArray::from_slice(master_key));
    let mut counter: u16 = 0;
    for chunk in out.chunks_mut(16) {
        let mut block = iv;
        block[14..16].copy_from_slice(&counter.to_be_bytes());
        let mut b = GenericArray::from(block);
        aes.encrypt_block(&mut b);
        chunk.copy_from_slice(&b[..chunk.len()]);
        counter += 1;
    }
}

/// Rewrites the RTP timestamp from ffmpeg's 48 kHz Opus clock to the HomeKit
/// clock (the negotiated sample rate). The first packet's timestamp is kept as
/// the base so the absolute value stays in range.
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

    pub fn rescale(&mut self, packet: &mut [u8]) {
        if packet.len() < 12 {
            return;
        }
        let ts = u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]);
        let base_in = *self.base_in.get_or_insert(ts);
        let scaled = self
            .base_out
            .wrapping_add(ts.wrapping_sub(base_in) / self.ratio);
        packet[4..8].copy_from_slice(&scaled.to_be_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 3711 appendix B.3 key derivation test vectors.
    #[test]
    fn rfc3711_key_derivation() {
        let master_key: [u8; 16] = [
            0xE1, 0xF9, 0x7A, 0x0D, 0x3E, 0x01, 0x8B, 0xE0, 0xD6, 0x4F, 0xA3, 0x2C, 0x06, 0xDE,
            0x41, 0x39,
        ];
        let master_salt: [u8; 14] = [
            0x0E, 0xC6, 0x75, 0xAD, 0x49, 0x8A, 0xFE, 0xEB, 0xB6, 0x96, 0x0B, 0x3A, 0xAB, 0xE6,
        ];

        let mut cipher_key = [0u8; 16];
        derive(&master_key, &master_salt, 0x00, &mut cipher_key);
        assert_eq!(
            cipher_key,
            [
                0xC6, 0x1E, 0x7A, 0x93, 0x74, 0x4F, 0x39, 0xEE, 0x10, 0x73, 0x4A, 0xFE, 0x3F, 0xF7,
                0xA0, 0x87
            ]
        );

        let mut salt = [0u8; 16];
        derive(&master_key, &master_salt, 0x02, &mut salt);
        assert_eq!(
            &salt[..14],
            &[
                0x30, 0xCB, 0xBC, 0x08, 0x86, 0x3D, 0x8C, 0x85, 0xD4, 0x9D, 0xB3, 0x4A, 0x9A, 0xE1
            ][..]
        );

        let mut auth_key = [0u8; 20];
        derive(&master_key, &master_salt, 0x01, &mut auth_key);
        assert_eq!(
            auth_key,
            [
                0xCE, 0xBE, 0x32, 0x1F, 0x6F, 0xF7, 0x71, 0x6B, 0x6F, 0xD4, 0xAB, 0x49, 0xAF, 0x25,
                0x6A, 0x15, 0x6D, 0x38, 0xBA, 0xA4,
            ]
        );
    }

    #[test]
    fn protect_produces_tagged_packet() {
        let master = [7u8; 30];
        let mut sender = SrtpSender::new(&master).unwrap();
        // Minimal RTP packet: V=2, PT=110, seq 1, ts 1000, ssrc 42, payload.
        let mut packet = vec![0x80, 110, 0, 1, 0, 0, 3, 0xE8, 0, 0, 0, 42];
        packet.extend_from_slice(b"opusdata");
        let protected = sender.protect(&packet).unwrap();
        assert_eq!(protected.len(), packet.len() + AUTH_TAG_LEN);
        // Header must be untouched.
        assert_eq!(&protected[..12], &packet[..12]);
        // Payload must differ (encrypted).
        assert_ne!(&protected[12..packet.len()], &packet[12..]);
    }

    #[test]
    fn rescaler_divides_deltas() {
        let mut r = TimestampRescaler::new(3);
        let mut p1 = vec![0u8; 12];
        p1[4..8].copy_from_slice(&9000u32.to_be_bytes());
        r.rescale(&mut p1);
        let t1 = u32::from_be_bytes([p1[4], p1[5], p1[6], p1[7]]);

        let mut p2 = vec![0u8; 12];
        p2[4..8].copy_from_slice(&(9000u32 + 960).to_be_bytes());
        r.rescale(&mut p2);
        let t2 = u32::from_be_bytes([p2[4], p2[5], p2[6], p2[7]]);

        // 960 ticks at 48 kHz → 320 ticks at 16 kHz.
        assert_eq!(t2.wrapping_sub(t1), 320);
    }
}
