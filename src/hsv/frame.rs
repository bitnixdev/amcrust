//! HDS TCP frame encryption: HKDF-SHA512 key derivation and
//! ChaCha20-Poly1305 (IETF) framing. See docs/hds-wire-format.md §3–§4.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use hkdf::Hkdf;
use sha2::Sha512;

pub const MAX_PAYLOAD: usize = 0xF_FFFF;
pub const FRAME_HEADER_LEN: usize = 4;
pub const TAG_LEN: usize = 16;

/// Per-session HDS keys, derived at SetupDataStreamTransport time.
#[derive(Clone)]
pub struct SessionKeys {
    /// Accessory → controller ("HDS-Read-Encryption-Key").
    pub accessory_to_controller: [u8; 32],
    /// Controller → accessory ("HDS-Write-Encryption-Key").
    pub controller_to_accessory: [u8; 32],
}

pub fn derive_keys(
    shared_secret: &[u8; 32],
    controller_salt: &[u8],
    accessory_salt: &[u8],
) -> SessionKeys {
    let mut salt = Vec::with_capacity(controller_salt.len() + accessory_salt.len());
    salt.extend_from_slice(controller_salt);
    salt.extend_from_slice(accessory_salt);

    let hk = Hkdf::<Sha512>::new(Some(&salt), shared_secret);
    let mut a2c = [0u8; 32];
    let mut c2a = [0u8; 32];
    hk.expand(b"HDS-Read-Encryption-Key", &mut a2c).unwrap();
    hk.expand(b"HDS-Write-Encryption-Key", &mut c2a).unwrap();

    SessionKeys {
        accessory_to_controller: a2c,
        controller_to_accessory: c2a,
    }
}

fn nonce_for(counter: u64) -> Nonce {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&counter.to_le_bytes());
    Nonce::from(nonce)
}

/// Encrypts one frame (accessory → controller), advancing the counter.
pub fn encrypt_frame(
    key: &[u8; 32],
    counter: &mut u64,
    plaintext: &[u8],
) -> Result<Vec<u8>, String> {
    if plaintext.len() > MAX_PAYLOAD {
        return Err(format!("HDS payload too large: {}", plaintext.len()));
    }
    let mut header = [0u8; FRAME_HEADER_LEN];
    header[0] = 0x01;
    header[1..4].copy_from_slice(&(plaintext.len() as u32).to_be_bytes()[1..4]);

    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let ciphertext = cipher
        .encrypt(
            &nonce_for(*counter),
            Payload {
                msg: plaintext,
                aad: &header,
            },
        )
        .map_err(|e| format!("HDS encrypt failed: {e}"))?;
    *counter += 1;

    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + ciphertext.len());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(&ciphertext); // includes the 16-byte tag
    Ok(frame)
}

/// Attempts to decrypt one full frame (header + ciphertext + tag) without
/// advancing the caller's counter unless successful.
pub fn decrypt_frame(key: &[u8; 32], counter: &mut u64, frame: &[u8]) -> Result<Vec<u8>, String> {
    if frame.len() < FRAME_HEADER_LEN + TAG_LEN {
        return Err("frame too short".into());
    }
    let header = &frame[..FRAME_HEADER_LEN];
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let plaintext = cipher
        .decrypt(
            &nonce_for(*counter),
            Payload {
                msg: &frame[FRAME_HEADER_LEN..],
                aad: header,
            },
        )
        .map_err(|_| "HDS decrypt failed".to_string())?;
    *counter += 1;
    Ok(plaintext)
}

/// Parses the length of the next frame from a buffer, if a complete frame is
/// available. Returns the total frame length (header + payload + tag).
pub fn complete_frame_len(buf: &[u8]) -> Result<Option<usize>, String> {
    if buf.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    if buf[0] != 0x01 {
        return Err(format!("unexpected HDS payload type {:#04x}", buf[0]));
    }
    let len = ((buf[1] as usize) << 16) | ((buf[2] as usize) << 8) | buf[3] as usize;
    if len > MAX_PAYLOAD + TAG_LEN {
        return Err(format!("HDS frame too large: {len}"));
    }
    let total = FRAME_HEADER_LEN + len + TAG_LEN;
    Ok(if buf.len() >= total {
        Some(total)
    } else {
        None
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let keys = derive_keys(&[7u8; 32], &[1u8; 32], &[2u8; 32]);
        let mut tx = 0u64;
        let mut rx = 0u64;

        let payload = b"hello hds".to_vec();
        let frame = encrypt_frame(&keys.accessory_to_controller, &mut tx, &payload).unwrap();
        assert_eq!(tx, 1);
        assert_eq!(complete_frame_len(&frame).unwrap(), Some(frame.len()));

        let decrypted = decrypt_frame(&keys.accessory_to_controller, &mut rx, &frame).unwrap();
        assert_eq!(decrypted, payload);
        assert_eq!(rx, 1);
    }

    #[test]
    fn failed_decrypt_does_not_advance_counter() {
        let keys = derive_keys(&[7u8; 32], &[1u8; 32], &[2u8; 32]);
        let wrong = derive_keys(&[8u8; 32], &[1u8; 32], &[2u8; 32]);
        let mut tx = 0u64;
        let frame = encrypt_frame(&keys.accessory_to_controller, &mut tx, b"x").unwrap();

        let mut rx = 0u64;
        assert!(decrypt_frame(&wrong.accessory_to_controller, &mut rx, &frame).is_err());
        assert_eq!(rx, 0);
        assert!(decrypt_frame(&keys.accessory_to_controller, &mut rx, &frame).is_ok());
        assert_eq!(rx, 1);
    }

    #[test]
    fn direction_keys_differ() {
        let keys = derive_keys(&[7u8; 32], &[1u8; 32], &[2u8; 32]);
        assert_ne!(keys.accessory_to_controller, keys.controller_to_accessory);
    }
}
