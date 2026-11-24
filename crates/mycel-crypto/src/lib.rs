use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
use ring::agreement::{agree_ephemeral, EphemeralPrivateKey, UnparsedPublicKey, X25519};
use ring::hkdf::{KeyType, Salt, HKDF_SHA256};
use ring::rand::SystemRandom;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CryptoError {
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),
    #[error("Hex decoding error: {0}")]
    HexError(#[from] hex::FromHexError),
    #[error("Key format error: {0}")]
    KeyError(String),
    #[error("Key exchange error: {0}")]
    KeyExchangeError(String),
    #[error("AEAD error: {0}")]
    AeadError(String),
}

#[derive(Clone)]
pub struct NodeIdentity {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
    peer_id: String,
}

impl NodeIdentity {
    pub fn generate() -> Self {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();
        let pub_hex = hex::encode(verifying_key.as_bytes());
        let peer_id = format!("12D3KooW{}", &pub_hex[..20]);

        Self {
            signing_key,
            verifying_key,
            peer_id,
        }
    }

    pub fn from_bytes(secret_bytes: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(secret_bytes);
        let verifying_key = signing_key.verifying_key();
        let pub_hex = hex::encode(verifying_key.as_bytes());
        let peer_id = format!("12D3KooW{}", &pub_hex[..20]);

        Self {
            signing_key,
            verifying_key,
            peer_id,
        }
    }

    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    pub fn public_key_hex(&self) -> String {
        hex::encode(self.verifying_key.as_bytes())
    }

    pub fn secret_bytes(&self) -> [u8; 32] {
        self.signing_key.to_bytes()
    }

    pub fn sign(&self, message: &[u8]) -> String {
        let signature = self.signing_key.sign(message);
        hex::encode(signature.to_bytes())
    }

    /// Verify that a `peer_id` is deterministically derived from `public_key_hex`
    /// and that `public_key_hex` decodes to a valid 32-byte Ed25519 public key.
    pub fn verify_peer_id(peer_id: &str, public_key_hex: &str) -> Result<(), CryptoError> {
        let pub_bytes = hex::decode(public_key_hex)?;
        if pub_bytes.len() != 32 {
            return Err(CryptoError::KeyError("Public key must be 32 bytes".into()));
        }
        let mut key_arr = [0u8; 32];
        key_arr.copy_from_slice(&pub_bytes);
        VerifyingKey::from_bytes(&key_arr)
            .map_err(|e| CryptoError::KeyError(e.to_string()))?;

        let canonical_hex = hex::encode(key_arr);
        let expected_peer_id = format!("12D3KooW{}", &canonical_hex[..20]);
        if peer_id != expected_peer_id {
            return Err(CryptoError::KeyError(format!(
                "Peer ID {} does not match public key {}",
                peer_id, expected_peer_id
            )));
        }
        Ok(())
    }

    pub fn verify_signature(
        public_key_hex: &str,
        message: &[u8],
        signature_hex: &str,
    ) -> Result<bool, CryptoError> {
        let pub_bytes = hex::decode(public_key_hex)?;
        if pub_bytes.len() != 32 {
            return Err(CryptoError::KeyError("Public key must be 32 bytes".into()));
        }
        let mut key_arr = [0u8; 32];
        key_arr.copy_from_slice(&pub_bytes);
        let verifying_key = VerifyingKey::from_bytes(&key_arr)
            .map_err(|e| CryptoError::KeyError(e.to_string()))?;

        let sig_bytes = hex::decode(signature_hex)?;
        if sig_bytes.len() != 64 {
            return Err(CryptoError::InvalidSignature("Signature must be 64 bytes".into()));
        }
        let mut sig_arr = [0u8; 64];
        sig_arr.copy_from_slice(&sig_bytes);
        let signature = Signature::from_bytes(&sig_arr);

        match verifying_key.verify(message, &signature) {
            Ok(_) => Ok(true),
            Err(e) => Err(CryptoError::InvalidSignature(e.to_string())),
        }
    }

    pub fn sha256_digest(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }
}

impl Default for NodeIdentity {
    fn default() -> Self {
        Self::generate()
    }
}

struct ChaCha20KeyLen;

impl KeyType for ChaCha20KeyLen {
    fn len(&self) -> usize {
        32
    }
}

pub struct EphemeralKeyPair {
    private_key: EphemeralPrivateKey,
    pub public_key_bytes: [u8; 32],
}

impl EphemeralKeyPair {
    pub fn generate() -> Result<Self, CryptoError> {
        let rng = SystemRandom::new();
        let private_key = EphemeralPrivateKey::generate(&X25519, &rng)
            .map_err(|_| CryptoError::KeyExchangeError("Failed to generate X25519 keypair".into()))?;
        let public_key = private_key
            .compute_public_key()
            .map_err(|_| CryptoError::KeyExchangeError("Failed to compute X25519 public key".into()))?;
        let pub_slice = public_key.as_ref();
        if pub_slice.len() != 32 {
            return Err(CryptoError::KeyExchangeError("Unexpected X25519 public key length".into()));
        }
        let mut public_key_bytes = [0u8; 32];
        public_key_bytes.copy_from_slice(pub_slice);
        Ok(Self {
            private_key,
            public_key_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeRole {
    Initiator,
    Responder,
}

pub struct SealCipher {
    key: LessSafeKey,
    seq: u64,
}

impl SealCipher {
    fn from_raw_key(raw: &[u8; 32]) -> Result<Self, CryptoError> {
        let unbound = UnboundKey::new(&CHACHA20_POLY1305, raw)
            .map_err(|_| CryptoError::AeadError("Failed to initialize ChaCha20-Poly1305 key".into()))?;
        Ok(Self {
            key: LessSafeKey::new(unbound),
            seq: 0,
        })
    }

    pub fn seal(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if self.seq == u64::MAX {
            return Err(CryptoError::AeadError("Sequence nonce exhausted".into()));
        }
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[4..12].copy_from_slice(&self.seq.to_be_bytes());
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let aad_bytes = self.seq.to_be_bytes();
        let aad = Aad::from(&aad_bytes);

        let mut buf = Vec::with_capacity(plaintext.len() + CHACHA20_POLY1305.tag_len());
        buf.extend_from_slice(plaintext);
        self.key
            .seal_in_place_append_tag(nonce, aad, &mut buf)
            .map_err(|_| CryptoError::AeadError("AEAD encryption failed".into()))?;
        self.seq += 1;
        Ok(buf)
    }

    pub fn sequence(&self) -> u64 {
        self.seq
    }
}

pub struct OpenCipher {
    key: LessSafeKey,
    seq: u64,
}

impl OpenCipher {
    fn from_raw_key(raw: &[u8; 32]) -> Result<Self, CryptoError> {
        let unbound = UnboundKey::new(&CHACHA20_POLY1305, raw)
            .map_err(|_| CryptoError::AeadError("Failed to initialize ChaCha20-Poly1305 key".into()))?;
        Ok(Self {
            key: LessSafeKey::new(unbound),
            seq: 0,
        })
    }

    pub fn open(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if self.seq == u64::MAX {
            return Err(CryptoError::AeadError("Sequence nonce exhausted".into()));
        }
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[4..12].copy_from_slice(&self.seq.to_be_bytes());
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let aad_bytes = self.seq.to_be_bytes();
        let aad = Aad::from(&aad_bytes);

        let mut buf = ciphertext.to_vec();
        let plain_len = {
            let plain = self
                .key
                .open_in_place(nonce, aad, &mut buf)
                .map_err(|_| CryptoError::AeadError("AEAD authentication/decryption failed".into()))?;
            plain.len()
        };
        buf.truncate(plain_len);
        self.seq += 1;
        Ok(buf)
    }

    pub fn sequence(&self) -> u64 {
        self.seq
    }
}

/// Derive directional ChaCha20-Poly1305 AEAD ciphers using X25519 + HKDF-SHA256.
pub fn derive_session_ciphers(
    local_ephem: EphemeralKeyPair,
    remote_ephem_pub: &[u8; 32],
    initiator_peer_id: &str,
    responder_peer_id: &str,
    initiator_ephem_pub: &[u8; 32],
    responder_ephem_pub: &[u8; 32],
    role: HandshakeRole,
) -> Result<(SealCipher, OpenCipher), CryptoError> {
    let peer_pub = UnparsedPublicKey::new(&X25519, remote_ephem_pub);
    let (i2r_raw, r2i_raw) = agree_ephemeral(local_ephem.private_key, &peer_pub, |shared_secret| {
        let mut hasher = Sha256::new();
        hasher.update(b"mycel-x25519-hkdf-v1");
        hasher.update(initiator_peer_id.as_bytes());
        hasher.update(responder_peer_id.as_bytes());
        hasher.update(initiator_ephem_pub);
        hasher.update(responder_ephem_pub);
        let salt_bytes = hasher.finalize();

        let salt = Salt::new(HKDF_SHA256, &salt_bytes);
        let prk = salt.extract(shared_secret);

        let mut i2r = [0u8; 32];
        let mut r2i = [0u8; 32];

        prk.expand(&[b"mycel-i2r-chacha20poly1305"], ChaCha20KeyLen)
            .and_then(|okm| okm.fill(&mut i2r))
            .map_err(|_| CryptoError::KeyExchangeError("HKDF expand failed for i2r".into()))?;

        prk.expand(&[b"mycel-r2i-chacha20poly1305"], ChaCha20KeyLen)
            .and_then(|okm| okm.fill(&mut r2i))
            .map_err(|_| CryptoError::KeyExchangeError("HKDF expand failed for r2i".into()))?;

        Ok::<([u8; 32], [u8; 32]), CryptoError>((i2r, r2i))
    })
    .map_err(|_| CryptoError::KeyExchangeError("X25519 key agreement failed".into()))??;

    match role {
        HandshakeRole::Initiator => Ok((
            SealCipher::from_raw_key(&i2r_raw)?,
            OpenCipher::from_raw_key(&r2i_raw)?,
        )),
        HandshakeRole::Responder => Ok((
            SealCipher::from_raw_key(&r2i_raw)?,
            OpenCipher::from_raw_key(&i2r_raw)?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature_roundtrip() {
        let identity = NodeIdentity::generate();
        let payload = b"mycel-task-contract-42";

        let sig_hex = identity.sign(payload);
        let pub_hex = identity.public_key_hex();

        let is_valid = NodeIdentity::verify_signature(&pub_hex, payload, &sig_hex)
            .expect("Verification check should not fail");
        assert!(is_valid);
        NodeIdentity::verify_peer_id(identity.peer_id(), &pub_hex)
            .expect("Peer ID should match public key");
    }

    #[test]
    fn test_tampered_payload_fails() {
        let identity = NodeIdentity::generate();
        let payload = b"legitimate-task";
        let tampered = b"malicious-task";

        let sig_hex = identity.sign(payload);
        let pub_hex = identity.public_key_hex();

        let result = NodeIdentity::verify_signature(&pub_hex, tampered, &sig_hex);
        assert!(result.is_err());
    }

    #[test]
    fn test_mismatched_peer_id_fails() {
        let id1 = NodeIdentity::generate();
        let id2 = NodeIdentity::generate();
        assert!(NodeIdentity::verify_peer_id(id1.peer_id(), &id2.public_key_hex()).is_err());
    }

    #[test]
    fn test_x25519_chacha20_poly1305_session_and_tamper_rejection() {
        let init_id = NodeIdentity::generate();
        let resp_id = NodeIdentity::generate();

        let init_ephem = EphemeralKeyPair::generate().unwrap();
        let resp_ephem = EphemeralKeyPair::generate().unwrap();
        let init_pub = init_ephem.public_key_bytes;
        let resp_pub = resp_ephem.public_key_bytes;

        let (mut init_tx, mut init_rx) = derive_session_ciphers(
            init_ephem,
            &resp_pub,
            init_id.peer_id(),
            resp_id.peer_id(),
            &init_pub,
            &resp_pub,
            HandshakeRole::Initiator,
        )
        .unwrap();

        let (mut resp_tx, mut resp_rx) = derive_session_ciphers(
            resp_ephem,
            &init_pub,
            init_id.peer_id(),
            resp_id.peer_id(),
            &init_pub,
            &resp_pub,
            HandshakeRole::Responder,
        )
        .unwrap();

        let secret_msg = b"CONFIDENTIAL_MYCEL_FRAME_12345";
        let ct = init_tx.seal(secret_msg).unwrap();
        assert!(!ct.windows(secret_msg.len()).any(|w| w == secret_msg));

        let pt = resp_rx.open(&ct).unwrap();
        assert_eq!(pt, secret_msg);

        // Tampered ciphertext must be rejected
        let mut tampered = ct.clone();
        tampered[0] ^= 0xff;
        assert!(resp_rx.open(&tampered).is_err());

        // Reverse direction works too
        let reply = b"ACK_FROM_RESPONDER";
        let reply_ct = resp_tx.seal(reply).unwrap();
        let reply_pt = init_rx.open(&reply_ct).unwrap();
        assert_eq!(reply_pt, reply);
    }
}
