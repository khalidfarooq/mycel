use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
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
}
