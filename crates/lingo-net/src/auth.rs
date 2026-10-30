//! Application-level device authentication.
//!
//! Both peers contribute a fresh challenge, sign a payload that includes the
//! QUIC connection's exported keying material, and verify the other side's
//! Ed25519 signature. The exporter binding prevents proofs from being replayed
//! on another connection; the challenges prevent replay on the same connection.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};

use lingo_core::identity::Identity;
use lingo_core::model::{DeviceId, ModelError};

pub const AUTH_LABEL: &[u8] = b"lingo-auth-v1";
pub const NONCE_LEN: usize = 32;
pub const EXPORTER_LEN: usize = 32;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthProof {
    pub did: DeviceId,
    pub signature: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthContext {
    pub client_did: DeviceId,
    pub server_did: DeviceId,
    pub client_nonce: [u8; NONCE_LEN],
    pub server_nonce: [u8; NONCE_LEN],
    pub exporter: [u8; EXPORTER_LEN],
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("invalid device id: {0}")]
    InvalidDid(#[from] ModelError),
    #[error("invalid public key")]
    InvalidPublicKey,
    #[error("malformed proof")]
    Malformed,
    #[error("invalid signature")]
    BadSignature,
    #[error("expected device {expected}, got {actual}")]
    UnexpectedDid {
        expected: DeviceId,
        actual: DeviceId,
    },
    #[error("could not export connection keying material")]
    Exporter,
}

impl AuthContext {
    fn message(&self) -> Vec<u8> {
        let mut message = Vec::with_capacity(128);
        message.extend_from_slice(AUTH_LABEL);
        push_field(&mut message, &self.exporter);
        push_field(&mut message, self.client_did.as_str().as_bytes());
        push_field(&mut message, self.server_did.as_str().as_bytes());
        push_field(&mut message, &self.client_nonce);
        push_field(&mut message, &self.server_nonce);
        message
    }
}

pub fn new_nonce() -> [u8; NONCE_LEN] {
    let mut nonce = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce);
    nonce
}

pub fn encode_nonce(nonce: &[u8; NONCE_LEN]) -> String {
    URL_SAFE_NO_PAD.encode(nonce)
}

pub fn decode_nonce(value: &str) -> Result<[u8; NONCE_LEN], AuthError> {
    URL_SAFE_NO_PAD
        .decode(value.as_bytes())
        .map_err(|_| AuthError::Malformed)?
        .try_into()
        .map_err(|_| AuthError::Malformed)
}

pub fn create_proof(identity: &Identity, context: &AuthContext) -> AuthProof {
    let signature = identity.sign(&context.message());
    AuthProof {
        did: identity.did(),
        signature: URL_SAFE_NO_PAD.encode(signature.to_bytes()),
    }
}

pub fn verify_proof(
    proof: &AuthProof,
    context: &AuthContext,
    expected_did: &DeviceId,
) -> Result<(), AuthError> {
    if proof.did != *expected_did {
        return Err(AuthError::UnexpectedDid {
            expected: expected_did.clone(),
            actual: proof.did.clone(),
        });
    }

    let key_bytes = expected_did.to_bytes()?;
    let verifying_key =
        VerifyingKey::from_bytes(&key_bytes).map_err(|_| AuthError::InvalidPublicKey)?;
    let signature_bytes: [u8; 64] = URL_SAFE_NO_PAD
        .decode(proof.signature.as_bytes())
        .map_err(|_| AuthError::Malformed)?
        .try_into()
        .map_err(|_| AuthError::Malformed)?;
    let signature = Signature::from_bytes(&signature_bytes);

    verifying_key
        .verify(&context.message(), &signature)
        .map_err(|_| AuthError::BadSignature)
}

fn push_field(message: &mut Vec<u8>, value: &[u8]) {
    message.extend_from_slice(&(value.len() as u32).to_be_bytes());
    message.extend_from_slice(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> (Identity, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let identity = Identity::load_or_create(dir.path()).unwrap();
        (identity, dir)
    }

    fn context(identity: &Identity, exporter: [u8; EXPORTER_LEN]) -> AuthContext {
        AuthContext {
            client_did: identity.did(),
            server_did: DeviceId::from_public_key(&[9u8; 32]),
            client_nonce: [1u8; NONCE_LEN],
            server_nonce: [2u8; NONCE_LEN],
            exporter,
        }
    }

    #[test]
    fn proof_round_trip() {
        let (identity, _dir) = identity();
        let context = context(&identity, [3u8; EXPORTER_LEN]);
        let proof = create_proof(&identity, &context);
        verify_proof(&proof, &context, &identity.did()).unwrap();
    }

    #[test]
    fn rejects_a_forged_signature() {
        let (identity, _dir) = identity();
        let context = context(&identity, [3u8; EXPORTER_LEN]);
        let proof = create_proof(&identity, &context);
        let tampered = AuthContext {
            server_nonce: [7u8; NONCE_LEN],
            ..context
        };
        assert!(matches!(
            verify_proof(&proof, &tampered, &identity.did()),
            Err(AuthError::BadSignature)
        ));
    }

    #[test]
    fn rejects_a_proof_on_another_connection() {
        let (identity, _dir) = identity();
        let first = context(&identity, [3u8; EXPORTER_LEN]);
        let proof = create_proof(&identity, &first);
        let second = context(&identity, [4u8; EXPORTER_LEN]);
        assert!(matches!(
            verify_proof(&proof, &second, &identity.did()),
            Err(AuthError::BadSignature)
        ));
    }

    #[test]
    fn rejects_an_unexpected_device() {
        let (identity, _dir) = identity();
        let context = context(&identity, [3u8; EXPORTER_LEN]);
        let proof = create_proof(&identity, &context);
        let other = DeviceId::from_public_key(&[8u8; 32]);
        assert!(matches!(
            verify_proof(&proof, &context, &other),
            Err(AuthError::UnexpectedDid { .. })
        ));
    }

    #[test]
    fn nonce_round_trip() {
        let nonce = new_nonce();
        assert_eq!(decode_nonce(&encode_nonce(&nonce)).unwrap(), nonce);
    }

    #[test]
    fn rejects_malformed_nonce() {
        assert!(matches!(
            decode_nonce("not-a-nonce"),
            Err(AuthError::Malformed)
        ));
    }
}
