//! Device identity: Ed25519 keypair, DID, JWT and PEM persistence.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use pkcs8::{DecodePrivateKey, EncodePrivateKey, LineEnding};
use serde::{Deserialize, Serialize};

use crate::model::DeviceId;

#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid key material: {0}")]
    Key(#[from] pkcs8::Error),
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("malformed jwt")]
    MalformedJwt,
    #[error("invalid jwt signature")]
    BadSignature,
    #[error("jwt expired")]
    Expired,
    #[error("unexpected audience")]
    WrongAudience,
    #[error("device id does not match the signing key")]
    DidMismatch,
    #[error("invalid device id: {0}")]
    Model(#[from] crate::model::ModelError),
}

#[derive(Clone)]
pub struct Identity {
    signing: SigningKey,
}

impl Identity {
    pub fn load_or_create(dir: &Path) -> Result<Self, IdentityError> {
        let path = dir.join("id.key");
        if path.exists() {
            return Self::load(&path);
        }

        fs::create_dir_all(dir).map_err(|source| IdentityError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o700));
        }

        let signing = SigningKey::generate(&mut rand_core::OsRng);
        let pem = signing.to_pkcs8_pem(LineEnding::LF)?;

        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }

        match options.open(&path) {
            Ok(mut file) => {
                file.write_all(pem.as_bytes())
                    .and_then(|_| file.sync_all())
                    .map_err(|source| IdentityError::Io {
                        path: path.clone(),
                        source,
                    })?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                return Self::load(&path);
            }
            Err(source) => {
                return Err(IdentityError::Io { path, source });
            }
        }

        Ok(Self { signing })
    }

    pub fn load(path: &Path) -> Result<Self, IdentityError> {
        let pem = fs::read_to_string(path).map_err(|source| IdentityError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let signing = SigningKey::from_pkcs8_pem(&pem)?;
        Ok(Self { signing })
    }

    pub fn did(&self) -> DeviceId {
        DeviceId::from_public_key(self.signing.verifying_key().as_bytes())
    }

    pub fn public_key(&self) -> [u8; 32] {
        *self.signing.verifying_key().as_bytes()
    }

    pub fn sign(&self, message: &[u8]) -> Signature {
        self.signing.sign(message)
    }

    /// Create a short-lived EdDSA JWT. The service uses `kid` as the public key.
    pub fn jwt(&self, audience: &str, ttl: Duration) -> Result<String, IdentityError> {
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.jwt_at(audience, issued_at, issued_at + ttl.as_secs())
    }

    pub fn jwt_at(
        &self,
        audience: &str,
        issued_at: u64,
        expires_at: u64,
    ) -> Result<String, IdentityError> {
        let header = JwtHeader {
            alg: "EdDSA".to_string(),
            kid: self.did().as_str().to_string(),
            typ: "JWT".to_string(),
        };
        let claims = JwtClaims {
            aud: vec![audience.to_string()],
            iat: issued_at,
            exp: expires_at,
        };
        let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?);
        let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
        let signing_input = format!("{header}.{claims}");
        let signature = self.sign(signing_input.as_bytes());
        Ok(format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature.to_bytes())
        ))
    }

    pub fn verify_jwt(&self, token: &str, audience: &str) -> Result<JwtClaims, IdentityError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.verify_jwt_at(token, audience, now)
    }

    pub fn verify_jwt_at(
        &self,
        token: &str,
        audience: &str,
        now: u64,
    ) -> Result<JwtClaims, IdentityError> {
        let mut parts = token.split('.');
        let header_b64 = parts.next().ok_or(IdentityError::MalformedJwt)?;
        let claims_b64 = parts.next().ok_or(IdentityError::MalformedJwt)?;
        let signature_b64 = parts.next().ok_or(IdentityError::MalformedJwt)?;
        if parts.next().is_some() {
            return Err(IdentityError::MalformedJwt);
        }

        let header: JwtHeader = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header_b64)?)?;
        if header.alg != "EdDSA" || header.typ != "JWT" {
            return Err(IdentityError::MalformedJwt);
        }
        if header.kid != self.did().as_str() {
            return Err(IdentityError::DidMismatch);
        }

        let key_bytes = DeviceId::parse(header.kid)?.to_bytes()?;
        let verifying_key =
            VerifyingKey::from_bytes(&key_bytes).map_err(|_| IdentityError::DidMismatch)?;
        let signature_bytes: [u8; 64] = URL_SAFE_NO_PAD
            .decode(signature_b64)?
            .try_into()
            .map_err(|_| IdentityError::MalformedJwt)?;
        let signature = Signature::from_bytes(&signature_bytes);
        verifying_key
            .verify(format!("{header_b64}.{claims_b64}").as_bytes(), &signature)
            .map_err(|_| IdentityError::BadSignature)?;

        let claims: JwtClaims = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(claims_b64)?)?;
        if claims.exp <= now {
            return Err(IdentityError::Expired);
        }
        if !claims.aud.iter().any(|value| value == audience) {
            return Err(IdentityError::WrongAudience);
        }
        Ok(claims)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JwtHeader {
    pub alg: String,
    pub kid: String,
    pub typ: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JwtClaims {
    pub aud: Vec<String>,
    pub iat: u64,
    pub exp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_is_persisted_and_reloaded() {
        let dir = tempfile::tempdir().unwrap();
        let identity = Identity::load_or_create(dir.path()).unwrap();
        let did = identity.did();

        let path = dir.path().join("id.key");
        let pem = fs::read_to_string(&path).unwrap();
        assert!(pem.starts_with("-----BEGIN PRIVATE KEY-----"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }

        let reloaded = Identity::load(&path).unwrap();
        assert_eq!(reloaded.did(), did);
    }

    #[test]
    fn jwt_round_trip_and_rejects_tampering() {
        let dir = tempfile::tempdir().unwrap();
        let identity = Identity::load_or_create(dir.path()).unwrap();
        let token = identity.jwt_at("api.lingo.app", 1_000, 2_000).unwrap();

        let claims = identity
            .verify_jwt_at(&token, "api.lingo.app", 1_500)
            .unwrap();
        assert_eq!(claims.exp, 2_000);
        assert_eq!(claims.aud, vec!["api.lingo.app"]);

        assert!(matches!(
            identity.verify_jwt_at(&token, "api.lingo.app", 2_000),
            Err(IdentityError::Expired)
        ));
        assert!(matches!(
            identity.verify_jwt_at(&token, "other.app", 1_500),
            Err(IdentityError::WrongAudience)
        ));

        let mut tampered = token.to_string();
        tampered.push('x');
        assert!(
            identity
                .verify_jwt_at(&tampered, "api.lingo.app", 1_500)
                .is_err()
        );
    }

    #[test]
    fn another_identity_cannot_verify_the_token() {
        let a = Identity::load_or_create(tempfile::tempdir().unwrap().path()).unwrap();
        let b = Identity::load_or_create(tempfile::tempdir().unwrap().path()).unwrap();
        let token = a.jwt_at("api.lingo.app", 1_000, 2_000).unwrap();
        assert!(matches!(
            b.verify_jwt_at(&token, "api.lingo.app", 1_500),
            Err(IdentityError::DidMismatch)
        ));
    }
}
