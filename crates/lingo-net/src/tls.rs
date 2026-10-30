//! TLS configuration for peer connections.
//!
//! Every device presents a self-signed ECDSA P-256 certificate. rustls' chain
//! validation is intentionally not used for authentication: devices authenticate
//! each other with the Ed25519 `AuthProof` exchanged immediately after the
//! handshake. This keeps the Ed25519 DID independent of X.509 signature support.

use std::sync::Arc;

use rcgen::{CertificateParams, KeyPair};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{DigitallySignedStruct, SignatureScheme};

use crate::NetError;

fn ensure_provider() {
    let _ =
        rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider());
}

pub fn self_signed() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), NetError> {
    ensure_provider();
    let params = CertificateParams::default();
    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;
    let key = PrivatePkcs8KeyDer::from(key_pair.serialize_der()).into();
    Ok((cert.der().clone(), key))
}

pub fn client_config() -> Result<quinn::ClientConfig, NetError> {
    ensure_provider();
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(InsecureServerVerifier))
        .with_no_client_auth();
    let config = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .map_err(|err| NetError::TlsConfig(format!("{err:?}")))?;
    Ok(quinn::ClientConfig::new(Arc::new(config)))
}

pub fn server_config() -> Result<quinn::ServerConfig, NetError> {
    let (cert, key) = self_signed()?;
    Ok(quinn::ServerConfig::with_single_cert(vec![cert], key)?)
}

#[derive(Debug)]
struct InsecureServerVerifier;

impl ServerCertVerifier for InsecureServerVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PKCS1_SHA256,
        ]
    }
}
