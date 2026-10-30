//! Encrypted transport, discovery and framing for Lingo.

pub mod auth;
pub mod control;
pub mod discovery;
pub mod endpoint;
pub mod framing;
pub mod peer;
pub mod session;
pub mod tls;
pub mod transfer;

#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("certificate error: {0}")]
    Certificate(#[from] rcgen::Error),
    #[error("tls configuration error: {0}")]
    TlsConfig(String),
    #[error("quic connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("quic connect error: {0}")]
    Connect(#[from] quinn::ConnectError),
    #[error("frame error: {0}")]
    Frame(#[from] framing::FrameError),
    #[error("auth error: {0}")]
    Auth(#[from] auth::AuthError),
    #[error("session error: {0}")]
    Session(#[from] session::SessionError),
    #[error("peer registry error: {0}")]
    Peer(#[from] peer::RegistryError),
    #[error("discovery error: {0}")]
    Discovery(#[from] mdns_sd::Error),
    #[error("identity error: {0}")]
    Identity(#[from] lingo_core::identity::IdentityError),
}
