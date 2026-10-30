//! Authenticated peer sessions over QUIC.
//!
//! A session begins with a mutual challenge-response handshake. Both proofs
//! cover the connection's exported keying material, so a proof recorded on one
//! connection cannot be reused on another. After the handshake the session owns
//! one bidirectional control stream for length-prefixed messages.

use std::time::Duration;

use quinn::{Connection, RecvStream, SendStream};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;

use lingo_core::identity::Identity;
use lingo_core::model::DeviceId;

use crate::auth::{self, AuthContext, AuthError, AuthProof};
use crate::framing::{FrameError, read_frame_limited, write_frame_limited};

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_HANDSHAKE_FRAME: usize = 64 * 1024;
pub const MAX_CONTROL_FRAME: usize = 1024 * 1024;
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

const CLOSE_AUTH_FAILED: u32 = 1;
const CLOSE_PROTOCOL: u32 = 2;
const CLOSE_TIMEOUT: u32 = 3;

#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub handshake_timeout: Duration,
    pub control_frame_limit: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: HANDSHAKE_TIMEOUT,
            control_frame_limit: MAX_CONTROL_FRAME,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame error: {0}")]
    Frame(#[from] FrameError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("auth error: {0}")]
    Auth(#[from] AuthError),
    #[error("quic connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("handshake timed out")]
    Timeout,
    #[error("unsupported protocol version: {0}")]
    Version(u16),
    #[error("unexpected handshake message")]
    UnexpectedMessage,
    #[error("expected peer {expected}, got {actual}")]
    DidMismatch {
        expected: DeviceId,
        actual: DeviceId,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
enum HandshakeMessage {
    ClientHello(ClientHello),
    ServerHello(ServerHello),
    ClientAuth(ClientAuth),
    SessionReady(SessionReady),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ClientHello {
    version: u16,
    did: DeviceId,
    nonce: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ServerHello {
    version: u16,
    did: DeviceId,
    nonce: String,
    proof: AuthProof,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ClientAuth {
    proof: AuthProof,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct SessionReady {
    version: u16,
}

#[derive(Debug)]
pub struct Session {
    connection: Connection,
    send: SendStream,
    recv: RecvStream,
    peer_did: DeviceId,
    local_did: DeviceId,
    control_frame_limit: usize,
}

impl Session {
    pub fn peer_did(&self) -> &DeviceId {
        &self.peer_did
    }

    pub fn local_did(&self) -> &DeviceId {
        &self.local_did
    }

    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    pub async fn send(&mut self, payload: &[u8]) -> Result<(), SessionError> {
        write_frame_limited(&mut self.send, payload, self.control_frame_limit).await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>, SessionError> {
        Ok(read_frame_limited(&mut self.recv, self.control_frame_limit).await?)
    }

    pub async fn send_json<T>(&mut self, value: &T) -> Result<(), SessionError>
    where
        T: Serialize,
    {
        let payload = serde_json::to_vec(value)?;
        self.send(&payload).await
    }

    pub async fn recv_json<T>(&mut self) -> Result<T, SessionError>
    where
        T: DeserializeOwned,
    {
        let payload = self.recv().await?;
        Ok(serde_json::from_slice(&payload)?)
    }

    pub fn close(&self, code: u32, reason: &[u8]) {
        self.connection.close(code.into(), reason);
    }
}

pub async fn connect(
    connection: &Connection,
    identity: &Identity,
    expected_peer: &DeviceId,
    config: &SessionConfig,
) -> Result<Session, SessionError> {
    let result = timeout(
        config.handshake_timeout,
        client_handshake(connection, identity, expected_peer, config),
    )
    .await;
    finish_handshake(connection, result)
}

pub async fn accept(
    connection: &Connection,
    identity: &Identity,
    expected_peer: Option<&DeviceId>,
    config: &SessionConfig,
) -> Result<Session, SessionError> {
    let result = timeout(
        config.handshake_timeout,
        server_handshake(connection, identity, expected_peer, config),
    )
    .await;
    finish_handshake(connection, result)
}

async fn client_handshake(
    connection: &Connection,
    identity: &Identity,
    expected_peer: &DeviceId,
    config: &SessionConfig,
) -> Result<Session, SessionError> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let exporter = connection_exporter(connection)?;
    let client_nonce = auth::new_nonce();

    write_json(
        &mut send,
        &HandshakeMessage::ClientHello(ClientHello {
            version: PROTOCOL_VERSION,
            did: identity.did(),
            nonce: auth::encode_nonce(&client_nonce),
        }),
    )
    .await?;

    let response: HandshakeMessage = read_json(&mut recv).await?;
    let HandshakeMessage::ServerHello(server) = response else {
        return Err(SessionError::UnexpectedMessage);
    };
    if server.version != PROTOCOL_VERSION {
        return Err(SessionError::Version(server.version));
    }
    if server.did != *expected_peer {
        return Err(SessionError::DidMismatch {
            expected: expected_peer.clone(),
            actual: server.did.clone(),
        });
    }

    let context = AuthContext {
        client_did: identity.did(),
        server_did: server.did.clone(),
        client_nonce,
        server_nonce: auth::decode_nonce(&server.nonce)?,
        exporter,
    };
    auth::verify_proof(&server.proof, &context, &server.did)?;

    write_json(
        &mut send,
        &HandshakeMessage::ClientAuth(ClientAuth {
            proof: auth::create_proof(identity, &context),
        }),
    )
    .await?;

    let ready: HandshakeMessage = read_json(&mut recv).await?;
    let HandshakeMessage::SessionReady(ready) = ready else {
        return Err(SessionError::UnexpectedMessage);
    };
    if ready.version != PROTOCOL_VERSION {
        return Err(SessionError::Version(ready.version));
    }

    Ok(Session {
        connection: connection.clone(),
        send,
        recv,
        peer_did: server.did,
        local_did: identity.did(),
        control_frame_limit: config.control_frame_limit,
    })
}

async fn server_handshake(
    connection: &Connection,
    identity: &Identity,
    expected_peer: Option<&DeviceId>,
    config: &SessionConfig,
) -> Result<Session, SessionError> {
    let (mut send, mut recv) = connection.accept_bi().await?;
    let hello: HandshakeMessage = read_json(&mut recv).await?;
    let HandshakeMessage::ClientHello(client) = hello else {
        return Err(SessionError::UnexpectedMessage);
    };
    if client.version != PROTOCOL_VERSION {
        return Err(SessionError::Version(client.version));
    }
    if let Some(expected) = expected_peer
        && client.did != *expected
    {
        return Err(SessionError::DidMismatch {
            expected: expected.clone(),
            actual: client.did.clone(),
        });
    }

    let exporter = connection_exporter(connection)?;
    let server_nonce = auth::new_nonce();
    let context = AuthContext {
        client_did: client.did.clone(),
        server_did: identity.did(),
        client_nonce: auth::decode_nonce(&client.nonce)?,
        server_nonce,
        exporter,
    };

    write_json(
        &mut send,
        &HandshakeMessage::ServerHello(ServerHello {
            version: PROTOCOL_VERSION,
            did: identity.did(),
            nonce: auth::encode_nonce(&server_nonce),
            proof: auth::create_proof(identity, &context),
        }),
    )
    .await?;

    let response: HandshakeMessage = read_json(&mut recv).await?;
    let HandshakeMessage::ClientAuth(auth_message) = response else {
        return Err(SessionError::UnexpectedMessage);
    };
    auth::verify_proof(&auth_message.proof, &context, &client.did)?;

    write_json(
        &mut send,
        &HandshakeMessage::SessionReady(SessionReady {
            version: PROTOCOL_VERSION,
        }),
    )
    .await?;

    Ok(Session {
        connection: connection.clone(),
        send,
        recv,
        peer_did: client.did,
        local_did: identity.did(),
        control_frame_limit: config.control_frame_limit,
    })
}

fn finish_handshake(
    connection: &Connection,
    result: Result<Result<Session, SessionError>, tokio::time::error::Elapsed>,
) -> Result<Session, SessionError> {
    match result {
        Ok(Ok(session)) => Ok(session),
        Ok(Err(error)) => {
            close_failed_handshake(connection, &error);
            Err(error)
        }
        Err(_) => {
            connection.close(CLOSE_TIMEOUT.into(), b"handshake timed out");
            Err(SessionError::Timeout)
        }
    }
}

fn close_failed_handshake(connection: &Connection, error: &SessionError) {
    let code = match error {
        SessionError::Auth(_) | SessionError::DidMismatch { .. } => CLOSE_AUTH_FAILED,
        SessionError::Timeout => CLOSE_TIMEOUT,
        _ => CLOSE_PROTOCOL,
    };
    connection.close(code.into(), b"handshake failed");
}

fn connection_exporter(connection: &Connection) -> Result<[u8; auth::EXPORTER_LEN], AuthError> {
    let mut exporter = [0u8; auth::EXPORTER_LEN];
    connection
        .export_keying_material(&mut exporter, auth::AUTH_LABEL, b"")
        .map_err(|_| AuthError::Exporter)?;
    Ok(exporter)
}

async fn write_json<W, T>(writer: &mut W, message: &T) -> Result<(), SessionError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let payload = serde_json::to_vec(message)?;
    write_frame_limited(writer, &payload, MAX_HANDSHAKE_FRAME).await?;
    Ok(())
}

async fn read_json<R, T>(reader: &mut R) -> Result<T, SessionError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let payload = read_frame_limited(reader, MAX_HANDSHAKE_FRAME).await?;
    Ok(serde_json::from_slice(&payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::PeerEndpoint;
    use crate::framing::{FrameError, write_frame};

    async fn pair() -> (
        Identity,
        Identity,
        tempfile::TempDir,
        tempfile::TempDir,
        PeerEndpoint,
        PeerEndpoint,
    ) {
        let a_dir = tempfile::tempdir().unwrap();
        let b_dir = tempfile::tempdir().unwrap();
        let a_identity = Identity::load_or_create(a_dir.path()).unwrap();
        let b_identity = Identity::load_or_create(b_dir.path()).unwrap();
        let a = PeerEndpoint::bind(&a_identity, "127.0.0.1:0".parse().unwrap()).unwrap();
        let b = PeerEndpoint::bind(&b_identity, "127.0.0.1:0".parse().unwrap()).unwrap();
        (a_identity, b_identity, a_dir, b_dir, a, b)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handshake_and_control_frames() {
        let (a_identity, b_identity, _a_dir, _b_dir, a, b) = pair().await;
        let a_did = a_identity.did();
        let b_did = b_identity.did();
        let addr = a.local_addr();

        let server = tokio::spawn(async move {
            let incoming = a.accept().await.unwrap();
            let connection = incoming.await.unwrap();
            let config = SessionConfig {
                handshake_timeout: Duration::from_secs(2),
                ..SessionConfig::default()
            };
            let mut session = accept(&connection, &a_identity, Some(&b_did), &config)
                .await
                .unwrap();
            assert_eq!(session.peer_did(), &b_did);
            assert_eq!(session.recv().await.unwrap(), b"ping");
            session.send(b"pong").await.unwrap();
            session.connection().closed().await;
            a.close();
        });

        let connection = b.connect(addr).await.unwrap();
        let config = SessionConfig {
            handshake_timeout: Duration::from_secs(2),
            ..SessionConfig::default()
        };
        let mut session = connect(&connection, &b_identity, &a_did, &config)
            .await
            .unwrap();
        assert_eq!(session.peer_did(), &a_did);
        session.send(b"ping").await.unwrap();
        assert_eq!(session.recv().await.unwrap(), b"pong");
        session.close(0, b"done");
        b.close();
        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejects_an_unexpected_peer() {
        let (a_identity, b_identity, _a_dir, _b_dir, a, b) = pair().await;
        let a_did = a_identity.did();
        let expected = a_did.clone();
        let addr = a.local_addr();

        let server = tokio::spawn(async move {
            let incoming = a.accept().await.unwrap();
            let connection = incoming.await.unwrap();
            let config = SessionConfig {
                handshake_timeout: Duration::from_millis(500),
                ..SessionConfig::default()
            };
            let result = accept(&connection, &a_identity, Some(&expected), &config).await;
            a.close();
            result
        });

        let connection = b.connect(addr).await.unwrap();
        let config = SessionConfig {
            handshake_timeout: Duration::from_millis(500),
            ..SessionConfig::default()
        };
        let _ = connect(&connection, &b_identity, &a_did, &config).await;
        b.close();

        let error = server.await.unwrap().unwrap_err();
        assert!(matches!(error, SessionError::DidMismatch { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejects_control_before_handshake() {
        let (a_identity, _b_identity, _a_dir, _b_dir, a, b) = pair().await;
        let addr = a.local_addr();

        let server = tokio::spawn(async move {
            let incoming = a.accept().await.unwrap();
            let connection = incoming.await.unwrap();
            let config = SessionConfig {
                handshake_timeout: Duration::from_millis(500),
                ..SessionConfig::default()
            };
            let result = accept(&connection, &a_identity, None, &config).await;
            a.close();
            result
        });

        let connection = b.connect(addr).await.unwrap();
        let (mut send, _recv) = connection.open_bi().await.unwrap();
        write_frame(&mut send, b"not a handshake").await.unwrap();
        send.finish().unwrap();

        let error = server.await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            SessionError::Json(_) | SessionError::UnexpectedMessage
        ));
        b.close();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejects_oversized_handshake_frames() {
        let (a_identity, _b_identity, _a_dir, _b_dir, a, b) = pair().await;
        let addr = a.local_addr();

        let server = tokio::spawn(async move {
            let incoming = a.accept().await.unwrap();
            let connection = incoming.await.unwrap();
            let config = SessionConfig {
                handshake_timeout: Duration::from_millis(500),
                ..SessionConfig::default()
            };
            let result = accept(&connection, &a_identity, None, &config).await;
            a.close();
            result
        });

        let connection = b.connect(addr).await.unwrap();
        let (mut send, _recv) = connection.open_bi().await.unwrap();
        write_frame(&mut send, &vec![b'x'; MAX_HANDSHAKE_FRAME + 1])
            .await
            .unwrap();
        send.finish().unwrap();

        let error = server.await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            SessionError::Frame(FrameError::TooLarge(_))
        ));
        b.close();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn times_out_a_silent_peer() {
        let (a_identity, b_identity, _a_dir, _b_dir, a, b) = pair().await;
        let a_did = a_identity.did();
        let addr = a.local_addr();

        let server = tokio::spawn(async move {
            let incoming = a.accept().await.unwrap();
            let connection = incoming.await.unwrap();
            // Hold the connection open without accepting the control stream.
            tokio::time::sleep(Duration::from_secs(1)).await;
            connection.close(0u32.into(), b"done");
            a.close();
        });

        let connection = b.connect(addr).await.unwrap();
        let config = SessionConfig {
            handshake_timeout: Duration::from_millis(200),
            ..SessionConfig::default()
        };
        let error = connect(&connection, &b_identity, &a_did, &config)
            .await
            .unwrap_err();
        assert!(matches!(error, SessionError::Timeout));
        b.close();
        server.await.unwrap();
    }
}
