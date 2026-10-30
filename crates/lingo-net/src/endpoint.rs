//! QUIC peer endpoint with application-level authentication.

use std::net::SocketAddr;
use std::sync::Arc;

use lingo_core::identity::Identity;
use quinn::{Connection, Endpoint, EndpointConfig, Incoming};

use crate::{NetError, tls};

pub const DEFAULT_BIND_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0);

pub struct PeerEndpoint {
    endpoint: Endpoint,
}

impl PeerEndpoint {
    pub fn new(identity: &Identity) -> Result<Self, NetError> {
        Self::bind(identity, DEFAULT_BIND_ADDR)
    }

    pub fn bind(_identity: &Identity, addr: SocketAddr) -> Result<Self, NetError> {
        let client = tls::client_config()?;
        let server = tls::server_config()?;
        let socket = std::net::UdpSocket::bind(addr)?;
        let mut endpoint = Endpoint::new(
            EndpointConfig::default(),
            Some(server),
            socket,
            Arc::new(quinn::TokioRuntime),
        )?;
        endpoint.set_default_client_config(client);
        Ok(Self { endpoint })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.endpoint.local_addr().expect("endpoint is bound")
    }

    pub async fn connect(&self, addr: SocketAddr) -> Result<Connection, NetError> {
        let connecting = self.endpoint.connect(addr, "lingo")?;
        Ok(connecting.await?)
    }

    pub async fn accept(&self) -> Option<Incoming> {
        self.endpoint.accept().await
    }

    pub fn close(&self) {
        self.endpoint.close(0u32.into(), b"closing");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Session, SessionConfig, accept, connect};
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connects_and_authenticates_both_peers() {
        let a_dir = tempfile::tempdir().unwrap();
        let b_dir = tempfile::tempdir().unwrap();
        let a_identity = Identity::load_or_create(a_dir.path()).unwrap();
        let b_identity = Identity::load_or_create(b_dir.path()).unwrap();
        let a_did = a_identity.did();
        let b_did = b_identity.did();

        let a = PeerEndpoint::bind(&a_identity, "127.0.0.1:0".parse().unwrap()).unwrap();
        let b = PeerEndpoint::bind(&b_identity, "127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = a.local_addr();

        let server = tokio::spawn(async move {
            let incoming = a.accept().await.expect("incoming connection");
            let connection = incoming.await.unwrap();
            let config = SessionConfig {
                handshake_timeout: Duration::from_secs(2),
                ..SessionConfig::default()
            };
            let mut session: Session = accept(&connection, &a_identity, Some(&b_did), &config)
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
}
