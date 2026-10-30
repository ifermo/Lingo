//! In-memory peer registry.
//!
//! Discovery records are display data; a session is only bound to a peer after
//! authentication. Keeping both in one place lets the app resolve a peer to its
//! current address and detect duplicate sessions.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use lingo_core::model::{Avatar, DeviceId};

use crate::discovery::PeerService;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerRecord {
    pub did: DeviceId,
    pub avatar: Avatar,
    pub addrs: Vec<SocketAddr>,
    pub online: bool,
    pub session: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("authenticated device {actual} does not match expected {expected}")]
    DidMismatch {
        expected: DeviceId,
        actual: DeviceId,
    },
}

#[derive(Debug, Default)]
pub struct PeerRegistry {
    peers: BTreeMap<DeviceId, PeerRecord>,
}

impl PeerRegistry {
    pub fn upsert(&mut self, peer: PeerService) -> &PeerRecord {
        let record = self
            .peers
            .entry(peer.did.clone())
            .or_insert_with(|| PeerRecord {
                did: peer.did.clone(),
                avatar: peer.avatar.clone(),
                addrs: Vec::new(),
                online: true,
                session: None,
            });
        record.avatar = peer.avatar;
        record.addrs = peer.addrs;
        record.online = true;
        record
    }

    pub fn mark_offline(&mut self, did: &DeviceId) -> Option<&PeerRecord> {
        let record = self.peers.get_mut(did)?;
        record.online = false;
        Some(record)
    }

    /// Bind an authenticated session to the expected DID.
    ///
    /// Returns the previous session id when it differs, so the caller can close
    /// the superseded connection.
    pub fn bind_session(
        &mut self,
        expected: &DeviceId,
        authenticated: &DeviceId,
        session: u64,
    ) -> Result<Option<u64>, RegistryError> {
        if expected != authenticated {
            return Err(RegistryError::DidMismatch {
                expected: expected.clone(),
                actual: authenticated.clone(),
            });
        }

        let record = self
            .peers
            .entry(authenticated.clone())
            .or_insert_with(|| PeerRecord {
                did: authenticated.clone(),
                avatar: Avatar::default(),
                addrs: Vec::new(),
                online: true,
                session: None,
            });
        record.online = true;
        Ok(record
            .session
            .replace(session)
            .filter(|old| *old != session))
    }

    pub fn end_session(&mut self, did: &DeviceId, session: u64) -> bool {
        let Some(record) = self.peers.get_mut(did) else {
            return false;
        };
        if record.session == Some(session) {
            record.session = None;
            true
        } else {
            false
        }
    }

    pub fn get(&self, did: &DeviceId) -> Option<&PeerRecord> {
        self.peers.get(did)
    }

    pub fn peers(&self) -> impl Iterator<Item = &PeerRecord> {
        self.peers.values()
    }

    pub fn online(&self) -> impl Iterator<Item = &PeerRecord> {
        self.peers.values().filter(|peer| peer.online)
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(seed: u8) -> PeerService {
        PeerService {
            did: DeviceId::from_public_key(&[seed; 32]),
            avatar: Avatar {
                owner: "Ken".into(),
                name: "Studio".into(),
                variant: "macbook".into(),
                color: "green".into(),
            },
            port: 4321,
            addrs: vec!["127.0.0.1:4321".parse().unwrap()],
            fullname: "Studio._lingo._tcp.local.".into(),
        }
    }

    #[test]
    fn upserts_discovered_peers() {
        let mut registry = PeerRegistry::default();
        let discovered = peer(1);
        let did = discovered.did.clone();
        registry.upsert(discovered);

        let record = registry.get(&did).unwrap();
        assert!(record.online);
        assert_eq!(record.avatar.name, "Studio");

        registry.mark_offline(&did);
        assert!(!registry.get(&did).unwrap().online);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn rejects_a_did_mismatch() {
        let mut registry = PeerRegistry::default();
        let expected = DeviceId::from_public_key(&[1u8; 32]);
        let actual = DeviceId::from_public_key(&[2u8; 32]);
        assert!(matches!(
            registry.bind_session(&expected, &actual, 1),
            Err(RegistryError::DidMismatch { .. })
        ));
    }

    #[test]
    fn replaces_an_existing_session() {
        let mut registry = PeerRegistry::default();
        let did = DeviceId::from_public_key(&[1u8; 32]);
        assert_eq!(registry.bind_session(&did, &did, 1).unwrap(), None);
        assert_eq!(registry.bind_session(&did, &did, 2).unwrap(), Some(1));
        assert_eq!(registry.get(&did).unwrap().session, Some(2));
        assert!(!registry.end_session(&did, 1));
        assert!(registry.end_session(&did, 2));
        assert_eq!(registry.get(&did).unwrap().session, None);
    }
}
