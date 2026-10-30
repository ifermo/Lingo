//! mDNS/DNS-SD discovery on the local network.
//!
//! Advertisement and browsing are kept separate: `advertise` publishes the
//! local device, while `stream` yields resolved peers and removals as a typed
//! async stream. TXT identity is treated as display data until a session
//! authenticates the advertised DID.

use std::collections::HashMap;
use std::net::SocketAddr;

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tracing::{debug, warn};

use lingo_core::model::{Avatar, DeviceId, ModelError};

use crate::NetError;

pub const SERVICE_TYPE: &str = "_lingo._tcp.local.";
const MAX_TXT_VALUE_CHARS: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerService {
    pub did: DeviceId,
    pub avatar: Avatar,
    pub port: u16,
    pub addrs: Vec<SocketAddr>,
    pub fullname: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiscoveryEvent {
    Resolved(PeerService),
    Removed { did: DeviceId, fullname: String },
}

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("missing did in TXT record")]
    MissingDid,
    #[error("invalid did: {0}")]
    InvalidDid(#[from] ModelError),
    #[error("service has no usable address")]
    NoAddresses,
    #[error("service has an invalid port")]
    InvalidPort,
}

pub struct Discovery {
    daemon: ServiceDaemon,
    full_name: Option<String>,
}

impl Discovery {
    pub fn new() -> Result<Self, NetError> {
        let daemon = ServiceDaemon::new()?;
        Ok(Self {
            daemon,
            full_name: None,
        })
    }

    pub fn advertise(
        &mut self,
        did: &DeviceId,
        avatar: &Avatar,
        port: u16,
    ) -> Result<(), NetError> {
        self.clear()?;

        let suffix: String = did.as_str().chars().take(8).collect();
        let instance = format!("{}-{suffix}", sanitize(&avatar.name));
        let host = format!("lingo-{instance}.local.");
        let properties = HashMap::from([
            ("did".to_string(), did.as_str().to_string()),
            ("owner".to_string(), txt_value(&avatar.owner)),
            ("name".to_string(), txt_value(&avatar.name)),
            ("variant".to_string(), txt_value(&avatar.variant)),
            ("color".to_string(), txt_value(&avatar.color)),
        ]);

        let info = ServiceInfo::new(SERVICE_TYPE, &instance, &host, "", port, properties)?;
        let full_name = info.get_fullname().to_string();
        self.daemon.register(info)?;
        self.full_name = Some(full_name);
        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), NetError> {
        if let Some(full_name) = self.full_name.take() {
            self.daemon.unregister(&full_name)?;
        }
        Ok(())
    }

    pub fn browse(&self) -> Result<mdns_sd::Receiver<ServiceEvent>, NetError> {
        Ok(self.daemon.browse(SERVICE_TYPE)?)
    }

    pub fn stream(&self) -> Result<DiscoveryStream, NetError> {
        Ok(DiscoveryStream {
            receiver: self.daemon.browse(SERVICE_TYPE)?,
            names: HashMap::new(),
        })
    }

    pub fn shutdown(self) {
        let _ = self.daemon.shutdown();
    }
}

pub struct DiscoveryStream {
    receiver: mdns_sd::Receiver<ServiceEvent>,
    names: HashMap<String, DeviceId>,
}

impl DiscoveryStream {
    pub async fn next(&mut self) -> Option<DiscoveryEvent> {
        loop {
            let event = self.receiver.recv_async().await.ok()?;
            if let Some(event) = handle_event(&mut self.names, event) {
                return Some(event);
            }
        }
    }
}

fn handle_event(
    names: &mut HashMap<String, DeviceId>,
    event: ServiceEvent,
) -> Option<DiscoveryEvent> {
    match event {
        ServiceEvent::ServiceResolved(info) => {
            let fullname = info.get_fullname().to_string();
            match parse_service(&info) {
                Ok(peer) => {
                    names.insert(fullname, peer.did.clone());
                    Some(DiscoveryEvent::Resolved(peer))
                }
                Err(error) => {
                    warn!(fullname, error = %error, "ignoring malformed Lingo service record");
                    None
                }
            }
        }
        ServiceEvent::ServiceRemoved(_service_type, fullname) => {
            let did = names.remove(&fullname)?;
            Some(DiscoveryEvent::Removed { did, fullname })
        }
        other => {
            debug!(event = ?other, "ignoring mDNS discovery event");
            None
        }
    }
}

fn parse_service(info: &ServiceInfo) -> Result<PeerService, DiscoveryError> {
    let raw_did = info
        .get_property_val_str("did")
        .ok_or(DiscoveryError::MissingDid)?;
    let did = DeviceId::parse(raw_did)?;
    let port = info.get_port();
    if port == 0 {
        return Err(DiscoveryError::InvalidPort);
    }

    let mut addrs: Vec<SocketAddr> = info
        .get_addresses()
        .iter()
        .filter(|addr| !addr.is_unspecified())
        .map(|addr| SocketAddr::new(*addr, port))
        .collect();
    addrs.sort_by_key(|addr| (addr.is_ipv6(), addr.ip().to_string()));
    addrs.dedup();
    if addrs.is_empty() {
        return Err(DiscoveryError::NoAddresses);
    }

    let fallback = info
        .get_fullname()
        .split('.')
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or("Unknown device");
    let avatar = Avatar {
        owner: txt(info, "owner").unwrap_or_default(),
        name: txt(info, "name").unwrap_or_else(|| fallback.to_string()),
        variant: txt(info, "variant").unwrap_or_else(|| "macbook".to_string()),
        color: txt(info, "color").unwrap_or_else(|| "green".to_string()),
    };

    Ok(PeerService {
        did,
        avatar,
        port,
        addrs,
        fullname: info.get_fullname().to_string(),
    })
}

fn txt(info: &ServiceInfo, key: &str) -> Option<String> {
    info.get_property_val_str(key)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn txt_value(value: &str) -> String {
    value.trim().chars().take(MAX_TXT_VALUE_CHARS).collect()
}

fn sanitize(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let sanitized = sanitized.trim_matches('-');
    if sanitized.is_empty() {
        "device".to_string()
    } else {
        sanitized.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn sample_info() -> ServiceInfo {
        let did = DeviceId::from_public_key(&[7u8; 32]);
        let properties = HashMap::from([
            ("did".to_string(), did.as_str().to_string()),
            ("owner".to_string(), "Ken".to_string()),
            ("name".to_string(), "Studio".to_string()),
            ("variant".to_string(), "macbook".to_string()),
            ("color".to_string(), "green".to_string()),
        ]);
        ServiceInfo::new(
            SERVICE_TYPE,
            "Studio-abcdefgh",
            "lingo-Studio-abcdefgh.local.",
            "127.0.0.1",
            4321,
            properties,
        )
        .unwrap()
    }

    #[test]
    fn sanitizes_host_names() {
        assert_eq!(sanitize("Ken's Macbook"), "Ken-s-Macbook");
        assert_eq!(sanitize(""), "device");
        assert_eq!(sanitize("---"), "device");
    }

    #[test]
    fn parses_a_service_record() {
        let info = sample_info();
        let peer = parse_service(&info).unwrap();
        assert_eq!(peer.did, DeviceId::from_public_key(&[7u8; 32]));
        assert_eq!(peer.avatar.owner, "Ken");
        assert_eq!(peer.avatar.name, "Studio");
        assert_eq!(peer.port, 4321);
        assert_eq!(peer.addrs, vec!["127.0.0.1:4321".parse().unwrap()]);
    }

    #[test]
    fn missing_did_is_rejected() {
        let properties = HashMap::from([("name".to_string(), "Studio".to_string())]);
        let info = ServiceInfo::new(
            SERVICE_TYPE,
            "Studio",
            "lingo-Studio.local.",
            "127.0.0.1",
            4321,
            properties,
        )
        .unwrap();
        assert!(matches!(
            parse_service(&info),
            Err(DiscoveryError::MissingDid)
        ));
    }

    #[test]
    fn invalid_did_is_rejected() {
        let properties = HashMap::from([("did".to_string(), "not-a-did".to_string())]);
        let info = ServiceInfo::new(
            SERVICE_TYPE,
            "Studio",
            "lingo-Studio.local.",
            "127.0.0.1",
            4321,
            properties,
        )
        .unwrap();
        assert!(matches!(
            parse_service(&info),
            Err(DiscoveryError::InvalidDid(_))
        ));
    }

    #[test]
    fn maps_resolved_and_removed_events() {
        let info = sample_info();
        let fullname = info.get_fullname().to_string();
        let mut names = HashMap::new();

        let resolved = handle_event(&mut names, ServiceEvent::ServiceResolved(info)).unwrap();
        assert!(matches!(
            resolved,
            DiscoveryEvent::Resolved(PeerService { port: 4321, .. })
        ));

        let removed = handle_event(
            &mut names,
            ServiceEvent::ServiceRemoved(SERVICE_TYPE.to_string(), fullname),
        )
        .unwrap();
        assert!(matches!(removed, DiscoveryEvent::Removed { .. }));
    }

    #[test]
    fn ignores_removals_for_unknown_services() {
        let mut names = HashMap::new();
        let removed = handle_event(
            &mut names,
            ServiceEvent::ServiceRemoved(SERVICE_TYPE.to_string(), "missing".to_string()),
        );
        assert!(removed.is_none());
    }
}
