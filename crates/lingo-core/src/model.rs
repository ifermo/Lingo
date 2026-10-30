//! Domain models shared by the UI, the agent and the sync service.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Micros = i64;
pub type PathMap = BTreeMap<String, String>;
pub type Manifest = Vec<ManifestEntry>;

#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    #[error("invalid device id: {0}")]
    InvalidDeviceId(String),
    #[error("invalid role: {0}")]
    InvalidRole(String),
    #[error("invalid transfer status: {0}")]
    InvalidStatus(String),
    #[error("invalid file state: {0}")]
    InvalidFileState(String),
}

pub fn now_micros() -> Micros {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or_default()
}

/// A device identity is the unpadded base64url encoding of an Ed25519 public key.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeviceId(String);

impl DeviceId {
    pub fn from_public_key(bytes: &[u8; 32]) -> Self {
        Self(URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into();
        let bytes = URL_SAFE_NO_PAD
            .decode(value.as_bytes())
            .map_err(|_| ModelError::InvalidDeviceId(value.clone()))?;
        if bytes.len() != 32 {
            return Err(ModelError::InvalidDeviceId(value));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn to_bytes(&self) -> Result<[u8; 32], ModelError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(self.0.as_bytes())
            .map_err(|_| ModelError::InvalidDeviceId(self.0.clone()))?;
        bytes
            .try_into()
            .map_err(|_| ModelError::InvalidDeviceId(self.0.clone()))
    }
}

impl std::fmt::Display for DeviceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Avatar {
    pub owner: String,
    pub name: String,
    pub variant: String,
    pub color: String,
}

impl Avatar {
    pub fn is_complete(&self) -> bool {
        !self.owner.trim().is_empty() && !self.name.trim().is_empty()
    }

    pub fn initials(&self) -> String {
        let owner = self.owner.trim();
        let name = self.name.trim();
        let mut out = String::new();
        if let Some(c) = owner.chars().next() {
            out.extend(c.to_uppercase());
        }
        if let Some(c) = name.chars().next() {
            out.extend(c.to_uppercase());
        }
        if out.is_empty() { "?".to_string() } else { out }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Device {
    #[serde(default)]
    pub id: i64,
    pub did: DeviceId,
    #[serde(default)]
    pub avatar: Avatar,
    #[serde(default)]
    pub is_online: bool,
    #[serde(default)]
    pub lan_assist: bool,
    #[serde(default)]
    pub has_quota: bool,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub active_at: Micros,
    #[serde(default)]
    pub last_seen_at: Micros,
    #[serde(default)]
    pub secret: String,
    #[serde(default)]
    pub dialing: bool,
    #[serde(default)]
    pub local_addrs: Vec<String>,
}

impl Device {
    pub fn title(&self) -> String {
        let owner = self.avatar.owner.trim();
        let name = self.avatar.name.trim();
        match (owner.is_empty(), name.is_empty()) {
            (true, true) => "Unknown device".to_string(),
            (false, true) => owner.to_string(),
            (true, false) => name.to_string(),
            (false, false) => format!("{owner} {name}"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestEntry {
    pub path: String,
    pub size: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Staging {
    #[serde(default)]
    pub manifest: Manifest,
    #[serde(default)]
    pub path_map: PathMap,
    #[serde(default)]
    pub note: String,
}

impl Staging {
    pub fn total_size(&self) -> u64 {
        self.manifest.iter().map(|entry| entry.size).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.manifest.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    #[default]
    Src,
    Dst,
}

impl FromStr for Role {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "src" => Ok(Self::Src),
            "dst" => Ok(Self::Dst),
            other => Err(ModelError::InvalidRole(other.to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferStatus {
    #[default]
    Paused,
    Pending,
    Ready,
    Complete,
    Canceled,
    Failed,
}

impl FromStr for TransferStatus {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "paused" => Ok(Self::Paused),
            "pending" => Ok(Self::Pending),
            "ready" => Ok(Self::Ready),
            "complete" => Ok(Self::Complete),
            "canceled" => Ok(Self::Canceled),
            "failed" => Ok(Self::Failed),
            other => Err(ModelError::InvalidStatus(other.to_string())),
        }
    }
}

impl TransferStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Canceled | Self::Failed)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileState {
    #[default]
    Pending,
    Transferring,
    Complete,
    Failed,
}

impl FromStr for FileState {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "transferring" => Ok(Self::Transferring),
            "complete" => Ok(Self::Complete),
            "failed" => Ok(Self::Failed),
            other => Err(ModelError::InvalidFileState(other.to_string())),
        }
    }
}

impl FileState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Failed)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Transfer {
    #[serde(default)]
    pub id: i64,
    pub tid: Uuid,
    pub role: Role,
    #[serde(default)]
    pub created_at: Micros,
    #[serde(default)]
    pub status_at: Micros,
    #[serde(default)]
    pub manifest: Manifest,
    #[serde(default)]
    pub note: String,
    #[serde(default)]
    pub removed: bool,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub offset: u64,
    pub peer_id: i64,
    #[serde(default)]
    pub path_map: PathMap,
    #[serde(default)]
    pub dir: Option<String>,
    #[serde(default)]
    pub err: Option<String>,
    #[serde(default)]
    pub status: TransferStatus,
    #[serde(default)]
    pub peer_status: TransferStatus,
    #[serde(default)]
    pub duration: Micros,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed: Option<u64>,
}

impl Transfer {
    pub fn effective_status(&self) -> TransferStatus {
        let statuses = [self.status, self.peer_status];
        if statuses.contains(&TransferStatus::Failed) {
            return TransferStatus::Failed;
        }
        if statuses.contains(&TransferStatus::Canceled) {
            return TransferStatus::Canceled;
        }
        if statuses
            .iter()
            .all(|status| *status == TransferStatus::Complete)
        {
            return TransferStatus::Complete;
        }
        if statuses.contains(&TransferStatus::Ready) {
            return TransferStatus::Ready;
        }
        if statuses.contains(&TransferStatus::Pending) {
            return TransferStatus::Pending;
        }
        TransferStatus::Paused
    }

    pub fn total_size(&self) -> u64 {
        self.manifest.iter().map(|entry| entry.size).sum()
    }

    pub fn progress(&self) -> f32 {
        let total = self.total_size();
        if total == 0 {
            return 0.0;
        }
        (self.offset.min(total) as f64 / total as f64) as f32
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferFile {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub transfer_id: i64,
    pub path: String,
    #[serde(default)]
    pub final_path: Option<String>,
    pub size: u64,
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub state: FileState,
    #[serde(default)]
    pub sha256: Option<String>,
}

impl TransferFile {
    pub fn pending(path: impl Into<String>, size: u64) -> Self {
        Self {
            path: path.into(),
            size,
            ..Self::default()
        }
    }

    pub fn progress(&self) -> f32 {
        if self.size == 0 {
            return if self.state == FileState::Complete {
                1.0
            } else {
                0.0
            };
        }
        (self.offset.min(self.size) as f64 / self.size as f64) as f32
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Account {
    pub email: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrivateRemoteState {
    #[serde(default)]
    pub auth_code: String,
    #[serde(default)]
    pub link_code: String,
    #[serde(default)]
    pub account: Option<Account>,
    #[serde(default)]
    pub has_quota: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppState {
    pub did: DeviceId,
    #[serde(default)]
    pub avatar: Avatar,
    #[serde(default)]
    pub setup: bool,
    #[serde(default)]
    pub setup_account: bool,
    #[serde(default)]
    pub setup_available: bool,
    #[serde(default)]
    pub private_remote_state: Option<PrivateRemoteState>,
    #[serde(default)]
    pub staging: Staging,
    #[serde(default)]
    pub push_token: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub live: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinkSet {
    pub code: String,
    #[serde(default)]
    pub did: Vec<DeviceId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_id_round_trip() {
        let bytes = [7u8; 32];
        let did = DeviceId::from_public_key(&bytes);
        assert_eq!(did.as_str().len(), 43);
        assert_eq!(did.to_bytes().unwrap(), bytes);
        assert_eq!(DeviceId::parse(did.as_str()).unwrap(), did);
        assert!(DeviceId::parse("not-a-key").is_err());
    }

    #[test]
    fn avatar_requires_owner_and_name() {
        let mut avatar = Avatar::default();
        assert!(!avatar.is_complete());
        avatar.owner = "Ken".into();
        assert!(!avatar.is_complete());
        avatar.name = "Macbook".into();
        assert!(avatar.is_complete());
        assert_eq!(avatar.initials(), "KM");
    }

    fn transfer(status: TransferStatus, peer_status: TransferStatus) -> Transfer {
        Transfer {
            tid: Uuid::nil(),
            role: Role::Src,
            peer_id: 1,
            status,
            peer_status,
            ..Transfer::default()
        }
    }

    #[test]
    fn effective_status_prefers_failure_then_cancel() {
        assert_eq!(
            transfer(TransferStatus::Canceled, TransferStatus::Failed).effective_status(),
            TransferStatus::Failed
        );
        assert_eq!(
            transfer(TransferStatus::Complete, TransferStatus::Canceled).effective_status(),
            TransferStatus::Canceled
        );
        assert_eq!(
            transfer(TransferStatus::Ready, TransferStatus::Canceled).effective_status(),
            TransferStatus::Canceled
        );
    }

    #[test]
    fn effective_status_requires_both_sides_complete() {
        assert_eq!(
            transfer(TransferStatus::Complete, TransferStatus::Complete).effective_status(),
            TransferStatus::Complete
        );
        assert_eq!(
            transfer(TransferStatus::Paused, TransferStatus::Complete).effective_status(),
            TransferStatus::Paused
        );
        assert_eq!(
            transfer(TransferStatus::Complete, TransferStatus::Paused).effective_status(),
            TransferStatus::Paused
        );
    }

    #[test]
    fn effective_status_prefers_ready_then_pending() {
        assert_eq!(
            transfer(TransferStatus::Pending, TransferStatus::Ready).effective_status(),
            TransferStatus::Ready
        );
        assert_eq!(
            transfer(TransferStatus::Pending, TransferStatus::Paused).effective_status(),
            TransferStatus::Pending
        );
    }

    #[test]
    fn transfer_status_round_trips_new_states() {
        assert_eq!(
            "pending".parse::<TransferStatus>().unwrap(),
            TransferStatus::Pending
        );
        assert_eq!(
            "failed".parse::<TransferStatus>().unwrap(),
            TransferStatus::Failed
        );
        assert!(TransferStatus::Failed.is_terminal());
        assert!(!TransferStatus::Pending.is_terminal());
    }

    #[test]
    fn file_state_round_trips() {
        assert_eq!("pending".parse::<FileState>().unwrap(), FileState::Pending);
        assert_eq!(
            "transferring".parse::<FileState>().unwrap(),
            FileState::Transferring
        );
        assert_eq!(
            "complete".parse::<FileState>().unwrap(),
            FileState::Complete
        );
        assert_eq!("failed".parse::<FileState>().unwrap(), FileState::Failed);
        assert!("unknown".parse::<FileState>().is_err());
    }

    #[test]
    fn transfer_file_progress_handles_empty_files() {
        let mut file = TransferFile::pending("empty.txt", 0);
        assert_eq!(file.progress(), 0.0);
        file.state = FileState::Complete;
        assert_eq!(file.progress(), 1.0);
    }
}
