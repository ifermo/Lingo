//! Message envelope shared by the UI and the application service.
//!
//! UI/service messages use `Envelope`; authenticated peer control streams use
//! the separate `PeerMessage` enum. Both use the same tagged JSON shape.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::{
    AppState, Avatar, Device, DeviceId, FileState, LinkSet, ManifestEntry, Micros,
    PrivateRemoteState, Staging, Transfer,
};

pub const MAX_LINE_BYTES: usize = 1024 * 1024;
pub const OFFER_CHUNK_ENTRIES: usize = 256;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Empty {}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagingAdd {
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagingClear {
    #[serde(default)]
    pub delete_files: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagingNote {
    pub note: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientSend {
    pub dst: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetAvatar {
    pub avatar: Avatar,
    #[serde(default)]
    pub syn: Micros,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetPushToken {
    pub token: String,
    #[serde(default)]
    pub syn: Micros,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinkAccept {
    pub code: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferAccept {
    pub id: i64,
    pub dir: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferRef {
    pub id: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferRemove {
    pub id: i64,
    #[serde(default)]
    pub delete_files: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoveDevice {
    pub id: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiBadRequest {
    pub msg: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalBadDevice {
    pub version: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Nudge {
    pub did: DeviceId,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Relay {
    pub url: String,
    #[serde(default)]
    pub ttl_secs: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetApiState {
    #[serde(default)]
    pub relays: Vec<Relay>,
    #[serde(default)]
    pub token: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetRemoteStatus {
    #[serde(default)]
    pub online: bool,
    #[serde(default)]
    pub has_quota: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Chat {
    pub text: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteAdd {
    pub did: DeviceId,
    #[serde(default)]
    pub addrs: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferTid {
    pub tid: Uuid,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferOffer {
    pub tid: Uuid,
    #[serde(default)]
    pub note: String,
    #[serde(default)]
    pub syn: Micros,
    pub entry_count: u32,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferOfferChunk {
    pub tid: Uuid,
    #[serde(default)]
    pub entries: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferDecline {
    pub tid: Uuid,
    #[serde(default)]
    pub reason: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferControlAction {
    Pause,
    Resume,
    Cancel,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferControl {
    pub tid: Uuid,
    pub action: TransferControlAction,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferProgress {
    pub tid: Uuid,
    #[serde(default)]
    pub bytes_done: u64,
    #[serde(default)]
    pub total_bytes: u64,
    #[serde(default)]
    pub speed: u64,
    #[serde(default)]
    pub duration: Micros,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferFailed {
    pub tid: Uuid,
    #[serde(default)]
    pub err: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileProgress {
    pub path: String,
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub state: FileState,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferResumeState {
    pub tid: Uuid,
    #[serde(default)]
    pub files: Vec<FileProgress>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileStart {
    pub tid: Uuid,
    pub path: String,
    pub size: u64,
    #[serde(default)]
    pub offset: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileAccept {
    pub offset: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileDone {
    pub ok: bool,
    #[serde(default)]
    pub err: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileError {
    pub err: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum FileStreamMessage {
    FileStart(FileStart),
    FileAccept(FileAccept),
    FileDone(FileDone),
    FileError(FileError),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferProgressEvent {
    pub tid: Uuid,
    pub path: String,
    pub offset: u64,
    #[serde(default)]
    pub speed: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferFileEvent {
    pub tid: Uuid,
    pub path: String,
    pub offset: u64,
    #[serde(default)]
    pub sha256: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransferFileFailure {
    pub tid: Uuid,
    pub path: String,
    pub err: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum TransferEvent {
    Progress(TransferProgressEvent),
    FileComplete(TransferFileEvent),
    FileFailed(TransferFileFailure),
    TransferComplete(TransferTid),
    TransferPaused(TransferTid),
    TransferCanceled(TransferTid),
    TransferFailed(TransferFailed),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum Envelope {
    // Server/agent -> UI
    AppState(AppState),
    StagingSet(Staging),
    SetPeer(Device),
    TransferSet(Transfer),
    LinkSet(LinkSet),
    ApiBadRequest(ApiBadRequest),
    LocalBadDevice(LocalBadDevice),
    NudgeTx(Nudge),
    NudgeRx(Nudge),
    ClientExit(Empty),

    // UI -> agent
    StagingAdd(StagingAdd),
    StagingClear(StagingClear),
    StagingNote(StagingNote),
    ClientSend(ClientSend),
    SetAvatar(SetAvatar),
    SetGuestMode(Empty),
    SetSetupAvailable(Empty),
    SetPushToken(SetPushToken),
    LinkAccept(LinkAccept),
    TransferAccept(TransferAccept),
    TransferPause(TransferRef),
    TransferResume(TransferRef),
    TransferCancel(TransferRef),
    TransferRemove(TransferRemove),
    RemoveDevice(RemoveDevice),
    AppLogout(Empty),
    ZeroconfClear(Empty),
    ClientNetChange(Empty),

    // Sync service
    SetApiState(SetApiState),
    SetPrivateRemoteState(PrivateRemoteState),
}

/// Messages exchanged on an authenticated peer control stream.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum PeerMessage {
    Ack(Empty),
    PeerOk(Empty),
    Chat(Chat),
    SetRemoteStatus(SetRemoteStatus),
    RemoteAdd(RemoteAdd),
    TransferOffer(TransferOffer),
    TransferOfferChunk(TransferOfferChunk),
    TransferOfferDone(TransferTid),
    TransferAccept(TransferTid),
    TransferDecline(TransferDecline),
    TransferControl(TransferControl),
    TransferProgress(TransferProgress),
    TransferComplete(TransferTid),
    TransferFailed(TransferFailed),
    TransferResumeQuery(TransferTid),
    TransferResumeState(TransferResumeState),
}

impl PeerMessage {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Ack(_) => "ack",
            Self::PeerOk(_) => "peer_ok",
            Self::Chat(_) => "chat",
            Self::SetRemoteStatus(_) => "set_remote_status",
            Self::RemoteAdd(_) => "remote_add",
            Self::TransferOffer(_) => "transfer_offer",
            Self::TransferOfferChunk(_) => "transfer_offer_chunk",
            Self::TransferOfferDone(_) => "transfer_offer_done",
            Self::TransferAccept(_) => "transfer_accept",
            Self::TransferDecline(_) => "transfer_decline",
            Self::TransferControl(_) => "transfer_control",
            Self::TransferProgress(_) => "transfer_progress",
            Self::TransferComplete(_) => "transfer_complete",
            Self::TransferFailed(_) => "transfer_failed",
            Self::TransferResumeQuery(_) => "transfer_resume_query",
            Self::TransferResumeState(_) => "transfer_resume_state",
        }
    }
}

impl Envelope {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::AppState(_) => "app_state",
            Self::StagingSet(_) => "staging_set",
            Self::SetPeer(_) => "set_peer",
            Self::TransferSet(_) => "transfer_set",
            Self::LinkSet(_) => "link_set",
            Self::ApiBadRequest(_) => "api_bad_request",
            Self::LocalBadDevice(_) => "local_bad_device",
            Self::NudgeTx(_) => "nudge_tx",
            Self::NudgeRx(_) => "nudge_rx",
            Self::ClientExit(_) => "client_exit",
            Self::StagingAdd(_) => "staging_add",
            Self::StagingClear(_) => "staging_clear",
            Self::StagingNote(_) => "staging_note",
            Self::ClientSend(_) => "client_send",
            Self::SetAvatar(_) => "set_avatar",
            Self::SetGuestMode(_) => "set_guest_mode",
            Self::SetSetupAvailable(_) => "set_setup_available",
            Self::SetPushToken(_) => "set_push_token",
            Self::LinkAccept(_) => "link_accept",
            Self::TransferAccept(_) => "transfer_accept",
            Self::TransferPause(_) => "transfer_pause",
            Self::TransferResume(_) => "transfer_resume",
            Self::TransferCancel(_) => "transfer_cancel",
            Self::TransferRemove(_) => "transfer_remove",
            Self::RemoveDevice(_) => "remove_device",
            Self::AppLogout(_) => "app_logout",
            Self::ZeroconfClear(_) => "zeroconf_clear",
            Self::ClientNetChange(_) => "client_net_change",
            Self::SetApiState(_) => "set_api_state",
            Self::SetPrivateRemoteState(_) => "set_private_remote_state",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("message too large: {0} bytes")]
    TooLarge(usize),
    #[error("invalid json: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn encode_line(envelope: &Envelope) -> Result<String, CodecError> {
    let line = serde_json::to_string(envelope)?;
    if line.len() > MAX_LINE_BYTES {
        return Err(CodecError::TooLarge(line.len()));
    }
    Ok(line)
}

pub fn decode_line(line: &str) -> Result<Envelope, CodecError> {
    let line = line.trim();
    if line.len() > MAX_LINE_BYTES {
        return Err(CodecError::TooLarge(line.len()));
    }
    Ok(serde_json::from_str(line)?)
}

pub fn encode_peer_message(message: &PeerMessage) -> Result<Vec<u8>, CodecError> {
    let bytes = serde_json::to_vec(message)?;
    if bytes.len() > MAX_LINE_BYTES {
        return Err(CodecError::TooLarge(bytes.len()));
    }
    Ok(bytes)
}

pub fn decode_peer_message(payload: &[u8]) -> Result<PeerMessage, CodecError> {
    if payload.len() > MAX_LINE_BYTES {
        return Err(CodecError::TooLarge(payload.len()));
    }
    Ok(serde_json::from_slice(payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Avatar, DeviceId};

    #[test]
    fn envelope_round_trip() {
        let envelope = Envelope::SetAvatar(SetAvatar {
            avatar: Avatar {
                owner: "Ken".into(),
                name: "Macbook".into(),
                variant: "macbook".into(),
                color: "green".into(),
            },
            syn: 42,
        });
        let line = encode_line(&envelope).unwrap();
        assert!(line.contains("\"type\":\"set_avatar\""));
        let decoded = decode_line(&line).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn app_state_round_trip() {
        let envelope = Envelope::AppState(AppState {
            did: DeviceId::from_public_key(&[3u8; 32]),
            setup: true,
            ..AppState::default()
        });
        let line = encode_line(&envelope).unwrap();
        assert_eq!(decode_line(&line).unwrap(), envelope);
    }

    #[test]
    fn rejects_unknown_type() {
        let err = decode_line(r#"{"type":"nope","data":{}}"#).unwrap_err();
        assert!(matches!(err, CodecError::Json(_)));
    }

    #[test]
    fn rejects_oversized_line() {
        let line = "x".repeat(MAX_LINE_BYTES + 1);
        let err = decode_line(&line).unwrap_err();
        assert!(matches!(err, CodecError::TooLarge(_)));
    }

    #[test]
    fn empty_payload_is_an_object() {
        let line = encode_line(&Envelope::SetGuestMode(Empty {})).unwrap();
        assert_eq!(line, r#"{"type":"set_guest_mode","data":{}}"#);
    }

    #[test]
    fn peer_messages_round_trip() {
        let messages = vec![
            PeerMessage::TransferOffer(TransferOffer {
                tid: Uuid::nil(),
                note: "hi".into(),
                syn: 1,
                entry_count: 1,
                total_bytes: 5,
            }),
            PeerMessage::TransferOfferChunk(TransferOfferChunk {
                tid: Uuid::nil(),
                entries: vec![ManifestEntry {
                    path: "a.txt".into(),
                    size: 5,
                }],
            }),
            PeerMessage::TransferOfferDone(TransferTid { tid: Uuid::nil() }),
            PeerMessage::TransferDecline(TransferDecline {
                tid: Uuid::nil(),
                reason: "no".into(),
            }),
            PeerMessage::TransferControl(TransferControl {
                tid: Uuid::nil(),
                action: TransferControlAction::Pause,
            }),
            PeerMessage::TransferProgress(TransferProgress {
                tid: Uuid::nil(),
                bytes_done: 3,
                total_bytes: 5,
                speed: 10,
                duration: 20,
            }),
            PeerMessage::TransferResumeState(TransferResumeState {
                tid: Uuid::nil(),
                files: vec![FileProgress {
                    path: "a.txt".into(),
                    offset: 3,
                    state: FileState::Transferring,
                }],
            }),
        ];

        for message in messages {
            let encoded = encode_peer_message(&message).unwrap();
            assert_eq!(decode_peer_message(&encoded).unwrap(), message);
        }
    }

    #[test]
    fn rejects_oversized_peer_messages() {
        let message = PeerMessage::TransferOfferChunk(TransferOfferChunk {
            tid: Uuid::nil(),
            entries: vec![ManifestEntry {
                path: "x".repeat(MAX_LINE_BYTES),
                size: 0,
            }],
        });
        assert!(matches!(
            encode_peer_message(&message),
            Err(CodecError::TooLarge(_))
        ));
    }
}
