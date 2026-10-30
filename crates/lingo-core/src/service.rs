//! Application service: owns state and applies UI/peer/service messages.
//!
//! Networking is intentionally not wired yet. The service already implements
//! the state machine and persistence so the UI can be exercised end-to-end.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use tracing::{debug, warn};
use uuid::Uuid;

use crate::config::Paths;
use crate::identity::Identity;
use crate::model::{
    AppState, Device, DeviceId, FileState, Manifest, PathMap, Role, Staging, Transfer,
    TransferFile, TransferStatus, now_micros,
};
use crate::msg::{
    ApiBadRequest, ClientSend, Envelope, FileProgress, OFFER_CHUNK_ENTRIES, PeerMessage, SetAvatar,
    SetPushToken, StagingAdd, StagingClear, StagingNote, TransferAccept, TransferControl,
    TransferControlAction, TransferDecline, TransferEvent, TransferFailed, TransferOffer,
    TransferOfferChunk, TransferProgress, TransferRef, TransferRemove, TransferResumeState,
    TransferTid,
};
use crate::paths::{
    MAX_MANIFEST_ENTRIES, MAX_PATH_BYTES, delete_transfer_files, destination_path,
    ensure_within_root, remove_temp_dir, resolve_source, scan_paths, unique_path_avoiding,
    validate_manifest_path,
};
use crate::store::{Prefs, Store, StoreError};

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Identity(#[from] crate::identity::IdentityError),
}

#[derive(Clone, Debug, PartialEq)]
pub struct PeerOutbound {
    pub peer_did: DeviceId,
    pub message: PeerMessage,
}

struct IncomingOffer {
    peer_id: i64,
    note: String,
    entry_count: usize,
    total_bytes: u64,
    path_bytes: usize,
    entries: Manifest,
}

struct FileStateUpdate<'a> {
    tid: Uuid,
    path: &'a str,
    offset: u64,
    state: FileState,
    sha256: Option<String>,
    speed: Option<u64>,
}

const MAX_PENDING_OFFERS: usize = 64;
const MAX_OFFER_PATH_BYTES: usize = 8 * 1024 * 1024;

pub struct Service {
    store: Store,
    identity: Identity,
    state: AppState,
    devices: BTreeMap<i64, Device>,
    transfers: BTreeMap<i64, Transfer>,
    files: BTreeMap<i64, Vec<TransferFile>>,
    incoming_offers: BTreeMap<Uuid, IncomingOffer>,
    peer_outbox: Vec<PeerOutbound>,
}

impl Service {
    pub fn start(paths: &Paths) -> Result<(Self, Vec<Envelope>), ServiceError> {
        paths.ensure()?;
        let identity = Identity::load_or_create(&paths.data_dir)?;
        let store = Store::open(&paths.db)?;

        let prefs = store.load_prefs()?;
        let mut state = AppState {
            did: identity.did(),
            live: true,
            ..AppState::default()
        };

        match prefs {
            Some(prefs) => {
                state.avatar = prefs.avatar;
                state.setup = prefs.setup;
                state.setup_account = prefs.setup_account;
                state.setup_available = prefs.setup_available;
                state.push_token = prefs.push_token;
            }
            None => {
                let prefs = Prefs {
                    did: state.did.clone(),
                    ..Prefs::default()
                };
                store.save_prefs(&prefs)?;
            }
        }

        let devices: BTreeMap<i64, Device> = store
            .list_devices()?
            .into_iter()
            .map(|device| (device.id, device))
            .collect();
        let transfers: BTreeMap<i64, Transfer> = store
            .list_transfers()?
            .into_iter()
            .map(|transfer| (transfer.id, transfer))
            .collect();
        let mut files: BTreeMap<i64, Vec<TransferFile>> = BTreeMap::new();
        for file in store.list_all_transfer_files()? {
            files.entry(file.transfer_id).or_default().push(file);
        }

        let mut service = Self {
            store,
            identity,
            state,
            devices,
            transfers,
            files,
            incoming_offers: BTreeMap::new(),
            peer_outbox: Vec::new(),
        };
        service.recover()?;

        let events = service.initial_events();
        Ok((service, events))
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    pub fn state(&self) -> &AppState {
        &self.state
    }

    pub fn transfer_by_id(&self, id: i64) -> Option<Transfer> {
        self.transfers.get(&id).cloned()
    }

    pub fn device_did(&self, id: i64) -> Option<DeviceId> {
        self.devices.get(&id).map(|device| device.did.clone())
    }

    pub fn ready_transfers(&self) -> Vec<Transfer> {
        self.transfers
            .values()
            .filter(|transfer| transfer.status == TransferStatus::Ready && !transfer.removed)
            .cloned()
            .collect()
    }

    pub fn drain_peer_outbox(&mut self) -> Vec<PeerOutbound> {
        std::mem::take(&mut self.peer_outbox)
    }

    fn initial_events(&self) -> Vec<Envelope> {
        let mut events = vec![
            Envelope::AppState(self.state.clone()),
            Envelope::StagingSet(self.state.staging.clone()),
        ];
        events.extend(self.devices.values().cloned().map(Envelope::SetPeer));
        events.extend(self.transfers.values().cloned().map(Envelope::TransferSet));
        events
    }

    fn recover(&mut self) -> Result<(), ServiceError> {
        let now = now_micros();
        let ids: Vec<i64> = self.transfers.keys().copied().collect();
        let mut changed_transfers = Vec::new();
        let mut changed_files = Vec::new();

        for id in ids {
            let Some(mut transfer) = self.transfers.get(&id).cloned() else {
                continue;
            };

            if transfer.removed {
                self.cleanup_temp(&transfer);
                continue;
            }

            let mut modified = false;

            if transfer.status == TransferStatus::Ready {
                transfer.status = TransferStatus::Paused;
                transfer.peer_status = TransferStatus::Paused;
                transfer.status_at = now;
                modified = true;
            } else if transfer.status == TransferStatus::Paused
                && transfer.peer_status == TransferStatus::Ready
            {
                transfer.peer_status = TransferStatus::Paused;
                modified = true;
            }

            if transfer.role == Role::Dst {
                let mut failure = None;
                if let Some(files) = self.files.get_mut(&id) {
                    let root = transfer.dir.as_deref().map(Path::new);
                    for (index, file) in files.iter_mut().enumerate() {
                        let Some(root) = root else {
                            if transfer.status != TransferStatus::Pending {
                                failure = Some("Destination folder is missing".to_string());
                            }
                            break;
                        };
                        let Some(final_path) = file.final_path.as_deref() else {
                            if transfer.status != TransferStatus::Pending {
                                failure =
                                    Some(format!("Destination path is missing for {}", file.path));
                            }
                            continue;
                        };
                        let final_path = PathBuf::from(final_path);
                        let final_exists = std::fs::symlink_metadata(&final_path).is_ok();
                        let temp_path = crate::paths::temp_file(root, transfer.tid, index);

                        if final_exists {
                            if std::fs::remove_file(&temp_path).is_ok() {
                                debug!(path = %temp_path.display(), "removed leftover temporary file");
                            }
                            if file.state != FileState::Complete || file.offset != file.size {
                                file.state = FileState::Complete;
                                file.offset = file.size;
                                changed_files.push(file.clone());
                                modified = true;
                            }
                        } else if std::fs::symlink_metadata(&temp_path).is_ok() {
                            let length = std::fs::metadata(&temp_path)
                                .map(|metadata| metadata.len())
                                .unwrap_or(0);
                            if length > file.size
                                && let Ok(temp) =
                                    std::fs::OpenOptions::new().write(true).open(&temp_path)
                            {
                                let _ = temp.set_len(0);
                            }
                            let offset = length.min(file.size);
                            let state = if offset == 0 {
                                FileState::Pending
                            } else {
                                FileState::Transferring
                            };
                            if file.offset != offset || file.state != state {
                                file.offset = offset;
                                file.state = state;
                                changed_files.push(file.clone());
                                modified = true;
                            }
                        } else if file.state == FileState::Complete {
                            file.state = FileState::Failed;
                            changed_files.push(file.clone());
                            failure = Some(format!(
                                "Received file is missing: {}",
                                final_path.display()
                            ));
                            modified = true;
                        } else if file.offset != 0 {
                            file.offset = 0;
                            file.state = FileState::Pending;
                            changed_files.push(file.clone());
                            modified = true;
                        }
                    }
                }
                if let Some(reason) = failure {
                    transfer.status = TransferStatus::Failed;
                    transfer.peer_status = TransferStatus::Failed;
                    transfer.err = Some(reason);
                    transfer.status_at = now;
                    modified = true;
                }
                if matches!(
                    transfer.status,
                    TransferStatus::Complete | TransferStatus::Canceled
                ) {
                    self.cleanup_temp(&transfer);
                } else if let Some(root) = transfer.dir.as_deref() {
                    let _ =
                        std::fs::remove_dir(crate::paths::temp_dir(Path::new(root), transfer.tid));
                }
            }

            if transfer.role == Role::Src
                && !transfer.status.is_terminal()
                && let Some(reason) = source_problem(&transfer)
            {
                transfer.status = TransferStatus::Failed;
                transfer.peer_status = TransferStatus::Failed;
                transfer.err = Some(reason);
                transfer.status_at = now;
                modified = true;
            }

            if let Some(files) = self.files.get(&id) {
                let offset = files.iter().map(|file| file.offset).sum();
                if transfer.offset != offset {
                    transfer.offset = offset;
                    modified = true;
                }
            }

            if modified {
                self.transfers.insert(id, transfer.clone());
                changed_transfers.push(transfer);
            }
        }

        for file in changed_files {
            self.store.update_transfer_file(&file)?;
        }
        for transfer in changed_transfers {
            self.store.update_transfer(&transfer)?;
        }
        Ok(())
    }

    pub fn handle(&mut self, envelope: Envelope) -> Result<Vec<Envelope>, ServiceError> {
        debug!(kind = envelope.kind(), "handling message");
        let mut events = Vec::new();

        match envelope {
            Envelope::StagingAdd(msg) => self.handle_staging_add(msg, &mut events),
            Envelope::StagingClear(msg) => self.handle_staging_clear(msg, &mut events),
            Envelope::StagingNote(msg) => self.handle_staging_note(msg, &mut events),
            Envelope::SetAvatar(msg) => self.handle_set_avatar(msg, &mut events),
            Envelope::SetGuestMode(_) => {
                self.state.setup_account = true;
                self.persist();
                events.push(Envelope::AppState(self.state.clone()));
            }
            Envelope::SetSetupAvailable(_) => {
                self.state.setup_available = true;
                self.persist();
                events.push(Envelope::AppState(self.state.clone()));
            }
            Envelope::SetPushToken(msg) => self.handle_set_push_token(msg, &mut events),
            Envelope::ClientSend(msg) => self.handle_client_send(msg, &mut events),
            Envelope::TransferAccept(msg) => self.handle_transfer_accept(msg, &mut events),
            Envelope::TransferPause(msg) => {
                self.set_transfer_status(msg, TransferStatus::Paused, &mut events)
            }
            Envelope::TransferResume(msg) => {
                self.set_transfer_status(msg, TransferStatus::Ready, &mut events)
            }
            Envelope::TransferCancel(msg) => {
                self.set_transfer_status(msg, TransferStatus::Canceled, &mut events)
            }
            Envelope::TransferRemove(msg) => self.handle_transfer_remove(msg, &mut events),
            Envelope::RemoveDevice(msg) => {
                if let Err(err) = self.store.remove_device(msg.id) {
                    warn!(error = %err, "failed to remove device");
                }
                self.devices.remove(&msg.id);
            }
            Envelope::AppLogout(_) => {
                self.state.private_remote_state = None;
                self.state.setup_account = false;
                self.persist();
                events.push(Envelope::AppState(self.state.clone()));
            }
            Envelope::SetPeer(device) => {
                let mut device = device;
                match self.store.upsert_device(&device) {
                    Ok(id) => {
                        device.id = id;
                        self.devices.insert(id, device.clone());
                        events.push(Envelope::SetPeer(device));
                    }
                    Err(err) => warn!(error = %err, "failed to persist device"),
                }
            }
            Envelope::TransferSet(transfer) => self.handle_transfer_set(transfer, &mut events),
            Envelope::LinkSet(link_set) => {
                events.push(Envelope::LinkSet(link_set));
            }
            Envelope::LinkAccept(msg) => {
                events.push(Envelope::ApiBadRequest(ApiBadRequest {
                    msg: format!(
                        "Link code {} could not be resolved. Connect the sync service first.",
                        msg.code
                    ),
                }));
            }
            Envelope::SetPrivateRemoteState(state) => {
                self.state.private_remote_state = Some(state);
                self.persist();
                events.push(Envelope::AppState(self.state.clone()));
            }
            Envelope::AppState(_)
            | Envelope::StagingSet(_)
            | Envelope::ApiBadRequest(_)
            | Envelope::LocalBadDevice(_)
            | Envelope::NudgeTx(_)
            | Envelope::NudgeRx(_)
            | Envelope::ClientExit(_)
            | Envelope::SetApiState(_)
            | Envelope::ZeroconfClear(_)
            | Envelope::ClientNetChange(_) => {
                debug!(
                    kind = envelope.kind(),
                    "message accepted without a local action"
                );
            }
        }

        Ok(events)
    }

    pub fn handle_peer(
        &mut self,
        peer_did: &DeviceId,
        message: PeerMessage,
    ) -> Result<Vec<Envelope>, ServiceError> {
        debug!(peer = %peer_did, kind = message.kind(), "handling peer message");
        let mut events = Vec::new();
        let (peer_id, new_device) = self.ensure_peer(peer_did)?;
        if let Some(device) = new_device {
            events.push(Envelope::SetPeer(device));
        }

        match message {
            PeerMessage::TransferOffer(offer) => self.handle_peer_offer(peer_id, peer_did, offer),
            PeerMessage::TransferOfferChunk(chunk) => self.handle_peer_offer_chunk(peer_did, chunk),
            PeerMessage::TransferOfferDone(done) => {
                self.handle_peer_offer_done(peer_id, peer_did, done, &mut events)?
            }
            PeerMessage::TransferAccept(accept) => {
                self.handle_peer_accept(peer_id, accept, &mut events)
            }
            PeerMessage::TransferDecline(decline) => {
                self.handle_peer_decline(peer_id, decline, &mut events)
            }
            PeerMessage::TransferControl(control) => {
                self.handle_peer_control(peer_id, control, &mut events)
            }
            PeerMessage::TransferProgress(progress) => {
                self.handle_peer_progress(peer_id, progress, &mut events)
            }
            PeerMessage::TransferComplete(done) => {
                self.handle_peer_complete(peer_id, done, &mut events)
            }
            PeerMessage::TransferFailed(failed) => {
                self.handle_peer_failed(peer_id, failed, &mut events)
            }
            PeerMessage::TransferResumeQuery(query) => {
                self.handle_peer_resume_query(peer_id, query)
            }
            PeerMessage::TransferResumeState(state) => {
                self.handle_peer_resume_state(peer_id, state, &mut events)?
            }
            PeerMessage::Ack(_)
            | PeerMessage::PeerOk(_)
            | PeerMessage::Chat(_)
            | PeerMessage::SetRemoteStatus(_)
            | PeerMessage::RemoteAdd(_) => {
                debug!(kind = message.kind(), "peer message has no local action");
            }
        }

        Ok(events)
    }

    pub fn record_progress(
        &mut self,
        tid: Uuid,
        path: &str,
        offset: u64,
        speed: Option<u64>,
    ) -> Result<Vec<Envelope>, ServiceError> {
        let Some(mut transfer) = self.transfer_by_tid(tid) else {
            warn!(%tid, "progress for an unknown transfer");
            return Ok(Vec::new());
        };
        if transfer.status != TransferStatus::Ready {
            return Ok(Vec::new());
        }

        let Some(files) = self.files.get_mut(&transfer.id) else {
            return Ok(Vec::new());
        };
        let Some(index) = files.iter().position(|file| file.path == path) else {
            warn!(%tid, path, "progress for an unknown file");
            return Ok(Vec::new());
        };
        files[index].offset = offset.min(files[index].size);
        if files[index].state == FileState::Pending {
            files[index].state = FileState::Transferring;
        }
        let file = files[index].clone();
        let aggregate = files.iter().map(|file| file.offset).sum::<u64>();

        self.store.update_transfer_file(&file)?;
        transfer.offset = aggregate;
        transfer.speed = speed.or(transfer.speed);
        let mut events = Vec::new();
        self.persist_transfer(transfer, &mut events);
        Ok(events)
    }

    pub fn transfer(&self, tid: Uuid) -> Option<Transfer> {
        self.transfer_by_tid(tid)
    }

    pub fn transfer_files(&self, tid: Uuid) -> Vec<TransferFile> {
        self.transfer_by_tid(tid)
            .and_then(|transfer| self.files.get(&transfer.id).cloned())
            .unwrap_or_default()
    }

    pub fn apply_transfer_event(
        &mut self,
        event: TransferEvent,
    ) -> Result<Vec<Envelope>, ServiceError> {
        let mut events = Vec::new();
        match event {
            TransferEvent::Progress(progress) => {
                self.update_file_state(
                    FileStateUpdate {
                        tid: progress.tid,
                        path: &progress.path,
                        offset: progress.offset,
                        state: FileState::Transferring,
                        sha256: None,
                        speed: Some(progress.speed),
                    },
                    &mut events,
                )?;
            }
            TransferEvent::FileComplete(file) => {
                let sha256 = if file.sha256.is_empty() {
                    None
                } else {
                    Some(file.sha256)
                };
                self.update_file_state(
                    FileStateUpdate {
                        tid: file.tid,
                        path: &file.path,
                        offset: file.offset,
                        state: FileState::Complete,
                        sha256,
                        speed: None,
                    },
                    &mut events,
                )?;
            }
            TransferEvent::FileFailed(file) => {
                self.update_file_state(
                    FileStateUpdate {
                        tid: file.tid,
                        path: &file.path,
                        offset: 0,
                        state: FileState::Failed,
                        sha256: None,
                        speed: None,
                    },
                    &mut events,
                )?;
                if let Some(mut transfer) = self.transfer_by_tid(file.tid)
                    && !transfer.status.is_terminal()
                {
                    transfer.err = Some(file.err);
                    self.persist_transfer(transfer, &mut events);
                }
            }
            TransferEvent::TransferComplete(complete) => {
                if let Some(mut transfer) = self.transfer_by_tid(complete.tid)
                    && !transfer.status.is_terminal()
                {
                    transfer.status = TransferStatus::Complete;
                    transfer.status_at = now_micros();
                    transfer.err = None;
                    self.persist_transfer(transfer, &mut events);
                }
            }
            TransferEvent::TransferPaused(paused) => {
                if let Some(mut transfer) = self.transfer_by_tid(paused.tid)
                    && !transfer.status.is_terminal()
                {
                    transfer.status = TransferStatus::Paused;
                    transfer.peer_status = TransferStatus::Paused;
                    transfer.status_at = now_micros();
                    self.persist_transfer(transfer, &mut events);
                }
            }
            TransferEvent::TransferCanceled(canceled) => {
                if let Some(mut transfer) = self.transfer_by_tid(canceled.tid)
                    && !transfer.status.is_terminal()
                {
                    transfer.status = TransferStatus::Canceled;
                    transfer.peer_status = TransferStatus::Canceled;
                    transfer.status_at = now_micros();
                    self.cleanup_temp(&transfer);
                    self.persist_transfer(transfer, &mut events);
                }
            }
            TransferEvent::TransferFailed(failed) => {
                if let Some(mut transfer) = self.transfer_by_tid(failed.tid)
                    && !transfer.status.is_terminal()
                {
                    transfer.status = TransferStatus::Failed;
                    transfer.peer_status = TransferStatus::Failed;
                    transfer.err = Some(failed.err);
                    transfer.status_at = now_micros();
                    self.persist_transfer(transfer, &mut events);
                }
            }
        }
        Ok(events)
    }

    fn update_file_state(
        &mut self,
        update: FileStateUpdate<'_>,
        events: &mut Vec<Envelope>,
    ) -> Result<(), ServiceError> {
        let Some(mut transfer) = self.transfer_by_tid(update.tid) else {
            warn!(tid = %update.tid, "event for an unknown transfer");
            return Ok(());
        };
        if transfer.status.is_terminal() {
            return Ok(());
        }
        let Some(files) = self.files.get_mut(&transfer.id) else {
            return Ok(());
        };
        let Some(index) = files.iter().position(|file| file.path == update.path) else {
            warn!(tid = %update.tid, path = update.path, "event for an unknown file");
            return Ok(());
        };
        if files[index].state == FileState::Complete && update.state != FileState::Complete {
            return Ok(());
        }
        files[index].offset = update.offset.min(files[index].size);
        files[index].state = update.state;
        if let Some(sha256) = update.sha256 {
            files[index].sha256 = Some(sha256);
        }
        let file = files[index].clone();
        let aggregate = files.iter().map(|file| file.offset).sum::<u64>();

        self.store.update_transfer_file(&file)?;
        transfer.offset = aggregate;
        transfer.speed = update.speed.or(transfer.speed);
        self.persist_transfer(transfer, events);
        Ok(())
    }

    fn cleanup_temp(&self, transfer: &Transfer) {
        if transfer.role != Role::Dst {
            return;
        }
        let Some(dir) = transfer.dir.as_deref() else {
            return;
        };
        if let Err(err) = remove_temp_dir(Path::new(dir), transfer.tid) {
            warn!(error = %err, tid = %transfer.tid, "failed to remove temporary files");
        }
    }

    fn handle_staging_add(&mut self, msg: StagingAdd, events: &mut Vec<Envelope>) {
        let paths: Vec<PathBuf> = msg.paths.iter().map(PathBuf::from).collect();
        match scan_paths(&paths) {
            Ok(staged) => {
                merge_staging(&mut self.state.staging, staged);
                events.push(Envelope::StagingSet(self.state.staging.clone()));
            }
            Err(err) => events.push(Envelope::ApiBadRequest(ApiBadRequest {
                msg: err.to_string(),
            })),
        }
    }

    fn handle_staging_clear(&mut self, _msg: StagingClear, events: &mut Vec<Envelope>) {
        self.state.staging = Staging::default();
        events.push(Envelope::StagingSet(self.state.staging.clone()));
    }

    fn handle_staging_note(&mut self, msg: StagingNote, events: &mut Vec<Envelope>) {
        self.state.staging.note = msg.note;
        events.push(Envelope::StagingSet(self.state.staging.clone()));
    }

    fn handle_set_avatar(&mut self, msg: SetAvatar, events: &mut Vec<Envelope>) {
        self.state.avatar = msg.avatar;
        self.state.setup = self.state.avatar.is_complete();
        self.persist();
        events.push(Envelope::AppState(self.state.clone()));
    }

    fn handle_set_push_token(&mut self, msg: SetPushToken, events: &mut Vec<Envelope>) {
        self.state.push_token = msg.token;
        self.persist();
        events.push(Envelope::AppState(self.state.clone()));
    }

    fn handle_client_send(&mut self, msg: ClientSend, events: &mut Vec<Envelope>) {
        if self.state.staging.is_empty() {
            events.push(Envelope::ApiBadRequest(ApiBadRequest {
                msg: "Add files before sending".into(),
            }));
            return;
        }
        let Some(device) = self.devices.get(&msg.dst) else {
            events.push(Envelope::ApiBadRequest(ApiBadRequest {
                msg: "Choose a device first".into(),
            }));
            return;
        };

        let now = now_micros();
        let mut transfer = Transfer {
            id: 0,
            tid: Uuid::new_v4(),
            role: Role::Src,
            created_at: now,
            status_at: now,
            manifest: self.state.staging.manifest.clone(),
            note: self.state.staging.note.clone(),
            removed: false,
            deleted: false,
            offset: 0,
            peer_id: device.id,
            path_map: self.state.staging.path_map.clone(),
            dir: None,
            err: None,
            status: TransferStatus::Pending,
            peer_status: TransferStatus::Pending,
            duration: 0,
            speed: None,
        };
        let files: Vec<TransferFile> = transfer
            .manifest
            .iter()
            .map(|entry| TransferFile::pending(entry.path.clone(), entry.size))
            .collect();

        match self.store.insert_transfer_with_files(&transfer, &files) {
            Ok(id) => {
                transfer.id = id;
                match self.store.list_transfer_files(id) {
                    Ok(files) => self.files.insert(id, files),
                    Err(err) => {
                        warn!(error = %err, "failed to load transfer files");
                        None
                    }
                };
                self.transfers.insert(id, transfer.clone());
                self.state.staging = Staging::default();
                self.queue_offer(&transfer);
                events.push(Envelope::StagingSet(self.state.staging.clone()));
                events.push(Envelope::TransferSet(transfer));
            }
            Err(err) => {
                warn!(error = %err, "failed to create transfer");
                events.push(Envelope::ApiBadRequest(ApiBadRequest {
                    msg: "Could not create the transfer".into(),
                }));
            }
        }
    }

    fn handle_transfer_set(&mut self, transfer: Transfer, events: &mut Vec<Envelope>) {
        if transfer.id == 0 {
            let files: Vec<TransferFile> = transfer
                .manifest
                .iter()
                .map(|entry| TransferFile::pending(entry.path.clone(), entry.size))
                .collect();
            match self.store.insert_transfer_with_files(&transfer, &files) {
                Ok(id) => {
                    let mut transfer = transfer;
                    transfer.id = id;
                    match self.store.list_transfer_files(id) {
                        Ok(files) => self.files.insert(id, files),
                        Err(err) => {
                            warn!(error = %err, "failed to load transfer files");
                            None
                        }
                    };
                    self.transfers.insert(id, transfer.clone());
                    events.push(Envelope::TransferSet(transfer));
                }
                Err(err) => warn!(error = %err, "failed to persist transfer"),
            }
            return;
        }

        if let Err(err) = self.store.update_transfer(&transfer) {
            warn!(error = %err, "failed to update transfer");
        }
        self.transfers.insert(transfer.id, transfer.clone());
        events.push(Envelope::TransferSet(transfer));
    }

    fn handle_transfer_accept(&mut self, msg: TransferAccept, events: &mut Vec<Envelope>) {
        if msg.dir.trim().is_empty() {
            events.push(Envelope::ApiBadRequest(ApiBadRequest {
                msg: "Choose a destination folder".into(),
            }));
            return;
        }
        let Some(transfer) = self.transfers.get(&msg.id).cloned() else {
            events.push(Envelope::ApiBadRequest(ApiBadRequest {
                msg: "Transfer no longer exists".into(),
            }));
            return;
        };
        if transfer.role != Role::Dst || transfer.status.is_terminal() {
            events.push(Envelope::ApiBadRequest(ApiBadRequest {
                msg: "This transfer cannot be accepted".into(),
            }));
            return;
        }
        if transfer.status == TransferStatus::Ready {
            return;
        }

        let dir = if transfer.dir.is_some() && transfer.status == TransferStatus::Paused {
            transfer
                .dir
                .clone()
                .expect("paused transfer has a directory")
        } else {
            match self.prepare_destination_files(transfer.id, &msg.dir) {
                Ok(dir) => dir,
                Err(err) => {
                    events.push(Envelope::ApiBadRequest(ApiBadRequest {
                        msg: format!("Cannot use this folder: {err}"),
                    }));
                    return;
                }
            }
        };

        let mut transfer = transfer;
        transfer.dir = Some(dir);
        transfer.status = TransferStatus::Ready;
        transfer.peer_status = TransferStatus::Ready;
        transfer.status_at = now_micros();
        transfer.err = None;
        self.persist_transfer(transfer.clone(), events);
        self.queue_to_peer(
            transfer.peer_id,
            PeerMessage::TransferAccept(TransferTid { tid: transfer.tid }),
        );
    }

    fn prepare_destination_files(&mut self, transfer_id: i64, dir: &str) -> Result<String, String> {
        let root = Path::new(dir)
            .canonicalize()
            .map_err(|err| format!("{err}"))?;
        if !root.is_dir() {
            return Err("The destination is not a folder".into());
        }
        let files = self.files.get(&transfer_id).cloned().unwrap_or_default();
        let mut reserved = BTreeSet::new();
        let mut planned = Vec::with_capacity(files.len());

        for file in files {
            let final_path = if let Some(existing) = file.final_path.as_deref() {
                let path = PathBuf::from(existing);
                ensure_within_root(&root, &path).map_err(|err| err.to_string())?;
                path
            } else {
                let candidate =
                    destination_path(&root, &file.path).map_err(|err| err.to_string())?;
                ensure_within_root(&root, &candidate).map_err(|err| err.to_string())?;
                unique_path_avoiding(&candidate, &reserved)
            };
            reserved.insert(final_path.clone());
            let value = final_path
                .to_str()
                .ok_or_else(|| "The destination path is not valid UTF-8".to_string())?
                .to_string();
            let mut file = file;
            file.final_path = Some(value);
            planned.push(file);
        }

        for file in &planned {
            self.store
                .update_transfer_file(file)
                .map_err(|err| err.to_string())?;
        }
        self.files.insert(transfer_id, planned);
        root.to_str()
            .map(str::to_string)
            .ok_or_else(|| "The destination path is not valid UTF-8".to_string())
    }

    fn set_transfer_status(
        &mut self,
        msg: TransferRef,
        status: TransferStatus,
        events: &mut Vec<Envelope>,
    ) {
        let Some(transfer) = self.transfers.get(&msg.id) else {
            return;
        };
        let previous = transfer.status;
        if previous.is_terminal() || previous == status {
            return;
        }
        if previous == TransferStatus::Pending && status != TransferStatus::Canceled {
            return;
        }

        let tid = transfer.tid;
        let role = transfer.role;
        let peer_id = transfer.peer_id;
        let mut transfer = transfer.clone();
        transfer.status = status;
        transfer.peer_status = status;
        transfer.status_at = now_micros();
        if status == TransferStatus::Canceled {
            transfer.err = None;
            self.cleanup_temp(&transfer);
        }
        self.persist_transfer(transfer, events);

        let message = match status {
            TransferStatus::Paused => Some(PeerMessage::TransferControl(TransferControl {
                tid,
                action: TransferControlAction::Pause,
            })),
            TransferStatus::Ready => Some(PeerMessage::TransferControl(TransferControl {
                tid,
                action: TransferControlAction::Resume,
            })),
            TransferStatus::Canceled
                if role == Role::Dst && previous == TransferStatus::Pending =>
            {
                Some(PeerMessage::TransferDecline(TransferDecline {
                    tid,
                    reason: "Declined".into(),
                }))
            }
            TransferStatus::Canceled => Some(PeerMessage::TransferControl(TransferControl {
                tid,
                action: TransferControlAction::Cancel,
            })),
            TransferStatus::Pending | TransferStatus::Complete | TransferStatus::Failed => None,
        };
        if let Some(message) = message {
            self.queue_to_peer(peer_id, message);
        }
    }

    fn handle_transfer_remove(&mut self, msg: TransferRemove, events: &mut Vec<Envelope>) {
        let files = self.files.get(&msg.id).cloned().unwrap_or_default();
        let Some(transfer) = self.transfers.get_mut(&msg.id) else {
            return;
        };
        if msg.delete_files {
            if let (Role::Dst, Some(dir)) = (transfer.role, transfer.dir.as_deref()) {
                let root = Path::new(dir);
                let mut count = 0;
                for file in &files {
                    let Some(final_path) = file.final_path.as_deref() else {
                        continue;
                    };
                    let path = PathBuf::from(final_path);
                    if let Err(err) = ensure_within_root(root, &path) {
                        warn!(error = %err, path = %path.display(), "refusing to delete an unsafe path");
                        continue;
                    }
                    match std::fs::symlink_metadata(&path) {
                        Ok(metadata) if metadata.is_file() => match std::fs::remove_file(&path) {
                            Ok(()) => count += 1,
                            Err(err) => {
                                warn!(error = %err, path = %path.display(), "failed to delete file")
                            }
                        },
                        Ok(_) => {}
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                        Err(err) => {
                            warn!(error = %err, path = %path.display(), "failed to inspect file")
                        }
                    }
                }
                if files.is_empty()
                    && let Err(err) = delete_transfer_files(root, &transfer.manifest)
                {
                    warn!(error = %err, "failed to remove transfer files");
                }
                debug!(count, "removed transfer files");
            }
            transfer.deleted = true;
        }
        let was_active = !transfer.status.is_terminal();
        let tid = transfer.tid;
        let peer_id = transfer.peer_id;
        transfer.removed = true;
        let transfer = transfer.clone();
        self.cleanup_temp(&transfer);
        if let Err(err) = self.store.update_transfer(&transfer) {
            warn!(error = %err, "failed to remove transfer");
        }
        if was_active {
            self.queue_to_peer(
                peer_id,
                PeerMessage::TransferControl(TransferControl {
                    tid,
                    action: TransferControlAction::Cancel,
                }),
            );
        }
        events.push(Envelope::TransferSet(transfer));
    }

    fn handle_peer_offer(&mut self, peer_id: i64, peer_did: &DeviceId, offer: TransferOffer) {
        let entry_count = offer.entry_count as usize;
        if entry_count == 0 || entry_count > MAX_MANIFEST_ENTRIES {
            self.queue_decline(peer_did, offer.tid, "Invalid offer size");
            return;
        }
        if self.incoming_offers.len() >= MAX_PENDING_OFFERS {
            self.queue_decline(peer_did, offer.tid, "Too many pending offers");
            return;
        }
        if self.incoming_offers.contains_key(&offer.tid)
            || self
                .transfers
                .values()
                .any(|transfer| transfer.tid == offer.tid)
        {
            self.queue_decline(peer_did, offer.tid, "Duplicate transfer");
            return;
        }
        self.incoming_offers.insert(
            offer.tid,
            IncomingOffer {
                peer_id,
                note: offer.note,
                entry_count,
                total_bytes: offer.total_bytes,
                path_bytes: 0,
                entries: Vec::new(),
            },
        );
    }

    fn handle_peer_offer_chunk(&mut self, peer_did: &DeviceId, chunk: TransferOfferChunk) {
        let Some((entry_count, current, path_bytes)) = self
            .incoming_offers
            .get(&chunk.tid)
            .map(|offer| (offer.entry_count, offer.entries.len(), offer.path_bytes))
        else {
            self.queue_decline(peer_did, chunk.tid, "Unknown offer");
            return;
        };
        let incoming_bytes: usize = chunk.entries.iter().map(|entry| entry.path.len()).sum();
        if chunk.entries.is_empty()
            || chunk.entries.len() > OFFER_CHUNK_ENTRIES
            || current.saturating_add(chunk.entries.len()) > entry_count
            || chunk
                .entries
                .iter()
                .any(|entry| entry.path.len() > MAX_PATH_BYTES)
            || path_bytes.saturating_add(incoming_bytes) > MAX_OFFER_PATH_BYTES
        {
            self.incoming_offers.remove(&chunk.tid);
            self.queue_decline(peer_did, chunk.tid, "Invalid offer chunk");
            return;
        }
        if let Some(offer) = self.incoming_offers.get_mut(&chunk.tid) {
            offer.path_bytes = offer.path_bytes.saturating_add(incoming_bytes);
            offer.entries.extend(chunk.entries);
        }
    }

    fn handle_peer_offer_done(
        &mut self,
        peer_id: i64,
        peer_did: &DeviceId,
        done: TransferTid,
        events: &mut Vec<Envelope>,
    ) -> Result<(), ServiceError> {
        let Some(offer) = self.incoming_offers.remove(&done.tid) else {
            self.queue_decline(peer_did, done.tid, "Unknown offer");
            return Ok(());
        };
        if offer.peer_id != peer_id {
            self.queue_decline(peer_did, done.tid, "Offer peer mismatch");
            return Ok(());
        }
        if offer.entries.len() != offer.entry_count {
            self.queue_decline(peer_did, done.tid, "Offer is incomplete");
            return Ok(());
        }

        let mut seen = BTreeSet::new();
        let mut total = 0u64;
        for entry in &offer.entries {
            match validate_manifest_path(&entry.path) {
                Ok(normalized) if normalized == entry.path => {}
                _ => {
                    self.queue_decline(peer_did, done.tid, "Invalid path in offer");
                    return Ok(());
                }
            }
            if !seen.insert(entry.path.clone()) {
                self.queue_decline(peer_did, done.tid, "Duplicate path in offer");
                return Ok(());
            }
            let Some(next) = total.checked_add(entry.size) else {
                self.queue_decline(peer_did, done.tid, "Invalid size in offer");
                return Ok(());
            };
            total = next;
        }
        if total != offer.total_bytes {
            self.queue_decline(peer_did, done.tid, "Offer size mismatch");
            return Ok(());
        }

        let now = now_micros();
        let mut transfer = Transfer {
            id: 0,
            tid: done.tid,
            role: Role::Dst,
            created_at: now,
            status_at: now,
            manifest: offer.entries,
            note: offer.note,
            removed: false,
            deleted: false,
            offset: 0,
            peer_id,
            path_map: PathMap::new(),
            dir: None,
            err: None,
            status: TransferStatus::Pending,
            peer_status: TransferStatus::Pending,
            duration: 0,
            speed: None,
        };
        let files: Vec<TransferFile> = transfer
            .manifest
            .iter()
            .map(|entry| TransferFile::pending(entry.path.clone(), entry.size))
            .collect();

        match self.store.insert_transfer_with_files(&transfer, &files) {
            Ok(id) => {
                transfer.id = id;
                let files = self.store.list_transfer_files(id)?;
                self.transfers.insert(id, transfer.clone());
                self.files.insert(id, files);
                events.push(Envelope::TransferSet(transfer));
            }
            Err(err) => {
                warn!(error = %err, "failed to store inbound transfer");
                self.queue_decline(peer_did, done.tid, "Could not store transfer");
            }
        }
        Ok(())
    }

    fn handle_peer_accept(
        &mut self,
        peer_id: i64,
        accept: TransferTid,
        events: &mut Vec<Envelope>,
    ) {
        let Some(mut transfer) = self.transfer_by_tid(accept.tid) else {
            warn!(tid = %accept.tid, "accept for an unknown transfer");
            return;
        };
        if transfer.peer_id != peer_id
            || transfer.role != Role::Src
            || transfer.status.is_terminal()
        {
            warn!(tid = %accept.tid, "ignoring invalid peer accept");
            return;
        }
        transfer.status = TransferStatus::Ready;
        transfer.peer_status = TransferStatus::Ready;
        transfer.status_at = now_micros();
        transfer.err = None;
        self.persist_transfer(transfer, events);
    }

    fn handle_peer_decline(
        &mut self,
        peer_id: i64,
        decline: TransferDecline,
        events: &mut Vec<Envelope>,
    ) {
        let Some(mut transfer) = self.transfer_by_tid(decline.tid) else {
            warn!(tid = %decline.tid, "decline for an unknown transfer");
            return;
        };
        if transfer.peer_id != peer_id || transfer.status.is_terminal() {
            return;
        }
        transfer.status = TransferStatus::Canceled;
        transfer.peer_status = TransferStatus::Canceled;
        transfer.err = Some(if decline.reason.is_empty() {
            "Declined by peer".into()
        } else {
            decline.reason
        });
        transfer.status_at = now_micros();
        self.persist_transfer(transfer, events);
    }

    fn handle_peer_control(
        &mut self,
        peer_id: i64,
        control: TransferControl,
        events: &mut Vec<Envelope>,
    ) {
        let Some(mut transfer) = self.transfer_by_tid(control.tid) else {
            warn!(tid = %control.tid, "control for an unknown transfer");
            return;
        };
        if transfer.peer_id != peer_id || transfer.status.is_terminal() {
            return;
        }
        let canceled = matches!(control.action, TransferControlAction::Cancel);
        if transfer.status == TransferStatus::Pending && !canceled {
            return;
        }
        match control.action {
            TransferControlAction::Pause => {
                transfer.status = TransferStatus::Paused;
                transfer.peer_status = TransferStatus::Paused;
            }
            TransferControlAction::Resume => {
                transfer.status = TransferStatus::Ready;
                transfer.peer_status = TransferStatus::Ready;
            }
            TransferControlAction::Cancel => {
                transfer.status = TransferStatus::Canceled;
                transfer.peer_status = TransferStatus::Canceled;
                transfer.err = Some("Canceled by peer".into());
            }
        }
        transfer.status_at = now_micros();
        if canceled {
            self.cleanup_temp(&transfer);
        }
        self.persist_transfer(transfer, events);
    }

    fn handle_peer_progress(
        &mut self,
        peer_id: i64,
        progress: TransferProgress,
        events: &mut Vec<Envelope>,
    ) {
        let Some(mut transfer) = self.transfer_by_tid(progress.tid) else {
            warn!(tid = %progress.tid, "progress for an unknown transfer");
            return;
        };
        if transfer.peer_id != peer_id
            || transfer.status.is_terminal()
            || transfer.status == TransferStatus::Pending
        {
            return;
        }
        if transfer.status != TransferStatus::Paused {
            transfer.peer_status = TransferStatus::Ready;
        }
        if progress.speed > 0 {
            transfer.speed = Some(progress.speed);
        }
        transfer.duration = progress.duration;
        self.persist_transfer(transfer, events);
    }

    fn handle_peer_complete(
        &mut self,
        peer_id: i64,
        complete: TransferTid,
        events: &mut Vec<Envelope>,
    ) {
        let Some(mut transfer) = self.transfer_by_tid(complete.tid) else {
            warn!(tid = %complete.tid, "complete for an unknown transfer");
            return;
        };
        if transfer.peer_id != peer_id {
            return;
        }
        if transfer.status.is_terminal() && transfer.status != TransferStatus::Complete {
            return;
        }
        if transfer.status == TransferStatus::Pending {
            warn!(tid = %complete.tid, "peer completed a transfer before it was accepted");
            return;
        }
        transfer.peer_status = TransferStatus::Complete;
        if transfer.role == Role::Src {
            transfer.status = TransferStatus::Complete;
        }
        transfer.status_at = now_micros();
        self.persist_transfer(transfer, events);
    }

    fn handle_peer_failed(
        &mut self,
        peer_id: i64,
        failed: TransferFailed,
        events: &mut Vec<Envelope>,
    ) {
        let Some(mut transfer) = self.transfer_by_tid(failed.tid) else {
            warn!(tid = %failed.tid, "failure for an unknown transfer");
            return;
        };
        if transfer.peer_id != peer_id || transfer.status.is_terminal() {
            return;
        }
        transfer.status = TransferStatus::Failed;
        transfer.peer_status = TransferStatus::Failed;
        transfer.err = Some(if failed.err.is_empty() {
            "Transfer failed on the other device".into()
        } else {
            failed.err
        });
        transfer.status_at = now_micros();
        self.persist_transfer(transfer, events);
    }

    fn handle_peer_resume_query(&mut self, peer_id: i64, query: TransferTid) {
        let Some(transfer) = self.transfer_by_tid(query.tid) else {
            warn!(tid = %query.tid, "resume query for an unknown transfer");
            return;
        };
        if transfer.peer_id != peer_id || transfer.role != Role::Dst {
            return;
        }
        let files = self
            .files
            .get(&transfer.id)
            .map(|files| {
                files
                    .iter()
                    .map(|file| FileProgress {
                        path: file.path.clone(),
                        offset: file.offset,
                        state: file.state,
                    })
                    .collect()
            })
            .unwrap_or_default();
        self.queue_to_peer(
            peer_id,
            PeerMessage::TransferResumeState(TransferResumeState {
                tid: transfer.tid,
                files,
            }),
        );
    }

    fn handle_peer_resume_state(
        &mut self,
        peer_id: i64,
        state: TransferResumeState,
        events: &mut Vec<Envelope>,
    ) -> Result<(), ServiceError> {
        let Some(mut transfer) = self.transfer_by_tid(state.tid) else {
            warn!(tid = %state.tid, "resume state for an unknown transfer");
            return Ok(());
        };
        if transfer.peer_id != peer_id
            || transfer.role != Role::Src
            || transfer.status.is_terminal()
            || transfer.status == TransferStatus::Pending
        {
            return Ok(());
        }

        let mut updated = Vec::new();
        if let Some(files) = self.files.get_mut(&transfer.id) {
            for progress in state.files {
                let Some(file) = files.iter_mut().find(|file| file.path == progress.path) else {
                    warn!(tid = %state.tid, path = %progress.path, "resume state for an unknown file");
                    continue;
                };
                file.offset = progress.offset.min(file.size);
                file.state = progress.state;
                updated.push(file.clone());
            }
            transfer.offset = files.iter().map(|file| file.offset).sum();
        }

        for file in updated {
            self.store.update_transfer_file(&file)?;
        }
        self.persist_transfer(transfer, events);
        Ok(())
    }

    fn ensure_peer(&mut self, did: &DeviceId) -> Result<(i64, Option<Device>), ServiceError> {
        if let Some(device) = self.devices.values().find(|device| device.did == *did) {
            return Ok((device.id, None));
        }
        let now = now_micros();
        let mut device = Device {
            did: did.clone(),
            active: true,
            active_at: now,
            last_seen_at: now,
            ..Device::default()
        };
        let id = self.store.upsert_device(&device)?;
        device.id = id;
        self.devices.insert(id, device.clone());
        Ok((id, Some(device)))
    }

    fn transfer_by_tid(&self, tid: Uuid) -> Option<Transfer> {
        self.transfers
            .values()
            .find(|transfer| transfer.tid == tid)
            .cloned()
    }

    fn persist_transfer(&mut self, transfer: Transfer, events: &mut Vec<Envelope>) {
        if let Err(err) = self.store.update_transfer(&transfer) {
            warn!(error = %err, "failed to update transfer");
        }
        self.transfers.insert(transfer.id, transfer.clone());
        events.push(Envelope::TransferSet(transfer));
    }

    fn queue_offer(&mut self, transfer: &Transfer) {
        self.queue_to_peer(
            transfer.peer_id,
            PeerMessage::TransferOffer(TransferOffer {
                tid: transfer.tid,
                note: transfer.note.clone(),
                syn: now_micros(),
                entry_count: transfer.manifest.len() as u32,
                total_bytes: transfer.total_size(),
            }),
        );
        for chunk in transfer.manifest.chunks(OFFER_CHUNK_ENTRIES) {
            self.queue_to_peer(
                transfer.peer_id,
                PeerMessage::TransferOfferChunk(TransferOfferChunk {
                    tid: transfer.tid,
                    entries: chunk.to_vec(),
                }),
            );
        }
        self.queue_to_peer(
            transfer.peer_id,
            PeerMessage::TransferOfferDone(TransferTid { tid: transfer.tid }),
        );
    }

    fn queue_decline(&mut self, peer_did: &DeviceId, tid: Uuid, reason: impl Into<String>) {
        self.peer_outbox.push(PeerOutbound {
            peer_did: peer_did.clone(),
            message: PeerMessage::TransferDecline(TransferDecline {
                tid,
                reason: reason.into(),
            }),
        });
    }

    fn queue_to_peer(&mut self, peer_id: i64, message: PeerMessage) {
        let Some(device) = self.devices.get(&peer_id) else {
            warn!(
                peer_id,
                kind = message.kind(),
                "cannot queue a peer message for an unknown device"
            );
            return;
        };
        self.peer_outbox.push(PeerOutbound {
            peer_did: device.did.clone(),
            message,
        });
    }

    fn persist(&mut self) {
        let mut prefs = match self.store.load_prefs() {
            Ok(Some(prefs)) => prefs,
            Ok(None) | Err(_) => Prefs::default(),
        };
        prefs.did = self.state.did.clone();
        prefs.avatar = self.state.avatar.clone();
        prefs.push_token = self.state.push_token.clone();
        prefs.setup = self.state.setup;
        prefs.setup_account = self.state.setup_account;
        prefs.setup_available = self.state.setup_available;
        if let Err(err) = self.store.save_prefs(&prefs) {
            warn!(error = %err, "failed to persist preferences");
        }
    }
}

fn source_problem(transfer: &Transfer) -> Option<String> {
    for entry in &transfer.manifest {
        let path = match resolve_source(&transfer.path_map, &entry.path) {
            Ok(path) => path,
            Err(err) => return Some(format!("Source path is unavailable: {err}")),
        };
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => {
                if metadata.len() != entry.size {
                    return Some(format!("Source file changed: {}", path.display()));
                }
            }
            Ok(_) => return Some(format!("Source is not a regular file: {}", path.display())),
            Err(_) => return Some(format!("Source file is missing: {}", path.display())),
        }
    }
    None
}

fn merge_staging(current: &mut Staging, mut incoming: Staging) {
    if current.manifest.is_empty() {
        incoming.note = current.note.clone();
        *current = incoming;
        return;
    }

    let mut entries: BTreeMap<String, u64> = current
        .manifest
        .iter()
        .map(|entry| (entry.path.clone(), entry.size))
        .collect();
    entries.extend(
        incoming
            .manifest
            .iter()
            .map(|entry| (entry.path.clone(), entry.size)),
    );
    current.manifest = entries
        .into_iter()
        .map(|(path, size)| crate::model::ManifestEntry { path, size })
        .collect();
    current.path_map.extend(incoming.path_map);
}

/// Convenience for tests and the gallery: build a synthetic device.
pub fn demo_device(id: i64, seed: u8, owner: &str, name: &str) -> Device {
    Device {
        id,
        did: DeviceId::from_public_key(&[seed; 32]),
        avatar: crate::model::Avatar {
            owner: owner.into(),
            name: name.into(),
            variant: "macbook".into(),
            color: "green".into(),
        },
        active: true,
        active_at: now_micros(),
        last_seen_at: now_micros(),
        ..Device::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::{Empty, StagingNote, TransferControl, TransferControlAction, TransferFailed};

    fn service() -> (Service, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::with_root(dir.path());
        let (service, _events) = Service::start(&paths).unwrap();
        (service, dir)
    }

    fn stage_file(service: &mut Service, dir: &tempfile::TempDir, name: &str, body: &[u8]) {
        let file = dir.path().join(name);
        std::fs::write(&file, body).unwrap();
        service
            .handle(Envelope::StagingAdd(StagingAdd {
                paths: vec![file.to_string_lossy().to_string()],
            }))
            .unwrap();
    }

    fn peer(service: &mut Service, seed: u8) -> DeviceId {
        let device = demo_device(1, seed, "Ada", "Studio");
        let did = device.did.clone();
        service.handle(Envelope::SetPeer(device)).unwrap();
        did
    }

    fn device_for(did: DeviceId) -> Device {
        Device {
            did,
            avatar: crate::model::Avatar {
                owner: "Ada".into(),
                name: "Studio".into(),
                variant: "macbook".into(),
                color: "green".into(),
            },
            active: true,
            active_at: now_micros(),
            last_seen_at: now_micros(),
            ..Device::default()
        }
    }

    #[test]
    fn starts_with_identity_and_live_state() {
        let (service, _dir) = service();
        assert!(service.state().live);
        assert_eq!(service.state().did.as_str().len(), 43);
        assert!(!service.state().setup);
    }

    #[test]
    fn staging_and_avatar_update_state() {
        let (mut service, dir) = service();
        stage_file(&mut service, &dir, "hello.txt", b"hello");
        assert_eq!(service.state().staging.manifest.len(), 1);

        service
            .handle(Envelope::SetAvatar(SetAvatar {
                avatar: crate::model::Avatar {
                    owner: "Ken".into(),
                    name: "Macbook".into(),
                    variant: "macbook".into(),
                    color: "green".into(),
                },
                syn: 0,
            }))
            .unwrap();
        assert!(service.state().setup);
    }

    #[test]
    fn guest_mode_marks_account_setup() {
        let (mut service, _dir) = service();
        service.handle(Envelope::SetGuestMode(Empty {})).unwrap();
        assert!(service.state().setup_account);
    }

    #[test]
    fn send_requires_staging_and_device() {
        let (mut service, dir) = service();
        let events = service
            .handle(Envelope::ClientSend(ClientSend { dst: 1 }))
            .unwrap();
        assert!(matches!(events[0], Envelope::ApiBadRequest(_)));

        stage_file(&mut service, &dir, "hello.txt", b"hello");
        let events = service
            .handle(Envelope::ClientSend(ClientSend { dst: 99 }))
            .unwrap();
        assert!(matches!(events[0], Envelope::ApiBadRequest(_)));
    }

    #[test]
    fn sending_creates_a_pending_transfer_and_offer() {
        let (mut service, dir) = service();
        let did = peer(&mut service, 9);
        stage_file(&mut service, &dir, "hello.txt", b"hello");
        service
            .handle(Envelope::StagingNote(StagingNote { note: "hi".into() }))
            .unwrap();

        let events = service
            .handle(Envelope::ClientSend(ClientSend { dst: 1 }))
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Envelope::TransferSet(_)))
        );
        assert!(service.state().staging.is_empty());

        let transfer = service.transfers.values().next().unwrap();
        assert_eq!(transfer.status, TransferStatus::Pending);
        assert_eq!(transfer.peer_status, TransferStatus::Pending);
        assert_eq!(transfer.manifest.len(), 1);
        assert_eq!(service.files.get(&transfer.id).unwrap().len(), 1);

        let outbox = service.drain_peer_outbox();
        assert_eq!(outbox[0].peer_did, did);
        assert!(matches!(outbox[0].message, PeerMessage::TransferOffer(_)));
        assert!(
            outbox
                .iter()
                .any(|message| matches!(message.message, PeerMessage::TransferOfferDone(_)))
        );
    }

    #[test]
    fn accepts_an_inbound_offer_after_validation() {
        let (mut service, _dir) = service();
        let did = peer(&mut service, 7);
        let tid = Uuid::new_v4();

        service
            .handle_peer(
                &did,
                PeerMessage::TransferOffer(TransferOffer {
                    tid,
                    note: "hello".into(),
                    syn: 0,
                    entry_count: 2,
                    total_bytes: 11,
                }),
            )
            .unwrap();
        service
            .handle_peer(
                &did,
                PeerMessage::TransferOfferChunk(TransferOfferChunk {
                    tid,
                    entries: vec![
                        crate::model::ManifestEntry {
                            path: "a.txt".into(),
                            size: 5,
                        },
                        crate::model::ManifestEntry {
                            path: "b.txt".into(),
                            size: 6,
                        },
                    ],
                }),
            )
            .unwrap();
        let events = service
            .handle_peer(&did, PeerMessage::TransferOfferDone(TransferTid { tid }))
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Envelope::TransferSet(_)))
        );

        let transfer = service.transfers.values().next().unwrap();
        assert_eq!(transfer.status, TransferStatus::Pending);
        assert_eq!(transfer.role, Role::Dst);
        assert_eq!(service.files.get(&transfer.id).unwrap().len(), 2);

        let transfer_id = transfer.id;
        std::fs::create_dir_all(_dir.path().join("inbox")).unwrap();
        service
            .handle(Envelope::TransferAccept(TransferAccept {
                id: transfer_id,
                dir: _dir.path().join("inbox").to_string_lossy().to_string(),
            }))
            .unwrap();
        let outbox = service.drain_peer_outbox();
        assert!(matches!(
            outbox.last().unwrap().message,
            PeerMessage::TransferAccept(_)
        ));
        let transfer = service.transfers.get(&transfer_id).unwrap();
        assert_eq!(transfer.status, TransferStatus::Ready);
    }

    #[test]
    fn accept_assigns_non_colliding_destination_paths() {
        let (mut service, _dir) = service();
        let did = peer(&mut service, 7);
        let tid = Uuid::new_v4();
        let destination = tempfile::tempdir().unwrap();
        std::fs::write(destination.path().join("hello.txt"), b"old").unwrap();

        service
            .handle_peer(
                &did,
                PeerMessage::TransferOffer(TransferOffer {
                    tid,
                    note: String::new(),
                    syn: 0,
                    entry_count: 1,
                    total_bytes: 5,
                }),
            )
            .unwrap();
        service
            .handle_peer(
                &did,
                PeerMessage::TransferOfferChunk(TransferOfferChunk {
                    tid,
                    entries: vec![crate::model::ManifestEntry {
                        path: "hello.txt".into(),
                        size: 5,
                    }],
                }),
            )
            .unwrap();
        service
            .handle_peer(&did, PeerMessage::TransferOfferDone(TransferTid { tid }))
            .unwrap();
        let id = service.transfers.values().next().unwrap().id;
        service
            .handle(Envelope::TransferAccept(TransferAccept {
                id,
                dir: destination.path().to_string_lossy().to_string(),
            }))
            .unwrap();

        let files = service.files.get(&id).unwrap();
        assert_eq!(files.len(), 1);
        let final_path = files[0].final_path.as_deref().unwrap();
        assert!(final_path.ends_with("hello (1).txt"));
        assert_eq!(
            std::fs::read(destination.path().join("hello.txt")).unwrap(),
            b"old"
        );
    }

    #[test]
    fn applies_data_plane_events_to_files_and_status() {
        let (mut service, dir) = service();
        let did = peer(&mut service, 8);
        stage_file(&mut service, &dir, "hello.txt", b"hello");
        service
            .handle(Envelope::ClientSend(ClientSend { dst: 1 }))
            .unwrap();
        let id = service.transfers.values().next().unwrap().id;
        let tid = service.transfers.values().next().unwrap().tid;
        service
            .handle_peer(&did, PeerMessage::TransferAccept(TransferTid { tid }))
            .unwrap();

        service
            .apply_transfer_event(TransferEvent::Progress(crate::msg::TransferProgressEvent {
                tid,
                path: "hello.txt".into(),
                offset: 3,
                speed: 10,
            }))
            .unwrap();
        let file = &service.files.get(&id).unwrap()[0];
        assert_eq!(file.offset, 3);
        assert_eq!(file.state, FileState::Transferring);

        service
            .apply_transfer_event(TransferEvent::FileComplete(crate::msg::TransferFileEvent {
                tid,
                path: "hello.txt".into(),
                offset: 5,
                sha256: "abc".into(),
            }))
            .unwrap();
        service
            .apply_transfer_event(TransferEvent::TransferComplete(TransferTid { tid }))
            .unwrap();
        service
            .handle_peer(&did, PeerMessage::TransferComplete(TransferTid { tid }))
            .unwrap();

        let transfer = service.transfers.get(&id).unwrap();
        assert_eq!(transfer.status, TransferStatus::Complete);
        assert_eq!(transfer.peer_status, TransferStatus::Complete);
        assert_eq!(transfer.effective_status(), TransferStatus::Complete);
        let file = &service.files.get(&id).unwrap()[0];
        assert_eq!(file.state, FileState::Complete);
        assert_eq!(file.offset, 5);
        assert_eq!(file.sha256.as_deref(), Some("abc"));
    }

    #[test]
    fn cancel_event_removes_the_temporary_directory() {
        let (mut service, dir) = service();
        let did = peer(&mut service, 7);
        let tid = Uuid::new_v4();
        let destination = tempfile::tempdir().unwrap();
        service
            .handle_peer(
                &did,
                PeerMessage::TransferOffer(TransferOffer {
                    tid,
                    note: String::new(),
                    syn: 0,
                    entry_count: 1,
                    total_bytes: 5,
                }),
            )
            .unwrap();
        service
            .handle_peer(
                &did,
                PeerMessage::TransferOfferChunk(TransferOfferChunk {
                    tid,
                    entries: vec![crate::model::ManifestEntry {
                        path: "hello.txt".into(),
                        size: 5,
                    }],
                }),
            )
            .unwrap();
        service
            .handle_peer(&did, PeerMessage::TransferOfferDone(TransferTid { tid }))
            .unwrap();
        let id = service.transfers.values().next().unwrap().id;
        service
            .handle(Envelope::TransferAccept(TransferAccept {
                id,
                dir: destination.path().to_string_lossy().to_string(),
            }))
            .unwrap();
        let temp_dir = crate::paths::temp_dir(destination.path(), tid);
        std::fs::create_dir_all(&temp_dir).unwrap();
        std::fs::write(temp_dir.join("0.part"), b"part").unwrap();

        service
            .apply_transfer_event(TransferEvent::TransferCanceled(TransferTid { tid }))
            .unwrap();
        assert!(!temp_dir.exists());
        assert_eq!(
            service.transfers.get(&id).unwrap().status,
            TransferStatus::Canceled
        );
        let _ = dir;
    }

    #[test]
    fn rejects_an_offer_with_an_unsafe_path() {
        let (mut service, _dir) = service();
        let did = peer(&mut service, 7);
        let tid = Uuid::new_v4();

        service
            .handle_peer(
                &did,
                PeerMessage::TransferOffer(TransferOffer {
                    tid,
                    note: String::new(),
                    syn: 0,
                    entry_count: 1,
                    total_bytes: 5,
                }),
            )
            .unwrap();
        service
            .handle_peer(
                &did,
                PeerMessage::TransferOfferChunk(TransferOfferChunk {
                    tid,
                    entries: vec![crate::model::ManifestEntry {
                        path: "../escape.txt".into(),
                        size: 5,
                    }],
                }),
            )
            .unwrap();
        service
            .handle_peer(&did, PeerMessage::TransferOfferDone(TransferTid { tid }))
            .unwrap();

        assert!(service.transfers.is_empty());
        let outbox = service.drain_peer_outbox();
        assert!(matches!(
            outbox.last().unwrap().message,
            PeerMessage::TransferDecline(_)
        ));
    }

    #[test]
    fn peer_cancel_overrides_local_ready() {
        let (mut service, dir) = service();
        let did = peer(&mut service, 8);
        stage_file(&mut service, &dir, "hello.txt", b"hello");
        service
            .handle(Envelope::ClientSend(ClientSend { dst: 1 }))
            .unwrap();
        let tid = service.transfers.values().next().unwrap().tid;
        service
            .handle_peer(&did, PeerMessage::TransferAccept(TransferTid { tid }))
            .unwrap();
        assert_eq!(
            service.transfers.values().next().unwrap().status,
            TransferStatus::Ready
        );

        service
            .handle_peer(
                &did,
                PeerMessage::TransferControl(TransferControl {
                    tid,
                    action: TransferControlAction::Cancel,
                }),
            )
            .unwrap();
        let transfer = service.transfers.values().next().unwrap();
        assert_eq!(transfer.status, TransferStatus::Canceled);
        assert_eq!(transfer.effective_status(), TransferStatus::Canceled);
    }

    #[test]
    fn local_pause_and_resume_are_mirrored_to_the_peer() {
        let (mut service, dir) = service();
        let did = peer(&mut service, 8);
        stage_file(&mut service, &dir, "hello.txt", b"hello");
        service
            .handle(Envelope::ClientSend(ClientSend { dst: 1 }))
            .unwrap();
        let id = service.transfers.values().next().unwrap().id;
        let tid = service.transfers.values().next().unwrap().tid;
        service
            .handle_peer(&did, PeerMessage::TransferAccept(TransferTid { tid }))
            .unwrap();
        let _ = service.drain_peer_outbox();

        service
            .handle(Envelope::TransferPause(TransferRef { id }))
            .unwrap();
        let transfer = service.transfers.get(&id).unwrap();
        assert_eq!(transfer.status, TransferStatus::Paused);
        assert_eq!(transfer.peer_status, TransferStatus::Paused);
        assert_eq!(transfer.effective_status(), TransferStatus::Paused);
        assert!(matches!(
            service.drain_peer_outbox().last().unwrap().message,
            PeerMessage::TransferControl(TransferControl {
                action: TransferControlAction::Pause,
                ..
            })
        ));

        service
            .handle(Envelope::TransferResume(TransferRef { id }))
            .unwrap();
        let transfer = service.transfers.get(&id).unwrap();
        assert_eq!(transfer.status, TransferStatus::Ready);
        assert_eq!(transfer.effective_status(), TransferStatus::Ready);
        assert!(matches!(
            service.drain_peer_outbox().last().unwrap().message,
            PeerMessage::TransferControl(TransferControl {
                action: TransferControlAction::Resume,
                ..
            })
        ));
    }

    #[test]
    fn declining_a_pending_inbound_offer_sends_a_decline() {
        let (mut service, _dir) = service();
        let did = peer(&mut service, 7);
        let tid = Uuid::new_v4();
        service
            .handle_peer(
                &did,
                PeerMessage::TransferOffer(TransferOffer {
                    tid,
                    note: String::new(),
                    syn: 0,
                    entry_count: 1,
                    total_bytes: 5,
                }),
            )
            .unwrap();
        service
            .handle_peer(
                &did,
                PeerMessage::TransferOfferChunk(TransferOfferChunk {
                    tid,
                    entries: vec![crate::model::ManifestEntry {
                        path: "hello.txt".into(),
                        size: 5,
                    }],
                }),
            )
            .unwrap();
        service
            .handle_peer(&did, PeerMessage::TransferOfferDone(TransferTid { tid }))
            .unwrap();
        let id = service.transfers.values().next().unwrap().id;
        let _ = service.drain_peer_outbox();

        service
            .handle(Envelope::TransferCancel(TransferRef { id }))
            .unwrap();
        let transfer = service.transfers.get(&id).unwrap();
        assert_eq!(transfer.status, TransferStatus::Canceled);
        assert!(matches!(
            service.drain_peer_outbox().last().unwrap().message,
            PeerMessage::TransferDecline(_)
        ));
    }

    #[test]
    fn peer_failure_is_terminal() {
        let (mut service, dir) = service();
        let did = peer(&mut service, 8);
        stage_file(&mut service, &dir, "hello.txt", b"hello");
        service
            .handle(Envelope::ClientSend(ClientSend { dst: 1 }))
            .unwrap();
        let tid = service.transfers.values().next().unwrap().tid;
        service
            .handle_peer(
                &did,
                PeerMessage::TransferFailed(TransferFailed {
                    tid,
                    err: "disk full".into(),
                }),
            )
            .unwrap();
        let transfer = service.transfers.values().next().unwrap();
        assert_eq!(transfer.status, TransferStatus::Failed);
        assert_eq!(transfer.effective_status(), TransferStatus::Failed);
        assert_eq!(transfer.err.as_deref(), Some("disk full"));
    }

    #[test]
    fn records_progress_on_an_accepted_transfer() {
        let (mut service, dir) = service();
        let did = peer(&mut service, 8);
        stage_file(&mut service, &dir, "hello.txt", b"hello");
        service
            .handle(Envelope::ClientSend(ClientSend { dst: 1 }))
            .unwrap();
        let transfer = service.transfers.values().next().unwrap().clone();
        service
            .handle_peer(
                &did,
                PeerMessage::TransferAccept(TransferTid { tid: transfer.tid }),
            )
            .unwrap();

        let events = service
            .record_progress(transfer.tid, "hello.txt", 3, Some(10))
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Envelope::TransferSet(_)))
        );
        let transfer = service.transfers.get(&transfer.id).unwrap();
        assert_eq!(transfer.offset, 3);
        assert_eq!(transfer.speed, Some(10));
        assert_eq!(
            service.files.get(&transfer.id).unwrap()[0].state,
            FileState::Transferring
        );
    }

    #[test]
    fn replies_to_resume_queries_with_file_offsets() {
        let (mut service, dir) = service();
        let did = peer(&mut service, 8);
        let tid = Uuid::new_v4();
        service
            .handle_peer(
                &did,
                PeerMessage::TransferOffer(TransferOffer {
                    tid,
                    note: String::new(),
                    syn: 0,
                    entry_count: 1,
                    total_bytes: 5,
                }),
            )
            .unwrap();
        service
            .handle_peer(
                &did,
                PeerMessage::TransferOfferChunk(TransferOfferChunk {
                    tid,
                    entries: vec![crate::model::ManifestEntry {
                        path: "hello.txt".into(),
                        size: 5,
                    }],
                }),
            )
            .unwrap();
        service
            .handle_peer(&did, PeerMessage::TransferOfferDone(TransferTid { tid }))
            .unwrap();
        let id = service.transfers.values().next().unwrap().id;
        std::fs::create_dir_all(dir.path().join("inbox")).unwrap();
        service
            .handle(Envelope::TransferAccept(TransferAccept {
                id,
                dir: dir.path().join("inbox").to_string_lossy().to_string(),
            }))
            .unwrap();
        service.record_progress(tid, "hello.txt", 2, None).unwrap();
        let _ = service.drain_peer_outbox();

        service
            .handle_peer(&did, PeerMessage::TransferResumeQuery(TransferTid { tid }))
            .unwrap();
        let outbox = service.drain_peer_outbox();
        let PeerMessage::TransferResumeState(state) = &outbox.last().unwrap().message else {
            panic!("expected a resume state");
        };
        assert_eq!(state.files.len(), 1);
        assert_eq!(state.files[0].offset, 2);
    }

    #[test]
    fn two_services_complete_the_offer_and_accept_flow() {
        let (mut sender, sender_dir) = service();
        let (mut receiver, receiver_dir) = service();
        let sender_did = sender.state().did.clone();
        let receiver_did = receiver.state().did.clone();

        sender
            .handle(Envelope::SetPeer(device_for(receiver_did.clone())))
            .unwrap();
        let dst = sender.devices.values().next().unwrap().id;
        stage_file(&mut sender, &sender_dir, "hello.txt", b"hello");
        sender
            .handle(Envelope::ClientSend(ClientSend { dst }))
            .unwrap();

        for outbound in sender.drain_peer_outbox() {
            receiver.handle_peer(&sender_did, outbound.message).unwrap();
        }
        let inbound = receiver.transfers.values().next().unwrap().clone();
        assert_eq!(inbound.status, TransferStatus::Pending);
        assert_eq!(inbound.role, Role::Dst);
        let sender_peer = receiver.devices.values().next().unwrap();
        assert_eq!(sender_peer.did, sender_did);
        assert_eq!(inbound.peer_id, sender_peer.id);

        std::fs::create_dir_all(receiver_dir.path().join("inbox")).unwrap();
        receiver
            .handle(Envelope::TransferAccept(TransferAccept {
                id: inbound.id,
                dir: receiver_dir
                    .path()
                    .join("inbox")
                    .to_string_lossy()
                    .to_string(),
            }))
            .unwrap();
        for outbound in receiver.drain_peer_outbox() {
            sender.handle_peer(&receiver_did, outbound.message).unwrap();
        }

        assert_eq!(
            sender.transfers.values().next().unwrap().status,
            TransferStatus::Ready
        );
        assert_eq!(
            receiver.transfers.get(&inbound.id).unwrap().status,
            TransferStatus::Ready
        );
        assert_eq!(
            sender.transfers.values().next().unwrap().effective_status(),
            TransferStatus::Ready
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_dst_transfer(
        service: &Service,
        tid: Uuid,
        root: &Path,
        entries: &[(&str, u64)],
        final_paths: &[PathBuf],
        status: TransferStatus,
        states: &[FileState],
        offsets: &[u64],
    ) -> i64 {
        let device_id = service
            .store
            .upsert_device(&demo_device(1, 9, "Ada", "Studio"))
            .unwrap();
        let manifest: crate::model::Manifest = entries
            .iter()
            .map(|(path, size)| crate::model::ManifestEntry {
                path: (*path).to_string(),
                size: *size,
            })
            .collect();
        let files: Vec<TransferFile> = entries
            .iter()
            .enumerate()
            .map(|(index, (path, size))| {
                let mut file = TransferFile::pending(*path, *size);
                file.final_path = Some(final_paths[index].to_string_lossy().to_string());
                file.state = states.get(index).copied().unwrap_or(FileState::Pending);
                file.offset = offsets.get(index).copied().unwrap_or(0);
                file
            })
            .collect();
        let transfer = Transfer {
            id: 0,
            tid,
            role: Role::Dst,
            created_at: now_micros(),
            status_at: now_micros(),
            manifest,
            note: String::new(),
            removed: false,
            deleted: false,
            offset: offsets.iter().sum(),
            peer_id: device_id,
            path_map: BTreeMap::new(),
            dir: Some(root.to_string_lossy().to_string()),
            err: None,
            status,
            peer_status: status,
            duration: 0,
            speed: None,
        };
        service
            .store
            .insert_transfer_with_files(&transfer, &files)
            .unwrap()
    }

    #[test]
    fn recovery_completes_a_file_whose_final_exists() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::with_root(dir.path());
        let destination = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(destination.path()).unwrap();
        let tid = Uuid::new_v4();
        let final_path = root.join("a.txt");
        std::fs::write(&final_path, b"hello").unwrap();
        let temp_dir = crate::paths::temp_dir(&root, tid);
        std::fs::create_dir_all(&temp_dir).unwrap();
        std::fs::write(crate::paths::temp_file(&root, tid, 0), b"part").unwrap();
        let id;
        {
            let (service, _events) = Service::start(&paths).unwrap();
            id = insert_dst_transfer(
                &service,
                tid,
                &root,
                &[("a.txt", 5)],
                std::slice::from_ref(&final_path),
                TransferStatus::Ready,
                &[FileState::Transferring],
                &[2],
            );
        }

        let (service, _events) = Service::start(&paths).unwrap();
        let transfer = service.transfers.get(&id).unwrap();
        assert_eq!(transfer.status, TransferStatus::Paused);
        let file = &service.files.get(&id).unwrap()[0];
        assert_eq!(file.state, FileState::Complete);
        assert_eq!(file.offset, 5);
        assert!(!crate::paths::temp_dir(&root, tid).exists());
    }

    #[test]
    fn recovery_resumes_from_the_temporary_file_length() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::with_root(dir.path());
        let destination = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(destination.path()).unwrap();
        let tid = Uuid::new_v4();
        let final_path = root.join("a.txt");
        std::fs::create_dir_all(crate::paths::temp_dir(&root, tid)).unwrap();
        std::fs::write(crate::paths::temp_file(&root, tid, 0), b"abc").unwrap();
        let id;
        {
            let (service, _events) = Service::start(&paths).unwrap();
            id = insert_dst_transfer(
                &service,
                tid,
                &root,
                &[("a.txt", 5)],
                std::slice::from_ref(&final_path),
                TransferStatus::Ready,
                &[FileState::Transferring],
                &[0],
            );
        }

        let (service, _events) = Service::start(&paths).unwrap();
        let transfer = service.transfers.get(&id).unwrap();
        assert_eq!(transfer.status, TransferStatus::Paused);
        assert_eq!(transfer.offset, 3);
        let file = &service.files.get(&id).unwrap()[0];
        assert_eq!(file.offset, 3);
        assert_eq!(file.state, FileState::Transferring);
        assert!(crate::paths::temp_file(&root, tid, 0).exists());
    }

    #[test]
    fn recovery_removes_temporary_files_for_canceled_transfers() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::with_root(dir.path());
        let destination = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(destination.path()).unwrap();
        let tid = Uuid::new_v4();
        let final_path = root.join("a.txt");
        std::fs::create_dir_all(crate::paths::temp_dir(&root, tid)).unwrap();
        std::fs::write(crate::paths::temp_file(&root, tid, 0), b"part").unwrap();
        let id;
        {
            let (service, _events) = Service::start(&paths).unwrap();
            id = insert_dst_transfer(
                &service,
                tid,
                &root,
                &[("a.txt", 5)],
                std::slice::from_ref(&final_path),
                TransferStatus::Canceled,
                &[FileState::Transferring],
                &[2],
            );
        }

        let (service, _events) = Service::start(&paths).unwrap();
        assert_eq!(
            service.transfers.get(&id).unwrap().status,
            TransferStatus::Canceled
        );
        assert!(!crate::paths::temp_dir(&root, tid).exists());
    }

    #[test]
    fn recovery_fails_when_a_completed_file_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::with_root(dir.path());
        let destination = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(destination.path()).unwrap();
        let tid = Uuid::new_v4();
        let final_path = root.join("a.txt");
        let id;
        {
            let (service, _events) = Service::start(&paths).unwrap();
            id = insert_dst_transfer(
                &service,
                tid,
                &root,
                &[("a.txt", 5)],
                std::slice::from_ref(&final_path),
                TransferStatus::Paused,
                &[FileState::Complete],
                &[5],
            );
        }

        let (service, _events) = Service::start(&paths).unwrap();
        let transfer = service.transfers.get(&id).unwrap();
        assert_eq!(transfer.status, TransferStatus::Failed);
        assert!(transfer.err.as_deref().unwrap().contains("missing"));
        assert_eq!(service.files.get(&id).unwrap()[0].state, FileState::Failed);
    }

    #[test]
    fn recovery_pauses_ready_and_keeps_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::with_root(dir.path());
        let transfer_id;
        let tid;
        {
            let (mut service, _events) = Service::start(&paths).unwrap();
            peer(&mut service, 9);
            stage_file(&mut service, &dir, "hello.txt", b"hello");
            service
                .handle(Envelope::ClientSend(ClientSend { dst: 1 }))
                .unwrap();
            let mut transfer = service.transfers.values().next().unwrap().clone();
            transfer_id = transfer.id;
            tid = transfer.tid;
            transfer.status = TransferStatus::Ready;
            service.store.update_transfer(&transfer).unwrap();
            service.transfers.insert(transfer_id, transfer);
            service.record_progress(tid, "hello.txt", 2, None).unwrap();
            assert_eq!(
                service.transfers.get(&transfer_id).unwrap().status,
                TransferStatus::Ready
            );
        }

        let (mut service, _events) = Service::start(&paths).unwrap();
        let transfer = service.transfers.get(&transfer_id).unwrap();
        assert_eq!(transfer.status, TransferStatus::Paused);
        assert_eq!(transfer.offset, 2);
        assert_eq!(service.files.get(&transfer_id).unwrap()[0].offset, 2);
        assert!(service.drain_peer_outbox().is_empty());
    }

    #[test]
    fn recovery_fails_a_sender_when_the_source_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::with_root(dir.path());
        let transfer_id;
        {
            let (mut service, _events) = Service::start(&paths).unwrap();
            peer(&mut service, 9);
            stage_file(&mut service, &dir, "hello.txt", b"hello");
            service
                .handle(Envelope::ClientSend(ClientSend { dst: 1 }))
                .unwrap();
            transfer_id = service.transfers.values().next().unwrap().id;
        }
        std::fs::remove_file(dir.path().join("hello.txt")).unwrap();

        let (service, _events) = Service::start(&paths).unwrap();
        let transfer = service.transfers.get(&transfer_id).unwrap();
        assert_eq!(transfer.status, TransferStatus::Failed);
        assert_eq!(transfer.effective_status(), TransferStatus::Failed);
        assert!(transfer.err.as_deref().unwrap().contains("missing"));
    }

    #[test]
    fn removes_a_transfer_and_notifies_an_active_peer() {
        let (mut service, dir) = service();
        peer(&mut service, 9);
        stage_file(&mut service, &dir, "hello.txt", b"hello");
        service
            .handle(Envelope::ClientSend(ClientSend { dst: 1 }))
            .unwrap();
        let id = service.transfers.values().next().unwrap().id;
        service
            .handle(Envelope::TransferRemove(TransferRemove {
                id,
                delete_files: false,
            }))
            .unwrap();
        let transfer = service.transfers.get(&id).unwrap();
        assert!(transfer.removed);
        assert!(
            service
                .drain_peer_outbox()
                .iter()
                .any(|message| matches!(message.message, PeerMessage::TransferControl(_)))
        );
    }
}
