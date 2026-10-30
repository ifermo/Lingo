//! SQLite persistence for devices, preferences and transfers.

use std::path::Path;

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::{
    AppState, Avatar, Device, DeviceId, FileState, Role, Transfer, TransferFile, TransferStatus,
};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Prefs {
    pub did: DeviceId,
    pub avatar: Avatar,
    pub api_ack: i64,
    pub init_at: i64,
    pub avatar_at: i64,
    pub push_token: String,
    pub push_token_at: i64,
    pub setup: bool,
    pub setup_account: bool,
    pub setup_available: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite error: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS devices (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    did BLOB NOT NULL,
    avatar JSON NULL,
    avatar_at INTEGER NOT NULL DEFAULT 0,
    active BOOL NOT NULL DEFAULT 0,
    active_at INTEGER NOT NULL DEFAULT 0,
    last_seen_at INTEGER NOT NULL DEFAULT 0,
    ack INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS devices_did_key ON devices (did);

CREATE TABLE IF NOT EXISTS prefs (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    did BLOB NOT NULL,
    avatar JSON NULL,
    api_ack INTEGER NOT NULL DEFAULT 0,
    init_at INTEGER NOT NULL DEFAULT 0,
    avatar_at INTEGER NOT NULL DEFAULT 0,
    push_token TEXT NULL,
    push_token_at INTEGER NOT NULL DEFAULT 0,
    setup BOOL NOT NULL DEFAULT 0,
    setup_account BOOL NOT NULL DEFAULT 0,
    setup_available BOOL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    tid UUID NOT NULL,
    role TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    manifest JSON NULL,
    path_map JSON NULL,
    note TEXT NULL,
    dir TEXT NULL,
    status TEXT NOT NULL DEFAULT 'paused',
    status_at INTEGER NOT NULL DEFAULT 0,
    peer_status TEXT NOT NULL DEFAULT 'paused',
    removed BOOL NOT NULL DEFAULT 0,
    deleted BOOL NOT NULL DEFAULT 0,
    offset INTEGER NOT NULL DEFAULT 0,
    duration INTEGER NOT NULL DEFAULT 0,
    err TEXT NULL,
    peer_id INTEGER NOT NULL,
    CONSTRAINT transfers_devices_peer FOREIGN KEY (peer_id) REFERENCES devices (id) ON DELETE NO ACTION
);
CREATE UNIQUE INDEX IF NOT EXISTS transfers_tid_key ON transfers (tid);

CREATE TABLE IF NOT EXISTS transfer_files (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    transfer_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    final_path TEXT NULL,
    size INTEGER NOT NULL,
    offset INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending',
    sha256 TEXT NULL,
    UNIQUE (transfer_id, path),
    CONSTRAINT transfer_files_transfer FOREIGN KEY (transfer_id) REFERENCES transfers (id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS transfer_files_transfer_idx ON transfer_files (transfer_id);
"#;

pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;",
        )?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<(), StoreError> {
        self.conn.execute_batch(SCHEMA)?;
        Ok(())
    }

    pub fn load_prefs(&self) -> Result<Option<Prefs>, StoreError> {
        let prefs = self
            .conn
            .query_row(
                "SELECT did, avatar, api_ack, init_at, avatar_at, push_token, push_token_at,
                        setup, setup_account, setup_available
                 FROM prefs WHERE id = 1",
                [],
                |row| Ok(Self::row_to_prefs(row)),
            )
            .optional()?;
        match prefs {
            Some(prefs) => Ok(Some(prefs?)),
            None => Ok(None),
        }
    }

    fn row_to_prefs(row: &Row<'_>) -> rusqlite::Result<Prefs> {
        let did: Vec<u8> = row.get(0)?;
        let did = device_id_from_blob(&did).map_err(|_| rusqlite::Error::InvalidQuery)?;
        let avatar: Option<String> = row.get(1)?;
        Ok(Prefs {
            did,
            avatar: parse_json_or_default(avatar.as_deref()),
            api_ack: row.get(2)?,
            init_at: row.get(3)?,
            avatar_at: row.get(4)?,
            push_token: row.get::<_, Option<String>>(5)?.unwrap_or_default(),
            push_token_at: row.get(6)?,
            setup: row.get::<_, i64>(7)? != 0,
            setup_account: row.get::<_, i64>(8)? != 0,
            setup_available: row.get::<_, i64>(9)? != 0,
        })
    }

    pub fn save_prefs(&self, prefs: &Prefs) -> Result<(), StoreError> {
        let avatar = serde_json::to_string(&prefs.avatar)?;
        let did = prefs
            .did
            .to_bytes()
            .map_err(|_| StoreError::Sql(rusqlite::Error::InvalidQuery))?;
        self.conn.execute(
            "INSERT INTO prefs (id, did, avatar, api_ack, init_at, avatar_at, push_token,
                                push_token_at, setup, setup_account, setup_available)
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(id) DO UPDATE SET
                did = excluded.did,
                avatar = excluded.avatar,
                api_ack = excluded.api_ack,
                init_at = excluded.init_at,
                avatar_at = excluded.avatar_at,
                push_token = excluded.push_token,
                push_token_at = excluded.push_token_at,
                setup = excluded.setup,
                setup_account = excluded.setup_account,
                setup_available = excluded.setup_available",
            params![
                did.as_slice(),
                avatar,
                prefs.api_ack,
                prefs.init_at,
                prefs.avatar_at,
                prefs.push_token,
                prefs.push_token_at,
                prefs.setup as i64,
                prefs.setup_account as i64,
                prefs.setup_available as i64,
            ],
        )?;
        Ok(())
    }

    pub fn prefs_from_state(state: &AppState) -> Prefs {
        Prefs {
            did: state.did.clone(),
            avatar: state.avatar.clone(),
            api_ack: 0,
            init_at: 0,
            avatar_at: 0,
            push_token: state.push_token.clone(),
            push_token_at: 0,
            setup: state.setup,
            setup_account: state.setup_account,
            setup_available: state.setup_available,
        }
    }

    pub fn upsert_device(&self, device: &Device) -> Result<i64, StoreError> {
        let did = device
            .did
            .to_bytes()
            .map_err(|_| StoreError::Sql(rusqlite::Error::InvalidQuery))?;
        let avatar = serde_json::to_string(&device.avatar)?;
        let id = self.conn.query_row(
            "INSERT INTO devices (did, avatar, avatar_at, active, active_at, last_seen_at, ack)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(did) DO UPDATE SET
                avatar = excluded.avatar,
                active = excluded.active,
                active_at = excluded.active_at,
                last_seen_at = excluded.last_seen_at,
                ack = excluded.ack
             RETURNING id",
            params![
                did.as_slice(),
                avatar,
                device.active_at,
                device.active as i64,
                device.active_at,
                device.last_seen_at,
                0i64,
            ],
            |row| row.get(0),
        )?;
        Ok(id)
    }

    pub fn list_devices(&self) -> Result<Vec<Device>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, did, avatar, active, active_at, last_seen_at
             FROM devices ORDER BY last_seen_at DESC, id DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            let did: Vec<u8> = row.get(1)?;
            let did = device_id_from_blob(&did).map_err(|_| rusqlite::Error::InvalidQuery)?;
            let avatar: Option<String> = row.get(2)?;
            Ok(Device {
                id: row.get(0)?,
                did,
                avatar: parse_json_or_default(avatar.as_deref()),
                active: row.get::<_, i64>(3)? != 0,
                active_at: row.get(4)?,
                last_seen_at: row.get(5)?,
                ..Device::default()
            })
        })?;
        let mut devices = Vec::new();
        for row in rows {
            devices.push(row?);
        }
        Ok(devices)
    }

    pub fn remove_device(&self, id: i64) -> Result<(), StoreError> {
        self.conn
            .execute("DELETE FROM devices WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn insert_transfer(&self, transfer: &Transfer) -> Result<i64, StoreError> {
        insert_transfer_row(&self.conn, transfer)
    }

    pub fn insert_transfer_with_files(
        &self,
        transfer: &Transfer,
        files: &[TransferFile],
    ) -> Result<i64, StoreError> {
        let transaction = self.conn.unchecked_transaction()?;
        let id = insert_transfer_row(&transaction, transfer)?;
        for file in files {
            insert_transfer_file_row(&transaction, id, file)?;
        }
        transaction.commit()?;
        Ok(id)
    }

    pub fn insert_transfer_files(
        &self,
        transfer_id: i64,
        files: &[TransferFile],
    ) -> Result<(), StoreError> {
        let transaction = self.conn.unchecked_transaction()?;
        for file in files {
            insert_transfer_file_row(&transaction, transfer_id, file)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn update_transfer(&self, transfer: &Transfer) -> Result<(), StoreError> {
        let manifest = serde_json::to_string(&transfer.manifest)?;
        let path_map = serde_json::to_string(&transfer.path_map)?;
        self.conn.execute(
            "UPDATE transfers SET
                manifest = ?2, path_map = ?3, note = ?4, dir = ?5, status = ?6,
                status_at = ?7, peer_status = ?8, removed = ?9, deleted = ?10,
                offset = ?11, duration = ?12, err = ?13, peer_id = ?14
             WHERE id = ?1",
            params![
                transfer.id,
                manifest,
                path_map,
                transfer.note,
                transfer.dir,
                status_str(transfer.status),
                transfer.status_at,
                status_str(transfer.peer_status),
                transfer.removed as i64,
                transfer.deleted as i64,
                transfer.offset as i64,
                transfer.duration,
                transfer.err,
                transfer.peer_id,
            ],
        )?;
        Ok(())
    }

    pub fn get_transfer(&self, id: i64) -> Result<Option<Transfer>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT id, tid, role, created_at, status_at, manifest, note, removed, deleted,
                        offset, peer_id, path_map, dir, err, status, peer_status, duration
                 FROM transfers WHERE id = ?1",
                params![id],
                Self::row_to_transfer,
            )
            .optional()?)
    }

    pub fn list_transfers(&self) -> Result<Vec<Transfer>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, tid, role, created_at, status_at, manifest, note, removed, deleted,
                    offset, peer_id, path_map, dir, err, status, peer_status, duration
             FROM transfers ORDER BY created_at DESC, id DESC",
        )?;
        let rows = stmt.query_map([], Self::row_to_transfer)?;
        let mut transfers = Vec::new();
        for row in rows {
            transfers.push(row?);
        }
        Ok(transfers)
    }

    pub fn get_transfer_by_tid(&self, tid: Uuid) -> Result<Option<Transfer>, StoreError> {
        Ok(self
            .conn
            .query_row(
                "SELECT id, tid, role, created_at, status_at, manifest, note, removed, deleted,
                        offset, peer_id, path_map, dir, err, status, peer_status, duration
                 FROM transfers WHERE tid = ?1",
                params![tid.to_string()],
                Self::row_to_transfer,
            )
            .optional()?)
    }

    pub fn list_transfer_files(&self, transfer_id: i64) -> Result<Vec<TransferFile>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, transfer_id, path, final_path, size, offset, state, sha256
             FROM transfer_files WHERE transfer_id = ?1 ORDER BY id",
        )?;
        let rows = stmt.query_map(params![transfer_id], Self::row_to_transfer_file)?;
        let mut files = Vec::new();
        for row in rows {
            files.push(row?);
        }
        Ok(files)
    }

    pub fn list_all_transfer_files(&self) -> Result<Vec<TransferFile>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, transfer_id, path, final_path, size, offset, state, sha256
             FROM transfer_files ORDER BY transfer_id, id",
        )?;
        let rows = stmt.query_map([], Self::row_to_transfer_file)?;
        let mut files = Vec::new();
        for row in rows {
            files.push(row?);
        }
        Ok(files)
    }

    pub fn update_transfer_file(&self, file: &TransferFile) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE transfer_files SET final_path = ?2, offset = ?3, state = ?4, sha256 = ?5
             WHERE id = ?1",
            params![
                file.id,
                file.final_path,
                file.offset as i64,
                file_state_str(file.state),
                file.sha256,
            ],
        )?;
        Ok(())
    }

    fn row_to_transfer(row: &Row<'_>) -> rusqlite::Result<Transfer> {
        let tid: String = row.get(1)?;
        let role: String = row.get(2)?;
        let manifest: Option<String> = row.get(5)?;
        let path_map: Option<String> = row.get(11)?;
        let status: String = row.get(14)?;
        let peer_status: String = row.get(15)?;
        Ok(Transfer {
            id: row.get(0)?,
            tid: Uuid::parse_str(&tid).unwrap_or_else(|_| Uuid::nil()),
            role: role.parse().unwrap_or(Role::Src),
            created_at: row.get(3)?,
            status_at: row.get(4)?,
            manifest: parse_json_or_default(manifest.as_deref()),
            note: row.get::<_, Option<String>>(6)?.unwrap_or_default(),
            removed: row.get::<_, i64>(7)? != 0,
            deleted: row.get::<_, i64>(8)? != 0,
            offset: row.get::<_, i64>(9)?.max(0) as u64,
            peer_id: row.get(10)?,
            path_map: parse_json_or_default(path_map.as_deref()),
            dir: row.get(12)?,
            err: row.get(13)?,
            status: status.parse().unwrap_or(TransferStatus::Paused),
            peer_status: peer_status.parse().unwrap_or(TransferStatus::Paused),
            duration: row.get(16)?,
            speed: None,
        })
    }

    fn row_to_transfer_file(row: &Row<'_>) -> rusqlite::Result<TransferFile> {
        let state: String = row.get(6)?;
        Ok(TransferFile {
            id: row.get(0)?,
            transfer_id: row.get(1)?,
            path: row.get(2)?,
            final_path: row.get(3)?,
            size: row.get::<_, i64>(4)?.max(0) as u64,
            offset: row.get::<_, i64>(5)?.max(0) as u64,
            state: state.parse().unwrap_or(FileState::Pending),
            sha256: row.get(7)?,
        })
    }
}

fn insert_transfer_row(conn: &Connection, transfer: &Transfer) -> Result<i64, StoreError> {
    let manifest = serde_json::to_string(&transfer.manifest)?;
    let path_map = serde_json::to_string(&transfer.path_map)?;
    let status = status_str(transfer.status);
    let peer_status = status_str(transfer.peer_status);
    let role = role_str(transfer.role);
    let id = conn.query_row(
        "INSERT INTO transfers (tid, role, created_at, manifest, path_map, note, dir,
                                status, status_at, peer_status, removed, deleted, offset,
                                duration, err, peer_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
         RETURNING id",
        params![
            transfer.tid.to_string(),
            role,
            transfer.created_at,
            manifest,
            path_map,
            transfer.note,
            transfer.dir,
            status,
            transfer.status_at,
            peer_status,
            transfer.removed as i64,
            transfer.deleted as i64,
            transfer.offset as i64,
            transfer.duration,
            transfer.err,
            transfer.peer_id,
        ],
        |row| row.get(0),
    )?;
    Ok(id)
}

fn insert_transfer_file_row(
    conn: &Connection,
    transfer_id: i64,
    file: &TransferFile,
) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO transfer_files (transfer_id, path, final_path, size, offset, state, sha256)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            transfer_id,
            file.path,
            file.final_path,
            file.size as i64,
            file.offset as i64,
            file_state_str(file.state),
            file.sha256,
        ],
    )?;
    Ok(())
}

fn device_id_from_blob(blob: &[u8]) -> Result<DeviceId, ()> {
    let bytes: [u8; 32] = blob.try_into().map_err(|_| ())?;
    Ok(DeviceId::from_public_key(&bytes))
}

fn parse_json_or_default<T: serde::de::DeserializeOwned + Default>(value: Option<&str>) -> T {
    value
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or_default()
}

fn status_str(status: TransferStatus) -> &'static str {
    match status {
        TransferStatus::Paused => "paused",
        TransferStatus::Pending => "pending",
        TransferStatus::Ready => "ready",
        TransferStatus::Complete => "complete",
        TransferStatus::Canceled => "canceled",
        TransferStatus::Failed => "failed",
    }
}

fn file_state_str(state: FileState) -> &'static str {
    match state {
        FileState::Pending => "pending",
        FileState::Transferring => "transferring",
        FileState::Complete => "complete",
        FileState::Failed => "failed",
    }
}

fn role_str(role: Role) -> &'static str {
    match role {
        Role::Src => "src",
        Role::Dst => "dst",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ManifestEntry, PathMap, now_micros};

    fn device(id: i64) -> Device {
        Device {
            id,
            did: DeviceId::from_public_key(&[id as u8; 32]),
            avatar: Avatar {
                owner: "Ken".into(),
                name: "Macbook".into(),
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
    fn prefs_round_trip() {
        let store = Store::open_in_memory().unwrap();
        assert!(store.load_prefs().unwrap().is_none());
        let prefs = Prefs {
            did: DeviceId::from_public_key(&[1u8; 32]),
            avatar: Avatar {
                owner: "Ken".into(),
                name: "Macbook".into(),
                ..Avatar::default()
            },
            setup: true,
            ..Prefs::default()
        };
        store.save_prefs(&prefs).unwrap();
        let loaded = store.load_prefs().unwrap().unwrap();
        assert_eq!(loaded, prefs);
    }

    #[test]
    fn devices_are_upserted_by_did() {
        let store = Store::open_in_memory().unwrap();
        let mut dev = device(1);
        let id = store.upsert_device(&dev).unwrap();
        dev.id = id;
        dev.avatar.name = "Studio".into();
        let same_id = store.upsert_device(&dev).unwrap();
        assert_eq!(id, same_id);

        let devices = store.list_devices().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].avatar.name, "Studio");
    }

    #[test]
    fn transfers_round_trip_and_update() {
        let store = Store::open_in_memory().unwrap();
        let device_id = store.upsert_device(&device(1)).unwrap();
        let transfer = Transfer {
            id: 0,
            tid: Uuid::new_v4(),
            role: Role::Src,
            created_at: now_micros(),
            status_at: now_micros(),
            manifest: vec![ManifestEntry {
                path: "hello.txt".into(),
                size: 5,
            }],
            note: "hi".into(),
            removed: false,
            deleted: false,
            offset: 0,
            peer_id: device_id,
            path_map: PathMap::new(),
            dir: None,
            err: None,
            status: TransferStatus::Ready,
            peer_status: TransferStatus::Paused,
            duration: 0,
            speed: None,
        };
        let id = store.insert_transfer(&transfer).unwrap();
        let mut loaded = store.get_transfer(id).unwrap().unwrap();
        assert_eq!(loaded.status, TransferStatus::Ready);
        assert_eq!(loaded.manifest[0].path, "hello.txt");

        loaded.offset = 5;
        loaded.status = TransferStatus::Complete;
        store.update_transfer(&loaded).unwrap();
        let updated = store.get_transfer(id).unwrap().unwrap();
        assert_eq!(updated.offset, 5);
        assert_eq!(updated.status, TransferStatus::Complete);
        assert_eq!(store.list_transfers().unwrap().len(), 1);
    }

    #[test]
    fn transfer_files_round_trip_and_update() {
        let store = Store::open_in_memory().unwrap();
        let device_id = store.upsert_device(&device(1)).unwrap();
        let transfer = Transfer {
            id: 0,
            tid: Uuid::new_v4(),
            role: Role::Dst,
            created_at: now_micros(),
            status_at: now_micros(),
            manifest: vec![
                ManifestEntry {
                    path: "a.txt".into(),
                    size: 5,
                },
                ManifestEntry {
                    path: "b.txt".into(),
                    size: 0,
                },
            ],
            note: "hi".into(),
            removed: false,
            deleted: false,
            offset: 0,
            peer_id: device_id,
            path_map: PathMap::new(),
            dir: None,
            err: None,
            status: TransferStatus::Pending,
            peer_status: TransferStatus::Pending,
            duration: 0,
            speed: None,
        };
        let files = vec![
            TransferFile::pending("a.txt", 5),
            TransferFile::pending("b.txt", 0),
        ];
        let id = store.insert_transfer_with_files(&transfer, &files).unwrap();
        assert_eq!(
            store.get_transfer_by_tid(transfer.tid).unwrap().unwrap().id,
            id
        );

        let mut loaded = store.list_transfer_files(id).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(store.list_all_transfer_files().unwrap().len(), 2);
        assert_eq!(loaded[0].state, FileState::Pending);

        loaded[0].offset = 3;
        loaded[0].state = FileState::Transferring;
        loaded[0].final_path = Some("/tmp/a.txt".into());
        loaded[0].sha256 = Some("abc".into());
        store.update_transfer_file(&loaded[0]).unwrap();

        let updated = store.list_transfer_files(id).unwrap();
        assert_eq!(updated[0].offset, 3);
        assert_eq!(updated[0].state, FileState::Transferring);
        assert_eq!(updated[0].final_path.as_deref(), Some("/tmp/a.txt"));
        assert_eq!(updated[0].sha256.as_deref(), Some("abc"));
    }
}
