//! Per-file data plane over authenticated QUIC streams.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use quinn::{Connection, RecvStream, SendStream};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use uuid::Uuid;

use lingo_core::model::{FileState, Transfer, TransferFile};
use lingo_core::msg::{
    FileAccept, FileDone, FileError, FileStart, FileStreamMessage, PeerMessage, TransferEvent,
    TransferFailed, TransferFileEvent, TransferFileFailure, TransferProgressEvent, TransferTid,
};
use lingo_core::paths;

use crate::control::{ControlError, ControlHandle};
use crate::framing::{FrameError, read_frame_limited, write_frame_limited};

const HEADER_LIMIT: usize = 64 * 1024;
const DATA_TAG: u8 = 0;
const END_TAG: u8 = 1;
const PAUSE_TAG: u8 = 2;
const ERROR_TAG: u8 = 3;
const PREFIX_BUFFER: usize = 64 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct DataConfig {
    pub chunk_bytes: usize,
    pub max_parallel_files: usize,
    pub progress_interval: Duration,
    pub accept_timeout: Duration,
    pub read_timeout: Duration,
    pub chunk_delay: Option<Duration>,
}

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            chunk_bytes: 256 * 1024,
            max_parallel_files: 4,
            progress_interval: Duration::from_millis(200),
            accept_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(30),
            chunk_delay: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunState {
    Running,
    Paused,
    Canceled,
}

#[derive(Clone)]
pub struct RunHandle {
    tx: watch::Sender<RunState>,
}

#[derive(Clone)]
pub struct RunControl {
    rx: watch::Receiver<RunState>,
}

impl RunHandle {
    pub fn new() -> (Self, RunControl) {
        let (tx, rx) = watch::channel(RunState::Running);
        (Self { tx }, RunControl { rx })
    }

    pub fn pause(&self) {
        let _ = self.tx.send(RunState::Paused);
    }

    pub fn resume(&self) {
        let _ = self.tx.send(RunState::Running);
    }

    pub fn cancel(&self) {
        let _ = self.tx.send(RunState::Canceled);
    }

    pub fn state(&self) -> RunState {
        *self.tx.borrow()
    }
}

impl RunControl {
    pub fn state(&self) -> RunState {
        *self.rx.borrow()
    }

    pub async fn changed(&mut self) -> RunState {
        if self.rx.changed().await.is_err() {
            return RunState::Canceled;
        }
        *self.rx.borrow()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunOutcome {
    Complete,
    Paused,
    Canceled,
}

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame error: {0}")]
    Frame(#[from] FrameError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("path error: {0}")]
    Path(#[from] paths::PathError),
    #[error("control error: {0}")]
    Control(#[from] ControlError),
    #[error("source file is missing: {0}")]
    SourceMissing(PathBuf),
    #[error("source file changed: {0}")]
    SourceChanged(PathBuf),
    #[error("hash mismatch for {0}")]
    HashMismatch(String),
    #[error("peer error: {0}")]
    Peer(String),
    #[error("invalid final path: {0}")]
    InvalidFinalPath(String),
    #[error("destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("timed out waiting for the peer")]
    Timeout,
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FileOutcome {
    Complete,
    Paused,
    Canceled,
}

struct SourceFile {
    manifest_path: String,
    path: PathBuf,
    size: u64,
}

pub async fn send_transfer(
    connection: &Connection,
    control: &ControlHandle,
    transfer: &Transfer,
    progress: &mpsc::Sender<TransferEvent>,
    run: RunControl,
    config: &DataConfig,
) -> Result<RunOutcome, TransferError> {
    let result = send_transfer_inner(connection, control, transfer, progress, run, config).await;
    if let Err(err) = &result {
        let message = err.to_string();
        let _ = control
            .send(PeerMessage::TransferFailed(TransferFailed {
                tid: transfer.tid,
                err: message.clone(),
            }))
            .await;
        let _ = progress
            .send(TransferEvent::TransferFailed(TransferFailed {
                tid: transfer.tid,
                err: message,
            }))
            .await;
    }
    result
}

async fn send_transfer_inner(
    connection: &Connection,
    control: &ControlHandle,
    transfer: &Transfer,
    progress: &mpsc::Sender<TransferEvent>,
    run: RunControl,
    config: &DataConfig,
) -> Result<RunOutcome, TransferError> {
    let sources = resolve_sources(transfer)?;
    let semaphore = Arc::new(Semaphore::new(config.max_parallel_files.max(1)));
    let mut tasks = JoinSet::new();

    for source in sources {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TransferError::Internal("file limiter is closed".into()))?;
        let connection = connection.clone();
        let progress = progress.clone();
        let run = run.clone();
        let config = *config;
        let tid = transfer.tid;
        tasks.spawn(async move {
            let _permit = permit;
            send_one_file(&connection, tid, source, &progress, run, &config).await
        });
    }

    let mut outcome = RunOutcome::Complete;
    while let Some(result) = tasks.join_next().await {
        let result = match result {
            Ok(result) => result,
            Err(err) => {
                tasks.abort_all();
                return Err(TransferError::Internal(err.to_string()));
            }
        };
        match result {
            Ok(FileOutcome::Complete) => {}
            Ok(FileOutcome::Paused) => {
                outcome = RunOutcome::Paused;
                tasks.abort_all();
                break;
            }
            Ok(FileOutcome::Canceled) => {
                outcome = RunOutcome::Canceled;
                tasks.abort_all();
                break;
            }
            Err(err) => {
                tasks.abort_all();
                return Err(err);
            }
        }
    }

    match outcome {
        RunOutcome::Complete => {
            let _ = control
                .send(PeerMessage::TransferComplete(TransferTid {
                    tid: transfer.tid,
                }))
                .await;
            let _ = progress
                .send(TransferEvent::TransferComplete(TransferTid {
                    tid: transfer.tid,
                }))
                .await;
        }
        RunOutcome::Paused => {
            let _ = progress
                .send(TransferEvent::TransferPaused(TransferTid {
                    tid: transfer.tid,
                }))
                .await;
        }
        RunOutcome::Canceled => {
            let _ = progress
                .send(TransferEvent::TransferCanceled(TransferTid {
                    tid: transfer.tid,
                }))
                .await;
        }
    }
    Ok(outcome)
}

fn resolve_sources(transfer: &Transfer) -> Result<Vec<SourceFile>, TransferError> {
    let mut sources = Vec::with_capacity(transfer.manifest.len());
    for entry in &transfer.manifest {
        let path = paths::resolve_source(&transfer.path_map, &entry.path)?;
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|_| TransferError::SourceMissing(path.clone()))?;
        if !metadata.file_type().is_file() {
            return Err(TransferError::SourceMissing(path));
        }
        if metadata.len() != entry.size {
            return Err(TransferError::SourceChanged(path));
        }
        sources.push(SourceFile {
            manifest_path: entry.path.clone(),
            path,
            size: entry.size,
        });
    }
    Ok(sources)
}

async fn send_one_file(
    connection: &Connection,
    tid: Uuid,
    source: SourceFile,
    progress: &mpsc::Sender<TransferEvent>,
    run: RunControl,
    config: &DataConfig,
) -> Result<FileOutcome, TransferError> {
    let manifest_path = source.manifest_path.clone();
    let result = send_one_file_inner(connection, tid, source, progress, run, config).await;
    if let Err(err) = &result {
        let _ = progress
            .send(TransferEvent::FileFailed(TransferFileFailure {
                tid,
                path: manifest_path,
                err: err.to_string(),
            }))
            .await;
    }
    result
}

async fn send_one_file_inner(
    connection: &Connection,
    tid: Uuid,
    source: SourceFile,
    progress: &mpsc::Sender<TransferEvent>,
    run: RunControl,
    config: &DataConfig,
) -> Result<FileOutcome, TransferError> {
    let (mut send, mut recv) = match connection.open_bi().await {
        Ok(stream) => stream,
        Err(err) => {
            if let Some(outcome) = halted_outcome(&run) {
                return Ok(outcome);
            }
            return Err(err.into());
        }
    };
    if let Err(err) = write_header(
        &mut send,
        &FileStreamMessage::FileStart(FileStart {
            tid,
            path: source.manifest_path.clone(),
            size: source.size,
            offset: 0,
        }),
    )
    .await
    {
        if let Some(outcome) = halted_outcome(&run) {
            return Ok(outcome);
        }
        return Err(err);
    }

    let response: FileStreamMessage =
        match read_header_timeout(&mut recv, config.read_timeout).await {
            Ok(response) => response,
            Err(err) => {
                if let Some(outcome) = halted_outcome(&run) {
                    return Ok(outcome);
                }
                return Err(err);
            }
        };
    let FileStreamMessage::FileAccept(accept) = response else {
        return Err(TransferError::Peer("expected FileAccept".into()));
    };
    if accept.offset > source.size {
        return Err(TransferError::Peer("offset is larger than the file".into()));
    }

    let mut file = File::open(&source.path)
        .await
        .map_err(|_| TransferError::SourceMissing(source.path.clone()))?;
    let metadata = file.metadata().await?;
    if metadata.len() != source.size {
        return Err(TransferError::SourceChanged(source.path.clone()));
    }
    let initial_modified = metadata.modified().ok();

    let mut hasher = Sha256::new();
    hash_prefix(&mut file, accept.offset, &mut hasher).await?;
    let mut offset = accept.offset;
    let mut meter = ProgressMeter::new(offset, config.progress_interval);
    send_progress(progress, tid, &source.manifest_path, offset, 0);

    let chunk_bytes = config.chunk_bytes.max(1);
    let mut buffer = vec![0u8; chunk_bytes];
    while offset < source.size {
        match run.state() {
            RunState::Paused => {
                let _ = write_pause(&mut send, offset, config).await;
                send_progress(
                    progress,
                    tid,
                    &source.manifest_path,
                    offset,
                    meter.last_speed(),
                );
                return Ok(FileOutcome::Paused);
            }
            RunState::Canceled => {
                let _ = send.reset(0u32.into());
                return Ok(FileOutcome::Canceled);
            }
            RunState::Running => {}
        }

        let remaining = (source.size - offset) as usize;
        let limit = remaining.min(buffer.len());
        let read = file.read(&mut buffer[..limit]).await?;
        if read == 0 {
            return Err(TransferError::SourceChanged(source.path.clone()));
        }
        hasher.update(&buffer[..read]);
        if let Err(err) = write_data(&mut send, &buffer[..read], config).await {
            if let Some(outcome) = halted_outcome(&run) {
                return Ok(outcome);
            }
            return Err(err);
        }
        offset += read as u64;
        if let Some(speed) = meter.sample(offset) {
            send_progress(progress, tid, &source.manifest_path, offset, speed);
        }
        if let Some(delay) = config.chunk_delay {
            tokio::time::sleep(delay).await;
        }
    }

    let after = file.metadata().await?;
    if after.len() != source.size || after.modified().ok() != initial_modified {
        let _ = write_binary(&mut send, ERROR_TAG, b"source file changed", config).await;
        return Err(TransferError::SourceChanged(source.path.clone()));
    }

    let digest = finish_digest(hasher);
    if let Err(err) = write_binary(&mut send, END_TAG, digest.as_bytes(), config).await {
        if let Some(outcome) = halted_outcome(&run) {
            return Ok(outcome);
        }
        return Err(err);
    }
    if let Err(err) = send.flush().await {
        if let Some(outcome) = halted_outcome(&run) {
            return Ok(outcome);
        }
        return Err(err.into());
    }

    let response: FileStreamMessage =
        match read_header_timeout(&mut recv, config.accept_timeout).await {
            Ok(response) => response,
            Err(err) => {
                if let Some(outcome) = halted_outcome(&run) {
                    return Ok(outcome);
                }
                return Err(err);
            }
        };
    match response {
        FileStreamMessage::FileDone(done) if done.ok => {
            let _ = progress
                .send(TransferEvent::FileComplete(TransferFileEvent {
                    tid,
                    path: source.manifest_path,
                    offset: source.size,
                    sha256: digest,
                }))
                .await;
            Ok(FileOutcome::Complete)
        }
        FileStreamMessage::FileDone(done) => Err(TransferError::Peer(
            done.err
                .unwrap_or_else(|| "receiver rejected the file".into()),
        )),
        FileStreamMessage::FileError(error) => Err(TransferError::Peer(error.err)),
        _ => Err(TransferError::Peer("expected FileDone".into())),
    }
}

pub async fn receive_transfer(
    connection: &Connection,
    control: &ControlHandle,
    transfer: &Transfer,
    files: &[TransferFile],
    progress: &mpsc::Sender<TransferEvent>,
    run: RunControl,
    config: &DataConfig,
) -> Result<RunOutcome, TransferError> {
    let result =
        receive_transfer_inner(connection, control, transfer, files, progress, run, config).await;
    if let Err(err) = &result {
        let message = err.to_string();
        let _ = control
            .send(PeerMessage::TransferFailed(TransferFailed {
                tid: transfer.tid,
                err: message.clone(),
            }))
            .await;
        let _ = progress
            .send(TransferEvent::TransferFailed(TransferFailed {
                tid: transfer.tid,
                err: message,
            }))
            .await;
    }
    result
}

async fn receive_transfer_inner(
    connection: &Connection,
    control: &ControlHandle,
    transfer: &Transfer,
    files: &[TransferFile],
    progress: &mpsc::Sender<TransferEvent>,
    mut run: RunControl,
    config: &DataConfig,
) -> Result<RunOutcome, TransferError> {
    let root = transfer
        .dir
        .as_deref()
        .ok_or_else(|| TransferError::InvalidFinalPath("destination is missing".into()))?;
    let root = tokio::fs::canonicalize(root).await?;
    paths::ensure_within_root(&root, &root)?;

    let expected = transfer.manifest.len();
    let mut tasks = JoinSet::new();
    let mut seen = BTreeSet::new();
    let mut accepted = 0usize;

    while accepted < expected {
        if run.state() != RunState::Running {
            tasks.abort_all();
            return Ok(outcome_for(run.state()));
        }

        enum Next {
            State(RunState),
            Stream(
                Result<
                    Result<(SendStream, RecvStream), quinn::ConnectionError>,
                    tokio::time::error::Elapsed,
                >,
            ),
        }

        let next = tokio::select! {
            state = run.changed() => Next::State(state),
            stream = tokio::time::timeout(config.accept_timeout, connection.accept_bi()) => Next::Stream(stream),
        };

        let (mut send, mut recv) = match next {
            Next::State(RunState::Running) => continue,
            Next::State(state) => {
                tasks.abort_all();
                return Ok(outcome_for(state));
            }
            Next::Stream(Ok(Ok(stream))) => stream,
            Next::Stream(Ok(Err(err))) => {
                tasks.abort_all();
                return Err(err.into());
            }
            Next::Stream(Err(_)) => {
                tasks.abort_all();
                return Err(TransferError::Timeout);
            }
        };

        let header: FileStreamMessage = read_header_timeout(&mut recv, config.read_timeout).await?;
        let FileStreamMessage::FileStart(start) = header else {
            let message = "expected FileStart";
            let _ = write_header(
                &mut send,
                &FileStreamMessage::FileError(FileError {
                    err: message.into(),
                }),
            )
            .await;
            tasks.abort_all();
            return Err(TransferError::Peer(message.into()));
        };

        let rejection = if start.tid != transfer.tid {
            Some("transfer id does not match")
        } else if !seen.insert(start.path.clone()) {
            Some("duplicate file stream")
        } else if start.offset > start.size {
            Some("offset is larger than the file")
        } else {
            None
        };
        if let Some(reason) = rejection {
            let _ = write_header(
                &mut send,
                &FileStreamMessage::FileError(FileError { err: reason.into() }),
            )
            .await;
            tasks.abort_all();
            return Err(TransferError::Peer(reason.into()));
        }

        let Some(index) = files.iter().position(|file| file.path == start.path) else {
            let _ = write_header(
                &mut send,
                &FileStreamMessage::FileError(FileError {
                    err: "file is not in the manifest".into(),
                }),
            )
            .await;
            tasks.abort_all();
            return Err(TransferError::Peer("file is not in the manifest".into()));
        };
        let file = files[index].clone();
        if start.size != file.size {
            let _ = write_header(
                &mut send,
                &FileStreamMessage::FileError(FileError {
                    err: "file size does not match the manifest".into(),
                }),
            )
            .await;
            tasks.abort_all();
            return Err(TransferError::Peer(
                "file size does not match the manifest".into(),
            ));
        }

        let root = root.clone();
        let progress = progress.clone();
        let run = run.clone();
        let config = *config;
        let tid = transfer.tid;
        tasks.spawn(async move {
            receive_one_file(
                &mut send, &mut recv, tid, start, file, index, &root, &progress, run, &config,
            )
            .await
        });
        accepted += 1;
    }

    let mut outcome = RunOutcome::Complete;
    while let Some(result) = tasks.join_next().await {
        let result = match result {
            Ok(result) => result,
            Err(err) => {
                tasks.abort_all();
                return Err(TransferError::Internal(err.to_string()));
            }
        };
        match result {
            Ok(FileOutcome::Complete) => {}
            Ok(FileOutcome::Paused) => {
                outcome = RunOutcome::Paused;
                tasks.abort_all();
                break;
            }
            Ok(FileOutcome::Canceled) => {
                outcome = RunOutcome::Canceled;
                tasks.abort_all();
                break;
            }
            Err(err) => {
                tasks.abort_all();
                return Err(err);
            }
        }
    }

    match outcome {
        RunOutcome::Complete => {
            let _ = control
                .send(PeerMessage::TransferComplete(TransferTid {
                    tid: transfer.tid,
                }))
                .await;
            let _ = paths::remove_temp_dir(&root, transfer.tid);
            let _ = progress
                .send(TransferEvent::TransferComplete(TransferTid {
                    tid: transfer.tid,
                }))
                .await;
        }
        RunOutcome::Paused => {
            let _ = progress
                .send(TransferEvent::TransferPaused(TransferTid {
                    tid: transfer.tid,
                }))
                .await;
        }
        RunOutcome::Canceled => {
            let _ = paths::remove_temp_dir(&root, transfer.tid);
            let _ = progress
                .send(TransferEvent::TransferCanceled(TransferTid {
                    tid: transfer.tid,
                }))
                .await;
        }
    }
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
async fn receive_one_file(
    send: &mut SendStream,
    recv: &mut RecvStream,
    tid: Uuid,
    start: FileStart,
    file: TransferFile,
    index: usize,
    root: &Path,
    progress: &mpsc::Sender<TransferEvent>,
    run: RunControl,
    config: &DataConfig,
) -> Result<FileOutcome, TransferError> {
    let result = receive_one_file_inner(
        send, recv, tid, &start, &file, index, root, progress, run, config,
    )
    .await;
    if let Err(err) = &result {
        let _ = write_header(
            send,
            &FileStreamMessage::FileDone(FileDone {
                ok: false,
                err: Some(err.to_string()),
            }),
        )
        .await;
        let _ = progress
            .send(TransferEvent::FileFailed(TransferFileFailure {
                tid,
                path: start.path.clone(),
                err: err.to_string(),
            }))
            .await;
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn receive_one_file_inner(
    send: &mut SendStream,
    recv: &mut RecvStream,
    tid: Uuid,
    start: &FileStart,
    file: &TransferFile,
    index: usize,
    root: &Path,
    progress: &mpsc::Sender<TransferEvent>,
    run: RunControl,
    config: &DataConfig,
) -> Result<FileOutcome, TransferError> {
    let final_path = validate_final_path(root, file)?;
    let final_exists = tokio::fs::symlink_metadata(&final_path).await.is_ok();

    if file.state == FileState::Complete && final_exists {
        write_header(
            send,
            &FileStreamMessage::FileAccept(FileAccept { offset: file.size }),
        )
        .await?;
        let (received_hash, outcome) = read_until_end(recv, run, config).await?;
        if outcome != FileOutcome::Complete {
            return Ok(outcome);
        }
        let expected = match file.sha256.as_deref() {
            Some(expected) => expected.to_string(),
            None => hash_file(&final_path).await?,
        };
        if !received_hash.eq_ignore_ascii_case(&expected) {
            return Err(TransferError::HashMismatch(start.path.clone()));
        }
        write_header(
            send,
            &FileStreamMessage::FileDone(FileDone {
                ok: true,
                err: None,
            }),
        )
        .await?;
        return Ok(FileOutcome::Complete);
    }

    if final_exists {
        return Err(TransferError::DestinationExists(final_path));
    }

    let temp_dir = paths::temp_dir(root, tid);
    paths::ensure_within_root(root, &temp_dir)?;
    tokio::fs::create_dir_all(&temp_dir).await?;
    paths::ensure_within_root(root, &temp_dir)?;
    let temp_path = paths::temp_file(root, tid, index);
    paths::ensure_within_root(root, &temp_path)?;
    if let Ok(metadata) = tokio::fs::symlink_metadata(&temp_path).await
        && metadata.file_type().is_symlink()
    {
        return Err(TransferError::InvalidFinalPath(
            temp_path.to_string_lossy().to_string(),
        ));
    }

    let mut temp = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&temp_path)
        .await?;
    let mut length = temp.metadata().await?.len();
    if length > file.size {
        temp.set_len(0).await?;
        length = 0;
    }
    let offset = length;
    let mut hasher = Sha256::new();
    hash_prefix(&mut temp, offset, &mut hasher).await?;
    temp.seek(std::io::SeekFrom::Start(offset)).await?;

    write_header(send, &FileStreamMessage::FileAccept(FileAccept { offset })).await?;

    let mut offset = offset;
    let mut meter = ProgressMeter::new(offset, config.progress_interval);
    send_progress(progress, tid, &start.path, offset, 0);
    let frame_limit = config.chunk_bytes.max(HEADER_LIMIT).saturating_add(1);

    loop {
        if run.state() != RunState::Running {
            temp.flush().await?;
            temp.sync_data().await?;
            return Ok(file_outcome_for(run.state()));
        }

        let frame = read_frame_timeout(recv, frame_limit, config.read_timeout).await?;
        let Some((&tag, payload)) = frame.split_first() else {
            return Err(TransferError::Peer("empty data frame".into()));
        };
        match tag {
            DATA_TAG => {
                temp.write_all(payload).await?;
                hasher.update(payload);
                offset += payload.len() as u64;
                if offset > file.size {
                    return Err(TransferError::Peer("received too many bytes".into()));
                }
                if let Some(speed) = meter.sample(offset) {
                    send_progress(progress, tid, &start.path, offset, speed);
                }
                if let Some(delay) = config.chunk_delay {
                    tokio::time::sleep(delay).await;
                }
            }
            END_TAG => {
                let received = std::str::from_utf8(payload)
                    .map_err(|_| TransferError::Peer("invalid digest".into()))?
                    .to_string();
                if offset != file.size {
                    return Err(TransferError::Peer("file ended before all bytes".into()));
                }
                let digest = finish_digest(hasher);
                if !received.eq_ignore_ascii_case(&digest) {
                    return Err(TransferError::HashMismatch(start.path.clone()));
                }
                temp.flush().await?;
                temp.sync_data().await?;
                drop(temp);

                if let Some(parent) = final_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                paths::ensure_within_root(root, &final_path)?;
                if tokio::fs::symlink_metadata(&final_path).await.is_ok() {
                    return Err(TransferError::DestinationExists(final_path));
                }
                tokio::fs::rename(&temp_path, &final_path).await?;
                let _ = tokio::fs::remove_dir(&temp_dir).await;

                write_header(
                    send,
                    &FileStreamMessage::FileDone(FileDone {
                        ok: true,
                        err: None,
                    }),
                )
                .await?;
                let _ = progress
                    .send(TransferEvent::FileComplete(TransferFileEvent {
                        tid,
                        path: start.path.clone(),
                        offset: file.size,
                        sha256: digest,
                    }))
                    .await;
                return Ok(FileOutcome::Complete);
            }
            PAUSE_TAG => {
                temp.flush().await?;
                temp.sync_data().await?;
                return Ok(FileOutcome::Paused);
            }
            ERROR_TAG => {
                temp.flush().await?;
                temp.sync_data().await?;
                let err = String::from_utf8_lossy(payload).to_string();
                return Err(TransferError::Peer(err));
            }
            _ => return Err(TransferError::Peer("unknown data frame".into())),
        }
    }
}

async fn read_until_end(
    recv: &mut RecvStream,
    run: RunControl,
    config: &DataConfig,
) -> Result<(String, FileOutcome), TransferError> {
    if run.state() != RunState::Running {
        return Ok((String::new(), file_outcome_for(run.state())));
    }
    let frame_limit = config.chunk_bytes.max(HEADER_LIMIT).saturating_add(1);
    let frame = read_frame_timeout(recv, frame_limit, config.read_timeout).await?;
    let Some((&tag, payload)) = frame.split_first() else {
        return Err(TransferError::Peer("empty data frame".into()));
    };
    match tag {
        DATA_TAG => Err(TransferError::Peer("data for a completed file".into())),
        END_TAG => {
            let digest = std::str::from_utf8(payload)
                .map_err(|_| TransferError::Peer("invalid digest".into()))?
                .to_string();
            Ok((digest, FileOutcome::Complete))
        }
        PAUSE_TAG => Ok((String::new(), FileOutcome::Paused)),
        ERROR_TAG => Err(TransferError::Peer(
            String::from_utf8_lossy(payload).to_string(),
        )),
        _ => Err(TransferError::Peer("unknown data frame".into())),
    }
}

fn validate_final_path(root: &Path, file: &TransferFile) -> Result<PathBuf, TransferError> {
    let Some(final_path) = file.final_path.as_deref() else {
        return Err(TransferError::InvalidFinalPath(file.path.clone()));
    };
    let path = PathBuf::from(final_path);
    if !path.starts_with(root) {
        return Err(TransferError::InvalidFinalPath(final_path.to_string()));
    }
    paths::ensure_within_root(root, &path)?;
    Ok(path)
}

async fn hash_file(path: &Path) -> Result<String, TransferError> {
    let mut file = File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; PREFIX_BUFFER];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(finish_digest(hasher))
}

async fn hash_prefix(
    file: &mut File,
    offset: u64,
    hasher: &mut Sha256,
) -> Result<(), TransferError> {
    file.seek(std::io::SeekFrom::Start(0)).await?;
    let mut remaining = offset;
    let mut buffer = vec![0u8; PREFIX_BUFFER];
    while remaining > 0 {
        let limit = (remaining as usize).min(buffer.len());
        let read = file.read(&mut buffer[..limit]).await?;
        if read == 0 {
            return Err(TransferError::Internal(
                "file ended while hashing its prefix".into(),
            ));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(())
}

fn halted_outcome(run: &RunControl) -> Option<FileOutcome> {
    match run.state() {
        RunState::Paused => Some(FileOutcome::Paused),
        RunState::Canceled => Some(FileOutcome::Canceled),
        RunState::Running => None,
    }
}

fn outcome_for(state: RunState) -> RunOutcome {
    match state {
        RunState::Paused => RunOutcome::Paused,
        RunState::Canceled => RunOutcome::Canceled,
        RunState::Running => RunOutcome::Complete,
    }
}

fn file_outcome_for(state: RunState) -> FileOutcome {
    match state {
        RunState::Paused => FileOutcome::Paused,
        RunState::Canceled => FileOutcome::Canceled,
        RunState::Running => FileOutcome::Complete,
    }
}

fn finish_digest(hasher: Sha256) -> String {
    format!("{:x}", hasher.finalize())
}

struct ProgressMeter {
    interval: Duration,
    last_at: Instant,
    last_offset: u64,
    last_speed: u64,
}

impl ProgressMeter {
    fn new(offset: u64, interval: Duration) -> Self {
        Self {
            interval,
            last_at: Instant::now(),
            last_offset: offset,
            last_speed: 0,
        }
    }

    fn last_speed(&self) -> u64 {
        self.last_speed
    }

    fn sample(&mut self, offset: u64) -> Option<u64> {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_at);
        if elapsed < self.interval {
            return None;
        }
        let delta = offset.saturating_sub(self.last_offset);
        let seconds = elapsed.as_secs_f64().max(0.001);
        let speed = (delta as f64 / seconds) as u64;
        self.last_at = now;
        self.last_offset = offset;
        self.last_speed = speed;
        Some(speed)
    }
}

fn send_progress(
    progress: &mpsc::Sender<TransferEvent>,
    tid: Uuid,
    path: &str,
    offset: u64,
    speed: u64,
) {
    let _ = progress.try_send(TransferEvent::Progress(TransferProgressEvent {
        tid,
        path: path.to_string(),
        offset,
        speed,
    }));
}

async fn write_header<T>(send: &mut SendStream, message: &T) -> Result<(), TransferError>
where
    T: Serialize,
{
    let payload = serde_json::to_vec(message)?;
    write_frame_limited(send, &payload, HEADER_LIMIT).await?;
    Ok(())
}

#[cfg(test)]
async fn read_header<T>(recv: &mut RecvStream) -> Result<T, TransferError>
where
    T: DeserializeOwned,
{
    let payload = read_frame_limited(recv, HEADER_LIMIT).await?;
    Ok(serde_json::from_slice(&payload)?)
}

async fn read_header_timeout<T>(
    recv: &mut RecvStream,
    timeout: Duration,
) -> Result<T, TransferError>
where
    T: DeserializeOwned,
{
    let payload = read_frame_timeout(recv, HEADER_LIMIT, timeout).await?;
    Ok(serde_json::from_slice(&payload)?)
}

async fn read_frame_timeout(
    recv: &mut RecvStream,
    limit: usize,
    timeout: Duration,
) -> Result<Vec<u8>, TransferError> {
    match tokio::time::timeout(timeout, read_frame_limited(recv, limit)).await {
        Ok(result) => Ok(result?),
        Err(_) => Err(TransferError::Timeout),
    }
}

async fn write_data(
    send: &mut SendStream,
    data: &[u8],
    config: &DataConfig,
) -> Result<(), TransferError> {
    write_binary(send, DATA_TAG, data, config).await
}

async fn write_pause(
    send: &mut SendStream,
    offset: u64,
    config: &DataConfig,
) -> Result<(), TransferError> {
    write_binary(send, PAUSE_TAG, &offset.to_be_bytes(), config).await
}

async fn write_binary(
    send: &mut SendStream,
    tag: u8,
    payload: &[u8],
    config: &DataConfig,
) -> Result<(), TransferError> {
    let mut frame = Vec::with_capacity(payload.len() + 1);
    frame.push(tag);
    frame.extend_from_slice(payload);
    let limit = config.chunk_bytes.max(HEADER_LIMIT).saturating_add(1);
    write_frame_limited(send, &frame, limit).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    use lingo_core::identity::Identity;
    use lingo_core::model::{ManifestEntry, Role, TransferFile, TransferStatus, now_micros};
    use lingo_core::msg::FileStreamMessage;

    use crate::control;
    use crate::endpoint::PeerEndpoint;
    use crate::session::{SessionConfig, accept, connect};

    struct Pair {
        _server_endpoint: PeerEndpoint,
        _client_endpoint: PeerEndpoint,
        server: ControlHandle,
        client: ControlHandle,
        _server_inbox: mpsc::Receiver<PeerMessage>,
        _client_inbox: mpsc::Receiver<PeerMessage>,
        _server_dir: tempfile::TempDir,
        _client_dir: tempfile::TempDir,
    }

    async fn pair() -> Pair {
        let server_dir = tempfile::tempdir().unwrap();
        let client_dir = tempfile::tempdir().unwrap();
        let server_identity = Identity::load_or_create(server_dir.path()).unwrap();
        let client_identity = Identity::load_or_create(client_dir.path()).unwrap();
        let server_did = server_identity.did();
        let client_did = client_identity.did();

        let server_endpoint =
            PeerEndpoint::bind(&server_identity, "127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = server_endpoint.local_addr();
        let expected_client = client_did.clone();
        let server_task = tokio::spawn(async move {
            let incoming = server_endpoint.accept().await.unwrap();
            let connection = incoming.await.unwrap();
            let config = SessionConfig {
                handshake_timeout: Duration::from_secs(2),
                ..SessionConfig::default()
            };
            let session = accept(
                &connection,
                &server_identity,
                Some(&expected_client),
                &config,
            )
            .await
            .unwrap();
            let (handle, inbox) = control::spawn(session);
            (server_endpoint, handle, inbox)
        });

        let client_endpoint =
            PeerEndpoint::bind(&client_identity, "127.0.0.1:0".parse().unwrap()).unwrap();
        let connection = client_endpoint.connect(addr).await.unwrap();
        let config = SessionConfig {
            handshake_timeout: Duration::from_secs(2),
            ..SessionConfig::default()
        };
        let session = connect(&connection, &client_identity, &server_did, &config)
            .await
            .unwrap();
        let (client, client_inbox) = control::spawn(session);
        let (server_endpoint, server, server_inbox) = server_task.await.unwrap();

        Pair {
            _server_endpoint: server_endpoint,
            _client_endpoint: client_endpoint,
            server,
            client,
            _server_inbox: server_inbox,
            _client_inbox: client_inbox,
            _server_dir: server_dir,
            _client_dir: client_dir,
        }
    }

    fn manifest(entries: &[(&str, u64)]) -> Vec<ManifestEntry> {
        entries
            .iter()
            .map(|(path, size)| ManifestEntry {
                path: (*path).to_string(),
                size: *size,
            })
            .collect()
    }

    fn make_transfer(
        tid: Uuid,
        role: Role,
        manifest: Vec<ManifestEntry>,
        path_map: BTreeMap<String, String>,
        dir: Option<String>,
    ) -> Transfer {
        Transfer {
            id: 1,
            tid,
            role,
            created_at: now_micros(),
            status_at: now_micros(),
            manifest,
            note: "test".into(),
            removed: false,
            deleted: false,
            offset: 0,
            peer_id: 1,
            path_map,
            dir,
            err: None,
            status: TransferStatus::Ready,
            peer_status: TransferStatus::Ready,
            duration: 0,
            speed: None,
        }
    }

    fn destination_files(root: &Path, manifest: &[ManifestEntry]) -> Vec<TransferFile> {
        let mut reserved = BTreeSet::new();
        manifest
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let final_path = paths::unique_path_avoiding(&root.join(&entry.path), &reserved);
                reserved.insert(final_path.clone());
                TransferFile {
                    id: index as i64 + 1,
                    transfer_id: 1,
                    path: entry.path.clone(),
                    final_path: Some(final_path.to_string_lossy().to_string()),
                    size: entry.size,
                    offset: 0,
                    state: FileState::Pending,
                    sha256: None,
                }
            })
            .collect()
    }

    fn test_config(chunk_bytes: usize, delay: Option<Duration>) -> DataConfig {
        DataConfig {
            chunk_bytes,
            max_parallel_files: 2,
            progress_interval: Duration::from_millis(5),
            accept_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            chunk_delay: delay,
        }
    }

    #[test]
    fn progress_meter_samples_speed() {
        let mut meter = ProgressMeter::new(0, Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(2));
        assert!(meter.sample(1024).is_some());
    }

    #[test]
    fn digest_changes_with_content() {
        let mut a = Sha256::new();
        a.update(b"a");
        let mut b = Sha256::new();
        b.update(b"b");
        assert_ne!(finish_digest(a), finish_digest(b));
    }

    #[test]
    fn rejects_final_paths_outside_the_root() {
        let root = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(root.path()).unwrap();
        let file = TransferFile {
            path: "escape.txt".into(),
            final_path: Some("/tmp/lingo-escape.txt".into()),
            size: 1,
            ..TransferFile::default()
        };
        assert!(matches!(
            validate_final_path(&root, &file),
            Err(TransferError::InvalidFinalPath(_))
        ));

        let outside = tempfile::tempdir().unwrap();
        let link = root.join("outside-link");
        std::os::unix::fs::symlink(outside.path(), &link).unwrap();
        let file = TransferFile {
            path: "linked.txt".into(),
            final_path: Some(link.join("linked.txt").to_string_lossy().to_string()),
            size: 1,
            ..TransferFile::default()
        };
        assert!(matches!(
            validate_final_path(&root, &file),
            Err(TransferError::Path(paths::PathError::Escape))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn transfers_a_directory_tree_with_empty_and_unicode_files() {
        let pair = pair().await;
        let source = tempfile::tempdir().unwrap();
        let source_folder = source.path().join("folder");
        std::fs::create_dir_all(&source_folder).unwrap();
        std::fs::write(source_folder.join("a.txt"), b"hello").unwrap();
        let payload: Vec<u8> = (0..200_000).map(|index| (index % 251) as u8).collect();
        std::fs::write(source_folder.join("b.bin"), &payload).unwrap();
        std::fs::write(source_folder.join("empty.txt"), b"").unwrap();
        std::fs::write(source_folder.join("照片.txt"), "你好".as_bytes()).unwrap();

        let entries = manifest(&[
            ("folder/a.txt", 5),
            ("folder/b.bin", payload.len() as u64),
            ("folder/empty.txt", 0),
            ("folder/照片.txt", "你好".len() as u64),
        ]);
        let tid = Uuid::new_v4();
        let path_map = BTreeMap::from([(
            "folder".to_string(),
            source_folder.to_string_lossy().to_string(),
        )]);
        let sender_transfer = make_transfer(tid, Role::Src, entries.clone(), path_map, None);

        let destination = tempfile::tempdir().unwrap();
        let destination_root = std::fs::canonicalize(destination.path()).unwrap();
        let receiver_transfer = make_transfer(
            tid,
            Role::Dst,
            entries.clone(),
            BTreeMap::new(),
            Some(destination_root.to_string_lossy().to_string()),
        );
        let files = destination_files(&destination_root, &entries);
        let config = test_config(32 * 1024, None);

        let (receiver_tx, mut receiver_rx) = mpsc::channel(1024);
        let (sender_tx, mut sender_rx) = mpsc::channel(1024);
        let (_receiver_run, receiver_run_control) = RunHandle::new();
        let (_sender_run, sender_run_control) = RunHandle::new();

        let receiver_connection = pair.server.connection.clone();
        let receiver_control = pair.server.clone();
        let receiver_task = tokio::spawn(async move {
            receive_transfer(
                &receiver_connection,
                &receiver_control,
                &receiver_transfer,
                &files,
                &receiver_tx,
                receiver_run_control,
                &config,
            )
            .await
        });
        let sender_connection = pair.client.connection.clone();
        let sender_control = pair.client.clone();
        let sender_task = tokio::spawn(async move {
            send_transfer(
                &sender_connection,
                &sender_control,
                &sender_transfer,
                &sender_tx,
                sender_run_control,
                &config,
            )
            .await
        });

        let receiver_outcome = tokio::time::timeout(Duration::from_secs(10), receiver_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let sender_outcome = tokio::time::timeout(Duration::from_secs(10), sender_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(receiver_outcome, RunOutcome::Complete);
        assert_eq!(sender_outcome, RunOutcome::Complete);

        assert_eq!(
            std::fs::read(destination_root.join("folder/a.txt")).unwrap(),
            b"hello"
        );
        assert_eq!(
            std::fs::read(destination_root.join("folder/b.bin")).unwrap(),
            payload
        );
        assert_eq!(
            std::fs::read(destination_root.join("folder/empty.txt")).unwrap(),
            Vec::<u8>::new()
        );
        assert_eq!(
            std::fs::read(destination_root.join("folder/照片.txt")).unwrap(),
            "你好".as_bytes()
        );
        assert!(!paths::temp_dir(&destination_root, tid).exists());

        let mut sender_complete = false;
        while let Ok(event) = sender_rx.try_recv() {
            if matches!(event, TransferEvent::TransferComplete(_)) {
                sender_complete = true;
            }
        }
        assert!(sender_complete);
        let mut receiver_complete = false;
        while let Ok(event) = receiver_rx.try_recv() {
            if matches!(event, TransferEvent::TransferComplete(_)) {
                receiver_complete = true;
            }
        }
        assert!(receiver_complete);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pauses_and_resumes_from_the_receiver_offset() {
        let pair = pair().await;
        let source = tempfile::tempdir().unwrap();
        let payload: Vec<u8> = (0..4 * 1024 * 1024)
            .map(|index| (index % 239) as u8)
            .collect();
        let source_path = source.path().join("big.bin");
        std::fs::write(&source_path, &payload).unwrap();
        let entries = manifest(&[("big.bin", payload.len() as u64)]);
        let tid = Uuid::new_v4();
        let path_map = BTreeMap::from([(
            "big.bin".to_string(),
            source_path.to_string_lossy().to_string(),
        )]);
        let sender_transfer = make_transfer(tid, Role::Src, entries.clone(), path_map, None);
        let destination = tempfile::tempdir().unwrap();
        let destination_root = std::fs::canonicalize(destination.path()).unwrap();
        let receiver_transfer = make_transfer(
            tid,
            Role::Dst,
            entries.clone(),
            BTreeMap::new(),
            Some(destination_root.to_string_lossy().to_string()),
        );
        let files = destination_files(&destination_root, &entries);
        let config = test_config(64 * 1024, Some(Duration::from_millis(5)));

        let (receiver_tx, mut receiver_rx) = mpsc::channel(1024);
        let (receiver_run, receiver_run_control) = RunHandle::new();
        let (sender_tx, _sender_rx) = mpsc::channel(1024);
        let (sender_run, sender_run_control) = RunHandle::new();

        let receiver_connection = pair.server.connection.clone();
        let receiver_control = pair.server.clone();
        let mut first_receiver = tokio::spawn(async move {
            receive_transfer(
                &receiver_connection,
                &receiver_control,
                &receiver_transfer,
                &files,
                &receiver_tx,
                receiver_run_control,
                &config,
            )
            .await
        });
        let sender_connection = pair.client.connection.clone();
        let sender_control = pair.client.clone();
        let mut first_sender = tokio::spawn(async move {
            send_transfer(
                &sender_connection,
                &sender_control,
                &sender_transfer,
                &sender_tx,
                sender_run_control,
                &config,
            )
            .await
        });

        let saw_progress = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match receiver_rx.recv().await {
                    Some(TransferEvent::Progress(progress)) if progress.offset > 0 => break,
                    Some(_) => {}
                    None => panic!("progress channel closed"),
                }
            }
        })
        .await;
        assert!(saw_progress.is_ok());

        receiver_run.pause();
        sender_run.pause();
        let receiver_outcome = tokio::time::timeout(Duration::from_secs(5), &mut first_receiver)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let sender_outcome = tokio::time::timeout(Duration::from_secs(5), &mut first_sender)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(receiver_outcome, RunOutcome::Paused);
        assert_eq!(sender_outcome, RunOutcome::Paused);

        let temp_path = paths::temp_file(&destination_root, tid, 0);
        let length = std::fs::metadata(&temp_path).unwrap().len();
        assert!(length > 0);
        assert!(length < payload.len() as u64);

        let (receiver_tx, _receiver_rx) = mpsc::channel(1024);
        let (_receiver_run, receiver_run_control) = RunHandle::new();
        let (sender_tx, _sender_rx) = mpsc::channel(1024);
        let (_sender_run, sender_run_control) = RunHandle::new();
        let receiver_connection = pair.server.connection.clone();
        let receiver_control = pair.server.clone();
        let receiver_transfer = make_transfer(
            tid,
            Role::Dst,
            entries.clone(),
            BTreeMap::new(),
            Some(destination_root.to_string_lossy().to_string()),
        );
        let files = destination_files(&destination_root, &entries);
        let receiver_task = tokio::spawn(async move {
            receive_transfer(
                &receiver_connection,
                &receiver_control,
                &receiver_transfer,
                &files,
                &receiver_tx,
                receiver_run_control,
                &config,
            )
            .await
        });
        let sender_connection = pair.client.connection.clone();
        let sender_control = pair.client.clone();
        let sender_transfer = make_transfer(
            tid,
            Role::Src,
            entries.clone(),
            BTreeMap::from([(
                "big.bin".to_string(),
                source_path.to_string_lossy().to_string(),
            )]),
            None,
        );
        let sender_task = tokio::spawn(async move {
            send_transfer(
                &sender_connection,
                &sender_control,
                &sender_transfer,
                &sender_tx,
                sender_run_control,
                &config,
            )
            .await
        });
        let receiver_outcome = tokio::time::timeout(Duration::from_secs(10), receiver_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let sender_outcome = tokio::time::timeout(Duration::from_secs(10), sender_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(receiver_outcome, RunOutcome::Complete);
        assert_eq!(sender_outcome, RunOutcome::Complete);
        assert_eq!(
            std::fs::read(destination_root.join("big.bin")).unwrap(),
            payload
        );
        assert!(!paths::temp_dir(&destination_root, tid).exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancels_and_removes_temporary_files() {
        let pair = pair().await;
        let source = tempfile::tempdir().unwrap();
        let payload: Vec<u8> = (0..2 * 1024 * 1024)
            .map(|index| (index % 241) as u8)
            .collect();
        let source_path = source.path().join("cancel.bin");
        std::fs::write(&source_path, &payload).unwrap();
        let entries = manifest(&[("cancel.bin", payload.len() as u64)]);
        let tid = Uuid::new_v4();
        let sender_transfer = make_transfer(
            tid,
            Role::Src,
            entries.clone(),
            BTreeMap::from([(
                "cancel.bin".to_string(),
                source_path.to_string_lossy().to_string(),
            )]),
            None,
        );
        let destination = tempfile::tempdir().unwrap();
        let destination_root = std::fs::canonicalize(destination.path()).unwrap();
        let receiver_transfer = make_transfer(
            tid,
            Role::Dst,
            entries.clone(),
            BTreeMap::new(),
            Some(destination_root.to_string_lossy().to_string()),
        );
        let files = destination_files(&destination_root, &entries);
        let config = test_config(64 * 1024, Some(Duration::from_millis(5)));

        let (receiver_tx, mut receiver_rx) = mpsc::channel(1024);
        let (receiver_run, receiver_run_control) = RunHandle::new();
        let (sender_tx, _sender_rx) = mpsc::channel(1024);
        let (sender_run, sender_run_control) = RunHandle::new();
        let receiver_connection = pair.server.connection.clone();
        let receiver_control = pair.server.clone();
        let receiver_task = tokio::spawn(async move {
            receive_transfer(
                &receiver_connection,
                &receiver_control,
                &receiver_transfer,
                &files,
                &receiver_tx,
                receiver_run_control,
                &config,
            )
            .await
        });
        let sender_connection = pair.client.connection.clone();
        let sender_control = pair.client.clone();
        let sender_task = tokio::spawn(async move {
            send_transfer(
                &sender_connection,
                &sender_control,
                &sender_transfer,
                &sender_tx,
                sender_run_control,
                &config,
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match receiver_rx.recv().await {
                    Some(TransferEvent::Progress(progress)) if progress.offset > 0 => break,
                    Some(_) => {}
                    None => panic!("progress channel closed"),
                }
            }
        })
        .await
        .unwrap();
        receiver_run.cancel();
        sender_run.cancel();

        let receiver_outcome = tokio::time::timeout(Duration::from_secs(5), receiver_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let sender_outcome = tokio::time::timeout(Duration::from_secs(5), sender_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(receiver_outcome, RunOutcome::Canceled);
        assert_eq!(sender_outcome, RunOutcome::Canceled);
        assert!(!paths::temp_dir(&destination_root, tid).exists());
        assert!(!destination_root.join("cancel.bin").exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn source_changes_are_rejected_before_opening_streams() {
        let pair = pair().await;
        let source = tempfile::tempdir().unwrap();
        let source_path = source.path().join("changed.bin");
        std::fs::write(&source_path, b"changed").unwrap();
        let entries = manifest(&[("changed.bin", 5)]);
        let transfer = make_transfer(
            Uuid::new_v4(),
            Role::Src,
            entries,
            BTreeMap::from([(
                "changed.bin".to_string(),
                source_path.to_string_lossy().to_string(),
            )]),
            None,
        );
        let (progress, _progress_rx) = mpsc::channel(16);
        let (_run, run_control) = RunHandle::new();
        let result = send_transfer(
            &pair.client.connection,
            &pair.client,
            &transfer,
            &progress,
            run_control,
            &test_config(64 * 1024, None),
        )
        .await;
        assert!(matches!(result, Err(TransferError::SourceChanged(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stalls_are_bounded_by_the_read_timeout() {
        let pair = pair().await;
        let entries = manifest(&[("stall.txt", 5)]);
        let tid = Uuid::new_v4();
        let destination = tempfile::tempdir().unwrap();
        let destination_root = std::fs::canonicalize(destination.path()).unwrap();
        let receiver_transfer = make_transfer(
            tid,
            Role::Dst,
            entries.clone(),
            BTreeMap::new(),
            Some(destination_root.to_string_lossy().to_string()),
        );
        let files = destination_files(&destination_root, &entries);
        let (progress, _progress_rx) = mpsc::channel(16);
        let (_run, run_control) = RunHandle::new();
        let mut config = test_config(64 * 1024, None);
        config.read_timeout = Duration::from_millis(100);
        let receiver_connection = pair.server.connection.clone();
        let receiver_control = pair.server.clone();
        let receiver_task = tokio::spawn(async move {
            receive_transfer(
                &receiver_connection,
                &receiver_control,
                &receiver_transfer,
                &files,
                &progress,
                run_control,
                &config,
            )
            .await
        });

        let (mut send, mut recv) = pair.client.connection.open_bi().await.unwrap();
        write_header(
            &mut send,
            &FileStreamMessage::FileStart(FileStart {
                tid,
                path: "stall.txt".into(),
                size: 5,
                offset: 0,
            }),
        )
        .await
        .unwrap();
        let accept: FileStreamMessage = read_header(&mut recv).await.unwrap();
        assert!(matches!(accept, FileStreamMessage::FileAccept(_)));
        tokio::time::sleep(Duration::from_millis(300)).await;

        let result = tokio::time::timeout(Duration::from_secs(5), receiver_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(TransferError::Timeout)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hash_mismatch_keeps_the_final_file_absent() {
        let pair = pair().await;
        let entries = manifest(&[("hash.txt", 5)]);
        let tid = Uuid::new_v4();
        let destination = tempfile::tempdir().unwrap();
        let destination_root = std::fs::canonicalize(destination.path()).unwrap();
        let receiver_transfer = make_transfer(
            tid,
            Role::Dst,
            entries.clone(),
            BTreeMap::new(),
            Some(destination_root.to_string_lossy().to_string()),
        );
        let files = destination_files(&destination_root, &entries);
        let (progress, _progress_rx) = mpsc::channel(16);
        let (_run, run_control) = RunHandle::new();
        let config = test_config(64 * 1024, None);
        let receiver_connection = pair.server.connection.clone();
        let receiver_control = pair.server.clone();
        let receiver_task = tokio::spawn(async move {
            receive_transfer(
                &receiver_connection,
                &receiver_control,
                &receiver_transfer,
                &files,
                &progress,
                run_control,
                &config,
            )
            .await
        });

        let (mut send, mut recv) = pair.client.connection.open_bi().await.unwrap();
        write_header(
            &mut send,
            &FileStreamMessage::FileStart(FileStart {
                tid,
                path: "hash.txt".into(),
                size: 5,
                offset: 0,
            }),
        )
        .await
        .unwrap();
        let accept: FileStreamMessage = read_header(&mut recv).await.unwrap();
        assert!(matches!(accept, FileStreamMessage::FileAccept(_)));
        write_binary(&mut send, DATA_TAG, b"hello", &config)
            .await
            .unwrap();
        write_binary(&mut send, END_TAG, b"deadbeef", &config)
            .await
            .unwrap();
        let done: FileStreamMessage = read_header(&mut recv).await.unwrap();
        assert!(matches!(
            done,
            FileStreamMessage::FileDone(FileDone { ok: false, .. })
        ));

        let result = tokio::time::timeout(Duration::from_secs(5), receiver_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(TransferError::HashMismatch(_))));
        assert!(!destination_root.join("hash.txt").exists());
        assert!(paths::temp_file(&destination_root, tid, 0).exists());
    }
}
