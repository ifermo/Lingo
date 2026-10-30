## ADDED Requirements

### Requirement: Per-File Progress Persistence

The receiver MUST persist per-file offsets and states so that an interrupted transfer can resume after a restart.

#### Scenario: Offset is persisted during transfer
- **WHEN** a file stream has made progress
- **THEN** the receiver persists the offset at a bounded interval and updates it on pause or shutdown

#### Scenario: Restart reads persisted state
- **WHEN** the application starts with an unfinished inbound transfer
- **THEN** it loads per-file offsets and final paths from storage

### Requirement: Receiver-Authoritative Resume

After reconnecting, the sender MUST request resume state from the receiver and MUST continue from the offsets reported by the receiver.

#### Scenario: Resume after reconnect
- **WHEN** a paused transfer is resumed after a session reconnect
- **THEN** the sender asks the receiver for per-file offsets and starts each file at the reported offset

#### Scenario: Receiver offset exceeds database offset
- **WHEN** the actual temporary file length is greater than the last persisted offset
- **THEN** the receiver uses the actual length after validation and reports it to the sender

### Requirement: Restart Recovery

The application MUST reconcile transfer state on startup without silently claiming completion.

#### Scenario: Unfinished transfer on startup
- **WHEN** a transfer was `Ready` when the process stopped
- **THEN** it is restored as `Paused` and requires an explicit resume

#### Scenario: Final file already present
- **WHEN** both a final file and a temporary file exist for the same entry at startup
- **THEN** the temporary file is removed and the entry is treated as complete

#### Scenario: Source missing on sender restart
- **WHEN** a sender restarts and a source file is missing
- **THEN** the transfer is marked `Failed` with a source-missing reason

### Requirement: Prefix Verification on Resume

Both peers MUST recompute the digest of the already-transferred prefix before appending so that the final digest covers the complete file.

#### Scenario: Prefix is consistent
- **WHEN** both peers resume from the same offset and the prefix digests match
- **THEN** transmission continues and the final digest is verified against the complete file

#### Scenario: Prefix is inconsistent
- **WHEN** the sender's source prefix no longer matches the receiver's temporary file prefix
- **THEN** the file is restarted from offset zero or the transfer fails with a clear reason

### Requirement: Temporary File Lifecycle

The application MUST clean up temporary data when it is no longer needed and MUST retain it when a resume is still possible.

#### Scenario: Cancel removes temporary files
- **WHEN** a transfer is canceled
- **THEN** its temporary directory is removed and completed final files are retained

#### Scenario: Failure retains resumable data
- **WHEN** a transfer fails in a way that can be resumed
- **THEN** temporary files are retained and the UI offers a resume path
