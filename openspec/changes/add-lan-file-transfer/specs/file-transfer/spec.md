## ADDED Requirements

### Requirement: Transfer Offer and Acceptance

A sender MUST offer a transfer by UUID, sending the manifest in bounded chunks, and MUST NOT transmit file data until the receiver accepts.

#### Scenario: Receiver accepts
- **WHEN** the receiver accepts an offer
- **THEN** it selects and persists a destination path per file and the sender begins transmitting

#### Scenario: Receiver declines
- **WHEN** the receiver declines an offer
- **THEN** both sides mark the transfer canceled and no file data is written

#### Scenario: Offer times out
- **WHEN** the receiver does not respond within the configured offer timeout
- **THEN** the sender marks the transfer failed with a timeout reason

### Requirement: Manifest Validation

The receiver MUST validate every manifest entry before creating an inbound transfer and MUST reject the entire offer if any entry is invalid.

#### Scenario: Path traversal
- **WHEN** a manifest contains an absolute path, a `..` component, a control character, or a path longer than the configured limit
- **THEN** the offer is rejected and no file or directory is created

#### Scenario: Duplicate path
- **WHEN** a manifest contains the same path twice
- **THEN** the offer is rejected

#### Scenario: Too many entries
- **WHEN** a manifest exceeds the configured entry limit
- **THEN** the offer is rejected before allocating unbounded memory

### Requirement: Per-File Streaming with Integrity

Each file MUST be transmitted over its own stream and verified with a rolling SHA-256 digest.

#### Scenario: Successful file
- **WHEN** the sender finishes a file and sends its digest
- **THEN** the receiver compares the digest, fsyncs the temporary file, and atomically renames it to the final path

#### Scenario: Digest mismatch
- **WHEN** the received digest does not match the sender's digest
- **THEN** the file is marked failed, is not renamed to the final path, and the transfer reports the failure

#### Scenario: Empty file
- **WHEN** a manifest entry has size zero
- **THEN** the transfer creates an empty final file without sending file bytes

### Requirement: Safe Destination Writes

The receiver MUST constrain all writes to the selected destination root, MUST never overwrite an existing file, and MUST use a temporary file followed by an atomic rename.

#### Scenario: Name collision
- **WHEN** the selected final path already exists at accept time
- **THEN** the receiver chooses a non-colliding name and persists it for the transfer

#### Scenario: Symlink escape
- **WHEN** a destination parent directory resolves outside the selected root
- **THEN** the file is rejected and nothing is written outside the root

#### Scenario: Interrupted write
- **WHEN** the process stops before completion
- **THEN** the final path contains either the previous file or no file, never a partially written final file

### Requirement: Transfer Controls

The application MUST support pause, resume, and cancel, synchronized between both peers.

#### Scenario: Pause
- **WHEN** either peer pauses an active transfer
- **THEN** data streams are stopped, received offsets are flushed, and both sides show `Paused`

#### Scenario: Resume
- **WHEN** either peer resumes a paused transfer
- **THEN** both sides return to `Ready` and transmission continues from the receiver's persisted offsets

#### Scenario: Cancel
- **WHEN** either peer cancels a transfer
- **THEN** data streams are reset, temporary files are removed, and both sides show `Canceled`

### Requirement: Progress and Failure Reporting

The application MUST report aggregated progress and a user-visible reason for every terminal failure.

#### Scenario: Progress updates
- **WHEN** file data is being written
- **THEN** the UI receives progress and speed at a bounded sampling rate rather than per chunk

#### Scenario: Source changed
- **WHEN** a source file's size or modification time changes before or during sending
- **THEN** the transfer fails with a source-changed reason and does not produce a final file with incorrect content

### Requirement: Transfer Concurrency Limit

The sender MUST limit concurrent file streams per transfer and MUST keep the control stream independent of data streams.

#### Scenario: Many files
- **WHEN** a manifest contains more files than the concurrency limit
- **THEN** the sender queues the remaining files and keeps at most the configured number of file streams open
