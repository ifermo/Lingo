## ADDED Requirements

### Requirement: Mutually Authenticated Handshake

Every peer session MUST perform a mutual challenge-response handshake that binds both device proofs to the QUIC connection's exported keying material.

#### Scenario: Successful handshake
- **WHEN** two devices complete the challenge-response exchange over a QUIC connection
- **THEN** each side verifies the other's Ed25519 signature over the agreed payload and marks the session authenticated

#### Scenario: Replayed proof is rejected
- **WHEN** a peer sends a previously recorded authentication proof
- **THEN** the receiver rejects the session because the proof does not cover the receiver's fresh challenge

#### Scenario: Proof from another connection is rejected
- **WHEN** a proof is replayed over a different QUIC connection with different exported keying material
- **THEN** verification fails and the session is rejected

### Requirement: Authenticate Before Control

A peer MUST NOT be able to send transfer control or file stream messages before authentication completes.

#### Scenario: Unauthenticated control message
- **WHEN** a connection sends a transfer message before a successful handshake
- **THEN** the connection is closed and no state is created

#### Scenario: Handshake limits
- **WHEN** a peer sends a handshake frame larger than the configured handshake limit
- **THEN** the connection is closed without allocating the full payload

### Requirement: Single Session per Device

The application MUST keep at most one active session per authenticated DID.

#### Scenario: Duplicate connection
- **WHEN** a second authenticated connection arrives for a DID that already has an active session
- **THEN** the older session is closed and the new session becomes authoritative

### Requirement: Session Failure Handling

The application MUST surface session loss to active transfers instead of leaving them in a transferring state.

#### Scenario: Connection drops during transfer
- **WHEN** an authenticated session closes while a transfer is active
- **THEN** the transfer is moved to `Paused` when resumable and `Failed` when it cannot continue, and the UI reflects the reason

#### Scenario: Authentication failure is reported
- **WHEN** a peer fails authentication
- **THEN** the session is closed, no transfer is created, and the failure is reported to the local UI
