## ADDED Requirements

### Requirement: LAN Service Advertisement

The application MUST advertise a `_lingo._tcp` service on a non-loopback interface while networking is enabled, publishing its current DID, device profile, and actual listening port.

#### Scenario: Service is advertised after startup
- **WHEN** the application starts with networking enabled
- **THEN** a `_lingo._tcp` service is registered with the current DID and the bound port

#### Scenario: Endpoint is reachable from the LAN
- **WHEN** another device on the same LAN resolves the advertised service
- **THEN** it can open a QUIC connection to the advertised address and port

### Requirement: Peer Discovery Events

The application MUST browse for Lingo services and map each resolved service to a device record keyed by DID.

#### Scenario: Peer appears
- **WHEN** another Lingo instance advertises on the LAN
- **THEN** the device is added or updated with online state and appears in the device list

#### Scenario: Peer disappears
- **WHEN** a service is removed or its record times out
- **THEN** the device is marked offline and its transfer history is retained

### Requirement: Discovery Identity Is Untrusted

The application MUST treat TXT identity data as display-only until an authenticated session confirms the same DID.

#### Scenario: Advertised DID differs from authenticated DID
- **WHEN** a connection authenticates with a DID different from the DID that was discovered for that address
- **THEN** the session is rejected and the discovered device is not marked online

#### Scenario: Display name spoofing does not grant trust
- **WHEN** a peer advertises a name that matches an existing device but authenticates with a different DID
- **THEN** the device records remain separate and no transfer is automatically accepted
