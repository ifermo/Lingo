//! Network agent: owns peer connections, sessions and transfer tasks.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use lingo_core::identity::Identity;
use lingo_core::model::{Avatar, Device, DeviceId, Transfer, TransferFile, now_micros};
use lingo_core::msg::{PeerMessage, TransferControlAction, TransferEvent, TransferFailed};
use lingo_net::control::{self, ControlHandle};
use lingo_net::discovery::{Discovery, DiscoveryEvent, DiscoveryStream};
use lingo_net::endpoint::PeerEndpoint;
use lingo_net::peer::PeerRegistry;
use lingo_net::session::{self, SessionConfig};
use lingo_net::transfer::{DataConfig, RunHandle, RunState, receive_transfer, send_transfer};
use quinn::Incoming;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("network error: {0}")]
    Net(#[from] lingo_net::NetError),
}

#[derive(Debug)]
pub enum AgentCommand {
    SendPeer {
        peer_did: DeviceId,
        message: PeerMessage,
    },
    StartSend {
        peer_did: DeviceId,
        transfer: Transfer,
    },
    StartReceive {
        peer_did: DeviceId,
        transfer: Transfer,
        files: Vec<TransferFile>,
    },
    Control {
        tid: Uuid,
        action: TransferControlAction,
    },
    Advertise {
        avatar: Avatar,
    },
    #[allow(dead_code)]
    AddPeer {
        did: DeviceId,
        addrs: Vec<SocketAddr>,
    },
}

#[derive(Debug)]
pub enum AgentEvent {
    PeerDiscovered(Device),
    PeerOffline(Device),
    PeerMessage {
        peer_did: DeviceId,
        message: PeerMessage,
    },
    TransferEvent(TransferEvent),
    Error(String),
}

#[derive(Clone)]
pub struct AgentHandle {
    tx: mpsc::Sender<AgentCommand>,
}

impl AgentHandle {
    pub async fn send(&self, command: AgentCommand) -> bool {
        self.tx.send(command).await.is_ok()
    }

    #[allow(dead_code)]
    pub fn try_send(&self, command: AgentCommand) -> bool {
        self.tx.try_send(command).is_ok()
    }

    #[allow(dead_code)]
    pub async fn add_peer(&self, did: DeviceId, addrs: Vec<SocketAddr>) -> bool {
        self.send(AgentCommand::AddPeer { did, addrs }).await
    }
}

#[derive(Clone)]
struct PeerConnection {
    connection: quinn::Connection,
    control: ControlHandle,
}

pub struct Agent;

impl Agent {
    pub fn start(
        identity: Identity,
        avatar: Avatar,
    ) -> Result<(AgentHandle, mpsc::Receiver<AgentEvent>, SocketAddr), AgentError> {
        let endpoint = PeerEndpoint::new(&identity)?;
        let local_addr = endpoint.local_addr();
        let mut discovery = Discovery::new()?;
        discovery.advertise(&identity.did(), &avatar, local_addr.port())?;
        let discovery_stream = discovery.stream()?;

        let (command_tx, command_rx) = mpsc::channel(256);
        let (event_tx, event_rx) = mpsc::channel(256);
        let (inbound_tx, inbound_rx) = mpsc::channel(256);
        let (internal_tx, internal_rx) = mpsc::channel(64);
        let (finished_tx, finished_rx) = mpsc::channel(64);

        let state = AgentState {
            local_did: identity.did(),
            identity,
            avatar,
            endpoint,
            discovery,
            discovery_stream,
            registry: PeerRegistry::default(),
            known_addrs: HashMap::new(),
            connections: HashMap::new(),
            runs: HashMap::new(),
            next_run: 1,
            next_session: 1,
            event_tx,
            command_rx,
            inbound_tx,
            inbound_rx,
            internal_tx,
            internal_rx,
            finished_tx,
            finished_rx,
            session_config: SessionConfig::default(),
            data_config: DataConfig::default(),
        };
        tokio::spawn(state.run());

        Ok((AgentHandle { tx: command_tx }, event_rx, local_addr))
    }
}

struct AgentState {
    local_did: DeviceId,
    identity: Identity,
    avatar: Avatar,
    endpoint: PeerEndpoint,
    discovery: Discovery,
    discovery_stream: DiscoveryStream,
    registry: PeerRegistry,
    known_addrs: HashMap<DeviceId, Vec<SocketAddr>>,
    connections: HashMap<DeviceId, PeerConnection>,
    runs: HashMap<Uuid, (u64, RunHandle)>,
    next_run: u64,
    next_session: u64,
    event_tx: mpsc::Sender<AgentEvent>,
    command_rx: mpsc::Receiver<AgentCommand>,
    inbound_tx: mpsc::Sender<(DeviceId, PeerMessage)>,
    inbound_rx: mpsc::Receiver<(DeviceId, PeerMessage)>,
    internal_tx: mpsc::Sender<(DeviceId, PeerConnection, oneshot::Sender<()>)>,
    internal_rx: mpsc::Receiver<(DeviceId, PeerConnection, oneshot::Sender<()>)>,
    finished_tx: mpsc::Sender<(Uuid, u64)>,
    finished_rx: mpsc::Receiver<(Uuid, u64)>,
    session_config: SessionConfig,
    data_config: DataConfig,
}

impl AgentState {
    async fn run(mut self) {
        loop {
            tokio::select! {
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    self.handle_command(command).await;
                }
                event = self.discovery_stream.next() => {
                    if let Some(event) = event {
                        self.handle_discovery(event).await;
                    }
                }
                incoming = self.endpoint.accept() => {
                    if let Some(incoming) = incoming {
                        self.spawn_incoming(incoming);
                    }
                }
                inbound = self.inbound_rx.recv() => {
                    if let Some((peer_did, message)) = inbound {
                        self.handle_peer_message(peer_did, message).await;
                    }
                }
                internal = self.internal_rx.recv() => {
                    if let Some((peer_did, connection, ready)) = internal {
                        self.register_connection(peer_did, connection).await;
                        let _ = ready.send(());
                    }
                }
                finished = self.finished_rx.recv() => {
                    if let Some((tid, run_id)) = finished
                        && matches!(self.runs.get(&tid), Some((current, _)) if *current == run_id)
                    {
                        self.runs.remove(&tid);
                    }
                }
            }
        }
        self.endpoint.close();
    }

    fn spawn_incoming(&self, incoming: Incoming) {
        let identity = self.identity.clone();
        let internal_tx = self.internal_tx.clone();
        let inbound_tx = self.inbound_tx.clone();
        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            let connection = match incoming.await {
                Ok(connection) => connection,
                Err(err) => {
                    let _ = event_tx
                        .send(AgentEvent::Error(format!(
                            "Incoming connection failed: {err}"
                        )))
                        .await;
                    return;
                }
            };
            let session = match session::accept(
                &connection,
                &identity,
                None,
                &SessionConfig::default(),
            )
            .await
            {
                Ok(session) => session,
                Err(err) => {
                    debug!(error = %err, "inbound authentication failed");
                    return;
                }
            };
            let peer_did = session.peer_did().clone();
            let (control, mut inbound) = control::spawn(session);
            let (ready_tx, ready_rx) = oneshot::channel();
            if internal_tx
                .send((
                    peer_did.clone(),
                    PeerConnection {
                        connection,
                        control,
                    },
                    ready_tx,
                ))
                .await
                .is_err()
            {
                return;
            }
            let _ = ready_rx.await;
            tokio::spawn(async move {
                while let Some(message) = inbound.recv().await {
                    if inbound_tx.send((peer_did.clone(), message)).await.is_err() {
                        break;
                    }
                }
            });
        });
    }

    async fn register_connection(&mut self, peer_did: DeviceId, connection: PeerConnection) {
        if let Some(previous) = self.connections.insert(peer_did.clone(), connection) {
            previous.connection.close(0u32.into(), b"replaced");
        }
        let session_id = self.next_session;
        self.next_session += 1;
        let _ = self.registry.bind_session(&peer_did, &peer_did, session_id);
        let device = self.device_for(&peer_did, true);
        let _ = self.event_tx.send(AgentEvent::PeerDiscovered(device)).await;
    }

    async fn handle_command(&mut self, command: AgentCommand) {
        match command {
            AgentCommand::SendPeer { peer_did, message } => {
                let tid = message_tid(&message);
                let Some(connection) = self.ensure_connection(&peer_did).await else {
                    if let Some(tid) = tid {
                        self.fail(tid, "The other device is offline").await;
                    } else {
                        let _ = self
                            .event_tx
                            .send(AgentEvent::Error(format!("{} is not reachable", peer_did)))
                            .await;
                    }
                    return;
                };
                if let Err(err) = connection.control.send(message).await {
                    warn!(error = %err, peer = %peer_did, "failed to send peer message");
                    self.connections.remove(&peer_did);
                    if let Some(tid) = tid {
                        self.fail(tid, "Lost the connection to the other device")
                            .await;
                    } else {
                        let _ = self
                            .event_tx
                            .send(AgentEvent::Error(format!("Failed to reach {peer_did}")))
                            .await;
                    }
                }
            }
            AgentCommand::StartSend { peer_did, transfer } => {
                self.start_send(peer_did, transfer).await;
            }
            AgentCommand::StartReceive {
                peer_did,
                transfer,
                files,
            } => {
                self.start_receive(peer_did, transfer, files).await;
            }
            AgentCommand::Control { tid, action } => self.signal(tid, action),
            AgentCommand::Advertise { avatar } => {
                self.avatar = avatar;
                if let Err(err) = self.discovery.advertise(
                    &self.local_did,
                    &self.avatar,
                    self.endpoint.local_addr().port(),
                ) {
                    let _ = self
                        .event_tx
                        .send(AgentEvent::Error(format!(
                            "Could not advertise this device: {err}"
                        )))
                        .await;
                }
            }
            AgentCommand::AddPeer { did, addrs } => {
                self.known_addrs.insert(did, addrs);
            }
        }
    }

    async fn handle_discovery(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::Resolved(peer) => {
                if peer.did == self.local_did {
                    return;
                }
                let did = peer.did.clone();
                self.known_addrs.insert(did.clone(), peer.addrs.clone());
                self.registry.upsert(peer);
                let device = self.device_for(&did, true);
                let _ = self.event_tx.send(AgentEvent::PeerDiscovered(device)).await;
            }
            DiscoveryEvent::Removed { did, .. } => {
                self.known_addrs.remove(&did);
                self.registry.mark_offline(&did);
                let device = self.device_for(&did, false);
                let _ = self.event_tx.send(AgentEvent::PeerOffline(device)).await;
            }
        }
    }

    async fn handle_peer_message(&mut self, peer_did: DeviceId, message: PeerMessage) {
        match &message {
            PeerMessage::TransferControl(control) => self.signal(control.tid, control.action),
            PeerMessage::TransferDecline(decline) => self.cancel_run(decline.tid),
            PeerMessage::TransferFailed(failed) => self.cancel_run(failed.tid),
            _ => {}
        }
        let _ = self
            .event_tx
            .send(AgentEvent::PeerMessage { peer_did, message })
            .await;
    }

    async fn ensure_connection(&mut self, peer_did: &DeviceId) -> Option<PeerConnection> {
        if let Some(connection) = self.connections.get(peer_did)
            && connection.connection.close_reason().is_none()
        {
            return Some(connection.clone());
        }
        self.connections.remove(peer_did);

        let addrs = self
            .registry
            .get(peer_did)
            .map(|record| record.addrs.clone())
            .or_else(|| self.known_addrs.get(peer_did).cloned())
            .unwrap_or_default();
        if addrs.is_empty() {
            debug!(peer = %peer_did, "no known addresses");
            return None;
        }

        for addr in addrs {
            let connection =
                match tokio::time::timeout(Duration::from_secs(5), self.endpoint.connect(addr))
                    .await
                {
                    Ok(Ok(connection)) => connection,
                    Ok(Err(err)) => {
                        debug!(error = %err, %addr, "failed to connect to peer");
                        continue;
                    }
                    Err(_) => {
                        debug!(%addr, "peer connection timed out");
                        continue;
                    }
                };
            let session =
                match session::connect(&connection, &self.identity, peer_did, &self.session_config)
                    .await
                {
                    Ok(session) => session,
                    Err(err) => {
                        debug!(error = %err, peer = %peer_did, "peer authentication failed");
                        connection.close(0u32.into(), b"auth failed");
                        continue;
                    }
                };
            let (control, mut inbound) = control::spawn(session);
            let peer = PeerConnection {
                connection,
                control,
            };
            if let Some(previous) = self.connections.insert(peer_did.clone(), peer.clone()) {
                previous.connection.close(0u32.into(), b"replaced");
            }
            let forward_did = peer_did.clone();
            let inbound_tx = self.inbound_tx.clone();
            tokio::spawn(async move {
                while let Some(message) = inbound.recv().await {
                    if inbound_tx
                        .send((forward_did.clone(), message))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let session_id = self.next_session;
            self.next_session += 1;
            let _ = self.registry.bind_session(peer_did, peer_did, session_id);
            let device = self.device_for(peer_did, true);
            let _ = self.event_tx.send(AgentEvent::PeerDiscovered(device)).await;
            info!(peer = %peer_did, "connected to peer");
            return Some(peer);
        }
        None
    }

    async fn start_send(&mut self, peer_did: DeviceId, transfer: Transfer) {
        let tid = transfer.tid;
        if let Some((_, run)) = self.runs.get(&tid)
            && run.state() == RunState::Running
        {
            return;
        }
        let Some(connection) = self.ensure_connection(&peer_did).await else {
            self.fail(tid, "The other device is offline").await;
            return;
        };

        let (run, run_control) = RunHandle::new();
        let run_id = self.next_run;
        self.next_run += 1;
        self.runs.insert(tid, (run_id, run));

        let (progress_tx, mut progress_rx) = mpsc::channel(256);
        let event_tx = self.event_tx.clone();
        tokio::spawn(async move {
            while let Some(event) = progress_rx.recv().await {
                if event_tx
                    .send(AgentEvent::TransferEvent(event))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        let finished = self.finished_tx.clone();
        let config = self.data_config;
        tokio::spawn(async move {
            let result = send_transfer(
                &connection.connection,
                &connection.control,
                &transfer,
                &progress_tx,
                run_control,
                &config,
            )
            .await;
            if let Err(err) = &result {
                debug!(error = %err, tid = %tid, "send transfer ended");
            }
            let _ = finished.send((tid, run_id)).await;
        });
    }

    async fn start_receive(
        &mut self,
        peer_did: DeviceId,
        transfer: Transfer,
        files: Vec<TransferFile>,
    ) {
        let tid = transfer.tid;
        if let Some((_, run)) = self.runs.get(&tid)
            && run.state() == RunState::Running
        {
            return;
        }
        let Some(connection) = self.connections.get(&peer_did).cloned() else {
            self.fail(tid, "The other device is offline").await;
            return;
        };

        let (run, run_control) = RunHandle::new();
        let run_id = self.next_run;
        self.next_run += 1;
        self.runs.insert(tid, (run_id, run));

        let (progress_tx, mut progress_rx) = mpsc::channel(256);
        let event_tx = self.event_tx.clone();
        tokio::spawn(async move {
            while let Some(event) = progress_rx.recv().await {
                if event_tx
                    .send(AgentEvent::TransferEvent(event))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        let finished = self.finished_tx.clone();
        let config = self.data_config;
        tokio::spawn(async move {
            let result = receive_transfer(
                &connection.connection,
                &connection.control,
                &transfer,
                &files,
                &progress_tx,
                run_control,
                &config,
            )
            .await;
            if let Err(err) = &result {
                debug!(error = %err, tid = %tid, "receive transfer ended");
            }
            let _ = finished.send((tid, run_id)).await;
        });
    }

    fn signal(&mut self, tid: Uuid, action: TransferControlAction) {
        let Some((_, run)) = self.runs.get(&tid) else {
            return;
        };
        match action {
            TransferControlAction::Pause => run.pause(),
            // Resume is handled by starting a fresh task once the service reports Ready.
            TransferControlAction::Resume => {}
            TransferControlAction::Cancel => run.cancel(),
        }
    }

    fn cancel_run(&mut self, tid: Uuid) {
        if let Some((_, run)) = self.runs.get(&tid) {
            run.cancel();
        }
    }

    async fn fail(&self, tid: Uuid, reason: &str) {
        let _ = self
            .event_tx
            .send(AgentEvent::TransferEvent(TransferEvent::TransferFailed(
                TransferFailed {
                    tid,
                    err: reason.to_string(),
                },
            )))
            .await;
    }

    fn device_for(&self, did: &DeviceId, online: bool) -> Device {
        let record = self.registry.get(did);
        let avatar = record
            .map(|record| record.avatar.clone())
            .unwrap_or_default();
        let addrs = record
            .map(|record| record.addrs.clone())
            .or_else(|| self.known_addrs.get(did).cloned())
            .unwrap_or_default();
        let now = now_micros();
        Device {
            id: 0,
            did: did.clone(),
            avatar,
            is_online: online,
            active: online,
            active_at: now,
            last_seen_at: now,
            local_addrs: addrs.iter().map(ToString::to_string).collect(),
            ..Device::default()
        }
    }
}

fn message_tid(message: &PeerMessage) -> Option<Uuid> {
    match message {
        PeerMessage::TransferOffer(message) => Some(message.tid),
        PeerMessage::TransferOfferChunk(message) => Some(message.tid),
        PeerMessage::TransferOfferDone(message)
        | PeerMessage::TransferAccept(message)
        | PeerMessage::TransferComplete(message)
        | PeerMessage::TransferResumeQuery(message) => Some(message.tid),
        PeerMessage::TransferDecline(message) => Some(message.tid),
        PeerMessage::TransferControl(message) => Some(message.tid),
        PeerMessage::TransferProgress(message) => Some(message.tid),
        PeerMessage::TransferFailed(message) => Some(message.tid),
        PeerMessage::TransferResumeState(message) => Some(message.tid),
        PeerMessage::Ack(_)
        | PeerMessage::PeerOk(_)
        | PeerMessage::Chat(_)
        | PeerMessage::SetRemoteStatus(_)
        | PeerMessage::RemoteAdd(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr};

    use lingo_core::identity::Identity;
    use lingo_core::model::{ManifestEntry, Role, TransferFile, TransferStatus, now_micros};
    use lingo_core::msg::{Chat, TransferEvent, TransferTid};
    use lingo_core::paths;

    fn avatar() -> Avatar {
        Avatar {
            owner: "Test".into(),
            name: "Agent".into(),
            variant: "macbook".into(),
            color: "green".into(),
        }
    }

    fn loopback(addr: SocketAddr) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
    }

    async fn wait_for_chat(events: &mut mpsc::Receiver<AgentEvent>) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match events.recv().await {
                    Some(AgentEvent::PeerMessage {
                        message: PeerMessage::Chat(_),
                        ..
                    }) => break,
                    Some(_) => {}
                    None => panic!("agent events closed"),
                }
            }
        })
        .await
        .unwrap();
    }

    async fn wait_for_complete(events: &mut mpsc::Receiver<AgentEvent>, tid: Uuid) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match events.recv().await {
                    Some(AgentEvent::TransferEvent(TransferEvent::TransferComplete(
                        TransferTid { tid: complete },
                    ))) if complete == tid => break,
                    Some(_) => {}
                    None => panic!("agent events closed"),
                }
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn agents_connect_and_transfer_a_file() {
        let a_dir = tempfile::tempdir().unwrap();
        let b_dir = tempfile::tempdir().unwrap();
        let a_identity = Identity::load_or_create(a_dir.path()).unwrap();
        let b_identity = Identity::load_or_create(b_dir.path()).unwrap();
        let a_did = a_identity.did();
        let b_did = b_identity.did();

        let (a, mut a_events, a_addr) = Agent::start(a_identity.clone(), avatar()).unwrap();
        let (b, mut b_events, b_addr) = Agent::start(b_identity.clone(), avatar()).unwrap();
        assert!(a.add_peer(b_did.clone(), vec![loopback(b_addr)]).await);
        assert!(b.add_peer(a_did.clone(), vec![loopback(a_addr)]).await);

        assert!(
            a.send(AgentCommand::SendPeer {
                peer_did: b_did.clone(),
                message: PeerMessage::Chat(Chat {
                    text: "hello".into(),
                }),
            })
            .await
        );
        wait_for_chat(&mut b_events).await;

        let source = tempfile::tempdir().unwrap();
        let payload: Vec<u8> = (0..128 * 1024).map(|index| (index % 251) as u8).collect();
        let source_path = source.path().join("hello.bin");
        std::fs::write(&source_path, &payload).unwrap();
        let manifest = vec![ManifestEntry {
            path: "hello.bin".into(),
            size: payload.len() as u64,
        }];
        let tid = Uuid::new_v4();
        let sender_transfer = Transfer {
            id: 1,
            tid,
            role: Role::Src,
            created_at: now_micros(),
            status_at: now_micros(),
            manifest: manifest.clone(),
            note: String::new(),
            removed: false,
            deleted: false,
            offset: 0,
            peer_id: 0,
            path_map: BTreeMap::from([(
                "hello.bin".to_string(),
                source_path.to_string_lossy().to_string(),
            )]),
            dir: None,
            err: None,
            status: TransferStatus::Ready,
            peer_status: TransferStatus::Ready,
            duration: 0,
            speed: None,
        };
        let destination = tempfile::tempdir().unwrap();
        let destination_root = std::fs::canonicalize(destination.path()).unwrap();
        let receiver_transfer = Transfer {
            dir: Some(destination_root.to_string_lossy().to_string()),
            ..sender_transfer.clone()
        };
        let files = vec![TransferFile {
            id: 1,
            transfer_id: 1,
            path: "hello.bin".into(),
            final_path: Some(
                destination_root
                    .join("hello.bin")
                    .to_string_lossy()
                    .to_string(),
            ),
            size: payload.len() as u64,
            offset: 0,
            state: Default::default(),
            sha256: None,
        }];

        assert!(
            b.send(AgentCommand::StartReceive {
                peer_did: a_did.clone(),
                transfer: receiver_transfer,
                files,
            })
            .await
        );
        assert!(
            a.send(AgentCommand::StartSend {
                peer_did: b_did.clone(),
                transfer: sender_transfer,
            })
            .await
        );

        wait_for_complete(&mut a_events, tid).await;
        wait_for_complete(&mut b_events, tid).await;
        assert_eq!(
            std::fs::read(destination_root.join("hello.bin")).unwrap(),
            payload
        );
        assert!(!paths::temp_dir(&destination_root, tid).exists());
    }
}
