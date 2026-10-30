mod agent;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use flume::{Receiver, Sender};
use lingo_core::config::Paths;
use lingo_core::model::Role;
use lingo_core::msg::{ApiBadRequest, Envelope, TransferControlAction};
use lingo_core::service::{Service, ServiceError};
use lingo_ui::UiOptions;
use tracing::{error, info};

use agent::{Agent, AgentCommand, AgentEvent, AgentHandle};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    let hidden = args.iter().any(|arg| arg == "--hidden");
    let gallery = args.iter().any(|arg| arg == "--gallery");
    let show_incoming = args.iter().any(|arg| arg == "--incoming");
    let page = args
        .iter()
        .position(|arg| arg == "--page")
        .and_then(|index| args.get(index + 1))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let screenshot = args
        .iter()
        .position(|arg| arg == "--screenshot")
        .and_then(|index| args.get(index + 1))
        .map(std::path::PathBuf::from);
    let background_mode = Arc::new(AtomicBool::new(false));

    if gallery {
        return lingo_ui::run(
            UiOptions {
                hidden: false,
                gallery: true,
                background_mode,
                screenshot,
                page,
                show_incoming,
            },
            None,
            None,
        )
        .map_err(Into::into);
    }

    let paths = Paths::discover()?;
    let (commands_tx, commands_rx) = flume::bounded(64);
    let (events_tx, events_rx) = flume::bounded(256);
    spawn_core(paths, commands_rx, events_tx);

    lingo_ui::run(
        UiOptions {
            hidden,
            gallery: false,
            background_mode,
            screenshot,
            page,
            show_incoming,
        },
        Some(commands_tx),
        Some(events_rx),
    )?;
    Ok(())
}

fn spawn_core(paths: Paths, commands: Receiver<Envelope>, events: Sender<Envelope>) {
    let result = std::thread::Builder::new()
        .name("lingo-core".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    error!(error = %err, "failed to create the core runtime");
                    return;
                }
            };

            runtime.block_on(async move {
                let (mut service, initial) = match Service::start(&paths) {
                    Ok(started) => started,
                    Err(err) => {
                        let _ = events.send(Envelope::ApiBadRequest(ApiBadRequest {
                            msg: format!("Could not start the agent: {err}"),
                        }));
                        return;
                    }
                };

                let (agent, mut agent_events, local_addr) = match Agent::start(
                    service.identity().clone(),
                    service.state().avatar.clone(),
                ) {
                    Ok(started) => started,
                    Err(err) => {
                        let _ = events.send(Envelope::ApiBadRequest(ApiBadRequest {
                            msg: format!("Could not start networking: {err}"),
                        }));
                        return;
                    }
                };

                info!(did = %service.state().did, %local_addr, "core started");
                if !emit(&events, initial) {
                    return;
                }

                loop {
                    tokio::select! {
                        command = commands.recv_async() => {
                            let Ok(command) = command else {
                                break;
                            };
                            let control = match &command {
                                Envelope::TransferPause(msg) => {
                                    Some((msg.id, TransferControlAction::Pause))
                                }
                                Envelope::TransferResume(msg) => {
                                    Some((msg.id, TransferControlAction::Resume))
                                }
                                Envelope::TransferCancel(msg) => {
                                    Some((msg.id, TransferControlAction::Cancel))
                                }
                                Envelope::TransferRemove(msg) => {
                                    Some((msg.id, TransferControlAction::Cancel))
                                }
                                _ => None,
                            };
                            let advertise = matches!(command, Envelope::SetAvatar(_));

                            match service.handle(command) {
                                Ok(outgoing) => {
                                    if !emit(&events, outgoing) {
                                        return;
                                    }
                                }
                                Err(err) => {
                                    error!(error = %err, "core command failed");
                                    if !emit(&events, vec![Envelope::ApiBadRequest(ApiBadRequest {
                                        msg: err.to_string(),
                                    })]) {
                                        return;
                                    }
                                }
                            }

                            if let Some((id, action)) = control
                                && let Some(transfer) = service.transfer_by_id(id)
                            {
                                let _ = agent
                                    .send(AgentCommand::Control {
                                        tid: transfer.tid,
                                        action,
                                    })
                                    .await;
                            }
                            if advertise {
                                let _ = agent
                                    .send(AgentCommand::Advertise {
                                        avatar: service.state().avatar.clone(),
                                    })
                                    .await;
                            }
                            flush_agent(&mut service, &agent, &events).await;
                        }
                        event = agent_events.recv() => {
                            let Some(event) = event else {
                                break;
                            };
                            match handle_agent_event(&mut service, event).await {
                                Ok(outgoing) => {
                                    if !emit(&events, outgoing) {
                                        return;
                                    }
                                }
                                Err(err) => {
                                    error!(error = %err, "agent event failed");
                                    if !emit(&events, vec![Envelope::ApiBadRequest(ApiBadRequest {
                                        msg: err.to_string(),
                                    })]) {
                                        return;
                                    }
                                }
                            }
                            flush_agent(&mut service, &agent, &events).await;
                        }
                    }
                }
            });
        });

    if let Err(err) = result {
        error!(error = %err, "failed to spawn the core thread");
    }
}

async fn handle_agent_event(
    service: &mut Service,
    event: AgentEvent,
) -> Result<Vec<Envelope>, ServiceError> {
    match event {
        AgentEvent::PeerDiscovered(device) | AgentEvent::PeerOffline(device) => {
            service.handle(Envelope::SetPeer(device))
        }
        AgentEvent::PeerMessage { peer_did, message } => service.handle_peer(&peer_did, message),
        AgentEvent::TransferEvent(event) => service.apply_transfer_event(event),
        AgentEvent::Error(msg) => Ok(vec![Envelope::ApiBadRequest(ApiBadRequest { msg })]),
    }
}

async fn flush_agent(service: &mut Service, agent: &AgentHandle, events: &Sender<Envelope>) {
    for outbound in service.drain_peer_outbox() {
        if !agent
            .send(AgentCommand::SendPeer {
                peer_did: outbound.peer_did,
                message: outbound.message,
            })
            .await
        {
            let _ = events.send(Envelope::ApiBadRequest(ApiBadRequest {
                msg: "Network agent stopped".into(),
            }));
            return;
        }
    }

    for transfer in service.ready_transfers() {
        let Some(peer_did) = service.device_did(transfer.peer_id) else {
            continue;
        };
        match transfer.role {
            Role::Src => {
                let _ = agent
                    .send(AgentCommand::StartSend { peer_did, transfer })
                    .await;
            }
            Role::Dst => {
                let files = service.transfer_files(transfer.tid);
                if files.is_empty() || files.iter().any(|file| file.final_path.is_none()) {
                    continue;
                }
                let _ = agent
                    .send(AgentCommand::StartReceive {
                        peer_did,
                        transfer,
                        files,
                    })
                    .await;
            }
        }
    }
}

fn emit(events: &Sender<Envelope>, outgoing: Vec<Envelope>) -> bool {
    for envelope in outgoing {
        if events.send(envelope).is_err() {
            return false;
        }
    }
    true
}
