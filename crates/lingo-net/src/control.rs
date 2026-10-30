//! Authenticated control-channel driver.

use tokio::sync::mpsc;
use tracing::{debug, warn};

use lingo_core::model::DeviceId;
use lingo_core::msg::{PeerMessage, decode_peer_message, encode_peer_message};

use crate::session::{Session, SessionError};

pub const CONTROL_CHANNEL_CAPACITY: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("control channel is closed")]
    Closed,
    #[error("control channel is full")]
    Full,
}

#[derive(Clone)]
pub struct ControlHandle {
    pub peer_did: DeviceId,
    pub connection: quinn::Connection,
    outbound: mpsc::Sender<PeerMessage>,
}

impl ControlHandle {
    pub async fn send(&self, message: PeerMessage) -> Result<(), ControlError> {
        self.outbound
            .send(message)
            .await
            .map_err(|_| ControlError::Closed)
    }

    pub fn try_send(&self, message: PeerMessage) -> Result<(), ControlError> {
        self.outbound.try_send(message).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => ControlError::Full,
            mpsc::error::TrySendError::Closed(_) => ControlError::Closed,
        })
    }
}

/// Spawn a task that owns the control stream and multiplexes sends and receives.
pub fn spawn(mut session: Session) -> (ControlHandle, mpsc::Receiver<PeerMessage>) {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(CONTROL_CHANNEL_CAPACITY);
    let (inbound_tx, inbound_rx) = mpsc::channel(CONTROL_CHANNEL_CAPACITY);
    let handle = ControlHandle {
        peer_did: session.peer_did().clone(),
        connection: session.connection().clone(),
        outbound: outbound_tx,
    };

    tokio::spawn(async move {
        loop {
            tokio::select! {
                outgoing = outbound_rx.recv() => {
                    let Some(message) = outgoing else {
                        break;
                    };
                    let bytes = match encode_peer_message(&message) {
                        Ok(bytes) => bytes,
                        Err(err) => {
                            warn!(error = %err, kind = message.kind(), "failed to encode peer message");
                            continue;
                        }
                    };
                    if let Err(err) = session.send(&bytes).await {
                        warn!(error = %err, "failed to send peer message");
                        break;
                    }
                }
                incoming = session.recv() => {
                    match incoming {
                        Ok(bytes) => {
                            let message = match decode_peer_message(&bytes) {
                                Ok(message) => message,
                                Err(err) => {
                                    warn!(error = %err, "failed to decode peer message");
                                    continue;
                                }
                            };
                            if inbound_tx.send(message).await.is_err() {
                                break;
                            }
                        }
                        Err(err) => {
                            if !matches!(err, SessionError::Frame(_)) {
                                debug!(error = %err, "control stream ended");
                            }
                            break;
                        }
                    }
                }
            }
        }
        session.close(0, b"control closed");
    });

    (handle, inbound_rx)
}
