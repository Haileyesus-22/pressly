use std::sync::Arc;

use axum::{
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::room::{Room, RoomMode, RoomRegistry};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsMessage {
    Welcome {
        peer_id: String,
        room_id: String,
        snapshot: String,
        language: String,
    },
    Edit {
        peer_id: String,
        content: String,
        cursor_line: Option<u32>,
        cursor_col: Option<u32>,
    },
    OutputLine {
        run_id: String,
        line: String,
        stream: OutputStream,
    },
    RunResult {
        run_id: String,
        exit_code: i32,
        duration_ms: u64,
        diff: Vec<DiffHunk>,
        timed_out: bool,
    },
    PeerEvent {
        peer_id: String,
        event: PeerEventKind,
        peer_count: usize,
    },
    Error {
        code: String,
        message: String,
    },
    ClientEdit {
        content: String,
        cursor_line: Option<u32>,
        cursor_col: Option<u32>,
    },
    RunRequest {
        run_id: String,
    },
    WitnessHello,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerEventKind {
    Joined,
    Left,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffHunk {
    pub kind: DiffKind,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffKind {
    Equal,
    Added,
    Removed,
}

#[derive(Debug, Deserialize)]
pub struct WsQuery {
    pub role: Option<String>,
}

pub async fn handler(
    ws: WebSocketUpgrade,
    Path(room_id): Path<String>,
    Query(query): Query<WsQuery>,
    State(registry): State<Arc<RoomRegistry>>,
) -> impl IntoResponse {
    let room = match registry.get(&room_id) {
        Some(r) => r,
        None => {
            return ws.on_upgrade(|mut socket| async move {
                let _ = send_error(
                    &mut socket,
                    "room_not_found",
                    "room does not exist or has expired",
                )
                .await;
            });
        }
    };

    if room.is_expired() {
        return ws.on_upgrade(|mut socket| async move {
            let _ = send_error(&mut socket, "room_expired", "this room has expired").await;
        });
    }

    let is_witness = query.role.as_deref() == Some("witness") || room.mode == RoomMode::Witness;
    ws.on_upgrade(move |socket| peer_loop(socket, room, is_witness))
}

async fn peer_loop(mut socket: WebSocket, room: Arc<Room>, is_witness: bool) {
    let peer_id = Uuid::new_v4().to_string()[..8].to_string();
    let _guard = room.peer_joined();
    let mut rx = room.subscribe();

    info!(
        "peer {} joined room {} (witness={})",
        peer_id, room.id, is_witness
    );

    {
        let snap = room.snapshot.lock().await;
        let welcome = WsMessage::Welcome {
            peer_id: peer_id.clone(),
            room_id: room.id.clone(),
            snapshot: snap.content.clone(),
            language: snap.language.clone(),
        };
        if socket.send(to_ws_msg(&welcome)).await.is_err() {
            return;
        }
    }

    room.broadcast(WsMessage::PeerEvent {
        peer_id: peer_id.clone(),
        event: PeerEventKind::Joined,
        peer_count: room.peer_count(),
    });

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        if socket.send(to_ws_msg(&msg)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("peer {} lagged {} messages", peer_id, n);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            result = socket.recv() => {
                match result {
                    Some(Ok(Message::Text(text))) => {
                        handle_client_message(&text, &peer_id, &room, is_witness).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(_))) => debug!("ping from {}", peer_id),
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        warn!("ws error from {}: {}", peer_id, e);
                        break;
                    }
                }
            }
        }
    }

    room.broadcast(WsMessage::PeerEvent {
        peer_id: peer_id.clone(),
        event: PeerEventKind::Left,
        peer_count: room.peer_count().saturating_sub(1),
    });

    info!("peer {} left room {}", peer_id, room.id);
}

async fn handle_client_message(raw: &str, peer_id: &str, room: &Arc<Room>, is_witness: bool) {
    let msg: WsMessage = match serde_json::from_str(raw) {
        Ok(m) => m,
        Err(e) => {
            warn!("bad message from {}: {}", peer_id, e);
            return;
        }
    };

    match msg {
        WsMessage::ClientEdit {
            content,
            cursor_line,
            cursor_col,
        } => {
            if is_witness {
                return;
            }
            {
                let mut snap = room.snapshot.lock().await;
                snap.content = content.clone();
            }
            room.broadcast(WsMessage::Edit {
                peer_id: peer_id.to_string(),
                content,
                cursor_line,
                cursor_col,
            });
        }
        WsMessage::RunRequest { run_id } => {
            if is_witness {
                return;
            }

            let snapshot = {
                let snap = room.snapshot.lock().await;
                (snap.content.clone(), snap.language.clone(), snap.last_output.clone())
            };
            let (code, language, last_output) = snapshot;
            let previous_output = last_output.map(|o| o.stdout);

            if code.trim().is_empty() {
                room.broadcast(WsMessage::Error {
                    code: "empty_code".into(),
                    message: "Nothing to run — editor is empty".into(),
                });
                return;
            }

            let room_clone = room.clone();
            let run_id_clone = run_id.clone();

            tokio::spawn(async move {
                let req = crate::executor::ExecutionRequest {
                    run_id: run_id_clone.clone(),
                    language,
                    code,
                    previous_output,
                };

                let result = crate::executor::execute(req).await;

                for line in result.stdout.lines() {
                    room_clone.broadcast(WsMessage::OutputLine {
                        run_id: run_id_clone.clone(),
                        line: line.to_string(),
                        stream: OutputStream::Stdout,
                    });
                }

                for line in result.stderr.lines() {
                    room_clone.broadcast(WsMessage::OutputLine {
                        run_id: run_id_clone.clone(),
                        line: line.to_string(),
                        stream: OutputStream::Stderr,
                    });
                }

                {
                    let mut snap = room_clone.snapshot.lock().await;
                    snap.last_output = Some(crate::room::RunOutput {
                        stdout: result.stdout.clone(),
                        stderr: result.stderr.clone(),
                        exit_code: result.exit_code.unwrap_or(1),
                        duration_ms: result.duration_ms,
                        ran_at: Utc::now(),
                    });
                }

                room_clone.broadcast(WsMessage::RunResult {
                    run_id: run_id_clone,
                    exit_code: result.exit_code.unwrap_or(1),
                    duration_ms: result.duration_ms,
                    diff: result.diff,
                    timed_out: result.timed_out,
                });
            });
        }
        WsMessage::WitnessHello => {
            debug!("witness {} acknowledged read-only mode", peer_id);
        }
        _ => warn!("unexpected client message type from {}", peer_id),
    }
}

fn to_ws_msg(msg: &WsMessage) -> Message {
    Message::Text(serde_json::to_string(msg).unwrap_or_default().into())
}

async fn send_error(socket: &mut WebSocket, code: &str, message: &str) -> Result<(), axum::Error> {
    let err = WsMessage::Error {
        code: code.to_string(),
        message: message.to_string(),
    };
    socket
        .send(Message::Text(
            serde_json::to_string(&err).unwrap_or_default().into(),
        ))
        .await
}
