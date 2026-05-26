use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};
use uuid::Uuid;

use crate::ws::WsMessage;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RoomMode {
    Collab,
    Witness,
    DeadDrop,
}

#[derive(Debug, Clone, Default)]
pub struct EditorSnapshot {
    pub content: String,
    pub language: String,
    pub last_output: Option<RunOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub duration_ms: u64,
    pub ran_at: DateTime<Utc>,
}

pub struct Room {
    pub id: String,
    pub mode: RoomMode,
    pub language: String,
    pub expires_at: DateTime<Utc>,
    pub ttl: Duration,
    pub tx: broadcast::Sender<WsMessage>,
    pub snapshot: Arc<Mutex<EditorSnapshot>>,
    peer_count: Arc<AtomicUsize>,
}

impl Room {
    pub fn new(id: String, mode: RoomMode, language: String, ttl_secs: u64) -> Self {
        let (tx, _) = broadcast::channel(256);
        let ttl = Duration::from_secs(ttl_secs);

        Self {
            id,
            mode,
            language: language.clone(),
            expires_at: Utc::now() + chrono::Duration::seconds(ttl_secs as i64),
            ttl,
            tx,
            snapshot: Arc::new(Mutex::new(EditorSnapshot {
                content: String::new(),
                language,
                last_output: None,
            })),
            peer_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }

    pub fn peer_count(&self) -> usize {
        self.peer_count.load(Ordering::Relaxed)
    }

    pub fn peer_joined(&self) -> PeerGuard {
        self.peer_count.fetch_add(1, Ordering::Relaxed);
        PeerGuard {
            count: self.peer_count.clone(),
        }
    }

    pub fn broadcast(&self, msg: WsMessage) {
        let _ = self.tx.send(msg);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WsMessage> {
        self.tx.subscribe()
    }
}

pub struct PeerGuard {
    count: Arc<AtomicUsize>,
}

impl Drop for PeerGuard {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct RoomRegistry {
    rooms: DashMap<String, Arc<Room>>,
}

impl RoomRegistry {
    pub fn new() -> Self {
        Self {
            rooms: DashMap::new(),
        }
    }

    pub fn create(&self, req: CreateRoomRequest) -> Arc<Room> {
        let id = format!("{}-{}", adjective(), &Uuid::new_v4().to_string()[..6]);
        let ttl_secs = req.ttl_minutes.unwrap_or(120) * 60;
        let language = req.language.unwrap_or_else(|| "rust".into());
        let mode = req.mode.unwrap_or(RoomMode::Collab);

        let room = Arc::new(Room::new(id.clone(), mode, language, ttl_secs));
        self.rooms.insert(id, room.clone());
        room
    }

    pub fn get(&self, id: &str) -> Option<Arc<Room>> {
        self.rooms.get(id).map(|r| r.clone())
    }

    pub fn reap_expired(&self) -> usize {
        let before = self.rooms.len();
        self.rooms.retain(|_, room| !room.is_expired());
        before - self.rooms.len()
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateRoomRequest {
    pub language: Option<String>,
    pub mode: Option<RoomMode>,
    pub ttl_minutes: Option<u64>,
}

fn adjective() -> &'static str {
    const WORDS: &[&str] = &[
        "swift", "amber", "quiet", "bold", "crisp", "dim", "eager", "faint", "grand", "hasty",
        "idle", "jolly", "keen", "lush", "muted", "noble", "oddly", "prime", "quick", "rapid",
        "sharp", "terse", "ultra", "vivid", "witty", "xenon", "young", "zesty",
    ];

    let idx = (Uuid::new_v4().as_u128() % WORDS.len() as u128) as usize;
    WORDS[idx]
}
