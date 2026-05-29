mod error;
mod executor;
mod room;
mod ws;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use room::RoomRegistry;
use tower_http::cors::CorsLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pressly=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let registry = Arc::new(RoomRegistry::new());
    {
        let reg = registry.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                let removed = reg.reap_expired();
                if removed > 0 {
                    tracing::info!("reaped {} expired rooms", removed);
                }
            }
        });
    }

    let app = Router::new()
        .route("/health", get(handlers::health))
        .route("/rooms", post(handlers::create_room))
        .route("/rooms/{room_id}", get(handlers::room_info))
        .route("/rooms/{room_id}/ws", get(ws::handler))
        .layer(CorsLayer::permissive())
        .with_state(registry);

    let addr = "0.0.0.0:3001";
    info!("Pressly listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

mod handlers {
    use super::*;
    use crate::room::CreateRoomRequest;

    pub async fn health() -> &'static str {
        "ok"
    }

    pub async fn create_room(
        State(registry): State<Arc<RoomRegistry>>,
        Json(req): Json<CreateRoomRequest>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let room = registry.create(req);
        Ok(Json(serde_json::json!({
            "room_id": room.id,
            "expires_at": room.expires_at,
            "mode": room.mode,
            "language": room.language,
        })))
    }

    pub async fn room_info(
        State(registry): State<Arc<RoomRegistry>>,
        Path(room_id): Path<String>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let room = registry.get(&room_id).ok_or(StatusCode::NOT_FOUND)?;
        Ok(Json(serde_json::json!({
            "room_id": room.id,
            "expires_at": room.expires_at,
            "mode": room.mode,
            "peer_count": room.peer_count(),
            "language": room.language,
        })))
    }
}
