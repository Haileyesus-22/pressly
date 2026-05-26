use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("room not found: {0}")]
    RoomNotFound(String),
    #[error("room expired: {0}")]
    RoomExpired(String),
    #[error("forbidden: witnesses cannot perform this action")]
    WitnessForbidden,
    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            AppError::RoomNotFound(_) => (StatusCode::NOT_FOUND, "room_not_found"),
            AppError::RoomExpired(_) => (StatusCode::GONE, "room_expired"),
            AppError::WitnessForbidden => (StatusCode::FORBIDDEN, "forbidden"),
            AppError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        };

        (
            status,
            Json(json!({
                "error": code,
                "message": self.to_string(),
            })),
        )
            .into_response()
    }
}
