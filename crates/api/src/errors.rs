use crate::primitives::Response;
use axum::http::StatusCode;
use tracing::{error, warn};

const INTERNAL_ERROR: &str = "Internal Error";

pub fn internal_error(message: &str) -> Response<()> {
    error!(
        status = StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        "{message:#?}"
    );
    Response::error(INTERNAL_ERROR, StatusCode::INTERNAL_SERVER_ERROR)
}

pub fn bad_request(message: &str) -> Response<()> {
    warn!(status = StatusCode::BAD_REQUEST.as_u16(), "{message:#?}");
    Response::error(message, StatusCode::BAD_REQUEST)
}

pub fn not_found(message: &str) -> Response<()> {
    error!(status = StatusCode::NOT_FOUND.as_u16(), "{message:#?}");
    Response::error(message, StatusCode::NOT_FOUND)
}
