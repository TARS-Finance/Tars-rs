use axum::{http::StatusCode, response::IntoResponse, response::Response as AxumResponse, Json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Status {
    Ok,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Response<T> {
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip)]
    pub status_code: StatusCode,
}

impl<T> Response<T> {
    pub fn ok(data: T) -> Self {
        Self {
            status: Status::Ok,
            result: Some(data),
            error: None,
            status_code: StatusCode::OK,
        }
    }

    pub fn ok_with_status(data: T, status_code: StatusCode) -> Self {
        Self {
            status: Status::Ok,
            result: Some(data),
            error: None,
            status_code,
        }
    }

    pub fn error<E: ToString>(error: E, status_code: StatusCode) -> Self {
        Self {
            status: Status::Error,
            result: None,
            error: Some(error.to_string()),
            status_code,
        }
    }

    pub fn into_json(self) -> Json<Self> {
        Json(self)
    }
}

impl<T> IntoResponse for Response<T>
where
    T: serde::Serialize,
{
    fn into_response(self) -> AxumResponse {
        let status_code = self.status_code;
        let mut response = Json(self).into_response();
        *response.status_mut() = status_code;
        response
    }
}

pub type ApiResult<T> = Result<Response<T>, Response<()>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Error {
    pub code: u32,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResponseLegacy<T> {
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
}

#[cfg(test)]
mod tests {
    use super::{Response, Status};
    use axum::{http::StatusCode, response::IntoResponse};

    #[test]
    fn test_response_ok() {
        let response = Response::ok("test data");
        let body = response.into_json();
        assert_eq!(body.status, Status::Ok);
        assert_eq!(body.result, Some("test data"));
        assert_eq!(body.error, None);
    }

    #[test]
    fn test_response_error() {
        let response = Response::<String>::error("test error", StatusCode::OK);
        let body = response.into_json();
        assert_eq!(body.status, Status::Error);
        assert_eq!(body.result, None);
        assert_eq!(body.error, Some("test error".to_string()));
    }

    #[test]
    fn test_response_into_response() {
        let response = Response::ok("test data");
        let response = response.into_response();
        assert!(response.headers().contains_key("content-type"));
        assert_eq!(response.headers()["content-type"], "application/json");
    }
}
