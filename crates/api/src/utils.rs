use crate::primitives::{Response, Status};
use eyre::{eyre, Result};
use reqwest::Url;
use serde::Deserialize;

pub fn join_url_path(base_url: &Url, path_segments: &[&str]) -> Result<Url> {
    let mut url = base_url.clone();

    if !url.path().ends_with('/') {
        let current_path = url.path().to_string();
        url.set_path(&format!("{current_path}/"));
    }

    for segment in path_segments {
        let trimmed_segment = segment.trim_matches('/');
        url = url
            .join(&format!("{trimmed_segment}/"))
            .map_err(|e| eyre!("Failed to join URL segment '{trimmed_segment}': {e}"))?;
    }

    let current_path = url.path().to_string();
    if current_path.ends_with('/') && current_path.len() > 1 {
        url.set_path(&current_path[..current_path.len() - 1]);
    }

    Ok(url)
}

pub async fn handle_response<T>(response: reqwest::Response) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status().as_u16();
    let text = response.text().await?;

    let resp: Response<T> = serde_json::from_str(&text)
        .map_err(|_| eyre!("JSON parse failed (status: {status}): {text}"))?;

    match resp.status {
        Status::Ok => resp.result.ok_or_else(|| eyre!("empty response")),
        Status::Error => {
            let error_message = resp.error.unwrap_or_else(|| "Unknown error".to_string());
            Err(eyre!("Executor error: {error_message}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        value: String,
        count: i32,
    }

    async fn setup_mock_server(body: &str, status_code: u16) -> MockServer {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(status_code)
                    .set_body_string(body)
                    .insert_header("content-type", "application/json"),
            )
            .mount(&mock_server)
            .await;

        mock_server
    }

    #[tokio::test]
    async fn test_handle_response_success() {
        let json_body = r#"{"status":"Ok","result":{"value":"test","count":42}}"#;
        let mock_server = setup_mock_server(json_body, 200).await;

        let response = reqwest::get(mock_server.uri()).await.unwrap();
        let result: TestData = handle_response(response).await.unwrap();

        assert_eq!(
            result,
            TestData {
                value: "test".to_string(),
                count: 42,
            }
        );
    }

    #[test]
    fn test_join_url_path() {
        let base = Url::parse("http://localhost:8080/api").unwrap();
        let joined = join_url_path(&base, &["v1", "orders", "123"]).unwrap();
        assert_eq!(joined.as_str(), "http://localhost:8080/api/v1/orders/123");
    }
}
