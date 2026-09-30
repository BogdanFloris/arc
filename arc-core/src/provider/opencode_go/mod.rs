use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::header::{ACCEPT, AUTHORIZATION};
use serde::Deserialize;

use crate::provider::{AccountAllowance, AllowanceWindow, Error};

const CACHE_PERIOD: Duration = Duration::from_secs(60);
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
pub(crate) struct Cache {
    attempted_at: Option<tokio::time::Instant>,
    snapshot: Option<AccountAllowance>,
}

#[tracing::instrument(name = "opencode_go.allowance", skip_all)]
pub(crate) async fn fetch(
    endpoint: &str,
    key: Option<&str>,
    http: &reqwest::Client,
    cache: &tokio::sync::Mutex<Cache>,
) -> Result<Option<AccountAllowance>, Error> {
    let Some(endpoint) = go_usage_endpoint(endpoint) else {
        return Ok(None);
    };
    let mut cache = cache.lock().await;
    if cache
        .attempted_at
        .is_some_and(|at| at.elapsed() < CACHE_PERIOD)
    {
        return cache.snapshot.clone().map(Some).ok_or_else(|| {
            Error::Refused("OpenCode Go allowance temporarily unavailable".to_owned())
        });
    }
    cache.attempted_at = Some(tokio::time::Instant::now());
    let result = tokio::time::timeout(FETCH_TIMEOUT, fetch_from_endpoint(http, key, &endpoint))
        .await
        .unwrap_or_else(|_| Err(Error::Refused("OpenCode Go allowance timed out".to_owned())));
    cache.attempted_at = Some(tokio::time::Instant::now());
    match result {
        Ok(snapshot) => {
            cache.snapshot = Some(snapshot.clone());
            Ok(Some(snapshot))
        }
        Err(error) => match cache.snapshot.as_mut() {
            Some(snapshot) => {
                snapshot.stale = true;
                Ok(Some(snapshot.clone()))
            }
            None => Err(error),
        },
    }
}

async fn fetch_from_endpoint(
    http: &reqwest::Client,
    key: Option<&str>,
    endpoint: &str,
) -> Result<AccountAllowance, Error> {
    let key =
        key.ok_or_else(|| Error::Auth("OpenCode Go allowance requires an API key".to_owned()))?;
    let response = http
        .get(endpoint)
        .header(ACCEPT, "application/json")
        .header(AUTHORIZATION, format!("Bearer {key}"))
        .timeout(FETCH_TIMEOUT)
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(match response.status().as_u16() {
            401 | 403 => Error::Auth(format!(
                "OpenCode Go allowance rejected with HTTP {}",
                response.status().as_u16()
            )),
            status => Error::http(status, "OpenCode Go allowance request failed"),
        });
    }
    let payload: Payload = response
        .json()
        .await
        .map_err(|_| Error::Refused("invalid OpenCode Go allowance response".to_owned()))?;
    Ok(payload.into_allowance())
}

pub fn is_endpoint(endpoint: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(endpoint) else {
        return false;
    };
    url.scheme() == "https"
        && url.host_str() == Some("opencode.ai")
        && url.path().trim_end_matches('/') == "/zen/go"
}

fn go_usage_endpoint(endpoint: &str) -> Option<String> {
    is_endpoint(endpoint).then(|| format!("{}/v1/usage", endpoint.trim_end_matches('/')))
}

#[derive(Deserialize)]
struct Payload {
    usage: Usage,
}

#[derive(Deserialize)]
struct Usage {
    rolling: Window,
    weekly: Window,
    monthly: Window,
}

#[derive(Deserialize)]
struct Window {
    percent: Option<f64>,
    #[serde(rename = "resetsAt")]
    resets_at: Option<String>,
}

impl Payload {
    fn into_allowance(self) -> AccountAllowance {
        AccountAllowance {
            observed_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|duration| i64::try_from(duration.as_secs()).ok())
                .unwrap_or_default(),
            stale: false,
            primary: normalize(self.usage.rolling, None, Some("rolling")),
            secondary: normalize(self.usage.weekly, Some(604_800), Some("weekly")),
            tertiary: normalize(self.usage.monthly, None, Some("monthly")),
        }
    }
}

fn normalize(window: Window, seconds: Option<u64>, label: Option<&str>) -> Option<AllowanceWindow> {
    let used = window
        .percent
        .filter(|value| value.is_finite() && *value >= 0.0)?;
    Some(AllowanceWindow {
        remaining_percent: (100.0 - used).clamp(0.0, 100.0),
        window_seconds: seconds,
        resets_at_unix_seconds: window
            .resets_at
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
            .map(|value| value.timestamp())
            .filter(|value| *value > 0),
        label: label.map(str::to_owned),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn sends_authenticated_usage_request_and_parses_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/zen/go/v1/usage"))
            .and(header("authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "usage": {
                    "rolling": {
                        "status": "ok",
                        "percent": 25,
                        "resetsAt": "2026-10-02T03:04:05Z"
                    },
                    "weekly": {"status": "ok", "percent": 50},
                    "monthly": {"status": "ok", "percent": 75}
                }
            })))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let allowance = fetch_from_endpoint(
            &http,
            Some("test-key"),
            &format!("{}/zen/go/v1/usage", server.uri()),
        )
        .await
        .expect("allowance response");
        assert!(
            (allowance.primary.as_ref().unwrap().remaining_percent - 75.0).abs() < f64::EPSILON
        );
        assert_eq!(
            allowance.primary.as_ref().unwrap().resets_at_unix_seconds,
            Some(
                chrono::DateTime::parse_from_rfc3339("2026-10-02T03:04:05Z")
                    .unwrap()
                    .timestamp()
            )
        );
    }

    #[test]
    fn parses_official_go_usage_shape_and_reset_timestamps() {
        let payload: Payload = serde_json::from_value(serde_json::json!({
            "usage": {
                "rolling": {"status": "ok", "percent": 25, "resetsAt": "2026-10-02T03:04:05Z"},
                "weekly": {"status": "ok", "percent": 50, "resetsAt": "2026-10-03T03:04:05Z"},
                "monthly": {"status": "ok", "percent": 75, "resetsAt": "2026-11-01T03:04:05Z"}
            }
        }))
        .expect("official usage response");
        let allowance = payload.into_allowance();
        assert!(
            (allowance.primary.as_ref().unwrap().remaining_percent - 75.0).abs() < f64::EPSILON
        );
        assert_eq!(
            allowance.primary.as_ref().unwrap().resets_at_unix_seconds,
            Some(
                chrono::DateTime::parse_from_rfc3339("2026-10-02T03:04:05Z")
                    .unwrap()
                    .timestamp()
            )
        );
        assert_eq!(
            allowance.secondary.as_ref().unwrap().window_seconds,
            Some(604_800)
        );
        assert_eq!(
            allowance.tertiary.as_ref().unwrap().label.as_deref(),
            Some("monthly")
        );
    }

    #[test]
    fn missing_percent_leaves_that_window_unknown() {
        let payload: Payload = serde_json::from_value(serde_json::json!({
            "usage": {
                "rolling": {"status": "unknown", "resetsAt": "2026-10-02T03:04:05Z"},
                "weekly": {"percent": 20},
                "monthly": {"percent": 30}
            }
        }))
        .expect("missing percent is valid");
        assert!(payload.into_allowance().primary.is_none());
    }

    #[test]
    fn only_https_opencode_go_endpoint_is_eligible() {
        assert_eq!(
            go_usage_endpoint("https://opencode.ai/zen/go"),
            Some("https://opencode.ai/zen/go/v1/usage".to_owned())
        );
        assert!(go_usage_endpoint("http://opencode.ai/zen/go").is_none());
    }
}
