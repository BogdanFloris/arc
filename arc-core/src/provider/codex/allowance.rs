use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, USER_AGENT};
use serde::Deserialize;

use super::{ACCOUNT_HEADER, Codex};
use crate::provider::{AccountAllowance, AllowanceWindow, Error};

const USAGE_PATH: &str = "/wham/usage";
const CACHE_PERIOD: Duration = Duration::from_secs(60);
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
pub(super) struct Cache {
    attempted_at: Option<tokio::time::Instant>,
    snapshot: Option<AccountAllowance>,
}

impl Codex {
    #[tracing::instrument(name = "codex.allowance", skip_all)]
    pub async fn allowance(&self) -> Result<AccountAllowance, Error> {
        let mut cache = self.allowance_cache.lock().await;
        if cache
            .attempted_at
            .is_some_and(|at| at.elapsed() < CACHE_PERIOD)
        {
            return cache.snapshot.clone().ok_or_else(|| {
                Error::Refused("codex allowance temporarily unavailable".to_owned())
            });
        }
        cache.attempted_at = Some(tokio::time::Instant::now());
        let result = tokio::time::timeout(FETCH_TIMEOUT, self.fetch_allowance())
            .await
            .unwrap_or_else(|_| Err(Error::Refused("codex allowance timed out".to_owned())));
        cache.attempted_at = Some(tokio::time::Instant::now());
        match result {
            Ok(snapshot) => {
                cache.snapshot = Some(snapshot.clone());
                Ok(snapshot)
            }
            Err(error) => {
                if let Some(snapshot) = cache.snapshot.as_mut() {
                    snapshot.stale = true;
                    Ok(snapshot.clone())
                } else {
                    Err(error)
                }
            }
        }
    }

    async fn fetch_allowance(&self) -> Result<AccountAllowance, Error> {
        let mut response = self.send_allowance().await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            self.tokens.refresh().await.map_err(safe_auth_error)?;
            response = self.send_allowance().await?;
        }
        let status = response.status();
        if !status.is_success() {
            return Err(match status.as_u16() {
                401 | 403 => Error::Auth(format!("codex allowance rejected with HTTP {status}")),
                429 => Error::RateLimited {
                    retry_after: response
                        .headers()
                        .get("retry-after")
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse().ok()),
                    detail: "codex allowance request rate limited".to_owned(),
                },
                status => Error::http(status, "codex allowance request failed"),
            });
        }
        let body = response.bytes().await?;
        let payload: Payload = serde_json::from_slice(&body)
            .map_err(|_| Error::Refused("invalid codex allowance response".to_owned()))?;
        let limits = payload.rate_limit.unwrap_or_default();
        Ok(AccountAllowance {
            observed_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|duration| i64::try_from(duration.as_secs()).ok())
                .unwrap_or_default(),
            stale: false,
            primary: limits.primary_window.and_then(Window::normalize),
            secondary: limits.secondary_window.and_then(Window::normalize),
        })
    }

    async fn send_allowance(&self) -> Result<reqwest::Response, Error> {
        let (token, account) = self.tokens.bearer().await.map_err(safe_auth_error)?;
        Ok(self
            .http
            .get(format!("{}{USAGE_PATH}", self.endpoint))
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(ACCOUNT_HEADER, account)
            .header("originator", "arc")
            .header(USER_AGENT, format!("arc/{}", crate::VERSION))
            .timeout(Duration::from_secs(30))
            .send()
            .await?)
    }
}

fn safe_auth_error(error: Error) -> Error {
    match error {
        Error::Auth(_) => Error::Auth(
            "codex allowance authentication failed; run `arcd login codex` to sign in again"
                .to_owned(),
        ),
        other => other,
    }
}

#[derive(Deserialize)]
struct Payload {
    rate_limit: Option<Limits>,
}

#[derive(Default, Deserialize)]
struct Limits {
    primary_window: Option<Window>,
    secondary_window: Option<Window>,
}

#[derive(Deserialize)]
struct Window {
    used_percent: Option<f64>,
    limit_window_seconds: Option<i64>,
    reset_at: Option<i64>,
}

impl Window {
    fn normalize(self) -> Option<AllowanceWindow> {
        let used = self
            .used_percent
            .filter(|used| used.is_finite() && *used >= 0.0)?;
        Some(AllowanceWindow {
            remaining_percent: (100.0 - used).clamp(0.0, 100.0),
            window_seconds: self
                .limit_window_seconds
                .filter(|seconds| *seconds > 0)
                .and_then(|seconds| u64::try_from(seconds).ok()),
            resets_at_unix_seconds: self.reset_at.filter(|seconds| *seconds > 0),
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::provider::Provider;
    use crate::provider::codex::auth::{Credential, fake_access_token};
    use crate::secrets::Secrets;

    fn provider(dir: &std::path::Path, server: &MockServer) -> Codex {
        let secrets = Secrets::new(dir);
        secrets
            .write(
                "codex",
                &Credential {
                    access_token: fake_access_token("acct_old"),
                    refresh_token: "test-refresh".to_owned(),
                    expires_at: u64::MAX,
                }
                .to_json(),
            )
            .unwrap();
        Codex::with_auth(&server.uri(), secrets, "codex", &server.uri()).unwrap()
    }

    #[tokio::test]
    async fn fetches_authenticated_remote_windows_through_the_trait() {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        Mock::given(method("GET"))
            .and(path(USAGE_PATH))
            .and(header(
                "authorization",
                format!("Bearer {}", fake_access_token("acct_old")),
            ))
            .and(header(ACCOUNT_HEADER, "acct_old"))
            .and(header("accept", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "plan_type": "pro", "rate_limit": {
                    "allowed": true, "limit_reached": false,
                    "primary_window": {"used_percent": 25, "limit_window_seconds": 18000,
                        "reset_after_seconds": 600, "reset_at": 1_900_000_000},
                    "secondary_window": {"used_percent": 100, "limit_window_seconds": 604_800,
                        "reset_at": 1_900_600_000}
                }, "credits": {"balance": "not retained"}
            })))
            .expect(1)
            .mount(&server)
            .await;
        let codex = provider(dir.path(), &server);
        let provider: &dyn Provider = &codex;
        let result = provider.allowance().await.unwrap().unwrap();
        assert_eq!(
            result.primary,
            Some(AllowanceWindow {
                remaining_percent: 75.0,
                window_seconds: Some(18000),
                resets_at_unix_seconds: Some(1_900_000_000),
            })
        );
        assert!(result.secondary.unwrap().remaining_percent.abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn absent_or_unknown_windows_never_become_zero() {
        for body in [
            json!({}),
            json!({"rate_limit": null}),
            json!({"rate_limit": {"primary_window": null}}),
            json!({"rate_limit": {"primary_window": {"reset_at": 1_900_000_000}}}),
            json!({"rate_limit": {"primary_window": {"used_percent": -1}}}),
        ] {
            let server = MockServer::start().await;
            let dir = tempfile::tempdir().unwrap();
            Mock::given(path(USAGE_PATH))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
            let result = provider(dir.path(), &server).allowance().await.unwrap();
            assert!(result.primary.is_none());
            assert!(result.secondary.is_none());
            assert!(result.observed_at > 0);
            assert!(!result.stale);
        }
    }

    #[tokio::test]
    async fn percent_does_not_require_reset_metadata() {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        Mock::given(path(USAGE_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    json!({"rate_limit": {"secondary_window": {"used_percent": 10.5}}}),
                ),
            )
            .mount(&server)
            .await;
        let result = provider(dir.path(), &server).allowance().await.unwrap();
        assert_eq!(
            result.secondary,
            Some(AllowanceWindow {
                remaining_percent: 89.5,
                window_seconds: None,
                resets_at_unix_seconds: None,
            })
        );
    }

    #[tokio::test]
    async fn refreshes_once_and_uses_the_new_account() {
        for status in [200, 401] {
            let server = MockServer::start().await;
            let dir = tempfile::tempdir().unwrap();
            Mock::given(path(USAGE_PATH))
                .and(header(ACCOUNT_HEADER, "acct_old"))
                .respond_with(ResponseTemplate::new(401))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/oauth/token"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "access_token": fake_access_token("acct_new"),
                    "refresh_token": "test-new-refresh", "expires_in": 3600
                })))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(path(USAGE_PATH))
                .and(header(ACCOUNT_HEADER, "acct_new"))
                .and(header(
                    "authorization",
                    format!("Bearer {}", fake_access_token("acct_new")),
                ))
                .respond_with(ResponseTemplate::new(status).set_body_json(json!({})))
                .expect(1)
                .mount(&server)
                .await;
            let result = provider(dir.path(), &server).allowance().await;
            if status == 200 {
                assert!(result.is_ok());
            } else {
                assert!(matches!(result, Err(Error::Auth(_))));
            }
        }
    }

    #[tokio::test]
    async fn cache_coalesces_and_preserves_stale_snapshot_until_recovery() {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let codex = provider(dir.path(), &server);
        Mock::given(path(USAGE_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "rate_limit": {"primary_window": {"used_percent": 25}}
            })))
            .expect(1)
            .mount(&server)
            .await;
        let (first, second) = tokio::join!(codex.allowance(), codex.allowance());
        let first = first.unwrap();
        assert_eq!(first, second.unwrap());
        server.reset().await;
        codex.allowance_cache.lock().await.attempted_at =
            Some(tokio::time::Instant::now() - CACHE_PERIOD);
        Mock::given(path(USAGE_PATH))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;
        let stale = codex.allowance().await.unwrap();
        assert!(stale.stale);
        assert_eq!(stale.observed_at, first.observed_at);
        assert_eq!(stale.primary, first.primary);
        assert_eq!(stale, codex.allowance().await.unwrap());
        server.reset().await;
        codex.allowance_cache.lock().await.attempted_at =
            Some(tokio::time::Instant::now() - CACHE_PERIOD);
        Mock::given(path(USAGE_PATH))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"rate_limit": {"primary_window": {"used_percent": 30}}})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let fresh = codex.allowance().await.unwrap();
        assert!(!fresh.stale);
        assert!((fresh.primary.unwrap().remaining_percent - 70.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn failed_initial_fetch_is_throttled() {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let codex = provider(dir.path(), &server);
        Mock::given(path(USAGE_PATH))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;
        assert!(codex.allowance().await.is_err());
        assert!(codex.allowance().await.is_err());
    }

    #[tokio::test]
    async fn errors_do_not_expose_remote_payloads() {
        for status in [200, 403, 429, 500] {
            let server = MockServer::start().await;
            let dir = tempfile::tempdir().unwrap();
            Mock::given(path(USAGE_PATH))
                .respond_with(
                    ResponseTemplate::new(status)
                        .insert_header("retry-after", "17")
                        .set_body_string("private-remote-payload"),
                )
                .expect(1)
                .mount(&server)
                .await;
            let error = provider(dir.path(), &server).allowance().await.unwrap_err();
            assert!(!format!("{error:?}").contains("private-remote-payload"));
            if status == 429 {
                assert!(matches!(
                    error,
                    Error::RateLimited {
                        retry_after: Some(17),
                        ..
                    }
                ));
            }
        }
    }
}
