use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::provider::Error;
use crate::secrets::Secrets;

pub const DEFAULT_AUTH_ENDPOINT: &str = "https://auth.openai.com";

// the Codex CLI's public OAuth client; the ChatGPT plan is bound to it
const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";

const TOKEN_PATH: &str = "/oauth/token";

const DEVICE_USER_CODE_PATH: &str = "/api/accounts/deviceauth/usercode";

const DEVICE_TOKEN_PATH: &str = "/api/accounts/deviceauth/token";

const DEVICE_VERIFICATION_PATH: &str = "/codex/device";

const DEVICE_REDIRECT_PATH: &str = "/deviceauth/callback";

const ACCOUNT_CLAIM: &str = "https://api.openai.com/auth";

const REFRESH_MARGIN: Duration = Duration::from_secs(60);

pub const DEVICE_CODE_TIMEOUT: Duration = Duration::from_secs(15 * 60);

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credential {
    pub access_token: String,

    pub refresh_token: String,

    pub expires_at: u64,
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("access_token", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl Credential {
    pub fn parse(text: &str) -> Result<Self, Error> {
        let credential: Self = serde_json::from_str(text).map_err(|source| {
            Error::Auth(format!(
                "the codex credential is not the JSON `arcd login codex` writes: {source}"
            ))
        })?;
        credential.account_id()?;
        Ok(credential)
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("a credential serializes")
    }

    /// The account the token was issued for, read from the access token's
    /// claims; the Codex backend wants it as a header on every call.
    pub fn account_id(&self) -> Result<String, Error> {
        let payload = self
            .access_token
            .split('.')
            .nth(1)
            .ok_or_else(|| Error::Auth("the codex access token is not a JWT".to_owned()))?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload.trim_end_matches('='))
            .map_err(|_| Error::Auth("the codex access token's claims do not decode".to_owned()))?;
        let claims: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Auth("the codex access token's claims are not JSON".to_owned()))?;
        claims[ACCOUNT_CLAIM]["chatgpt_account_id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| {
                Error::Auth("the codex access token carries no ChatGPT account id".to_owned())
            })
    }

    fn expires_within(&self, margin: Duration) -> bool {
        self.expires_at <= now().saturating_add(margin.as_secs())
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

fn form_encode(pairs: &[(&str, &str)]) -> String {
    fn escape(out: &mut String, value: &str) {
        use std::fmt::Write as _;
        for byte in value.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(byte as char);
                }
                _ => write!(out, "%{byte:02X}").expect("writing to a String"),
            }
        }
    }
    let mut out = String::new();
    for (i, (key, value)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        escape(&mut out, key);
        out.push('=');
        escape(&mut out, value);
    }
    out
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    expires_in: u64,
}

impl TokenResponse {
    fn credential(self) -> Credential {
        Credential {
            access_token: self.access_token,
            refresh_token: self.refresh_token,
            expires_at: now().saturating_add(self.expires_in),
        }
    }
}

async fn token_grant(
    http: &reqwest::Client,
    auth_endpoint: &str,
    form: &[(&str, &str)],
    what: &str,
) -> Result<Credential, Error> {
    let response = http
        .post(format!("{auth_endpoint}{TOKEN_PATH}"))
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body(form_encode(form))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(Error::Auth(format!(
            "codex token {what} failed with HTTP {}: {}",
            status.as_u16(),
            crate::provider::snippet(&body)
        )));
    }
    let parsed: TokenResponse = serde_json::from_str(&body).map_err(|source| {
        Error::Auth(format!(
            "codex token {what} answered without the token fields: {source}"
        ))
    })?;
    let credential = parsed.credential();
    credential.account_id()?;
    Ok(credential)
}

/// The live credential for one account: loaded from the secrets directory,
/// refreshed before it expires, and written back whenever it changes.
pub struct Tokens {
    secrets: Secrets,
    name: String,
    auth_endpoint: String,
    http: reqwest::Client,
    current: Mutex<Credential>,
}

impl std::fmt::Debug for Tokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tokens")
            .field("name", &self.name)
            .field("auth_endpoint", &self.auth_endpoint)
            .finish_non_exhaustive()
    }
}

impl Tokens {
    pub fn open(secrets: Secrets, name: &str, auth_endpoint: &str) -> Result<Self, Error> {
        let text = secrets.read(name).map_err(|source| {
            Error::Auth(format!("{source}; run `arcd login codex` to create it"))
        })?;
        let current = Credential::parse(&text)?;
        Ok(Self {
            secrets,
            name: name.to_owned(),
            auth_endpoint: auth_endpoint.trim_end_matches('/').to_owned(),
            http: reqwest::Client::new(),
            current: Mutex::new(current),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The bearer token and account id to send, refreshed first if it is
    /// about to expire.
    pub async fn bearer(&self) -> Result<(String, String), Error> {
        let mut current = self.current.lock().await;
        if current.expires_within(REFRESH_MARGIN) {
            *current = self.refreshed(&current.refresh_token).await?;
        }
        Ok((current.access_token.clone(), current.account_id()?))
    }

    /// Forced refresh after the backend rejected a token it had accepted.
    pub async fn refresh(&self) -> Result<(), Error> {
        let mut current = self.current.lock().await;
        *current = self.refreshed(&current.refresh_token).await?;
        Ok(())
    }

    async fn refreshed(&self, refresh_token: &str) -> Result<Credential, Error> {
        let credential = token_grant(
            &self.http,
            &self.auth_endpoint,
            &[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token),
                ("client_id", CLIENT_ID),
            ],
            "refresh",
        )
        .await
        .map_err(|error| match error {
            Error::Auth(reason) => {
                Error::Auth(format!("{reason}; run `arcd login codex` to sign in again"))
            }
            other => other,
        })?;
        self.secrets
            .write(&self.name, &credential.to_json())
            .map_err(|source| {
                Error::Auth(format!("saving the refreshed codex credential: {source}"))
            })?;
        tracing::info!(name = %self.name, "codex credential refreshed");
        Ok(credential)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceCode {
    pub user_code: String,

    pub verification_url: String,

    pub interval: Duration,

    device_auth_id: String,
}

#[derive(Deserialize)]
struct DeviceCodeResponse {
    device_auth_id: String,
    user_code: String,
    #[serde(default)]
    interval: serde_json::Value,
}

pub async fn request_device_code(
    http: &reqwest::Client,
    auth_endpoint: &str,
) -> Result<DeviceCode, Error> {
    let auth_endpoint = auth_endpoint.trim_end_matches('/');
    let response = http
        .post(format!("{auth_endpoint}{DEVICE_USER_CODE_PATH}"))
        .json(&serde_json::json!({ "client_id": CLIENT_ID }))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(Error::Auth(format!(
            "codex device code request failed with HTTP {}: {}",
            status.as_u16(),
            crate::provider::snippet(&body)
        )));
    }
    let parsed: DeviceCodeResponse = serde_json::from_str(&body).map_err(|source| {
        Error::Auth(format!(
            "codex device code response is missing fields: {source}"
        ))
    })?;
    // the server has sent the interval both as a number and as a string
    let interval = match &parsed.interval {
        serde_json::Value::Number(n) => n.as_u64().unwrap_or(5),
        serde_json::Value::String(s) => s.trim().parse().unwrap_or(5),
        _ => 5,
    };
    Ok(DeviceCode {
        user_code: parsed.user_code,
        verification_url: format!("{auth_endpoint}{DEVICE_VERIFICATION_PATH}"),
        interval: Duration::from_secs(interval),
        device_auth_id: parsed.device_auth_id,
    })
}

#[derive(Deserialize)]
struct DeviceTokenResponse {
    authorization_code: String,
    code_verifier: String,
}

/// Polls until the user approves the code in a browser, then exchanges the
/// approval for a credential. Gives up after `timeout`.
pub async fn wait_for_device_approval(
    http: &reqwest::Client,
    auth_endpoint: &str,
    device: &DeviceCode,
    timeout: Duration,
) -> Result<Credential, Error> {
    let auth_endpoint = auth_endpoint.trim_end_matches('/');
    let deadline = tokio::time::Instant::now() + timeout;
    let mut interval = device.interval;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Auth(
                "the codex device code expired before it was approved".to_owned(),
            ));
        }
        tokio::time::sleep(interval).await;

        let response = http
            .post(format!("{auth_endpoint}{DEVICE_TOKEN_PATH}"))
            .json(&serde_json::json!({
                "device_auth_id": device.device_auth_id,
                "user_code": device.user_code,
            }))
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status.is_success() {
            let approved: DeviceTokenResponse = serde_json::from_str(&body).map_err(|source| {
                Error::Auth(format!("codex device approval is missing fields: {source}"))
            })?;
            return token_grant(
                http,
                auth_endpoint,
                &[
                    ("grant_type", "authorization_code"),
                    ("client_id", CLIENT_ID),
                    ("code", &approved.authorization_code),
                    ("code_verifier", &approved.code_verifier),
                    (
                        "redirect_uri",
                        &format!("{auth_endpoint}{DEVICE_REDIRECT_PATH}"),
                    ),
                ],
                "exchange",
            )
            .await;
        }
        // 403 and 404 are "not yet" on this endpoint, not failures
        if matches!(status.as_u16(), 403 | 404) {
            continue;
        }
        let code = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .map(|json| {
                let error = &json["error"];
                error
                    .as_str()
                    .or_else(|| error["code"].as_str())
                    .unwrap_or_default()
                    .to_owned()
            })
            .unwrap_or_default();
        match code.as_str() {
            "deviceauth_authorization_pending" => {}
            "slow_down" => interval += Duration::from_secs(5),
            _ => {
                return Err(Error::Auth(format!(
                    "codex device approval failed with HTTP {}: {}",
                    status.as_u16(),
                    crate::provider::snippet(&body)
                )));
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn fake_access_token(account_id: &str) -> String {
    let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#);
    let claims = serde_json::json!({
        ACCOUNT_CLAIM: { "chatgpt_account_id": account_id },
        "exp": 4_000_000_000u64,
    });
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string());
    format!("{header}.{payload}.sig")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{
        Credential, DeviceCode, Tokens, fake_access_token, now, request_device_code,
        wait_for_device_approval,
    };
    use crate::provider::Error;
    use crate::secrets::Secrets;

    fn credential(expires_at: u64) -> Credential {
        Credential {
            access_token: fake_access_token("acct_123"),
            refresh_token: "rt-old".to_owned(),
            expires_at,
        }
    }

    fn stored(dir: &std::path::Path, credential: &Credential) -> Secrets {
        let secrets = Secrets::new(dir);
        secrets
            .write("codex", &credential.to_json())
            .expect("writes");
        secrets
    }

    #[test]
    fn form_encoding_escapes_everything_outside_the_unreserved_set() {
        assert_eq!(
            super::form_encode(&[("a", "x y/z"), ("b", "ok-_.~"), ("c", "é")]),
            "a=x%20y%2Fz&b=ok-_.~&c=%C3%A9"
        );
    }

    #[test]
    fn the_account_id_comes_out_of_the_access_tokens_claims() {
        assert_eq!(credential(0).account_id().expect("claim"), "acct_123");
    }

    #[test]
    fn a_token_without_the_claim_is_refused_at_parse_time() {
        let text = json!({
            "access_token": "not.a-jwt",
            "refresh_token": "rt",
            "expires_at": 1,
        })
        .to_string();

        let err = Credential::parse(&text).expect_err("no claim, no credential");
        assert!(matches!(err, Error::Auth(_)), "{err}");
    }

    #[test]
    fn debug_output_redacts_both_tokens() {
        let rendered = format!("{:?}", credential(7));
        assert!(!rendered.contains("rt-old"), "{rendered}");
        assert!(!rendered.contains("eyJ"), "{rendered}");
        assert!(rendered.contains("redacted"));
    }

    #[tokio::test]
    async fn a_fresh_credential_is_used_as_is() {
        let dir = tempfile::tempdir().expect("temp dir");
        let secrets = stored(dir.path(), &credential(now() + 3600));
        let server = MockServer::start().await;

        let tokens = Tokens::open(secrets, "codex", &server.uri()).expect("opens");
        let (access, account) = tokens.bearer().await.expect("bearer");

        assert_eq!(access, fake_access_token("acct_123"));
        assert_eq!(account, "acct_123");
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_expiring_credential_is_refreshed_and_written_back() {
        let dir = tempfile::tempdir().expect("temp dir");
        let secrets = stored(dir.path(), &credential(now() + 10));
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains("refresh_token=rt-old"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": fake_access_token("acct_123"),
                "refresh_token": "rt-new",
                "expires_in": 864_000,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tokens = Tokens::open(secrets.clone(), "codex", &server.uri()).expect("opens");
        tokens.bearer().await.expect("refreshes");
        tokens.bearer().await.expect("the second call reuses it");

        let saved = Credential::parse(&secrets.read("codex").expect("saved")).expect("parses");
        assert_eq!(saved.refresh_token, "rt-new");
        assert!(saved.expires_at > now() + 800_000);
    }

    #[tokio::test]
    async fn a_failed_refresh_says_to_log_in_again() {
        let dir = tempfile::tempdir().expect("temp dir");
        let secrets = stored(dir.path(), &credential(0));
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": {"message": "invalid_grant"}
            })))
            .mount(&server)
            .await;

        let tokens = Tokens::open(secrets, "codex", &server.uri()).expect("opens");
        let err = tokens.bearer().await.expect_err("refresh fails");

        let text = err.to_string();
        assert!(
            text.contains("invalid_grant") && text.contains("arcd login codex"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_missing_credential_names_the_login_command() {
        let dir = tempfile::tempdir().expect("temp dir");

        let err = Tokens::open(Secrets::new(dir.path()), "codex", "http://unused")
            .expect_err("nothing there");

        let text = err.to_string();
        assert!(
            text.contains("codex") && text.contains("arcd login codex"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn the_device_flow_polls_until_approved_then_exchanges_the_code() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/accounts/deviceauth/usercode"))
            .and(body_string_contains("app_EMoamEEZ73f0CkXaXp7hrann"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "device_auth_id": "dev-1",
                "user_code": "ABCD-EFGH",
                "interval": "0",
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/accounts/deviceauth/token"))
            .respond_with(ResponseTemplate::new(403))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/accounts/deviceauth/token"))
            .and(body_string_contains("dev-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "authorization_code": "code-9",
                "code_verifier": "verifier-9",
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .and(body_string_contains("grant_type=authorization_code"))
            .and(body_string_contains("code=code-9"))
            .and(body_string_contains("code_verifier=verifier-9"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": fake_access_token("acct_777"),
                "refresh_token": "rt-777",
                "expires_in": 3600,
            })))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();

        let device = request_device_code(&http, &server.uri())
            .await
            .expect("code");
        assert_eq!(
            device,
            DeviceCode {
                user_code: "ABCD-EFGH".to_owned(),
                verification_url: format!("{}/codex/device", server.uri()),
                interval: Duration::ZERO,
                device_auth_id: "dev-1".to_owned(),
            }
        );

        let credential =
            wait_for_device_approval(&http, &server.uri(), &device, Duration::from_secs(5))
                .await
                .expect("approved");
        assert_eq!(credential.account_id().expect("claim"), "acct_777");
        assert_eq!(credential.refresh_token, "rt-777");
    }

    #[tokio::test]
    async fn a_device_flow_that_is_never_approved_times_out() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/accounts/deviceauth/token"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let device = DeviceCode {
            user_code: "X".to_owned(),
            verification_url: String::new(),
            interval: Duration::ZERO,
            device_auth_id: "dev-2".to_owned(),
        };

        let err = wait_for_device_approval(
            &reqwest::Client::new(),
            &server.uri(),
            &device,
            Duration::from_millis(50),
        )
        .await
        .expect_err("times out");

        assert!(err.to_string().contains("expired"), "{err}");
    }
}
