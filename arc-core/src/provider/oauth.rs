//! Google OAuth for the Antigravity (Code Assist) backend.
//!
//! ARC signs in the way the community harnesses do: its own OAuth loopback flow
//! against Google's standard endpoints, using the public client the Antigravity
//! desktop app ships (DESIGN.md §6). That is a deliberate terms-of-service gray
//! area, decided and recorded, not hidden — the constants below name their
//! source and the
//! request shapes are the plain ones the protocol calls for.
//!
//! Two pieces live here and nothing else. [`login`] runs the interactive flow
//! once and leaves a token file behind. [`TokenManager`] is what everything
//! afterwards holds: [`bearer`](TokenManager::bearer) hands out an access token
//! and refreshes it when it is close to expiring. Calling `cloudcode-pa`
//! endpoints with that token is task 4.3's job, not this module's.
//!
//! # Secrets discipline
//!
//! Invariant 5 is a hard requirement here, since this is the one module that
//! handles credentials. Token values never appear in a span field, a log line,
//! an error message, a `Debug` output, or a test fixture. The rules that follow
//! from it and are easy to break by accident:
//!
//! - [`TokenSet`] and [`OauthConfig`] implement `Debug` by hand. Deriving it
//!   would print the tokens.
//! - Spans carry the expiry and the number of scopes, never the tokens.
//! - An error body from the token endpoint reaches an [`Error`] only through
//!   [`token_endpoint_error`], which keeps the OAuth error code and discards
//!   anything it cannot parse — a body that failed to parse might be a proxy
//!   echoing the request, and the request contains the refresh token.
//!
//! # Storage
//!
//! Tokens sit in one JSON file at a path the caller picks (the daemon's is
//! `data/secrets/google_oauth.json`), written 0600 and replaced atomically.
//! DESIGN.md §10 calls this the Phase 1 simplification; the keychain comes
//! later, and it changes only [`TokenSet::load`] and [`TokenSet::store`].
//!
//! File I/O is synchronous `std::fs`, like the log layer, and for the same
//! reason: it is a few hundred bytes on a path that already blocks on a network
//! round trip.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};

use super::Error;

// The OAuth client below is the public one the Antigravity desktop app ships.
// It is not an ARC credential and not a user secret: it is published, and every
// harness that speaks to this backend uses the same pair. The "secret" half is
// what OAuth calls a client secret and what an installed application actually
// has — a value baked into a binary that anyone can read.
//
// Source: https://github.com/NoeFabris/opencode-antigravity-auth
//         `src/constants.ts` and `src/antigravity/oauth.ts`, at HEAD
//         (repository archived 2026-07-17), retrieved 2026-08-12.

/// OAuth client id of the Antigravity desktop app.
const CLIENT_ID: &str = "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com";

/// OAuth client secret paired with [`CLIENT_ID`].
const CLIENT_SECRET: &str = "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";

/// Scopes the Antigravity client requests, in the upstream order.
///
/// `cloud-platform` is what the Code Assist API checks. `cclog` and
/// `experimentsandconfigs` are Google-internal scopes the real client asks for;
/// they are carried unchanged because the consent screen and the backend are
/// entitled to expect the set they issued the client for.
const SCOPES: [&str; 5] = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/cclog",
    "https://www.googleapis.com/auth/experimentsandconfigs",
];

/// Redirect URI registered for [`CLIENT_ID`], port included.
///
/// The port is fixed at 51121 and not ours to choose. Google matches a web
/// client's `redirect_uri` exactly, so an ephemeral port would come back as
/// `redirect_uri_mismatch`; the loopback-any-port relaxation applies to clients
/// registered as installed apps, which this one is not (it has a client
/// secret). If 51121 is busy, [`login`] fails and says so — there is no
/// fallback port that would be accepted.
const REDIRECT_URI: &str = "http://localhost:51121/oauth-callback";

/// Google's authorization endpoint, where the user grants consent.
const AUTH_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";

/// Google's token endpoint, for both the code exchange and refreshes.
const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";

/// How long before expiry an access token counts as spent.
///
/// Five minutes covers a completion that starts just under the wire and a
/// modest clock skew between us and Google. Refreshing early costs one cheap
/// request; refreshing late fails a completion that was already in flight.
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

/// Lifetime assumed when the token endpoint omits `expires_in`.
///
/// Google always sends it. If it ever does not, an hour is its documented
/// default, and being wrong here costs at most one early refresh.
const DEFAULT_EXPIRES_IN: u64 = 3600;

/// How long a loopback connection has to send its request line before it is
/// dropped.
///
/// Browsers open speculative connections and send nothing on them. Each one is
/// handled on its own task so an idle socket cannot delay the real callback,
/// and this bound is what eventually reclaims those tasks.
const CALLBACK_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Reads the wall clock, in seconds since the Unix epoch.
///
/// Every expiry decision in this module goes through a value of this type, so a
/// test can pin time by passing a function that returns a constant. Production
/// uses [`now_unix`].
type Clock = fn() -> u64;

/// Seconds since the Unix epoch, now.
///
/// A clock set before 1970 reads as 0, which makes every token look expired and
/// triggers a refresh. That is the safe direction to fail: the alternative is
/// treating a spent token as valid.
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// Where and as whom to authenticate.
///
/// [`Default`] is the community client described at the top of this module and
/// is what production uses. The fields exist so tests can point the flow at a
/// local server; nothing in ARC configures them.
#[derive(Clone)]
pub struct OauthConfig {
    /// OAuth client id.
    pub client_id: String,

    /// OAuth client secret.
    pub client_secret: String,

    /// Scopes to request, space-joined into the authorization URL.
    pub scopes: Vec<String>,

    /// Redirect URI. Its port and path decide where [`login`] listens, and it
    /// must match the value registered for the client exactly.
    pub redirect_uri: String,

    /// Authorization endpoint the user visits.
    pub auth_endpoint: String,

    /// Token endpoint for code exchange and refresh.
    pub token_endpoint: String,

    /// How long before expiry a token is treated as spent.
    pub refresh_margin: Duration,
}

impl Default for OauthConfig {
    fn default() -> Self {
        Self {
            client_id: CLIENT_ID.to_owned(),
            client_secret: CLIENT_SECRET.to_owned(),
            scopes: SCOPES.iter().map(|scope| (*scope).to_owned()).collect(),
            redirect_uri: REDIRECT_URI.to_owned(),
            auth_endpoint: AUTH_ENDPOINT.to_owned(),
            token_endpoint: TOKEN_ENDPOINT.to_owned(),
            refresh_margin: REFRESH_MARGIN,
        }
    }
}

/// Redacts `client_secret`.
///
/// The value is public knowledge, but a config is the kind of thing that ends
/// up in a debug line, and a field printed as a secret in one place and not
/// another is how the habit erodes.
impl fmt::Debug for OauthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OauthConfig")
            .field("client_id", &self.client_id)
            .field("client_secret", &"<redacted>")
            .field("scopes", &self.scopes)
            .field("redirect_uri", &self.redirect_uri)
            .field("auth_endpoint", &self.auth_endpoint)
            .field("token_endpoint", &self.token_endpoint)
            .field("refresh_margin", &self.refresh_margin)
            .finish()
    }
}

/// One signed-in account's tokens, as stored on disk.
///
/// The JSON is the obvious four fields:
///
/// ```json
/// {
///   "access_token": "...",
///   "refresh_token": "...",
///   "expires_at": 1786000000,
///   "scopes": ["https://www.googleapis.com/auth/cloud-platform", "..."]
/// }
/// ```
///
/// `expires_at` is absolute (Unix seconds) rather than the `expires_in` the
/// endpoint returns, because a duration is meaningless once it has been written
/// to a file and read back an hour later.
///
/// Unknown fields are ignored on load, so a later version can add one without
/// invalidating existing files.
#[derive(Clone, Serialize, Deserialize)]
pub struct TokenSet {
    /// Bearer token for API calls. Short-lived.
    access_token: String,

    /// Long-lived token used to mint new access tokens. Losing this means
    /// signing in again, so nothing in this module discards it on a failure.
    refresh_token: String,

    /// When `access_token` stops being valid, in Unix seconds.
    expires_at: u64,

    /// Scopes the tokens were granted for.
    scopes: Vec<String>,
}

/// Redacts both tokens. Tested, because this is the impl that stands between a
/// stray `{:?}` and a credential in a log file.
impl fmt::Debug for TokenSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenSet")
            .field("access_token", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .field("scopes", &self.scopes)
            .finish()
    }
}

impl TokenSet {
    /// When the access token expires, in Unix seconds.
    #[must_use]
    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }

    /// Scopes these tokens were granted for.
    #[must_use]
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }

    /// True if the access token is expired or within `margin` of expiring.
    fn is_stale(&self, now: u64, margin: Duration) -> bool {
        self.expires_at <= now.saturating_add(margin.as_secs())
    }

    /// Reads a token file.
    ///
    /// # Errors
    ///
    /// [`Error::Auth`] if the file is missing, unreadable, or not the JSON this
    /// type writes. All three mean the same thing to a caller: there is no
    /// usable credential and a person has to sign in.
    fn load(path: &Path) -> Result<Self, Error> {
        let bytes = fs::read(path).map_err(|source| {
            Error::Auth(format!(
                "no usable token file at {}: {source}; run the login flow",
                path.display()
            ))
        })?;
        serde_json::from_slice(&bytes).map_err(|source| {
            Error::Auth(format!(
                "token file {} is not valid token JSON: {source}",
                path.display()
            ))
        })
    }

    /// Writes a token file atomically, owner-readable only.
    ///
    /// The write goes to a sibling temp file created with mode 0600, is fsynced,
    /// and is then renamed over the target. A crash at any point leaves either
    /// the previous file or the new one — never a half-written token file, and
    /// never one that was briefly world-readable, because the permissions are
    /// set by `open` rather than patched afterwards.
    ///
    /// # Errors
    ///
    /// [`Error::Auth`] if the directory cannot be created or any step of the
    /// write fails. The temp file is removed on the way out so a failed write
    /// leaves no debris.
    fn store(&self, path: &Path) -> Result<(), Error> {
        let fail = |context: &str, source: &dyn fmt::Display| {
            Error::Auth(format!("could not {context} {}: {source}", path.display()))
        };

        let json = serde_json::to_vec_pretty(self)
            .map_err(|_| Error::Auth("could not serialise the token set".to_owned()))?;

        let dir = path.parent().filter(|dir| !dir.as_os_str().is_empty());
        if let Some(dir) = dir {
            fs::create_dir_all(dir).map_err(|source| fail("create the directory for", &source))?;
        }
        let temp = temp_path(path)?;

        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }

        let write = || -> Result<(), std::io::Error> {
            let mut file = options.open(&temp)?;
            file.write_all(&json)?;
            // The rename below only publishes a directory entry. Without this
            // the entry can reach the disk pointing at unwritten bytes.
            file.sync_all()?;
            drop(file);
            fs::rename(&temp, path)
        };

        if let Err(source) = write() {
            // Best effort: the write already failed, and a leftover temp file
            // is the lesser problem to report.
            drop(fs::remove_file(&temp));
            return Err(fail("write", &source));
        }

        if let Some(dir) = dir {
            File::open(dir)
                .and_then(|handle| handle.sync_all())
                .map_err(|source| fail("flush the directory holding", &source))?;
        }
        Ok(())
    }
}

/// The temp file `path` is written through: the same name with `.tmp` appended,
/// in the same directory so the rename stays within one filesystem.
fn temp_path(path: &Path) -> Result<PathBuf, Error> {
    let name = path.file_name().ok_or_else(|| {
        Error::Auth(format!(
            "token path {} does not name a file",
            path.display()
        ))
    })?;
    let mut temp = name.to_os_string();
    temp.push(".tmp");
    Ok(path.with_file_name(temp))
}

/// Holds the tokens for one account and hands out valid access tokens.
///
/// The tokens are loaded from disk on first use and kept in memory afterwards;
/// the file is the source of truth across restarts, not per call. A refresh
/// updates both.
///
/// # Sharing
///
/// Not `Clone`. Share it as `Arc<TokenManager>`: [`bearer`](Self::bearer) takes
/// `&self` and the whole cache sits behind one mutex, so concurrent callers
/// serialize for the length of a refresh and exactly one refresh request goes
/// out. Callers that arrive during it get the token it produced. Cloning the
/// manager instead would give each clone its own cache and let them refresh
/// against each other — which is why it cannot be cloned.
#[derive(Debug)]
pub struct TokenManager {
    /// Client, endpoints, and refresh margin.
    config: OauthConfig,

    /// Token file this manager reads and writes.
    path: PathBuf,

    /// Reused so refreshes share a connection pool with nothing else to set up.
    http: reqwest::Client,

    /// Indirection for tests; always [`now_unix`] in production.
    clock: Clock,

    /// The cache. `None` until the first load. The mutex is what makes the
    /// refresh single-flight, so it is held across the request.
    cached: Mutex<Option<TokenSet>>,
}

impl TokenManager {
    /// A manager over the token file at `path`.
    ///
    /// Nothing is read here; the first [`bearer`](Self::bearer) call loads the
    /// file. Constructing a manager for a path that does not exist yet is fine
    /// and is what [`login`] does.
    #[must_use]
    pub fn new(config: OauthConfig, path: impl Into<PathBuf>) -> Self {
        Self {
            config,
            path: path.into(),
            http: reqwest::Client::new(),
            clock: now_unix,
            cached: Mutex::new(None),
        }
    }

    /// The token file this manager is backed by.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// A valid access token, refreshing first if the cached one is spent.
    ///
    /// "Spent" means expired or within the configured margin of expiring
    /// ([`REFRESH_MARGIN`] by default), so a caller that gets a token here has
    /// time to use it.
    ///
    /// # Errors
    ///
    /// - [`Error::Auth`] if there is no token file, it does not parse, or the
    ///   refresh was rejected. Every one of them needs a person to sign in
    ///   again, except a transient rejection, which the caller can retry on its
    ///   own schedule — there is no retry loop in here.
    /// - [`Error::Transport`] if the refresh request never completed.
    /// - [`Error::Http`] if the token endpoint failed on its own side (5xx).
    pub async fn bearer(&self) -> Result<String, Error> {
        let mut cached = self.cached.lock().await;

        // Taken out of the cache for the duration. Every path below either puts
        // a token set back or leaves the cache empty deliberately, and the lock
        // is held throughout, so no other caller can observe the gap.
        let tokens = match cached.take() {
            Some(tokens) => tokens,
            None => TokenSet::load(&self.path)?,
        };

        if !tokens.is_stale((self.clock)(), self.config.refresh_margin) {
            let access = tokens.access_token.clone();
            *cached = Some(tokens);
            return Ok(access);
        }

        let refreshed = match refresh(&self.http, &self.config, &tokens, self.clock).await {
            Ok(refreshed) => refreshed,
            // A failed refresh puts the old tokens back untouched: the refresh
            // token in them may well be good, and the failure may be Google
            // having a bad minute. Discarding it would turn a retryable error
            // into a mandatory re-login.
            Err(error) => {
                *cached = Some(tokens);
                return Err(error);
            }
        };

        let access = refreshed.access_token.clone();
        let stored = refreshed.store(&self.path);
        // The refreshed tokens are valid whether or not the file accepted them,
        // so they are cached either way — but a token file that could not be
        // written is still reported, because the next process start would find
        // a credential Google may have already rotated away.
        *cached = Some(refreshed);
        stored?;
        Ok(access)
    }

    /// Same manager with a pinned clock, so expiry tests do not race real time.
    #[cfg(test)]
    fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }
}

/// Runs the interactive sign-in and writes the tokens to `path`.
///
/// The flow is the loopback one, deliberately manual: it prints the
/// authorization URL rather than launching a browser, because `arcd` may be
/// running headless or over SSH and the browser that has the Google session is
/// somewhere else. The listener stays on 127.0.0.1 and the code arrives on the
/// redirect; nothing is typed back in.
///
/// PKCE is used (S256) and the `state` parameter is a fresh random value that
/// is checked on return.
///
/// Both `println!` calls are the point of the function — this is a foreground
/// CLI flow, and its output is instructions for the person running it, not
/// logging.
///
/// # Errors
///
/// - [`Error::Auth`] if the redirect port is busy, the user denied consent, the
///   `state` did not come back intact, the exchange was rejected, or the tokens
///   could not be written.
/// - [`Error::Transport`] if the exchange request never completed.
/// - [`Error::Http`] if the token endpoint failed on its own side (5xx).
#[tracing::instrument(
    level = "info",
    name = "oauth.login",
    skip_all,
    fields(
        scopes = config.scopes.len(),
        expires_at = tracing::field::Empty,
    )
)]
pub async fn login(config: &OauthConfig, path: &Path) -> Result<TokenManager, Error> {
    let redirect = Url::parse(&config.redirect_uri)
        .map_err(|source| Error::Auth(format!("redirect URI is not a URL: {source}")))?;
    let port = redirect
        .port_or_known_default()
        .ok_or_else(|| Error::Auth("redirect URI has no port to listen on".to_owned()))?;
    let callback_path = redirect.path().to_owned();

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
        .await
        .map_err(|source| {
            Error::Auth(format!(
                "could not listen on 127.0.0.1:{port} for the OAuth redirect: {source}. \
                 The port is fixed by the registered redirect URI, so free it and retry."
            ))
        })?;

    let verifier = random_urlsafe(32)?;
    let state = random_urlsafe(16)?;
    let auth_url = authorization_url(config, &pkce_challenge(&verifier), &state)?;

    println!("Open this URL in a browser signed in to the Google account to use:\n");
    println!("{auth_url}\n");
    println!("Waiting for the redirect to {}...", config.redirect_uri);

    let code = wait_for_code(&listener, &callback_path, &state).await?;

    let issued_at = (now_unix)();
    let response = post_token_request(
        &reqwest::Client::new(),
        config,
        &[
            ("client_id", config.client_id.as_str()),
            ("client_secret", config.client_secret.as_str()),
            ("code", code.as_str()),
            ("grant_type", "authorization_code"),
            ("redirect_uri", config.redirect_uri.as_str()),
            ("code_verifier", verifier.as_str()),
        ],
    )
    .await?;

    let refresh_token = response.refresh_token.ok_or_else(|| {
        Error::Auth(
            "the token endpoint returned no refresh token; \
             the authorization URL must ask for offline access"
                .to_owned(),
        )
    })?;
    let tokens = TokenSet {
        access_token: response.access_token,
        refresh_token,
        expires_at: issued_at.saturating_add(response.expires_in.unwrap_or(DEFAULT_EXPIRES_IN)),
        scopes: response
            .scope
            .map_or_else(|| config.scopes.clone(), |scope| split_scopes(&scope)),
    };
    tokens.store(path)?;
    tracing::Span::current().record("expires_at", tokens.expires_at);

    println!("Signed in. Tokens written to {}.", path.display());

    let manager = TokenManager::new(config.clone(), path);
    *manager.cached.lock().await = Some(tokens);
    Ok(manager)
}

/// Exchanges the refresh token for a fresh access token.
///
/// One attempt, no retries: backoff policy belongs to whoever is driving the
/// completion, which has context this function does not. Google may or may not
/// return a new refresh token; when it does not, the old one carries over.
#[tracing::instrument(
    level = "debug",
    name = "oauth.refresh",
    skip_all,
    fields(
        scopes = tokens.scopes.len(),
        expires_at = tracing::field::Empty,
    )
)]
async fn refresh(
    http: &reqwest::Client,
    config: &OauthConfig,
    tokens: &TokenSet,
    clock: Clock,
) -> Result<TokenSet, Error> {
    let issued_at = clock();
    let response = post_token_request(
        http,
        config,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", tokens.refresh_token.as_str()),
            ("client_id", config.client_id.as_str()),
            ("client_secret", config.client_secret.as_str()),
        ],
    )
    .await?;

    let refreshed = TokenSet {
        access_token: response.access_token,
        refresh_token: response
            .refresh_token
            .unwrap_or_else(|| tokens.refresh_token.clone()),
        expires_at: issued_at.saturating_add(response.expires_in.unwrap_or(DEFAULT_EXPIRES_IN)),
        scopes: response
            .scope
            .map_or_else(|| tokens.scopes.clone(), |scope| split_scopes(&scope)),
    };
    tracing::Span::current().record("expires_at", refreshed.expires_at);
    Ok(refreshed)
}

/// What the token endpoint sends back on success.
#[derive(Deserialize)]
struct TokenResponse {
    /// The new bearer token.
    access_token: String,

    /// Lifetime in seconds. Google always sends it; see [`DEFAULT_EXPIRES_IN`].
    expires_in: Option<u64>,

    /// Present on a code exchange, usually absent on a refresh.
    refresh_token: Option<String>,

    /// Space-separated granted scopes, when the endpoint reports them.
    scope: Option<String>,
}

/// What the token endpoint sends back on failure.
#[derive(Deserialize)]
struct TokenErrorResponse {
    /// Machine-readable code, e.g. `invalid_grant`.
    error: Option<String>,

    /// Human-readable detail, when there is any.
    error_description: Option<String>,
}

/// POSTs a form to the token endpoint and parses the response.
async fn post_token_request(
    http: &reqwest::Client,
    config: &OauthConfig,
    form: &[(&str, &str)],
) -> Result<TokenResponse, Error> {
    let response = http.post(&config.token_endpoint).form(form).send().await?;

    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(token_endpoint_error(status.as_u16(), &body));
    }
    serde_json::from_str(&body).map_err(|source| {
        Error::Auth(format!(
            "the token endpoint returned a body that is not a token response: {source}"
        ))
    })
}

/// Turns a token endpoint failure into an [`Error`], keeping only fields that
/// are safe to repeat.
///
/// A 4xx is [`Error::Auth`]: the credential is the problem and a person has to
/// sign in again. A 5xx is [`Error::Http`]: Google's side broke and retrying is
/// meaningful.
///
/// The body is never passed through whole. If it parses as an OAuth error, its
/// `error` and `error_description` are safe by definition. If it does not, it
/// is discarded — an unrecognised body could be a proxy or an error page
/// echoing the request, and the request carries the refresh token.
fn token_endpoint_error(status: u16, body: &str) -> Error {
    let detail = serde_json::from_str::<TokenErrorResponse>(body).map_or_else(
        |_| "unrecognised error body, withheld".to_owned(),
        |parsed| match (parsed.error, parsed.error_description) {
            (Some(code), Some(description)) => format!("{code}: {description}"),
            (Some(code), None) => code,
            (None, Some(description)) => description,
            (None, None) => "no error detail".to_owned(),
        },
    );

    if status >= 500 {
        Error::http(status, &detail)
    } else {
        Error::Auth(format!("token endpoint returned HTTP {status}: {detail}"))
    }
}

/// Builds the authorization URL the user visits.
///
/// `access_type=offline` with `prompt=consent` is what makes Google issue a
/// refresh token; without both, a second sign-in for an already-consented
/// account comes back with an access token only.
fn authorization_url(config: &OauthConfig, challenge: &str, state: &str) -> Result<Url, Error> {
    Url::parse_with_params(
        &config.auth_endpoint,
        &[
            ("client_id", config.client_id.as_str()),
            ("response_type", "code"),
            ("redirect_uri", config.redirect_uri.as_str()),
            ("scope", config.scopes.join(" ").as_str()),
            ("code_challenge", challenge),
            ("code_challenge_method", "S256"),
            ("state", state),
            ("access_type", "offline"),
            ("prompt", "consent"),
        ],
    )
    .map_err(|source| Error::Auth(format!("could not build the authorization URL: {source}")))
}

/// Accepts loopback connections until one is the callback, and returns its code.
///
/// Every connection is handled on its own task. Browsers open speculative
/// sockets and send nothing on them, and serving connections one at a time would
/// let one of those stall the sign-in until it timed out.
async fn wait_for_code(
    listener: &TcpListener,
    callback_path: &str,
    expected_state: &str,
) -> Result<String, Error> {
    let (sender, mut receiver) = mpsc::channel::<Result<String, Error>>(1);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|source| {
                    Error::Auth(format!("the OAuth redirect listener failed: {source}"))
                })?;
                let sender = sender.clone();
                let callback_path = callback_path.to_owned();
                let expected_state = expected_state.to_owned();
                tokio::spawn(async move {
                    let handled = tokio::time::timeout(
                        CALLBACK_READ_TIMEOUT,
                        handle_callback(stream, &callback_path, &expected_state),
                    )
                    .await;
                    // A connection that was not the callback — a favicon
                    // request, a speculative socket, a timeout — is not an
                    // outcome. Only a decided one goes to the channel, and a
                    // closed channel means someone else decided first.
                    if let Ok(Some(outcome)) = handled {
                        drop(sender.send(outcome).await);
                    }
                });
            }
            outcome = receiver.recv() => {
                if let Some(outcome) = outcome {
                    return outcome;
                }
            }
        }
    }
}

/// Reads one HTTP request, answers it, and reports the callback's outcome.
///
/// `None` means the request was not the callback and the caller should keep
/// waiting.
async fn handle_callback(
    mut stream: TcpStream,
    callback_path: &str,
    expected_state: &str,
) -> Option<Result<String, Error>> {
    let mut request_line = String::new();
    let mut reader = BufReader::new(&mut stream);
    // Only the request line matters: the query is in it, and the response is
    // fixed. Headers are left unread and the connection is closed after.
    if reader.read_line(&mut request_line).await.is_err() {
        return None;
    }

    let target = request_line.split_whitespace().nth(1)?;
    // A request target is a path, not a URL; a base makes it parseable and is
    // otherwise ignored.
    let url = Url::parse("http://127.0.0.1").ok()?.join(target).ok()?;
    if url.path() != callback_path {
        respond(&mut stream, "404 Not Found", "Not the OAuth callback.").await;
        return None;
    }

    let outcome = read_callback_query(&url, expected_state);
    match &outcome {
        Ok(_) => {
            respond(
                &mut stream,
                "200 OK",
                "ARC is signed in. You can close this tab.",
            )
            .await;
        }
        Err(error) => {
            respond(&mut stream, "400 Bad Request", &error.to_string()).await;
        }
    }
    Some(outcome)
}

/// Pulls the authorization code out of a callback URL, checking `state` first.
fn read_callback_query(url: &Url, expected_state: &str) -> Result<String, Error> {
    let mut code = None;
    let mut state = None;
    let mut denied = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "code" => code = Some(value.into_owned()),
            "state" => state = Some(value.into_owned()),
            "error" => denied = Some(value.into_owned()),
            _ => {}
        }
    }

    // Checked before the code is looked at: a mismatched state means this
    // callback belongs to a different flow, and its code is not ours to use.
    if state.as_deref() != Some(expected_state) {
        return Err(Error::Auth(
            "the OAuth callback carried the wrong state; ignoring the code".to_owned(),
        ));
    }
    if let Some(denied) = denied {
        return Err(Error::Auth(format!("authorization was refused: {denied}")));
    }
    code.ok_or_else(|| Error::Auth("the OAuth callback carried no code".to_owned()))
}

/// Writes a minimal HTTP response and closes the connection.
///
/// Best effort throughout: the browser tab is a courtesy, and the sign-in has
/// already succeeded or failed by the time this runs.
async fn respond(stream: &mut TcpStream, status: &str, message: &str) {
    let body = format!("<!doctype html><meta charset=\"utf-8\"><p>{message}</p>\n");
    let response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: text/html; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    drop(stream.write_all(response.as_bytes()).await);
    drop(stream.shutdown().await);
}

/// A random URL-safe string carrying `bytes` bytes of entropy.
///
/// # Errors
///
/// [`Error::Auth`] if the OS has no randomness to give, which would make both
/// PKCE and the state check meaningless.
fn random_urlsafe(bytes: usize) -> Result<String, Error> {
    let mut buffer = vec![0u8; bytes];
    getrandom::fill(&mut buffer)
        .map_err(|source| Error::Auth(format!("no system randomness available: {source}")))?;
    Ok(URL_SAFE_NO_PAD.encode(buffer))
}

/// The S256 PKCE challenge for `verifier`: base64url of its SHA-256, unpadded.
fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

/// Splits a space-separated scope string, dropping empties.
fn split_scopes(scope: &str) -> Vec<String> {
    scope.split_whitespace().map(str::to_owned).collect()
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use tempfile::TempDir;
    use wiremock::matchers::{body_string_contains, method, path as path_matcher};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{
        Error, OauthConfig, TokenManager, TokenSet, Url, pkce_challenge, read_callback_query,
        temp_path, token_endpoint_error,
    };

    /// Every token value in this module's tests is one of these. Nothing here
    /// is or resembles a real credential (invariant 5).
    const FAKE_ACCESS: &str = "test-access-token";
    const FAKE_REFRESH: &str = "test-refresh-token";

    /// Pinned "now" for expiry arithmetic. The value is arbitrary; what matters
    /// is that it does not move while a test runs.
    const NOW: u64 = 1_800_000_000;

    fn pinned_now() -> u64 {
        NOW
    }

    fn tokens(expires_at: u64) -> TokenSet {
        TokenSet {
            access_token: FAKE_ACCESS.to_owned(),
            refresh_token: FAKE_REFRESH.to_owned(),
            expires_at,
            scopes: vec!["https://www.googleapis.com/auth/cloud-platform".to_owned()],
        }
    }

    /// A config pointed at `server` instead of Google.
    fn config_against(server: &MockServer) -> OauthConfig {
        OauthConfig {
            client_id: "test-client-id".to_owned(),
            client_secret: "test-client-secret".to_owned(),
            token_endpoint: format!("{}/token", server.uri()),
            ..OauthConfig::default()
        }
    }

    fn manager(config: OauthConfig, path: &Path) -> TokenManager {
        TokenManager::new(config, path).with_clock(pinned_now)
    }

    #[test]
    fn token_file_round_trips_and_is_owner_only() {
        let dir = TempDir::new().expect("temp dir");
        // A path one level down: store() has to create the directory, the way
        // it will for data/secrets/ on a fresh install.
        let path = dir.path().join("secrets").join("google_oauth.json");

        tokens(NOW + 3600).store(&path).expect("store");
        let loaded = TokenSet::load(&path).expect("load");

        assert_eq!(loaded.access_token, FAKE_ACCESS);
        assert_eq!(loaded.refresh_token, FAKE_REFRESH);
        assert_eq!(loaded.expires_at(), NOW + 3600);
        assert_eq!(
            loaded.scopes(),
            ["https://www.googleapis.com/auth/cloud-platform"]
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path)
                .expect("metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "token file must be owner-only");
        }
    }

    #[test]
    fn a_successful_write_leaves_no_temp_file() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("google_oauth.json");

        tokens(NOW + 3600).store(&path).expect("store");

        let temp = temp_path(&path).expect("temp path");
        assert_eq!(temp.file_name().expect("name"), "google_oauth.json.tmp");
        assert!(!temp.exists(), "temp file survived a successful write");
        assert!(path.exists());
    }

    #[test]
    fn overwriting_replaces_the_previous_tokens() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("google_oauth.json");

        tokens(NOW + 3600).store(&path).expect("first store");
        let mut second = tokens(NOW + 7200);
        second.access_token = "test-access-token-2".to_owned();
        second.store(&path).expect("second store");

        let loaded = TokenSet::load(&path).expect("load");
        assert_eq!(loaded.access_token, "test-access-token-2");
        assert_eq!(loaded.expires_at(), NOW + 7200);
    }

    #[test]
    fn debug_redacts_both_tokens() {
        let rendered = format!("{:?}", tokens(NOW + 3600));

        assert!(
            !rendered.contains(FAKE_ACCESS),
            "access token leaked: {rendered}"
        );
        assert!(
            !rendered.contains(FAKE_REFRESH),
            "refresh token leaked: {rendered}"
        );
        assert!(rendered.contains("<redacted>"));
        // The non-secret fields are still there; redaction should not make the
        // type useless to debug with.
        assert!(rendered.contains(&(NOW + 3600).to_string()));
    }

    #[test]
    fn debug_of_the_config_redacts_the_client_secret() {
        let rendered = format!("{:?}", OauthConfig::default());

        assert!(
            !rendered.contains("GOCSPX"),
            "client secret leaked: {rendered}"
        );
        assert!(rendered.contains("<redacted>"));
    }

    /// A manager's Debug goes through the two impls above; this pins that it
    /// stays that way if the struct grows a field.
    #[tokio::test]
    async fn debug_of_the_manager_redacts_cached_tokens() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("google_oauth.json");
        tokens(NOW + 3600).store(&path).expect("store");
        let manager = manager(OauthConfig::default(), &path);
        manager.bearer().await.expect("cached token");

        let rendered = format!("{manager:?}");

        assert!(
            !rendered.contains(FAKE_ACCESS),
            "access token leaked: {rendered}"
        );
        assert!(
            !rendered.contains(FAKE_REFRESH),
            "refresh token leaked: {rendered}"
        );
    }

    #[test]
    fn a_missing_token_file_is_an_auth_error() {
        let dir = TempDir::new().expect("temp dir");

        let error = TokenSet::load(&dir.path().join("absent.json")).expect_err("load");

        assert!(matches!(error, Error::Auth(message) if message.contains("run the login flow")));
    }

    #[tokio::test]
    async fn a_fresh_token_is_returned_without_contacting_google() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("google_oauth.json");
        tokens(NOW + 3600).store(&path).expect("store");

        let bearer = manager(config_against(&server), &path)
            .bearer()
            .await
            .expect("bearer");

        assert_eq!(bearer, FAKE_ACCESS);
        // No mock is mounted, so any request would have failed. Assert it
        // anyway: the point is that the token endpoint was not touched.
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn an_expired_token_is_refreshed_and_the_new_one_persisted() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_matcher("/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains("refresh_token=test-refresh-token"))
            .and(body_string_contains("client_id=test-client-id"))
            .and(body_string_contains("client_secret=test-client-secret"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"access_token":"test-access-token-refreshed","expires_in":3599}"#,
            ))
            .expect(1)
            .mount(&server)
            .await;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("google_oauth.json");
        tokens(NOW - 10).store(&path).expect("store");
        let manager = manager(config_against(&server), &path);

        let bearer = manager.bearer().await.expect("bearer");

        assert_eq!(bearer, "test-access-token-refreshed");

        let stored = TokenSet::load(&path).expect("load");
        assert_eq!(stored.access_token, "test-access-token-refreshed");
        assert_eq!(stored.expires_at(), NOW + 3599);
        // A refresh response without a refresh_token keeps the existing one.
        assert_eq!(stored.refresh_token, FAKE_REFRESH);
        // ...and without a scope keeps the recorded scopes.
        assert_eq!(
            stored.scopes(),
            ["https://www.googleapis.com/auth/cloud-platform"]
        );

        // The second call is served from the cache: `expect(1)` above fails on
        // drop if the token endpoint is hit twice.
        assert_eq!(
            manager.bearer().await.expect("cached bearer"),
            "test-access-token-refreshed"
        );
    }

    #[tokio::test]
    async fn a_token_inside_the_refresh_margin_is_treated_as_spent() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_matcher("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"access_token":"test-access-token-early","expires_in":3600,"refresh_token":"test-refresh-token-2","scope":"https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/userinfo.email"}"#,
            ))
            .expect(1)
            .mount(&server)
            .await;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("google_oauth.json");
        // Valid for four more minutes: not expired, but inside the five-minute
        // margin, so it must still be refreshed.
        tokens(NOW + 240).store(&path).expect("store");

        let bearer = manager(config_against(&server), &path)
            .bearer()
            .await
            .expect("bearer");

        assert_eq!(bearer, "test-access-token-early");
        let stored = TokenSet::load(&path).expect("load");
        // A rotated refresh token replaces the old one, and reported scopes
        // replace the recorded ones.
        assert_eq!(stored.refresh_token, "test-refresh-token-2");
        assert_eq!(stored.scopes().len(), 2);
    }

    #[tokio::test]
    async fn a_token_just_outside_the_margin_is_still_used() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("google_oauth.json");
        // One second past the margin: the boundary is `<=`, so this is fresh.
        tokens(NOW + 301).store(&path).expect("store");

        let bearer = manager(config_against(&server), &path)
            .bearer()
            .await
            .expect("bearer");

        assert_eq!(bearer, FAKE_ACCESS);
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_custom_margin_is_honoured() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("google_oauth.json");
        tokens(NOW + 240).store(&path).expect("store");

        let config = OauthConfig {
            refresh_margin: Duration::from_secs(60),
            ..config_against(&server)
        };
        let bearer = manager(config, &path).bearer().await.expect("bearer");

        // Four minutes left and a one-minute margin: no refresh.
        assert_eq!(bearer, FAKE_ACCESS);
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_rejected_refresh_is_an_auth_error_that_keeps_the_refresh_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_matcher("/token"))
            .respond_with(ResponseTemplate::new(400).set_body_string(
                r#"{"error":"invalid_grant","error_description":"Token has been expired or revoked."}"#,
            ))
            // One attempt only: no retry loop lives in this module.
            .expect(1)
            .mount(&server)
            .await;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("google_oauth.json");
        tokens(NOW - 10).store(&path).expect("store");

        let error = manager(config_against(&server), &path)
            .bearer()
            .await
            .expect_err("refresh should fail");

        let Error::Auth(message) = error else {
            panic!("a rejected refresh must be an auth error");
        };
        assert!(message.contains("invalid_grant"), "{message}");
        assert!(
            !message.contains(FAKE_REFRESH),
            "refresh token leaked: {message}"
        );

        // The cached credential survives: the refresh token may be fine and the
        // failure transient. Throwing it away would force an avoidable re-login.
        let stored = TokenSet::load(&path).expect("load");
        assert_eq!(stored.refresh_token, FAKE_REFRESH);
        assert_eq!(stored.access_token, FAKE_ACCESS);
    }

    #[tokio::test]
    async fn a_token_endpoint_outage_is_an_http_error_not_an_auth_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_matcher("/token"))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream unavailable"))
            .mount(&server)
            .await;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("google_oauth.json");
        tokens(NOW - 10).store(&path).expect("store");

        let error = manager(config_against(&server), &path)
            .bearer()
            .await
            .expect_err("refresh should fail");

        // 5xx is Google's problem, not the credential's: the caller may retry.
        assert!(
            matches!(error, Error::Http { status: 503, .. }),
            "{error:?}"
        );
    }

    #[test]
    fn an_unrecognised_error_body_is_withheld() {
        // A proxy or error page can echo the request, and the request carries
        // the refresh token. Nothing unparsed is repeated back.
        let error = token_endpoint_error(400, "POST /token refresh_token=test-refresh-token");

        let Error::Auth(message) = error else {
            panic!("a 4xx must be an auth error");
        };
        assert!(
            !message.contains(FAKE_REFRESH),
            "echoed request leaked: {message}"
        );
        assert!(message.contains("withheld"));
    }

    #[test]
    fn pkce_challenge_matches_the_rfc_7636_test_vector() {
        // RFC 7636 appendix B.
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    fn callback(query: &str) -> Url {
        Url::parse(&format!("http://127.0.0.1/oauth-callback?{query}")).expect("url")
    }

    #[test]
    fn a_callback_with_the_expected_state_yields_its_code() {
        let code = read_callback_query(&callback("code=test-code&state=abc"), "abc").expect("code");

        assert_eq!(code, "test-code");
    }

    #[test]
    fn a_callback_with_the_wrong_state_is_refused_even_with_a_code() {
        let error = read_callback_query(&callback("code=test-code&state=other"), "abc")
            .expect_err("state mismatch");

        assert!(matches!(error, Error::Auth(message) if message.contains("wrong state")));
    }

    #[test]
    fn a_refused_authorization_reports_googles_reason() {
        let error = read_callback_query(&callback("error=access_denied&state=abc"), "abc")
            .expect_err("denied");

        assert!(matches!(error, Error::Auth(message) if message.contains("access_denied")));
    }

    /// The browser half of the flow is exercised by `arcd login`, but
    /// the listener under it is ordinary TCP and worth pinning here: a
    /// speculative connection that never speaks and an unrelated request must
    /// not stop the real callback from being served.
    #[tokio::test]
    async fn the_loopback_listener_serves_the_callback_past_noisy_connections() {
        use std::net::Ipv4Addr;

        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");

        let browser = tokio::spawn(async move {
            // Held open and silent, the way a browser preconnects.
            let _speculative = TcpStream::connect(addr).await.expect("preconnect");

            let mut unrelated = TcpStream::connect(addr).await.expect("connect");
            unrelated
                .write_all(b"GET /favicon.ico HTTP/1.1\r\n\r\n")
                .await
                .expect("write");

            let mut callback = TcpStream::connect(addr).await.expect("connect");
            callback
                .write_all(b"GET /oauth-callback?code=test-code&state=abc HTTP/1.1\r\n\r\n")
                .await
                .expect("write");
            let mut response = String::new();
            callback
                .read_to_string(&mut response)
                .await
                .expect("response");
            response
        });

        let code = super::wait_for_code(&listener, "/oauth-callback", "abc")
            .await
            .expect("code");

        assert_eq!(code, "test-code");
        let response = browser.await.expect("browser task");
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("signed in"), "{response}");
        assert!(!response.contains("test-code"), "code echoed: {response}");
    }

    #[test]
    fn the_default_config_is_the_documented_client() {
        let config = OauthConfig::default();

        assert_eq!(config.scopes.len(), 5);
        assert_eq!(config.redirect_uri, "http://localhost:51121/oauth-callback");
        assert!(
            config
                .auth_endpoint
                .starts_with("https://accounts.google.com/")
        );
        assert_eq!(config.refresh_margin, Duration::from_secs(300));
    }

    #[test]
    fn the_authorization_url_carries_pkce_and_offline_access() {
        let config = OauthConfig::default();

        let url = super::authorization_url(&config, "test-challenge", "test-state").expect("url");

        let params: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(params["response_type"], "code");
        assert_eq!(params["code_challenge"], "test-challenge");
        assert_eq!(params["code_challenge_method"], "S256");
        assert_eq!(params["state"], "test-state");
        // Both are needed for Google to return a refresh token.
        assert_eq!(params["access_type"], "offline");
        assert_eq!(params["prompt"], "consent");
        assert_eq!(params["scope"], config.scopes.join(" "));
        assert_eq!(params["redirect_uri"], config.redirect_uri);
    }
}
