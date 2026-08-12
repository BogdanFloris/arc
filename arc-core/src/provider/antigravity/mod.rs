//! The Antigravity (Code Assist) backend.
//!
//! Google's Code Assist endpoint is a unified gateway: one Gemini-shaped API in
//! front of Gemini, Claude and others, reachable with the Antigravity desktop
//! app's OAuth credential (DESIGN.md §6, [`super::oauth`]). It is not a public
//! API and has no published reference, so everything here — paths, the request
//! wrapper, the headers — is copied from the community plugin named below and
//! checked against a live call.
//!
//! A completion has two halves, split where the error contract in
//! [`Provider::complete`](super::Provider::complete) splits. This file owns the
//! eager half: resolve a project, build a body, send it, and hand back a
//! [`reqwest::Response`] whose status has been checked and whose body has not
//! been touched. Everything that can fail before the first byte fails here, as a
//! plain `Err`, and nothing in this file constructs [`Error::MalformedStream`].
//! [`stream`] owns the rest — response bytes to
//! [`CompletionDelta`](super::CompletionDelta)s, and the errors that only a
//! started response can produce. The [`Provider`](super::Provider) impl below is
//! the seam between the two.
//!
//! # Onboarding
//!
//! Every request carries a Google Cloud project id, and a personal account does
//! not know its own. `loadCodeAssist` reports the managed project the account
//! already has; an account that has never used Code Assist has none, and
//! `onboardUser` provisions one. That dance happens once per process — the
//! result is cached for the provider's lifetime — because it is a property of
//! the account, not of a completion.
//!
//! # What this does not copy from the plugin
//!
//! The plugin rotates User-Agent strings across a pool, falls back across three
//! endpoints per request, and ships a hard-coded project id to use when
//! resolution fails. None of that is here: the first is fingerprint evasion ARC
//! has no reason to want, the second is a retry policy that belongs to whoever
//! drives the completion (the same reasoning as [`super::oauth`]'s single-shot
//! refresh), and the third would send someone else's project id. When
//! resolution fails, this module says so.

mod stream;

use std::sync::Arc;
use std::time::Duration;

use arc_proto::v1::Role;
use reqwest::header::{ACCEPT, HeaderMap, RETRY_AFTER, USER_AGENT};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::OnceCell;
use tracing::field::Empty;
use uuid::Uuid;

use super::oauth::TokenManager;
use super::{CompletionRequest, CompletionStream, Error, Message, Provider};

// Wire details below — paths, header values, body shapes — come from the
// community plugin that speaks this API, cross-checked between its
// documentation and its source because the two have drifted apart in places
// (see the notes at each constant).
//
// Source: https://github.com/NoeFabris/opencode-antigravity-auth
//         `docs/ANTIGRAVITY_API_SPEC.md` (spec, revised 2025-12-14),
//         `src/constants.ts`, `src/plugin/project.ts`, `src/plugin/request.ts`,
//         at commit 9a2cf7e (main, 2026-06-25), retrieved 2026-08-12.

/// Name this backend goes by in traces and `SessionCreated.provider`.
///
/// A constant rather than only a [`Provider::name`] return value: the daemon
/// records the provider on a session it has not built yet, and both spellings
/// have to be the same one.
pub const NAME: &str = "antigravity";

/// Production Code Assist endpoint, and the default.
pub const PRODUCTION_ENDPOINT: &str = "https://cloudcode-pa.googleapis.com";

/// Sandbox endpoint. The plugin prefers it for content requests; it is offered
/// here for a person debugging against a non-production backend, and nothing in
/// ARC selects it.
pub const SANDBOX_ENDPOINT: &str = "https://daily-cloudcode-pa.sandbox.googleapis.com";

/// Streaming completion action, with the query that makes the response SSE
/// rather than a JSON array.
const STREAM_ACTION: &str = "streamGenerateContent?alt=sse";

/// Project discovery action.
const LOAD_ACTION: &str = "loadCodeAssist";

/// Project provisioning action.
const ONBOARD_ACTION: &str = "onboardUser";

/// `User-Agent` the Antigravity client sends.
///
/// The version matters: the backend rejects clients it considers too old, and
/// the plugin goes as far as fetching the current version at startup. This is
/// its pinned fallback (`constants.ts`, `ANTIGRAVITY_VERSION_FALLBACK`), and a
/// 400 naming a version is the signal to raise it. The platform is this host's,
/// not one of the three the plugin rotates through: rotating identities is
/// fingerprint evasion, and the honest value is accepted.
const CLIENT_USER_AGENT: &str = "antigravity/1.18.3 linux/amd64";

/// `X-Goog-Api-Client` the Antigravity client sends.
const API_CLIENT: &str = "google-cloud-sdk vscode_cloudshelleditor/0.1";

/// `Client-Metadata` header: the same three values as [`ClientMetadata`], as
/// compact JSON, the way the plugin builds it.
const CLIENT_METADATA: &str =
    r#"{"ideType":"ANTIGRAVITY","platform":"LINUX_AMD64","pluginType":"GEMINI"}"#;

/// Platform reported to the backend, in both the header and the body.
///
/// A `ClientMetadata.Platform` enum value, and validated as one: the plugin's
/// `WINDOWS`/`MACOS` are not members and the backend answers a 400 naming the
/// field. `LINUX_AMD64`, `DARWIN_ARM64`, `WINDOWS_AMD64` and
/// `PLATFORM_UNSPECIFIED` all pass; this is the one that is true here.
const PLATFORM: &str = "LINUX_AMD64";

/// `userAgent` inside the request wrapper — a body field, unrelated to the
/// `User-Agent` header, and a bare product name rather than a UA string.
const WRAPPER_USER_AGENT: &str = "antigravity";

/// `requestType` inside the request wrapper. The plugin sends `agent` on every
/// Antigravity-mode request; the documented wrapper omits the field entirely.
/// Following the code over the document, since the code is what the backend
/// answers.
const REQUEST_TYPE: &str = "agent";

/// Prefix the plugin puts on `requestId`, ahead of a UUID.
const REQUEST_ID_PREFIX: &str = "agent-";

/// Tier to onboard into when `loadCodeAssist` names no allowed tier.
const DEFAULT_TIER: &str = "FREE";

/// How many times `onboardUser` is polled before giving up.
const ONBOARD_ATTEMPTS: u32 = 10;

/// How long to wait between `onboardUser` polls.
///
/// Provisioning a project is a long-running operation: the first call usually
/// answers `done: false`. Ten five-second polls is the plugin's budget and
/// roughly a minute of patience, which is the right order of magnitude for
/// something that happens once per account.
const ONBOARD_POLL: Duration = Duration::from_secs(5);

/// Longest error detail kept out of an onboarding response body.
const MAX_DETAIL: usize = 200;

/// The Antigravity backend for one signed-in account.
///
/// Holds the [`TokenManager`] rather than a token: every request asks it for a
/// bearer, and it decides whether a refresh is needed. There is no retry loop
/// around that call — a rejected credential is reported, not worked around.
///
/// Share it as `Arc<Antigravity>` if more than one task needs it. Cloning would
/// be wrong for the same reason it is wrong on [`TokenManager`]: each clone
/// would resolve and cache its own project id.
#[derive(Debug)]
pub struct Antigravity {
    /// Credential source. `Arc` because the manager is shared, not copied.
    tokens: Arc<TokenManager>,

    /// Endpoint base, without a trailing slash.
    endpoint: String,

    /// Reused across onboarding and completions so they share a connection
    /// pool, and so TLS is negotiated once.
    http: reqwest::Client,

    /// The account's project id, resolved on first use.
    ///
    /// [`OnceCell`] rather than a mutex around an `Option`: concurrent first
    /// callers serialize, exactly one onboarding runs, and every later read is
    /// lock-free.
    project: OnceCell<String>,

    /// Gap between `onboardUser` polls. A field so tests do not sleep.
    onboard_poll: Duration,
}

impl Antigravity {
    /// A provider against the production endpoint for the account `tokens`
    /// holds.
    ///
    /// Nothing is resolved or requested here. The first [`send`](Self::send)
    /// does the onboarding round trip.
    #[must_use]
    pub fn new(tokens: Arc<TokenManager>) -> Self {
        Self {
            tokens,
            endpoint: PRODUCTION_ENDPOINT.to_owned(),
            http: reqwest::Client::new(),
            project: OnceCell::new(),
            onboard_poll: ONBOARD_POLL,
        }
    }

    /// Same provider pointed at a different endpoint base.
    ///
    /// [`SANDBOX_ENDPOINT`] and a local test server are the two reasons this
    /// exists. A trailing slash is trimmed so the path built onto it is
    /// well-formed either way.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        endpoint
            .trim_end_matches('/')
            .clone_into(&mut self.endpoint);
        self
    }

    /// The endpoint base this provider talks to.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The account's project id, onboarding the account if it has none.
    ///
    /// Resolved once and cached; every later call returns the same id without
    /// touching the network.
    ///
    /// # Errors
    ///
    /// - [`Error::Auth`] if the credential is missing or rejected, or if
    ///   onboarding never produced a project id.
    /// - [`Error::Transport`], [`Error::Http`], [`Error::RateLimited`] as the
    ///   backend gave them.
    pub async fn project(&self) -> Result<&str, Error> {
        self.project
            .get_or_try_init(|| self.resolve_project())
            .await
            .map(String::as_str)
    }

    /// Sends one completion and returns the response with its status checked.
    ///
    /// The body is untouched — not a byte is read here — so the caller gets a
    /// response that is ready to stream. Reading it is task 4.4's job.
    ///
    /// The request is validated before anything is sent, so a caller bug costs
    /// no round trip and no token refresh.
    ///
    /// # Errors
    ///
    /// - [`Error::InvalidRequest`] if the messages cannot be expressed as
    ///   Gemini `contents` — see [`wire_role`].
    /// - [`Error::Auth`] if there is no usable credential, if the backend
    ///   rejected it (401/403), or if onboarding failed.
    /// - [`Error::RateLimited`] on a 429, carrying the backend's own advice
    ///   when it gave any.
    /// - [`Error::Http`] on any other rejection, and [`Error::Transport`] if
    ///   the request never completed.
    #[tracing::instrument(
        level = "info",
        name = "antigravity.request",
        skip_all,
        fields(
            model = %request.model,
            messages = request.messages.len(),
            project = Empty,
            request_id = Empty,
            status = Empty,
        )
    )]
    pub async fn send(&self, request: &CompletionRequest) -> Result<reqwest::Response, Error> {
        let contents = contents(&request.messages)?;
        let project = self.project().await?;
        let bearer = self.tokens.bearer().await?;
        let request_id = format!("{REQUEST_ID_PREFIX}{}", Uuid::new_v4());

        let span = tracing::Span::current();
        span.record("project", project);
        span.record("request_id", request_id.as_str());

        let payload = GenerateContent {
            project,
            model: &request.model,
            request: Generation {
                contents,
                // Empty text would be a systemInstruction that says nothing;
                // the caller means "no system prompt" and the backend agrees.
                system_instruction: request
                    .system
                    .as_deref()
                    .filter(|system| !system.trim().is_empty())
                    .map(|text| Instruction {
                        parts: vec![Part { text }],
                    }),
            },
            request_type: REQUEST_TYPE,
            user_agent: WRAPPER_USER_AGENT,
            request_id: &request_id,
        };

        let response = self
            .post(STREAM_ACTION)
            .bearer_auth(bearer)
            .header(ACCEPT, "text/event-stream")
            .json(&payload)
            .send()
            .await?;

        span.record("status", response.status().as_u16());
        checked(response).await
    }

    /// Resolves the account's project id, provisioning one if it has none.
    ///
    /// `loadCodeAssist` answers with the managed project when there is one. An
    /// account that has never used Code Assist gets no project and a list of
    /// tiers it may join; `onboardUser` then provisions one, and its
    /// long-running operation is polled until `done`. An operation that
    /// finishes without naming a project is answered by asking
    /// `loadCodeAssist` again, which is the authority on what the account has.
    #[tracing::instrument(
        level = "info",
        name = "antigravity.onboard",
        skip_all,
        fields(
            endpoint = %self.endpoint,
            project = Empty,
            onboarded = Empty,
            tier = Empty,
        )
    )]
    async fn resolve_project(&self) -> Result<String, Error> {
        let span = tracing::Span::current();

        let loaded: LoadCodeAssistResponse = self.call(LOAD_ACTION, &LoadCodeAssist::new()).await?;
        if let Some(project) = loaded.project_id() {
            span.record("project", project.as_str());
            span.record("onboarded", false);
            return Ok(project);
        }

        let tier = loaded
            .default_tier()
            .unwrap_or_else(|| DEFAULT_TIER.to_owned());
        span.record("tier", tier.as_str());
        let project = self.onboard(&tier).await?;
        span.record("project", project.as_str());
        span.record("onboarded", true);
        Ok(project)
    }

    /// Polls `onboardUser` until the operation completes, then reports the
    /// project it provisioned.
    async fn onboard(&self, tier: &str) -> Result<String, Error> {
        let body = OnboardUser::new(tier);

        for attempt in 0..ONBOARD_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(self.onboard_poll).await;
            }

            let onboarded: OnboardUserResponse = self.call(ONBOARD_ACTION, &body).await?;
            if !onboarded.done.unwrap_or(false) {
                continue;
            }
            if let Some(project) = onboarded.project_id() {
                return Ok(project);
            }

            // Onboarding finished but named no project. Ask the endpoint that
            // reports what the account has rather than inferring one.
            let loaded: LoadCodeAssistResponse =
                self.call(LOAD_ACTION, &LoadCodeAssist::new()).await?;
            return loaded.project_id().ok_or_else(|| {
                Error::Auth(
                    "antigravity onboarding completed without provisioning a project; \
                     the account may not be entitled to Code Assist"
                        .to_owned(),
                )
            });
        }

        Err(Error::Auth(format!(
            "antigravity onboarding did not finish after {ONBOARD_ATTEMPTS} attempts; \
             tier {tier} may be unavailable to this account"
        )))
    }

    /// POSTs a JSON body to an onboarding action and parses the JSON answer.
    ///
    /// Only onboarding goes through here. Completions are not parsed — their
    /// whole point is to be handed back unread.
    async fn call<B: Serialize, R: DeserializeOwned>(
        &self,
        action: &str,
        body: &B,
    ) -> Result<R, Error> {
        let bearer = self.tokens.bearer().await?;
        let response = self
            .post(action)
            .bearer_auth(bearer)
            .json(body)
            .send()
            .await?;
        let body = checked(response).await?.text().await?;

        serde_json::from_str(&body).map_err(|source| {
            Error::Auth(format!(
                "antigravity {action} returned a body that is not the expected JSON: \
                 {source}: {}",
                detail(&body)
            ))
        })
    }

    /// A POST to `action` carrying the headers the Antigravity client sends on
    /// every call. The credential is added by the caller, which is the only
    /// part that differs between them.
    fn post(&self, action: &str) -> reqwest::RequestBuilder {
        self.http
            .post(format!("{}/v1internal:{action}", self.endpoint))
            .header(USER_AGENT, CLIENT_USER_AGENT)
            .header("X-Goog-Api-Client", API_CLIENT)
            .header("Client-Metadata", CLIENT_METADATA)
    }

    /// Same provider with a shorter onboarding poll, so tests do not sleep.
    #[cfg(test)]
    fn with_onboard_poll(mut self, poll: Duration) -> Self {
        self.onboard_poll = poll;
        self
    }
}

impl Provider for Antigravity {
    fn name(&self) -> &'static str {
        NAME
    }

    /// Sends the request, then hands back the response body as deltas.
    ///
    /// The two halves of a completion, composed: [`send`](Self::send) fails
    /// eagerly and returns nothing streamable, and everything after the first
    /// byte is reported inside the stream. Nothing is read from the body here —
    /// the first poll of the returned stream is what starts reading.
    ///
    /// The span opened here stays open until the stream is dropped, so a trace
    /// shows a completion lasting as long as it really did, and the closing
    /// event ([`stream`]) lands inside it.
    ///
    /// # Errors
    ///
    /// Whatever [`send`](Self::send) failed with. Stream-side failures are not
    /// errors here: they arrive as `Err` items in the returned stream.
    #[tracing::instrument(
        level = "info",
        name = "antigravity.complete",
        skip_all,
        fields(provider = NAME, model = %request.model)
    )]
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionStream, Error> {
        let response = self.send(&request).await?;
        Ok(stream::deltas(response, tracing::Span::current()))
    }
}

/// Maps our [`Role`] onto Gemini's two content roles.
///
/// Gemini has `user` and `model`, and no third thing. A [`Role::System`] in the
/// history has nowhere to go: the system prompt is a separate field of the
/// request, and quietly moving a message into it would mean the request no
/// longer matched what the caller wrote or what the log recorded. So it is
/// refused, and a caller that meant the system prompt sets
/// [`CompletionRequest::system`].
///
/// [`Role::Unspecified`] is proto3's zero value — an unset field, not a role —
/// and is refused for the same reason: guessing which one was meant is how a
/// forgotten assignment turns into a wrong conversation.
fn wire_role(role: Role) -> Result<&'static str, Error> {
    match role {
        Role::User => Ok("user"),
        Role::Assistant => Ok("model"),
        Role::System => Err(Error::InvalidRequest(
            "antigravity has no system role inside the conversation; \
             put the system prompt in CompletionRequest::system"
                .to_owned(),
        )),
        Role::Unspecified => Err(Error::InvalidRequest(
            "a message in the request has an unset role".to_owned(),
        )),
    }
}

/// Turns our messages into Gemini `contents`, one part per message.
fn contents(messages: &[Message]) -> Result<Vec<Content<'_>>, Error> {
    if messages.is_empty() {
        return Err(Error::InvalidRequest(
            "a completion needs at least one message".to_owned(),
        ));
    }

    messages
        .iter()
        .map(|message| {
            Ok(Content {
                role: wire_role(message.role)?,
                parts: vec![Part {
                    text: &message.content,
                }],
            })
        })
        .collect()
}

/// Passes a successful response through untouched, and turns any other into an
/// [`Error`].
///
/// The body is read only on the failure path. On success the response is
/// returned with every byte still unread, which is what makes it streamable.
async fn checked(response: reqwest::Response) -> Result<reqwest::Response, Error> {
    if response.status().is_success() {
        return Ok(response);
    }
    Err(failure(response).await)
}

/// Classifies a rejection per the error contract in
/// [`Provider::complete`](super::Provider::complete).
///
/// 401 and 403 are [`Error::Auth`]: the credential is the problem and a person
/// has to act. 429 is [`Error::RateLimited`], carrying whatever the backend
/// said about when to come back. Everything else is [`Error::Http`] with a
/// snippet of the body, because there is nothing structured left to say.
async fn failure(response: reqwest::Response) -> Error {
    let status = response.status().as_u16();
    let header_advice = retry_after(response.headers());
    // A body that will not read has already told us its status, which is the
    // part that decides the variant.
    let body = response.text().await.unwrap_or_default();

    match status {
        401 | 403 => Error::Auth(format!(
            "antigravity rejected the credential with HTTP {status}: {}. \
             Run the login flow if this persists.",
            detail(&body)
        )),
        429 => Error::RateLimited {
            retry_after: header_advice.or_else(|| retry_delay(&body)),
        },
        _ => Error::http(status, &body),
    }
}

/// Reads `Retry-After` as a whole number of seconds.
///
/// Only the delta-seconds form is understood. The HTTP-date form is legal and
/// Google does not use it here; a date reads as no advice, which leaves the
/// caller backing off on its own schedule rather than parsing a second date
/// format for a case that does not arise.
fn retry_after(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

/// Reads the `RetryInfo` delay out of a Google API error body.
///
/// A 429 from this backend puts its advice in the body rather than in a header:
/// `error.details[]` carries a `RetryInfo` whose `retryDelay` is a protobuf
/// duration string like `3.957525076s`. Rounded up, because advice to wait 3.95
/// seconds is not satisfied by waiting 3.
fn retry_delay(body: &str) -> Option<u64> {
    let parsed: serde_json::Value = serde_json::from_str(body).ok()?;
    let delay = parsed
        .get("error")?
        .get("details")?
        .as_array()?
        .iter()
        .find_map(|entry| entry.get("retryDelay")?.as_str())?;
    let seconds: f64 = delay.strip_suffix('s')?.parse().ok()?;
    if seconds.is_finite() && seconds >= 0.0 {
        // `ceil` on a finite non-negative float, cast to an integer: the
        // saturating cast makes an absurd advice a large wait, not a wrong one.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        Some(seconds.ceil() as u64)
    } else {
        None
    }
}

/// A short, safe rendering of an error body.
///
/// Google API errors carry a `error.message`; when one is there it is the whole
/// story and the rest is envelope. Anything else is passed through truncated,
/// on a character boundary.
fn detail(body: &str) -> String {
    let message = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|parsed| {
            parsed
                .get("error")?
                .get("message")?
                .as_str()
                .map(str::to_owned)
        });
    let text = message.unwrap_or_else(|| body.trim().to_owned());

    if text.len() <= MAX_DETAIL {
        return text;
    }
    let end = (0..=MAX_DETAIL)
        .rev()
        .find(|&index| text.is_char_boundary(index))
        .unwrap_or(0);
    format!("{}…", &text[..end])
}

/// The `loadCodeAssist` request body.
#[derive(Serialize)]
struct LoadCodeAssist {
    /// Who is asking. The plugin also sends `duetProject` when it already knows
    /// a project id; the whole point of this call is that we do not.
    metadata: ClientMetadata,
}

impl LoadCodeAssist {
    fn new() -> Self {
        Self {
            metadata: ClientMetadata::CURRENT,
        }
    }
}

/// The `onboardUser` request body.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OnboardUser<'a> {
    /// Tier to provision into, from `loadCodeAssist`'s `allowedTiers`.
    tier_id: &'a str,

    /// Same client identification as the load call.
    metadata: ClientMetadata,
}

impl<'a> OnboardUser<'a> {
    fn new(tier_id: &'a str) -> Self {
        Self {
            tier_id,
            metadata: ClientMetadata::CURRENT,
        }
    }
}

/// Client identification sent in onboarding bodies — the same three values as
/// the `Client-Metadata` header, which the backend wants in both places.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientMetadata {
    ide_type: &'static str,
    platform: &'static str,
    plugin_type: &'static str,
}

impl ClientMetadata {
    const CURRENT: Self = Self {
        ide_type: "ANTIGRAVITY",
        platform: PLATFORM,
        plugin_type: "GEMINI",
    };
}

/// What `loadCodeAssist` answers.
///
/// Every field is optional: this is an internal API with no compatibility
/// promise, and a response missing a field it used to send should degrade into
/// "no project, ask to onboard" rather than a parse failure.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoadCodeAssistResponse {
    /// The account's managed project, when it has one.
    cloudaicompanion_project: Option<ProjectRef>,

    /// Tiers this account may be onboarded into.
    allowed_tiers: Option<Vec<Tier>>,
}

impl LoadCodeAssistResponse {
    /// The managed project id, if the account has one.
    fn project_id(&self) -> Option<String> {
        self.cloudaicompanion_project
            .as_ref()
            .and_then(ProjectRef::id)
    }

    /// The tier to onboard into: the one flagged default, else the first
    /// offered.
    fn default_tier(&self) -> Option<String> {
        let tiers = self.allowed_tiers.as_ref()?;
        tiers
            .iter()
            .find(|tier| tier.is_default.unwrap_or(false))
            .or_else(|| tiers.first())?
            .id
            .clone()
    }
}

/// A project as this API names one: sometimes the bare id, sometimes an object
/// holding it. Both spellings appear in the wild, so both are accepted.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ProjectRef {
    /// `"cloudaicompanionProject": "my-project"`
    Id(String),

    /// `"cloudaicompanionProject": { "id": "my-project" }`
    Object {
        /// The project id, when the object carries one.
        id: Option<String>,
    },
}

impl ProjectRef {
    fn id(&self) -> Option<String> {
        match self {
            Self::Id(id) => Some(id.clone()),
            Self::Object { id } => id.clone(),
        }
    }
}

/// One tier an account may be onboarded into.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Tier {
    /// Tier identifier, e.g. `free-tier`.
    id: Option<String>,

    /// Whether the backend considers this the tier to pick.
    is_default: Option<bool>,
}

/// What `onboardUser` answers: a long-running operation.
#[derive(serde::Deserialize)]
struct OnboardUserResponse {
    /// Whether provisioning has finished.
    done: Option<bool>,

    /// The operation's result, once it is done.
    response: Option<OnboardUserResult>,
}

impl OnboardUserResponse {
    fn project_id(&self) -> Option<String> {
        self.response
            .as_ref()?
            .cloudaicompanion_project
            .as_ref()
            .and_then(ProjectRef::id)
    }
}

/// The provisioned project, inside a completed operation.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OnboardUserResult {
    cloudaicompanion_project: Option<ProjectRef>,
}

/// The request wrapper: routing outside, the Gemini request inside.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GenerateContent<'a> {
    /// Google Cloud project the completion is billed and quota'd against.
    project: &'a str,

    /// Backend model id, passed through from the caller untouched.
    model: &'a str,

    /// The Gemini-shaped request itself.
    request: Generation<'a>,

    /// What kind of client is asking. See [`REQUEST_TYPE`].
    request_type: &'static str,

    /// Product name, not a UA string. See [`WRAPPER_USER_AGENT`].
    user_agent: &'static str,

    /// Unique per request. The backend uses it to deduplicate retries, so it
    /// has to be fresh for every send rather than per session or per provider.
    request_id: &'a str,
}

/// The Gemini request: the conversation, and the system prompt beside it.
///
/// `generationConfig` is documented here and deliberately absent:
/// [`CompletionRequest`] carries no sampling knobs to put in it, and the
/// backend accepts a request without one (verified live). It arrives in the
/// same change as the first field that needs it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Generation<'a> {
    /// Conversation history, oldest first.
    contents: Vec<Content<'a>>,

    /// The system prompt. An object with `parts` — a bare string is a 400.
    #[serde(skip_serializing_if = "Option::is_none")]
    system_instruction: Option<Instruction<'a>>,
}

/// One turn, in Gemini's shape.
#[derive(Serialize)]
struct Content<'a> {
    /// `user` or `model`. See [`wire_role`].
    role: &'static str,

    /// The turn's content. One text part per message: ARC has no multimodal or
    /// tool-call parts to send in Phase 1.
    parts: Vec<Part<'a>>,
}

/// The system prompt, in the object form the backend requires.
#[derive(Serialize)]
struct Instruction<'a> {
    parts: Vec<Part<'a>>,
}

/// A text part.
#[derive(Serialize)]
struct Part<'a> {
    text: &'a str,
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;

    use arc_proto::v1::Role;
    use futures::StreamExt;
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use wiremock::matchers::{body_json_string, method, path, query_param};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    use super::super::oauth::{OauthConfig, TokenManager};
    use super::super::{CompletionDelta, Usage};
    use super::{Antigravity, CompletionRequest, Error, Message, Provider, detail, retry_delay};

    /// The only credential these tests know. Nothing here is or resembles a
    /// real token (invariant 5), and nothing reads `data/secrets/`.
    const FAKE_ACCESS: &str = "test-access-token";

    /// Far enough out that [`TokenManager`] never tries to refresh: the token
    /// endpoint is not mocked, so a refresh would fail loudly.
    const EXPIRES_AT: u64 = 4_102_444_800;

    /// Writes a token file the manager can load.
    ///
    /// Written as raw JSON rather than through `TokenSet`, which is private to
    /// the oauth module. That the two agree on the format is that module's
    /// test; what matters here is having a bearer to assert on.
    fn token_file(dir: &Path) -> PathBuf {
        let path = dir.join("google_oauth.json");
        std::fs::write(
            &path,
            json!({
                "access_token": FAKE_ACCESS,
                "refresh_token": "test-refresh-token",
                "expires_at": EXPIRES_AT,
                "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            })
            .to_string(),
        )
        .expect("write token file");
        path
    }

    /// A provider pointed at `server`, with onboarding polls that do not sleep.
    fn provider(server: &MockServer, dir: &TempDir) -> Antigravity {
        let tokens = TokenManager::new(OauthConfig::default(), token_file(dir.path()));
        Antigravity::new(Arc::new(tokens))
            .with_endpoint(server.uri())
            .with_onboard_poll(Duration::ZERO)
    }

    fn request() -> CompletionRequest {
        CompletionRequest {
            model: "gemini-3-flash".to_owned(),
            system: Some("be terse".to_owned()),
            messages: vec![
                Message {
                    role: Role::User,
                    content: "hello".to_owned(),
                },
                Message {
                    role: Role::Assistant,
                    content: "hi".to_owned(),
                },
                Message {
                    role: Role::User,
                    content: "again".to_owned(),
                },
            ],
        }
    }

    /// `loadCodeAssist` answering with an already-provisioned project.
    async fn mount_load(server: &MockServer, body: Value, calls: u64) {
        Mock::given(method("POST"))
            .and(path("/v1internal:loadCodeAssist"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(calls)
            .mount(server)
            .await;
    }

    /// The one SSE frame the mocked completion answers with.
    const SSE_FRAME: &str = "data: {\"response\":{\"candidates\":[]}}\n\n";

    /// A streaming response with one frame — enough to be a 200 with a body
    /// that is never read.
    fn sse_response() -> ResponseTemplate {
        // `set_body_raw` rather than `set_body_string`, which would label the
        // frames `text/plain`.
        ResponseTemplate::new(200).set_body_raw(SSE_FRAME, "text/event-stream")
    }

    async fn mount_stream(server: &MockServer, calls: u64) {
        Mock::given(method("POST"))
            .and(path("/v1internal:streamGenerateContent"))
            .and(query_param("alt", "sse"))
            .respond_with(sse_response())
            .expect(calls)
            .mount(server)
            .await;
    }

    /// The JSON body of the nth recorded request.
    async fn recorded_body(server: &MockServer, index: usize) -> Value {
        let requests = server.received_requests().await.expect("requests");
        serde_json::from_slice(&requests[index].body).expect("json body")
    }

    #[tokio::test]
    async fn an_onboarded_account_resolves_its_project_from_load_code_assist() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(
            &server,
            json!({ "cloudaicompanionProject": "test-project" }),
            1,
        )
        .await;
        mount_stream(&server, 1).await;

        let response = provider(&server, &dir)
            .send(&request())
            .await
            .expect("send");

        assert_eq!(response.status(), 200);
        // Discovery asks with the client identification and nothing else: the
        // plugin also sends `duetProject`, which needs a project id we do not
        // have yet. `platform` is an enum value the backend validates.
        assert_eq!(
            recorded_body(&server, 0).await,
            json!({
                "metadata": {
                    "ideType": "ANTIGRAVITY",
                    "platform": "LINUX_AMD64",
                    "pluginType": "GEMINI",
                }
            })
        );
        assert_eq!(recorded_body(&server, 1).await["project"], "test-project");
    }

    /// The object spelling of the same field, which the backend also uses.
    #[tokio::test]
    async fn a_project_named_by_an_object_resolves_the_same_way() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(
            &server,
            json!({ "cloudaicompanionProject": { "id": "test-project" } }),
            1,
        )
        .await;

        let project = provider(&server, &dir)
            .project()
            .await
            .expect("project")
            .to_owned();

        assert_eq!(project, "test-project");
    }

    #[tokio::test]
    async fn an_account_without_a_project_is_onboarded_into_its_default_tier() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(
            &server,
            json!({
                "allowedTiers": [
                    { "id": "legacy-tier" },
                    { "id": "free-tier", "isDefault": true },
                ]
            }),
            1,
        )
        .await;
        // The first poll is still running; the second reports the project.
        Mock::given(method("POST"))
            .and(path("/v1internal:onboardUser"))
            .and(body_json_string(
                json!({
                    "tierId": "free-tier",
                    "metadata": {
                        "ideType": "ANTIGRAVITY",
                        "platform": "LINUX_AMD64",
                        "pluginType": "GEMINI",
                    },
                })
                .to_string(),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "done": false })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1internal:onboardUser"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "done": true,
                "response": { "cloudaicompanionProject": { "id": "provisioned-project" } },
            })))
            .expect(1)
            .mount(&server)
            .await;

        let project = provider(&server, &dir)
            .project()
            .await
            .expect("project")
            .to_owned();

        assert_eq!(project, "provisioned-project");
    }

    /// No `allowedTiers` at all: onboarding still has to name a tier, and
    /// `FREE` is the documented fallback.
    #[tokio::test]
    async fn onboarding_without_offered_tiers_falls_back_to_the_free_tier() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(&server, json!({}), 1).await;
        Mock::given(method("POST"))
            .and(path("/v1internal:onboardUser"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "done": true,
                "response": { "cloudaicompanionProject": { "id": "provisioned-project" } },
            })))
            .expect(1)
            .mount(&server)
            .await;

        provider(&server, &dir).project().await.expect("project");

        let body = recorded_body(&server, 1).await;
        assert_eq!(body["tierId"], "FREE");
    }

    /// Onboarding that finishes without naming a project falls back to asking
    /// `loadCodeAssist` again.
    #[tokio::test]
    async fn a_silent_onboarding_result_is_resolved_by_asking_again() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        Mock::given(method("POST"))
            .and(path("/v1internal:loadCodeAssist"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1internal:onboardUser"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "done": true })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1internal:loadCodeAssist"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "cloudaicompanionProject": "late-project" })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let project = provider(&server, &dir)
            .project()
            .await
            .expect("project")
            .to_owned();

        assert_eq!(project, "late-project");
    }

    #[tokio::test]
    async fn the_project_is_resolved_once_and_reused_across_sends() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        // `expect(1)` is the assertion: a second onboarding round trip fails
        // the test when the server is dropped.
        mount_load(
            &server,
            json!({ "cloudaicompanionProject": "test-project" }),
            1,
        )
        .await;
        mount_stream(&server, 2).await;

        let provider = provider(&server, &dir);
        provider.send(&request()).await.expect("first send");
        provider.send(&request()).await.expect("second send");

        assert_eq!(recorded_body(&server, 2).await["project"], "test-project");
    }

    #[tokio::test]
    async fn the_request_body_is_the_documented_wrapper() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(
            &server,
            json!({ "cloudaicompanionProject": "test-project" }),
            1,
        )
        .await;
        mount_stream(&server, 1).await;

        provider(&server, &dir)
            .send(&request())
            .await
            .expect("send");

        let body = recorded_body(&server, 1).await;
        let request_id = body["requestId"].as_str().expect("requestId");
        assert_eq!(
            body,
            json!({
                "project": "test-project",
                "model": "gemini-3-flash",
                "request": {
                    "contents": [
                        { "role": "user", "parts": [{ "text": "hello" }] },
                        // ROLE_ASSISTANT is Gemini's "model", not "assistant".
                        { "role": "model", "parts": [{ "text": "hi" }] },
                        { "role": "user", "parts": [{ "text": "again" }] },
                    ],
                    // An object with parts. A bare string is a 400.
                    "systemInstruction": { "parts": [{ "text": "be terse" }] },
                },
                "requestType": "agent",
                "userAgent": "antigravity",
                "requestId": request_id,
            })
        );
        let uuid = request_id.strip_prefix("agent-").expect("agent- prefix");
        assert_eq!(uuid.len(), 36, "requestId should carry a UUID: {uuid}");
    }

    #[tokio::test]
    async fn a_request_without_a_system_prompt_omits_the_system_instruction() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(
            &server,
            json!({ "cloudaicompanionProject": "test-project" }),
            1,
        )
        .await;
        mount_stream(&server, 1).await;

        let request = CompletionRequest {
            // An empty system prompt says nothing and is not sent.
            system: Some("   ".to_owned()),
            ..request()
        };
        provider(&server, &dir).send(&request).await.expect("send");

        let body = recorded_body(&server, 1).await;
        assert!(
            body["request"].get("systemInstruction").is_none(),
            "empty system prompt was sent: {body}"
        );
    }

    #[tokio::test]
    async fn every_request_carries_a_fresh_request_id() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(
            &server,
            json!({ "cloudaicompanionProject": "test-project" }),
            1,
        )
        .await;
        mount_stream(&server, 2).await;

        let provider = provider(&server, &dir);
        provider.send(&request()).await.expect("first send");
        provider.send(&request()).await.expect("second send");

        let first = recorded_body(&server, 1).await;
        let second = recorded_body(&server, 2).await;
        assert_ne!(first["requestId"], second["requestId"]);
    }

    #[tokio::test]
    async fn requests_carry_the_antigravity_client_headers_and_the_bearer() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(
            &server,
            json!({ "cloudaicompanionProject": "test-project" }),
            1,
        )
        .await;
        mount_stream(&server, 1).await;

        provider(&server, &dir)
            .send(&request())
            .await
            .expect("send");

        let requests = server.received_requests().await.expect("requests");
        let sent = |request: &Request, name: &str| {
            request
                .headers
                .get(name)
                .map(|value| value.to_str().expect("ascii header").to_owned())
        };

        // Both calls identify the client the same way and carry the bearer.
        for call in &requests {
            assert_eq!(
                sent(call, "user-agent").as_deref(),
                Some("antigravity/1.18.3 linux/amd64")
            );
            assert_eq!(
                sent(call, "x-goog-api-client").as_deref(),
                Some("google-cloud-sdk vscode_cloudshelleditor/0.1")
            );
            assert_eq!(
                sent(call, "client-metadata").as_deref(),
                Some(r#"{"ideType":"ANTIGRAVITY","platform":"LINUX_AMD64","pluginType":"GEMINI"}"#)
            );
            assert_eq!(
                sent(call, "authorization").as_deref(),
                Some("Bearer test-access-token")
            );
            assert_eq!(
                sent(call, "content-type").as_deref(),
                Some("application/json")
            );
        }

        // Only the streaming call asks for SSE.
        let (load, stream) = (&requests[0], &requests[1]);
        assert_eq!(sent(stream, "accept").as_deref(), Some("text/event-stream"));
        assert_ne!(sent(load, "accept").as_deref(), Some("text/event-stream"));
    }

    #[tokio::test]
    async fn a_successful_response_arrives_with_its_body_unread() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(
            &server,
            json!({ "cloudaicompanionProject": "test-project" }),
            1,
        )
        .await;
        mount_stream(&server, 1).await;

        let response = provider(&server, &dir)
            .send(&request())
            .await
            .expect("send");

        assert_eq!(
            response
                .headers()
                .get("content-type")
                .expect("content-type"),
            "text/event-stream"
        );
        // Nothing in `send` consumed it, so the frames are all still here.
        let body = response.text().await.expect("body");
        assert_eq!(body, SSE_FRAME);
    }

    /// One real completion, captured live. The parsing it exercises is
    /// [`super::stream`]'s; what this file tests with it is that the two halves
    /// are wired together at all.
    const FIXTURE: &[u8] = include_bytes!("../../../tests/fixtures/antigravity_stream.sse");

    /// The whole trait, end to end: a request goes out and the response body
    /// comes back as deltas.
    #[tokio::test]
    async fn a_completion_arrives_as_text_then_a_closing_usage() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(
            &server,
            json!({ "cloudaicompanionProject": "test-project" }),
            1,
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/v1internal:streamGenerateContent"))
            .and(query_param("alt", "sse"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(FIXTURE, "text/event-stream"))
            .expect(1)
            .mount(&server)
            .await;

        let provider = provider(&server, &dir);
        let deltas: Vec<_> = provider
            .complete(request())
            .await
            .expect("stream")
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, Error>>()
            .expect("deltas");

        assert_eq!(provider.name(), "antigravity");
        assert_eq!(
            deltas,
            [
                CompletionDelta::Text("hello arc".to_owned()),
                CompletionDelta::Done {
                    usage: Usage {
                        input_tokens: 11,
                        // 2 tokens of reply and 73 of thinking, both billed.
                        output_tokens: 75,
                    },
                },
            ]
        );
    }

    /// The eager half stays eager through the trait: a rejected request is an
    /// `Err` from `complete`, not an item in a stream the caller now has to
    /// drain.
    #[tokio::test]
    async fn a_rejected_completion_yields_no_stream_at_all() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(
            &server,
            json!({ "cloudaicompanionProject": "test-project" }),
            1,
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/v1internal:streamGenerateContent"))
            .respond_with(ResponseTemplate::new(401).set_body_string("expired"))
            .expect(1)
            .mount(&server)
            .await;

        let error = provider(&server, &dir)
            .complete(request())
            .await
            .err()
            .expect("401 should fail before any stream exists");

        assert!(matches!(error, Error::Auth(_)), "{error:?}");
    }

    /// Every status the contract names, mapped from the completion call.
    async fn error_from(status: u16, template: ResponseTemplate) -> Error {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        mount_load(
            &server,
            json!({ "cloudaicompanionProject": "test-project" }),
            1,
        )
        .await;
        Mock::given(method("POST"))
            .and(path("/v1internal:streamGenerateContent"))
            .respond_with(template)
            .expect(1)
            .mount(&server)
            .await;

        let error = provider(&server, &dir)
            .send(&request())
            .await
            .expect_err("status {status} should fail");
        assert!(status >= 400);
        error
    }

    #[tokio::test]
    async fn an_unauthenticated_response_is_an_auth_error() {
        let error = error_from(
            401,
            ResponseTemplate::new(401).set_body_json(json!({
                "error": { "code": 401, "message": "Request had invalid authentication credentials.", "status": "UNAUTHENTICATED" }
            })),
        )
        .await;

        let Error::Auth(message) = error else {
            panic!("401 must be an auth error, got {error:?}");
        };
        assert!(message.contains("invalid authentication"), "{message}");
    }

    #[tokio::test]
    async fn a_permission_denied_response_is_an_auth_error() {
        let error = error_from(403, ResponseTemplate::new(403).set_body_string("nope")).await;

        assert!(matches!(error, Error::Auth(_)), "{error:?}");
    }

    #[tokio::test]
    async fn a_rate_limit_reports_the_retry_after_header() {
        let error = error_from(
            429,
            ResponseTemplate::new(429)
                .insert_header("retry-after", "30")
                .set_body_string("slow down"),
        )
        .await;

        assert!(
            matches!(
                error,
                Error::RateLimited {
                    retry_after: Some(30)
                }
            ),
            "{error:?}"
        );
    }

    /// This backend puts its advice in the body, not the header.
    #[tokio::test]
    async fn a_rate_limit_reports_the_retry_info_in_the_body() {
        let error = error_from(
            429,
            ResponseTemplate::new(429).set_body_json(json!({
                "error": {
                    "code": 429,
                    "message": "You have exhausted your capacity on this model.",
                    "status": "RESOURCE_EXHAUSTED",
                    "details": [{
                        "@type": "type.googleapis.com/google.rpc.RetryInfo",
                        "retryDelay": "3.957525076s",
                    }],
                }
            })),
        )
        .await;

        // Rounded up: waiting three seconds does not satisfy advice of 3.95.
        assert!(
            matches!(
                error,
                Error::RateLimited {
                    retry_after: Some(4)
                }
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_rate_limit_with_no_advice_says_so() {
        let error = error_from(429, ResponseTemplate::new(429).set_body_string("slow down")).await;

        assert!(
            matches!(error, Error::RateLimited { retry_after: None }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn any_other_rejection_keeps_its_status_and_body() {
        let error = error_from(
            500,
            ResponseTemplate::new(500).set_body_string("upstream exploded"),
        )
        .await;

        assert!(
            matches!(error, Error::Http { status: 500, ref body } if body == "upstream exploded"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_request_that_never_completes_is_a_transport_error() {
        let dir = TempDir::new().expect("temp dir");
        let tokens = TokenManager::new(OauthConfig::default(), token_file(dir.path()));
        // Port 1 is not something anyone is listening on.
        let provider = Antigravity::new(Arc::new(tokens)).with_endpoint("http://127.0.0.1:1");

        let error = provider.send(&request()).await.expect_err("no server");

        assert!(matches!(error, Error::Transport(_)), "{error:?}");
    }

    /// Onboarding maps failures the same way a completion does.
    #[tokio::test]
    async fn a_rejected_onboarding_credential_is_an_auth_error() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");
        Mock::given(method("POST"))
            .and(path("/v1internal:loadCodeAssist"))
            .respond_with(ResponseTemplate::new(401).set_body_string("expired"))
            .mount(&server)
            .await;

        let error = provider(&server, &dir)
            .send(&request())
            .await
            .expect_err("onboarding should fail");

        assert!(matches!(error, Error::Auth(_)), "{error:?}");
    }

    #[tokio::test]
    async fn a_system_role_message_is_refused_before_anything_is_sent() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");

        let request = CompletionRequest {
            messages: vec![Message {
                role: Role::System,
                content: "be terse".to_owned(),
            }],
            ..request()
        };
        let error = provider(&server, &dir)
            .send(&request)
            .await
            .expect_err("system role in the history");

        let Error::InvalidRequest(message) = error else {
            panic!("expected an invalid request, got {error:?}");
        };
        assert!(message.contains("CompletionRequest::system"), "{message}");
        // Validation happens first: no project resolution, no token, no call.
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .is_empty(),
            "a request was sent despite the caller bug"
        );
    }

    #[tokio::test]
    async fn an_unset_role_is_refused() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");

        let request = CompletionRequest {
            messages: vec![Message {
                role: Role::Unspecified,
                content: "hello".to_owned(),
            }],
            ..request()
        };
        let error = provider(&server, &dir)
            .send(&request)
            .await
            .expect_err("unset role");

        assert!(matches!(error, Error::InvalidRequest(_)), "{error:?}");
    }

    #[tokio::test]
    async fn a_completion_with_no_messages_is_refused() {
        let server = MockServer::start().await;
        let dir = TempDir::new().expect("temp dir");

        let request = CompletionRequest {
            messages: Vec::new(),
            ..request()
        };
        let error = provider(&server, &dir)
            .send(&request)
            .await
            .expect_err("no messages");

        assert!(matches!(error, Error::InvalidRequest(_)), "{error:?}");
    }

    #[test]
    fn the_default_endpoint_is_production_and_overrides_lose_their_slash() {
        let dir = TempDir::new().expect("temp dir");
        let tokens = Arc::new(TokenManager::new(
            OauthConfig::default(),
            token_file(dir.path()),
        ));

        let default = Antigravity::new(Arc::clone(&tokens));
        assert_eq!(default.endpoint(), super::PRODUCTION_ENDPOINT);

        let overridden = Antigravity::new(tokens).with_endpoint("http://localhost:8080/");
        assert_eq!(overridden.endpoint(), "http://localhost:8080");
    }

    #[test]
    fn retry_delay_reads_the_protobuf_duration_and_ignores_anything_else() {
        assert_eq!(
            retry_delay(r#"{"error":{"details":[{"retryDelay":"3.957525076s"}]}}"#),
            Some(4)
        );
        assert_eq!(
            retry_delay(r#"{"error":{"details":[{"retryDelay":"12s"}]}}"#),
            Some(12)
        );
        // A duration in a shape we do not understand is no advice at all.
        assert_eq!(
            retry_delay(r#"{"error":{"details":[{"retryDelay":"soon"}]}}"#),
            None
        );
        assert_eq!(retry_delay("not json"), None);
        assert_eq!(retry_delay(r#"{"error":{}}"#), None);
    }

    #[test]
    fn detail_prefers_the_api_message_and_bounds_what_it_repeats() {
        assert_eq!(
            detail(r#"{"error":{"code":400,"message":"Invalid argument."}}"#),
            "Invalid argument."
        );
        assert_eq!(detail("  plain text  "), "plain text");

        // Multi-byte characters straddling the cap: a byte-index cut would
        // panic.
        let long = "é".repeat(super::MAX_DETAIL);
        let bounded = detail(&long);
        assert!(bounded.len() <= super::MAX_DETAIL + '…'.len_utf8());
        assert!(bounded.ends_with('…'));
    }
}
