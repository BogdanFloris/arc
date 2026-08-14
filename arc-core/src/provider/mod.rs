//! Providers: one streaming-completion interface over every model backend.
//!
//! A provider turns a [`CompletionRequest`] into a stream of
//! [`CompletionDelta`]s (DESIGN.md §6). Everything a particular backend needs —
//! its wire format, its auth, its required headers, its SSE dialect — stays
//! behind [`Provider`]. Callers see text chunks and a closing token count, and
//! never learn which vendor produced them.
//!
//! This module is the interface and nothing else. Auth, HTTP, and stream
//! parsing land in sibling modules and construct the [`Error`] variants defined
//! here. Tracing spans belong with those calls, not with these type
//! definitions: there is no operation here to instrument.
//!
//! The vocabulary is protobuf's wherever protobuf already has one. [`Message`]
//! carries [`arc_proto::v1::Role`] rather than a parallel enum, and [`Usage`]
//! mirrors the token counts in `wire.proto`'s `StreamEnd` (DESIGN.md §7). One
//! spelling of a concept, defined in `arc-proto`, reused here.
//!
//! # Deliberately absent
//!
//! DESIGN.md §6 also names model listing, token counting, and capability flags.
//! All three are future *additive* trait methods. Nothing in Phase 1 calls
//! them, and inferring their shape from a single implementation is how an
//! interface ends up shaped like its first backend.
//!
//! Tool calls are absent for that reason and one more: normalizing
//! tool-calling differences across providers is later-phase work.
//! [`CompletionDelta`] grows a variant when that work starts. Leaving the
//! types out now means no half-designed tool-call vocabulary to migrate off.

pub mod openai;
pub mod sse;
pub(crate) mod stream;

use std::future::Future;
use std::pin::Pin;

use arc_proto::v1::Role;
use futures::Stream;

/// Longest error body [`Error::http`] keeps, in bytes.
///
/// Error bodies are read by a person looking at a log line. A backend that
/// answers a rejected request with a megabyte of HTML has still told us
/// everything useful in its first few hundred bytes.
const MAX_BODY_SNIPPET: usize = 512;

/// A completion to run: which model, what context, and the conversation so far.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionRequest {
    /// Backend-specific model identifier, passed through untouched. `arc-core`
    /// does not maintain a model registry; the caller names a model the chosen
    /// provider understands.
    pub model: String,

    /// System prompt, when the caller has one. Providers place it wherever
    /// their API wants it — a top-level field, a first message, a header —
    /// which is exactly the difference this option exists to hide.
    pub system: Option<String>,

    /// Conversation history, oldest first. The final message is normally the
    /// user turn being answered.
    pub messages: Vec<Message>,
}

/// One turn of conversation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Who produced the turn. [`arc_proto::v1::Role`] is the log's vocabulary
    /// already; the provider layer reuses it instead of translating at this
    /// boundary, and each provider maps it to its own wire names.
    pub role: Role,

    /// The turn's text.
    pub content: String,
}

/// One event in a completion stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionDelta {
    /// A chunk of generated text. Chunk boundaries are the backend's and mean
    /// nothing: concatenating every `Text` in order yields the whole reply.
    Text(String),

    /// The last item of a well-formed stream. A stream that ends without it was
    /// cut short, and a caller that needs to know the difference should track
    /// whether it saw one.
    Done {
        /// What the completion cost.
        usage: Usage,
    },
}

/// Token counts for one completion.
///
/// Mirrors `StreamEnd` in `wire.proto` so the daemon can forward these numbers
/// to clients without a second definition of the same two integers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    /// Tokens in the prompt the backend billed for.
    pub input_tokens: u32,

    /// Tokens the backend generated.
    pub output_tokens: u32,
}

/// A completion in flight: text chunks, then a [`CompletionDelta::Done`].
///
/// Boxed on purpose. A completion is network-bound, so one allocation per call
/// is not measurable beside a round trip, and the alternative — an associated
/// stream type — puts a second type parameter and its bounds in every
/// signature that touches a provider.
pub type CompletionStream = Pin<Box<dyn Stream<Item = Result<CompletionDelta, Error>> + Send>>;

/// A model backend.
///
/// Not dyn-compatible: `complete` returns an opaque future, so `Provider` has
/// an associated type and `dyn Provider` will not compile. Phase 1 has one
/// implementation and one call site, so static dispatch costs nothing.
/// DESIGN.md §6 does want provider choice per completion, and the day a second
/// provider makes that a runtime decision this trait needs an answer — an
/// erased shim that boxes the future, or an enum over the implementations.
/// That question gets decided with both providers in hand, not now.
pub trait Provider: Send + Sync {
    /// Short stable name for this backend, used in traces, log lines, and
    /// `SessionCreated.provider`. It identifies the implementation, not the
    /// model: one provider serves many models.
    fn name(&self) -> &'static str;

    /// Start a completion.
    ///
    /// Failure splits by when it happens. Everything up to the first byte of
    /// the response — no credential, connection refused, request rejected —
    /// fails here, eagerly, and the caller gets no stream at all. Anything
    /// after that point arrives as an `Err` item inside the stream, because by
    /// then the caller may already hold text it has to decide what to do with.
    ///
    /// The returned future is `Send` so the daemon can drive completions from
    /// spawned tasks; that bound is why this is a desugared `impl Future`
    /// rather than a bare `async fn`, which leaves auto traits unspecified.
    ///
    /// # Errors
    ///
    /// Any [`Error`] variant, depending on where setup failed:
    /// [`Error::InvalidRequest`] when the request cannot be expressed at all,
    /// [`Error::Auth`] when there is no usable credential,
    /// [`Error::Transport`] when the request never completed, [`Error::Http`]
    /// or [`Error::RateLimited`] when the backend rejected it.
    fn complete(
        &self,
        request: CompletionRequest,
    ) -> impl Future<Output = Result<CompletionStream, Error>> + Send;
}

/// Everything a provider call can fail with, at setup or mid-stream.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The [`CompletionRequest`] cannot be expressed in the backend's wire
    /// format, so nothing was sent. A caller bug, not a backend condition:
    /// retrying the same request produces the same error.
    ///
    /// This is where a provider says no rather than guessing. A backend with no
    /// place for a [`Role::System`] message inside the history, for instance,
    /// could quietly fold it into the system prompt — and then the log and the
    /// prompt would disagree about what was sent. Refusing keeps the caller
    /// honest about which field it meant.
    #[error("provider cannot send this request: {0}")]
    InvalidRequest(String),

    /// The request never reached the backend, or the connection broke under
    /// it: DNS, TLS, timeout, socket. Retryable in the way network failures
    /// are.
    #[error("provider transport failed: {0}")]
    Transport(#[from] reqwest::Error),

    /// The backend answered with a non-success status. `body` is a snippet —
    /// see [`Error::http`].
    #[error("provider returned HTTP {status}: {body}")]
    Http {
        /// Status code the backend returned.
        status: u16,
        /// Leading bytes of the response body, for a human reading the log.
        body: String,
    },

    /// No usable credential: never signed in, refresh failed, token rejected.
    /// Separate from [`Error::Http`] with a 401 because the fix is a person
    /// re-authenticating, not a retry.
    #[error("provider authentication failed: {0}")]
    Auth(String),

    /// The backend is throttling. `retry_after` is its own advice, in seconds,
    /// when it gave any; `None` means back off on the caller's schedule.
    #[error(
        "provider rate limited{}{}",
        retry_after.map_or(String::new(), |s| format!(", retry after {s}s")),
        if detail.is_empty() { String::new() } else { format!(": {detail}") }
    )]
    RateLimited {
        /// Seconds the backend asked the caller to wait.
        retry_after: Option<u64>,
        /// What the backend said about which limit tripped — a 429 body names
        /// its quota, and "which quota" is the whole diagnosis. Empty when
        /// the body said nothing usable.
        detail: String,
    },

    /// The response did not parse: a malformed SSE frame, JSON that does not
    /// match the schema, a stream that stopped mid-event. The transport worked
    /// and what came over it did not make sense — which is a bug in one of the
    /// two ends, not a condition to retry through.
    #[error("malformed provider stream: {0}")]
    MalformedStream(String),
}

impl Error {
    /// Build an [`Error::Http`], truncating `body` to [`MAX_BODY_SNIPPET`]
    /// bytes on a character boundary.
    ///
    /// Truncation lives here so "snippet" is a property of the variant rather
    /// than a rule each caller has to remember.
    #[must_use]
    pub fn http(status: u16, body: &str) -> Self {
        let end = if body.len() <= MAX_BODY_SNIPPET {
            body.len()
        } else {
            (0..=MAX_BODY_SNIPPET)
                .rev()
                .find(|&i| body.is_char_boundary(i))
                .unwrap_or(0)
        };
        Self::Http {
            status,
            body: body[..end].to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, stream};

    use super::{
        CompletionDelta, CompletionRequest, CompletionStream, Error, MAX_BODY_SNIPPET, Message,
        Provider, Role, Usage,
    };

    /// A provider with no backend: it replays a canned item sequence.
    ///
    /// This exists to prove the trait is implementable and consumable without a
    /// network — that the stream type, the eager/lazy error split, and the
    /// `Send` bounds all hold together.
    struct MockProvider {
        /// Items the returned stream yields, in order.
        items: Vec<Result<CompletionDelta, Error>>,
        /// When set, `complete` fails eagerly and yields no stream.
        setup_failure: Option<&'static str>,
    }

    impl MockProvider {
        fn streaming(items: Vec<Result<CompletionDelta, Error>>) -> Self {
            Self {
                items,
                setup_failure: None,
            }
        }

        fn failing_setup(reason: &'static str) -> Self {
            Self {
                items: Vec::new(),
                setup_failure: Some(reason),
            }
        }
    }

    impl Provider for MockProvider {
        fn name(&self) -> &'static str {
            "mock"
        }

        async fn complete(&self, _request: CompletionRequest) -> Result<CompletionStream, Error> {
            if let Some(reason) = self.setup_failure {
                return Err(Error::Auth(reason.to_owned()));
            }
            let items: Vec<_> = self
                .items
                .iter()
                .map(|item| match item {
                    Ok(delta) => Ok(delta.clone()),
                    Err(err) => Err(Error::MalformedStream(err.to_string())),
                })
                .collect();
            Ok(Box::pin(stream::iter(items)))
        }
    }

    fn request() -> CompletionRequest {
        CompletionRequest {
            model: "test-model".to_owned(),
            system: Some("be terse".to_owned()),
            messages: vec![Message {
                role: Role::User,
                content: "hello".to_owned(),
            }],
        }
    }

    /// Drive any provider to completion, generically: text joined in order,
    /// the closing usage if the stream produced one.
    async fn drain<P: Provider>(
        provider: &P,
        request: CompletionRequest,
    ) -> Result<(String, Option<Usage>), Error> {
        let mut stream = provider.complete(request).await?;
        let mut text = String::new();
        let mut usage = None;
        while let Some(item) = stream.next().await {
            match item? {
                CompletionDelta::Text(chunk) => text.push_str(&chunk),
                CompletionDelta::Done { usage: done } => usage = Some(done),
            }
        }
        Ok((text, usage))
    }

    #[tokio::test]
    async fn collects_text_chunks_and_closing_usage() {
        let usage = Usage {
            input_tokens: 12,
            output_tokens: 3,
        };
        let provider = MockProvider::streaming(vec![
            Ok(CompletionDelta::Text("hello".to_owned())),
            Ok(CompletionDelta::Text(", world".to_owned())),
            Ok(CompletionDelta::Done { usage }),
        ]);

        let (text, seen) = drain(&provider, request()).await.expect("stream");

        assert_eq!(provider.name(), "mock");
        assert_eq!(text, "hello, world");
        assert_eq!(seen, Some(usage));
    }

    #[tokio::test]
    async fn setup_failure_yields_no_stream() {
        let provider = MockProvider::failing_setup("no token");

        let err = drain(&provider, request())
            .await
            .expect_err("setup failure");

        assert!(matches!(err, Error::Auth(reason) if reason == "no token"));
    }

    #[tokio::test]
    async fn mid_stream_failure_arrives_after_the_text_before_it() {
        let provider = MockProvider::streaming(vec![
            Ok(CompletionDelta::Text("partial".to_owned())),
            Err(Error::MalformedStream("truncated frame".to_owned())),
        ]);

        let mut stream = provider.complete(request()).await.expect("stream");

        assert_eq!(
            stream.next().await.expect("first item").expect("text"),
            CompletionDelta::Text("partial".to_owned())
        );
        let err = stream
            .next()
            .await
            .expect("second item")
            .expect_err("mid-stream failure");
        assert!(matches!(err, Error::MalformedStream(_)));
    }

    /// The daemon drives completions from spawned tasks, so both the future and
    /// the stream have to be `Send`. `tokio::spawn` is what proves it.
    #[tokio::test]
    async fn completions_are_drivable_from_a_spawned_task() {
        let usage = Usage {
            input_tokens: 1,
            output_tokens: 1,
        };
        let provider = MockProvider::streaming(vec![
            Ok(CompletionDelta::Text("spawned".to_owned())),
            Ok(CompletionDelta::Done { usage }),
        ]);

        let joined = tokio::spawn(async move { drain(&provider, request()).await })
            .await
            .expect("task");

        assert_eq!(joined.expect("stream"), ("spawned".to_owned(), Some(usage)));
    }

    #[test]
    fn http_error_keeps_short_bodies_whole() {
        let err = Error::http(429, "slow down");

        assert!(matches!(err, Error::Http { status: 429, body } if body == "slow down"));
    }

    #[test]
    fn http_error_truncates_long_bodies_on_a_character_boundary() {
        // Multi-byte characters straddling the cap: a byte-index cut would panic.
        let body = "é".repeat(MAX_BODY_SNIPPET);

        let Error::Http { body: snippet, .. } = Error::http(500, &body) else {
            panic!("expected an HTTP error");
        };

        assert!(snippet.len() <= MAX_BODY_SNIPPET);
        assert!(body.starts_with(&snippet));
        assert!(
            snippet.len() > MAX_BODY_SNIPPET - 4,
            "truncated too eagerly"
        );
    }

    #[test]
    fn rate_limit_display_mentions_retry_advice_and_detail_only_when_given() {
        assert_eq!(
            Error::RateLimited {
                retry_after: Some(30),
                detail: String::new(),
            }
            .to_string(),
            "provider rate limited, retry after 30s"
        );
        assert_eq!(
            Error::RateLimited {
                retry_after: None,
                detail: String::new(),
            }
            .to_string(),
            "provider rate limited"
        );
        assert_eq!(
            Error::RateLimited {
                retry_after: Some(4),
                detail: "quota exceeded: GenerateRequestsPerMinute".to_owned(),
            }
            .to_string(),
            "provider rate limited, retry after 4s: quota exceeded: GenerateRequestsPerMinute"
        );
    }
}
