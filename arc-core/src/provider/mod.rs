pub mod openai;
pub mod sse;
pub(crate) mod stream;

use std::future::Future;
use std::pin::Pin;

use arc_proto::v1::Role;
use futures::Stream;

const MAX_BODY_SNIPPET: usize = 512;

#[derive(Debug, Clone, PartialEq)]
pub struct CompletionRequest {
    pub model: String,

    pub system: Option<String>,

    pub messages: Vec<Message>,

    pub tools: Vec<ToolDefinition>,

    pub seed: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolDefinition {
    pub name: String,

    pub description: String,

    pub parameters: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Text { role: Role, content: String },

    ToolCalls(Vec<ToolCall>),

    ToolResult { call_id: String, content: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    pub id: String,

    pub index: u32,

    pub name: String,

    pub arguments: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionDelta {
    Text(String),

    Reasoning(String),

    ToolCall(ToolCall),

    Done { usage: Usage, stop: Stop },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    EndTurn,

    ToolCalls,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub input_tokens: u32,

    pub output_tokens: u32,
}

pub type CompletionStream = Pin<Box<dyn Stream<Item = Result<CompletionDelta, Error>> + Send>>;

pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str;

    fn complete(
        &self,
        request: CompletionRequest,
    ) -> impl Future<Output = Result<CompletionStream, Error>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("provider cannot send this request: {0}")]
    InvalidRequest(String),

    #[error("provider transport failed: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("provider returned HTTP {status}: {body}")]
    Http { status: u16, body: String },

    #[error("provider authentication failed: {0}")]
    Auth(String),

    #[error(
        "provider rate limited{}{}",
        retry_after.map_or(String::new(), |s| format!(", retry after {s}s")),
        if detail.is_empty() { String::new() } else { format!(": {detail}") }
    )]
    RateLimited {
        retry_after: Option<u64>,
        detail: String,
    },

    #[error("malformed provider stream: {0}")]
    MalformedStream(String),
}

impl Error {
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
        Provider, Role, Stop, ToolCall, Usage,
    };

    struct MockProvider {
        items: Vec<Result<CompletionDelta, Error>>,
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
            messages: vec![Message::Text {
                role: Role::User,
                content: "hello".to_owned(),
            }],
            tools: Vec::new(),
            seed: None,
        }
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    struct Drained {
        text: String,
        reasoning: String,
        calls: Vec<ToolCall>,
        ending: Option<(Usage, Stop)>,
    }

    async fn drain<P: Provider>(
        provider: &P,
        request: CompletionRequest,
    ) -> Result<Drained, Error> {
        let mut stream = provider.complete(request).await?;
        let mut drained = Drained::default();
        while let Some(item) = stream.next().await {
            match item? {
                CompletionDelta::Text(chunk) => drained.text.push_str(&chunk),
                CompletionDelta::Reasoning(chunk) => drained.reasoning.push_str(&chunk),
                CompletionDelta::ToolCall(call) => drained.calls.push(call),
                CompletionDelta::Done { usage, stop } => drained.ending = Some((usage, stop)),
            }
        }
        Ok(drained)
    }

    #[tokio::test]
    async fn collects_text_chunks_and_closing_usage() {
        let usage = Usage {
            input_tokens: 12,
            output_tokens: 3,
        };
        let provider = MockProvider::streaming(vec![
            Ok(CompletionDelta::Reasoning("thinking".to_owned())),
            Ok(CompletionDelta::Text("hello".to_owned())),
            Ok(CompletionDelta::Text(", world".to_owned())),
            Ok(CompletionDelta::Done {
                usage,
                stop: Stop::EndTurn,
            }),
        ]);

        let drained = drain(&provider, request()).await.expect("stream");

        assert_eq!(provider.name(), "mock");
        assert_eq!(
            drained,
            Drained {
                text: "hello, world".to_owned(),
                reasoning: "thinking".to_owned(),
                calls: Vec::new(),
                ending: Some((usage, Stop::EndTurn)),
            }
        );
    }

    #[tokio::test]
    async fn a_turn_that_wants_tools_ends_saying_so() {
        let call = ToolCall {
            id: "Mv4PbWn7".to_owned(),
            index: 0,
            name: "memory_search".to_owned(),
            arguments: r#"{"query": "arc"}"#.to_owned(),
        };
        let provider = MockProvider::streaming(vec![
            Ok(CompletionDelta::ToolCall(call.clone())),
            Ok(CompletionDelta::Done {
                usage: Usage::default(),
                stop: Stop::ToolCalls,
            }),
        ]);

        let drained = drain(&provider, request()).await.expect("stream");

        assert!(drained.text.is_empty());
        assert_eq!(drained.calls, [call]);
        assert_eq!(drained.ending, Some((Usage::default(), Stop::ToolCalls)));
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

    #[tokio::test]
    async fn completions_are_drivable_from_a_spawned_task() {
        let usage = Usage {
            input_tokens: 1,
            output_tokens: 1,
        };
        let provider = MockProvider::streaming(vec![
            Ok(CompletionDelta::Text("spawned".to_owned())),
            Ok(CompletionDelta::Done {
                usage,
                stop: Stop::EndTurn,
            }),
        ]);

        let joined = tokio::spawn(async move { drain(&provider, request()).await })
            .await
            .expect("task");

        assert_eq!(
            joined.expect("stream"),
            Drained {
                text: "spawned".to_owned(),
                ending: Some((usage, Stop::EndTurn)),
                ..Drained::default()
            }
        );
    }

    #[test]
    fn http_error_keeps_short_bodies_whole() {
        let err = Error::http(429, "slow down");

        assert!(matches!(err, Error::Http { status: 429, body } if body == "slow down"));
    }

    #[test]
    fn http_error_truncates_long_bodies_on_a_character_boundary() {
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
