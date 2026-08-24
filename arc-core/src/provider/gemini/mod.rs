mod stream;

use reqwest::header::{ACCEPT, AUTHORIZATION};
use serde::Serialize;

use crate::provider::{
    CompletionRequest, CompletionStream, Error, Message, Provider, Thinking, ToolDefinition,
    failure, stream as delta_stream,
};
use arc_proto::v1::Role;

const NAME: &str = "gemini";

pub const DEFAULT_ENDPOINT: &str = "https://generativelanguage.googleapis.com/v1beta/openai";

const COMPLETIONS_PATH: &str = "/chat/completions";

pub struct Gemini {
    endpoint: String,
    key: String,
    http: reqwest::Client,
}

// hand-written so a key never reaches a log line or a panic message
impl std::fmt::Debug for Gemini {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gemini")
            .field("endpoint", &self.endpoint)
            .field("key", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl Gemini {
    pub fn new(endpoint: &str, key: String) -> Self {
        let mut endpoint = endpoint.to_owned();
        endpoint.truncate(endpoint.trim_end_matches('/').len());
        Self {
            endpoint,
            key,
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(0)
                .build()
                .expect("default reqwest client"),
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Provider for Gemini {
    fn name(&self) -> &'static str {
        NAME
    }

    #[tracing::instrument(
        level = "info",
        name = "gemini.complete",
        skip_all,
        fields(
            provider = NAME,
            model = %request.model,
            role = crate::provider::role_label(request.role),
            messages = request.messages.len(),
        )
    )]
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionStream, Error> {
        let payload = Payload::new(&request)?;

        let response = self
            .http
            .post(format!("{}{COMPLETIONS_PATH}", self.endpoint))
            .header(ACCEPT, "text/event-stream")
            .header(AUTHORIZATION, format!("Bearer {}", self.key))
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(failure(response).await);
        }
        Ok(delta_stream::deltas(
            response,
            stream::Parser::default(),
            tracing::Span::current(),
        ))
    }
}

#[derive(Serialize)]
struct Payload<'a> {
    model: &'a str,
    messages: Vec<WireMessage<'a>>,
    stream: bool,
    stream_options: StreamOptions,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<WireTool<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    seed: Option<u64>,
    // measured 2026-08-24: `low` is the cheapest setting, and none of them
    // stop the thinking that `completion_tokens` leaves out
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<&'static str>,
}

#[derive(Serialize)]
struct StreamOptions {
    include_usage: bool,
}

#[derive(Serialize)]
#[serde(untagged)]
enum WireMessage<'a> {
    Text {
        role: &'static str,
        content: &'a str,
    },

    ToolCalls {
        role: &'static str,
        tool_calls: Vec<WireToolCall<'a>>,
    },

    ToolResult {
        role: &'static str,
        tool_call_id: &'a str,
        content: &'a str,
    },
}

#[derive(Serialize)]
struct WireToolCall<'a> {
    id: &'a str,
    #[serde(rename = "type")]
    kind: &'static str,
    function: WireCalledFunction<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    extra_content: Option<ExtraContent<'a>>,
}

#[derive(Serialize)]
struct ExtraContent<'a> {
    google: Google<'a>,
}

#[derive(Serialize)]
struct Google<'a> {
    thought_signature: &'a str,
}

#[derive(Serialize)]
struct WireCalledFunction<'a> {
    name: &'a str,
    arguments: &'a str,
}

#[derive(Serialize)]
struct WireTool<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    function: WireFunction<'a>,
}

#[derive(Serialize)]
struct WireFunction<'a> {
    name: &'a str,
    description: &'a str,
    parameters: &'a serde_json::Value,
}

const FUNCTION: &str = "function";

impl<'a> Payload<'a> {
    fn new(request: &'a CompletionRequest) -> Result<Self, Error> {
        let system = request
            .system
            .as_deref()
            .filter(|system| !system.trim().is_empty())
            .map(|content| WireMessage::Text {
                role: "system",
                content,
            });

        let turns: Vec<WireMessage> = request
            .messages
            .iter()
            .map(wire_message)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            model: &request.model,
            messages: system.into_iter().chain(turns).collect(),
            stream: true,
            stream_options: StreamOptions {
                include_usage: true,
            },
            tools: request.tools.iter().map(wire_tool).collect(),
            seed: request.seed,
            reasoning_effort: match request.thinking {
                Thinking::Default => None,
                Thinking::Off => Some("none"),
                // only some models have it, and it is the one level that
                // actually stops thinking; 3.7 Flash answers 400
                Thinking::Minimal => Some("minimal"),
                Thinking::Low => Some("low"),
                Thinking::Medium => Some("medium"),
                Thinking::High => Some("high"),
            },
        })
    }
}

fn wire_tool(tool: &ToolDefinition) -> WireTool<'_> {
    WireTool {
        kind: FUNCTION,
        function: WireFunction {
            name: &tool.name,
            description: &tool.description,
            parameters: &tool.parameters,
        },
    }
}

fn wire_message(message: &Message) -> Result<WireMessage<'_>, Error> {
    let (role, content) = match message {
        Message::Text { role, content } => (role, content),
        Message::ToolCalls(calls) => {
            let tool_calls = calls
                .iter()
                .map(|call| {
                    let signature = std::str::from_utf8(&call.provider_roundtrip).map_err(|_| {
                        Error::InvalidRequest(format!(
                            "the round-trip data for tool call `{}` is not the base64 text gemini sent",
                            call.name
                        ))
                    })?;
                    Ok(WireToolCall {
                        id: &call.id,
                        kind: FUNCTION,
                        function: WireCalledFunction {
                            name: &call.name,
                            arguments: &call.arguments,
                        },
                        // without this the next turn is a 400, not a degraded answer
                        extra_content: (!signature.is_empty()).then_some(ExtraContent {
                            google: Google {
                                thought_signature: signature,
                            },
                        }),
                    })
                })
                .collect::<Result<_, Error>>()?;
            return Ok(WireMessage::ToolCalls {
                role: "assistant",
                tool_calls,
            });
        }
        Message::ToolResult { call_id, content } => {
            return Ok(WireMessage::ToolResult {
                role: "tool",
                tool_call_id: call_id,
                content,
            });
        }
    };

    let role = match role {
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::System => {
            return Err(Error::InvalidRequest(
                "system prompts go in CompletionRequest::system, not the history".to_owned(),
            ));
        }
        Role::Unspecified => {
            return Err(Error::InvalidRequest(
                "a message in the request has an unset role".to_owned(),
            ));
        }
    };
    Ok(WireMessage::Text { role, content })
}

#[cfg(test)]
mod tests {
    use super::{Gemini, Payload};
    use crate::provider::{CompletionRequest, Message, Provider as _, Thinking, ToolCall};
    use arc_proto::v1::{Role, SessionRole};
    use serde_json::Value;

    fn request(messages: Vec<Message>) -> CompletionRequest {
        CompletionRequest {
            model: "gemini-3.7-flash".to_owned(),
            role: SessionRole::Concierge,
            system: None,
            messages,
            tools: Vec::new(),
            seed: None,
            thinking: Thinking::Default,
        }
    }

    fn body(request: &CompletionRequest) -> Value {
        serde_json::to_value(Payload::new(request).expect("builds")).expect("serializes")
    }

    fn call(signature: &[u8]) -> ToolCall {
        ToolCall {
            id: "call_4051480".to_owned(),
            index: 0,
            name: "get_time".to_owned(),
            arguments: r#"{"timezone":"UTC"}"#.to_owned(),
            provider_roundtrip: signature.to_vec(),
        }
    }

    #[test]
    fn a_tool_call_carries_its_thought_signature_back() {
        let json = body(&request(vec![Message::ToolCalls(vec![call(
            b"EoADCv0CARFN",
        )])]));

        let tool_call = &json["messages"][0]["tool_calls"][0];
        assert_eq!(
            tool_call["extra_content"]["google"]["thought_signature"],
            "EoADCv0CARFN"
        );
        assert_eq!(tool_call["id"], "call_4051480");
        assert_eq!(tool_call["function"]["name"], "get_time");
    }

    #[test]
    fn a_call_with_no_signature_sends_no_extra_content() {
        let json = body(&request(vec![Message::ToolCalls(vec![call(b"")])]));

        assert!(
            json["messages"][0]["tool_calls"][0]
                .get("extra_content")
                .is_none(),
            "an empty signature must not become an empty object: {json}"
        );
    }

    #[test]
    fn round_trip_data_that_is_not_text_is_refused_before_the_wire() {
        let request = request(vec![Message::ToolCalls(vec![call(&[0xff, 0xfe])])]);

        let Err(err) = Payload::new(&request) else {
            panic!("invalid utf-8 is not a signature");
        };
        assert!(err.to_string().contains("get_time"), "{err}");
    }

    #[test]
    fn a_plain_turn_serializes_as_openai_does() {
        let json = body(&request(vec![Message::Text {
            role: Role::User,
            content: "hi".to_owned(),
        }]));

        assert_eq!(json["model"], "gemini-3.7-flash");
        assert_eq!(json["stream"], true);
        assert_eq!(json["stream_options"]["include_usage"], true);
        assert_eq!(json["messages"][0]["role"], "user");
        assert_eq!(json["messages"][0]["content"], "hi");
    }

    #[test]
    fn a_key_never_appears_in_debug_output() {
        let provider = Gemini::new(super::DEFAULT_ENDPOINT, "sk-supersecret".to_owned());

        let rendered = format!("{provider:?}");
        assert!(!rendered.contains("sk-supersecret"), "{rendered}");
        assert_eq!(provider.name(), "gemini");
    }

    #[test]
    fn a_trailing_slash_does_not_change_the_endpoint() {
        let provider = Gemini::new("https://example.test/v1beta/openai/", "k".to_owned());
        assert_eq!(provider.endpoint(), "https://example.test/v1beta/openai");
    }
}
