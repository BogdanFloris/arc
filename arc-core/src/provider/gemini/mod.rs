mod stream;

use std::collections::HashMap;

use reqwest::header::ACCEPT;
use serde::Serialize;

use crate::provider::{
    CompletionRequest, CompletionStream, Error, Message, Provider, Thinking, ToolDefinition,
    failure, stream as delta_stream,
};
use arc_proto::v1::Role;

const NAME: &str = "gemini";

pub const DEFAULT_ENDPOINT: &str = "https://generativelanguage.googleapis.com/v1beta";

const KEY_HEADER: &str = "x-goog-api-key";

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
            .post(format!(
                "{}/models/{}:streamGenerateContent?alt=sse",
                self.endpoint, request.model
            ))
            .header(ACCEPT, "text/event-stream")
            .header(KEY_HEADER, &self.key)
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
#[serde(rename_all = "camelCase")]
struct Payload<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    system_instruction: Option<Content<'a>>,
    contents: Vec<Content<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<Tools<'a>>,
    #[serde(skip_serializing_if = "GenerationConfig::is_empty")]
    generation_config: GenerationConfig,
}

#[derive(Serialize)]
struct Content<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'static str>,
    parts: Vec<Part<'a>>,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Part<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    function_call: Option<FunctionCall<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    function_response: Option<FunctionResponse<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thought_signature: Option<&'a str>,
}

#[derive(Serialize)]
struct FunctionCall<'a> {
    name: &'a str,
    id: &'a str,
    args: serde_json::Value,
}

#[derive(Serialize)]
struct FunctionResponse<'a> {
    name: &'a str,
    id: &'a str,
    response: Output<'a>,
}

#[derive(Serialize)]
struct Output<'a> {
    output: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Tools<'a> {
    function_declarations: Vec<Declaration<'a>>,
}

#[derive(Serialize)]
struct Declaration<'a> {
    name: &'a str,
    description: &'a str,
    parameters: &'a serde_json::Value,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct GenerationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking_config: Option<ThinkingConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    seed: Option<u64>,
}

impl GenerationConfig {
    fn is_empty(&self) -> bool {
        self.thinking_config.is_none() && self.seed.is_none()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ThinkingConfig {
    thinking_level: &'static str,
}

impl<'a> Payload<'a> {
    fn new(request: &'a CompletionRequest) -> Result<Self, Error> {
        // there is no NONE level; MINIMAL is as far down as it goes
        let thinking_config = match request.thinking {
            Thinking::Default => None,
            Thinking::Minimal => Some("MINIMAL"),
            Thinking::Low => Some("LOW"),
            Thinking::Medium => Some("MEDIUM"),
            Thinking::High => Some("HIGH"),
        }
        .map(|thinking_level| ThinkingConfig { thinking_level });

        Ok(Self {
            system_instruction: request
                .system
                .as_deref()
                .filter(|system| !system.trim().is_empty())
                .map(|text| Content {
                    role: None,
                    parts: vec![Part {
                        text: Some(text),
                        ..Part::default()
                    }],
                }),
            contents: contents(&request.messages)?,
            tools: if request.tools.is_empty() {
                Vec::new()
            } else {
                vec![Tools {
                    function_declarations: request.tools.iter().map(declaration).collect(),
                }]
            },
            generation_config: GenerationConfig {
                thinking_config,
                seed: request.seed,
            },
        })
    }
}

fn declaration(tool: &ToolDefinition) -> Declaration<'_> {
    Declaration {
        name: &tool.name,
        description: &tool.description,
        parameters: &tool.parameters,
    }
}

fn contents(messages: &[Message]) -> Result<Vec<Content<'_>>, Error> {
    // a functionResponse must name its function, and only the call knows it
    let mut names: HashMap<&str, &str> = HashMap::new();
    let mut contents: Vec<Content> = Vec::new();

    for message in messages {
        match message {
            Message::Text { role, content } => {
                let role = match role {
                    Role::User => "user",
                    Role::Assistant => "model",
                    Role::System => {
                        return Err(Error::InvalidRequest(
                            "system prompts go in CompletionRequest::system, not the history"
                                .to_owned(),
                        ));
                    }
                    Role::Unspecified => {
                        return Err(Error::InvalidRequest(
                            "a message in the request has an unset role".to_owned(),
                        ));
                    }
                };
                contents.push(Content {
                    role: Some(role),
                    parts: vec![Part {
                        text: Some(content),
                        ..Part::default()
                    }],
                });
            }
            Message::ToolCalls(calls) => {
                let mut parts = Vec::with_capacity(calls.len());
                for call in calls {
                    names.insert(call.id.as_str(), call.name.as_str());
                    let args = serde_json::from_str(&call.arguments).map_err(|source| {
                        Error::InvalidRequest(format!(
                            "the arguments for tool call `{}` are not a JSON object: {source}",
                            call.name
                        ))
                    })?;
                    let signature = std::str::from_utf8(&call.provider_roundtrip).map_err(|_| {
                        Error::InvalidRequest(format!(
                            "the round-trip data for tool call `{}` is not the base64 text gemini sent",
                            call.name
                        ))
                    })?;
                    parts.push(Part {
                        function_call: Some(FunctionCall {
                            name: &call.name,
                            id: &call.id,
                            args,
                        }),
                        thought_signature: (!signature.is_empty()).then_some(signature),
                        ..Part::default()
                    });
                }
                contents.push(Content {
                    role: Some("model"),
                    parts,
                });
            }
            Message::ToolResult { call_id, content } => {
                let name = names.get(call_id.as_str()).copied().ok_or_else(|| {
                    Error::InvalidRequest(format!(
                        "the result for call {call_id} has no matching call in this request"
                    ))
                })?;
                let part = Part {
                    function_response: Some(FunctionResponse {
                        name,
                        id: call_id,
                        response: Output { output: content },
                    }),
                    ..Part::default()
                };
                // parallel results belong in one turn, the way the calls were
                match contents.last_mut() {
                    Some(last) if is_responses(last) => last.parts.push(part),
                    _ => contents.push(Content {
                        role: Some("user"),
                        parts: vec![part],
                    }),
                }
            }
        }
    }
    Ok(contents)
}

fn is_responses(content: &Content<'_>) -> bool {
    content
        .parts
        .iter()
        .all(|part| part.function_response.is_some())
}

#[cfg(test)]
mod tests {
    use super::{Gemini, Payload};
    use crate::provider::{CompletionRequest, Message, Provider as _, Thinking, ToolCall};
    use arc_proto::v1::{Role, SessionRole};
    use serde_json::Value;

    fn request(messages: Vec<Message>) -> CompletionRequest {
        CompletionRequest {
            model: "gemini-3.6-flash".to_owned(),
            role: SessionRole::Concierge,
            thinking: Thinking::Minimal,
            system: Some("Be terse.".to_owned()),
            messages,
            tools: Vec::new(),
            seed: None,
        }
    }

    fn body(request: &CompletionRequest) -> Value {
        serde_json::to_value(Payload::new(request).expect("builds")).expect("serializes")
    }

    fn call(signature: &[u8]) -> ToolCall {
        ToolCall {
            id: "call_2418851".to_owned(),
            index: 0,
            name: "get_time".to_owned(),
            arguments: r#"{"timezone":"UTC"}"#.to_owned(),
            provider_roundtrip: signature.to_vec(),
        }
    }

    #[test]
    fn a_plain_turn_is_system_instruction_and_contents() {
        let json = body(&request(vec![Message::Text {
            role: Role::User,
            content: "hi".to_owned(),
        }]));

        assert_eq!(json["systemInstruction"]["parts"][0]["text"], "Be terse.");
        assert!(
            json["systemInstruction"].get("role").is_none(),
            "the system instruction carries no role"
        );
        assert_eq!(json["contents"][0]["role"], "user");
        assert_eq!(json["contents"][0]["parts"][0]["text"], "hi");
        assert_eq!(
            json["generationConfig"]["thinkingConfig"]["thinkingLevel"],
            "MINIMAL"
        );
    }

    #[test]
    fn an_assistant_turn_is_a_model_turn() {
        let json = body(&request(vec![Message::Text {
            role: Role::Assistant,
            content: "sure".to_owned(),
        }]));

        assert_eq!(json["contents"][0]["role"], "model");
    }

    #[test]
    fn a_call_carries_its_signature_and_object_arguments() {
        let json = body(&request(vec![Message::ToolCalls(vec![call(b"EmYKZAER")])]));

        let part = &json["contents"][0]["parts"][0];
        assert_eq!(part["thoughtSignature"], "EmYKZAER");
        assert_eq!(part["functionCall"]["name"], "get_time");
        assert_eq!(part["functionCall"]["id"], "call_2418851");
        assert_eq!(
            part["functionCall"]["args"]["timezone"], "UTC",
            "the log stores a string; the wire wants an object"
        );
    }

    #[test]
    fn a_result_names_the_function_its_call_named() {
        let json = body(&request(vec![
            Message::ToolCalls(vec![call(b"sig")]),
            Message::ToolResult {
                call_id: "call_2418851".to_owned(),
                content: "2026-08-24T14:00:00Z".to_owned(),
            },
        ]));

        let response = &json["contents"][1]["parts"][0]["functionResponse"];
        assert_eq!(json["contents"][1]["role"], "user");
        assert_eq!(
            response["name"], "get_time",
            "the API rejects an empty name"
        );
        assert_eq!(response["id"], "call_2418851");
        assert_eq!(response["response"]["output"], "2026-08-24T14:00:00Z");
    }

    #[test]
    fn parallel_results_share_one_turn() {
        let second = ToolCall {
            id: "call_2".to_owned(),
            index: 1,
            name: "lookup".to_owned(),
            ..call(b"sig")
        };
        let json = body(&request(vec![
            Message::ToolCalls(vec![call(b"sig"), second]),
            Message::ToolResult {
                call_id: "call_2418851".to_owned(),
                content: "a".to_owned(),
            },
            Message::ToolResult {
                call_id: "call_2".to_owned(),
                content: "b".to_owned(),
            },
        ]));

        assert_eq!(json["contents"].as_array().expect("contents").len(), 2);
        assert_eq!(
            json["contents"][1]["parts"]
                .as_array()
                .expect("parts")
                .len(),
            2,
            "both results ride the same turn, the way the calls did"
        );
    }

    #[test]
    fn a_result_with_no_matching_call_is_refused_before_the_wire() {
        let request = request(vec![Message::ToolResult {
            call_id: "orphan".to_owned(),
            content: "x".to_owned(),
        }]);

        let Err(err) = Payload::new(&request) else {
            panic!("a response with no name cannot be built");
        };
        assert!(err.to_string().contains("orphan"), "{err}");
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
    fn no_thinking_level_means_the_field_is_absent() {
        let mut plain = request(vec![Message::Text {
            role: Role::User,
            content: "hi".to_owned(),
        }]);
        plain.thinking = Thinking::Default;

        assert!(
            body(&plain).get("generationConfig").is_none(),
            "an empty generationConfig should not be sent at all"
        );
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
        let provider = Gemini::new("https://example.test/v1beta/", "k".to_owned());
        assert_eq!(provider.endpoint(), "https://example.test/v1beta");
    }
}
