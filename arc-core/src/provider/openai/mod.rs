mod stream;

use reqwest::header::ACCEPT;
use serde::Serialize;

use crate::provider::{
    CompletionRequest, CompletionStream, Error, Message, Provider, ToolDefinition,
    stream as delta_stream,
};
use arc_proto::v1::Role;

const NAME: &str = "openai-compat";

const COMPLETIONS_PATH: &str = "/v1/chat/completions";

#[derive(Debug)]
pub struct OpenAiCompat {
    endpoint: String,

    http: reqwest::Client,
}

impl OpenAiCompat {
    #[must_use]
    pub fn new(endpoint: impl Into<String>) -> Self {
        let mut endpoint = endpoint.into();
        endpoint.truncate(endpoint.trim_end_matches('/').len());
        Self {
            endpoint,
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(0)
                .build()
                .expect("default reqwest client"),
        }
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Provider for OpenAiCompat {
    fn name(&self) -> &'static str {
        NAME
    }

    #[tracing::instrument(
        level = "info",
        name = "openai.complete",
        skip_all,
        fields(provider = NAME, model = %request.model, messages = request.messages.len())
    )]
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionStream, Error> {
        let payload = Payload::new(&request)?;

        let response = self
            .http
            .post(format!("{}{COMPLETIONS_PATH}", self.endpoint))
            .header(ACCEPT, "text/event-stream")
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
            return Ok(WireMessage::ToolCalls {
                role: "assistant",
                tool_calls: calls
                    .iter()
                    .map(|call| WireToolCall {
                        id: &call.id,
                        kind: FUNCTION,
                        function: WireCalledFunction {
                            name: &call.name,
                            arguments: &call.arguments,
                        },
                    })
                    .collect(),
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

async fn failure(response: reqwest::Response) -> Error {
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();

    match status {
        401 | 403 => Error::Auth(format!(
            "the endpoint rejected the request with HTTP {status}: {}",
            snippet(&body)
        )),
        429 => Error::RateLimited {
            retry_after: None,
            detail: snippet(&body),
        },
        _ => Error::http(status, &body),
    }
}

fn snippet(body: &str) -> String {
    #[derive(serde::Deserialize)]
    struct ErrorBody {
        error: ErrorDetail,
    }
    #[derive(serde::Deserialize)]
    struct ErrorDetail {
        message: String,
    }

    if let Ok(parsed) = serde_json::from_str::<ErrorBody>(body) {
        return parsed.error.message;
    }
    let mut prefix = body.trim().to_owned();
    let mut end = prefix.len().min(200);
    while !prefix.is_char_boundary(end) {
        end -= 1;
    }
    prefix.truncate(end);
    prefix
}

#[cfg(test)]
mod tests {
    use futures::StreamExt as _;
    use serde_json::{Value, json};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    use super::*;
    use crate::provider::{CompletionDelta, Stop, ToolCall, Usage};

    fn request(system: Option<&str>, turns: &[(Role, &str)]) -> CompletionRequest {
        CompletionRequest {
            model: "qwen3-8b".to_owned(),
            system: system.map(str::to_owned),
            messages: turns
                .iter()
                .map(|(role, content)| Message::Text {
                    role: *role,
                    content: (*content).to_owned(),
                })
                .collect(),
            tools: Vec::new(),
            seed: None,
        }
    }

    async fn complete_against(
        template: ResponseTemplate,
        req: CompletionRequest,
    ) -> (
        Result<Vec<Result<CompletionDelta, Error>>, Error>,
        Vec<Request>,
    ) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(COMPLETIONS_PATH))
            .respond_with(template)
            .mount(&server)
            .await;

        let provider = OpenAiCompat::new(server.uri());
        let outcome = match provider.complete(req).await {
            Ok(stream) => Ok(stream.collect().await),
            Err(error) => Err(error),
        };
        (
            outcome,
            server.received_requests().await.unwrap_or_default(),
        )
    }

    fn sse_body(text: &str) -> String {
        format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"index":0,"delta":{"content":text},"finish_reason":null}]}),
            json!({"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":3}}),
        )
    }

    #[tokio::test]
    async fn a_completion_round_trips_text_and_usage() {
        let template = ResponseTemplate::new(200)
            .insert_header("content-type", "text/event-stream")
            .set_body_string(sse_body("hello arc"));
        let (outcome, _) = complete_against(template, request(None, &[(Role::User, "hi")])).await;

        let deltas: Vec<CompletionDelta> = outcome
            .expect("request accepted")
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("stream decodes");
        assert_eq!(
            deltas,
            [
                CompletionDelta::Text("hello arc".to_owned()),
                CompletionDelta::Done {
                    usage: Usage {
                        input_tokens: 7,
                        output_tokens: 3
                    },
                    stop: Stop::EndTurn,
                },
            ]
        );
    }

    #[tokio::test]
    async fn the_request_carries_the_wire_shape() {
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));
        let (_, requests) = complete_against(
            template,
            request(
                Some("be terse"),
                &[(Role::User, "one"), (Role::Assistant, "re: one")],
            ),
        )
        .await;

        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(&requests[0].body).expect("json body");
        assert_eq!(body["model"], "qwen3-8b");
        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
        assert_eq!(
            body["messages"],
            json!([
                {"role": "system", "content": "be terse"},
                {"role": "user", "content": "one"},
                {"role": "assistant", "content": "re: one"},
            ])
        );
    }

    #[tokio::test]
    async fn a_request_without_tools_has_no_tools_key() {
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));
        let (_, requests) = complete_against(template, request(None, &[(Role::User, "hi")])).await;

        let body: Value = serde_json::from_slice(&requests[0].body).expect("json body");
        assert_eq!(body.get("tools"), None, "{body}");
        assert_eq!(body.get("seed"), None, "unset seed sends no key: {body}");
    }

    #[tokio::test]
    async fn a_set_seed_is_serialized() {
        let mut req = request(None, &[(Role::User, "hi")]);
        req.seed = Some(42);
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));

        let (_, requests) = complete_against(template, req).await;

        let body: Value = serde_json::from_slice(&requests[0].body).expect("json body");
        assert_eq!(body["seed"], 42, "{body}");
    }

    #[tokio::test]
    async fn tools_are_offered_in_the_dialects_shape() {
        let mut req = request(None, &[(Role::User, "what do you know about arc?")]);
        req.tools = vec![ToolDefinition {
            name: "memory_search".to_owned(),
            description: "Search durable memory".to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            }),
        }];
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));

        let (_, requests) = complete_against(template, req).await;

        let body: Value = serde_json::from_slice(&requests[0].body).expect("json body");
        assert_eq!(
            body["tools"],
            json!([{
                "type": "function",
                "function": {
                    "name": "memory_search",
                    "description": "Search durable memory",
                    "parameters": {
                        "type": "object",
                        "properties": {"query": {"type": "string"}},
                        "required": ["query"],
                    },
                },
            }])
        );
    }

    #[tokio::test]
    async fn tool_calls_and_their_results_go_back_as_history() {
        let mut req = request(None, &[(Role::User, "what time is it?")]);
        req.messages.push(Message::ToolCalls(vec![ToolCall {
            id: "VB3c1GM6".to_owned(),
            index: 0,
            name: "get_time".to_owned(),
            arguments: "{}".to_owned(),
        }]));
        req.messages.push(Message::ToolResult {
            call_id: "VB3c1GM6".to_owned(),
            content: "2026-08-14T09:12:00Z".to_owned(),
        });
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));

        let (_, requests) = complete_against(template, req).await;

        let body: Value = serde_json::from_slice(&requests[0].body).expect("json body");
        assert_eq!(
            body["messages"],
            json!([
                {"role": "user", "content": "what time is it?"},
                {
                    "role": "assistant",
                    "tool_calls": [{
                        "id": "VB3c1GM6",
                        "type": "function",
                        "function": {"name": "get_time", "arguments": "{}"},
                    }],
                },
                {
                    "role": "tool",
                    "tool_call_id": "VB3c1GM6",
                    "content": "2026-08-14T09:12:00Z",
                },
            ])
        );
    }

    #[tokio::test]
    async fn a_blank_system_prompt_sends_no_system_message() {
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));
        let (_, requests) =
            complete_against(template, request(Some("  \n"), &[(Role::User, "hi")])).await;

        let body: Value = serde_json::from_slice(&requests[0].body).expect("json body");
        assert_eq!(body["messages"], json!([{"role": "user", "content": "hi"}]));
    }

    #[tokio::test]
    async fn a_system_role_in_the_history_is_refused_before_sending() {
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));
        let (outcome, requests) =
            complete_against(template, request(None, &[(Role::System, "sneaky")])).await;

        assert!(matches!(outcome, Err(Error::InvalidRequest(_))));
        assert!(requests.is_empty(), "nothing was sent");
    }

    #[tokio::test]
    async fn a_rejection_keeps_the_servers_own_words() {
        let template = ResponseTemplate::new(400).set_body_json(json!({
            "error": {"message": "model 'nope' not found", "type": "invalid_request_error"}
        }));
        let (outcome, _) = complete_against(template, request(None, &[(Role::User, "hi")])).await;

        match outcome {
            Err(Error::Http { status, body }) => {
                assert_eq!(status, 400);
                assert!(body.contains("not found"), "{body}");
            }
            other => panic!("expected Error::Http, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_429_is_rate_limited_with_the_servers_detail() {
        let template = ResponseTemplate::new(429).set_body_json(json!({
            "error": {"message": "server busy", "type": "unavailable_error"}
        }));
        let (outcome, _) = complete_against(template, request(None, &[(Role::User, "hi")])).await;

        match outcome {
            Err(Error::RateLimited { detail, .. }) => assert_eq!(detail, "server busy"),
            other => panic!("expected Error::RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn a_trailing_slash_in_the_endpoint_is_trimmed() {
        assert_eq!(
            OpenAiCompat::new("http://127.0.0.1:8080/").endpoint(),
            "http://127.0.0.1:8080"
        );
    }
}
