mod allowance;
pub mod auth;
mod stream;

use base64::Engine as _;
use futures::future::BoxFuture;
use reqwest::header::{ACCEPT, AUTHORIZATION, USER_AGENT};
use serde::Serialize;

use crate::provider::{
    CompletionRequest, CompletionStream, Error, Message, Provider, Thinking, ToolDefinition,
    failure, stream as delta_stream,
};
use crate::secrets::Secrets;
use arc_proto::v1::Role;

pub use auth::{DEFAULT_AUTH_ENDPOINT, Tokens};

const NAME: &str = "codex";

pub const DEFAULT_ENDPOINT: &str = "https://chatgpt.com/backend-api";

const RESPONSES_PATH: &str = "/codex/responses";

const ACCOUNT_HEADER: &str = "chatgpt-account-id";

// the backend refuses a request without instructions
const NO_SYSTEM: &str = "You are a helpful assistant.";

// Codex's own grammar for apply_patch (codex-rs/core/assets/tools/apply_patch.lark);
// the model was trained against it, so it goes out verbatim as a custom tool
const APPLY_PATCH_GRAMMAR: &str = r#"start: begin_patch hunk+ end_patch
begin_patch: "*** Begin Patch" LF
end_patch: "*** End Patch" LF?

hunk: add_hunk | delete_hunk | update_hunk
add_hunk: "*** Add File: " filename LF add_line+
delete_hunk: "*** Delete File: " filename LF
update_hunk: "*** Update File: " filename LF change_move? change?

filename: /(.+)/
add_line: "+" /(.*)/ LF -> line

change_move: "*** Move to: " filename LF
change: (change_context | change_line)+ eof_line?
change_context: ("@@" | "@@ " /(.+)/) LF
change_line: ("+" | "-" | " ") /(.*)/ LF
eof_line: "*** End of File" LF

%import common.LF
"#;

const APPLY_PATCH: &str = crate::tool::workspace::patch::NAME;

// a custom tool's text lands in the one string property the JSON tool declares
const CUSTOM_INPUT: &str = "input";

pub struct Codex {
    endpoint: String,
    tokens: Tokens,
    http: reqwest::Client,
    allowance_cache: tokio::sync::Mutex<allowance::Cache>,
}

impl std::fmt::Debug for Codex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Codex")
            .field("endpoint", &self.endpoint)
            .field("credential", &self.tokens.name())
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl Codex {
    pub fn open(endpoint: &str, secrets: Secrets, credential: &str) -> Result<Self, Error> {
        Self::with_auth(endpoint, secrets, credential, DEFAULT_AUTH_ENDPOINT)
    }

    pub fn with_auth(
        endpoint: &str,
        secrets: Secrets,
        credential: &str,
        auth_endpoint: &str,
    ) -> Result<Self, Error> {
        let mut endpoint = endpoint.to_owned();
        endpoint.truncate(endpoint.trim_end_matches('/').len());
        Ok(Self {
            endpoint,
            tokens: Tokens::open(secrets, credential, auth_endpoint)?,
            allowance_cache: tokio::sync::Mutex::new(allowance::Cache::default()),
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(0)
                .build()
                .expect("default reqwest client"),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    async fn send(&self, payload: &Payload<'_>) -> Result<reqwest::Response, Error> {
        let (token, account) = self.tokens.bearer().await?;
        Ok(self
            .http
            .post(format!("{}{RESPONSES_PATH}", self.endpoint))
            .header(ACCEPT, "text/event-stream")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(ACCOUNT_HEADER, account)
            .header("originator", "arc")
            .header("OpenAI-Beta", "responses=experimental")
            .header(USER_AGENT, format!("arc/{}", crate::VERSION))
            .json(payload)
            .send()
            .await?)
    }
}

impl Provider for Codex {
    fn name(&self) -> &'static str {
        NAME
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn allowance(&self) -> BoxFuture<'_, Result<Option<crate::provider::AccountAllowance>, Error>> {
        Box::pin(async move { Codex::allowance(self).await.map(Some) })
    }

    fn supports_images(&self) -> bool {
        true
    }

    #[tracing::instrument(
        level = "info",
        name = "codex.complete",
        skip_all,
        fields(
            provider = NAME,
            model = %request.model,
            role = crate::provider::role_label(request.role),
            messages = request.messages.len(),
        )
    )]
    fn complete(
        &self,
        request: CompletionRequest,
    ) -> BoxFuture<'_, Result<CompletionStream, Error>> {
        Box::pin(async move {
            let payload = Payload::new(&request)?;

            let mut response = self.send(&payload).await?;
            if response.status() == reqwest::StatusCode::UNAUTHORIZED {
                tracing::info!("codex rejected the token; refreshing once");
                self.tokens.refresh().await?;
                response = self.send(&payload).await?;
            }
            if !response.status().is_success() {
                return Err(failure(response).await);
            }
            Ok(delta_stream::deltas(
                response,
                stream::Parser::default(),
                tracing::Span::current(),
            ))
        })
    }
}

#[derive(Serialize)]
struct Payload<'a> {
    model: &'a str,
    instructions: &'a str,
    input: Vec<Item<'a>>,
    store: bool,
    stream: bool,
    include: [&'static str; 1],
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<WireTool<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parallel_tool_calls: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<Reasoning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt_cache_key: Option<&'a str>,
}

#[derive(Serialize)]
struct Reasoning {
    effort: &'static str,
    summary: &'static str,
}

#[derive(Serialize)]
#[serde(untagged)]
enum Item<'a> {
    User {
        role: &'static str,
        content: Vec<InputContent<'a>>,
    },

    Assistant {
        #[serde(rename = "type")]
        kind: &'static str,
        role: &'static str,
        content: [OutputText<'a>; 1],
        status: &'static str,
    },

    Replayed(serde_json::Value),

    FunctionCall {
        #[serde(rename = "type")]
        kind: &'static str,
        call_id: &'a str,
        name: &'a str,
        arguments: &'a str,
    },

    FunctionOutput {
        #[serde(rename = "type")]
        kind: &'static str,
        call_id: &'a str,
        output: &'a str,
    },

    CustomCall {
        #[serde(rename = "type")]
        kind: &'static str,
        call_id: &'a str,
        name: &'a str,
        input: String,
    },

    CustomOutput {
        #[serde(rename = "type")]
        kind: &'static str,
        call_id: &'a str,
        output: &'a str,
    },
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum InputContent<'a> {
    #[serde(rename = "input_text")]
    Text { text: &'a str },

    #[serde(rename = "input_image")]
    Image { image_url: String },
}

#[derive(Serialize)]
struct OutputText<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    text: &'a str,
    annotations: [(); 0],
}

#[derive(Serialize)]
#[serde(untagged)]
enum WireTool<'a> {
    Function {
        #[serde(rename = "type")]
        kind: &'static str,
        name: &'a str,
        description: &'a str,
        parameters: &'a serde_json::Value,
        strict: bool,
    },

    Hosted {
        #[serde(rename = "type")]
        kind: &'static str,
    },

    Custom {
        #[serde(rename = "type")]
        kind: &'static str,
        name: &'a str,
        description: &'a str,
        format: Grammar,
    },
}

#[derive(Serialize)]
struct Grammar {
    #[serde(rename = "type")]
    kind: &'static str,
    syntax: &'static str,
    definition: &'static str,
}

impl<'a> Payload<'a> {
    fn new(request: &'a CompletionRequest) -> Result<Self, Error> {
        let mut input = Vec::with_capacity(request.messages.len());
        let mut custom_calls = std::collections::HashSet::new();
        for message in &request.messages {
            items(message, &mut input, &mut custom_calls)?;
        }
        let mut tools: Vec<WireTool> = request
            .tools
            .iter()
            .map(|tool: &ToolDefinition| {
                if tool.name == APPLY_PATCH {
                    WireTool::Custom {
                        kind: "custom",
                        name: &tool.name,
                        description: &tool.description,
                        format: Grammar {
                            kind: "grammar",
                            syntax: "lark",
                            definition: APPLY_PATCH_GRAMMAR,
                        },
                    }
                } else {
                    WireTool::Function {
                        kind: "function",
                        name: &tool.name,
                        description: &tool.description,
                        parameters: &tool.parameters,
                        strict: false,
                    }
                }
            })
            .collect();
        if request.web {
            tools.push(WireTool::Hosted { kind: "web_search" });
        }
        let has_tools = !tools.is_empty();
        Ok(Self {
            model: &request.model,
            instructions: request
                .system
                .as_deref()
                .filter(|system| !system.trim().is_empty())
                .unwrap_or(NO_SYSTEM),
            input,
            store: false,
            stream: true,
            include: ["reasoning.encrypted_content"],
            tools,
            tool_choice: has_tools.then_some("auto"),
            parallel_tool_calls: has_tools.then_some(true),
            reasoning: effort(request.thinking).map(|effort| Reasoning {
                effort,
                summary: "auto",
            }),
            prompt_cache_key: request.cache_key.as_deref(),
        })
    }
}

// Default omits the field so the wire shape is byte-stable for cache hits;
// low is the floor these models offer
fn effort(thinking: Thinking) -> Option<&'static str> {
    match thinking {
        Thinking::Default => None,
        Thinking::Minimal | Thinking::Low => Some("low"),
        Thinking::Medium => Some("medium"),
        Thinking::High => Some("high"),
    }
}

fn items<'a>(
    message: &'a Message,
    out: &mut Vec<Item<'a>>,
    custom_calls: &mut std::collections::HashSet<&'a str>,
) -> Result<(), Error> {
    match message {
        Message::Text { role, content, .. } => match role {
            Role::User => out.push(Item::User {
                role: "user",
                content: vec![InputContent::Text { text: content }],
            }),
            Role::Assistant => out.push(Item::Assistant {
                kind: "message",
                role: "assistant",
                content: [OutputText {
                    kind: "output_text",
                    text: content,
                    annotations: [],
                }],
                status: "completed",
            }),
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
        },
        Message::UserWithAttachments {
            content,
            attachments,
        } => {
            let mut parts = Vec::with_capacity(attachments.len() + 1);
            if !content.is_empty() {
                parts.push(InputContent::Text { text: content });
            }
            parts.extend(attachments.iter().map(|attachment| InputContent::Image {
                image_url: format!(
                    "data:{};base64,{}",
                    attachment.media_type,
                    base64::engine::general_purpose::STANDARD.encode(&attachment.data)
                ),
            }));
            out.push(Item::User {
                role: "user",
                content: parts,
            });
        }
        Message::ToolCalls { calls, .. } => {
            if let Some(replayed) = calls
                .iter()
                .find(|call| !call.provider_roundtrip.is_empty())
                .map(|call| serde_json::from_slice::<serde_json::Value>(&call.provider_roundtrip))
            {
                let replayed = replayed.map_err(|_| {
                    Error::InvalidRequest(
                        "a tool call's roundtrip bytes are not the reasoning item codex issued"
                            .to_owned(),
                    )
                })?;
                out.push(Item::Replayed(replayed));
            }
            for call in calls {
                if call.name == APPLY_PATCH {
                    custom_calls.insert(call.id.as_str());
                    let input = serde_json::from_str::<serde_json::Value>(&call.arguments)
                        .ok()
                        .and_then(|args| args[CUSTOM_INPUT].as_str().map(str::to_owned))
                        .unwrap_or_default();
                    out.push(Item::CustomCall {
                        kind: "custom_tool_call",
                        call_id: &call.id,
                        name: &call.name,
                        input,
                    });
                } else {
                    out.push(Item::FunctionCall {
                        kind: "function_call",
                        call_id: &call.id,
                        name: &call.name,
                        arguments: &call.arguments,
                    });
                }
            }
        }
        Message::ToolResult { call_id, content } => {
            if custom_calls.contains(call_id.as_str()) {
                out.push(Item::CustomOutput {
                    kind: "custom_tool_call_output",
                    call_id,
                    output: content,
                });
            } else {
                out.push(Item::FunctionOutput {
                    kind: "function_call_output",
                    call_id,
                    output: content,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use futures::StreamExt as _;
    use serde_json::{Value, json};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    use super::auth::{Credential, fake_access_token};
    use super::*;
    use crate::provider::{CompletionDelta, Stop, ToolCall, Usage};
    use arc_proto::v1::SessionRole;

    fn request(system: Option<&str>, turns: &[(Role, &str)]) -> CompletionRequest {
        CompletionRequest {
            model: "gpt-5.5".to_owned(),
            role: SessionRole::Executor,
            system: system.map(str::to_owned),
            messages: turns
                .iter()
                .map(|(role, content)| Message::Text {
                    role: *role,
                    content: (*content).to_owned(),
                    reasoning: None,
                })
                .collect(),
            tools: Vec::new(),
            seed: None,
            thinking: Thinking::Default,
            web: false,
            cache_key: None,
        }
    }

    fn provider(dir: &std::path::Path, endpoint: &str, expires_at: u64) -> Codex {
        let secrets = Secrets::new(dir);
        let credential = Credential {
            access_token: fake_access_token("acct_42"),
            refresh_token: "rt-42".to_owned(),
            expires_at,
        };
        secrets
            .write("codex", &credential.to_json())
            .expect("writes");
        Codex::with_auth(endpoint, secrets, "codex", endpoint).expect("opens")
    }

    fn sse_body(text: &str) -> String {
        let events = [
            json!({"type": "response.output_item.added", "output_index": 0,
                   "item": {"type": "message", "id": "msg_1", "role": "assistant", "content": []}}),
            json!({"type": "response.output_text.delta", "output_index": 0, "delta": text}),
            json!({"type": "response.completed", "response": {
                "id": "resp_1", "status": "completed",
                "usage": {"input_tokens": 7, "output_tokens": 3}}}),
        ];
        let mut out = String::new();
        for event in events {
            out.push_str("data: ");
            out.push_str(&event.to_string());
            out.push_str("\n\n");
        }
        out
    }

    async fn complete_against(
        template: ResponseTemplate,
        req: CompletionRequest,
    ) -> (
        Result<Vec<Result<CompletionDelta, Error>>, Error>,
        Vec<Request>,
    ) {
        let dir = tempfile::tempdir().expect("temp dir");
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(RESPONSES_PATH))
            .respond_with(template)
            .mount(&server)
            .await;

        let provider = provider(dir.path(), &server.uri(), u64::MAX);
        let outcome = match provider.complete(req).await {
            Ok(stream) => Ok(stream.collect().await),
            Err(error) => Err(error),
        };
        (
            outcome,
            server.received_requests().await.unwrap_or_default(),
        )
    }

    fn body(requests: &[Request]) -> Value {
        serde_json::from_slice(&requests[0].body).expect("json body")
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
    async fn the_request_carries_the_codex_headers_and_wire_shape() {
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
        let headers = &requests[0].headers;
        assert_eq!(
            headers.get("authorization").unwrap().to_str().unwrap(),
            format!("Bearer {}", fake_access_token("acct_42"))
        );
        assert_eq!(headers.get("chatgpt-account-id").unwrap(), "acct_42");
        assert_eq!(headers.get("originator").unwrap(), "arc");
        assert_eq!(
            headers.get("openai-beta").unwrap(),
            "responses=experimental"
        );
        assert_eq!(headers.get("accept").unwrap(), "text/event-stream");

        let body = body(&requests);
        assert_eq!(body["model"], "gpt-5.5");
        assert_eq!(body["instructions"], "be terse");
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
        assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
        assert_eq!(
            body["input"],
            json!([
                {"role": "user", "content": [{"type": "input_text", "text": "one"}]},
                {"type": "message", "role": "assistant", "status": "completed",
                 "content": [{"type": "output_text", "text": "re: one", "annotations": []}]},
            ])
        );
        assert_eq!(body.get("tools"), None, "{body}");
        assert_eq!(body.get("tool_choice"), None, "{body}");
        assert_eq!(
            body.get("reasoning"),
            None,
            "default thinking sends no reasoning: {body}"
        );
    }

    #[tokio::test]
    async fn picture_bytes_go_out_as_responses_input_image_data_urls_without_detail() {
        let mut req = request(None, &[]);
        req.messages.push(Message::UserWithAttachments {
            content: "what is this?".to_owned(),
            attachments: vec![arc_proto::v1::ImageAttachment {
                name: "screen.png".to_owned(),
                media_type: "image/png".to_owned(),
                data: b"\x89PNG\r\n\x1a\nbody".to_vec(),
            }],
        });
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));

        let (_, requests) = complete_against(template, req).await;

        assert_eq!(
            body(&requests)["input"],
            json!([{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": "what is this?"},
                    {
                        "type": "input_image",
                        "image_url": "data:image/png;base64,iVBORw0KGgpib2R5"
                    }
                ]
            }])
        );
    }

    #[tokio::test]
    async fn the_cache_key_goes_out_as_prompt_cache_key_and_none_sends_nothing() {
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));
        let (_, requests) = complete_against(template, request(None, &[(Role::User, "hi")])).await;
        assert_eq!(body(&requests).get("prompt_cache_key"), None);

        let mut req = request(None, &[(Role::User, "hi")]);
        req.cache_key = Some("session-7".to_owned());
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));
        let (_, requests) = complete_against(template, req).await;
        assert_eq!(body(&requests)["prompt_cache_key"], "session-7");
    }

    #[tokio::test]
    async fn a_blank_system_prompt_falls_back_to_the_stock_instructions() {
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));
        let (_, requests) =
            complete_against(template, request(Some(" \n"), &[(Role::User, "hi")])).await;

        assert_eq!(body(&requests)["instructions"], NO_SYSTEM);
    }

    #[tokio::test]
    async fn thinking_levels_map_to_reasoning_effort_with_low_as_the_floor() {
        for (thinking, expected) in [
            (Thinking::Minimal, "low"),
            (Thinking::Low, "low"),
            (Thinking::Medium, "medium"),
            (Thinking::High, "high"),
        ] {
            let mut req = request(None, &[(Role::User, "hi")]);
            req.thinking = thinking;
            let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));

            let (_, requests) = complete_against(template, req).await;

            let body = body(&requests);
            assert_eq!(body["reasoning"]["effort"], expected, "{body}");
            assert_eq!(body["reasoning"]["summary"], "auto", "{body}");
        }
    }

    #[tokio::test]
    async fn tools_are_offered_as_responses_functions() {
        let mut req = request(None, &[(Role::User, "what do you know about arc?")]);
        req.tools = vec![ToolDefinition {
            name: "memory_search".to_owned(),
            description: "Search durable memory".to_owned(),
            parameters: json!({"type": "object", "properties": {"query": {"type": "string"}}}),
        }];
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));

        let (_, requests) = complete_against(template, req).await;

        let body = body(&requests);
        assert_eq!(
            body["tools"],
            json!([{
                "type": "function",
                "name": "memory_search",
                "description": "Search durable memory",
                "parameters": {"type": "object", "properties": {"query": {"type": "string"}}},
                "strict": false,
            }])
        );
        assert_eq!(body["tool_choice"], "auto");
        assert_eq!(body["parallel_tool_calls"], true);
    }

    #[tokio::test]
    async fn a_web_request_offers_the_hosted_search_tool() {
        let mut req = request(None, &[(Role::User, "what happened today?")]);
        req.web = true;
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));

        let (_, requests) = complete_against(template, req).await;

        let body = body(&requests);
        assert_eq!(body["tools"], json!([{"type": "web_search"}]));
        assert_eq!(body["tool_choice"], "auto");
    }

    #[tokio::test]
    async fn tool_calls_replay_behind_their_reasoning_item_and_results_follow() {
        let reasoning =
            json!({"type": "reasoning", "id": "rs_9", "summary": [], "encrypted_content": "ENC"});
        let mut req = request(None, &[(Role::User, "what time is it?")]);
        req.messages.push(Message::ToolCalls {
            calls: vec![
                ToolCall {
                    id: "call_a".to_owned(),
                    index: 0,
                    name: "get_time".to_owned(),
                    arguments: "{}".to_owned(),
                    provider_roundtrip: reasoning.to_string().into_bytes(),
                },
                ToolCall {
                    id: "call_b".to_owned(),
                    index: 1,
                    name: "read".to_owned(),
                    arguments: r#"{"path":"a"}"#.to_owned(),
                    provider_roundtrip: reasoning.to_string().into_bytes(),
                },
            ],
            reasoning: Some("checking".to_owned()),
        });
        req.messages.push(Message::ToolResult {
            call_id: "call_a".to_owned(),
            content: "09:12".to_owned(),
        });
        req.messages.push(Message::ToolResult {
            call_id: "call_b".to_owned(),
            content: "fn main() {}".to_owned(),
        });
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));

        let (_, requests) = complete_against(template, req).await;

        assert_eq!(
            body(&requests)["input"],
            json!([
                {"role": "user", "content": [{"type": "input_text", "text": "what time is it?"}]},
                reasoning,
                {"type": "function_call", "call_id": "call_a", "name": "get_time", "arguments": "{}"},
                {"type": "function_call", "call_id": "call_b", "name": "read", "arguments": "{\"path\":\"a\"}"},
                {"type": "function_call_output", "call_id": "call_a", "output": "09:12"},
                {"type": "function_call_output", "call_id": "call_b", "output": "fn main() {}"},
            ])
        );
    }

    #[tokio::test]
    async fn apply_patch_goes_out_as_a_custom_grammar_tool_and_replays_as_custom_items() {
        let mut req = request(None, &[(Role::User, "fix it")]);
        req.tools = vec![
            ToolDefinition {
                name: "apply_patch".to_owned(),
                description: "Edit files with a patch.".to_owned(),
                parameters: json!({"type": "object", "properties": {"input": {"type": "string"}}}),
            },
            ToolDefinition {
                name: "read".to_owned(),
                description: "Read a file.".to_owned(),
                parameters: json!({"type": "object", "properties": {"path": {"type": "string"}}}),
            },
        ];
        let patch = "*** Begin Patch\n*** Update File: a.rs\n-x\n+y\n*** End Patch";
        req.messages.push(Message::ToolCalls {
            calls: vec![
                ToolCall {
                    id: "call_p".to_owned(),
                    index: 0,
                    name: "apply_patch".to_owned(),
                    arguments: json!({"input": patch}).to_string(),
                    provider_roundtrip: Vec::new(),
                },
                ToolCall {
                    id: "call_r".to_owned(),
                    index: 1,
                    name: "read".to_owned(),
                    arguments: r#"{"path":"a.rs"}"#.to_owned(),
                    provider_roundtrip: Vec::new(),
                },
            ],
            reasoning: None,
        });
        req.messages.push(Message::ToolResult {
            call_id: "call_p".to_owned(),
            content: "Applied the patch:\nM a.rs".to_owned(),
        });
        req.messages.push(Message::ToolResult {
            call_id: "call_r".to_owned(),
            content: "y".to_owned(),
        });
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));

        let (_, requests) = complete_against(template, req).await;

        let body = body(&requests);
        assert_eq!(body["tools"][0]["type"], "custom");
        assert_eq!(body["tools"][0]["name"], "apply_patch");
        assert_eq!(body["tools"][0]["format"]["type"], "grammar");
        assert_eq!(body["tools"][0]["format"]["syntax"], "lark");
        assert!(
            body["tools"][0]["format"]["definition"]
                .as_str()
                .unwrap()
                .starts_with("start: begin_patch hunk+ end_patch"),
            "{body}"
        );
        assert_eq!(
            body["tools"][0].get("parameters"),
            None,
            "a custom tool has no schema"
        );
        assert_eq!(body["tools"][1]["type"], "function");
        assert_eq!(
            body["input"],
            json!([
                {"role": "user", "content": [{"type": "input_text", "text": "fix it"}]},
                {"type": "custom_tool_call", "call_id": "call_p", "name": "apply_patch", "input": patch},
                {"type": "function_call", "call_id": "call_r", "name": "read", "arguments": "{\"path\":\"a.rs\"}"},
                {"type": "custom_tool_call_output", "call_id": "call_p", "output": "Applied the patch:\nM a.rs"},
                {"type": "function_call_output", "call_id": "call_r", "output": "y"},
            ])
        );
    }

    #[tokio::test]
    async fn tool_calls_from_another_provider_replay_without_a_reasoning_item() {
        let mut req = request(None, &[(Role::User, "hi")]);
        req.messages.push(Message::ToolCalls {
            calls: vec![ToolCall {
                id: "VB3c1GM6".to_owned(),
                index: 0,
                name: "get_time".to_owned(),
                arguments: "{}".to_owned(),
                provider_roundtrip: Vec::new(),
            }],
            reasoning: None,
        });
        req.messages.push(Message::ToolResult {
            call_id: "VB3c1GM6".to_owned(),
            content: "now".to_owned(),
        });
        let template = ResponseTemplate::new(200).set_body_string(sse_body("ok"));

        let (_, requests) = complete_against(template, req).await;

        let input = body(&requests)["input"].clone();
        assert_eq!(input.as_array().unwrap().len(), 3, "{input}");
        assert_eq!(input[1]["type"], "function_call");
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
    async fn a_401_refreshes_the_token_once_and_retries() {
        let dir = tempfile::tempdir().expect("temp dir");
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(RESPONSES_PATH))
            .and(header(
                "authorization",
                format!("Bearer {}", fake_access_token("acct_42")).as_str(),
            ))
            .respond_with(ResponseTemplate::new(401).set_body_string("expired"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": fake_access_token("acct_43"),
                "refresh_token": "rt-43",
                "expires_in": 3600,
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(RESPONSES_PATH))
            .and(header(
                "authorization",
                format!("Bearer {}", fake_access_token("acct_43")).as_str(),
            ))
            .and(header("chatgpt-account-id", "acct_43"))
            .respond_with(ResponseTemplate::new(200).set_body_string(sse_body("back")))
            .mount(&server)
            .await;
        let provider = provider(dir.path(), &server.uri(), u64::MAX);

        let deltas: Vec<CompletionDelta> = provider
            .complete(request(None, &[(Role::User, "hi")]))
            .await
            .expect("accepted after refresh")
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("decodes");

        assert_eq!(deltas[0], CompletionDelta::Text("back".to_owned()));
        let saved = Credential::parse(&Secrets::new(dir.path()).read("codex").unwrap()).unwrap();
        assert_eq!(saved.refresh_token, "rt-43");
    }

    #[tokio::test]
    async fn a_429_is_rate_limited_with_the_servers_detail() {
        let template = ResponseTemplate::new(429).set_body_json(json!({
            "error": {"message": "usage limit", "type": "usage_limit_reached"}
        }));
        let (outcome, _) = complete_against(template, request(None, &[(Role::User, "hi")])).await;

        match outcome {
            Err(Error::RateLimited { detail, .. }) => assert_eq!(detail, "usage limit"),
            other => panic!("expected Error::RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn debug_output_names_the_credential_and_redacts_the_token() {
        let dir = tempfile::tempdir().expect("temp dir");
        let provider = provider(dir.path(), "http://127.0.0.1:1/", u64::MAX);

        let rendered = format!("{provider:?}");
        assert!(rendered.contains("codex"), "{rendered}");
        assert!(!rendered.contains("eyJ"), "{rendered}");
        assert_eq!(provider.endpoint(), "http://127.0.0.1:1");
    }
}
