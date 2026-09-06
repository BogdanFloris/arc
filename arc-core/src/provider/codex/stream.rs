use std::collections::BTreeMap;

use serde::Deserialize;

use crate::provider::stream::{Deltas, FrameParser};
use crate::provider::{CompletionDelta, Error, Stop, ToolCall, Usage};

#[derive(Default)]
pub(super) struct Parser {
    building: BTreeMap<u32, Building>,

    // finished calls wait for the terminal event so each can carry the
    // reasoning item the backend wants replayed ahead of them
    finished: Vec<ToolCall>,

    reasoning: Option<Vec<u8>>,

    citations: Vec<serde_json::Value>,
}

const WEB_SEARCH: &str = "web_search";

const CUSTOM_INPUT: &str = super::CUSTOM_INPUT;

impl FrameParser for Parser {
    const PROVIDER: &'static str = "codex";

    fn frame(&mut self, payload: &str) -> Result<Deltas, Error> {
        let event: Event = serde_json::from_str(payload).map_err(|source| {
            Error::MalformedStream(format!(
                "the endpoint sent a frame that is not a response event: {source}: {}",
                crate::provider::snippet(payload)
            ))
        })?;
        let mut items = Vec::new();
        match event.kind.as_str() {
            "response.output_item.added" => {
                let item = event
                    .item
                    .ok_or_else(|| malformed("output_item.added", payload))?;
                match item.kind() {
                    "function_call" | "custom_tool_call" => {
                        let index = event.output_index.unwrap_or_default();
                        let custom = item.kind() == "custom_tool_call";
                        self.building.insert(
                            index,
                            Building {
                                call_id: item.field("call_id"),
                                name: item.field("name"),
                                arguments: item.field(if custom { "input" } else { "arguments" }),
                            },
                        );
                    }
                    "web_search_call" => items.push(CompletionDelta::ServerCall {
                        name: WEB_SEARCH.to_owned(),
                        payload_json: item.0.to_string(),
                    }),
                    _ => {}
                }
            }
            "response.function_call_arguments.delta" | "response.custom_tool_call_input.delta" => {
                let index = event.output_index.unwrap_or_default();
                let Some(building) = self.building.get_mut(&index) else {
                    return Err(Error::MalformedStream(format!(
                        "the endpoint continued a tool call at output index {index} that never opened"
                    )));
                };
                building
                    .arguments
                    .push_str(event.delta.as_deref().unwrap_or_default());
            }
            "response.function_call_arguments.done" | "response.custom_tool_call_input.done" => {
                let index = event.output_index.unwrap_or_default();
                if let (Some(building), Some(arguments)) = (
                    self.building.get_mut(&index),
                    event.arguments.or(event.input),
                ) {
                    building.arguments = arguments;
                }
            }
            "response.output_item.done" => {
                let item = event
                    .item
                    .ok_or_else(|| malformed("output_item.done", payload))?;
                match item.kind() {
                    "custom_tool_call" => {
                        let index = event.output_index.unwrap_or_default();
                        let building = self.building.remove(&index).unwrap_or_default();
                        let input = item.get("input").map_or(building.arguments, Item::text);
                        let position = u32::try_from(self.finished.len()).unwrap_or(u32::MAX);
                        self.finished.push(ToolCall {
                            id: item.get("call_id").map_or(building.call_id, Item::text),
                            index: position,
                            name: item.get("name").map_or(building.name, Item::text),
                            arguments: serde_json::json!({ CUSTOM_INPUT: input }).to_string(),
                            provider_roundtrip: Vec::new(),
                        });
                    }
                    "function_call" => {
                        let index = event.output_index.unwrap_or_default();
                        let building = self.building.remove(&index).unwrap_or_default();
                        let arguments =
                            item.get("arguments").map_or(building.arguments, Item::text);
                        let name = item.get("name").map_or(building.name, Item::text);
                        if !matches!(
                            serde_json::from_str::<serde_json::Value>(&arguments),
                            Ok(serde_json::Value::Object(_))
                        ) {
                            return Err(Error::MalformedStream(format!(
                                "the endpoint's arguments for tool call `{name}` are not a JSON object: {}",
                                crate::provider::snippet(&arguments)
                            )));
                        }
                        let position = u32::try_from(self.finished.len()).unwrap_or(u32::MAX);
                        self.finished.push(ToolCall {
                            id: item.get("call_id").map_or(building.call_id, Item::text),
                            index: position,
                            name,
                            arguments,
                            provider_roundtrip: Vec::new(),
                        });
                    }
                    "reasoning" if item.0.get("encrypted_content").is_some() => {
                        self.reasoning = Some(item.0.to_string().into_bytes());
                    }
                    "web_search_call" => items.push(CompletionDelta::ServerResponse {
                        name: WEB_SEARCH.to_owned(),
                        payload_json: item.0.to_string(),
                    }),
                    "message" => {
                        let parts = item.0["content"].as_array().cloned().unwrap_or_default();
                        for annotation in parts
                            .iter()
                            .filter_map(|part| part["annotations"].as_array())
                            .flatten()
                            .filter(|it| it["type"] == "url_citation")
                        {
                            if !self
                                .citations
                                .iter()
                                .any(|seen| seen["url"] == annotation["url"])
                            {
                                self.citations.push(annotation.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            "response.output_text.delta" => {
                if let Some(delta) = event.delta.filter(|it| !it.is_empty()) {
                    items.push(CompletionDelta::Text(delta));
                }
            }
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                if let Some(delta) = event.delta.filter(|it| !it.is_empty()) {
                    items.push(CompletionDelta::Reasoning(delta));
                }
            }
            "response.reasoning_summary_part.done" => {
                items.push(CompletionDelta::Reasoning("\n\n".to_owned()));
            }
            "response.completed" | "response.done" | "response.incomplete" => {
                let response = event.response.unwrap_or_default();
                let usage = response.usage.map(|usage| {
                    if usage.input_tokens_details.cached_tokens > 0 {
                        tracing::info!(
                            counter.cached_tokens = usage.input_tokens_details.cached_tokens,
                            counter.prompt_tokens = usage.input_tokens,
                            "prompt cache hit"
                        );
                    }
                    Usage {
                        input_tokens: usage.input_tokens,
                        output_tokens: usage.output_tokens,
                    }
                });
                let reasoning = self.reasoning.take().unwrap_or_default();
                let calls: Vec<CompletionDelta> = std::mem::take(&mut self.finished)
                    .into_iter()
                    .map(|mut call| {
                        call.provider_roundtrip.clone_from(&reasoning);
                        CompletionDelta::ToolCall(call)
                    })
                    .collect();
                let stop = if calls.is_empty() {
                    Stop::EndTurn
                } else {
                    Stop::ToolCalls
                };
                if !self.citations.is_empty() {
                    let citations = std::mem::take(&mut self.citations);
                    items.push(CompletionDelta::Grounding(
                        serde_json::json!({ "annotations": citations }).to_string(),
                    ));
                }
                items.extend(calls);
                return Ok(Deltas {
                    items,
                    usage,
                    finished: Some(stop),
                });
            }
            "response.failed" => {
                let error = event.response.unwrap_or_default().error.unwrap_or_default();
                return Err(refusal(error));
            }
            "error" => {
                return Err(refusal(ErrorJson {
                    code: event.code,
                    message: event.message,
                    resets_at: None,
                }));
            }
            _ => {}
        }
        Ok(Deltas {
            items,
            usage: None,
            finished: None,
        })
    }
}

fn malformed(what: &str, payload: &str) -> Error {
    Error::MalformedStream(format!(
        "the endpoint's {what} event has no item: {}",
        crate::provider::snippet(payload)
    ))
}

fn refusal(error: ErrorJson) -> Error {
    let code = error.code.unwrap_or_default();
    let message = error.message.unwrap_or_default();
    let detail = if message.is_empty() {
        code.clone()
    } else if code.is_empty() {
        message
    } else {
        format!("{code}: {message}")
    };
    if matches!(
        code.as_str(),
        "usage_limit_reached" | "usage_not_included" | "rate_limit_exceeded"
    ) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        return Error::RateLimited {
            retry_after: error.resets_at.map(|at| at.saturating_sub(now)),
            detail,
        };
    }
    Error::Refused(detail)
}

#[derive(Default)]
struct Building {
    call_id: String,
    name: String,
    arguments: String,
}

#[derive(Deserialize)]
struct Event {
    #[serde(rename = "type")]
    kind: String,
    output_index: Option<u32>,
    delta: Option<String>,
    arguments: Option<String>,
    input: Option<String>,
    item: Option<Item>,
    response: Option<ResponseJson>,
    code: Option<String>,
    message: Option<String>,
}

// kept whole: a reasoning item is replayed byte-for-byte as the backend sent it
#[derive(Deserialize)]
struct Item(serde_json::Value);

impl Item {
    fn kind(&self) -> &str {
        self.0["type"].as_str().unwrap_or_default()
    }

    fn get(&self, field: &str) -> Option<Item> {
        self.0.get(field).map(|value| Item(value.clone()))
    }

    fn field(&self, field: &str) -> String {
        self.0[field].as_str().unwrap_or_default().to_owned()
    }

    fn text(self) -> String {
        match self.0 {
            serde_json::Value::String(text) => text,
            _ => String::new(),
        }
    }
}

#[derive(Default, Deserialize)]
struct ResponseJson {
    usage: Option<UsageJson>,
    error: Option<ErrorJson>,
}

#[derive(Default, Deserialize)]
struct ErrorJson {
    code: Option<String>,
    message: Option<String>,
    resets_at: Option<u64>,
}

#[derive(Deserialize)]
struct UsageJson {
    #[serde(default)]
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
    #[serde(default)]
    input_tokens_details: InputTokensDetails,
}

#[derive(Default, Deserialize)]
struct InputTokensDetails {
    #[serde(default)]
    cached_tokens: u32,
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, stream};
    use serde_json::{Value, json};
    use tracing::Span;

    use super::Parser;
    use crate::provider::stream::DeltaStream;
    use crate::provider::{CompletionDelta, Error, Stop, Usage};

    fn sse(events: &[Value]) -> Vec<u8> {
        let mut out = String::new();
        for event in events {
            out.push_str("event: ");
            out.push_str(event["type"].as_str().unwrap());
            out.push_str("\ndata: ");
            out.push_str(&event.to_string());
            out.push_str("\n\n");
        }
        out.into_bytes()
    }

    async fn deltas(chunks: Vec<Vec<u8>>) -> Vec<Result<CompletionDelta, Error>> {
        let bytes = stream::iter(chunks.into_iter().map(Ok::<Vec<u8>, reqwest::Error>));
        DeltaStream::new(bytes, Parser::default(), Span::none())
            .collect()
            .await
    }

    async fn ok(chunks: Vec<Vec<u8>>) -> Vec<CompletionDelta> {
        deltas(chunks)
            .await
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("stream should not fail")
    }

    fn completed(input: u32, output: u32, cached: u32) -> Value {
        json!({
            "type": "response.completed",
            "response": {
                "id": "resp_1",
                "status": "completed",
                "usage": {
                    "input_tokens": input,
                    "output_tokens": output,
                    "input_tokens_details": {"cached_tokens": cached},
                },
            },
        })
    }

    fn text_turn() -> Vec<Value> {
        vec![
            json!({"type": "response.created", "response": {"id": "resp_1"}}),
            json!({"type": "response.output_item.added", "output_index": 0,
                   "item": {"type": "reasoning", "id": "rs_1", "summary": []}}),
            json!({"type": "response.reasoning_summary_text.delta", "output_index": 0, "delta": "thinking"}),
            json!({"type": "response.output_item.done", "output_index": 0,
                   "item": {"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "enc"}}),
            json!({"type": "response.output_item.added", "output_index": 1,
                   "item": {"type": "message", "id": "msg_1", "role": "assistant", "content": []}}),
            json!({"type": "response.output_text.delta", "output_index": 1, "delta": "Hello"}),
            json!({"type": "response.output_text.delta", "output_index": 1, "delta": " arc"}),
            json!({"type": "response.output_text.done", "output_index": 1, "text": "Hello arc"}),
            json!({"type": "response.output_item.done", "output_index": 1,
                   "item": {"type": "message", "id": "msg_1", "role": "assistant",
                            "content": [{"type": "output_text", "text": "Hello arc"}]}}),
            completed(17, 7, 12),
        ]
    }

    #[tokio::test]
    async fn a_text_turn_decodes_to_reasoning_text_and_usage() {
        let seen = ok(vec![sse(&text_turn())]).await;

        assert_eq!(
            seen,
            [
                CompletionDelta::Reasoning("thinking".to_owned()),
                CompletionDelta::Text("Hello".to_owned()),
                CompletionDelta::Text(" arc".to_owned()),
                CompletionDelta::Done {
                    usage: Usage {
                        input_tokens: 17,
                        output_tokens: 7,
                    },
                    stop: Stop::EndTurn,
                },
            ]
        );
    }

    #[tokio::test]
    async fn no_split_point_changes_what_the_stream_yields() {
        let bytes = sse(&text_turn());
        let whole = ok(vec![bytes.clone()]).await;

        for split in (0..bytes.len()).step_by(7) {
            let seen = ok(vec![bytes[..split].to_vec(), bytes[split..].to_vec()]).await;
            assert_eq!(seen, whole, "split at {split}");
        }
    }

    fn tool_turn() -> Vec<Value> {
        vec![
            json!({"type": "response.output_item.added", "output_index": 0,
                   "item": {"type": "reasoning", "id": "rs_9", "summary": []}}),
            json!({"type": "response.output_item.done", "output_index": 0,
                   "item": {"type": "reasoning", "id": "rs_9", "summary": [], "encrypted_content": "ENC9"}}),
            json!({"type": "response.output_item.added", "output_index": 1,
                   "item": {"type": "function_call", "id": "fc_1", "call_id": "call_a",
                            "name": "get_time", "arguments": ""}}),
            json!({"type": "response.function_call_arguments.delta", "output_index": 1, "delta": "{\"zone\":"}),
            json!({"type": "response.function_call_arguments.delta", "output_index": 1, "delta": " \"UTC\"}"}),
            json!({"type": "response.function_call_arguments.done", "output_index": 1, "arguments": "{\"zone\": \"UTC\"}"}),
            json!({"type": "response.output_item.done", "output_index": 1,
                   "item": {"type": "function_call", "id": "fc_1", "call_id": "call_a",
                            "name": "get_time", "arguments": "{\"zone\": \"UTC\"}"}}),
            json!({"type": "response.output_item.added", "output_index": 2,
                   "item": {"type": "function_call", "id": "fc_2", "call_id": "call_b",
                            "name": "read", "arguments": ""}}),
            json!({"type": "response.output_item.done", "output_index": 2,
                   "item": {"type": "function_call", "id": "fc_2", "call_id": "call_b",
                            "name": "read", "arguments": "{\"path\": \"a.rs\"}"}}),
            completed(40, 9, 0),
        ]
    }

    #[tokio::test]
    async fn tool_calls_arrive_whole_at_the_end_carrying_the_reasoning_item() {
        let seen = ok(vec![sse(&tool_turn())]).await;

        let [
            CompletionDelta::ToolCall(first),
            CompletionDelta::ToolCall(second),
            done,
        ] = seen.as_slice()
        else {
            panic!("expected two calls and a done, got {seen:?}");
        };
        assert_eq!(
            (first.id.as_str(), first.index, first.name.as_str()),
            ("call_a", 0, "get_time")
        );
        assert_eq!(first.arguments, r#"{"zone": "UTC"}"#);
        assert_eq!(
            (second.id.as_str(), second.index, second.name.as_str()),
            ("call_b", 1, "read")
        );
        let replay: Value = serde_json::from_slice(&first.provider_roundtrip).expect("json");
        assert_eq!(replay["type"], "reasoning");
        assert_eq!(replay["id"], "rs_9");
        assert_eq!(replay["encrypted_content"], "ENC9");
        assert_eq!(first.provider_roundtrip, second.provider_roundtrip);
        assert_eq!(
            *done,
            CompletionDelta::Done {
                usage: Usage {
                    input_tokens: 40,
                    output_tokens: 9,
                },
                stop: Stop::ToolCalls,
            }
        );
    }

    #[tokio::test]
    async fn a_web_search_arrives_as_a_server_call_its_response_and_grounding() {
        let events = vec![
            json!({"type": "response.output_item.added", "output_index": 0,
                   "item": {"type": "web_search_call", "id": "ws_1", "status": "in_progress"}}),
            json!({"type": "response.web_search_call.searching", "output_index": 0, "item_id": "ws_1"}),
            json!({"type": "response.output_item.done", "output_index": 0,
                   "item": {"type": "web_search_call", "id": "ws_1", "status": "completed",
                            "action": {"type": "search", "query": "arc daemon"}}}),
            json!({"type": "response.output_item.added", "output_index": 1,
                   "item": {"type": "message", "id": "msg_1", "role": "assistant", "content": []}}),
            json!({"type": "response.output_text.delta", "output_index": 1, "delta": "It is a daemon."}),
            json!({"type": "response.output_item.done", "output_index": 1,
            "item": {"type": "message", "id": "msg_1", "role": "assistant", "content": [{
                "type": "output_text", "text": "It is a daemon.",
                "annotations": [
                    {"type": "url_citation", "url": "https://a.example/x", "title": "A", "start_index": 0, "end_index": 5},
                    {"type": "url_citation", "url": "https://a.example/x", "title": "A again", "start_index": 6, "end_index": 9},
                    {"type": "url_citation", "url": "https://b.example/y", "title": "B", "start_index": 10, "end_index": 14},
                ]}]}}),
            completed(30, 8, 0),
        ];

        let seen = ok(vec![sse(&events)]).await;

        let [
            CompletionDelta::ServerCall {
                name: call_name,
                payload_json: call,
            },
            CompletionDelta::ServerResponse {
                name: response_name,
                payload_json: response,
            },
            CompletionDelta::Text(text),
            CompletionDelta::Grounding(grounding),
            CompletionDelta::Done {
                stop: Stop::EndTurn,
                ..
            },
        ] = seen.as_slice()
        else {
            panic!("{seen:?}");
        };
        assert_eq!(
            (call_name.as_str(), response_name.as_str()),
            ("web_search", "web_search")
        );
        assert!(call.contains("in_progress"), "{call}");
        assert!(response.contains("arc daemon"), "{response}");
        assert_eq!(text, "It is a daemon.");
        let grounding: Value = serde_json::from_str(grounding).expect("json");
        let urls: Vec<&str> = grounding["annotations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["url"].as_str().unwrap())
            .collect();
        assert_eq!(
            urls,
            ["https://a.example/x", "https://b.example/y"],
            "deduplicated by url"
        );
    }

    #[tokio::test]
    async fn a_custom_tool_call_streams_its_input_into_the_json_input_property() {
        let events = vec![
            json!({"type": "response.output_item.added", "output_index": 0,
                   "item": {"type": "custom_tool_call", "id": "ctc_1", "call_id": "call_c",
                            "name": "apply_patch", "input": ""}}),
            json!({"type": "response.custom_tool_call_input.delta", "output_index": 0, "delta": "*** Begin Patch\n"}),
            json!({"type": "response.custom_tool_call_input.delta", "output_index": 0, "delta": "*** Delete File: x\n*** End Patch"}),
            json!({"type": "response.custom_tool_call_input.done", "output_index": 0,
                   "input": "*** Begin Patch\n*** Delete File: x\n*** End Patch"}),
            json!({"type": "response.output_item.done", "output_index": 0,
                   "item": {"type": "custom_tool_call", "id": "ctc_1", "call_id": "call_c",
                            "name": "apply_patch", "input": "*** Begin Patch\n*** Delete File: x\n*** End Patch"}}),
            completed(5, 5, 0),
        ];

        let seen = ok(vec![sse(&events)]).await;

        let [
            CompletionDelta::ToolCall(call),
            CompletionDelta::Done {
                stop: Stop::ToolCalls,
                ..
            },
        ] = seen.as_slice()
        else {
            panic!("{seen:?}");
        };
        assert_eq!(
            (call.id.as_str(), call.name.as_str()),
            ("call_c", "apply_patch")
        );
        let args: Value = serde_json::from_str(&call.arguments).expect("json object");
        assert_eq!(
            args["input"],
            "*** Begin Patch\n*** Delete File: x\n*** End Patch"
        );
    }

    #[tokio::test]
    async fn a_reasoning_item_without_encrypted_content_is_not_carried() {
        let events = vec![
            json!({"type": "response.output_item.done", "output_index": 0,
                   "item": {"type": "reasoning", "id": "rs_0", "summary": []}}),
            json!({"type": "response.output_item.done", "output_index": 1,
                   "item": {"type": "function_call", "call_id": "call_z", "name": "f", "arguments": "{}"}}),
            completed(1, 1, 0),
        ];

        let seen = ok(vec![sse(&events)]).await;

        let CompletionDelta::ToolCall(call) = &seen[0] else {
            panic!("{seen:?}");
        };
        assert!(call.provider_roundtrip.is_empty());
    }

    #[tokio::test]
    async fn arguments_that_are_not_an_object_fail_the_stream() {
        let events = vec![
            json!({"type": "response.output_item.done", "output_index": 0,
                   "item": {"type": "function_call", "call_id": "c", "name": "f", "arguments": "[1]"}}),
            completed(1, 1, 0),
        ];

        let seen = deltas(vec![sse(&events)]).await;

        assert!(
            matches!(seen.as_slice(), [Err(Error::MalformedStream(_))]),
            "{seen:?}"
        );
    }

    #[tokio::test]
    async fn a_stream_cut_before_completion_ends_without_a_done() {
        let mut events = text_turn();
        events.pop();

        let seen = ok(vec![sse(&events)]).await;

        assert!(
            !seen
                .iter()
                .any(|d| matches!(d, CompletionDelta::Done { .. })),
            "{seen:?}"
        );
        assert_eq!(seen.len(), 3);
    }

    #[tokio::test]
    async fn a_usage_limit_failure_is_rate_limited_with_the_reset() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let events = vec![json!({"type": "response.failed", "response": {
            "status": "failed",
            "error": {"code": "usage_limit_reached", "message": "plan limit", "resets_at": now + 600},
        }})];

        let seen = deltas(vec![sse(&events)]).await;

        match seen.as_slice() {
            [
                Err(Error::RateLimited {
                    retry_after: Some(after),
                    detail,
                }),
            ] => {
                assert!((590..=600).contains(after), "{after}");
                assert!(detail.contains("plan limit"), "{detail}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn any_other_failure_is_a_refusal_in_the_servers_words() {
        let events =
            vec![json!({"type": "error", "code": "invalid_prompt", "message": "too long"})];

        let seen = deltas(vec![sse(&events)]).await;

        match seen.as_slice() {
            [Err(Error::Refused(detail))] => assert_eq!(detail, "invalid_prompt: too long"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_frame_that_is_not_json_fails_the_stream_once() {
        let seen = deltas(vec![b"data: not json\n\n".to_vec()]).await;

        assert!(
            matches!(seen.as_slice(), [Err(Error::MalformedStream(_))]),
            "{seen:?}"
        );
    }
}
