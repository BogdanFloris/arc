use std::collections::BTreeMap;

use serde::Deserialize;

use crate::provider::stream::{Deltas, FrameParser};
use crate::provider::{CompletionDelta, Error, Stop, ToolCall, Usage};

const DONE: &str = "[DONE]";

const TOOL_CALLS: &str = "tool_calls";

#[derive(Default)]
pub(super) struct Parser {
    building: BTreeMap<u32, Building>,

    stop: Option<Stop>,
}

impl FrameParser for Parser {
    const PROVIDER: &'static str = "openai-compat";

    fn frame(&mut self, payload: &str) -> Result<Deltas, Error> {
        if payload.trim() == DONE {
            return Ok(Deltas {
                items: Vec::new(),
                usage: None,
                finished: Some(self.stop.unwrap_or(Stop::EndTurn)),
            });
        }

        let chunk: Chunk = serde_json::from_str(payload).map_err(|source| {
            Error::MalformedStream(format!(
                "the endpoint sent a frame that is not a completion chunk: {source}: {}",
                crate::provider::snippet(payload)
            ))
        })?;
        let (choices, usage) = match (chunk.choices, chunk.usage) {
            (None, None) => {
                return Err(Error::MalformedStream(format!(
                    "the endpoint sent a frame that is not a completion chunk: {}",
                    crate::provider::snippet(payload)
                )));
            }
            (choices, usage) => (choices.unwrap_or_default(), usage),
        };

        let mut items = Vec::new();
        for mut choice in choices {
            if let Some(content) = choice.delta.content.take().filter(|it| !it.is_empty()) {
                items.push(CompletionDelta::Text(content));
            }
            if let Some(thinking) = choice.delta.reasoning() {
                items.push(CompletionDelta::Reasoning(thinking));
            }
            for call in choice.delta.tool_calls {
                self.accumulate(call)?;
            }
            if let Some(reason) = choice.finish_reason {
                items.extend(self.close(&reason)?);
            }
        }

        Ok(Deltas {
            items,
            usage: usage.as_ref().map(UsageJson::usage),
            finished: None,
        })
    }
}

impl Parser {
    fn accumulate(&mut self, call: ToolCallJson) -> Result<(), Error> {
        let index = call.index;
        let function = call.function.unwrap_or_default();
        let fragment = function.arguments.unwrap_or_default();

        let Some(name) = function.name else {
            let Some(building) = self.building.get_mut(&index) else {
                return Err(Error::MalformedStream(format!(
                    "the endpoint continued a tool call at index {index} that never opened"
                )));
            };
            building.arguments.push_str(&fragment);
            return Ok(());
        };

        let opened = Building {
            id: call.id.unwrap_or_default(),
            name,
            arguments: fragment,
        };
        if let Some(displaced) = self.building.insert(index, opened) {
            return Err(Error::MalformedStream(format!(
                "the endpoint opened two tool calls at index {index}, `{}` and the next",
                displaced.name
            )));
        }
        Ok(())
    }

    fn close(&mut self, reason: &str) -> Result<Vec<CompletionDelta>, Error> {
        let calls: Vec<CompletionDelta> = std::mem::take(&mut self.building)
            .into_iter()
            .map(|(index, building)| building.finish(index).map(CompletionDelta::ToolCall))
            .collect::<Result<_, _>>()?;

        // some endpoints say "stop" while still emitting tool calls
        self.stop = Some(if reason == TOOL_CALLS || !calls.is_empty() {
            Stop::ToolCalls
        } else {
            Stop::EndTurn
        });
        Ok(calls)
    }
}

struct Building {
    id: String,
    name: String,
    arguments: String,
}

impl Building {
    fn finish(self, index: u32) -> Result<ToolCall, Error> {
        if !matches!(
            serde_json::from_str::<serde_json::Value>(&self.arguments),
            Ok(serde_json::Value::Object(_))
        ) {
            return Err(Error::MalformedStream(format!(
                "the endpoint's arguments for tool call `{}` at index {index} are not a JSON object: {}",
                self.name,
                crate::provider::snippet(&self.arguments)
            )));
        }
        Ok(ToolCall {
            id: self.id,
            index,
            name: self.name,
            arguments: self.arguments,
            provider_roundtrip: Vec::new(),
        })
    }
}

#[derive(Deserialize)]
struct Chunk {
    choices: Option<Vec<Choice>>,
    usage: Option<UsageJson>,
}

#[derive(Deserialize)]
struct Choice {
    delta: Delta,

    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct Delta {
    content: Option<String>,

    reasoning_content: Option<String>,

    reasoning: Option<String>,

    reasoning_text: Option<String>,

    // OpenCode Go sends an explicit `"tool_calls": null` on prose chunks
    #[serde(default, deserialize_with = "null_as_empty")]
    tool_calls: Vec<ToolCallJson>,
}

impl Delta {
    // first non-empty alias wins; some endpoints duplicate across fields
    fn reasoning(&mut self) -> Option<String> {
        [
            self.reasoning_content.take(),
            self.reasoning.take(),
            self.reasoning_text.take(),
        ]
        .into_iter()
        .flatten()
        .find(|it| !it.is_empty())
    }
}

fn null_as_empty<'de, D>(deserializer: D) -> Result<Vec<ToolCallJson>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let calls: Option<Vec<ToolCallJson>> = Deserialize::deserialize(deserializer)?;
    Ok(calls.unwrap_or_default())
}

#[derive(Deserialize)]
struct ToolCallJson {
    index: u32,
    id: Option<String>,
    function: Option<FunctionJson>,
}

#[derive(Default, Deserialize)]
struct FunctionJson {
    name: Option<String>,
    arguments: Option<String>,
}

#[derive(Deserialize)]
struct UsageJson {
    #[serde(default)]
    prompt_tokens: u32,
    #[serde(default)]
    completion_tokens: u32,
    #[serde(default)]
    prompt_tokens_details: PromptTokensDetailsJson,
}

#[derive(Deserialize, Default)]
struct PromptTokensDetailsJson {
    #[serde(default)]
    cached_tokens: u32,
}

impl UsageJson {
    fn usage(&self) -> Usage {
        let cached = self.prompt_tokens_details.cached_tokens;
        if cached > 0 {
            tracing::info!(
                counter.cached_tokens = cached,
                counter.prompt_tokens = self.prompt_tokens,
                "prompt cache hit"
            );
        }
        Usage {
            input_tokens: self.prompt_tokens,
            output_tokens: self.completion_tokens,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, stream};
    use tracing::Span;

    use super::Parser;
    use crate::provider::stream::DeltaStream;
    use crate::provider::{CompletionDelta, Error, Stop, ToolCall, Usage};

    const FIXTURE: &[u8] = include_bytes!("../../../tests/fixtures/openai_stream.sse");

    const FIXTURE_USAGE: Usage = Usage {
        input_tokens: 17,
        output_tokens: 7,
    };

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

    fn text(chunk: &str) -> CompletionDelta {
        CompletionDelta::Text(chunk.to_owned())
    }

    fn whole_fixture() -> [CompletionDelta; 3] {
        [
            text("Hello"),
            text(" arc"),
            CompletionDelta::Done {
                usage: FIXTURE_USAGE,
                stop: Stop::EndTurn,
            },
        ]
    }

    #[tokio::test]
    async fn a_completion_decodes_to_its_text_and_a_closing_usage() {
        assert_eq!(ok(vec![FIXTURE.to_vec()]).await, whole_fixture());
    }

    #[tokio::test]
    async fn the_same_bytes_one_at_a_time_decode_the_same_way() {
        let dribble = FIXTURE.iter().map(|byte| vec![*byte]).collect();
        assert_eq!(ok(dribble).await, whole_fixture());
    }

    #[tokio::test]
    async fn no_split_point_changes_what_the_stream_yields() {
        for split in 0..FIXTURE.len() {
            let chunks = vec![FIXTURE[..split].to_vec(), FIXTURE[split..].to_vec()];
            assert_eq!(ok(chunks).await, whole_fixture(), "split at {split}");
        }
    }

    #[tokio::test]
    async fn a_stream_cut_before_the_sentinel_ends_without_a_done() {
        let sentinel = FIXTURE
            .windows(DONE_FRAME.len())
            .position(|window| window == DONE_FRAME)
            .expect("fixture has the sentinel");

        let seen = ok(vec![FIXTURE[..sentinel].to_vec()]).await;

        assert_eq!(seen, [text("Hello"), text(" arc")]);
    }

    const DONE_FRAME: &[u8] = b"data: [DONE]";

    #[tokio::test]
    async fn a_sentinel_without_usage_reports_zero_counts() {
        let body = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
            "data: [DONE]\n\n",
        );

        assert_eq!(
            ok(vec![body.as_bytes().to_vec()]).await,
            [
                text("hi"),
                CompletionDelta::Done {
                    usage: Usage::default(),
                    stop: Stop::EndTurn,
                },
            ]
        );
    }

    #[tokio::test]
    async fn a_frame_that_is_not_json_fails_the_stream_once() {
        let mut seen = deltas(vec![b"data: not json\n\ndata: [DONE]\n\n".to_vec()]).await;

        let error = seen.pop().expect("an item").expect_err("malformed frame");
        assert!(matches!(error, Error::MalformedStream(_)), "{error:?}");
        assert!(seen.is_empty(), "{seen:?}");
    }

    #[tokio::test]
    async fn an_error_envelope_fails_the_stream_with_its_message() {
        let seen = deltas(vec![
            br#"data: {"error":{"message":"model is loading"}}"#.to_vec(),
            b"\n\n".to_vec(),
        ])
        .await;

        let [Err(Error::MalformedStream(message))] = seen.as_slice() else {
            panic!("expected one malformed-stream error, got {seen:?}");
        };
        assert!(message.contains("model is loading"), "{message}");
    }

    #[tokio::test]
    async fn chunks_without_content_produce_no_deltas() {
        let body = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n",
        );

        assert_eq!(
            ok(vec![body.as_bytes().to_vec()]).await,
            [CompletionDelta::Done {
                usage: Usage::default(),
                stop: Stop::EndTurn,
            }]
        );
    }

    #[tokio::test]
    async fn frames_after_the_sentinel_are_ignored() {
        let mut chunk = FIXTURE.to_vec();
        chunk.extend_from_slice(b"data: still talking\n\n");

        assert_eq!(ok(vec![chunk]).await, whole_fixture());
    }

    const TOOL_CALL: &[u8] = include_bytes!("../../../tests/fixtures/openai_tool_call_stream.sse");

    const PARALLEL: &[u8] =
        include_bytes!("../../../tests/fixtures/openai_parallel_tool_calls_stream.sse");

    const TOOL_RESULT: &[u8] =
        include_bytes!("../../../tests/fixtures/openai_tool_result_stream.sse");

    const REASONING: &[u8] = include_bytes!("../../../tests/fixtures/openai_reasoning_stream.sse");

    fn call(id: &str, index: u32, name: &str, arguments: &str) -> CompletionDelta {
        CompletionDelta::ToolCall(ToolCall {
            id: id.to_owned(),
            index,
            name: name.to_owned(),
            arguments: arguments.to_owned(),
            provider_roundtrip: Vec::new(),
        })
    }

    fn done(input_tokens: u32, output_tokens: u32, stop: Stop) -> CompletionDelta {
        CompletionDelta::Done {
            usage: Usage {
                input_tokens,
                output_tokens,
            },
            stop,
        }
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    struct Gathered {
        text: String,
        reasoning: String,
        calls: Vec<CompletionDelta>,
        ending: Option<CompletionDelta>,
    }

    fn gather(seen: Vec<CompletionDelta>) -> Gathered {
        let mut gathered = Gathered::default();
        for delta in seen {
            match delta {
                CompletionDelta::Text(chunk) => gathered.text.push_str(&chunk),
                CompletionDelta::Reasoning(chunk) => gathered.reasoning.push_str(&chunk),
                other @ CompletionDelta::ToolCall(_) => gathered.calls.push(other),
                other @ CompletionDelta::Done { .. } => gathered.ending = Some(other),
            }
        }
        gathered
    }

    #[tokio::test]
    async fn a_tool_call_stream_yields_one_whole_call() {
        assert_eq!(
            ok(vec![TOOL_CALL.to_vec()]).await,
            [
                call(
                    "Mv4PbWn7EMNC02U1RH2mAOs7iQAGCrR7",
                    0,
                    "memory_search",
                    r#"{"query": "database for the arc project", "namespace": "projects"}"#,
                ),
                done(339, 34, Stop::ToolCalls),
            ]
        );
    }

    #[tokio::test]
    async fn no_split_point_changes_the_call_a_stream_yields() {
        let whole = ok(vec![TOOL_CALL.to_vec()]).await;
        for split in 0..TOOL_CALL.len() {
            let chunks = vec![TOOL_CALL[..split].to_vec(), TOOL_CALL[split..].to_vec()];
            assert_eq!(ok(chunks).await, whole, "split at {split}");
        }
    }

    #[tokio::test]
    async fn parallel_calls_do_not_merge() {
        assert_eq!(
            ok(vec![PARALLEL.to_vec()]).await,
            [
                call("VB3c1GM6T8PkODpn2dKAIlzxgVhzqW8N", 0, "get_time", "{}"),
                call(
                    "BvOHKsJQZl7aEna9VlPIEgNsw5I4lMig",
                    1,
                    "memory_search",
                    r#"{"query": "arc project's database", "namespace": "projects"}"#,
                ),
                done(345, 49, Stop::ToolCalls),
            ]
        );
    }

    #[tokio::test]
    async fn a_stream_after_a_tool_result_is_plain_text() {
        let gathered = gather(ok(vec![TOOL_RESULT.to_vec()]).await);

        assert!(
            gathered.text.contains("SQLite with an FTS5 table"),
            "{gathered:?}"
        );
        assert!(gathered.reasoning.is_empty());
        assert!(gathered.calls.is_empty());
        assert_eq!(gathered.ending, Some(done(451, 52, Stop::EndTurn)));
    }

    #[tokio::test]
    async fn reasoning_arrives_as_its_own_deltas_before_the_text() {
        let seen = ok(vec![REASONING.to_vec()]).await;

        let kinds: Vec<&CompletionDelta> = seen.iter().collect();
        let first_text = kinds
            .iter()
            .position(|delta| matches!(delta, CompletionDelta::Text(_)))
            .expect("the fixture ends in text");
        assert!(
            kinds[first_text..]
                .iter()
                .all(|delta| !matches!(delta, CompletionDelta::Reasoning(_))),
            "the model stopped thinking before it spoke"
        );

        let gathered = gather(seen);
        assert!(
            gathered
                .reasoning
                .starts_with("Okay, the user wants a two-line rhyme"),
            "{:?}",
            gathered.reasoning
        );
        assert_eq!(
            gathered.text,
            "The cat tapped keys with claws so fine,  \nA digital purr echoed through time."
        );
        assert!(gathered.calls.is_empty());
        assert_eq!(gathered.ending, Some(done(335, 114, Stop::EndTurn)));
    }

    #[tokio::test]
    async fn a_call_cut_before_its_finish_chunk_never_surfaces() {
        let finish = TOOL_CALL
            .windows(FINISH_FRAME.len())
            .position(|window| window == FINISH_FRAME)
            .expect("the fixture has a finish chunk");

        assert_eq!(ok(vec![TOOL_CALL[..finish].to_vec()]).await, []);
    }

    const FINISH_FRAME: &[u8] = br#"{"finish_reason":"tool_calls""#;

    #[tokio::test]
    async fn a_continuation_for_an_index_that_never_opened_is_malformed() {
        let body = concat!(
            r#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":2,"function":{"arguments":"{}"}}]}}]}"#,
            "\n\n",
        );

        let seen = deltas(vec![body.into()]).await;

        let [Err(Error::MalformedStream(message))] = seen.as_slice() else {
            panic!("expected one malformed-stream error, got {seen:?}");
        };
        assert!(message.contains("index 2"), "{message}");
    }

    #[tokio::test]
    async fn two_calls_opening_at_one_index_are_malformed() {
        let body = concat!(
            r#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"a","type":"function","function":{"name":"get_time","arguments":"{}"}}]}}]}"#,
            "\n\n",
            r#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"b","type":"function","function":{"name":"memory_search","arguments":"{}"}}]}}]}"#,
            "\n\n",
        );

        let seen = deltas(vec![body.into()]).await;

        let [Err(Error::MalformedStream(message))] = seen.as_slice() else {
            panic!("expected one malformed-stream error, got {seen:?}");
        };
        assert!(message.contains("get_time"), "{message}");
    }

    #[tokio::test]
    async fn arguments_that_do_not_form_a_json_object_are_malformed() {
        let body = concat!(
            r#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"a","type":"function","function":{"name":"get_time","arguments":"{\"cut\":"}}]}}]}"#,
            "\n\n",
            r#"data: {"choices":[{"finish_reason":"tool_calls","index":0,"delta":{}}]}"#,
            "\n\n",
        );

        let seen = deltas(vec![body.into()]).await;

        let [Err(Error::MalformedStream(message))] = seen.as_slice() else {
            panic!("expected one malformed-stream error, got {seen:?}");
        };
        assert!(message.contains("get_time"), "{message}");
    }

    #[tokio::test]
    async fn the_reasoning_alias_decodes_like_reasoning_content() {
        let body = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"thinking\"},\"finish_reason\":null}]}\n\n",
            "data: [DONE]\n\n",
        );

        let seen = ok(vec![body.as_bytes().to_vec()]).await;
        assert!(
            seen.contains(&CompletionDelta::Reasoning("thinking".to_owned())),
            "{seen:?}"
        );
    }

    #[tokio::test]
    async fn a_chunk_with_both_reasoning_fields_counts_once() {
        let body = concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"a\",\"reasoning\":\"b\"},\"finish_reason\":null}]}\n\n",
            "data: [DONE]\n\n",
        );

        let seen = ok(vec![body.as_bytes().to_vec()]).await;
        let gathered = gather(seen);
        assert_eq!(gathered.reasoning, "a");
    }

    #[tokio::test]
    async fn an_explicit_null_tool_calls_field_is_an_empty_list_not_an_error() {
        let chunk = concat!(
            "data: {\"id\":\"router-x\",\"object\":\"chat.completion.chunk\",",
            "\"created\":1,\"model\":\"deepseek-v4-flash\",\"choices\":[{\"index\":0,",
            "\"finish_reason\":null,\"logprobs\":null,",
            "\"delta\":{\"reasoning_content\":null,\"content\":\"hi\",\"tool_calls\":null}}]}\n\n",
            "data: [DONE]\n\n"
        );
        let seen = ok(vec![chunk.as_bytes().to_vec()]).await;
        assert!(
            seen.iter()
                .any(|delta| matches!(delta, CompletionDelta::Text(text) if text == "hi")),
            "{seen:?}"
        );
    }
}
