//! The OpenAI-compatible streaming dialect.
//!
//! # What the endpoint sends
//!
//! The `OpenAI` `chat.completion.chunk` format, as llama.cpp's `llama-server`
//! implements it:
//!
//! - Frames are `data:` payloads, LF-framed. Text lives at
//!   `choices[].delta.content`; the first chunk may carry a bare role and an
//!   empty content, the finish chunk an empty delta beside a `finish_reason`.
//!   Empty content is not a text delta and is dropped.
//! - Thinking lives at `choices[].delta.reasoning_content`, its own field
//!   rather than `<think>` tags inside the text. No delta ever carries both.
//! - A tool call opens at `choices[].delta.tool_calls[]` with an `index`, an
//!   `id`, and a `function` giving the name and the first fragment of the
//!   arguments. Every later chunk of that call carries the `index` and one
//!   more fragment — never the id or the name again — so arguments are valid
//!   JSON only once concatenated, and a call taking no arguments arrives as
//!   `"{"` then `"}"`. One turn can open several calls, `index` dense from 0,
//!   interleaved with nothing between them; index is the only thing that
//!   tells them apart.
//! - `finish_reason: "tool_calls"`, on a chunk whose delta is empty, is what
//!   closes those calls. `"stop"` is not the only reason a turn can end.
//! - With `stream_options.include_usage`, the last data frame before the
//!   sentinel has empty `choices` and a `usage` object.
//! - The terminal frame is the literal sentinel `data: [DONE]`. A
//!   `finish_reason` alone is not terminal: the usage frame follows it, so
//!   ending there would drop the counts.
//!
//! The fixtures in `tests/fixtures/` are all real completions, captured live
//! from `llama-server` b10273 (Qwen3-8B-Q4_K_M): `openai_stream.sse` on
//! 2026-08-13, the four tool and reasoning captures on 2026-08-14 by the 1.1
//! spike, whose verdict in `docs/TASKS.md` is where the rules above are
//! evidenced. Details the documentation alone would not have shown: the
//! role-only first chunk carries `"content": null` rather than an empty
//! string, the usage frame brings llama.cpp's own `timings` object along, and
//! thinking is on by default — `openai_stream.sse`, with no reasoning in it
//! at all, is the unusual capture.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::provider::stream::{Deltas, FrameParser};
use crate::provider::{CompletionDelta, Error, Stop, ToolCall, Usage};

/// The terminal sentinel, sent instead of JSON.
const DONE: &str = "[DONE]";

/// The `finish_reason` that closes a step of tool calls.
const TOOL_CALLS: &str = "tool_calls";

/// This dialect's frame parser.
///
/// Stateful, because a tool call is spread across frames and only whole calls
/// cross the provider seam.
#[derive(Default)]
pub(super) struct Parser {
    /// Calls still arriving, keyed by the index that owns them. Keyed and
    /// ordered by index rather than by arrival, which is what keeps two
    /// parallel calls two calls.
    building: BTreeMap<u32, Building>,

    /// Why the model stopped, once a finish chunk has said. Kept because the
    /// sentinel that ends the stream arrives two frames later.
    stop: Option<Stop>,
}

impl FrameParser for Parser {
    const PROVIDER: &'static str = "openai-compat";

    /// Reads one `data:` payload.
    ///
    /// # Errors
    ///
    /// [`Error::MalformedStream`] if the payload is neither the `[DONE]`
    /// sentinel nor JSON with a completion-chunk shape. Unknown fields are
    /// not an error; a frame with *neither* `choices` nor `usage` is, because
    /// that is not a chunk — it is an error envelope or a different API's
    /// shape, and swallowing it would turn a server that is telling us
    /// something into a stream that silently says nothing. A tool call whose
    /// pieces do not add up — a continuation for an index that never opened,
    /// two calls opening at one index, arguments that are not a JSON object
    /// once joined — is malformed for the same reason: the alternative is
    /// handing the loop a call it cannot run.
    fn frame(&mut self, payload: &str) -> Result<Deltas, Error> {
        if payload.trim() == DONE {
            return Ok(Deltas {
                items: Vec::new(),
                usage: None,
                // No finish chunk seen at all means a server that does not
                // send one; the turn still ended.
                finished: Some(self.stop.unwrap_or(Stop::EndTurn)),
            });
        }

        let chunk: Chunk = serde_json::from_str(payload).map_err(|source| {
            Error::MalformedStream(format!(
                "the endpoint sent a frame that is not a completion chunk: {source}: {}",
                super::snippet(payload)
            ))
        })?;
        let (choices, usage) = match (chunk.choices, chunk.usage) {
            (None, None) => {
                return Err(Error::MalformedStream(format!(
                    "the endpoint sent a frame that is not a completion chunk: {}",
                    super::snippet(payload)
                )));
            }
            (choices, usage) => (choices.unwrap_or_default(), usage),
        };

        let mut items = Vec::new();
        for choice in choices {
            // The role-only first chunk and the finish chunk carry empty or
            // absent content; neither is a chunk of reply. Text, thinking and
            // calls are read as three independent fields rather than as
            // alternatives, even though this dialect never mixes them in one
            // delta.
            if let Some(content) = choice.delta.content.filter(|it| !it.is_empty()) {
                items.push(CompletionDelta::Text(content));
            }
            if let Some(thinking) = choice.delta.reasoning_content.filter(|it| !it.is_empty()) {
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
    /// Folds one `tool_calls[]` entry into the call its index names.
    fn accumulate(&mut self, call: ToolCallJson) -> Result<(), Error> {
        let index = call.index;
        let function = call.function.unwrap_or_default();
        let fragment = function.arguments.unwrap_or_default();

        // A name marks the opening chunk, and the opening chunk is the only
        // authority on id and name: continuations repeat neither.
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

    /// Ends a step: the calls it collected, whole, in index order.
    ///
    /// Only whole calls come out of here, and only here — a stream cut before
    /// its finish chunk drops what it was building, which is the partial rule
    /// in DESIGN.md §3.1: a half-streamed call never surfaces.
    fn close(&mut self, reason: &str) -> Result<Vec<CompletionDelta>, Error> {
        let calls: Vec<CompletionDelta> = std::mem::take(&mut self.building)
            .into_iter()
            .map(|(index, building)| building.finish(index).map(CompletionDelta::ToolCall))
            .collect::<Result<_, _>>()?;

        // A stream that delivered calls needs them run, whatever it called the
        // reason: saying `EndTurn` over a call the caller is holding would
        // strand it.
        self.stop = Some(if reason == TOOL_CALLS || !calls.is_empty() {
            Stop::ToolCalls
        } else {
            Stop::EndTurn
        });
        Ok(calls)
    }
}

/// A tool call mid-arrival.
struct Building {
    id: String,
    name: String,
    /// Fragments so far, concatenated. Valid JSON only when the call is whole.
    arguments: String,
}

impl Building {
    /// The call as the seam spells it, once its arguments are complete.
    fn finish(self, index: u32) -> Result<ToolCall, Error> {
        if !matches!(
            serde_json::from_str::<serde_json::Value>(&self.arguments),
            Ok(serde_json::Value::Object(_))
        ) {
            return Err(Error::MalformedStream(format!(
                "the endpoint's arguments for tool call `{}` at index {index} are not a JSON object: {}",
                self.name,
                super::snippet(&self.arguments)
            )));
        }
        Ok(ToolCall {
            id: self.id,
            index,
            name: self.name,
            arguments: self.arguments,
        })
    }
}

/// One `chat.completion.chunk`, reduced to what ARC reads.
///
/// Both fields optional at the serde level so [`Parser::frame`] can tell "a
/// chunk with no choices" (the usage frame) apart from "not a chunk at all"
/// (neither field present).
#[derive(Deserialize)]
struct Chunk {
    choices: Option<Vec<Choice>>,
    usage: Option<UsageJson>,
}

/// One streamed choice.
#[derive(Deserialize)]
struct Choice {
    /// What this chunk adds to the reply.
    delta: Delta,

    /// Why the model stopped, on the one chunk that says so. Not terminal on
    /// its own — the usage frame and the sentinel follow it — but it is what
    /// closes a step's tool calls and what tells the two endings apart.
    finish_reason: Option<String>,
}

/// The incremental part of a choice.
#[derive(Deserialize)]
struct Delta {
    /// Generated text, when this chunk carries any.
    content: Option<String>,

    /// The model's thinking, which this dialect streams beside the text
    /// rather than inside it. Dropping it is what left Phase 1 silent for
    /// seconds at a time while a thinking model worked (1.1's second
    /// surprise).
    reasoning_content: Option<String>,

    /// Pieces of the calls this chunk advances.
    #[serde(default)]
    tool_calls: Vec<ToolCallJson>,
}

/// One `tool_calls[]` entry: an opening or a continuation.
///
/// Everything but `index` is optional because a continuation carries nothing
/// else. `type` is not read: it has one value, and a second one would arrive
/// as a new field rather than a new string (DESIGN.md §3.1).
#[derive(Deserialize)]
struct ToolCallJson {
    index: u32,
    id: Option<String>,
    function: Option<FunctionJson>,
}

/// The called function, in pieces.
#[derive(Default, Deserialize)]
struct FunctionJson {
    name: Option<String>,
    arguments: Option<String>,
}

/// Token counts as this dialect reports them.
#[derive(Deserialize)]
struct UsageJson {
    #[serde(default)]
    prompt_tokens: u32,
    #[serde(default)]
    completion_tokens: u32,
}

impl UsageJson {
    /// The counts as [`Usage`] spells them. `total_tokens` is not read: it is
    /// the sum of the other two, and a derived number is not a source.
    fn usage(&self) -> Usage {
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

    /// One real completion, captured live: a role-only chunk with null
    /// content, two text chunks, the finish chunk, the usage frame, the
    /// sentinel. See the module docs.
    const FIXTURE: &[u8] = include_bytes!("../../../tests/fixtures/openai_stream.sse");

    /// What the fixture bills. Completion tokens exceed the visible text:
    /// the template's swallowed empty think block is generated and billed
    /// like anything else.
    const FIXTURE_USAGE: Usage = Usage {
        input_tokens: 17,
        output_tokens: 7,
    };

    /// Drives a stream built from `chunks` to its end.
    async fn deltas(chunks: Vec<Vec<u8>>) -> Vec<Result<CompletionDelta, Error>> {
        let bytes = stream::iter(chunks.into_iter().map(Ok::<Vec<u8>, reqwest::Error>));
        DeltaStream::new(bytes, Parser::default(), Span::none())
            .collect()
            .await
    }

    /// The same, for a stream that is expected to succeed.
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

    /// The worst chunking a network can produce.
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

    /// Cut before the sentinel: the text stands, no `Done` is invented —
    /// even when the finish chunk and the usage frame already arrived.
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

    /// A sentinel with no usage frame before it still closes the stream;
    /// counts degrade to zero, the reply does not.
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
        // Fused: the sentinel after the bad frame is never reported.
        assert!(seen.is_empty(), "{seen:?}");
    }

    /// An error envelope is JSON but not a chunk, and its own words survive
    /// into the error a person reads.
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

    /// The role-only first chunk and the empty finish delta produce nothing.
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

    /// Nothing after the sentinel is read.
    #[tokio::test]
    async fn frames_after_the_sentinel_are_ignored() {
        let mut chunk = FIXTURE.to_vec();
        chunk.extend_from_slice(b"data: still talking\n\n");

        assert_eq!(ok(vec![chunk]).await, whole_fixture());
    }

    /// One `memory_search` call, `/no_think`: the minimal tool case.
    const TOOL_CALL: &[u8] = include_bytes!("../../../tests/fixtures/openai_tool_call_stream.sse");

    /// Two calls in one turn, indexes 0 and 1, the first with no arguments.
    const PARALLEL: &[u8] =
        include_bytes!("../../../tests/fixtures/openai_parallel_tool_calls_stream.sse");

    /// The round trip: a `role: "tool"` result went in, a grounded answer and
    /// `finish_reason: "stop"` came back.
    const TOOL_RESULT: &[u8] =
        include_bytes!("../../../tests/fixtures/openai_tool_result_stream.sse");

    /// Thinking on: 91 `reasoning_content` deltas, then 18 of text.
    const REASONING: &[u8] = include_bytes!("../../../tests/fixtures/openai_reasoning_stream.sse");

    fn call(id: &str, index: u32, name: &str, arguments: &str) -> CompletionDelta {
        CompletionDelta::ToolCall(ToolCall {
            id: id.to_owned(),
            index,
            name: name.to_owned(),
            arguments: arguments.to_owned(),
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

    /// A drained stream by kind, for the long fixtures: text and thinking
    /// joined, calls in order.
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

    /// Fragments concatenate into the arguments the tool is handed, and the
    /// turn ends saying the model wants them run.
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

    /// Arguments arrive one token per frame, so every split point is a split
    /// inside a JSON string.
    #[tokio::test]
    async fn no_split_point_changes_the_call_a_stream_yields() {
        let whole = ok(vec![TOOL_CALL.to_vec()]).await;
        for split in 0..TOOL_CALL.len() {
            let chunks = vec![TOOL_CALL[..split].to_vec(), TOOL_CALL[split..].to_vec()];
            assert_eq!(ok(chunks).await, whole, "split at {split}");
        }
    }

    /// Two calls in one turn stay two: index is what tells them apart, and
    /// index 1's opener follows index 0's last fragment with nothing between.
    /// A call taking no arguments arrives as `"{"` then `"}"`.
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

    /// The other half of a loop: with the result in the history the model
    /// answers in prose and stops, and nothing about the turn says tools.
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

    /// Thinking reaches the caller instead of being dropped on the floor —
    /// the dead air 1.1 found in Phase 1's output. It is its own delta kind,
    /// never mixed into the text.
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

    /// The partial rule (DESIGN.md §3.1): a call whose arguments were still
    /// arriving is not a call, and nothing downstream ever sees it.
    #[tokio::test]
    async fn a_call_cut_before_its_finish_chunk_never_surfaces() {
        let finish = TOOL_CALL
            .windows(FINISH_FRAME.len())
            .position(|window| window == FINISH_FRAME)
            .expect("the fixture has a finish chunk");

        assert_eq!(ok(vec![TOOL_CALL[..finish].to_vec()]).await, []);
    }

    const FINISH_FRAME: &[u8] = br#"{"finish_reason":"tool_calls""#;

    /// Only the opening chunk names a call, so a fragment for an index that
    /// never opened cannot be attached to anything.
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

    /// Two openers at one index would silently overwrite the first call.
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

    /// A `ToolCall` promises complete arguments, so the parser that builds one
    /// is where the promise is checked.
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
}
