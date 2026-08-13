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
//! - With `stream_options.include_usage`, the last data frame before the
//!   sentinel has empty `choices` and a `usage` object.
//! - The terminal frame is the literal sentinel `data: [DONE]`. A
//!   `finish_reason` alone is not terminal: the usage frame follows it, so
//!   ending there would drop the counts.
//!
//! The fixture in `tests/fixtures/openai_stream.sse` is constructed from that
//! documented format, not captured live — task 7.2 note: recapture it from a
//! running `llama-server` once the sidecar exists, the way 4.4's fixture was
//! captured.

use serde::Deserialize;

use crate::provider::stream::{Deltas, FrameParser};
use crate::provider::{Error, Usage};

/// The terminal sentinel, sent instead of JSON.
const DONE: &str = "[DONE]";

/// This dialect's frame parser.
pub(super) struct Parser;

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
    /// something into a stream that silently says nothing.
    fn frame(&mut self, payload: &str) -> Result<Deltas, Error> {
        if payload.trim() == DONE {
            return Ok(Deltas {
                text: Vec::new(),
                usage: None,
                finished: true,
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

        Ok(Deltas {
            text: choices
                .into_iter()
                .filter_map(|choice| choice.delta.content)
                // The role-only first chunk and the finish chunk carry empty
                // or absent content; neither is a chunk of reply.
                .filter(|content| !content.is_empty())
                .collect(),
            usage: usage.as_ref().map(UsageJson::usage),
            finished: false,
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
    // `finish_reason` is deliberately not read: the `[DONE]` sentinel is the
    // terminal marker, and the usage frame arrives between the two.
}

/// The incremental part of a choice.
#[derive(Deserialize)]
struct Delta {
    /// Generated text, when this chunk carries any.
    content: Option<String>,
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
    use crate::provider::{CompletionDelta, Error, Usage};

    /// A full completion in this dialect: role chunk, two text chunks, finish
    /// chunk, usage frame, sentinel. Constructed from the documented format —
    /// see the module docs for the recapture note.
    const FIXTURE: &[u8] = include_bytes!("../../../tests/fixtures/openai_stream.sse");

    /// What the fixture bills.
    const FIXTURE_USAGE: Usage = Usage {
        input_tokens: 9,
        output_tokens: 2,
    };

    /// Drives a stream built from `chunks` to its end.
    async fn deltas(chunks: Vec<Vec<u8>>) -> Vec<Result<CompletionDelta, Error>> {
        let bytes = stream::iter(chunks.into_iter().map(Ok::<Vec<u8>, reqwest::Error>));
        DeltaStream::new(bytes, Parser, Span::none())
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
            text("hello"),
            text(" arc"),
            CompletionDelta::Done {
                usage: FIXTURE_USAGE,
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

        assert_eq!(seen, [text("hello"), text(" arc")]);
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
                    usage: Usage::default()
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
                usage: Usage::default()
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
}
