//! The Antigravity backend's streaming half: its frame dialect.
//!
//! The machine that drives bytes → frames → deltas lives in
//! [`stream`](crate::provider::stream); this module supplies the one piece
//! that is this backend's own — what a `data:` payload says — and the wrapper
//! that joins the two.
//!
//! # What the backend sends
//!
//! Verified against a live completion (task 4.3) and captured in
//! `tests/fixtures/antigravity_stream.sse`:
//!
//! - Frames are `data:` payloads, CRLF-framed. No `event:`, no `id:`, no
//!   `[DONE]` sentinel.
//! - Text lives at `response.candidates[].content.parts[].text`, and a part may
//!   carry an empty one — the terminal frame's part is `""` beside a
//!   `thoughtSignature`. An empty part is not a text delta and is dropped.
//! - The terminal frame is the one whose candidate carries a `finishReason`.
//!   After it the connection simply ends.
//! - `usageMetadata` repeats on every frame, with the same counts throughout.

use serde::Deserialize;
use tracing::Span;

use crate::provider::stream::{Deltas, FrameParser, deltas as stream_deltas};
use crate::provider::{CompletionStream, Error, Usage};

/// Wraps a started response as the stream of deltas it decodes to.
pub(super) fn deltas(response: reqwest::Response, span: Span) -> CompletionStream {
    stream_deltas(response, Parser, span)
}

/// This backend's frame dialect.
pub(super) struct Parser;

impl FrameParser for Parser {
    const PROVIDER: &'static str = "antigravity";

    /// Reads one `data:` payload.
    ///
    /// # Errors
    ///
    /// [`Error::MalformedStream`] if the payload is not JSON, or is JSON that
    /// is not a completion response. Unknown fields are not an error: this is
    /// an internal API that adds them without notice, and rejecting a frame
    /// for carrying a field we do not read would break a working stream over
    /// nothing.
    fn frame(&mut self, payload: &str) -> Result<Deltas, Error> {
        let frame: StreamFrame = serde_json::from_str(payload).map_err(|source| {
            Error::MalformedStream(format!(
                "antigravity sent a frame that is not a completion response: {source}: {}",
                super::detail(payload)
            ))
        })?;
        let response = frame.response;

        let text = response
            .candidates
            .iter()
            .filter_map(|candidate| candidate.content.as_ref())
            .flat_map(|content| &content.parts)
            .filter_map(|part| part.text.as_ref())
            // An empty part is not an empty chunk of reply: it is a part that
            // carries something else, like the terminal frame's thought
            // signature.
            .filter(|text| !text.is_empty())
            .map(String::clone)
            .collect();

        Ok(Deltas {
            text,
            usage: response.usage_metadata.as_ref().map(UsageMetadata::usage),
            finished: response
                .candidates
                .iter()
                .any(|candidate| candidate.finish_reason.is_some()),
        })
    }
}

/// One frame's JSON: the routing wrapper, and the Gemini response inside it.
///
/// `response` is required, and its absence is the one structural check here. A
/// frame without it is not a completion response — it is an error envelope or a
/// different API's shape — and treating it as an empty response would turn a
/// backend that is telling us something into a stream that silently says
/// nothing. Everything below is optional, because an internal API drops fields
/// as freely as it adds them.
#[derive(Deserialize)]
struct StreamFrame {
    /// The Gemini-shaped response for this chunk.
    response: Response,
}

/// The part of a frame that carries generated text and counts.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    /// Candidate completions. Nothing asks this backend for more than one, but
    /// the field is a list on the wire and is read as one.
    #[serde(default)]
    candidates: Vec<Candidate>,

    /// Token counts, repeated on every frame.
    usage_metadata: Option<UsageMetadata>,
}

/// One candidate completion.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Candidate {
    /// The text generated in this frame, when it generated any.
    content: Option<Content>,

    /// Why generation stopped — `STOP`, `MAX_TOKENS`, a safety reason. Present
    /// only on the terminal frame, which is the only thing this layer reads it
    /// for: the value itself is not yet exposed to callers, because
    /// [`CompletionDelta`](crate::provider::CompletionDelta) has nowhere to
    /// put it and inventing a place from one backend's vocabulary is how a
    /// stop reason ends up meaning different things per provider.
    finish_reason: Option<String>,
}

/// A candidate's content for one frame.
#[derive(Deserialize)]
struct Content {
    /// Parts of the turn. Text is the only kind ARC reads in Phase 1; a part
    /// carrying anything else has no `text` and is skipped.
    #[serde(default)]
    parts: Vec<Part>,
}

/// One part of a candidate's content.
#[derive(Deserialize)]
struct Part {
    /// Generated text, when this part carries any.
    text: Option<String>,
}

/// Token counts as this backend reports them.
#[derive(Deserialize)]
struct UsageMetadata {
    /// Tokens in the prompt.
    #[serde(default, rename = "promptTokenCount")]
    prompt: u32,

    /// Tokens of visible reply.
    #[serde(default, rename = "candidatesTokenCount")]
    candidates: u32,

    /// Tokens of thinking, which the reply does not contain.
    #[serde(default, rename = "thoughtsTokenCount")]
    thoughts: u32,
    // `totalTokenCount` is deliberately not read: see `usage`.
}

impl UsageMetadata {
    /// The counts as [`Usage`] spells them — the one place this mapping is
    /// decided.
    ///
    /// `output_tokens` sums the visible reply and the thinking behind it.
    /// Thinking tokens are generated and billed like any other output token;
    /// reporting only `candidatesTokenCount` would understate the cost of every
    /// completion downstream, and on this backend understate it badly — the
    /// fixture's reply is 2 visible tokens beside 73 thought ones.
    ///
    /// `totalTokenCount` is not the sum of the other two — it counts thoughts as
    /// well — so it is not a shortcut for either field here, and reading it
    /// would quietly double-count.
    fn usage(&self) -> Usage {
        Usage {
            input_tokens: self.prompt,
            // Saturating because a wrong number is better than a panic in a
            // release build and a panic in a debug one; these counts come from
            // a backend we do not control.
            output_tokens: self.candidates.saturating_add(self.thoughts),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures::executor::block_on;
    use futures::{StreamExt, stream};

    use super::{Parser, Span};
    use crate::provider::stream::DeltaStream;
    use crate::provider::{CompletionDelta, Error, Usage};

    /// One real completion, captured live: a text frame, then a terminal frame
    /// whose only part is an empty string beside a thought signature.
    const FIXTURE: &[u8] = include_bytes!("../../../tests/fixtures/antigravity_stream.sse");

    /// What the fixture bills: 11 prompt tokens, 2 of reply and 73 of thinking.
    const FIXTURE_USAGE: Usage = Usage {
        input_tokens: 11,
        output_tokens: 75,
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

    /// Runs `body` with everything it traces collected as text.
    ///
    /// Synchronous on purpose: `with_default` installs a subscriber for this
    /// thread and nothing else, so the work has to happen inside the call
    /// rather than across an executor's await points. Nothing in these streams
    /// does I/O, so a bare executor drives them fine.
    fn traced<T>(body: impl FnOnce() -> T) -> String {
        let written = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&written);
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || Collected(Arc::clone(&writer)))
            .finish();

        tracing::subscriber::with_default(subscriber, body);

        let written = written.lock().expect("trace buffer");
        String::from_utf8(written.clone()).expect("trace output is utf-8")
    }

    /// The sink [`traced`] points a subscriber at.
    struct Collected(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Collected {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("trace buffer").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// One byte past the blank line that ends the fixture's first frame — the
    /// prefix of the capture that is exactly one complete frame.
    fn first_frame_end() -> usize {
        FIXTURE
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("the fixture has a frame boundary")
            + 4
    }

    /// The fixture is CRLF-framed, which is what the backend sends. A checkout
    /// that normalized it would leave the CRLF tests testing nothing.
    #[test]
    fn the_fixture_keeps_the_framing_it_was_captured_with() {
        assert!(FIXTURE.ends_with(b"\r\n\r\n"), "fixture is not CRLF-framed");
    }

    #[tokio::test]
    async fn a_captured_completion_decodes_to_its_text_and_a_closing_usage() {
        let seen = ok(vec![FIXTURE.to_vec()]).await;

        assert_eq!(
            seen,
            [
                text("hello arc"),
                CompletionDelta::Done {
                    usage: FIXTURE_USAGE
                },
            ]
        );
    }

    /// The worst chunking a network can produce. Frame boundaries do not align
    /// with chunk boundaries on the wire either — this is the same property,
    /// tested at its limit.
    #[tokio::test]
    async fn the_same_bytes_one_at_a_time_decode_the_same_way() {
        let dribble = FIXTURE.iter().map(|byte| vec![*byte]).collect();

        assert_eq!(
            ok(dribble).await,
            [
                text("hello arc"),
                CompletionDelta::Done {
                    usage: FIXTURE_USAGE
                },
            ]
        );
    }

    /// Every split point, including the ones inside a frame and between a CR
    /// and its LF.
    #[tokio::test]
    async fn no_split_point_changes_what_the_stream_yields() {
        let expected = [
            text("hello arc"),
            CompletionDelta::Done {
                usage: FIXTURE_USAGE,
            },
        ];

        for split in 0..FIXTURE.len() {
            let chunks = vec![FIXTURE[..split].to_vec(), FIXTURE[split..].to_vec()];

            assert_eq!(ok(chunks).await, expected, "split at {split}");
        }
    }

    /// The same stream framed with bare LF, which the parser accepts even
    /// though this backend does not send it.
    #[tokio::test]
    async fn an_lf_framed_stream_decodes_the_same_way() {
        let lf = String::from_utf8(FIXTURE.to_vec())
            .expect("fixture is utf-8")
            .replace("\r\n", "\n");

        assert_eq!(
            ok(vec![lf.into_bytes()]).await,
            [
                text("hello arc"),
                CompletionDelta::Done {
                    usage: FIXTURE_USAGE
                },
            ]
        );
    }

    /// A connection that dies mid-frame: the text already delivered stands, and
    /// no `Done` is invented to make the reply look complete.
    #[tokio::test]
    async fn a_stream_cut_mid_frame_ends_without_a_done() {
        // Twenty bytes into the terminal frame: past the blank line that ended
        // the first one, nowhere near the CRLF that would end this one.
        let cut = first_frame_end() + 20;

        let seen = ok(vec![FIXTURE[..cut].to_vec()]).await;

        assert_eq!(seen, [text("hello arc")]);
    }

    /// Cut before any frame completes: nothing at all, and still no `Done`.
    #[tokio::test]
    async fn a_stream_cut_before_its_first_frame_yields_nothing() {
        assert!(ok(vec![b"data: {\"resp".to_vec()]).await.is_empty());
    }

    #[tokio::test]
    async fn a_frame_that_is_not_json_fails_the_stream_once() {
        let mut seen = deltas(vec![b"data: not json at all\n\ndata: {}\n\n".to_vec()]).await;

        let error = seen.pop().expect("an item").expect_err("malformed frame");
        assert!(matches!(error, Error::MalformedStream(_)), "{error:?}");
        // Fused: the frame after the bad one is never reported.
        assert!(seen.is_empty(), "{seen:?}");
    }

    /// JSON that parses but is not a completion response. The same failure, for
    /// the same reason: this layer will not guess what a frame meant.
    #[tokio::test]
    async fn a_frame_without_a_response_fails_the_stream() {
        let seen = deltas(vec![
            br#"data: {"error":{"message":"quota exhausted"}}"#.to_vec(),
            b"\n\n".to_vec(),
        ])
        .await;

        let [Err(Error::MalformedStream(message))] = seen.as_slice() else {
            panic!("expected one malformed-stream error, got {seen:?}");
        };
        // The backend's own words survive into the error a person reads.
        assert!(message.contains("quota exhausted"), "{message}");
    }

    /// Text already delivered arrives before the failure, per the trait's
    /// contract: a mid-stream error does not retract what came before it.
    #[tokio::test]
    async fn text_before_a_bad_frame_is_still_delivered() {
        let mut chunk = FIXTURE[..first_frame_end()].to_vec();
        chunk.extend_from_slice(b"data: {\"nonsense\": true}\r\n\r\n");

        let seen = deltas(vec![chunk]).await;

        assert_eq!(seen.len(), 2, "{seen:?}");
        assert_eq!(seen[0].as_ref().expect("text"), &text("hello arc"));
        assert!(seen[1].is_err(), "{seen:?}");
    }

    /// Empty parts are not empty deltas. The captured terminal frame carries
    /// one; so does a frame with nothing but a thought signature.
    #[tokio::test]
    async fn empty_text_parts_produce_no_deltas() {
        let frame = br#"data: {"response":{"candidates":[{"content":{"parts":[{"text":""},{"thoughtSignature":"sig"},{"text":"real"}]}}]}}"#;
        let mut chunk = frame.to_vec();
        chunk.extend_from_slice(b"\n\n");

        assert_eq!(ok(vec![chunk]).await, [text("real")]);
    }

    /// Several parts in one frame are several deltas, in order: chunk
    /// boundaries are the backend's and concatenation is the caller's.
    #[tokio::test]
    async fn every_non_empty_part_is_its_own_delta() {
        let frame =
            br#"data: {"response":{"candidates":[{"content":{"parts":[{"text":"a"},{"text":"b"}]}}]}}"#;
        let mut chunk = frame.to_vec();
        chunk.extend_from_slice(b"\n\n");

        assert_eq!(ok(vec![chunk]).await, [text("a"), text("b")]);
    }

    /// A terminal frame with no counts still closes the stream. Cost accounting
    /// degrades to the last counts seen; the reply does not.
    #[tokio::test]
    async fn a_terminal_frame_without_usage_reports_what_was_last_seen() {
        let mut chunk = FIXTURE[..first_frame_end()].to_vec();
        chunk.extend_from_slice(br#"data: {"response":{"candidates":[{"finishReason":"STOP"}]}}"#);
        chunk.extend_from_slice(b"\r\n\r\n");

        assert_eq!(
            ok(vec![chunk]).await,
            [
                text("hello arc"),
                CompletionDelta::Done {
                    usage: FIXTURE_USAGE
                },
            ]
        );
    }

    /// The closing trace event: one per stream, carrying how it ended and what
    /// it cost. Everything downstream that counts tokens — DESIGN.md §8's
    /// traces, the daemon's cost lines — reads these fields, so their names and
    /// values are part of the contract, not decoration.
    #[test]
    fn a_finished_stream_records_its_outcome_and_its_cost() {
        let logged = traced(|| block_on(ok(vec![FIXTURE.to_vec()])));

        assert_eq!(logged.matches("outcome=").count(), 1, "{logged}");
        assert!(logged.contains("outcome=\"done\""), "{logged}");
        assert!(logged.contains("provider=\"antigravity\""), "{logged}");
        assert!(logged.contains("input_tokens=11"), "{logged}");
        assert!(logged.contains("output_tokens=75"), "{logged}");
    }

    /// A cut stream says so, and still reports the counts the backend sent
    /// before it died.
    #[test]
    fn a_cut_stream_records_the_counts_it_did_receive() {
        let cut = first_frame_end() + 20;
        let logged = traced(|| block_on(ok(vec![FIXTURE[..cut].to_vec()])));

        assert!(logged.contains("outcome=\"cut\""), "{logged}");
        assert!(logged.contains("input_tokens=11"), "{logged}");
    }

    #[test]
    fn a_failed_stream_records_the_error_that_ended_it() {
        let logged = traced(|| block_on(deltas(vec![b"data: nonsense\r\n\r\n".to_vec()])));

        assert!(logged.contains("outcome=\"error\""), "{logged}");
        assert!(logged.contains("malformed provider stream"), "{logged}");
    }

    /// Nothing after the terminal frame is read — the connection is dropped
    /// there, and a backend that kept talking would not be listened to.
    #[tokio::test]
    async fn frames_after_the_terminal_one_are_ignored() {
        let mut chunk = FIXTURE.to_vec();
        chunk.extend_from_slice(b"data: still talking\r\n\r\n");

        assert_eq!(
            ok(vec![chunk]).await,
            [
                text("hello arc"),
                CompletionDelta::Done {
                    usage: FIXTURE_USAGE
                },
            ]
        );
    }
}
