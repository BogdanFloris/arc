//! The streaming state machine every SSE provider shares: payloads in,
//! deltas out.
//!
//! [`sse`](crate::provider::sse) turns bytes into `data:` payloads without
//! knowing what one says; a [`FrameParser`] reads what one says without
//! knowing how it arrived; [`DeltaStream`] is the join. Extracted from the
//! Antigravity implementation when the OpenAI-compatible provider needed the
//! same machine (task 7.1): backends differ in frame dialect, never in how a
//! stream is driven, cut, or fused.
//!
//! # Where a stream can stop
//!
//! Three endings, and the caller can tell them apart:
//!
//! - The terminal frame arrives: its text, then [`CompletionDelta::Done`],
//!   then the end of the stream. Whatever the connection does afterwards is
//!   its own business — dropping it here is fine.
//! - The bytes stop first: the stream ends with no `Done`. That absence is
//!   the partial-reply signal
//!   [`Provider::complete`](crate::provider::Provider::complete) documents, so
//!   nothing here invents a `Done` to tidy it up. The text already delivered
//!   was really generated; a synthesized `Done` would tell the caller a
//!   truncated reply was complete.
//! - A payload does not parse: one error from the parser, then the end. The
//!   stream fuses there because the framing state after garbage is not
//!   trustworthy — a payload that is not the expected shape means either the
//!   backend changed or the bytes are damaged, and reading on would be
//!   guessing which.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tracing::Span;

use crate::provider::sse::FrameDecoder;
use crate::provider::{CompletionDelta, CompletionStream, Error, Usage};

/// What one frame means for the stream.
pub(crate) struct Deltas {
    /// Non-empty text chunks, in the order the frame carried them.
    pub text: Vec<String>,

    /// Counts this frame reported, if it reported any.
    pub usage: Option<Usage>,

    /// Whether this frame ends the reply.
    pub finished: bool,
}

/// A backend's frame dialect: what one `data:` payload says.
pub(crate) trait FrameParser: Send {
    /// Short name for trace events. Matches the provider's
    /// [`name`](crate::provider::Provider::name).
    const PROVIDER: &'static str;

    /// Reads one payload.
    ///
    /// # Errors
    ///
    /// Whatever the dialect cannot read — the error is yielded to the caller
    /// once and the stream is fused there.
    fn frame(&mut self, payload: &str) -> Result<Deltas, Error>;
}

/// Wraps a started response as the stream of deltas it decodes to.
///
/// The response body is boxed on the way in so the decoder does not need a pin
/// projection to poll it. That is one allocation on a path that already made a
/// network round trip.
pub(crate) fn deltas<P>(response: reqwest::Response, parser: P, span: Span) -> CompletionStream
where
    P: FrameParser + Unpin + 'static,
{
    Box::pin(DeltaStream::new(
        Box::pin(response.bytes_stream()),
        parser,
        span,
    ))
}

/// A completion in flight: response bytes in, [`CompletionDelta`]s out.
///
/// A hand-written state machine rather than a combinator chain, because the
/// interesting states — deltas queued from one frame, terminal frame seen,
/// fused after a failure — are what the tests drive, and they are easier to
/// reason about named than folded into an accumulator.
pub(crate) struct DeltaStream<S, P> {
    /// The response body, in whatever chunks it arrives.
    bytes: S,

    /// Byte-level framing. Holds any partial frame between chunks.
    frames: FrameDecoder,

    /// The backend's dialect.
    parser: P,

    /// Deltas decoded from frames already read, oldest first. One frame can
    /// carry several text parts and a `Done`, and the caller takes one item at
    /// a time.
    pending: VecDeque<CompletionDelta>,

    /// The most recent counts the backend reported. Kept for the closing trace
    /// event even when the stream is cut before it finishes.
    usage: Option<Usage>,

    /// Set at every ending. A fused stream yields `None` forever after.
    finished: bool,

    /// The completion's span, entered only to record the closing event.
    /// Holding it keeps the span open for as long as the completion actually
    /// runs, which is until this stream is dropped.
    span: Span,
}

impl<S, P: FrameParser> DeltaStream<S, P> {
    pub(crate) fn new(bytes: S, parser: P, span: Span) -> Self {
        Self {
            bytes,
            frames: FrameDecoder::new(),
            parser,
            pending: VecDeque::new(),
            usage: None,
            finished: false,
            span,
        }
    }

    /// Decodes frames into [`Self::pending`] until something is queued or the
    /// decoder runs out, reporting which.
    ///
    /// One frame at a time, and only while nothing is queued. That ordering is
    /// what puts a bad frame's error *after* the text of the frames before it:
    /// decoding ahead would raise the error while earlier text was still
    /// waiting, and the caller would lose text the model really generated.
    fn decode_frames(&mut self) -> Result<bool, Error> {
        while self.pending.is_empty() {
            let Some(payload) = self.frames.next_frame() else {
                return Ok(false);
            };
            let decoded = self.parser.frame(&payload)?;

            if let Some(usage) = decoded.usage {
                self.usage = Some(usage);
            }
            for text in decoded.text {
                self.pending.push_back(CompletionDelta::Text(text));
            }
            if decoded.finished {
                self.pending.push_back(CompletionDelta::Done {
                    usage: self.usage.unwrap_or_default(),
                });
            }
        }
        Ok(true)
    }

    /// Records how the stream ended, once, and fuses it.
    ///
    /// This is the closing half of the LLM-call trace DESIGN.md §8 asks for:
    /// the request span covers everything up to the first byte, and a
    /// completion is not done until its last one. Token counts are the last
    /// the backend reported; zero means it never reported any, which happens
    /// only when a stream dies before its first counted frame.
    fn end(&mut self, outcome: Outcome) {
        self.finished = true;
        let usage = self.usage.unwrap_or_default();
        let input_tokens = usage.input_tokens;
        let output_tokens = usage.output_tokens;
        let provider = P::PROVIDER;

        self.span.in_scope(|| match outcome {
            Outcome::Done => tracing::info!(
                provider,
                outcome = "done",
                input_tokens,
                output_tokens,
                "completion finished"
            ),
            Outcome::Cut => tracing::warn!(
                provider,
                outcome = "cut",
                input_tokens,
                output_tokens,
                "stream ended before the model finished"
            ),
            Outcome::Failed(error) => tracing::warn!(
                provider,
                outcome = "error",
                input_tokens,
                output_tokens,
                error = %error,
                "stream failed"
            ),
        });
    }

    /// Ends the stream with an error, and yields it as the last item.
    fn fail(&mut self, error: Error) -> Poll<Option<Result<CompletionDelta, Error>>> {
        self.end(Outcome::Failed(&error));
        Poll::Ready(Some(Err(error)))
    }
}

impl<S, B, P> Stream for DeltaStream<S, P>
where
    S: Stream<Item = Result<B, reqwest::Error>> + Unpin,
    B: AsRef<[u8]>,
    P: FrameParser + Unpin,
{
    type Item = Result<CompletionDelta, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Every field is `Unpin`, so the stream is too and the body can be
        // polled through a fresh `Pin` each time.
        let this = self.get_mut();

        loop {
            if let Some(delta) = this.pending.pop_front() {
                if matches!(delta, CompletionDelta::Done { .. }) {
                    this.end(Outcome::Done);
                }
                return Poll::Ready(Some(Ok(delta)));
            }
            if this.finished {
                return Poll::Ready(None);
            }

            match this.decode_frames() {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => return this.fail(error),
            }

            match Pin::new(&mut this.bytes).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(chunk))) => this.frames.push(chunk.as_ref()),
                Poll::Ready(Some(Err(error))) => return this.fail(Error::Transport(error)),
                Poll::Ready(None) => {
                    // A frame still arriving is not a frame: whatever the
                    // decoder is holding is dropped with it.
                    this.end(Outcome::Cut);
                    return Poll::Ready(None);
                }
            }
        }
    }
}

/// How a stream ended, for the closing trace event.
#[derive(Clone, Copy)]
enum Outcome<'a> {
    /// The terminal frame arrived and `Done` was delivered.
    Done,

    /// The body ended with no terminal frame: a partial reply.
    Cut,

    /// A frame did not parse, or the connection broke mid-stream.
    Failed(&'a Error),
}
