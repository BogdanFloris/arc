use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tracing::Span;

use crate::provider::sse::FrameDecoder;
use crate::provider::{CompletionDelta, CompletionStream, Error, Stop, Usage};

pub(crate) struct Deltas {
    pub items: Vec<CompletionDelta>,

    pub usage: Option<Usage>,

    pub finished: Option<Stop>,
}

pub(crate) trait FrameParser: Send {
    const PROVIDER: &'static str;

    fn frame(&mut self, payload: &str) -> Result<Deltas, Error>;
}

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

pub(crate) struct DeltaStream<S, P> {
    bytes: S,

    frames: FrameDecoder,

    parser: P,

    pending: VecDeque<CompletionDelta>,

    usage: Option<Usage>,

    finished: bool,

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

    fn decode_frames(&mut self) -> Result<bool, Error> {
        while self.pending.is_empty() {
            let Some(payload) = self.frames.next_frame() else {
                return Ok(false);
            };
            let decoded = self.parser.frame(&payload)?;

            if let Some(usage) = decoded.usage {
                self.usage = Some(usage);
            }
            self.pending.extend(decoded.items);
            if let Some(stop) = decoded.finished {
                self.pending.push_back(match self.usage {
                    Some(usage) => CompletionDelta::Done { usage, stop },
                    None => CompletionDelta::UnmeasuredDone { stop },
                });
            }
        }
        Ok(true)
    }

    fn end(&mut self, outcome: Outcome) {
        self.finished = true;
        let usage = self.usage.unwrap_or_default();
        let input = usage.input_tokens;
        let output = usage.output_tokens;
        let provider = P::PROVIDER;

        self.span.in_scope(|| match outcome {
            Outcome::Done => tracing::info!(
                provider,
                outcome = "done",
                counter.input_tokens = input,
                counter.output_tokens = output,
                "completion finished"
            ),
            Outcome::Cut => tracing::warn!(
                provider,
                outcome = "cut",
                counter.input_tokens = input,
                counter.output_tokens = output,
                "stream ended before the model finished"
            ),
            Outcome::Failed(error) => tracing::warn!(
                provider,
                outcome = "error",
                counter.input_tokens = input,
                counter.output_tokens = output,
                error = %error,
                "stream failed"
            ),
        });
    }

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
        let this = self.get_mut();

        loop {
            if let Some(delta) = this.pending.pop_front() {
                if matches!(
                    delta,
                    CompletionDelta::Done { .. } | CompletionDelta::UnmeasuredDone { .. }
                ) {
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
                    this.end(Outcome::Cut);
                    return Poll::Ready(None);
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Outcome<'a> {
    Done,

    Cut,

    Failed(&'a Error),
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, stream};

    use super::*;

    struct Parser;

    impl FrameParser for Parser {
        const PROVIDER: &'static str = "test";

        fn frame(&mut self, payload: &str) -> Result<Deltas, Error> {
            Ok(Deltas {
                items: Vec::new(),
                usage: match payload {
                    "zero" => Some(Usage::default()),
                    "usage" => Some(Usage {
                        input_tokens: 12,
                        output_tokens: 3,
                    }),
                    _ => None,
                },
                finished: match payload {
                    "done" => Some(Stop::EndTurn),
                    "tools" => Some(Stop::ToolCalls),
                    _ => None,
                },
            })
        }
    }

    async fn drain(body: &str) -> Vec<CompletionDelta> {
        DeltaStream::new(stream::iter([Ok(body.as_bytes())]), Parser, Span::none())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[tokio::test]
    async fn missing_usage_finishes_once_with_the_original_stop_reason() {
        for (frame, stop) in [("done", Stop::EndTurn), ("tools", Stop::ToolCalls)] {
            assert_eq!(
                drain(&format!("data: {frame}\n\ndata: usage\n\ndata: done\n\n")).await,
                [CompletionDelta::UnmeasuredDone { stop }]
            );
        }
    }

    #[tokio::test]
    async fn explicitly_reported_zero_usage_is_still_measured() {
        assert_eq!(
            drain("data: zero\n\ndata: done\n\n").await,
            [CompletionDelta::Done {
                usage: Usage::default(),
                stop: Stop::EndTurn,
            }]
        );
    }

    #[tokio::test]
    async fn usage_from_an_earlier_frame_survives_an_unmeasured_final_frame() {
        assert_eq!(
            drain("data: usage\n\ndata: done\n\n").await,
            [CompletionDelta::Done {
                usage: Usage {
                    input_tokens: 12,
                    output_tokens: 3,
                },
                stop: Stop::EndTurn,
            }]
        );
    }
}
