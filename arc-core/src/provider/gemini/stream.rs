use serde::Deserialize;

use crate::provider::stream::{Deltas, FrameParser};
use crate::provider::{CompletionDelta, Error, Stop, ToolCall, Usage};

#[derive(Default)]
pub(super) struct Parser {
    calls: u32,
}

impl FrameParser for Parser {
    const PROVIDER: &'static str = "gemini";

    fn frame(&mut self, payload: &str) -> Result<Deltas, Error> {
        let chunk: Chunk = serde_json::from_str(payload).map_err(|source| {
            Error::MalformedStream(format!(
                "the endpoint sent a frame that is not a completion chunk: {source}: {}",
                crate::provider::snippet(payload)
            ))
        })?;

        let mut items = Vec::new();
        let mut finished = None;
        for candidate in chunk.candidates {
            for part in candidate.content.into_iter().flat_map(|it| it.parts) {
                items.extend(self.part(part)?);
            }
            if candidate.finish_reason.is_some() {
                finished = Some(if self.calls > 0 {
                    Stop::ToolCalls
                } else {
                    Stop::EndTurn
                });
            }
        }

        Ok(Deltas {
            items,
            usage: chunk.usage_metadata.as_ref().map(UsageJson::usage),
            finished,
        })
    }
}

impl Parser {
    fn part(&mut self, part: PartJson) -> Result<Option<CompletionDelta>, Error> {
        if let Some(call) = part.function_call {
            let arguments = serde_json::to_string(&call.args).map_err(|source| {
                Error::MalformedStream(format!(
                    "the endpoint's arguments for tool call `{}` are not JSON: {source}",
                    call.name
                ))
            })?;
            let index = self.calls;
            self.calls += 1;
            return Ok(Some(CompletionDelta::ToolCall(ToolCall {
                id: call.id.unwrap_or_default(),
                index,
                name: call.name,
                arguments,
                // base64 text back verbatim; the next turn is a 400 without it
                provider_roundtrip: part.thought_signature.unwrap_or_default().into_bytes(),
            })));
        }
        // a signature-only part closes a text turn; only calls have to echo one
        Ok(part
            .text
            .filter(|text| !text.is_empty())
            .map(CompletionDelta::Text))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Chunk {
    #[serde(default)]
    candidates: Vec<Candidate>,
    usage_metadata: Option<UsageJson>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Candidate {
    content: Option<ContentJson>,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct ContentJson {
    #[serde(default)]
    parts: Vec<PartJson>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PartJson {
    text: Option<String>,
    function_call: Option<FunctionCallJson>,
    thought_signature: Option<String>,
}

#[derive(Deserialize)]
struct FunctionCallJson {
    name: String,
    id: Option<String>,
    #[serde(default)]
    args: serde_json::Value,
}

#[derive(Deserialize)]
struct UsageJson {
    #[serde(default, rename = "promptTokenCount")]
    prompt: u32,
    #[serde(default, rename = "candidatesTokenCount")]
    answer: u32,
    // thinking is billed and never streamed; the native API at least counts it
    #[serde(default, rename = "thoughtsTokenCount")]
    thoughts: u32,
}

impl UsageJson {
    fn usage(&self) -> Usage {
        Usage {
            input_tokens: self.prompt,
            output_tokens: self.answer.saturating_add(self.thoughts),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Parser, UsageJson};
    use crate::provider::stream::{DeltaStream, FrameParser as _};
    use crate::provider::{CompletionDelta, Stop, Usage};
    use futures::StreamExt as _;

    async fn drain(fixture: &str) -> Vec<CompletionDelta> {
        let bytes = std::fs::read(format!("tests/fixtures/{fixture}")).expect("fixture");
        let chunks = futures::stream::iter(vec![Ok::<_, reqwest::Error>(bytes)]);
        DeltaStream::new(chunks, Parser::default(), tracing::Span::none())
            .map(|item| item.expect("the fixture parses"))
            .collect()
            .await
    }

    fn text(deltas: &[CompletionDelta]) -> String {
        deltas
            .iter()
            .filter_map(|delta| match delta {
                CompletionDelta::Text(chunk) => Some(chunk.as_str()),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_plain_turn_ends_on_finish_reason_with_no_done_sentinel() {
        let deltas = drain("gemini_stream.sse").await;

        assert_eq!(text(&deltas), "hello there");
        let Some(CompletionDelta::Done { usage, stop }) = deltas.last() else {
            panic!("the native stream has no [DONE]; finishReason ends it");
        };
        assert_eq!(*stop, Stop::EndTurn);
        assert_eq!(
            *usage,
            Usage {
                input_tokens: 9,
                output_tokens: 2,
            },
            "minimal thinking reports no thoughts, so output is just the answer"
        );
    }

    #[tokio::test]
    async fn a_tool_call_arrives_whole_and_keeps_its_signature() {
        let deltas = drain("gemini_tool_call_stream.sse").await;

        let calls: Vec<_> = deltas
            .iter()
            .filter_map(|delta| match delta {
                CompletionDelta::ToolCall(call) => Some(call),
                _ => None,
            })
            .collect();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "get_time");
        assert_eq!(calls[0].index, 0, "arrival order supplies the index");
        assert_eq!(
            calls[0].arguments, r#"{"timezone":"UTC"}"#,
            "args arrive as an object and go on as the string the log stores"
        );
        assert!(
            !calls[0].provider_roundtrip.is_empty(),
            "without the signature the next turn is a 400"
        );

        let Some(CompletionDelta::Done { stop, .. }) = deltas.last() else {
            panic!("the stream must end with Done");
        };
        assert_eq!(
            *stop,
            Stop::ToolCalls,
            "finishReason says STOP, but a call means tool calls"
        );
    }

    #[tokio::test]
    async fn the_turn_after_a_tool_result_reads_back_as_a_plain_answer() {
        let deltas = drain("gemini_tool_result_stream.sse").await;

        assert!(text(&deltas).contains("14:00"), "{}", text(&deltas));
        let Some(CompletionDelta::Done { stop, .. }) = deltas.last() else {
            panic!("the stream must end with Done");
        };
        assert_eq!(*stop, Stop::EndTurn);
    }

    #[test]
    fn thinking_tokens_count_as_output() {
        let usage = UsageJson {
            prompt: 2,
            answer: 9,
            thoughts: 146,
        };

        assert_eq!(
            usage.usage(),
            Usage {
                input_tokens: 2,
                output_tokens: 155,
            }
        );
    }

    #[test]
    fn a_frame_that_is_not_a_chunk_is_a_malformed_stream() {
        let Err(err) = Parser::default().frame("not json at all") else {
            panic!("a frame that is not JSON is malformed");
        };
        assert!(err.to_string().contains("not a completion chunk"), "{err}");
    }
}
