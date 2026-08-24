use serde::Deserialize;

use crate::provider::stream::{Deltas, FrameParser};
use crate::provider::{CompletionDelta, Error, Stop, ToolCall, Usage};

const DONE: &str = "[DONE]";

const TOOL_CALLS: &str = "tool_calls";

#[derive(Default)]
pub(super) struct Parser {
    building: Vec<Building>,

    stop: Option<Stop>,
}

impl FrameParser for Parser {
    const PROVIDER: &'static str = "gemini";

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
        for choice in choices {
            if let Some(content) = choice.delta.content.filter(|it| !it.is_empty()) {
                items.push(CompletionDelta::Text(content));
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
    // Gemini omits `index`, so arrival order is the only ordering there is
    fn accumulate(&mut self, call: ToolCallJson) -> Result<(), Error> {
        let function = call.function.unwrap_or_default();
        let fragment = function.arguments.unwrap_or_default();

        let Some(name) = function.name else {
            let Some(building) = self.building.last_mut() else {
                return Err(Error::MalformedStream(
                    "the endpoint continued a tool call that never opened".to_owned(),
                ));
            };
            building.arguments.push_str(&fragment);
            return Ok(());
        };

        self.building.push(Building {
            id: call.id.unwrap_or_default(),
            name,
            arguments: fragment,
            signature: call
                .extra_content
                .and_then(|extra| extra.google)
                .and_then(|google| google.thought_signature)
                .unwrap_or_default(),
        });
        Ok(())
    }

    fn close(&mut self, reason: &str) -> Result<Vec<CompletionDelta>, Error> {
        let calls: Vec<CompletionDelta> = std::mem::take(&mut self.building)
            .into_iter()
            .enumerate()
            .map(|(index, building)| {
                let index = u32::try_from(index).unwrap_or(u32::MAX);
                building.finish(index).map(CompletionDelta::ToolCall)
            })
            .collect::<Result<_, _>>()?;

        // Gemini says "stop" even when it is asking for tools
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
    signature: String,
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
            // the base64 text goes back verbatim; decoding it would only add a way to corrupt it
            provider_roundtrip: self.signature.into_bytes(),
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

    #[serde(default)]
    tool_calls: Vec<ToolCallJson>,
}

#[derive(Deserialize)]
struct ToolCallJson {
    id: Option<String>,
    function: Option<FunctionJson>,
    extra_content: Option<ExtraContent>,
}

#[derive(Deserialize)]
struct ExtraContent {
    google: Option<Google>,
}

#[derive(Deserialize)]
struct Google {
    thought_signature: Option<String>,
}

#[derive(Default, Deserialize)]
struct FunctionJson {
    name: Option<String>,
    arguments: Option<String>,
}

#[derive(Deserialize)]
struct UsageJson {
    #[serde(default, rename = "prompt_tokens")]
    prompt: u32,
    #[serde(default, rename = "completion_tokens")]
    completion: u32,
    #[serde(default, rename = "total_tokens")]
    total: u32,
}

impl UsageJson {
    fn usage(&self) -> Usage {
        // thinking is billed but never streamed, so completion_tokens under-reports
        // output by about five times; the total is the only figure that counts it
        let output = self.total.saturating_sub(self.prompt);
        Usage {
            input_tokens: self.prompt,
            output_tokens: output.max(self.completion),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Parser;
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

    #[tokio::test]
    async fn a_plain_turn_yields_text_and_counts_thinking_as_output() {
        let deltas = drain("gemini_stream.sse").await;

        let text: String = deltas
            .iter()
            .filter_map(|delta| match delta {
                CompletionDelta::Text(chunk) => Some(chunk.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, "hello there");

        let Some(CompletionDelta::Done { usage, stop }) = deltas.last() else {
            panic!("the stream must end with Done, got {:?}", deltas.last());
        };
        assert_eq!(*stop, Stop::EndTurn);
        assert_eq!(
            *usage,
            Usage {
                input_tokens: 10,
                // 77 total against 10 prompt: completion_tokens said 2
                output_tokens: 67,
            }
        );
    }

    #[tokio::test]
    async fn a_tool_call_with_no_index_still_gets_one_and_keeps_its_signature() {
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
        assert_eq!(calls[0].arguments, r#"{"timezone":"UTC"}"#);
        assert!(
            !calls[0].provider_roundtrip.is_empty(),
            "the thought signature must survive or the next turn is a 400"
        );

        let Some(CompletionDelta::Done { stop, .. }) = deltas.last() else {
            panic!("the stream must end with Done");
        };
        assert_eq!(
            *stop,
            Stop::ToolCalls,
            "finish_reason says stop, but tool calls mean tool calls"
        );
    }

    #[tokio::test]
    async fn the_turn_after_a_tool_result_reads_back_as_a_plain_answer() {
        let deltas = drain("gemini_tool_result_stream.sse").await;

        let text: String = deltas
            .iter()
            .filter_map(|delta| match delta {
                CompletionDelta::Text(chunk) => Some(chunk.as_str()),
                _ => None,
            })
            .collect();
        assert!(text.contains("14:00:00"), "{text}");

        let Some(CompletionDelta::Done { stop, .. }) = deltas.last() else {
            panic!("the stream must end with Done");
        };
        assert_eq!(*stop, Stop::EndTurn);
    }

    #[test]
    fn a_frame_that_is_not_a_chunk_is_a_malformed_stream() {
        let Err(err) = Parser::default().frame("{\"not\":\"a chunk\"}") else {
            panic!("a frame with neither choices nor usage is malformed");
        };
        assert!(err.to_string().contains("not a completion chunk"), "{err}");
    }
}
