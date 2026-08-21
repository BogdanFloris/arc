//! The integration-test spine: a scripted provider driving the real engine
//! over a real log and projection.
//!
//! Everything here is promoted from `session.rs`'s tests so the archive and
//! memory tiers can assert across the whole chain — engine events, log bytes,
//! replayed projection, rebuilt transcript — without re-growing their own
//! harness. Test-only: the module exists behind `#[cfg(test)]` in `lib.rs`.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arc_proto::v1::{Role, session_event};
use futures::stream;
use tempfile::TempDir;
use tokio::sync::mpsc;

use crate::log::{Log, LogReader, discover_segments};
use crate::projection::Projection;
use crate::provider::{
    CompletionDelta, CompletionRequest, CompletionStream, Error as ProviderError, Message,
    Provider, Stop, ToolCall, ToolDefinition, Usage,
};
use crate::session::{Engine, EngineEvent};
use crate::tool::{Registry, Tool, ToolReply};

/// A scripted provider: each `complete` call captures its request and
/// yields the next script entry.
pub(crate) struct ScriptedProvider {
    script: Mutex<VecDeque<Vec<Result<CompletionDelta, ProviderError>>>>,
    captured: Mutex<Vec<CompletionRequest>>,
}

impl ScriptedProvider {
    pub(crate) fn scripted(calls: Vec<Vec<Result<CompletionDelta, ProviderError>>>) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(calls.into()),
            captured: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn requests(&self) -> Vec<CompletionRequest> {
        self.captured.lock().expect("captured").clone()
    }
}

impl Provider for ScriptedProvider {
    fn name(&self) -> &'static str {
        "scripted"
    }

    async fn complete(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionStream, ProviderError> {
        self.captured.lock().expect("captured").push(request);
        let items = self
            .script
            .lock()
            .expect("script")
            .pop_front()
            .expect("script exhausted");
        Ok(Box::pin(stream::iter(items)))
    }
}

pub(crate) fn usage() -> Usage {
    Usage {
        input_tokens: 3,
        output_tokens: 5,
    }
}

pub(crate) fn done_reply(text: &str) -> Vec<Result<CompletionDelta, ProviderError>> {
    vec![
        Ok(CompletionDelta::Text(text.to_owned())),
        Ok(CompletionDelta::Done {
            usage: usage(),
            stop: Stop::EndTurn,
        }),
    ]
}

/// The role and text of a history message. Everything this engine sends is
/// text; a tool message here is a bug in the test.
pub(crate) fn turn(message: &Message) -> (Role, &str) {
    match message {
        Message::Text { role, content } => (*role, content.as_str()),
        other => panic!("expected a text message, got {other:?}"),
    }
}

/// An engine over a fresh log and in-memory index.
pub(crate) fn engine(provider: &Arc<ScriptedProvider>, dir: &TempDir) -> Engine<ScriptedProvider> {
    let log = Log::open(dir.path()).expect("open log");
    let projection = Projection::open(":memory:").expect("open projection");
    Engine::new(
        log,
        projection,
        Arc::clone(provider),
        "test-model",
        Some("be terse".to_owned()),
        Registry::new(512),
        false,
    )
}

/// An engine whose model may call tools, with `/no_think` off so system
/// prompt assertions stay simple.
pub(crate) fn engine_with_tools(
    provider: &Arc<ScriptedProvider>,
    dir: &TempDir,
    registry: Registry,
) -> Engine<ScriptedProvider> {
    let log = Log::open(dir.path()).expect("open log");
    let projection = Projection::open(":memory:").expect("open projection");
    Engine::new(
        log,
        projection,
        Arc::clone(provider),
        "test-model",
        Some("be terse".to_owned()),
        registry,
        false,
    )
}

/// An engine reopened over `dir`'s existing log with a fresh projection
/// replayed from its bytes — the way a restarted daemon starts. Drop the
/// previous engine first; two writers on one log is not a thing.
pub(crate) fn reopened_engine(
    provider: &Arc<ScriptedProvider>,
    dir: &TempDir,
    registry: Registry,
) -> Engine<ScriptedProvider> {
    let log = Log::open(dir.path()).expect("reopen log");
    let mut projection = Projection::open(":memory:").expect("open projection");
    crate::projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
    Engine::new(
        log,
        projection,
        Arc::clone(provider),
        "test-model",
        Some("be terse".to_owned()),
        registry,
        false,
    )
}

/// Appends raw session events to `dir`'s log, for histories a live engine
/// cannot produce — results in completion order, foreign roles, orphans.
pub(crate) fn seed_log(dir: &TempDir, events: Vec<session_event::Event>) {
    let mut log = Log::open(dir.path()).expect("open log");
    for payload in events {
        log.append(arc_proto::v1::Event {
            seq: 0, // added by the log
            ts: None,
            source: arc_proto::v1::Source::System as i32,
            payload: Some(arc_proto::v1::event::Payload::Session(
                arc_proto::v1::SessionEvent {
                    event: Some(payload),
                },
            )),
        })
        .expect("append");
    }
}

pub(crate) fn channel() -> (mpsc::Sender<EngineEvent>, mpsc::Receiver<EngineEvent>) {
    mpsc::channel(64)
}

pub(crate) fn drain(rx: &mut mpsc::Receiver<EngineEvent>) -> Vec<EngineEvent> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

/// Every event in `dir`'s log, replayed through the real reader.
pub(crate) fn replay_log(dir: impl AsRef<std::path::Path>) -> Vec<session_event::Event> {
    let segments = discover_segments(dir.as_ref()).expect("discover");
    LogReader::new(segments)
        .map(|result| {
            let event = result.expect("replay");
            match event.payload.expect("payload") {
                arc_proto::v1::event::Payload::Session(session) => {
                    session.event.expect("session event")
                }
                other @ arc_proto::v1::event::Payload::Memory(_) => {
                    panic!("expected a session event, got {other:?}")
                }
            }
        })
        .collect()
}

pub(crate) fn appended(event: &session_event::Event) -> &arc_proto::v1::MessageAppended {
    match event {
        session_event::Event::MessageAppended(m) => m,
        other => panic!("expected a message, got {other:?}"),
    }
}

pub(crate) fn issued(event: &session_event::Event) -> &arc_proto::v1::ToolCallIssued {
    match event {
        session_event::Event::ToolCallIssued(c) => c,
        other => panic!("expected a tool call, got {other:?}"),
    }
}

pub(crate) fn resulted(event: &session_event::Event) -> &arc_proto::v1::ToolResultRecorded {
    match event {
        session_event::Event::ToolResultRecorded(r) => r,
        other => panic!("expected a tool result, got {other:?}"),
    }
}

/// A test tool with a fixed reply.
pub(crate) struct Canned {
    pub(crate) name: &'static str,
    pub(crate) content: &'static str,
    pub(crate) ok: bool,
}

impl Tool for Canned {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name.to_owned(),
            description: String::new(),
            parameters: serde_json::json!({"type": "object"}),
        }
    }

    fn execute(
        &self,
        _arguments_json: String,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        let reply = ToolReply {
            content: self.content.to_owned(),
            ok: self.ok,
        };
        Box::pin(async move { reply })
    }
}

/// A registry of [`Canned`] tools: `(name, reply, ok)` each.
pub(crate) fn tools(entries: &[(&'static str, &'static str, bool)]) -> Registry {
    let mut registry = Registry::new(512);
    for &(name, content, ok) in entries {
        registry.register(Box::new(Canned { name, content, ok }));
    }
    registry
}

pub(crate) fn call(id: &str, index: u32, name: &str, args: &str) -> CompletionDelta {
    CompletionDelta::ToolCall(ToolCall {
        id: id.to_owned(),
        index,
        name: name.to_owned(),
        arguments: args.to_owned(),
    })
}

pub(crate) fn tool_stop() -> CompletionDelta {
    CompletionDelta::Done {
        usage: usage(),
        stop: Stop::ToolCalls,
    }
}

#[cfg(test)]
mod tests {
    use arc_proto::v1::{Role, ToolOutcome, session_event};
    use tempfile::TempDir;

    use super::{
        ScriptedProvider, appended, call, channel, done_reply, drain, engine_with_tools, issued,
        replay_log, resulted, tool_stop, tools,
    };
    use crate::log::{LogReader, discover_segments};
    use crate::projection::{self, MessageRow, Projection};
    use crate::provider::{Message, ToolCall};
    use crate::session::EngineEvent;

    /// The template for the archive- and memory-tier tests: one tool turn,
    /// asserted across the whole chain — engine events, log bytes, a fresh
    /// projection replay, and the rebuilt provider transcript.
    // One turn across the whole chain is the template's point; splitting it
    // would hide the chain.
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn a_tool_turn_holds_up_across_the_whole_chain() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "lookup", r#"{"q":1}"#)), Ok(tool_stop())],
            done_reply("final text"),
            done_reply("second reply"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine_with_tools(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, mut rx) = channel();

        let reply = engine
            .send_message(None, "question", tx)
            .await
            .expect("send");

        // (a) the engine's event stream saw the call, its result, the text.
        assert_eq!(
            drain(&mut rx),
            [
                EngineEvent::Accepted {
                    session_id: reply.session_id.clone()
                },
                EngineEvent::ToolCallStarted {
                    call_id: "c1".to_owned(),
                    index: 0,
                    name: "lookup".to_owned(),
                },
                EngineEvent::ToolCallEnded {
                    call_id: "c1".to_owned(),
                    outcome: ToolOutcome::Ok,
                },
                EngineEvent::Delta("final text".to_owned()),
            ]
        );

        // (b) the log replays through the real reader to the expected sequence.
        let logged = replay_log(&dir);
        assert_eq!(logged.len(), 5);
        let session_event::Event::SessionCreated(created) = &logged[0] else {
            panic!("expected SessionCreated first, got {:?}", logged[0]);
        };
        assert_eq!(created.session_id, reply.session_id);
        let user = appended(&logged[1]);
        assert_eq!(
            (user.role, user.content.as_str()),
            (Role::User as i32, "question")
        );
        let issued_call = issued(&logged[2]);
        assert_eq!(
            (issued_call.call_id.as_str(), issued_call.name.as_str()),
            ("c1", "lookup")
        );
        let result = resulted(&logged[3]);
        assert_eq!(
            (result.call_id.as_str(), result.content.as_str()),
            ("c1", "found it")
        );
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        let assistant = appended(&logged[4]);
        assert_eq!(
            (assistant.role, assistant.content.as_str()),
            (Role::Assistant as i32, "final text")
        );

        // (c) a fresh projection replayed from those bytes holds every row,
        // tool rows included, typed and in seq order.
        let mut fresh = Projection::open(":memory:").expect("open projection");
        let segments = discover_segments(dir.path()).expect("discover");
        let stats = projection::replay(LogReader::new(segments), &mut fresh).expect("replay");
        assert_eq!(stats.applied, 5);
        let sessions = fresh.sessions().expect("sessions");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, reply.session_id);
        let turn_id = user.turn_id.clone();
        assert_eq!(
            fresh.messages(&reply.session_id).expect("messages"),
            [
                MessageRow::Message {
                    role: Role::User as i32,
                    content: "question".to_owned(),
                    partial: false,
                    turn_id: turn_id.clone(),
                },
                MessageRow::ToolCall {
                    call_id: "c1".to_owned(),
                    call_index: 0,
                    name: "lookup".to_owned(),
                    arguments_json: r#"{"q":1}"#.to_owned(),
                    turn_id: turn_id.clone(),
                },
                MessageRow::ToolResult {
                    call_id: "c1".to_owned(),
                    outcome: ToolOutcome::Ok as i32,
                    content: "found it".to_owned(),
                    truncated: false,
                    turn_id: turn_id.clone(),
                },
                MessageRow::Message {
                    role: Role::Assistant as i32,
                    content: "final text".to_owned(),
                    partial: false,
                    turn_id,
                },
            ]
        );

        // (d) the rebuilt provider transcript — observed as the next turn's
        // request — carries the whole tool step back to the model: the call,
        // its result, and the ids unchanged (DESIGN.md §3.1).
        let (tx, _rx) = channel();
        engine
            .send_message(Some(&reply.session_id), "again", tx)
            .await
            .expect("second send");
        let requests = provider.requests();
        assert_eq!(
            requests[2].messages,
            [
                Message::Text {
                    role: Role::User,
                    content: "question".to_owned(),
                },
                Message::ToolCalls(vec![ToolCall {
                    id: "c1".to_owned(),
                    index: 0,
                    name: "lookup".to_owned(),
                    arguments: r#"{"q":1}"#.to_owned(),
                }]),
                Message::ToolResult {
                    call_id: "c1".to_owned(),
                    content: "found it".to_owned(),
                },
                Message::Text {
                    role: Role::Assistant,
                    content: "final text".to_owned(),
                },
                Message::Text {
                    role: Role::User,
                    content: "again".to_owned(),
                },
            ]
        );
    }
}
