use std::collections::VecDeque;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arc_proto::v1::{Role, SessionRole, memory_event, session_event};
use futures::future::BoxFuture;
use futures::stream;
use tempfile::TempDir;
use tokio::sync::mpsc;

use crate::archive::Archive;
use crate::log::{Log, LogReader, discover_segments};
use crate::projection::Projection;
use crate::provider::{
    CompletionDelta, CompletionRequest, CompletionStream, Error as ProviderError, Message,
    Provider, Stop, Thinking, ToolCall, ToolDefinition, Usage,
};
use crate::session::{Engine, EngineEvent, Runner};
use crate::store::Store;
use crate::tool::{Registry, Tool, ToolReply, ToolSource, TurnContext};

#[derive(Debug)]
pub struct ScriptedProvider {
    script: Mutex<VecDeque<Vec<Result<CompletionDelta, ProviderError>>>>,
    captured: Mutex<Vec<CompletionRequest>>,
}

impl ScriptedProvider {
    pub fn scripted(calls: Vec<Vec<Result<CompletionDelta, ProviderError>>>) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(calls.into()),
            captured: Mutex::new(Vec::new()),
        })
    }

    pub fn requests(&self) -> Vec<CompletionRequest> {
        self.captured.lock().expect("captured").clone()
    }
}

impl Provider for ScriptedProvider {
    fn name(&self) -> &'static str {
        "scripted"
    }

    fn complete(
        &self,
        request: CompletionRequest,
    ) -> BoxFuture<'_, Result<CompletionStream, ProviderError>> {
        self.captured.lock().expect("captured").push(request);
        let items = self
            .script
            .lock()
            .expect("script")
            .pop_front()
            .expect("script exhausted");
        Box::pin(async move { Ok(Box::pin(stream::iter(items)) as CompletionStream) })
    }
}

pub fn usage() -> Usage {
    Usage {
        input_tokens: 3,
        output_tokens: 5,
    }
}

pub fn done_reply(text: &str) -> Vec<Result<CompletionDelta, ProviderError>> {
    vec![
        Ok(CompletionDelta::Text(text.to_owned())),
        Ok(CompletionDelta::Done {
            usage: usage(),
            stop: Stop::EndTurn,
        }),
    ]
}

pub fn turn(message: &Message) -> (Role, &str) {
    match message {
        Message::Text { role, content } => (*role, content.as_str()),
        other => panic!("expected a text message, got {other:?}"),
    }
}

pub fn runner(provider: &Arc<ScriptedProvider>) -> Runner {
    Runner {
        role: SessionRole::Concierge,
        provider: Arc::clone(provider) as Arc<dyn Provider>,
        model: "test-model".to_owned(),
        thinking: Thinking::Default,
        system: Some("be terse".to_owned()),
    }
}

pub fn engine(provider: &Arc<ScriptedProvider>, dir: &TempDir) -> (Engine, Runner) {
    engine_with_tools(provider, dir, Registry::new(512))
}

pub fn engine_with_tools(
    provider: &Arc<ScriptedProvider>,
    dir: &TempDir,
    registry: Registry,
) -> (Engine, Runner) {
    let log = Log::open(dir.path()).expect("open log");
    let projection = Projection::in_memory().expect("open projection");
    (
        Engine::new(Store::new(log, projection), registry),
        runner(provider),
    )
}

pub fn engine_with_tools_at(
    provider: &Arc<ScriptedProvider>,
    dir: &TempDir,
    registry: Registry,
) -> (Engine, Runner) {
    let log = Log::open(dir.path()).expect("open log");
    let mut projection = Projection::open(&dir.path().join("index.db")).expect("open projection");
    crate::projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
    (
        Engine::new(Store::new(log, projection), registry),
        runner(provider),
    )
}

pub fn reopened_engine(
    provider: &Arc<ScriptedProvider>,
    dir: &TempDir,
    registry: Registry,
) -> (Engine, Runner) {
    let log = Log::open(dir.path()).expect("reopen log");
    let mut projection = Projection::in_memory().expect("open projection");
    crate::projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
    (
        Engine::new(Store::new(log, projection), registry),
        runner(provider),
    )
}

pub fn seed_log(dir: &TempDir, events: Vec<session_event::Event>) {
    seed_log_payloads(
        dir,
        events
            .into_iter()
            .map(|event| {
                arc_proto::v1::event::Payload::Session(arc_proto::v1::SessionEvent {
                    event: Some(event),
                })
            })
            .collect(),
    );
}

pub fn seed_memory_log(dir: &TempDir, events: Vec<memory_event::Event>) {
    seed_log_payloads(
        dir,
        events
            .into_iter()
            .map(|event| {
                arc_proto::v1::event::Payload::Memory(arc_proto::v1::MemoryEvent {
                    event: Some(event),
                })
            })
            .collect(),
    );
}

pub fn seed_memory_log_at(dir: &TempDir, events: Vec<memory_event::Event>, at_micros: i64) {
    let mut log = Log::open(dir.path()).expect("open log");
    for event in events {
        log.append(arc_proto::v1::Event {
            seq: 0,
            ts: Some(prost_types::Timestamp {
                seconds: at_micros / 1_000_000,
                nanos: i32::try_from((at_micros % 1_000_000) * 1_000).expect("in range"),
            }),
            source: arc_proto::v1::Source::System as i32,
            payload: Some(arc_proto::v1::event::Payload::Memory(
                arc_proto::v1::MemoryEvent { event: Some(event) },
            )),
        })
        .expect("append");
    }
}

pub fn seed_log_payloads(dir: &TempDir, payloads: Vec<arc_proto::v1::event::Payload>) {
    let mut log = Log::open(dir.path()).expect("open log");
    for payload in payloads {
        log.append(arc_proto::v1::Event {
            seq: 0,
            ts: None,
            source: arc_proto::v1::Source::System as i32,
            payload: Some(payload),
        })
        .expect("append");
    }
}

pub fn channel() -> (mpsc::Sender<EngineEvent>, mpsc::Receiver<EngineEvent>) {
    mpsc::channel(64)
}

pub fn drain(rx: &mut mpsc::Receiver<EngineEvent>) -> Vec<EngineEvent> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

pub fn archive_at(dir: &TempDir) -> Arc<Archive> {
    let log = Log::open(dir.path()).expect("open log");
    let index = dir.path().join("index.db");
    let mut projection = Projection::open(&index).expect("open projection");
    crate::projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
    drop(projection);
    Arc::new(Archive::open(&index).expect("open archive"))
}

pub fn replay_events(dir: &std::path::Path) -> Vec<arc_proto::v1::Event> {
    let segments = discover_segments(dir).expect("discover");
    LogReader::new(segments)
        .map(|result| result.expect("replay"))
        .collect()
}

pub fn replay_log(dir: &std::path::Path) -> Vec<session_event::Event> {
    let segments = discover_segments(dir).expect("discover");
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

pub fn appended(event: &session_event::Event) -> &arc_proto::v1::MessageAppended {
    match event {
        session_event::Event::MessageAppended(m) => m,
        other => panic!("expected a message, got {other:?}"),
    }
}

pub fn issued(event: &session_event::Event) -> &arc_proto::v1::ToolCallIssued {
    match event {
        session_event::Event::ToolCallIssued(c) => c,
        other => panic!("expected a tool call, got {other:?}"),
    }
}

pub fn resulted(event: &session_event::Event) -> &arc_proto::v1::ToolResultRecorded {
    match event {
        session_event::Event::ToolResultRecorded(r) => r,
        other => panic!("expected a tool result, got {other:?}"),
    }
}

pub struct Canned {
    pub name: &'static str,
    pub content: &'static str,
    pub ok: bool,
    pub source: ToolSource,
}

impl Tool for Canned {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name.to_owned(),
            description: String::new(),
            parameters: serde_json::json!({"type": "object"}),
        }
    }

    fn source(&self) -> ToolSource {
        self.source
    }

    fn execute(
        &self,
        _arguments_json: String,
        _ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        let reply = if self.ok {
            ToolReply::ok(self.content.to_owned())
        } else {
            ToolReply::error(self.content.to_owned())
        };
        Box::pin(async move { reply })
    }
}

pub fn tools(entries: &[(&'static str, &'static str, bool)]) -> Registry {
    let mut registry = Registry::new(512);
    for &(name, content, ok) in entries {
        registry.register(Box::new(Canned {
            name,
            content,
            ok,
            source: ToolSource::Builtin,
        }));
    }
    registry
}

pub struct TraceCapture {
    _dir: TempDir,
    path: PathBuf,
    guard: tracing::subscriber::DefaultGuard,
}

impl TraceCapture {
    pub fn start() -> Self {
        use tracing_subscriber::prelude::*;
        let dir = TempDir::new().expect("trace dir");
        let (layer, path) = crate::trace::perfetto(dir.path(), "test").expect("trace file");
        let subscriber = tracing_subscriber::registry().with(layer);
        let guard = tracing::subscriber::set_default(subscriber);
        Self {
            _dir: dir,
            path,
            guard,
        }
    }

    pub fn finish(self) -> arc_proto::perfetto::Trace {
        use prost::Message as _;
        let Self { _dir, path, guard } = self;
        drop(guard);
        let bytes = std::fs::read(&path).expect("trace file");
        arc_proto::perfetto::Trace::decode(bytes.as_slice()).expect("decodable trace")
    }
}

pub fn counter_samples(trace: &arc_proto::perfetto::Trace, name: &str) -> Vec<f64> {
    let Some(uuid) = trace
        .packet
        .iter()
        .filter_map(|packet| packet.track_descriptor.as_ref())
        .find(|track| track.counter.is_some() && track.name == name)
        .and_then(|track| track.uuid)
    else {
        return Vec::new();
    };
    trace
        .packet
        .iter()
        .filter_map(|packet| packet.track_event.as_ref())
        .filter(|event| event.track_uuid == Some(uuid))
        .filter_map(|event| event.double_counter_value)
        .collect()
}

pub fn call(id: &str, index: u32, name: &str, args: &str) -> CompletionDelta {
    call_carrying(id, index, name, args, Vec::new())
}

pub fn call_carrying(
    id: &str,
    index: u32,
    name: &str,
    args: &str,
    provider_roundtrip: Vec<u8>,
) -> CompletionDelta {
    CompletionDelta::ToolCall(ToolCall {
        id: id.to_owned(),
        index,
        name: name.to_owned(),
        arguments: args.to_owned(),
        provider_roundtrip,
    })
}

pub fn tool_stop() -> CompletionDelta {
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

    #[tokio::test]
    async fn a_tool_turn_holds_up_across_the_whole_chain() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "lookup", r#"{"q":1}"#)), Ok(tool_stop())],
            done_reply("final text"),
            done_reply("second reply"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (mut engine, run) =
            engine_with_tools(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, mut rx) = channel();

        let reply = engine
            .send_message(&run, None, "question", tx)
            .await
            .expect("send");

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

        let logged = replay_log(dir.path());
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

        let mut fresh = Projection::in_memory().expect("open projection");
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
                    provider_roundtrip: Vec::new(),
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

        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "again", tx)
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
                    provider_roundtrip: Vec::new(),
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
