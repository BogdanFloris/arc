use std::collections::VecDeque;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arc_proto::v1::{Role, SessionRole, memory_event, session_event};
use futures::StreamExt as _;
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

/// One scripted response to a `complete` call. `Gated` lets a test stall a
/// stream mid-turn: it yields `before`, then waits on `notify`, then yields
/// `after` — so another session's turn can be driven to completion in between.
#[derive(Debug)]
pub enum Step {
    Immediate(Vec<Result<CompletionDelta, ProviderError>>),
    Gated {
        before: Vec<Result<CompletionDelta, ProviderError>>,
        notify: Arc<tokio::sync::Notify>,
        after: Vec<Result<CompletionDelta, ProviderError>>,
    },
    /// Panics when the provider is asked to complete: drives the panic
    /// watchdog test without a second task or a real crashing tool.
    Panics,
}

#[derive(Debug)]
pub struct ScriptedProvider {
    script: Mutex<VecDeque<Step>>,
    captured: Mutex<Vec<CompletionRequest>>,
}

impl ScriptedProvider {
    pub fn scripted(calls: Vec<Vec<Result<CompletionDelta, ProviderError>>>) -> Arc<Self> {
        Self::scripted_steps(calls.into_iter().map(Step::Immediate).collect())
    }

    pub fn scripted_steps(steps: Vec<Step>) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(steps.into()),
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
        let step = self
            .script
            .lock()
            .expect("script")
            .pop_front()
            .expect("script exhausted");
        Box::pin(async move {
            let stream: CompletionStream = match step {
                Step::Immediate(items) => Box::pin(stream::iter(items)),
                Step::Gated {
                    before,
                    notify,
                    after,
                } => Box::pin(
                    stream::iter(before)
                        .chain(
                            stream::once(async move { notify.notified().await })
                                .filter_map(|()| async { None }),
                        )
                        .chain(stream::iter(after)),
                ),
                Step::Panics => panic!("scripted provider panic"),
            };
            Ok(stream)
        })
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
        Message::Text { role, content, .. } => (*role, content.as_str()),
        other => panic!("expected a text message, got {other:?}"),
    }
}

pub fn runner(provider: &Arc<ScriptedProvider>) -> Runner {
    runner_with_role(provider, SessionRole::Concierge)
}

pub fn runner_with_role(provider: &Arc<ScriptedProvider>, role: SessionRole) -> Runner {
    Runner {
        role,
        provider: Arc::clone(provider) as Arc<dyn Provider>,
        model: "test-model".to_owned(),
        thinking: Thinking::Default,
        system: Some("be terse".to_owned()),
    }
}

pub fn engine(provider: &Arc<ScriptedProvider>, dir: &TempDir) -> (Engine, Runner) {
    engine_with_tools(provider, dir, Registry::new(512))
}

pub fn engine_with_role(
    provider: &Arc<ScriptedProvider>,
    dir: &TempDir,
    role: SessionRole,
) -> (Engine, Runner) {
    let log = Log::open(dir.path()).expect("open log");
    let projection = Projection::in_memory().expect("open projection");
    (
        Engine::new(Store::new(log, projection), Registry::new(512)),
        runner_with_role(provider, role),
    )
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

pub fn seed_memory_log_each(dir: &TempDir, events: Vec<(memory_event::Event, i64)>) {
    let mut log = Log::open(dir.path()).expect("open log");
    for (event, at_micros) in events {
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

pub fn server_called(event: &session_event::Event) -> &arc_proto::v1::ServerCallRecorded {
    match event {
        session_event::Event::ServerCallRecorded(c) => c,
        other => panic!("expected a server call, got {other:?}"),
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

/// A tool that blocks on a `Notify` before returning: drives a cancel test,
/// where the turn needs to be caught mid-dispatch and never notified.
pub struct Gated {
    pub name: &'static str,
    pub notify: Arc<tokio::sync::Notify>,
}

impl Tool for Gated {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: self.name.to_owned(),
            description: String::new(),
            parameters: serde_json::json!({"type": "object"}),
        }
    }

    fn source(&self) -> ToolSource {
        ToolSource::Builtin
    }

    fn execute(
        &self,
        _arguments_json: String,
        _ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        let notify = Arc::clone(&self.notify);
        Box::pin(async move {
            notify.notified().await;
            ToolReply::ok("done".to_owned())
        })
    }
}

/// Echoes `ctx.command_prefix` back as its reply, joined by commas, so tests
/// can see what a real turn resolved without a subprocess.
pub struct PrefixEcho {
    pub name: &'static str,
    pub source: ToolSource,
}

impl Tool for PrefixEcho {
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
        ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async move { ToolReply::ok(ctx.command_prefix.join(",")) })
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

// spans from spawned threads land on the global default, so two parallel
// captures can steal each other's records; one at a time is the fix
static TRACE_CAPTURE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub struct TraceCapture {
    _dir: TempDir,
    path: PathBuf,
    guard: tracing::subscriber::DefaultGuard,
    _exclusive: std::sync::MutexGuard<'static, ()>,
}

impl TraceCapture {
    pub fn start() -> Self {
        use tracing_subscriber::prelude::*;
        let exclusive = TRACE_CAPTURE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = TempDir::new().expect("trace dir");
        let (layer, path) = crate::trace::perfetto(dir.path(), "test").expect("trace file");
        let subscriber = tracing_subscriber::registry().with(layer);
        let guard = tracing::subscriber::set_default(subscriber);
        Self {
            _dir: dir,
            path,
            guard,
            _exclusive: exclusive,
        }
    }

    pub fn finish(self) -> arc_proto::perfetto::Trace {
        use prost::Message as _;
        let Self {
            _dir,
            path,
            guard,
            _exclusive,
        } = self;
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
    use arc_proto::v1::{Role, Source, ToolOutcome, session_event};
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
        let (engine, run) =
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
                    arguments_json: r#"{"q":1}"#.to_owned(),
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
        let rows = fresh.messages(&reply.session_id).expect("messages");
        assert_eq!(
            rows[..3],
            [
                MessageRow::Message {
                    role: Role::User as i32,
                    content: "question".to_owned(),
                    partial: false,
                    turn_id: turn_id.clone(),
                    source: Source::User as i32,
                    input_tokens: 0,
                    output_tokens: 0,
                    elapsed_ms: 0,
                    grounding_json: String::new(),
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
            ]
        );
        let MessageRow::Message {
            role,
            content,
            partial,
            turn_id: final_turn_id,
            source,
            input_tokens,
            output_tokens,
            elapsed_ms: _,
            grounding_json: _,
        } = &rows[3]
        else {
            panic!("expected the final assistant message, got {:?}", rows[3]);
        };
        assert_eq!(*role, Role::Assistant as i32);
        assert_eq!(content, "final text");
        assert!(!partial);
        assert_eq!(final_turn_id, &turn_id);
        assert_eq!(*source, Source::Model as i32);
        assert_eq!(
            (*input_tokens, *output_tokens),
            (6, 10),
            "usage accumulated across both completion steps"
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
                    reasoning: None,
                },
                Message::ToolCalls {
                    calls: vec![ToolCall {
                        id: "c1".to_owned(),
                        index: 0,
                        name: "lookup".to_owned(),
                        arguments: r#"{"q":1}"#.to_owned(),
                        provider_roundtrip: Vec::new(),
                    }],
                    reasoning: None,
                },
                Message::ToolResult {
                    call_id: "c1".to_owned(),
                    content: "found it".to_owned(),
                },
                Message::Text {
                    role: Role::Assistant,
                    content: "final text".to_owned(),
                    reasoning: None,
                },
                Message::Text {
                    role: Role::User,
                    content: "again".to_owned(),
                    reasoning: None,
                },
            ]
        );
    }
}
