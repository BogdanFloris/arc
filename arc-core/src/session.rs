use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arc_proto::v1::{
    MemoryEvent, MessageAppended, Role, SessionCreated, SessionEvent, SessionRole, Source,
    ToolCallIssued, ToolOutcome, ToolResultRecorded,
};
use arc_proto::v1::{event, memory_event, session_event};
use futures::StreamExt as _;

use tokio::sync::mpsc;

use crate::memory::render_memory_index;
use crate::projection::{self, MessageRow, SessionSummary};
use crate::provider::{
    self, CompletionDelta, CompletionRequest, Message, Provider, Stop, ToolCall, Usage,
};
use crate::store::{self, Store, now_ts};
use crate::tool::{Registry, TurnContext};

const MAX_TOOL_STEPS: usize = 8;

pub struct Engine<P> {
    store: Store,
    provider: Arc<P>,
    model: String,
    role: SessionRole,
    system: Option<String>,
    registry: Registry,
    no_think: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    Accepted {
        session_id: String,
    },
    Delta(String),
    Reasoning(String),
    ToolCallStarted {
        call_id: String,
        index: u32,
        name: String,
    },
    ToolCallEnded {
        call_id: String,
        outcome: ToolOutcome,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub session_id: String,
    pub seq: u64,
    pub usage: Option<Usage>,
    pub partial: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("session store: {0}")]
    Store(#[from] store::Error),

    #[error("session projection: {0}")]
    Projection(#[from] projection::Error),

    #[error("session provider: {0}")]
    Provider(#[from] provider::Error),

    #[error("refusing to send an empty message")]
    EmptyMessage,

    #[error("the model produced no reply")]
    EmptyReply,
}

impl<P: Provider> Engine<P> {
    pub fn new(
        store: Store,
        provider: Arc<P>,
        model: &str,
        role: SessionRole,
        system: Option<String>,
        registry: Registry,
        no_think: bool,
    ) -> Self {
        Self {
            store,
            provider,
            model: model.to_owned(),
            role,
            system,
            registry,
            no_think,
        }
    }

    #[tracing::instrument(
        level = "info",
        name = "session.send_message",
        skip_all,
        fields(
            model = %self.model,
            role = provider::role_label(self.role),
            session_id = tracing::field::Empty,
            new_session = tracing::field::Empty,
            outcome = tracing::field::Empty,
            assistant_seq = tracing::field::Empty,
            tool_steps = tracing::field::Empty,
            counter.memory_searches = tracing::field::Empty,
            counter.memory_search_hits = tracing::field::Empty,
            counter.memory_reads_from_search = tracing::field::Empty,
            counter.records_created = tracing::field::Empty,
            counter.records_superseded = tracing::field::Empty,
        )
    )]
    pub async fn send_message(
        &mut self,
        session_id: Option<&str>,
        content: &str,
        events: mpsc::Sender<EngineEvent>,
    ) -> Result<Reply, Error> {
        if content.trim().is_empty() {
            return Err(Error::EmptyMessage);
        }
        let span = tracing::Span::current();
        let (session_id, new_session) = match session_id {
            Some(id) => (id.to_owned(), false),
            None => (uuid::Uuid::new_v4().to_string(), true),
        };
        span.record("session_id", session_id.as_str());
        span.record("new_session", new_session);
        let turn_id = uuid::Uuid::new_v4().to_string();

        if new_session {
            self.record(
                Source::User,
                session_event::Event::SessionCreated(SessionCreated {
                    session_id: session_id.clone(),
                    title: String::new(),
                    provider: self.provider.name().to_owned(),
                    model: self.model.clone(),
                    role: self.role as i32,
                    project: String::new(),
                    budget: None,
                }),
            )?;
        }
        self.record(
            Source::User,
            session_event::Event::MessageAppended(MessageAppended {
                session_id: session_id.clone(),
                role: Role::User as i32,
                content: content.to_owned(),
                partial: false,
                turn_id: turn_id.clone(),
            }),
        )?;

        let _ = events
            .send(EngineEvent::Accepted {
                session_id: session_id.clone(),
            })
            .await;

        let (mut transcript, system) = self.open_turn(&session_id)?;
        let mut total_usage: Option<Usage> = None;
        let mut steps = 0;
        let mut memory = MemoryCounters::default();

        let reply = loop {
            // the last step offers no tools, so the model has to answer
            let last_step = steps >= MAX_TOOL_STEPS;
            let request = self.completion_request(system.clone(), transcript.clone(), last_step);

            let (ending, text, calls) = self
                .run_completion(request, &events, &mut total_usage)
                .await?;

            match ending {
                Ending::Done(Stop::ToolCalls) if !last_step && !calls.is_empty() => {
                    steps += 1;
                    span.record("tool_steps", steps);
                    self.tool_step(
                        &session_id,
                        &turn_id,
                        text,
                        calls,
                        &mut transcript,
                        &mut memory,
                        &events,
                    )
                    .await?;
                }
                Ending::Done(_) => {
                    let seq = self.append_reply(&session_id, &turn_id, &text, false)?;
                    span.record("outcome", "done");
                    span.record("assistant_seq", seq);
                    break Ok(Reply {
                        session_id,
                        seq,
                        usage: total_usage,
                        partial: false,
                    });
                }
                Ending::Cut if text.is_empty() => {
                    span.record("outcome", "error");
                    break Err(Error::EmptyReply);
                }
                Ending::Cut => {
                    let seq = self.append_reply(&session_id, &turn_id, &text, true)?;
                    span.record("outcome", "partial");
                    span.record("assistant_seq", seq);
                    break Ok(Reply {
                        session_id,
                        seq,
                        usage: None,
                        partial: true,
                    });
                }
                Ending::Failed(error) => {
                    if !text.is_empty() {
                        let seq = self.append_reply(&session_id, &turn_id, &text, true)?;
                        span.record("assistant_seq", seq);
                    }
                    span.record("outcome", "error");
                    break Err(error.into());
                }
            }
        };
        memory.record_on(&span);
        reply
    }

    async fn run_completion(
        &mut self,
        request: CompletionRequest,
        events: &mpsc::Sender<EngineEvent>,
        total_usage: &mut Option<Usage>,
    ) -> Result<(Ending, String, Vec<ToolCall>), Error> {
        let mut stream = self.provider.complete(request).await?;
        let mut text = String::new();
        let mut calls = Vec::new();
        let ending = loop {
            match stream.next().await {
                Some(Ok(CompletionDelta::Text(chunk))) => {
                    text.push_str(&chunk);
                    let _ = events.send(EngineEvent::Delta(chunk)).await;
                }
                Some(Ok(CompletionDelta::Reasoning(chunk))) => {
                    let _ = events.send(EngineEvent::Reasoning(chunk)).await;
                }
                Some(Ok(CompletionDelta::ToolCall(call))) => calls.push(call),
                Some(Ok(CompletionDelta::Done { usage, stop })) => {
                    let total = total_usage.get_or_insert(Usage::default());
                    total.input_tokens = total.input_tokens.saturating_add(usage.input_tokens);
                    total.output_tokens = total.output_tokens.saturating_add(usage.output_tokens);
                    break Ending::Done(stop);
                }
                Some(Err(error)) => break Ending::Failed(error),
                None => break Ending::Cut,
            }
        };
        Ok((ending, text, calls))
    }

    #[allow(clippy::too_many_arguments)]
    async fn tool_step(
        &mut self,
        session_id: &str,
        turn_id: &str,
        text: String,
        mut calls: Vec<ToolCall>,
        transcript: &mut Vec<Message>,
        memory: &mut MemoryCounters,
        events: &mpsc::Sender<EngineEvent>,
    ) -> Result<(), Error> {
        if !text.is_empty() {
            self.record(
                Source::Model,
                session_event::Event::MessageAppended(MessageAppended {
                    session_id: session_id.to_owned(),
                    role: Role::Assistant as i32,
                    content: text.clone(),
                    partial: false,
                    turn_id: turn_id.to_owned(),
                }),
            )?;
            transcript.push(Message::Text {
                role: Role::Assistant,
                content: text,
            });
        }

        calls.sort_unstable_by_key(|call| call.index);
        // models sometimes omit or repeat call ids; the log needs them unique
        let mut seen = self.store.projection().call_ids(session_id)?;
        for call in &mut calls {
            if call.id.is_empty() || seen.contains(&call.id) {
                call.id = uuid::Uuid::new_v4().to_string();
            }
            seen.insert(call.id.clone());
        }

        for call in &calls {
            self.record(
                Source::Model,
                session_event::Event::ToolCallIssued(ToolCallIssued {
                    session_id: session_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    call_id: call.id.clone(),
                    index: call.index,
                    name: call.name.clone(),
                    arguments_json: call.arguments.clone(),
                    provider_roundtrip: Vec::new(),
                }),
            )?;
            let _ = events
                .send(EngineEvent::ToolCallStarted {
                    call_id: call.id.clone(),
                    index: call.index,
                    name: call.name.clone(),
                })
                .await;
        }

        let mut results = Vec::with_capacity(calls.len());
        for call in &calls {
            let ctx = TurnContext {
                session_id: session_id.to_owned(),
                turn_id: turn_id.to_owned(),
            };
            let dispatched = self
                .registry
                .dispatch(&call.name, call.arguments.clone(), ctx)
                .await;
            let outcome = if dispatched.ok {
                ToolOutcome::Ok
            } else {
                ToolOutcome::Error
            };
            memory.observe_call(&call.name, &call.arguments, &dispatched.content);
            for memory_event in dispatched.memory_events {
                memory.observe_event(&memory_event);
                self.record_memory(Source::Model, memory_event)?;
            }
            self.record(
                Source::System,
                session_event::Event::ToolResultRecorded(ToolResultRecorded {
                    session_id: session_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    call_id: call.id.clone(),
                    outcome: outcome as i32,
                    content: dispatched.content.clone(),
                    truncated: dispatched.truncated,
                }),
            )?;
            let _ = events
                .send(EngineEvent::ToolCallEnded {
                    call_id: call.id.clone(),
                    outcome,
                })
                .await;
            results.push((call.id.clone(), dispatched.content));
        }

        transcript.push(Message::ToolCalls(calls));
        for (call_id, content) in results {
            transcript.push(Message::ToolResult { call_id, content });
        }
        Ok(())
    }

    fn system_prompt(&self, memory_index: Option<&str>) -> Option<String> {
        let mut parts: Vec<&str> = Vec::new();
        if let Some(identity) = &self.system {
            parts.push(identity);
        }
        if let Some(index) = memory_index {
            parts.push(index);
        }
        let mut prompt = parts.join("\n\n");
        if self.no_think {
            if !prompt.is_empty() {
                prompt.push('\n');
            }
            prompt.push_str("/no_think");
        }
        (!prompt.is_empty()).then_some(prompt)
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut Store {
        &mut self.store
    }

    pub fn sessions(&self) -> Result<Vec<SessionSummary>, Error> {
        Ok(self.store.projection().sessions()?)
    }

    #[cfg(test)]
    pub(crate) fn transcript(
        &self,
        session_id: &str,
    ) -> Result<Vec<arc_proto::v1::HistoryEntry>, Error> {
        Ok(self
            .store
            .projection()
            .messages(session_id)?
            .into_iter()
            .map(projection::history_entry)
            .collect())
    }

    fn record(&mut self, source: Source, payload: session_event::Event) -> Result<u64, Error> {
        let payload = event::Payload::Session(SessionEvent {
            event: Some(payload),
        });
        Ok(self.store.append(source, Some(now_ts()), payload)?)
    }

    fn record_memory(
        &mut self,
        source: Source,
        payload: memory_event::Event,
    ) -> Result<u64, Error> {
        let payload = event::Payload::Memory(MemoryEvent {
            event: Some(payload),
        });
        Ok(self.store.append(source, Some(now_ts()), payload)?)
    }

    fn append_reply(
        &mut self,
        session_id: &str,
        turn_id: &str,
        text: &str,
        partial: bool,
    ) -> Result<u64, Error> {
        self.record(
            Source::Model,
            session_event::Event::MessageAppended(MessageAppended {
                session_id: session_id.to_owned(),
                role: Role::Assistant as i32,
                content: text.to_owned(),
                partial,
                turn_id: turn_id.to_owned(),
            }),
        )
    }

    fn open_turn(&mut self, session_id: &str) -> Result<(Vec<Message>, Option<String>), Error> {
        let rows = self.store.projection().messages(session_id)?;
        let system = self.system_prompt(
            render_memory_index(&self.store.projection().memory_index()?).as_deref(),
        );
        Ok((rebuild_transcript(&rows), system))
    }

    fn completion_request(
        &self,
        system: Option<String>,
        messages: Vec<Message>,
        last_step: bool,
    ) -> CompletionRequest {
        CompletionRequest {
            model: self.model.clone(),
            role: self.role,
            system,
            messages,
            tools: if last_step {
                Vec::new()
            } else {
                self.registry.definitions()
            },
            seed: None,
        }
    }
}

fn rebuild_transcript(rows: &[MessageRow]) -> Vec<Message> {
    let mut results: HashMap<&str, &str> = HashMap::new();
    let mut issued: HashSet<&str> = HashSet::new();
    for row in rows {
        match row {
            MessageRow::ToolResult {
                call_id, content, ..
            } => {
                results.entry(call_id.as_str()).or_insert(content.as_str());
            }
            MessageRow::ToolCall { call_id, .. } => {
                issued.insert(call_id.as_str());
            }
            MessageRow::Message { .. } => {}
        }
    }

    let mut messages = Vec::new();
    let mut i = 0;
    while i < rows.len() {
        match &rows[i] {
            MessageRow::Message { role, content, .. } => {
                if let Ok(mapped @ (Role::User | Role::Assistant)) = Role::try_from(*role) {
                    messages.push(Message::Text {
                        role: mapped,
                        content: content.clone(),
                    });
                } else {
                    tracing::warn!(role, "skipping history message with an unmappable role");
                }
                i += 1;
            }
            MessageRow::ToolCall {
                turn_id: step_turn, ..
            } => {
                let mut step = Vec::new();
                while let Some(MessageRow::ToolCall {
                    call_id,
                    call_index,
                    name,
                    arguments_json,
                    turn_id,
                }) = rows.get(i)
                {
                    if turn_id != step_turn {
                        break;
                    }
                    step.push(ToolCall {
                        id: call_id.clone(),
                        index: *call_index,
                        name: name.clone(),
                        arguments: arguments_json.clone(),
                    });
                    i += 1;
                }
                step.sort_unstable_by_key(|call| call.index);

                let mut answered = Vec::with_capacity(step.len());
                let mut step_results = Vec::with_capacity(step.len());
                for call in step {
                    let Some(content) = results.get(call.id.as_str()) else {
                        tracing::warn!(
                            call_id = %call.id,
                            "skipping a call with no result; the orphan closer has not run"
                        );
                        continue;
                    };
                    step_results.push(Message::ToolResult {
                        call_id: call.id.clone(),
                        content: (*content).to_owned(),
                    });
                    answered.push(call);
                }
                if !answered.is_empty() {
                    messages.push(Message::ToolCalls(answered));
                    messages.append(&mut step_results);
                }
            }
            MessageRow::ToolResult { call_id, .. } => {
                if !issued.contains(call_id.as_str()) {
                    tracing::warn!(%call_id, "skipping a tool result no call claimed");
                }
                i += 1;
            }
        }
    }
    messages
}

#[derive(Debug, Default)]
struct MemoryCounters {
    surfaced: HashSet<String>,
    searches: u64,
    search_hits: u64,
    reads_from_search: u64,
    records_created: u64,
    records_superseded: u64,
}

#[derive(serde::Deserialize)]
struct SearchReplyIds {
    records: Vec<SearchReplyId>,
}

#[derive(serde::Deserialize)]
struct SearchReplyId {
    id: String,
}

#[derive(serde::Deserialize)]
struct ReadArgsId {
    id: String,
}

impl MemoryCounters {
    fn observe_call(&mut self, name: &str, arguments_json: &str, result_content: &str) {
        match name {
            "memory_search" => {
                self.searches += 1;
                let Ok(reply) = serde_json::from_str::<SearchReplyIds>(result_content) else {
                    return;
                };
                if !reply.records.is_empty() {
                    self.search_hits += 1;
                }
                self.surfaced
                    .extend(reply.records.into_iter().map(|record| record.id));
            }
            "memory_read" => {
                if let Ok(args) = serde_json::from_str::<ReadArgsId>(arguments_json) {
                    if self.surfaced.contains(&args.id) {
                        self.reads_from_search += 1;
                    }
                }
            }
            _ => {}
        }
    }

    fn observe_event(&mut self, event: &memory_event::Event) {
        match event {
            memory_event::Event::RecordCreated(_) => self.records_created += 1,
            memory_event::Event::RecordSuperseded(_) => self.records_superseded += 1,
            _ => {}
        }
    }

    fn record_on(&self, span: &tracing::Span) {
        if self.searches > 0 {
            span.record("counter.memory_searches", self.searches);
            span.record("counter.memory_search_hits", self.search_hits);
            span.record("counter.memory_reads_from_search", self.reads_from_search);
        }
        if self.records_created > 0 {
            span.record("counter.records_created", self.records_created);
        }
        if self.records_superseded > 0 {
            span.record("counter.records_superseded", self.records_superseded);
        }
    }
}

enum Ending {
    Done(Stop),
    Cut,
    Failed(provider::Error),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arc_proto::v1::{
        HistoryEntry, HistoryMessage, HistoryToolCall, HistoryToolResult, MemoryRecord,
        MemoryRecordCreated, MemoryRecordSuperseded, Role, SessionRole, Source, ToolOutcome,
        history_entry, memory_event, memory_record, session_event,
    };
    use tempfile::TempDir;

    use super::{Engine, EngineEvent, Error, MAX_TOOL_STEPS, MemoryCounters};
    use crate::log::Log;
    use crate::projection::Projection;
    use crate::provider::{
        CompletionDelta, Error as ProviderError, Message, Stop, ToolCall, Usage,
    };
    use crate::store::{self, Store};
    use crate::testkit::{
        Canned, ScriptedProvider, TraceCapture, appended, call, channel, counter_samples,
        done_reply, drain, engine, engine_with_tools, issued, reopened_engine, replay_events,
        replay_log, resulted, seed_log, seed_memory_log, seed_memory_log_at, tool_stop, tools,
        turn, usage,
    };
    use crate::tool::Registry;

    fn seeded_session() -> session_event::Event {
        session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
            session_id: "s-01".to_owned(),
            title: String::new(),
            provider: "scripted".to_owned(),
            model: "test-model".to_owned(),
            role: arc_proto::v1::SessionRole::Unspecified as i32,
            project: String::new(),
            budget: None,
        })
    }

    fn seeded_message(role: Role, content: &str) -> session_event::Event {
        session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
            session_id: "s-01".to_owned(),
            role: role as i32,
            content: content.to_owned(),
            partial: false,
            turn_id: "t-01".to_owned(),
        })
    }

    fn seeded_call(call_id: &str, index: u32) -> session_event::Event {
        session_event::Event::ToolCallIssued(arc_proto::v1::ToolCallIssued {
            session_id: "s-01".to_owned(),
            turn_id: "t-01".to_owned(),
            call_id: call_id.to_owned(),
            index,
            name: "lookup".to_owned(),
            arguments_json: "{}".to_owned(),
            provider_roundtrip: Vec::new(),
        })
    }

    fn seeded_result(call_id: &str, content: &str) -> session_event::Event {
        session_event::Event::ToolResultRecorded(arc_proto::v1::ToolResultRecorded {
            session_id: "s-01".to_owned(),
            turn_id: "t-01".to_owned(),
            call_id: call_id.to_owned(),
            outcome: ToolOutcome::Ok as i32,
            content: content.to_owned(),
            truncated: false,
        })
    }

    fn prose_entry(role: i32, content: &str, partial: bool) -> HistoryEntry {
        HistoryEntry {
            entry: Some(history_entry::Entry::Message(HistoryMessage {
                role,
                content: content.to_owned(),
                partial,
            })),
        }
    }

    #[tokio::test]
    async fn a_new_session_logs_created_user_and_assistant() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello there")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, mut rx) = channel();

        let reply = engine
            .send_message(None, "hi", tx)
            .await
            .expect("send_message");

        assert_eq!(reply.usage, Some(usage()));
        assert!(!reply.partial);
        assert_eq!(reply.seq, 2);

        let events = replay_log(dir.path());
        assert_eq!(events.len(), 3);
        let session_event::Event::SessionCreated(created) = &events[0] else {
            panic!("expected SessionCreated first, got {:?}", events[0]);
        };
        assert_eq!(created.session_id, reply.session_id);
        assert_eq!(created.provider, "scripted");
        assert_eq!(created.model, "test-model");
        assert_eq!(created.role, SessionRole::Concierge as i32);

        let user = appended(&events[1]);
        assert_eq!(
            (user.role, user.content.as_str()),
            (Role::User as i32, "hi")
        );
        assert!(!user.partial);
        let assistant = appended(&events[2]);
        assert_eq!(assistant.role, Role::Assistant as i32);
        assert_eq!(assistant.content, "hello there");
        assert!(!assistant.partial);

        assert_eq!(
            engine.store.projection().last_seq().expect("last_seq"),
            Some(2)
        );
        assert_eq!(
            engine
                .store
                .projection()
                .messages(&reply.session_id)
                .expect("messages")
                .len(),
            2
        );

        let events = drain(&mut rx);
        assert_eq!(
            events,
            [
                EngineEvent::Accepted {
                    session_id: reply.session_id.clone()
                },
                EngineEvent::Delta("hello there".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn a_second_message_reuses_the_session_and_sends_history() {
        let provider =
            ScriptedProvider::scripted(vec![done_reply("first reply"), done_reply("second reply")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);

        let (tx, _rx) = channel();
        let first = engine
            .send_message(None, "one", tx)
            .await
            .expect("first send");
        let (tx, _rx) = channel();
        engine
            .send_message(Some(&first.session_id), "two", tx)
            .await
            .expect("second send");

        let events = replay_log(dir.path());
        assert_eq!(events.len(), 5, "exactly one SessionCreated");

        let requests = provider.requests();
        assert_eq!(requests[1].system.as_deref(), Some("be terse"));
        let turns: Vec<(Role, &str)> = requests[1].messages.iter().map(turn).collect();
        assert_eq!(
            turns,
            [
                (Role::User, "one"),
                (Role::Assistant, "first reply"),
                (Role::User, "two"),
            ],
            "history in order, current message last, nothing duplicated"
        );
    }

    #[tokio::test]
    async fn sessions_lists_what_send_message_created() {
        let provider = ScriptedProvider::scripted(vec![done_reply("one"), done_reply("two")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        assert_eq!(engine.sessions().expect("sessions"), []);

        let (tx, _rx) = channel();
        let first = engine
            .send_message(None, "a", tx)
            .await
            .expect("first send");
        let (tx, _rx) = channel();
        let second = engine
            .send_message(None, "b", tx)
            .await
            .expect("second send");

        let listed = engine.sessions().expect("sessions");
        let mut ids: Vec<&str> = listed.iter().map(|s| s.id.as_str()).collect();
        ids.sort_unstable();
        let mut expected = vec![first.session_id.as_str(), second.session_id.as_str()];
        expected.sort_unstable();
        assert_eq!(ids, expected);
        assert!(listed.iter().all(|s| s.title.is_empty()));
        assert!(listed.iter().all(|s| s.started_at.is_some()));
    }

    #[tokio::test]
    async fn a_cut_stream_appends_a_partial_reply() {
        let provider = ScriptedProvider::scripted(vec![vec![Ok(CompletionDelta::Text(
            "partial tex".to_owned(),
        ))]]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let reply = engine.send_message(None, "hi", tx).await.expect("send");

        assert!(reply.partial);
        assert_eq!(reply.usage, None);
        let events = replay_log(dir.path());
        let assistant = appended(&events[2]);
        assert!(assistant.partial);
        assert_eq!(assistant.content, "partial tex");

        assert_eq!(
            engine.transcript(&reply.session_id).expect("transcript"),
            [
                prose_entry(Role::User as i32, "hi", false),
                prose_entry(Role::Assistant as i32, "partial tex", true),
            ]
        );
    }

    #[tokio::test]
    async fn an_error_after_text_appends_partial_and_surfaces_the_error() {
        let provider = ScriptedProvider::scripted(vec![vec![
            Ok(CompletionDelta::Text("some tex".to_owned())),
            Err(ProviderError::MalformedStream("boom".to_owned())),
        ]]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(None, "hi", tx)
            .await
            .expect_err("must surface");

        assert!(matches!(err, Error::Provider(_)), "got: {err:?}");
        let events = replay_log(dir.path());
        assert_eq!(events.len(), 3, "the partial text was still appended");
        let assistant = appended(&events[2]);
        assert!(assistant.partial);
        assert_eq!(assistant.content, "some tex");
    }

    #[tokio::test]
    async fn an_error_before_text_appends_no_reply() {
        let provider = ScriptedProvider::scripted(vec![vec![Err(ProviderError::MalformedStream(
            "instant".to_owned(),
        ))]]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(None, "hi", tx)
            .await
            .expect_err("must surface");

        assert!(matches!(err, Error::Provider(_)), "got: {err:?}");
        let events = replay_log(dir.path());
        assert_eq!(
            events.len(),
            2,
            "session and user message survive, nothing else"
        );
    }

    #[tokio::test]
    async fn a_cut_before_any_text_is_an_empty_reply() {
        let provider = ScriptedProvider::scripted(vec![vec![]]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(None, "hi", tx)
            .await
            .expect_err("must surface");

        assert!(matches!(err, Error::EmptyReply), "got: {err:?}");
        assert_eq!(replay_log(dir.path()).len(), 2);
    }

    #[tokio::test]
    async fn a_dropped_receiver_does_not_lose_the_append() {
        let provider = ScriptedProvider::scripted(vec![done_reply("nobody watched")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, rx) = channel();
        drop(rx);

        let reply = engine.send_message(None, "hi", tx).await.expect("send");

        assert!(!reply.partial);
        let events = replay_log(dir.path());
        assert_eq!(appended(&events[2]).content, "nobody watched");
    }

    #[tokio::test]
    async fn an_empty_message_is_refused_before_anything_is_appended() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(None, "  \n\t ", tx)
            .await
            .expect_err("must refuse");

        assert!(matches!(err, Error::EmptyMessage), "got: {err:?}");
        assert_eq!(replay_log(dir.path()).len(), 0, "log untouched");
        assert!(provider.requests().is_empty(), "provider never called");
    }

    #[tokio::test]
    async fn an_unmappable_role_in_history_is_skipped() {
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);

        engine
            .record(
                Source::System,
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: "s-old".to_owned(),
                    title: String::new(),
                    provider: "scripted".to_owned(),
                    model: "test-model".to_owned(),
                    role: arc_proto::v1::SessionRole::Unspecified as i32,
                    project: String::new(),
                    budget: None,
                }),
            )
            .expect("record");
        engine
            .record(
                Source::System,
                session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
                    session_id: "s-old".to_owned(),
                    role: 99,
                    content: "from the future".to_owned(),
                    partial: false,
                    turn_id: String::new(),
                }),
            )
            .expect("record");

        let (tx, _rx) = channel();
        engine
            .send_message(Some("s-old"), "hi", tx)
            .await
            .expect("send");

        let requests = provider.requests();
        let turns: Vec<&str> = requests[0].messages.iter().map(|m| turn(m).1).collect();
        assert_eq!(turns, ["hi"], "the unmappable message stayed out");
    }

    #[tokio::test]
    async fn a_tool_turn_logs_calls_results_and_final_text_in_order() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("srv1", 0, "lookup", r#"{"q":1}"#)), Ok(tool_stop())],
            done_reply("answer"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine_with_tools(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, mut rx) = channel();

        let reply = engine.send_message(None, "hi", tx).await.expect("send");

        assert_eq!(
            reply.usage,
            Some(Usage {
                input_tokens: 6,
                output_tokens: 10
            })
        );

        let events = replay_log(dir.path());
        assert_eq!(events.len(), 5);
        let user = appended(&events[1]);
        let issued_call = issued(&events[2]);
        let result = resulted(&events[3]);
        let assistant = appended(&events[4]);

        assert!(!user.turn_id.is_empty(), "the turn has an id");
        for turn_id in [&issued_call.turn_id, &result.turn_id, &assistant.turn_id] {
            assert_eq!(turn_id, &user.turn_id, "one turn, one id");
        }
        assert_eq!(issued_call.call_id, "srv1", "the provider's id, verbatim");
        assert_eq!(issued_call.name, "lookup");
        assert_eq!(issued_call.arguments_json, r#"{"q":1}"#);
        assert_eq!(result.call_id, "srv1");
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert_eq!(result.content, "found it");
        assert!(!result.truncated);
        assert_eq!(assistant.content, "answer");

        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        assert!(!requests[1].tools.is_empty(), "tools stay offered");
        assert_eq!(requests[1].messages.len(), 3);
        let Message::ToolCalls(calls) = &requests[1].messages[1] else {
            panic!("expected the calls, got {:?}", requests[1].messages[1]);
        };
        assert_eq!(calls[0].id, "srv1");
        assert_eq!(
            requests[1].messages[2],
            Message::ToolResult {
                call_id: "srv1".to_owned(),
                content: "found it".to_owned(),
            }
        );

        assert_eq!(
            drain(&mut rx),
            [
                EngineEvent::Accepted {
                    session_id: reply.session_id.clone()
                },
                EngineEvent::ToolCallStarted {
                    call_id: "srv1".to_owned(),
                    index: 0,
                    name: "lookup".to_owned(),
                },
                EngineEvent::ToolCallEnded {
                    call_id: "srv1".to_owned(),
                    outcome: ToolOutcome::Ok,
                },
                EngineEvent::Delta("answer".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn parallel_calls_are_written_ahead_and_answered_in_index_order() {
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("b", 1, "beta", "{}")),
                Ok(call("a", 0, "alpha", "{}")),
                Ok(tool_stop()),
            ],
            done_reply("done"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let registry = tools(&[("alpha", "A", true), ("beta", "B", true)]);
        let mut engine = engine_with_tools(&provider, &dir, registry);
        let (tx, _rx) = channel();

        engine.send_message(None, "hi", tx).await.expect("send");

        let events = replay_log(dir.path());
        let first = issued(&events[2]);
        let second = issued(&events[3]);
        assert_eq!((first.index, first.call_id.as_str()), (0, "a"));
        assert_eq!((second.index, second.call_id.as_str()), (1, "b"));
        assert_eq!(resulted(&events[4]).call_id, "a");
        assert_eq!(resulted(&events[5]).call_id, "b");

        let requests = provider.requests();
        let Message::ToolCalls(calls) = &requests[1].messages[1] else {
            panic!("expected the calls");
        };
        assert_eq!(calls.len(), 2);
        assert_eq!((calls[0].index, calls[1].index), (0, 1));
        assert_eq!(
            &requests[1].messages[2..],
            [
                Message::ToolResult {
                    call_id: "a".to_owned(),
                    content: "A".to_owned(),
                },
                Message::ToolResult {
                    call_id: "b".to_owned(),
                    content: "B".to_owned(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn an_empty_or_colliding_call_id_is_minted() {
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("", 0, "alpha", "{}")),
                Ok(call("dup", 1, "alpha", "{}")),
                Ok(tool_stop()),
            ],
            vec![Ok(call("dup", 0, "alpha", "{}")), Ok(tool_stop())],
            done_reply("ok"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine_with_tools(&provider, &dir, tools(&[("alpha", "A", true)]));
        let (tx, _rx) = channel();

        engine.send_message(None, "hi", tx).await.expect("send");

        let events = replay_log(dir.path());
        let ids: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                session_event::Event::ToolCallIssued(c) => Some(c.call_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(ids.len(), 3);
        assert!(ids.iter().all(|id| !id.is_empty()), "no empty id survives");
        assert_eq!(ids[1], "dup", "the first use of an id is kept");
        assert_ne!(ids[2], "dup", "the second use is replaced");
        let mut unique = ids.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), 3, "ids are unique across the session");

        let result_ids: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                session_event::Event::ToolResultRecorded(r) => Some(r.call_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(result_ids, [ids[0], ids[1], ids[2]]);
    }

    #[tokio::test]
    async fn a_reopened_engine_still_rejects_logged_call_ids() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("dup", 0, "alpha", "{}")), Ok(tool_stop())],
            done_reply("first"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine_with_tools(&provider, &dir, tools(&[("alpha", "A", true)]));
        let (tx, _rx) = channel();
        let reply = engine.send_message(None, "hi", tx).await.expect("send");
        drop(engine);

        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("dup", 0, "alpha", "{}")), Ok(tool_stop())],
            done_reply("second"),
        ]);
        let mut engine = reopened_engine(&provider, &dir, tools(&[("alpha", "A", true)]));
        let (tx, _rx) = channel();
        engine
            .send_message(Some(&reply.session_id), "again", tx)
            .await
            .expect("send after restart");

        let ids: Vec<String> = replay_log(dir.path())
            .iter()
            .filter_map(|event| match event {
                session_event::Event::ToolCallIssued(c) => Some(c.call_id.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], "dup");
        assert_ne!(ids[1], "dup", "the logged id stays taken across restarts");
        assert!(!ids[1].is_empty());
    }

    #[tokio::test]
    async fn completion_order_results_rebuild_sorted_by_call_index() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                seeded_session(),
                seeded_message(Role::User, "question"),
                seeded_call("a", 0),
                seeded_call("b", 1),
                seeded_result("b", "B"),
                seeded_result("a", "A"),
            ],
        );

        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let mut engine = reopened_engine(&provider, &dir, Registry::new(512));
        let (tx, _rx) = channel();
        engine
            .send_message(Some("s-01"), "again", tx)
            .await
            .expect("send");

        let messages = &provider.requests()[0].messages;
        let Message::ToolCalls(calls) = &messages[1] else {
            panic!("expected the calls, got {:?}", messages[1]);
        };
        assert_eq!(
            calls.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            ["a", "b"],
            "calls in index order"
        );
        assert_eq!(
            messages[2..4],
            [
                Message::ToolResult {
                    call_id: "a".to_owned(),
                    content: "A".to_owned(),
                },
                Message::ToolResult {
                    call_id: "b".to_owned(),
                    content: "B".to_owned(),
                },
            ],
            "results by call index, not completion order"
        );
    }

    #[tokio::test]
    async fn history_anomalies_are_skipped_not_fatal() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                seeded_session(),
                seeded_message(Role::User, "question"),
                seeded_call("open", 0),
                seeded_result("ghost", "answers nothing"),
            ],
        );

        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let mut engine = reopened_engine(&provider, &dir, Registry::new(512));
        let (tx, _rx) = channel();
        engine
            .send_message(Some("s-01"), "again", tx)
            .await
            .expect("send");

        assert_eq!(
            provider.requests()[0].messages,
            [
                Message::Text {
                    role: Role::User,
                    content: "question".to_owned(),
                },
                Message::Text {
                    role: Role::User,
                    content: "again".to_owned(),
                },
            ],
            "the unanswered call and the unclaimed result stay out"
        );
    }

    #[test]
    fn transcript_maps_every_row_kind_and_preserves_raw_integers() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                seeded_session(),
                seeded_message(Role::User, "question"),
                session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
                    session_id: "s-01".to_owned(),
                    role: 99,
                    content: "from the future".to_owned(),
                    partial: false,
                    turn_id: "t-01".to_owned(),
                }),
                seeded_call("c1", 0),
                session_event::Event::ToolResultRecorded(arc_proto::v1::ToolResultRecorded {
                    session_id: "s-01".to_owned(),
                    turn_id: "t-01".to_owned(),
                    call_id: "c1".to_owned(),
                    outcome: 42,
                    content: "what the model saw".to_owned(),
                    truncated: true,
                }),
            ],
        );
        let provider = ScriptedProvider::scripted(vec![]);
        let engine = reopened_engine(&provider, &dir, Registry::new(512));

        assert_eq!(
            engine.transcript("s-01").expect("transcript"),
            [
                prose_entry(Role::User as i32, "question", false),
                prose_entry(99, "from the future", false),
                HistoryEntry {
                    entry: Some(history_entry::Entry::ToolCall(HistoryToolCall {
                        call_id: "c1".to_owned(),
                        name: "lookup".to_owned(),
                    })),
                },
                HistoryEntry {
                    entry: Some(history_entry::Entry::ToolResult(HistoryToolResult {
                        call_id: "c1".to_owned(),
                        outcome: 42,
                        truncated: true,
                    })),
                },
            ]
        );
    }

    #[tokio::test]
    async fn a_reopened_session_rebuilds_the_tool_step_into_the_next_request() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "lookup", r#"{"q":1}"#)), Ok(tool_stop())],
            done_reply("final text"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine_with_tools(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(None, "question", tx)
            .await
            .expect("send");
        drop(engine);

        let provider = ScriptedProvider::scripted(vec![done_reply("hello again")]);
        let mut engine = reopened_engine(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, _rx) = channel();
        engine
            .send_message(Some(&reply.session_id), "again", tx)
            .await
            .expect("send after restart");

        assert_eq!(
            provider.requests()[0].messages,
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

    #[tokio::test]
    async fn a_tool_error_is_a_result_and_the_turn_continues() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("x", 0, "fails", "{}")), Ok(tool_stop())],
            done_reply("recovered"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let registry = tools(&[("fails", "ERROR: nope", false)]);
        let mut engine = engine_with_tools(&provider, &dir, registry);
        let (tx, mut rx) = channel();

        let reply = engine.send_message(None, "hi", tx).await.expect("send");
        assert!(!reply.partial);

        let events = replay_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Error as i32);
        assert_eq!(result.content, "ERROR: nope");
        assert_eq!(appended(&events[4]).content, "recovered");
        assert!(drain(&mut rx).contains(&EngineEvent::ToolCallEnded {
            call_id: "x".to_owned(),
            outcome: ToolOutcome::Error,
        }));
    }

    #[tokio::test]
    async fn step_text_is_appended_before_its_calls() {
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(CompletionDelta::Text("checking".to_owned())),
                Ok(call("s", 0, "alpha", "{}")),
                Ok(tool_stop()),
            ],
            done_reply("final"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine_with_tools(&provider, &dir, tools(&[("alpha", "A", true)]));
        let (tx, _rx) = channel();

        engine.send_message(None, "hi", tx).await.expect("send");

        let events = replay_log(dir.path());
        assert_eq!(events.len(), 6);
        let step_text = appended(&events[2]);
        assert_eq!(step_text.content, "checking");
        assert!(!step_text.partial);
        assert_eq!(issued(&events[3]).call_id, "s");
        assert_eq!(appended(&events[5]).content, "final");
        assert_eq!(step_text.turn_id, appended(&events[5]).turn_id);
    }

    #[tokio::test]
    async fn the_step_cap_forces_a_final_completion_without_tools() {
        let mut script: Vec<Vec<Result<CompletionDelta, ProviderError>>> = (0..MAX_TOOL_STEPS)
            .map(|step| {
                vec![
                    Ok(call(&format!("c{step}"), 0, "alpha", "{}")),
                    Ok(tool_stop()),
                ]
            })
            .collect();
        script.push(done_reply("enough"));
        let provider = ScriptedProvider::scripted(script);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine_with_tools(&provider, &dir, tools(&[("alpha", "A", true)]));
        let (tx, _rx) = channel();

        let reply = engine.send_message(None, "hi", tx).await.expect("send");

        let requests = provider.requests();
        assert_eq!(requests.len(), MAX_TOOL_STEPS + 1);
        assert!(
            requests[..MAX_TOOL_STEPS]
                .iter()
                .all(|r| !r.tools.is_empty())
        );
        assert!(
            requests[MAX_TOOL_STEPS].tools.is_empty(),
            "the final completion offers no tools"
        );
        let events = replay_log(dir.path());
        assert_eq!(appended(events.last().expect("events")).content, "enough");
        assert!(!reply.partial);
    }

    #[tokio::test]
    async fn reasoning_is_forwarded_and_never_stored() {
        let provider = ScriptedProvider::scripted(vec![vec![
            Ok(CompletionDelta::Reasoning("hmm".to_owned())),
            Ok(CompletionDelta::Text("hi there".to_owned())),
            Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            }),
        ]]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, mut rx) = channel();

        engine.send_message(None, "hi", tx).await.expect("send");

        let forwarded = drain(&mut rx);
        assert!(forwarded.contains(&EngineEvent::Reasoning("hmm".to_owned())));
        let events = replay_log(dir.path());
        assert_eq!(events.len(), 3);
        assert_eq!(appended(&events[2]).content, "hi there");
    }

    #[tokio::test]
    async fn a_truncated_result_marks_the_event() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("t", 0, "big", "{}")), Ok(tool_stop())],
            done_reply("ok"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut registry = Registry::new(8);
        registry.register(Box::new(Canned {
            name: "big",
            content: "0123456789abcdef",
            ok: true,
        }));
        let mut engine = engine_with_tools(&provider, &dir, registry);
        let (tx, _rx) = channel();

        engine.send_message(None, "hi", tx).await.expect("send");

        let events = replay_log(dir.path());
        let result = resulted(&events[3]);
        assert!(result.truncated);
        assert_eq!(result.content, "01234567 [truncated]");
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
    }

    #[tokio::test]
    async fn no_think_rides_the_system_prompt() {
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let mut engine = Engine::new(
            Store::new(log, projection),
            Arc::clone(&provider),
            "test-model",
            SessionRole::Concierge,
            Some("be terse".to_owned()),
            Registry::new(512),
            true,
        );
        let (tx, _rx) = channel();

        engine.send_message(None, "hi", tx).await.expect("send");

        assert_eq!(
            provider.requests()[0].system.as_deref(),
            Some("be terse\n/no_think")
        );
    }

    #[tokio::test]
    async fn the_role_lands_on_the_session_and_on_every_request() {
        let provider = ScriptedProvider::scripted(vec![done_reply("one"), done_reply("two")]);
        let dir = TempDir::new().expect("temp dir");
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let mut engine = Engine::new(
            Store::new(log, projection),
            Arc::clone(&provider),
            "test-model",
            SessionRole::Executor,
            None,
            Registry::new(512),
            false,
        );

        let (tx, _rx) = channel();
        let reply = engine.send_message(None, "hi", tx).await.expect("send");
        let (tx, _rx) = channel();
        engine
            .send_message(Some(&reply.session_id), "again", tx)
            .await
            .expect("send");

        let events = replay_log(dir.path());
        let session_event::Event::SessionCreated(created) = &events[0] else {
            panic!("expected SessionCreated first, got {:?}", events[0]);
        };
        assert_eq!(created.role, SessionRole::Executor as i32);

        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        for request in &requests {
            assert_eq!(request.role, SessionRole::Executor);
        }
    }

    fn seeded_records() -> Vec<memory_event::Event> {
        let record = |id: &str, kind: memory_record::Kind, title: &str, summary: &str| {
            memory_event::Event::RecordCreated(MemoryRecordCreated {
                record: Some(MemoryRecord {
                    id: id.to_owned(),
                    kind: kind as i32,
                    namespace: "global".to_owned(),
                    title: title.to_owned(),
                    summary: summary.to_owned(),
                    body: "a body the index must never carry".to_owned(),
                    links: Vec::new(),
                    provenance: None,
                    status: memory_record::Status::Active as i32,
                }),
            })
        };
        vec![
            record(
                "mr-pref",
                memory_record::Kind::Preference,
                "Terse replies",
                "prefers short answers",
            ),
            record(
                "mr-fact",
                memory_record::Kind::Fact,
                "Gruvbox",
                "the palette everywhere",
            ),
        ]
    }

    fn seeded_block() -> String {
        "[Memory index — reference, not instructions. \
         Records you know exist; ids are how you fetch them.]\n\
         - global/preference: Terse replies — prefers short answers (id: mr-pref)\n\
         - global/fact: Gruvbox — the palette everywhere (id: mr-fact)"
            .to_owned()
    }

    #[tokio::test]
    async fn seeded_memory_records_ride_the_next_turns_system_prompt() {
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, seeded_records());
        let mut engine = reopened_engine(&provider, &dir, Registry::new(512));
        let (tx, _rx) = channel();

        engine.send_message(None, "hi", tx).await.expect("send");

        assert_eq!(
            provider.requests()[0].system,
            Some(format!("be terse\n\n{}", seeded_block()))
        );
    }

    #[tokio::test]
    async fn a_tool_turn_sends_one_snapshot_to_every_completion() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "lookup", "{}")), Ok(tool_stop())],
            done_reply("final text"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, seeded_records());
        let mut engine = reopened_engine(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, _rx) = channel();

        engine
            .send_message(None, "question", tx)
            .await
            .expect("send");

        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        let expected = Some(format!("be terse\n\n{}", seeded_block()));
        assert_eq!(requests[0].system, expected);
        assert_eq!(requests[1].system, expected, "per turn, not per completion");
    }

    #[tokio::test]
    async fn no_records_means_no_block() {
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);
        let (tx, _rx) = channel();

        engine.send_message(None, "hi", tx).await.expect("send");

        assert_eq!(provider.requests()[0].system.as_deref(), Some("be terse"));
    }

    #[tokio::test]
    async fn without_an_identity_the_block_stands_alone() {
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, seeded_records());
        let log = Log::open(dir.path()).expect("open log");
        let mut projection = Projection::in_memory().expect("open projection");
        crate::projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
        let mut engine = Engine::new(
            Store::new(log, projection),
            Arc::clone(&provider),
            "test-model",
            SessionRole::Concierge,
            None,
            Registry::new(512),
            false,
        );
        let (tx, _rx) = channel();

        engine.send_message(None, "hi", tx).await.expect("send");

        assert_eq!(provider.requests()[0].system, Some(seeded_block()));
    }

    const SEARCH_HIT: &str = r#"{"records":[{"id":"mr-pal","namespace":"global","kind":"fact","title":"Gruvbox","summary":"the palette"}]}"#;

    const TURN_COUNTERS: [&str; 5] = [
        "memory_searches",
        "memory_search_hits",
        "memory_reads_from_search",
        "records_created",
        "records_superseded",
    ];

    #[test]
    fn memory_counters_follow_search_and_read() {
        let mut counters = MemoryCounters::default();
        counters.observe_call(
            "memory_search",
            r#"{"query":"palette"}"#,
            r#"{"records":[{"id":"mr-1"},{"id":"mr-2"}]}"#,
        );
        counters.observe_call(
            "memory_search",
            r#"{"query":"nothing"}"#,
            "No memory records match. For something from a past conversation, \
             search sessions_search before giving up.",
        );
        counters.observe_call("memory_read", r#"{"id":"mr-1"}"#, "the body");
        counters.observe_call(
            "memory_read",
            r#"{"id":"mr-ghost"}"#,
            "ERROR: no such record",
        );
        counters.observe_call("sessions_search", r#"{"query":"x"}"#, "unrelated");

        assert_eq!(counters.searches, 2);
        assert_eq!(
            counters.search_hits, 1,
            "the prose no-match reply is not a hit"
        );
        assert_eq!(
            counters.reads_from_search, 1,
            "only the read of a surfaced id counts"
        );
        assert_eq!(counters.records_created, 0);
        assert_eq!(counters.records_superseded, 0);
    }

    #[test]
    fn memory_counters_split_events_by_kind() {
        let mut counters = MemoryCounters::default();
        counters.observe_event(&memory_event::Event::RecordCreated(MemoryRecordCreated {
            record: None,
        }));
        counters.observe_event(&memory_event::Event::RecordSuperseded(
            MemoryRecordSuperseded {
                superseded_id: "mr-old".to_owned(),
                record: None,
            },
        ));

        assert_eq!(counters.records_created, 1);
        assert_eq!(counters.records_superseded, 1);
        assert_eq!(counters.searches, 0);
    }

    #[tokio::test]
    async fn a_search_then_read_turn_records_the_retrieval_counters() {
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "memory_search", r#"{"query":"palette"}"#)),
                Ok(tool_stop()),
            ],
            vec![
                Ok(call("c2", 0, "memory_read", r#"{"id":"mr-pal"}"#)),
                Ok(tool_stop()),
            ],
            done_reply("gruvbox"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let registry = tools(&[
            ("memory_search", SEARCH_HIT, true),
            ("memory_read", "the full body", true),
        ]);
        let mut engine = engine_with_tools(&provider, &dir, registry);

        let capture = TraceCapture::start();
        let (tx, _rx) = channel();
        engine
            .send_message(None, "what palette?", tx)
            .await
            .expect("send");
        let trace = capture.finish();

        assert_eq!(counter_samples(&trace, "memory_searches"), [1.0]);
        assert_eq!(counter_samples(&trace, "memory_search_hits"), [1.0]);
        assert_eq!(counter_samples(&trace, "memory_reads_from_search"), [1.0]);
        assert!(
            counter_samples(&trace, "records_created").is_empty(),
            "no writes, no write counters"
        );
    }

    #[tokio::test]
    async fn a_search_without_a_read_records_zero_reads_from_search() {
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "memory_search", r#"{"query":"palette"}"#)),
                Ok(tool_stop()),
            ],
            done_reply("answered from the summary"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine_with_tools(
            &provider,
            &dir,
            tools(&[("memory_search", SEARCH_HIT, true)]),
        );

        let capture = TraceCapture::start();
        let (tx, _rx) = channel();
        engine.send_message(None, "hm", tx).await.expect("send");
        let trace = capture.finish();

        assert_eq!(counter_samples(&trace, "memory_searches"), [1.0]);
        assert_eq!(counter_samples(&trace, "memory_search_hits"), [1.0]);
        assert_eq!(
            counter_samples(&trace, "memory_reads_from_search"),
            [0.0],
            "a searched turn records its zero — the denominator exists"
        );
    }

    #[tokio::test]
    async fn a_memory_write_turn_records_one_record_created() {
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "memory_write",
                    r#"{"kind":"preference","title":"Terse replies",
                        "summary":"prefers short answers","body":"Prefers short answers."}"#,
                )),
                Ok(tool_stop()),
            ],
            done_reply("saved"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut registry = Registry::new(512);
        registry.register(Box::new(crate::tool::memory::MemoryWrite));
        let mut engine = engine_with_tools(&provider, &dir, registry);

        let capture = TraceCapture::start();
        let (tx, _rx) = channel();
        engine
            .send_message(None, "remember this", tx)
            .await
            .expect("send");
        let trace = capture.finish();

        assert_eq!(counter_samples(&trace, "records_created"), [1.0]);
        assert!(
            counter_samples(&trace, "memory_searches").is_empty(),
            "no retrieval traffic, no retrieval counters"
        );
    }

    #[tokio::test]
    async fn a_supersede_turn_records_one_record_superseded() {
        struct Superseder;

        impl crate::tool::Tool for Superseder {
            fn definition(&self) -> crate::provider::ToolDefinition {
                crate::provider::ToolDefinition {
                    name: "memory_supersede".to_owned(),
                    description: String::new(),
                    parameters: serde_json::json!({"type": "object"}),
                }
            }

            fn execute(
                &self,
                _arguments_json: String,
                _ctx: crate::tool::TurnContext,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = crate::tool::ToolReply> + Send + '_>,
            > {
                Box::pin(async move {
                    crate::tool::ToolReply {
                        content: "Superseded mr-pref with mr-new.".to_owned(),
                        ok: true,
                        memory_events: vec![memory_event::Event::RecordSuperseded(
                            MemoryRecordSuperseded {
                                superseded_id: "mr-pref".to_owned(),
                                record: Some(MemoryRecord {
                                    id: "mr-new".to_owned(),
                                    kind: memory_record::Kind::Preference as i32,
                                    namespace: "global".to_owned(),
                                    title: "Terse replies".to_owned(),
                                    summary: "still prefers short answers".to_owned(),
                                    body: "Still prefers short answers.".to_owned(),
                                    links: Vec::new(),
                                    provenance: None,
                                    status: memory_record::Status::Active as i32,
                                }),
                            },
                        )],
                    }
                })
            }
        }

        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "memory_supersede", r#"{"id":"mr-pref"}"#)),
                Ok(tool_stop()),
            ],
            done_reply("updated"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, seeded_records());
        let mut registry = Registry::new(512);
        registry.register(Box::new(Superseder));
        let mut engine = reopened_engine(&provider, &dir, registry);

        let capture = TraceCapture::start();
        let (tx, _rx) = channel();
        engine
            .send_message(None, "that changed", tx)
            .await
            .expect("send");
        let trace = capture.finish();

        assert_eq!(counter_samples(&trace, "records_superseded"), [1.0]);
        assert!(counter_samples(&trace, "records_created").is_empty());
    }

    #[tokio::test]
    async fn a_turn_without_memory_traffic_records_no_counters() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "lookup", "{}")), Ok(tool_stop())],
            done_reply("plain answer"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine_with_tools(&provider, &dir, tools(&[("lookup", "found", true)]));

        let capture = TraceCapture::start();
        let (tx, _rx) = channel();
        engine.send_message(None, "hi", tx).await.expect("send");
        let trace = capture.finish();

        for name in TURN_COUNTERS {
            assert!(
                counter_samples(&trace, name).is_empty(),
                "{name} must be absent on a memory-free turn"
            );
        }
    }

    #[tokio::test]
    async fn no_think_lands_after_the_block() {
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, seeded_records());
        let log = Log::open(dir.path()).expect("open log");
        let mut projection = Projection::in_memory().expect("open projection");
        crate::projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
        let mut engine = Engine::new(
            Store::new(log, projection),
            Arc::clone(&provider),
            "test-model",
            SessionRole::Concierge,
            Some("be terse".to_owned()),
            Registry::new(512),
            true,
        );
        let (tx, _rx) = channel();

        engine.send_message(None, "hi", tx).await.expect("send");

        assert_eq!(
            provider.requests()[0].system,
            Some(format!("be terse\n\n{}\n/no_think", seeded_block()))
        );
    }

    fn review_engine(provider: &Arc<ScriptedProvider>, dir: &TempDir) -> Engine<ScriptedProvider> {
        seed_memory_log_at(dir, seeded_records(), 1_700_000_000_000_000);
        reopened_engine(provider, dir, Registry::new(512))
    }

    fn memory_payload(event: &arc_proto::v1::Event) -> &memory_event::Event {
        match &event.payload {
            Some(arc_proto::v1::event::Payload::Memory(memory)) => {
                memory.event.as_ref().expect("memory event")
            }
            other => panic!("expected a memory payload, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn review_accept_appends_a_user_reviewed_event_and_clears_the_queue() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = review_engine(&provider, &dir);

        let queued: Vec<String> = engine
            .store()
            .projection()
            .review_items(0)
            .expect("review_items")
            .into_iter()
            .map(|item| item.record.id)
            .collect();
        assert_eq!(
            queued,
            ["mr-fact", "mr-pref"],
            "both records await a verdict"
        );

        engine.store_mut().review_accept("mr-fact").expect("accept");

        let events = replay_events(dir.path());
        let verdict = events.last().expect("the verdict");
        assert_eq!(
            verdict.source,
            Source::User as i32,
            "the verdict is the user's"
        );
        assert!(
            verdict.ts.is_some(),
            "stamped, so the projection can order it"
        );
        match memory_payload(verdict) {
            memory_event::Event::RecordReviewed(reviewed) => {
                assert_eq!(reviewed.record_id, "mr-fact");
            }
            other => panic!("expected RecordReviewed, got {other:?}"),
        }

        let queued: Vec<String> = engine
            .store()
            .projection()
            .review_items(0)
            .expect("review_items")
            .into_iter()
            .map(|item| item.record.id)
            .collect();
        assert_eq!(queued, ["mr-pref"], "the accepted record left the queue");
    }

    #[tokio::test]
    async fn review_delete_appends_a_user_deleted_event_and_removes_the_record() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = review_engine(&provider, &dir);

        engine.store_mut().review_delete("mr-pref").expect("delete");

        let events = replay_events(dir.path());
        let verdict = events.last().expect("the verdict");
        assert_eq!(verdict.source, Source::User as i32);
        match memory_payload(verdict) {
            memory_event::Event::RecordDeleted(deleted) => assert_eq!(deleted.id, "mr-pref"),
            other => panic!("expected RecordDeleted, got {other:?}"),
        }

        let queued: Vec<String> = engine
            .store()
            .projection()
            .review_items(0)
            .expect("review_items")
            .into_iter()
            .map(|item| item.record.id)
            .collect();
        assert_eq!(queued, ["mr-fact"], "the deleted record is gone entirely");
    }

    #[tokio::test]
    async fn a_verdict_for_an_unknown_record_is_refused_before_the_log() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = review_engine(&provider, &dir);
        let before = replay_events(dir.path()).len();

        let accept = engine.store_mut().review_accept("mr-ghost");
        assert!(
            matches!(accept, Err(store::Error::UnknownRecord { ref id }) if id == "mr-ghost"),
            "got: {accept:?}"
        );
        let delete = engine.store_mut().review_delete("mr-ghost");
        assert!(
            matches!(delete, Err(store::Error::UnknownRecord { ref id }) if id == "mr-ghost"),
            "got: {delete:?}"
        );

        assert_eq!(
            replay_events(dir.path()).len(),
            before,
            "nothing was appended"
        );
    }
}
