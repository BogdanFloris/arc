//! The session engine: the conversation loop over log, projection, and provider.
//!
//! [`Engine`] owns the only write path a conversation has. One call to
//! [`Engine::send_message`] is: make the user's message durable, drive the model,
//! make the reply durable. The log is the source of truth at every step.
//!
//! # How a completion ends
//!
//! The three stream endings map to log by one rule: the log records what the user
//! saw and errors are reported, never archived as messages (DESIGN.md §4).
//!
//! - `Done` seen → the reply is appended whole, `partial: false`.
//! - Stream cut after text → the text is appended with `partial: true`.
//! - An error, or a cut before any text → nothing model-side is appended; the
//!   user's message and the session stay in the log, and the error goes to
//!   the caller.
//!
//! # The tool loop
//!
//! A turn is one or more completions. A completion that stops for tool calls
//! appends every `ToolCallIssued` before dispatching any of them — §3.1's
//! write-ahead rule, which is what makes the log's silence meaningful — then
//! appends a `ToolResultRecorded` per call and completes again over the grown
//! transcript, until the model ends its turn with text. After
//! [`MAX_TOOL_STEPS`] tool steps the final completion offers no tools, so a
//! looping model is forced to prose instead of becoming the user's problem.
//!
//! # Streaming to callers
//!
//! [`EngineEvent`]s mirror the wire protocol event for event, so the daemon's
//! socket layer is a translator, not a decision-maker. `Accepted` is sent
//! before the provider is called: the caller learns its session id even if
//! the model fails instantly, because by then the message is already durable.
//! A closed channel means the client went away — the completion is driven to
//! its end and appended regardless; durability does not depend on anyone
//! watching.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arc_proto::v1::{
    Event, MemoryEvent, MessageAppended, Role, SessionCreated, SessionEvent, Source,
    ToolCallIssued, ToolOutcome, ToolResultRecorded,
};
use arc_proto::v1::{event, memory_event, session_event};
use futures::StreamExt as _;
use prost_types::Timestamp;
use tokio::sync::mpsc;

use crate::log::{self, Log};
use crate::memory::render_memory_index;
use crate::projection::{self, MessageRow, Projection, SessionSummary};
use crate::provider::{
    self, CompletionDelta, CompletionRequest, Message, Provider, Stop, ToolCall, Usage,
};
use crate::tool::{Registry, TurnContext};

/// Most tool steps one turn may take. The completion after the last step
/// offers no tools, forcing prose grounded in whatever results arrived
/// (banked 2026-08-17).
const MAX_TOOL_STEPS: usize = 8;

/// The conversation loop, generic over the model backend so tests drive it
/// with a scripted provider and no network.
///
/// `&mut self` on [`send_message`](Engine::send_message): one completion
/// at a time per engine. The daemon serializes callers around it.
pub struct Engine<P> {
    log: Log,
    projection: Projection,
    provider: Arc<P>,
    model: String,
    /// The identity file's content, when there is one (DESIGN.md §5.1).
    system: Option<String>,
    registry: Registry,
    /// Append `/no_think` to interactive turns
    no_think: bool,
    /// Call ids this process has logged, per session
    issued_call_ids: HashMap<String, HashSet<String>>,
}

/// What the engine reports to its caller while a message is in flight.
///
/// Mirrors the wire protocol: `Accepted` → `MessageAccepted`, `Delta` →
/// `Delta`; the returned [`Reply`] carries what `StreamEnd` needs, and a
/// returned error is the `Error` frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    Accepted {
        session_id: String,
    },
    Delta(String),
    /// A chunk of the model's thinking
    Reasoning(String),
    /// A call is durable and about to run; mirrors `ToolCallStarted`.
    ToolCallStarted {
        call_id: String,
        index: u32,
        name: String,
    },
    /// The call's result is durable; mirrors `ToolCallEnded`.
    ToolCallEnded {
        call_id: String,
        outcome: ToolOutcome,
    },
}

/// The outcome of one [`Engine::send_message`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub session_id: String,
    pub seq: u64,
    pub usage: Option<Usage>,
    pub partial: bool,
}

/// Everything one conversation turn can fail with.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Appending to the event log failed. Durable state is in doubt; the
    /// caller should treat this as fatal.
    #[error("session log append: {0}")]
    Log(#[from] log::Error),

    /// The index refused an event the log accepted — log and projection are
    /// out of step, which is a bug, not a runtime condition.
    #[error("session projection: {0}")]
    Projection(#[from] projection::Error),

    /// The provider failed, eagerly or mid-stream. Any text that arrived
    /// first is already in the log, marked partial.
    #[error("session provider: {0}")]
    Provider(#[from] provider::Error),

    /// A whitespace-only message. Nothing was appended.
    #[error("refusing to send an empty message")]
    EmptyMessage,

    /// The stream ended before the first token. No assistant message was
    /// appended: there was nothing seen to record.
    #[error("the model produced no reply")]
    EmptyReply,
}

impl<P: Provider> Engine<P> {
    /// An engine over an open log and its index.
    ///
    /// `system` is the identity file's content when present; it rides every
    /// completion as the system prompt.
    pub fn new(
        log: Log,
        projection: Projection,
        provider: Arc<P>,
        model: impl Into<String>,
        system: Option<String>,
        registry: Registry,
        no_think: bool,
    ) -> Self {
        Self {
            log,
            projection,
            provider,
            model: model.into(),
            system,
            registry,
            no_think,
            issued_call_ids: HashMap::new(),
        }
    }

    /// One conversation turn: durably append the user's message (creating the
    /// session when `session_id` is `None`), drive the model, durably append
    /// what came back.
    ///
    /// Progress is reported on `events` — [`EngineEvent::Accepted`] first,
    /// then each text chunk. A closed channel is not an error: the completion
    /// is driven to its end and appended regardless.
    ///
    /// # Errors
    ///
    /// - [`Error::EmptyMessage`] for whitespace-only content; nothing appended.
    /// - [`Error::Log`] / [`Error::Projection`] if durability fails; fatal.
    /// - [`Error::Provider`] if the model call fails. Text that arrived before
    ///   a mid-stream failure is appended with `partial: true` first.
    /// - [`Error::EmptyReply`] if the stream ended before the first token.
    #[tracing::instrument(
        level = "info",
        name = "session.send_message",
        skip_all,
        fields(
            model = %self.model,
            session_id = tracing::field::Empty,
            new_session = tracing::field::Empty,
            outcome = tracing::field::Empty,
            assistant_seq = tracing::field::Empty,
            tool_steps = tracing::field::Empty,
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

        // Durable from here on: tell the caller where its message lives.
        let _ = events
            .send(EngineEvent::Accepted {
                session_id: session_id.clone(),
            })
            .await;

        // History includes the message just appended; from here the turn's
        // transcript grows in memory as the loop appends durable events.
        let (mut transcript, system) = self.open_turn(&session_id)?;
        let mut total_usage: Option<Usage> = None;
        let mut steps = 0;

        loop {
            // After the last allowed tool step, offer no tools: the model can
            // only answer in prose.
            let last_step = steps >= MAX_TOOL_STEPS;
            let request = CompletionRequest {
                model: self.model.clone(),
                system: system.clone(),
                messages: transcript.clone(),
                tools: if last_step {
                    Vec::new()
                } else {
                    self.registry.definitions()
                },
            };

            let (ending, text, calls) = self
                .run_completion(request, &events, &mut total_usage)
                .await?;

            match ending {
                // A tool-call stop with nothing runnable — no calls delivered,
                // or the cap already spent — falls through to the end-turn arm:
                // there is nothing to run, so whatever text arrived is the reply.
                Ending::Done(Stop::ToolCalls) if !last_step && !calls.is_empty() => {
                    steps += 1;
                    span.record("tool_steps", steps);
                    self.tool_step(&session_id, &turn_id, text, calls, &mut transcript, &events)
                        .await?;
                }
                Ending::Done(_) => {
                    let seq = self.append_reply(&session_id, &turn_id, &text, false)?;
                    span.record("outcome", "done");
                    span.record("assistant_seq", seq);
                    return Ok(Reply {
                        session_id,
                        seq,
                        usage: total_usage,
                        partial: false,
                    });
                }
                Ending::Cut if text.is_empty() => {
                    span.record("outcome", "error");
                    return Err(Error::EmptyReply);
                }
                Ending::Cut => {
                    let seq = self.append_reply(&session_id, &turn_id, &text, true)?;
                    span.record("outcome", "partial");
                    span.record("assistant_seq", seq);
                    return Ok(Reply {
                        session_id,
                        seq,
                        usage: None,
                        partial: true,
                    });
                }
                Ending::Failed(error) => {
                    // The text seen so far is appended before the error is
                    // surfaced. An append failure takes precedence, because a
                    // durability problem outranks a provider problem.
                    if !text.is_empty() {
                        let seq = self.append_reply(&session_id, &turn_id, &text, true)?;
                        span.record("assistant_seq", seq);
                    }
                    span.record("outcome", "error");
                    return Err(error.into());
                }
            }
        }
    }

    /// Drives one completion to its end: text and reasoning forwarded as they
    /// arrive, calls collected whole, usage summed into the turn's total.
    // `&mut self` for the future's `Send` bound, not for mutation: a shared
    // borrow held across an await would demand `Engine: Sync`, and the
    // projection's SQLite connection is not.
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
                // The stream contract says `Done` is the last item; trusting
                // it saves a poll that could only return `None`.
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

    /// One tool step
    async fn tool_step(
        &mut self,
        session_id: &str,
        turn_id: &str,
        text: String,
        mut calls: Vec<ToolCall>,
        transcript: &mut Vec<Message>,
        events: &mpsc::Sender<EngineEvent>,
    ) -> Result<(), Error> {
        // Step text is rare.
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
        // an id the provider left empty, or one this
        // session has already logged, is replaced with a created one,
        // the log then records the id that was actually used, everywhere.
        let seen = self
            .issued_call_ids
            .entry(session_id.to_owned())
            .or_default();
        for call in &mut calls {
            if call.id.is_empty() || seen.contains(&call.id) {
                call.id = uuid::Uuid::new_v4().to_string();
            }
            seen.insert(call.id.clone());
        }

        // Write-ahead: the whole step's calls are durable before any of them
        // runs. Nothing ran that is not on disk.
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
            // A write tool's events go durable before the result that says
            // "saved" — the report must follow the write it reports.
            for memory_event in dispatched.memory_events {
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

    /// The system prompt as sent: identity file, then the turn's memory
    /// index block, then `/no_think` — each only when present, `/no_think`
    /// always last.
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

    /// Every session, oldest first (see [`Projection::sessions`]).
    ///
    /// The engine is the façade over durable state: callers above it — the
    /// daemon's socket layer — never reach past it to the log or the index.
    ///
    /// # Errors
    ///
    /// [`Error::Projection`] if the index cannot be read.
    pub fn sessions(&self) -> Result<Vec<SessionSummary>, Error> {
        Ok(self.projection.sessions()?)
    }

    /// One session's messages as a client renders them, oldest first, as
    /// `(role, content, partial)` rows.
    ///
    /// Prose only: tool rows reach the wire with task 5.1b. Unlike the
    /// private rebuild, this keeps every role verbatim: a client showing a
    /// transcript should not silently drop a message the provider vocabulary
    /// happens not to cover.
    ///
    /// An unknown id reads as a session with nothing in it — the projection
    /// has no row to distinguish "never existed" from "never spoken in", and
    /// the difference does not change what a client renders.
    ///
    /// # Errors
    ///
    /// [`Error::Projection`] if the index cannot be read.
    pub fn transcript(&self, session_id: &str) -> Result<Vec<(i32, String, bool)>, Error> {
        Ok(self
            .projection
            .messages(session_id)?
            .into_iter()
            .filter_map(|row| match row {
                MessageRow::Message {
                    role,
                    content,
                    partial,
                    ..
                } => Some((role, content, partial)),
                MessageRow::ToolCall { .. } | MessageRow::ToolResult { .. } => None,
            })
            .collect())
    }

    /// Appends one event to the log and applies it to the index, as a pair.
    ///
    /// The log stamps the seq; the same event, seq included, then goes into
    /// the projection so both stay in lockstep. Replay remains the cold-start
    /// path only.
    fn record(&mut self, source: Source, payload: session_event::Event) -> Result<u64, Error> {
        let mut event = Event {
            seq: 0, // added by the log
            ts: Some(now_ts()),
            source: source as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(payload),
            })),
        };
        let seq = self.log.append(event.clone())?;
        event.seq = seq;
        self.projection.apply(&event)?;
        Ok(seq)
    }

    /// [`Engine::record`] for the memory arm: appends one `MemoryEvent` and
    /// applies it, so a mid-turn `Archive` read sees the write.
    fn record_memory(
        &mut self,
        source: Source,
        payload: memory_event::Event,
    ) -> Result<u64, Error> {
        let mut event = Event {
            seq: 0, // added by the log
            ts: Some(now_ts()),
            source: source as i32,
            payload: Some(event::Payload::Memory(MemoryEvent {
                event: Some(payload),
            })),
        };
        let seq = self.log.append(event.clone())?;
        event.seq = seq;
        self.projection.apply(&event)?;
        Ok(seq)
    }

    /// Appends the model's reply.
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

    /// The transcript a turn starts from and the turn's one system prompt,
    /// read once: the rows rebuild the provider messages and seed the
    /// session's collision set; the memory index is snapshotted here and
    /// holds through the turn's completions, so mid-turn memory writes reach
    /// disk, not the live prompt (DESIGN.md §5.2).
    fn open_turn(&mut self, session_id: &str) -> Result<(Vec<Message>, Option<String>), Error> {
        let rows = self.projection.messages(session_id)?;
        self.seed_call_ids(session_id, &rows);
        let system =
            self.system_prompt(render_memory_index(&self.projection.memory_index()?).as_deref());
        Ok((rebuild_transcript(&rows), system))
    }

    /// Seeds the session's collision set from its projected call rows.
    ///
    /// The in-process set forgets everything at a restart; the log does not.
    /// Without this, a provider id that collides with a call an earlier
    /// daemon run logged would slip past `tool_step`'s check and put the
    /// same string on two calls of one session (DESIGN.md §3.1).
    fn seed_call_ids(&mut self, session_id: &str, rows: &[MessageRow]) {
        self.issued_call_ids
            .entry(session_id.to_owned())
            .or_insert_with(|| {
                rows.iter()
                    .filter_map(|row| match row {
                        MessageRow::ToolCall { call_id, .. } => Some(call_id.clone()),
                        _ => None,
                    })
                    .collect()
            });
    }
}

/// The provider transcript a session's projected rows imply (DESIGN.md §3.1).
///
/// Prose becomes [`Message::Text`]. A step — consecutive call rows of one
/// turn, §3.1's grouping rule — becomes one [`Message::ToolCalls`], followed
/// by its results sorted by the `call_index` of the call each one closes, not
/// completion order. Results are paired by `call_id` alone, never adjacency:
/// the orphan closer lands at the log tail, possibly far from its call.
///
/// Anomalies are skipped with a warning, never a panic, matching the engine's
/// tolerance for foreign logs: an unmappable role, a result no call claimed,
/// and a call with no result — legitimate only before arcd's orphan closer
/// has run, and this reader appends no repair (§3.1: only arcd at startup
/// may). A skipped call keeps the transcript valid: every call the provider
/// sees has a result answering it.
fn rebuild_transcript(rows: &[MessageRow]) -> Vec<Message> {
    // Results first, keyed by call id, so a step finds its answers wherever
    // they landed in the log. First result wins; the log never holds two.
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
                // One step: this turn's consecutive call rows.
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
                // Emitted beside its step above; alone it answers nothing.
                if !issued.contains(call_id.as_str()) {
                    tracing::warn!(%call_id, "skipping a tool result no call claimed");
                }
                i += 1;
            }
        }
    }
    messages
}

/// How one completion of the loop ended; see the module docs.
enum Ending {
    Done(Stop),
    Cut,
    Failed(provider::Error),
}

/// The current wall clock as a protobuf timestamp.
///
/// Clock-reading lives with the writers — the engine and the orphan closer —
/// because the log deliberately stamps nothing but seq.
pub(crate) fn now_ts() -> Timestamp {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp {
        seconds: i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(elapsed.subsec_nanos()).unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arc_proto::v1::{
        MemoryRecord, MemoryRecordCreated, Role, Source, ToolOutcome, memory_event, memory_record,
        session_event,
    };
    use tempfile::TempDir;

    use super::{Engine, EngineEvent, Error, MAX_TOOL_STEPS};
    use crate::log::Log;
    use crate::projection::Projection;
    use crate::provider::{
        CompletionDelta, Error as ProviderError, Message, Stop, ToolCall, Usage,
    };
    use crate::testkit::{
        Canned, ScriptedProvider, appended, call, channel, done_reply, drain, engine,
        engine_with_tools, issued, reopened_engine, replay_log, resulted, seed_log,
        seed_memory_log, tool_stop, tools, turn, usage,
    };
    use crate::tool::Registry;

    // --- seed events, for histories a live engine cannot produce ---

    fn seeded_session() -> session_event::Event {
        session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
            session_id: "s-01".to_owned(),
            title: String::new(),
            provider: "scripted".to_owned(),
            model: "test-model".to_owned(),
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

        let events = replay_log(&dir);
        assert_eq!(events.len(), 3);
        let session_event::Event::SessionCreated(created) = &events[0] else {
            panic!("expected SessionCreated first, got {:?}", events[0]);
        };
        assert_eq!(created.session_id, reply.session_id);
        assert_eq!(created.provider, "scripted");
        assert_eq!(created.model, "test-model");

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

        // The projection kept pace without a replay.
        assert_eq!(engine.projection.last_seq().expect("last_seq"), Some(2));
        assert_eq!(
            engine
                .projection
                .messages(&reply.session_id)
                .expect("messages")
                .len(),
            2
        );

        // Channel saw Accepted first, then the delta.
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

        let events = replay_log(&dir);
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
        // Both were created inside the same second, so their relative order is
        // whatever the tie-break says; membership is what this asserts.
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
        let events = replay_log(&dir);
        let assistant = appended(&events[2]);
        assert!(assistant.partial);
        assert_eq!(assistant.content, "partial tex");

        // The flag reaches clients too: a reopened session can tell a cut
        // reply from a whole one.
        assert_eq!(
            engine.transcript(&reply.session_id).expect("transcript"),
            [
                (Role::User as i32, "hi".to_owned(), false),
                (Role::Assistant as i32, "partial tex".to_owned(), true),
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
        let events = replay_log(&dir);
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
        let events = replay_log(&dir);
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
        assert_eq!(replay_log(&dir).len(), 2);
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
        let events = replay_log(&dir);
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
        assert_eq!(replay_log(&dir).len(), 0, "log untouched");
        assert!(provider.requests().is_empty(), "provider never called");
    }

    #[tokio::test]
    async fn an_unmappable_role_in_history_is_skipped() {
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        let mut engine = engine(&provider, &dir);

        // A message from a newer schema, planted through the engine's own
        // log-and-index pair so both stay consistent.
        engine
            .record(
                Source::System,
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: "s-old".to_owned(),
                    title: String::new(),
                    provider: "scripted".to_owned(),
                    model: "test-model".to_owned(),
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

        // Two completions billed; the reply sums them.
        assert_eq!(
            reply.usage,
            Some(Usage {
                input_tokens: 6,
                output_tokens: 10
            })
        );

        let events = replay_log(&dir);
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

        // The second completion saw the calls and their results, in order.
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

        // The caller watched the whole turn.
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
        // The provider delivers index 1 first; the log and transcript sort it.
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

        let events = replay_log(&dir);
        // Both calls durable before either result: the write-ahead rule.
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

        let events = replay_log(&dir);
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

        // Every result names the id the log recorded, minted or not.
        let result_ids: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                session_event::Event::ToolResultRecorded(r) => Some(r.call_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(result_ids, [ids[0], ids[1], ids[2]]);
    }

    /// The in-process collision set dies with the daemon; the log does not.
    /// A reopened engine must reject an id an earlier run already logged.
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

        let ids: Vec<String> = replay_log(&dir)
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

    /// The log records results in completion order; the rebuilt transcript
    /// sorts them by the index of the call each closes (DESIGN.md §3.1) —
    /// the replay side of the `index 1 first` engine test.
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
                // b finished first; the log says so.
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

    /// A call the orphan closer has not reached and a result no call claims
    /// are both skipped with a warning: the transcript stays valid and the
    /// turn goes on.
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

    /// The flipped 4.2 gap, focused: a daemon restart loses nothing — the
    /// next request over a replayed projection carries the whole tool step.
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

        let events = replay_log(&dir);
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

        let events = replay_log(&dir);
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
        let events = replay_log(&dir);
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
        // Nothing durable carries it: session, user, assistant, and that's all.
        let events = replay_log(&dir);
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

        let events = replay_log(&dir);
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
        let projection = Projection::open(":memory:").expect("open projection");
        let mut engine = Engine::new(
            log,
            projection,
            Arc::clone(&provider),
            "test-model",
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

    // --- the always-loaded memory index (DESIGN.md §5.2) ---

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

    /// What [`seeded_records`] must render as, exactly.
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
        let mut projection = Projection::open(":memory:").expect("open projection");
        crate::projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
        let mut engine = Engine::new(
            log,
            projection,
            Arc::clone(&provider),
            "test-model",
            None,
            Registry::new(512),
            false,
        );
        let (tx, _rx) = channel();

        engine.send_message(None, "hi", tx).await.expect("send");

        assert_eq!(provider.requests()[0].system, Some(seeded_block()));
    }

    #[tokio::test]
    async fn no_think_lands_after_the_block() {
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, seeded_records());
        let log = Log::open(dir.path()).expect("open log");
        let mut projection = Projection::open(":memory:").expect("open projection");
        crate::projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
        let mut engine = Engine::new(
            log,
            projection,
            Arc::clone(&provider),
            "test-model",
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
}
