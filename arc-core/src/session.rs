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
    Event, MemoryEvent, MemoryRecordDeleted, MemoryRecordReviewed, MessageAppended, Role,
    SessionConsolidated, SessionCreated, SessionEvent, Source, ToolCallIssued, ToolOutcome,
    ToolResultRecorded,
};
use arc_proto::v1::{event, memory_event, session_event};
use futures::StreamExt as _;
use prost_types::Timestamp;
use tokio::sync::mpsc;

use crate::consolidation::SessionSnapshot;
use crate::log::{self, Log};
use crate::memory::render_memory_index;
use crate::projection::{self, DueSession, MessageRow, Projection, ReviewItem, SessionSummary};
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

    /// A review verdict named a record the projection does not hold. Nothing
    /// was appended; the daemon maps this to a wire error frame.
    #[error("no memory record {id} to review")]
    UnknownRecord {
        /// The id the verdict carried.
        id: String,
    },

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
    // One turn is one function; splitting it would hide the loop's shape.
    #[allow(clippy::too_many_lines)]
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
        let mut memory = MemoryCounters::default();

        // Terminal arms break so the turn's memory counters are recorded
        // exactly once; a `?` escape (durability, eager provider failure)
        // skips them, which an aggregate metric tolerates.
        let reply = loop {
            let last_step = steps >= MAX_TOOL_STEPS;
            let request = self.completion_request(system.clone(), transcript.clone(), last_step);

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
                    // The text seen so far is appended before the error is
                    // surfaced. An append failure takes precedence, because a
                    // durability problem outranks a provider problem.
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
            memory.observe_call(&call.name, &call.arguments, &dispatched.content);
            // A write tool's events go durable before the result that says
            // "saved" — the report must follow the write it reports.
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

    /// Sessions due for consolidation (see
    /// [`Projection::due_for_consolidation`]) — the engine is the façade over
    /// durable state, here as everywhere.
    ///
    /// # Errors
    ///
    /// [`Error::Projection`] if the index cannot be read.
    pub fn due_for_consolidation(&self, idle_cutoff_micros: i64) -> Result<Vec<DueSession>, Error> {
        Ok(self.projection.due_for_consolidation(idle_cutoff_micros)?)
    }

    /// The review queue (see [`Projection::review_items`]): records changed
    /// at or after `since_micros` and not reviewed since their last change.
    ///
    /// # Errors
    ///
    /// [`Error::Projection`] if the index cannot be read.
    pub fn review_items(&self, since_micros: i64) -> Result<Vec<ReviewItem>, Error> {
        Ok(self.projection.review_items(since_micros)?)
    }

    /// The accept verdict (DESIGN.md §5.4): appends a `MemoryRecordReviewed`
    /// with [`Source::User`] — durable, because reviews are the ground truth
    /// the precision labels rest on.
    ///
    /// # Errors
    ///
    /// - [`Error::UnknownRecord`] if the projection does not hold `record_id`;
    ///   nothing is appended.
    /// - [`Error::Log`] / [`Error::Projection`] if durability fails; fatal.
    #[tracing::instrument(name = "session.review_accept", skip(self), fields(record_id))]
    pub fn review_accept(&mut self, record_id: &str) -> Result<(), Error> {
        self.reviewable(record_id)?;
        self.record_memory(
            Source::User,
            memory_event::Event::RecordReviewed(MemoryRecordReviewed {
                record_id: record_id.to_owned(),
            }),
        )?;
        Ok(())
    }

    /// The delete verdict (DESIGN.md §5.4): appends a `MemoryRecordDeleted`
    /// with [`Source::User`], excluding the record entirely.
    ///
    /// # Errors
    ///
    /// - [`Error::UnknownRecord`] if the projection does not hold `record_id`;
    ///   nothing is appended.
    /// - [`Error::Log`] / [`Error::Projection`] if durability fails; fatal.
    #[tracing::instrument(name = "session.review_delete", skip(self), fields(record_id))]
    pub fn review_delete(&mut self, record_id: &str) -> Result<(), Error> {
        self.reviewable(record_id)?;
        self.record_memory(
            Source::User,
            memory_event::Event::RecordDeleted(MemoryRecordDeleted {
                id: record_id.to_owned(),
            }),
        )?;
        Ok(())
    }

    /// Refuses a verdict for a record the projection does not hold, before
    /// anything touches the log.
    fn reviewable(&self, record_id: &str) -> Result<(), Error> {
        if self.projection.memory_record(record_id)?.is_none() {
            return Err(Error::UnknownRecord {
                id: record_id.to_owned(),
            });
        }
        Ok(())
    }

    /// Step 1 of the consolidation pass (DESIGN.md §5.4), under the caller's
    /// lock: the first due session outside `skip`, with its rows, its last
    /// seq, and the ACTIVE memory index — the extractor's entire view of
    /// existing memory, read in the same locked step. `None` when nothing is
    /// due. One session at a time is v1's concurrency bound; `skip` is the
    /// caller's strike list, so a forever-failing session yields the slot to
    /// the next due one.
    ///
    /// # Errors
    ///
    /// [`Error::Projection`] if the index cannot be read.
    pub fn snapshot_for_consolidation(
        &self,
        idle_cutoff_micros: i64,
        skip: &HashSet<String>,
    ) -> Result<Option<SessionSnapshot>, Error> {
        let Some(first) = self
            .projection
            .due_for_consolidation(idle_cutoff_micros)?
            .into_iter()
            .find(|due| !skip.contains(&due.session_id))
        else {
            return Ok(None);
        };
        let rows = self.projection.messages(&first.session_id)?;
        let memory_index = self.projection.memory_index()?;
        Ok(Some(SessionSnapshot {
            session_id: first.session_id,
            rows,
            latest_seq: first.latest_seq,
            memory_index,
        }))
    }

    /// Step 3 of the consolidation pass, back under the caller's lock:
    /// re-checks the session against the snapshot, then appends the
    /// extractor's events and the coverage marker. Returns `false` — a race,
    /// not an error — when the session grew since the snapshot: the pass is
    /// discarded whole and a later idle timeout re-runs it over the longer
    /// history.
    ///
    /// Everything appends with [`Source::System`]: arcd initiated these
    /// writes, not the user's turn (§5.4 amendment).
    ///
    /// # Errors
    ///
    /// [`Error::Log`] / [`Error::Projection`] if an append fails; durability
    /// is in doubt and the caller should treat this as fatal for the pass.
    pub fn commit_consolidation(
        &mut self,
        snapshot: &SessionSnapshot,
        events: Vec<memory_event::Event>,
        prompt_version: &str,
    ) -> Result<bool, Error> {
        let latest = self.projection.latest_seq(&snapshot.session_id)?;
        if latest != Some(snapshot.latest_seq) {
            tracing::info!(
                session_id = %snapshot.session_id,
                snapshot_seq = snapshot.latest_seq,
                latest_seq = latest,
                "session grew during consolidation; discarding the pass"
            );
            return Ok(false);
        }
        for event in events {
            self.record_memory(Source::System, event)?;
        }
        self.record(
            Source::System,
            session_event::Event::SessionConsolidated(SessionConsolidated {
                session_id: snapshot.session_id.clone(),
                through_seq: snapshot.latest_seq,
                prompt_version: prompt_version.to_owned(),
            }),
        )?;
        Ok(true)
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

    /// One step's completion request. After the last allowed tool step no
    /// tools are offered, so the model can only answer in prose. Interactive
    /// turns never pin a seed; that dial is replay's (task 7.3).
    fn completion_request(
        &self,
        system: Option<String>,
        messages: Vec<Message>,
        last_step: bool,
    ) -> CompletionRequest {
        CompletionRequest {
            model: self.model.clone(),
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

/// One turn's memory-traffic counters (DESIGN.md §5.4), fed by the tool loop
/// and recorded on the turn's span at its end.
#[derive(Debug, Default)]
struct MemoryCounters {
    /// Record ids surfaced by this turn's `memory_search` replies.
    surfaced: HashSet<String>,
    searches: u64,
    search_hits: u64,
    reads_from_search: u64,
    records_created: u64,
    records_superseded: u64,
}

/// The one part of a `memory_search` reply the counters read.
#[derive(serde::Deserialize)]
struct SearchReplyIds {
    records: Vec<SearchReplyId>,
}

#[derive(serde::Deserialize)]
struct SearchReplyId {
    id: String,
}

/// The one part of a `memory_read` call the counters read.
#[derive(serde::Deserialize)]
struct ReadArgsId {
    id: String,
}

impl MemoryCounters {
    /// Retrieval hit rate is a proxy: a `memory_read` whose id appeared in an
    /// earlier `memory_search` reply this turn counts as the search being
    /// used. Known undercount — the model can act on a summary straight from
    /// the search reply without ever reading the body.
    fn observe_call(&mut self, name: &str, arguments_json: &str, result_content: &str) {
        match name {
            "memory_search" => {
                self.searches += 1;
                // The no-match reply is prose, not JSON: not a hit.
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

    /// Counts a memory event a tool asked the engine to append.
    fn observe_event(&mut self, event: &memory_event::Event) {
        match event {
            memory_event::Event::RecordCreated(_) => self.records_created += 1,
            memory_event::Event::RecordSuperseded(_) => self.records_superseded += 1,
            _ => {}
        }
    }

    /// Absent means "no memory traffic": zeros land only where a denominator
    /// exists — never on turns that touched no memory (§5.4). Rates are for
    /// analysis time; a ratio in a trace would hide its sample size.
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
        MemoryRecord, MemoryRecordCreated, MemoryRecordSuperseded, Role, Source, ToolOutcome,
        memory_event, memory_record, session_event,
    };
    use tempfile::TempDir;

    use super::{Engine, EngineEvent, Error, MAX_TOOL_STEPS, MemoryCounters};
    use crate::log::Log;
    use crate::projection::Projection;
    use crate::provider::{
        CompletionDelta, Error as ProviderError, Message, Stop, ToolCall, Usage,
    };
    use crate::testkit::{
        Canned, ScriptedProvider, TraceCapture, appended, call, channel, counter_samples,
        done_reply, drain, engine, engine_with_tools, issued, reopened_engine, replay_events,
        replay_log, resulted, seed_log, seed_memory_log, seed_memory_log_at, tool_stop, tools,
        turn, usage,
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

    // --- the §5.4 memory counters on the turn's span ---

    /// A `memory_search` hit reply, as the real tool shapes it.
    const SEARCH_HIT: &str = r#"{"records":[{"id":"mr-pal","namespace":"global","kind":"fact","title":"Gruvbox","summary":"the palette"}]}"#;

    /// The §5.4 counter names a turn can record.
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
        /// A scripted supersede: replies like the real tool and carries the
        /// event the engine appends.
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

    // --- the weekly review (DESIGN.md §5.4) ---

    /// An engine over [`seeded_records`] stamped into the log at a fixed
    /// clock, so the records sit in every review window.
    fn review_engine(provider: &Arc<ScriptedProvider>, dir: &TempDir) -> Engine<ScriptedProvider> {
        seed_memory_log_at(dir, seeded_records(), 1_700_000_000_000_000);
        reopened_engine(provider, dir, Registry::new(512))
    }

    /// The memory payload of one whole log event.
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

        engine.review_accept("mr-fact").expect("accept");

        let events = replay_events(&dir);
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

        engine.review_delete("mr-pref").expect("delete");

        let events = replay_events(&dir);
        let verdict = events.last().expect("the verdict");
        assert_eq!(verdict.source, Source::User as i32);
        match memory_payload(verdict) {
            memory_event::Event::RecordDeleted(deleted) => assert_eq!(deleted.id, "mr-pref"),
            other => panic!("expected RecordDeleted, got {other:?}"),
        }

        let queued: Vec<String> = engine
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
        let before = replay_events(&dir).len();

        let accept = engine.review_accept("mr-ghost");
        assert!(
            matches!(accept, Err(Error::UnknownRecord { ref id }) if id == "mr-ghost"),
            "got: {accept:?}"
        );
        let delete = engine.review_delete("mr-ghost");
        assert!(
            matches!(delete, Err(Error::UnknownRecord { ref id }) if id == "mr-ghost"),
            "got: {delete:?}"
        );

        assert_eq!(replay_events(&dir).len(), before, "nothing was appended");
    }
}
