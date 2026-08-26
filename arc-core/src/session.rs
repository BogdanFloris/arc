use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

use arc_proto::v1::{
    Budget, MemoryEvent, MessageAppended, Role, SessionCreated, SessionEvent, SessionRole, Source,
    ToolCallIssued, ToolOutcome, ToolResultRecorded, WorkspaceGrant,
};
use arc_proto::v1::{event, memory_event, session_event};
use futures::StreamExt as _;

use tokio::sync::mpsc;

use crate::memory::render_memory_index;
use crate::projection::{self, MessageRow, SessionSummary};
use crate::provider::{
    self, CompletionDelta, CompletionRequest, Message, Provider, Stop, Thinking, ToolCall, Usage,
};
use crate::store::{self, Store, now_ts};
use crate::tool::workspace::{Grant, Grants, Mode};
use crate::tool::{DispatchOutcome, Registry, ToolSource, TurnContext};

const MAX_TOOL_STEPS: usize = 8;

/// What a project offers a session bound to it: the tools and the roots.
#[derive(Debug, Clone, Default)]
pub struct ProjectSpec {
    pub sources: Vec<ToolSource>,
    pub grants: Vec<Grant>,
}

/// Who is running a turn. Resolved once per role and handed to the engine
/// with each turn, so one log can serve a conversation and a job at once.
#[derive(Clone, Debug)]
pub struct Runner {
    pub role: SessionRole,
    pub provider: Arc<dyn Provider>,
    pub model: String,
    pub thinking: Thinking,
    /// The identity file. Concierge only: a job has no voice to pay for.
    pub system: Option<String>,
}

// never held across an .await: it fences a single append batch or
// projection read, so concurrent turns interleave everywhere else
pub struct Engine {
    store: StdMutex<Store>,
    registry: Registry,
    projects: BTreeMap<String, ProjectSpec>,
    // one guard per session, held for a whole turn: turns in the same
    // session serialize, turns in different sessions run concurrently
    turns: StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
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

    #[error("session {session_id} is pinned to the {pinned} role; this engine serves {serving}")]
    RoleMismatch {
        session_id: String,
        pinned: String,
        serving: String,
    },

    #[error("the model produced no reply")]
    EmptyReply,

    #[error("project {project} is not configured")]
    UnknownProject { project: String },

    #[error("project {project}: could not resolve its granted roots: {source}")]
    Grants {
        project: String,
        #[source]
        source: std::io::Error,
    },
}

impl Engine {
    pub fn new(store: Store, registry: Registry) -> Self {
        Self {
            store: StdMutex::new(store),
            registry,
            projects: BTreeMap::new(),
            turns: StdMutex::new(HashMap::new()),
        }
    }

    pub(crate) fn with_store<T>(&self, f: impl FnOnce(&Store) -> T) -> T {
        let store = self.store.lock().expect("store lock poisoned");
        f(&store)
    }

    pub(crate) fn with_store_mut<T>(&self, f: impl FnOnce(&mut Store) -> T) -> T {
        let mut store = self.store.lock().expect("store lock poisoned");
        f(&mut store)
    }

    /// The verdict on a proposed memory record: accept the review queue's
    /// suggestion, or delete the record outright.
    pub fn review_accept(&self, record_id: &str) -> Result<(), Error> {
        Ok(self.with_store_mut(|store| store.review_accept(record_id))?)
    }

    pub fn review_delete(&self, record_id: &str) -> Result<(), Error> {
        Ok(self.with_store_mut(|store| store.review_delete(record_id))?)
    }

    fn turn_guard(&self, session_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut turns = self.turns.lock().expect("turns lock poisoned");
        Arc::clone(
            turns
                .entry(session_id.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// The configured projects: name to sources and grants. Written once at
    /// startup from config; a session's project resolves through this.
    #[must_use]
    pub fn with_projects(mut self, projects: BTreeMap<String, ProjectSpec>) -> Self {
        self.projects = projects;
        self
    }

    /// A new session bound to a project: grants are canonicalized now and
    /// recorded in the log, so the session keeps them even if config changes.
    /// `runner` supplies the recorded provider and model; `role` and `budget`
    /// are what the new session is pinned to, which is `runner.role` for a
    /// session the runner starts for itself but differs for a dispatched
    /// job, where the runner is the dispatching parent, not the child.
    #[tracing::instrument(
        level = "info",
        name = "session.create_bound_session",
        skip_all,
        fields(project, session_id = tracing::field::Empty)
    )]
    pub fn create_bound_session(
        &self,
        runner: &Runner,
        project: &str,
        role: SessionRole,
        budget: Option<Budget>,
    ) -> Result<String, Error> {
        let spec = self
            .projects
            .get(project)
            .cloned()
            .ok_or_else(|| Error::UnknownProject {
                project: project.to_owned(),
            })?;
        let grants = Grants::new(spec.grants).map_err(|source| Error::Grants {
            project: project.to_owned(),
            source,
        })?;

        let session_id = uuid::Uuid::new_v4().to_string();
        tracing::Span::current().record("session_id", session_id.as_str());
        // a job session exists because a model asked for it
        self.record(
            Source::Model,
            session_event::Event::SessionCreated(SessionCreated {
                session_id: session_id.clone(),
                title: String::new(),
                provider: runner.provider.name().to_owned(),
                model: runner.model.clone(),
                role: role as i32,
                project: project.to_owned(),
                budget,
                grants: grants
                    .canonical_roots()
                    .iter()
                    .map(|(root, mode)| WorkspaceGrant {
                        root: root.to_string_lossy().into_owned(),
                        read_write: *mode == Mode::ReadWrite,
                    })
                    .collect(),
            }),
        )?;
        Ok(session_id)
    }

    /// Acts on a `dispatch` tool call: creates the child session durably and
    /// returns what the parent's tool result should say. The child does not
    /// start running here — that arrives with the supervised job task.
    fn dispatch_job(
        &self,
        runner: &Runner,
        job_request: crate::tool::JobRequest,
    ) -> (ToolOutcome, String) {
        let role = job_request.role;
        let project = job_request.project;
        match self.create_bound_session(runner, &project, role, job_request.budget) {
            Ok(child_id) => (
                ToolOutcome::Ok,
                format!(
                    "Dispatched {} into {project} as session {child_id}. The job has not \
                     started; job execution arrives with the supervised task.",
                    provider::role_label(role)
                ),
            ),
            Err(error) => (ToolOutcome::Error, format!("ERROR: {error}")),
        }
    }

    fn sources(&self, session_id: &str, new_session: bool) -> Result<Vec<ToolSource>, Error> {
        let project = if new_session {
            None
        } else {
            self.with_store(|store| store.projection().session_project(session_id))?
        };
        Ok(match project {
            None => vec![ToolSource::Builtin],
            Some(name) => {
                if let Some(spec) = self.projects.get(&name) {
                    spec.sources.clone()
                } else {
                    // fail closed: a project gone from config grants nothing
                    tracing::warn!(project = %name, "session names a project that is not configured");
                    vec![ToolSource::Builtin]
                }
            }
        })
    }

    /// The grants a session was created with, straight from the log: the
    /// authority even if config has since changed. `None` means unbound.
    fn grants(&self, session_id: &str, new_session: bool) -> Result<Option<Arc<Grants>>, Error> {
        if new_session {
            return Ok(None);
        }
        let recorded = self.with_store(|store| store.projection().session_grants(session_id))?;
        if recorded.is_empty() {
            return Ok(None);
        }
        let roots = recorded
            .into_iter()
            .map(|(root, read_write)| {
                let mode = if read_write {
                    Mode::ReadWrite
                } else {
                    Mode::ReadOnly
                };
                (PathBuf::from(root), mode)
            })
            .collect();
        Ok(Some(Arc::new(Grants::from_recorded(roots))))
    }

    #[tracing::instrument(
        level = "info",
        name = "session.send_message",
        skip_all,
        fields(
            model = %runner.model,
            role = provider::role_label(runner.role),
            thinking = runner.thinking.label(),
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
        &self,
        runner: &Runner,
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

        // held for the whole turn: same-session turns serialize here
        let guard = self.turn_guard(&session_id);
        let _turn = guard.lock().await;

        if !new_session {
            self.enforce_pin(runner, &session_id)?;
        }
        let sources = self.sources(&session_id, new_session)?;
        let grants = self.grants(&session_id, new_session)?;

        if new_session {
            self.record(
                Source::User,
                session_event::Event::SessionCreated(SessionCreated {
                    session_id: session_id.clone(),
                    title: String::new(),
                    provider: runner.provider.name().to_owned(),
                    model: runner.model.clone(),
                    role: runner.role as i32,
                    project: String::new(),
                    budget: None,
                    grants: Vec::new(),
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

        let (mut transcript, system) = self.open_turn(runner, &session_id)?;
        let mut total_usage: Option<Usage> = None;
        let mut steps = 0;
        let mut memory = MemoryCounters::default();

        let reply = loop {
            // the last step offers no tools, so the model has to answer
            let last_step = steps >= MAX_TOOL_STEPS;
            let request = self.completion_request(
                runner,
                system.clone(),
                transcript.clone(),
                last_step,
                &sources,
            );

            let (ending, text, calls) = self
                .run_completion(runner, request, &events, &mut total_usage)
                .await?;

            match ending {
                Ending::Done(Stop::ToolCalls) if !last_step && !calls.is_empty() => {
                    steps += 1;
                    span.record("tool_steps", steps);
                    self.tool_step(
                        runner,
                        &session_id,
                        &turn_id,
                        text,
                        calls,
                        &sources,
                        grants.as_ref(),
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
        &self,
        runner: &Runner,
        request: CompletionRequest,
        events: &mpsc::Sender<EngineEvent>,
        total_usage: &mut Option<Usage>,
    ) -> Result<(Ending, String, Vec<ToolCall>), Error> {
        let mut stream = runner.provider.complete(request).await?;
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
        &self,
        runner: &Runner,
        session_id: &str,
        turn_id: &str,
        text: String,
        mut calls: Vec<ToolCall>,
        sources: &[ToolSource],
        grants: Option<&Arc<Grants>>,
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
        let mut seen = self.with_store(|store| store.projection().call_ids(session_id))?;
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
                    provider_roundtrip: call.provider_roundtrip.clone(),
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
                grants: grants.cloned(),
            };
            let DispatchOutcome {
                content,
                ok,
                truncated,
                memory_events,
                job_request,
            } = self
                .registry
                .dispatch(&call.name, call.arguments.clone(), ctx, sources)
                .await;
            for memory_event in memory_events {
                memory.observe_event(&memory_event);
                self.record_memory(Source::Model, memory_event)?;
            }
            let (outcome, content) = if let Some(job_request) = job_request {
                self.dispatch_job(runner, job_request)
            } else {
                memory.observe_call(&call.name, &call.arguments, &content);
                (
                    if ok {
                        ToolOutcome::Ok
                    } else {
                        ToolOutcome::Error
                    },
                    content,
                )
            };
            self.record(
                Source::System,
                session_event::Event::ToolResultRecorded(ToolResultRecorded {
                    session_id: session_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    call_id: call.id.clone(),
                    outcome: outcome as i32,
                    content: content.clone(),
                    truncated,
                }),
            )?;
            let _ = events
                .send(EngineEvent::ToolCallEnded {
                    call_id: call.id.clone(),
                    outcome,
                })
                .await;
            results.push((call.id.clone(), content));
        }

        transcript.push(Message::ToolCalls(calls));
        for (call_id, content) in results {
            transcript.push(Message::ToolResult { call_id, content });
        }
        Ok(())
    }

    fn enforce_pin(&self, runner: &Runner, session_id: &str) -> Result<(), Error> {
        match self.with_store(|store| store.projection().session_role(session_id))? {
            Some(pinned) if pinned == runner.role as i32 => Ok(()),
            // sessions logged before roles exist stay unpinned
            Some(pinned) if pinned == SessionRole::Unspecified as i32 => Ok(()),
            Some(pinned) => Err(Error::RoleMismatch {
                session_id: session_id.to_owned(),
                pinned: role_name(pinned),
                serving: provider::role_label(runner.role).to_owned(),
            }),
            None => Ok(()),
        }
    }

    // stable first, volatile after: everything here is prefix the provider caches,
    // so anything that changes per turn has to go in the messages instead
    fn system_prompt(runner: &Runner, memory_index: Option<&str>) -> Option<String> {
        let mut parts: Vec<&str> = Vec::new();
        if let Some(identity) = &runner.system {
            parts.push(identity);
        }
        if let Some(index) = memory_index {
            parts.push(index);
        }
        let prompt = parts.join("\n\n");
        (!prompt.is_empty()).then_some(prompt)
    }

    pub fn sessions(&self) -> Result<Vec<SessionSummary>, Error> {
        Ok(self.with_store(|store| store.projection().sessions())?)
    }

    #[cfg(test)]
    pub(crate) fn transcript(
        &self,
        session_id: &str,
    ) -> Result<Vec<arc_proto::v1::HistoryEntry>, Error> {
        Ok(self
            .with_store(|store| store.projection().messages(session_id))?
            .into_iter()
            .map(projection::history_entry)
            .collect())
    }

    fn record(&self, source: Source, payload: session_event::Event) -> Result<u64, Error> {
        let payload = event::Payload::Session(SessionEvent {
            event: Some(payload),
        });
        Ok(self.with_store_mut(|store| store.append(source, Some(now_ts()), payload))?)
    }

    fn record_memory(&self, source: Source, payload: memory_event::Event) -> Result<u64, Error> {
        let payload = event::Payload::Memory(MemoryEvent {
            event: Some(payload),
        });
        Ok(self.with_store_mut(|store| store.append(source, Some(now_ts()), payload))?)
    }

    fn append_reply(
        &self,
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

    fn open_turn(
        &self,
        runner: &Runner,
        session_id: &str,
    ) -> Result<(Vec<Message>, Option<String>), Error> {
        let (rows, memory_index) = self.with_store(|store| {
            Ok::<_, Error>((
                store.projection().messages(session_id)?,
                store.projection().memory_index()?,
            ))
        })?;
        let system = Self::system_prompt(runner, render_memory_index(&memory_index).as_deref());
        Ok((rebuild_transcript(&rows), system))
    }

    fn completion_request(
        &self,
        runner: &Runner,
        system: Option<String>,
        messages: Vec<Message>,
        last_step: bool,
        sources: &[ToolSource],
    ) -> CompletionRequest {
        CompletionRequest {
            model: runner.model.clone(),
            role: runner.role,
            thinking: runner.thinking,
            system,
            messages,
            tools: if last_step {
                Vec::new()
            } else {
                self.registry.definitions(sources)
            },
            seed: None,
        }
    }
}

fn role_name(role: i32) -> String {
    match SessionRole::try_from(role) {
        Ok(role) => provider::role_label(role).to_owned(),
        Err(_) => format!("unknown role {role}"),
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
                    provider_roundtrip,
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
                        provider_roundtrip: provider_roundtrip.clone(),
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
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arc_proto::v1::{
        Budget, HistoryEntry, HistoryMessage, HistoryToolCall, HistoryToolResult, MemoryRecord,
        MemoryRecordCreated, MemoryRecordSuperseded, Role, SessionRole, Source, ToolOutcome,
        history_entry, memory_event, memory_record, session_event,
    };
    use tempfile::TempDir;

    use super::{Engine, EngineEvent, Error, MAX_TOOL_STEPS, MemoryCounters, ProjectSpec, Runner};
    use crate::log::Log;
    use crate::projection::Projection;
    use crate::provider::{
        CompletionDelta, Error as ProviderError, Message, Provider, Stop, Thinking, ToolCall, Usage,
    };
    use crate::store::{self, Store};
    use crate::testkit::{
        Canned, ScriptedProvider, Step, TraceCapture, appended, call, call_carrying, channel,
        counter_samples, done_reply, drain, engine, engine_with_tools, engine_with_tools_at,
        issued, reopened_engine, replay_events, replay_log, resulted, runner, seed_log,
        seed_memory_log, seed_memory_log_at, tool_stop, tools, turn, usage,
    };
    use crate::tool::builtin::dispatch::Dispatch;
    use crate::tool::workspace::{self, Grant, Mode, Workspace};
    use crate::tool::{JobRequest, Registry, Tool, ToolReply, ToolSource, TurnContext};

    fn seeded_session() -> session_event::Event {
        session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
            session_id: "s-01".to_owned(),
            title: String::new(),
            provider: "scripted".to_owned(),
            model: "test-model".to_owned(),
            role: arc_proto::v1::SessionRole::Unspecified as i32,
            project: String::new(),
            budget: None,
            grants: Vec::new(),
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

    fn seeded_session_with_project(project: &str) -> session_event::Event {
        session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
            session_id: "s-01".to_owned(),
            title: String::new(),
            provider: "scripted".to_owned(),
            model: "test-model".to_owned(),
            role: SessionRole::Unspecified as i32,
            project: project.to_owned(),
            budget: None,
            grants: Vec::new(),
        })
    }

    fn reopened_engine_with_projects(
        provider: &Arc<ScriptedProvider>,
        dir: &TempDir,
        registry: Registry,
        projects: BTreeMap<String, ProjectSpec>,
    ) -> (Engine, Runner) {
        let log = Log::open(dir.path()).expect("open log");
        let mut projection = Projection::in_memory().expect("open projection");
        crate::projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
        (
            Engine::new(Store::new(log, projection), registry).with_projects(projects),
            runner(provider),
        )
    }

    fn workspace_tool(name: &'static str, content: &'static str) -> Box<dyn crate::tool::Tool> {
        Box::new(Canned {
            name,
            content,
            ok: true,
            source: ToolSource::Workspace,
        })
    }

    #[tokio::test]
    async fn a_new_session_logs_created_user_and_assistant() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello there")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, mut rx) = channel();

        let reply = engine
            .send_message(&run, None, "hi", tx)
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
            engine
                .with_store(|store| store.projection().last_seq())
                .expect("last_seq"),
            Some(2)
        );
        assert_eq!(
            engine
                .with_store(|store| store.projection().messages(&reply.session_id))
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
        let (engine, run) = engine(&provider, &dir);

        let (tx, _rx) = channel();
        let first = engine
            .send_message(&run, None, "one", tx)
            .await
            .expect("first send");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&first.session_id), "two", tx)
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
    async fn a_session_pinned_to_another_role_refuses_and_logs_nothing() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: "s-01".to_owned(),
                    title: String::new(),
                    provider: "scripted".to_owned(),
                    model: "test-model".to_owned(),
                    role: SessionRole::Executor as i32,
                    project: String::new(),
                    budget: None,
                    grants: Vec::new(),
                }),
                seeded_message(Role::User, "earlier"),
            ],
        );
        let provider = ScriptedProvider::scripted(vec![done_reply("never sent")]);
        let (engine, run) = reopened_engine(&provider, &dir, Registry::new(512));
        let (tx, _rx) = channel();

        let err = engine
            .send_message(&run, Some("s-01"), "continue", tx)
            .await
            .expect_err("a concierge engine must refuse an executor session");

        assert!(matches!(err, Error::RoleMismatch { .. }), "got: {err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains("executor") && msg.contains("concierge"),
            "the refusal names both roles: {msg}"
        );
        assert_eq!(
            replay_log(dir.path()).len(),
            2,
            "the refusal appended nothing"
        );
        assert!(provider.requests().is_empty(), "the provider never ran");
    }

    #[tokio::test]
    async fn a_session_from_before_roles_stays_continuable() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![seeded_session(), seeded_message(Role::User, "earlier")],
        );
        let provider = ScriptedProvider::scripted(vec![done_reply("continued")]);
        let (engine, run) = reopened_engine(&provider, &dir, Registry::new(512));
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, Some("s-01"), "again", tx)
            .await
            .expect("a session logged before roles exist pins nothing");

        assert_eq!(reply.session_id, "s-01");
        let requests = provider.requests();
        let turns: Vec<(Role, &str)> = requests[0].messages.iter().map(turn).collect();
        assert_eq!(turns, [(Role::User, "earlier"), (Role::User, "again")]);
    }

    #[tokio::test]
    async fn sessions_lists_what_send_message_created() {
        let provider = ScriptedProvider::scripted(vec![done_reply("one"), done_reply("two")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        assert_eq!(engine.sessions().expect("sessions"), []);

        let (tx, _rx) = channel();
        let first = engine
            .send_message(&run, None, "a", tx)
            .await
            .expect("first send");
        let (tx, _rx) = channel();
        let second = engine
            .send_message(&run, None, "b", tx)
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
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

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
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(&run, None, "hi", tx)
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
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(&run, None, "hi", tx)
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
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect_err("must surface");

        assert!(matches!(err, Error::EmptyReply), "got: {err:?}");
        assert_eq!(replay_log(dir.path()).len(), 2);
    }

    #[tokio::test]
    async fn a_dropped_receiver_does_not_lose_the_append() {
        let provider = ScriptedProvider::scripted(vec![done_reply("nobody watched")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, rx) = channel();
        drop(rx);

        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        assert!(!reply.partial);
        let events = replay_log(dir.path());
        assert_eq!(appended(&events[2]).content, "nobody watched");
    }

    #[tokio::test]
    async fn an_empty_message_is_refused_before_anything_is_appended() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();

        let err = engine
            .send_message(&run, None, "  \n\t ", tx)
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
        let (engine, run) = engine(&provider, &dir);

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
                    grants: Vec::new(),
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
            .send_message(&run, Some("s-old"), "hi", tx)
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
        let (engine, run) =
            engine_with_tools(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, mut rx) = channel();

        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

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
    async fn provider_round_trip_data_survives_the_log_into_the_next_request() {
        let signature = b"opaque-thought-signature".to_vec();
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call_carrying(
                    "srv1",
                    0,
                    "lookup",
                    r#"{"q":1}"#,
                    signature.clone(),
                )),
                Ok(tool_stop()),
            ],
            done_reply("answer"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) =
            engine_with_tools_at(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, _rx) = channel();

        let session_id = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send")
            .session_id;

        let logged = replay_log(dir.path())
            .into_iter()
            .find_map(|event| match event {
                session_event::Event::ToolCallIssued(call) => Some(call),
                _ => None,
            })
            .expect("a tool call was issued");
        assert_eq!(
            logged.provider_roundtrip, signature,
            "the log kept the bytes"
        );

        // a resumed session rebuilds its transcript from the log, so the bytes
        // have to come back out of the projection, not out of memory
        let resumed = ScriptedProvider::scripted(vec![done_reply("still here")]);
        let (engine, run) = reopened_engine(&resumed, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&session_id), "again", tx)
            .await
            .expect("resume");

        let calls = resumed.requests()[0]
            .messages
            .iter()
            .find_map(|message| match message {
                Message::ToolCalls(calls) => Some(calls.clone()),
                _ => None,
            })
            .expect("the rebuilt transcript replays the call");
        assert_eq!(
            calls[0].provider_roundtrip, signature,
            "the rebuilt transcript handed the bytes back to the provider"
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
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

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
        let (engine, run) = engine_with_tools(&provider, &dir, tools(&[("alpha", "A", true)]));
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

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
        let (engine, run) = engine_with_tools(&provider, &dir, tools(&[("alpha", "A", true)]));
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");
        drop(engine);

        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("dup", 0, "alpha", "{}")), Ok(tool_stop())],
            done_reply("second"),
        ]);
        let (engine, run) = reopened_engine(&provider, &dir, tools(&[("alpha", "A", true)]));
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "again", tx)
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
        let (engine, run) = reopened_engine(&provider, &dir, Registry::new(512));
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some("s-01"), "again", tx)
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
        let (engine, run) = reopened_engine(&provider, &dir, Registry::new(512));
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some("s-01"), "again", tx)
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
        let (engine, _run) = reopened_engine(&provider, &dir, Registry::new(512));

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
        let (engine, run) =
            engine_with_tools(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "question", tx)
            .await
            .expect("send");
        drop(engine);

        let provider = ScriptedProvider::scripted(vec![done_reply("hello again")]);
        let (engine, run) =
            reopened_engine(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "again", tx)
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

    #[tokio::test]
    async fn a_tool_error_is_a_result_and_the_turn_continues() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("x", 0, "fails", "{}")), Ok(tool_stop())],
            done_reply("recovered"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let registry = tools(&[("fails", "ERROR: nope", false)]);
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let (tx, mut rx) = channel();

        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");
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
        let (engine, run) = engine_with_tools(&provider, &dir, tools(&[("alpha", "A", true)]));
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

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
        let (engine, run) = engine_with_tools(&provider, &dir, tools(&[("alpha", "A", true)]));
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

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
        let (engine, run) = engine(&provider, &dir);
        let (tx, mut rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

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
            source: ToolSource::Builtin,
        }));
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let events = replay_log(dir.path());
        let result = resulted(&events[3]);
        assert!(result.truncated);
        assert_eq!(result.content, "01234567 [truncated]");
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
    }

    #[tokio::test]
    async fn the_role_thinking_level_rides_the_request_not_the_prompt() {
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Engine::new(Store::new(log, projection), Registry::new(512));
        let run = Runner {
            role: SessionRole::Concierge,
            provider: Arc::clone(&provider) as Arc<dyn Provider>,
            model: "test-model".to_owned(),
            thinking: Thinking::Minimal,
            system: Some("be terse".to_owned()),
        };
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        assert_eq!(
            provider.requests()[0].system.as_deref(),
            Some("be terse"),
            "the marker is the sidecar's dialect and belongs to its provider"
        );
        assert_eq!(provider.requests()[0].thinking, Thinking::Minimal);
    }

    #[tokio::test]
    async fn the_role_lands_on_the_session_and_on_every_request() {
        let provider = ScriptedProvider::scripted(vec![done_reply("one"), done_reply("two")]);
        let dir = TempDir::new().expect("temp dir");
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Engine::new(Store::new(log, projection), Registry::new(512));
        let run = Runner {
            role: SessionRole::Executor,
            provider: Arc::clone(&provider) as Arc<dyn Provider>,
            model: "test-model".to_owned(),
            thinking: Thinking::Default,
            system: None,
        };

        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "again", tx)
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
    async fn two_turns_send_a_byte_identical_prefix() {
        let provider = ScriptedProvider::scripted(vec![done_reply("first"), done_reply("second")]);
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, seeded_records());
        let (engine, run) = reopened_engine(&provider, &dir, tools(&[("lookup", "found", true)]));

        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "first", tx)
            .await
            .expect("send");
        let (tx2, _rx2) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "second", tx2)
            .await
            .expect("send");

        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].system, requests[1].system,
            "the identity and record index are the cached prefix; they must not move"
        );
        assert_eq!(
            requests[0].tools, requests[1].tools,
            "tool schemas sit in the prefix too, so their order is part of it"
        );
        assert!(
            requests[1].messages.len() > requests[0].messages.len(),
            "only the transcript grew"
        );

        let system = requests[0].system.as_deref().expect("a system prompt");
        assert!(
            system.starts_with("be terse"),
            "the identity file renders first: {system}"
        );
        assert!(
            system.ends_with("(id: mr-fact)"),
            "the record index renders last, so nothing volatile precedes it: {system}"
        );

        // the index is rebuilt by replay, so a restart is where an ordering bug shows
        let restarted = ScriptedProvider::scripted(vec![done_reply("third")]);
        let (engine, run) = reopened_engine(&restarted, &dir, tools(&[("lookup", "found", true)]));
        let (tx3, _rx3) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "third", tx3)
            .await
            .expect("send");

        assert_eq!(
            restarted.requests()[0].system,
            requests[0].system,
            "a restart replays the log and must land on the same prefix"
        );
    }

    #[tokio::test]
    async fn seeded_memory_records_ride_the_next_turns_system_prompt() {
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, seeded_records());
        let (engine, run) = reopened_engine(&provider, &dir, Registry::new(512));
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

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
        let (engine, run) =
            reopened_engine(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "question", tx)
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
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

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
        let engine = Engine::new(Store::new(log, projection), Registry::new(512));
        let run = Runner {
            role: SessionRole::Concierge,
            provider: Arc::clone(&provider) as Arc<dyn Provider>,
            model: "test-model".to_owned(),
            thinking: Thinking::Default,
            system: None,
        };
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

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
        let (engine, run) = engine_with_tools(&provider, &dir, registry);

        let capture = TraceCapture::start();
        let (tx, _rx) = channel();
        engine
            .send_message(&run, None, "what palette?", tx)
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
        let (engine, run) = engine_with_tools(
            &provider,
            &dir,
            tools(&[("memory_search", SEARCH_HIT, true)]),
        );

        let capture = TraceCapture::start();
        let (tx, _rx) = channel();
        engine
            .send_message(&run, None, "hm", tx)
            .await
            .expect("send");
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
        registry.register(Box::new(crate::tool::builtin::memory::MemoryWrite));
        let (engine, run) = engine_with_tools(&provider, &dir, registry);

        let capture = TraceCapture::start();
        let (tx, _rx) = channel();
        engine
            .send_message(&run, None, "remember this", tx)
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

            fn source(&self) -> crate::tool::ToolSource {
                crate::tool::ToolSource::Builtin
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
                        job_request: None,
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
        let (engine, run) = reopened_engine(&provider, &dir, registry);

        let capture = TraceCapture::start();
        let (tx, _rx) = channel();
        engine
            .send_message(&run, None, "that changed", tx)
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
        let (engine, run) = engine_with_tools(&provider, &dir, tools(&[("lookup", "found", true)]));

        let capture = TraceCapture::start();
        let (tx, _rx) = channel();
        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");
        let trace = capture.finish();

        for name in TURN_COUNTERS {
            assert!(
                counter_samples(&trace, name).is_empty(),
                "{name} must be absent on a memory-free turn"
            );
        }
    }

    #[tokio::test]
    async fn the_memory_block_is_the_tail_of_the_system_prompt() {
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log(&dir, seeded_records());
        let log = Log::open(dir.path()).expect("open log");
        let mut projection = Projection::in_memory().expect("open projection");
        crate::projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
        let engine = Engine::new(Store::new(log, projection), Registry::new(512));
        let run = Runner {
            role: SessionRole::Concierge,
            provider: Arc::clone(&provider) as Arc<dyn Provider>,
            model: "test-model".to_owned(),
            thinking: Thinking::Minimal,
            system: Some("be terse".to_owned()),
        };
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        assert_eq!(
            provider.requests()[0].system,
            Some(format!("be terse\n\n{}", seeded_block()))
        );
    }

    fn review_engine(provider: &Arc<ScriptedProvider>, dir: &TempDir) -> (Engine, Runner) {
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
        let (engine, _run) = review_engine(&provider, &dir);

        let queued: Vec<String> = engine
            .with_store(|store| store.projection().review_items(0))
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
            .with_store(|store| store.projection().review_items(0))
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
        let (engine, _run) = review_engine(&provider, &dir);

        engine.review_delete("mr-pref").expect("delete");

        let events = replay_events(dir.path());
        let verdict = events.last().expect("the verdict");
        assert_eq!(verdict.source, Source::User as i32);
        match memory_payload(verdict) {
            memory_event::Event::RecordDeleted(deleted) => assert_eq!(deleted.id, "mr-pref"),
            other => panic!("expected RecordDeleted, got {other:?}"),
        }

        let queued: Vec<String> = engine
            .with_store(|store| store.projection().review_items(0))
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
        let (engine, _run) = review_engine(&provider, &dir);
        let before = replay_events(dir.path()).len();

        let accept = engine.review_accept("mr-ghost");
        assert!(
            matches!(accept, Err(Error::Store(store::Error::UnknownRecord { ref id })) if id == "mr-ghost"),
            "got: {accept:?}"
        );
        let delete = engine.review_delete("mr-ghost");
        assert!(
            matches!(delete, Err(Error::Store(store::Error::UnknownRecord { ref id })) if id == "mr-ghost"),
            "got: {delete:?}"
        );

        assert_eq!(
            replay_events(dir.path()).len(),
            before,
            "nothing was appended"
        );
    }

    #[tokio::test]
    async fn a_project_bound_session_offers_and_can_call_its_workspace_tool() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(&dir, vec![seeded_session_with_project("arc")]);
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "ws_read", "{}")), Ok(tool_stop())],
            done_reply("done"),
        ]);
        let mut registry = Registry::new(512);
        registry.register(workspace_tool("ws_read", "workspace file"));
        let mut projects = BTreeMap::new();
        projects.insert(
            "arc".to_owned(),
            ProjectSpec {
                sources: vec![ToolSource::Builtin, ToolSource::Workspace],
                grants: Vec::new(),
            },
        );
        let (engine, run) = reopened_engine_with_projects(&provider, &dir, registry, projects);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, Some("s-01"), "read it", tx)
            .await
            .expect("send");

        let requests = provider.requests();
        assert!(
            requests[0].tools.iter().any(|def| def.name == "ws_read"),
            "the project's workspace tool was offered: {:?}",
            requests[0].tools
        );
        let events = replay_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert_eq!(result.content, "workspace file");
    }

    #[tokio::test]
    async fn an_unbound_session_denies_a_workspace_tool_and_the_turn_still_completes() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "ws_read", "{}")), Ok(tool_stop())],
            done_reply("no access, sorry"),
        ]);
        let mut registry = Registry::new(512);
        registry.register(workspace_tool("ws_read", "workspace file"));
        let mut projects = BTreeMap::new();
        projects.insert(
            "arc".to_owned(),
            ProjectSpec {
                sources: vec![ToolSource::Builtin, ToolSource::Workspace],
                grants: Vec::new(),
            },
        );
        let (engine, run) = reopened_engine_with_projects(&provider, &dir, registry, projects);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "read it", tx)
            .await
            .expect("the turn still completes after a denied tool call");

        assert!(!reply.partial);
        let requests = provider.requests();
        assert!(
            requests[0].tools.iter().all(|def| def.name != "ws_read"),
            "an unbound session must not be offered a workspace tool: {:?}",
            requests[0].tools
        );
        let events = replay_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Error as i32);
        assert!(
            result.content.contains("not available in this session"),
            "{}",
            result.content
        );
    }

    #[tokio::test]
    async fn a_session_naming_an_unconfigured_project_fails_closed_to_builtin() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(&dir, vec![seeded_session_with_project("vanished")]);
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "ws_read", "{}")), Ok(tool_stop())],
            done_reply("no access, sorry"),
        ]);
        let mut registry = Registry::new(512);
        registry.register(workspace_tool("ws_read", "workspace file"));
        let mut projects = BTreeMap::new();
        projects.insert(
            "arc".to_owned(),
            ProjectSpec {
                sources: vec![ToolSource::Builtin, ToolSource::Workspace],
                grants: Vec::new(),
            },
        );
        let (engine, run) = reopened_engine_with_projects(&provider, &dir, registry, projects);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, Some("s-01"), "read it", tx)
            .await
            .expect("a project missing from config still gets builtin only");

        assert!(!reply.partial);
        let events = replay_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Error as i32);
        assert!(
            result.content.contains("not available in this session"),
            "{}",
            result.content
        );
    }

    #[tokio::test]
    async fn a_new_session_records_an_empty_project() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hi")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let events = replay_log(dir.path());
        let session_event::Event::SessionCreated(created) = &events[0] else {
            panic!("expected SessionCreated first, got {:?}", events[0]);
        };
        assert_eq!(created.project, "");
    }

    fn projects_with(
        name: &str,
        sources: Vec<ToolSource>,
        grants: Vec<Grant>,
    ) -> BTreeMap<String, ProjectSpec> {
        let mut projects = BTreeMap::new();
        projects.insert(name.to_owned(), ProjectSpec { sources, grants });
        projects
    }

    #[tokio::test]
    async fn create_bound_session_records_the_project_and_canonical_grants() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        let notes = dir.path().join("notes");
        std::fs::create_dir_all(&notes).expect("mkdir notes");

        let provider = ScriptedProvider::scripted(vec![]);
        let (mut engine, run) = engine_with_tools(&provider, &dir, Registry::new(512));
        engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![
                Grant::new(&root, Mode::ReadWrite),
                Grant::new(&notes, Mode::ReadOnly),
            ],
        ));

        let session_id = engine
            .create_bound_session(&run, "arc", SessionRole::Concierge, None)
            .expect("create a bound session");

        let events = replay_log(dir.path());
        let session_event::Event::SessionCreated(created) = &events[0] else {
            panic!("expected SessionCreated first, got {:?}", events[0]);
        };
        assert_eq!(created.session_id, session_id);
        assert_eq!(created.project, "arc");
        assert_eq!(created.role, SessionRole::Concierge as i32);
        assert_eq!(created.provider, "scripted");
        assert_eq!(created.model, "test-model");
        assert_eq!(
            created.grants,
            [
                arc_proto::v1::WorkspaceGrant {
                    root: root
                        .canonicalize()
                        .expect("canon")
                        .to_string_lossy()
                        .into_owned(),
                    read_write: true,
                },
                arc_proto::v1::WorkspaceGrant {
                    root: notes
                        .canonicalize()
                        .expect("canon")
                        .to_string_lossy()
                        .into_owned(),
                    read_write: false,
                },
            ]
        );
    }

    #[tokio::test]
    async fn create_bound_session_names_an_unknown_project() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);

        let err = engine
            .create_bound_session(&run, "ghost", SessionRole::Concierge, None)
            .expect_err("an unconfigured project must be refused");

        assert!(matches!(err, Error::UnknownProject { ref project } if project == "ghost"));
        assert!(err.to_string().contains("ghost"));
        assert_eq!(replay_log(dir.path()).len(), 0, "nothing was appended");
    }

    #[tokio::test]
    async fn create_bound_session_fails_when_the_root_does_not_exist() {
        let dir = TempDir::new().expect("temp dir");
        let missing = dir.path().join("nope");
        let provider = ScriptedProvider::scripted(vec![]);
        let (engine, run) = engine_with_tools(&provider, &dir, Registry::new(512));
        let engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin],
            vec![Grant::new(&missing, Mode::ReadWrite)],
        ));

        let err = engine
            .create_bound_session(&run, "arc", SessionRole::Concierge, None)
            .expect_err("a missing root must fail at creation");

        assert!(matches!(err, Error::Grants { ref project, .. } if project == "arc"));
        assert_eq!(replay_log(dir.path()).len(), 0, "nothing was appended");
    }

    #[tokio::test]
    async fn a_bound_sessions_grants_flow_from_the_log_through_the_gate() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        std::fs::write(root.join("inside.txt"), "hi").expect("write");
        let elsewhere = TempDir::new().expect("temp dir 2");
        std::fs::write(elsewhere.path().join("outside.txt"), "nope").expect("write");

        let mut registry = Registry::new(512);
        for tool in workspace::tools(Arc::new(Workspace::new())) {
            registry.register(tool);
        }
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "read",
                    &serde_json::json!({"path": root.join("inside.txt")}).to_string(),
                )),
                Ok(call(
                    "c2",
                    1,
                    "read",
                    &serde_json::json!({"path": elsewhere.path().join("outside.txt")}).to_string(),
                )),
                Ok(tool_stop()),
            ],
            done_reply("done"),
        ]);
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));

        let session_id = engine
            .create_bound_session(&run, "arc", SessionRole::Concierge, None)
            .expect("create a bound session");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&session_id), "read both", tx)
            .await
            .expect("send");

        let events = replay_log(dir.path());
        let inside = resulted(&events[4]);
        let outside = resulted(&events[5]);
        assert_eq!(inside.outcome, ToolOutcome::Ok as i32);
        assert_eq!(inside.content, "hi");
        assert_eq!(outside.outcome, ToolOutcome::Error as i32);
        assert!(outside.content.contains("outside"), "{}", outside.content);
    }

    #[tokio::test]
    async fn recorded_grants_win_over_a_changed_config() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        std::fs::write(root.join("keep.txt"), "true colors").expect("write");
        let changed_root = TempDir::new().expect("temp dir 2");

        let mut registry = Registry::new(512);
        for tool in workspace::tools(Arc::new(Workspace::new())) {
            registry.register(tool);
        }
        let creating_provider = ScriptedProvider::scripted(vec![]);
        let (creating_engine, run) = engine_with_tools(&creating_provider, &dir, registry);
        let creating_engine = creating_engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));
        let session_id = creating_engine
            .create_bound_session(&run, "arc", SessionRole::Concierge, None)
            .expect("create a bound session");
        drop(creating_engine);

        let mut later_registry = Registry::new(512);
        for tool in workspace::tools(Arc::new(Workspace::new())) {
            later_registry.register(tool);
        }
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "read",
                    &serde_json::json!({"path": root.join("keep.txt")}).to_string(),
                )),
                Ok(tool_stop()),
            ],
            done_reply("done"),
        ]);
        let projects = projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![Grant::new(changed_root.path(), Mode::ReadWrite)],
        );
        let (engine, run) =
            reopened_engine_with_projects(&provider, &dir, later_registry, projects);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, Some(&session_id), "read it", tx)
            .await
            .expect("send");

        let events = replay_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert_eq!(
            result.content, "true colors",
            "the session kept the grants recorded at creation, not the reconfigured ones"
        );
    }

    #[tokio::test]
    async fn an_unbound_sessions_workspace_call_gets_the_no_workspace_error() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(&dir, vec![seeded_session_with_project("arc")]);
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "read",
                    &serde_json::json!({"path": "/tmp/x"}).to_string(),
                )),
                Ok(tool_stop()),
            ],
            done_reply("no access"),
        ]);
        let mut registry = Registry::new(512);
        for tool in workspace::tools(Arc::new(Workspace::new())) {
            registry.register(tool);
        }
        let projects = projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            Vec::new(),
        );
        let (engine, run) = reopened_engine_with_projects(&provider, &dir, registry, projects);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, Some("s-01"), "read it", tx)
            .await
            .expect("send");

        let events = replay_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Error as i32);
        assert!(result.content.contains("granted"), "{}", result.content);
    }

    fn dispatch_args(
        role: &str,
        project: &str,
        brief: &str,
        budget_tokens: u64,
        budget_minutes: u32,
    ) -> String {
        serde_json::json!({
            "role": role,
            "project": project,
            "brief": brief,
            "budget_tokens": budget_tokens,
            "budget_minutes": budget_minutes,
        })
        .to_string()
    }

    #[tokio::test]
    async fn a_dispatched_call_creates_the_child_durably_and_the_parent_result_names_it() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "dispatch",
                    &dispatch_args("executor", "arc", "fix the bug", 500, 10),
                )),
                Ok(tool_stop()),
            ],
            done_reply("dispatched"),
        ]);
        let mut registry = Registry::new(512);
        registry.register(Box::new(Dispatch::new(vec!["arc".to_owned()], None)));
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "start a job", tx)
            .await
            .expect("send");

        let events = replay_log(dir.path());
        assert_eq!(events.len(), 6);
        let session_event::Event::SessionCreated(child) = &events[3] else {
            panic!("expected the child SessionCreated, got {:?}", events[3]);
        };
        assert_eq!(child.project, "arc");
        assert_eq!(child.role, SessionRole::Executor as i32);
        assert_eq!(
            child.budget,
            Some(Budget {
                total_tokens: 500,
                wall_clock_seconds: 600,
            })
        );
        assert_eq!(
            child.grants,
            [arc_proto::v1::WorkspaceGrant {
                root: root
                    .canonicalize()
                    .expect("canon")
                    .to_string_lossy()
                    .into_owned(),
                read_write: true,
            }]
        );
        let child_id = child.session_id.clone();

        let result = resulted(&events[4]);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert!(result.content.contains(&child_id), "{}", result.content);
        assert!(result.content.contains("executor"), "{}", result.content);
        assert!(result.content.contains("arc"), "{}", result.content);

        // the role-mismatch pin keys on the recorded role, not the runner
        // that created the session, so an executor runner can continue it
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("on it")]);
        let executor_run = Runner {
            role: SessionRole::Executor,
            provider: Arc::clone(&executor_provider) as Arc<dyn Provider>,
            model: "exec-model".to_owned(),
            thinking: Thinking::Default,
            system: None,
        };
        let (child_engine, _) = reopened_engine(&executor_provider, &dir, Registry::new(512));
        let child_engine = child_engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));
        let (tx, _rx) = channel();
        child_engine
            .send_message(&executor_run, Some(&child_id), "go", tx)
            .await
            .expect("an executor runner continues the dispatched child");
    }

    #[tokio::test]
    async fn a_dispatch_to_an_unknown_project_forged_past_the_enum_is_an_actionable_error_and_the_turn_completes()
     {
        struct Forged;

        impl Tool for Forged {
            fn definition(&self) -> crate::provider::ToolDefinition {
                crate::provider::ToolDefinition {
                    name: "forged_dispatch".to_owned(),
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
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ToolReply> + Send + '_>>
            {
                Box::pin(async move {
                    ToolReply {
                        content: "dispatching".to_owned(),
                        ok: true,
                        memory_events: Vec::new(),
                        job_request: Some(JobRequest {
                            role: SessionRole::Executor,
                            project: "ghost".to_owned(),
                            brief: "do it".to_owned(),
                            budget: None,
                        }),
                    }
                })
            }
        }

        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "forged_dispatch", "{}")), Ok(tool_stop())],
            done_reply("done"),
        ]);
        let mut registry = Registry::new(512);
        registry.register(Box::new(Forged));
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "start a job", tx)
            .await
            .expect("a bad dispatch fails the call, not the turn");

        let events = replay_log(dir.path());
        assert_eq!(events.len(), 5, "no child session was created");
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Error as i32);
        assert!(result.content.contains("ghost"), "{}", result.content);
        let assistant = appended(&events[4]);
        assert_eq!(assistant.content, "done");
    }

    #[tokio::test]
    async fn a_slow_turn_does_not_block_a_turn_in_a_different_session() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let slow_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("slow start".to_owned()))],
            notify: Arc::clone(&notify),
            after: done_reply("slow end"),
        }]);
        let fast_provider = ScriptedProvider::scripted(vec![done_reply("fast done")]);
        let dir = TempDir::new().expect("temp dir");
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(Engine::new(Store::new(log, projection), Registry::new(512)));
        let slow_run = runner(&slow_provider);
        let fast_run = runner(&fast_provider);

        let (slow_tx, mut slow_rx) = channel();
        let slow_engine = Arc::clone(&engine);
        let slow_handle = tokio::spawn(async move {
            slow_engine
                .send_message(&slow_run, None, "slow please", slow_tx)
                .await
        });

        assert!(
            matches!(
                slow_rx.recv().await.expect("accepted"),
                EngineEvent::Accepted { .. }
            ),
            "the slow turn opens its session before it stalls"
        );
        assert_eq!(
            slow_rx.recv().await.expect("first delta"),
            EngineEvent::Delta("slow start".to_owned()),
            "the slow turn streams some text before it stalls"
        );

        let (fast_tx, _fast_rx) = channel();
        let fast_reply = engine
            .send_message(&fast_run, None, "fast please", fast_tx)
            .await
            .expect("a different session's turn is not blocked by the stall");

        assert_eq!(fast_reply.usage, Some(usage()));
        assert!(
            !slow_handle.is_finished(),
            "the slow turn is still stalled on the gate"
        );

        notify.notify_one();
        let slow_reply = slow_handle
            .await
            .expect("the slow turn's task did not panic")
            .expect("the slow turn completes once released");

        assert_eq!(
            engine
                .transcript(&fast_reply.session_id)
                .expect("transcript"),
            [
                prose_entry(Role::User as i32, "fast please", false),
                prose_entry(Role::Assistant as i32, "fast done", false),
            ]
        );
        assert_eq!(
            engine
                .transcript(&slow_reply.session_id)
                .expect("transcript"),
            [
                prose_entry(Role::User as i32, "slow please", false),
                prose_entry(Role::Assistant as i32, "slow startslow end", false),
            ],
            "the gate held the reply together; nothing from the fast session leaked in"
        );
    }

    #[tokio::test]
    async fn two_sends_to_the_same_session_serialize_into_two_ordered_turns() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("first start".to_owned()))],
                notify: Arc::clone(&notify),
                after: done_reply("first end"),
            },
            Step::Immediate(done_reply("second reply")),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(Engine::new(Store::new(log, projection), Registry::new(512)));
        let run = runner(&provider);

        let (tx1, mut rx1) = channel();
        let engine_a = Arc::clone(&engine);
        let run1 = run.clone();
        let first_handle =
            tokio::spawn(async move { engine_a.send_message(&run1, None, "one", tx1).await });

        let session_id = match rx1.recv().await.expect("accepted") {
            EngineEvent::Accepted { session_id } => session_id,
            other => panic!("expected Accepted first, got {other:?}"),
        };
        assert_eq!(
            rx1.recv().await.expect("first delta"),
            EngineEvent::Delta("first start".to_owned()),
            "the first turn streams some text before it stalls"
        );

        let (tx2, _rx2) = channel();
        let engine_b = Arc::clone(&engine);
        let run2 = run.clone();
        let second_id = session_id.clone();
        let second_handle = tokio::spawn(async move {
            engine_b
                .send_message(&run2, Some(&second_id), "two", tx2)
                .await
        });

        tokio::task::yield_now().await;
        assert!(
            !second_handle.is_finished(),
            "the second turn waits for the first turn's session guard"
        );

        notify.notify_one();
        let first_reply = first_handle
            .await
            .expect("the first turn's task did not panic")
            .expect("the first turn completes once released");
        let second_reply = second_handle
            .await
            .expect("the second turn's task did not panic")
            .expect("the second turn completes after the first");

        assert_eq!(first_reply.session_id, session_id);
        assert_eq!(second_reply.session_id, session_id);
        assert_eq!(
            engine.transcript(&session_id).expect("transcript"),
            [
                prose_entry(Role::User as i32, "one", false),
                prose_entry(Role::Assistant as i32, "first startfirst end", false),
                prose_entry(Role::User as i32, "two", false),
                prose_entry(Role::Assistant as i32, "second reply", false),
            ],
            "the two turns land whole, in order, with nothing interleaved"
        );
    }
}
