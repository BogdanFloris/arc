use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

use arc_proto::v1::{
    Budget, MemoryEvent, MessageAppended, Notification, ReviewChanged, Role, ServerCallRecorded,
    SessionAppended, SessionCreated, SessionEvent, SessionRole, Source, ToolCallIssued,
    ToolOutcome, ToolResultRecorded, WorkspaceGrant,
};
use arc_proto::v1::{event, memory_event, notification, session_event};
use futures::StreamExt as _;

use tokio::sync::broadcast;
use tokio::sync::mpsc;

use crate::memory::render_memory_index;
use crate::projection::{self, MessageRow, SessionSummary};
use crate::provider::{
    self, CompletionDelta, CompletionRequest, Message, Provider, Stop, Thinking, ToolCall, Usage,
};
use crate::store::{self, Store, now_ts};
use crate::tool::workspace::{Grant, Grants, Mode};
use crate::tool::{ContinueRequest, DispatchOutcome, Intent, Registry, ToolSource, TurnContext};

const MAX_TOOL_STEPS: usize = 8;
// a coding turn explores and edits; 8 forced a nudge ratchet
const MAX_EXECUTOR_TOOL_STEPS: usize = 256;

fn max_tool_steps(role: SessionRole) -> usize {
    match role {
        SessionRole::Executor => MAX_EXECUTOR_TOOL_STEPS,
        _ => MAX_TOOL_STEPS,
    }
}

fn elapsed_ms_since(start: std::time::Instant) -> u32 {
    u32::try_from(start.elapsed().as_millis()).unwrap_or(u32::MAX)
}

/// What a project offers a session bound to it: the tools and the roots.
#[derive(Debug, Clone, Default)]
pub struct ProjectSpec {
    pub sources: Vec<ToolSource>,
    pub grants: Vec<Grant>,
    pub command_prefix: Vec<String>,
}

/// Who is running a turn. Resolved once per role and handed to the engine
/// with each turn, so one log can serve a conversation and a job at once.
#[derive(Clone, Debug)]
pub struct Runner {
    pub role: SessionRole,
    pub provider: Arc<dyn Provider>,
    pub model: String,
    pub thinking: Thinking,
    /// The concierge's identity file, or a job's spawn-built preamble.
    pub system: Option<String>,
}

// never held across an .await: it fences a single append batch or
// projection read, so concurrent turns interleave everywhere else
pub struct Engine {
    store: StdMutex<Store>,
    registry: Registry,
    projects: BTreeMap<String, ProjectSpec>,
    role_identities: BTreeMap<SessionRole, (String, String)>,
    // `[roles.counsel]` presence, not per-project config (§6.2): concierge and
    // executor hold `consult_expert` when true; archivist never does
    expert_enabled: bool,
    // one guard per session, held for a whole turn: turns in the same
    // session serialize, turns in different sessions run concurrently
    turns: StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    notifier: Option<broadcast::Sender<Notification>>,
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
        arguments_json: String,
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
    pub step_capped: bool,
    pub grounding_json: String,
    pub jobs: Vec<DispatchedJob>,
    pub continues: Vec<ContinuedJob>,
}

/// A child session `dispatch` created durably during this turn. The engine
/// does not start it; the caller hands it to a supervisor that does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedJob {
    pub session_id: String,
    /// The session that dispatched it, and where its handback lands.
    pub parent_session: String,
    pub role: SessionRole,
    pub project: String,
    pub brief: String,
    pub budget: Option<Budget>,
}

/// A validated `continue_job` request the engine handed off. The engine only
/// confirms the target is a job; whether it's still live is the
/// supervisor's to know, so this carries what a resume would need too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuedJob {
    pub session_id: String,
    /// The session whose turn called `continue_job` — where a resumed job's
    /// handback lands, same as a fresh dispatch's parent.
    pub parent_session: String,
    pub message: String,
    pub role: SessionRole,
    pub project: String,
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

    #[error(
        "session {session_id} was recorded on {pinned}; the {role} now runs {serving}. \
         Dispatch a fresh job instead."
    )]
    ModelMismatch {
        session_id: String,
        pinned: String,
        role: String,
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
            role_identities: BTreeMap::new(),
            expert_enabled: false,
            turns: StdMutex::new(HashMap::new()),
            notifier: None,
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
        self.with_store_mut(|store| store.review_accept(record_id))?;
        self.notify_review_changed()
    }

    pub fn review_delete(&self, record_id: &str) -> Result<(), Error> {
        self.with_store_mut(|store| store.review_delete(record_id))?;
        self.notify_review_changed()
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

    /// The provider and model each role records for sessions it creates.
    /// Written once at startup from the resolved roles; a role absent from
    /// this map falls back to the creating runner's own identity.
    #[must_use]
    pub fn with_role_identities(
        mut self,
        role_identities: BTreeMap<SessionRole, (String, String)>,
    ) -> Self {
        self.role_identities = role_identities;
        self
    }

    /// Wires the daemon's broadcast spine: every durable session append then
    /// also fans out as a `SessionAppended` push. Absent, `record` is silent.
    #[must_use]
    pub fn with_notifier(mut self, notifier: broadcast::Sender<Notification>) -> Self {
        self.notifier = Some(notifier);
        self
    }

    /// Whether `[roles.counsel]` is configured: true offers `consult_expert`
    /// to concierge and executor sessions; false offers it to no one.
    #[must_use]
    pub fn with_expert_enabled(mut self, enabled: bool) -> Self {
        self.expert_enabled = enabled;
        self
    }

    /// A new session bound to a project: grants are canonicalized now and
    /// recorded in the log, so the session keeps them even if config changes.
    /// `runner` supplies the recorded provider and model; `role` and `budget`
    /// are what the new session is pinned to, which is `runner.role` for a
    /// session the runner starts for itself but differs for a dispatched
    /// job, where the runner is the dispatching parent, not the child.
    pub fn create_bound_session(
        &self,
        runner: &Runner,
        project: &str,
        role: SessionRole,
        budget: Option<Budget>,
    ) -> Result<String, Error> {
        self.create_bound_session_with_intent(
            runner,
            project,
            role,
            budget,
            Intent::Implement,
            None,
            Source::Model,
        )
    }

    /// The `:code` door (row 9.1): a bound session the user opened directly,
    /// not a model's `dispatch`. Recorded `Source::User`, read-write grants
    /// (implement-style, no downgrade), no budget, no `dispatched_by` — it
    /// is a conversation, not a job.
    pub fn create_direct_session(
        &self,
        runner: &Runner,
        project: &str,
        role: SessionRole,
    ) -> Result<String, Error> {
        self.create_bound_session_with_intent(
            runner,
            project,
            role,
            None,
            Intent::Implement,
            None,
            Source::User,
        )
    }

    /// Same as `create_bound_session`, but `Intent::Analyze` records the
    /// project root grant read-only instead of read-write. `dispatched_by`
    /// is the parent recorded on a job's own creation (DESIGN.md/6.34); a
    /// session the runner starts for itself passes `None`. `source` is
    /// `Model` for a dispatch and `User` for the `:code` door — who asked
    /// for the session, not who runs it.
    #[tracing::instrument(
        level = "info",
        name = "session.create_bound_session",
        skip_all,
        fields(project, session_id = tracing::field::Empty)
    )]
    #[allow(clippy::too_many_arguments)]
    fn create_bound_session_with_intent(
        &self,
        runner: &Runner,
        project: &str,
        role: SessionRole,
        budget: Option<Budget>,
        intent: Intent,
        dispatched_by: Option<&str>,
        source: Source,
    ) -> Result<String, Error> {
        let spec = self
            .projects
            .get(project)
            .cloned()
            .ok_or_else(|| Error::UnknownProject {
                project: project.to_owned(),
            })?;
        let mut grant_specs = spec.grants;
        if intent == Intent::Analyze {
            // all of them, not grants[0]: ordering is convention, not contract
            for grant in &mut grant_specs {
                grant.mode = Mode::ReadOnly;
            }
        }
        let grants = Grants::new(grant_specs).map_err(|source| Error::Grants {
            project: project.to_owned(),
            source,
        })?;

        let session_id = uuid::Uuid::new_v4().to_string();
        tracing::Span::current().record("session_id", session_id.as_str());
        let (provider, model) = self
            .role_identities
            .get(&role)
            .cloned()
            .unwrap_or_else(|| (runner.provider.name().to_owned(), runner.model.clone()));
        self.record(
            source,
            session_event::Event::SessionCreated(SessionCreated {
                session_id: session_id.clone(),
                title: String::new(),
                provider,
                model,
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
                dispatched_by: dispatched_by.unwrap_or_default().to_owned(),
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
        parent_session: &str,
        job_request: crate::tool::JobRequest,
    ) -> (ToolOutcome, String, Option<DispatchedJob>) {
        let role = job_request.role;
        let project = job_request.project;
        let brief = job_request.brief;
        let budget = job_request.budget;
        let intent = job_request.intent;
        match self.create_bound_session_with_intent(
            runner,
            &project,
            role,
            budget,
            intent,
            Some(parent_session),
            Source::Model,
        ) {
            Ok(child_id) => (
                ToolOutcome::Ok,
                format!(
                    "Dispatched {} into {project} as session {child_id} ({}). The job is \
                     running; its summary will arrive here as a handback when it \
                     finishes. Do not call continue_job to ask for status or results — \
                     each message costs the job a full turn. End this reply and wait.",
                    provider::role_label(role),
                    match intent {
                        Intent::Analyze => "analyze: read-only, it reports but cannot edit",
                        Intent::Implement => "implement: read-write",
                    }
                ),
                Some(DispatchedJob {
                    session_id: child_id,
                    parent_session: parent_session.to_owned(),
                    role,
                    project,
                    brief,
                    budget,
                }),
            ),
            Err(error) => (ToolOutcome::Error, format!("ERROR: {error}"), None),
        }
    }

    /// Acts on a `continue_job` tool call: confirms `session_id` names a
    /// recorded job (Executor or Archivist) and hands back what the
    /// caller's tool result should say. Whether that job is still live is
    /// the supervisor's knowledge, not the engine's — this only validates
    /// identity.
    fn continue_job(
        &self,
        runner: &Runner,
        parent_session: &str,
        request: ContinueRequest,
    ) -> (ToolOutcome, String, Option<ContinuedJob>) {
        let raw_role =
            match self.with_store(|store| store.projection().session_role(&request.session_id)) {
                Ok(role) => role,
                Err(error) => return (ToolOutcome::Error, format!("ERROR: {error}"), None),
            };
        let Some(raw_role) = raw_role else {
            return (
                ToolOutcome::Error,
                format!("ERROR: unknown session {}.", request.session_id),
                None,
            );
        };
        let Some(role @ (SessionRole::Executor | SessionRole::Archivist)) =
            SessionRole::try_from(raw_role).ok()
        else {
            return (
                ToolOutcome::Error,
                format!(
                    "ERROR: session {} is a {} session, not a job. continue_job only resumes \
                     a dispatched job.",
                    request.session_id,
                    role_name(raw_role)
                ),
                None,
            );
        };
        if let Some(error) = self.identity_mismatch(runner, role, &request.session_id) {
            return (ToolOutcome::Error, format!("ERROR: {error}"), None);
        }
        let project = self
            .with_store(|store| store.projection().session_project(&request.session_id))
            .ok()
            .flatten()
            .unwrap_or_default();
        (
            ToolOutcome::Ok,
            format!(
                "Continuing job {}. Its reply arrives later as a handback; do not call \
                 continue_job again to fetch it.",
                request.session_id
            ),
            Some(ContinuedJob {
                session_id: request.session_id,
                parent_session: parent_session.to_owned(),
                message: request.message,
                role,
                project,
            }),
        )
    }

    /// The job's summary and its session id, appended into the parent as
    /// ordinary conversation (§4.1). `Event.source = System`; `role = User`
    /// because `rebuild_transcript` drops a `System`-role row instead of
    /// sending it to the model. `reason` is `None` for a clean finish.
    /// Takes the parent's turn guard, so a handback never lands mid-turn.
    #[tracing::instrument(
        level = "info",
        name = "session.record_handback",
        skip_all,
        fields(parent_session, child_session)
    )]
    pub async fn record_handback(
        &self,
        parent_session: &str,
        child_session: &str,
        reason: Option<&str>,
        summary: &str,
    ) -> Result<(), Error> {
        let span = tracing::Span::current();
        span.record("parent_session", parent_session);
        span.record("child_session", child_session);

        let guard = self.turn_guard(parent_session);
        let _turn = guard.lock().await;

        let summary = truncate_summary(summary);
        let content = match reason {
            None => {
                let grants =
                    self.with_store(|store| store.projection().session_grants(child_session))?;
                let read_only = !grants.is_empty() && grants.iter().all(|(_, rw)| !*rw);
                let tail = if read_only {
                    format!(
                        "For follow-ups about anything this job read, continue_job \
                         {child_session} keeps its context but stays read-only — a change \
                         needs a fresh implement dispatch; a new dispatch starts from nothing."
                    )
                } else {
                    format!(
                        "For follow-ups about anything this job read or did, continue_job \
                         {child_session} keeps its context; a new dispatch starts from nothing."
                    )
                };
                format!("Job {child_session} finished.\n{summary}\n{tail}")
            }
            Some(reason) => format!("Job {child_session} stopped: {reason}.\n{summary}"),
        };
        self.record(
            Source::System,
            session_event::Event::MessageAppended(MessageAppended {
                session_id: parent_session.to_owned(),
                role: Role::User as i32,
                content,
                partial: false,
                turn_id: uuid::Uuid::new_v4().to_string(),
                ..Default::default()
            }),
        )?;
        Ok(())
    }

    /// The most recent assistant reply in a session, straight from the
    /// projection. `None` covers both "no assistant text yet" and "the last
    /// reply was empty" — a handback treats an empty reply the same as no
    /// reply at all.
    pub fn last_assistant_message(&self, session_id: &str) -> Result<Option<String>, Error> {
        let rows = self.with_store(|store| store.projection().messages(session_id))?;
        Ok(rows.into_iter().rev().find_map(|row| match row {
            MessageRow::Message { role, content, .. }
                if role == Role::Assistant as i32 && !content.is_empty() =>
            {
                Some(content)
            }
            _ => None,
        }))
    }

    /// The role a session is pinned to, straight from the log. `None`
    /// covers both an unknown session and one logged before roles existed;
    /// a caller that needs to tell those apart wants `enforce_pin` instead.
    pub fn session_role(&self, session_id: &str) -> Result<Option<SessionRole>, Error> {
        let raw = self.with_store(|store| store.projection().session_role(session_id))?;
        Ok(raw.and_then(|role| SessionRole::try_from(role).ok()))
    }

    /// The project a session was bound to at creation, straight from the
    /// log. `None` for an unbound session or one predating project stamping.
    pub fn session_project(&self, session_id: &str) -> Result<Option<String>, Error> {
        Ok(self.with_store(|store| store.projection().session_project(session_id))?)
    }

    /// A session's summed input+output tokens across every message it
    /// carries (row 6.37): what a resumed job's spent counter seeds from
    /// instead of restarting at zero.
    pub fn session_usage_tokens(&self, session_id: &str) -> Result<u64, Error> {
        Ok(self.with_store(|store| store.projection().session_token_total(session_id))?)
    }

    /// Job sessions left dispatched-but-unconcluded by the last restart
    /// (row 6.34): executor/archivist sessions with a recorded parent and no
    /// handback message in that parent yet. A job session created before
    /// 6.34 has no recorded parent and cannot be found here.
    pub fn unfinished_jobs(&self) -> Result<Vec<DispatchedJob>, Error> {
        let candidates = self.with_store(|store| store.projection().parented_job_sessions())?;
        let mut unfinished = Vec::new();
        for (session_id, parent_session, role) in candidates {
            let Ok(role) = SessionRole::try_from(role) else {
                continue;
            };
            let concluded = self.with_store(|store| {
                store
                    .projection()
                    .parent_has_handback_for(&parent_session, &session_id)
            })?;
            if !concluded {
                unfinished.push(DispatchedJob {
                    session_id,
                    parent_session,
                    role,
                    project: String::new(),
                    brief: String::new(),
                    budget: None,
                });
            }
        }
        Ok(unfinished)
    }

    fn sources(
        &self,
        session_id: &str,
        new_session: bool,
        role: SessionRole,
    ) -> Result<Vec<ToolSource>, Error> {
        let project = if new_session {
            None
        } else {
            self.with_store(|store| store.projection().session_project(session_id))?
        };
        let mut sources = match project {
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
        };
        // resolved by role, not project config (§6.2): archivist never holds it
        if self.expert_enabled && matches!(role, SessionRole::Concierge | SessionRole::Executor) {
            sources.push(ToolSource::Expert);
        }
        // web is a concierge capability, no config gate (2026-08-24)
        if role == SessionRole::Concierge {
            sources.push(ToolSource::Web);
        }
        Ok(sources)
    }

    /// The `bash` wrapper argv for a session's project, re-derived from
    /// config each turn like `sources`: never recorded in the log.
    fn command_prefix(&self, session_id: &str, new_session: bool) -> Result<Vec<String>, Error> {
        let project = if new_session {
            None
        } else {
            self.with_store(|store| store.projection().session_project(session_id))?
        };
        Ok(match project {
            None => Vec::new(),
            Some(name) => self
                .projects
                .get(&name)
                .map(|spec| spec.command_prefix.clone())
                .unwrap_or_default(),
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
            server_calls = tracing::field::Empty,
            grounded = tracing::field::Empty,
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
        let sources = self.sources(&session_id, new_session, runner.role)?;
        let grants = self.grants(&session_id, new_session)?;
        let command_prefix = self.command_prefix(&session_id, new_session)?;

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
                    dispatched_by: String::new(),
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
                ..Default::default()
            }),
        )?;

        self.drive_turn(
            runner,
            &session_id,
            &turn_id,
            &sources,
            grants.as_ref(),
            &command_prefix,
            &events,
        )
        .await
    }

    /// Runs one full model turn over `session_id`'s existing transcript,
    /// appending no user message: the handback turn (DESIGN.md §4.1). Takes
    /// the same turn guard as `send_message`, so a handback never lands
    /// mid-turn and never overlaps a user turn on the same session. A
    /// session with nothing new to react to is still just a turn — the
    /// model sees the transcript as-is.
    #[tracing::instrument(
        level = "info",
        name = "session.continue_session",
        skip_all,
        fields(
            model = %runner.model,
            role = provider::role_label(runner.role),
            thinking = runner.thinking.label(),
            session_id = %session_id,
            outcome = tracing::field::Empty,
            assistant_seq = tracing::field::Empty,
            tool_steps = tracing::field::Empty,
            counter.memory_searches = tracing::field::Empty,
            counter.memory_search_hits = tracing::field::Empty,
            counter.memory_reads_from_search = tracing::field::Empty,
            counter.records_created = tracing::field::Empty,
            counter.records_superseded = tracing::field::Empty,
            server_calls = tracing::field::Empty,
            grounded = tracing::field::Empty,
        )
    )]
    pub async fn continue_session(
        &self,
        runner: &Runner,
        session_id: &str,
        events: mpsc::Sender<EngineEvent>,
    ) -> Result<Reply, Error> {
        let turn_id = uuid::Uuid::new_v4().to_string();

        // held for the whole turn: serializes against a user send_message
        // on this session through the same guard map
        let guard = self.turn_guard(session_id);
        let _turn = guard.lock().await;

        self.enforce_pin(runner, session_id)?;
        let sources = self.sources(session_id, false, runner.role)?;
        let grants = self.grants(session_id, false)?;
        let command_prefix = self.command_prefix(session_id, false)?;

        self.drive_turn(
            runner,
            session_id,
            &turn_id,
            &sources,
            grants.as_ref(),
            &command_prefix,
            &events,
        )
        .await
    }

    /// The completion loop shared by `send_message` and `continue_session`:
    /// opens the transcript, drives tool steps and dispatch, and appends the
    /// reply. The caller has already taken the turn guard and resolved
    /// sources/grants; this only reads and appends from `turn_id` onward.
    #[allow(clippy::too_many_arguments)]
    async fn drive_turn(
        &self,
        runner: &Runner,
        session_id: &str,
        turn_id: &str,
        sources: &[ToolSource],
        grants: Option<&Arc<Grants>>,
        command_prefix: &[String],
        events: &mpsc::Sender<EngineEvent>,
    ) -> Result<Reply, Error> {
        let _ = events
            .send(EngineEvent::Accepted {
                session_id: session_id.to_owned(),
            })
            .await;

        let span = tracing::Span::current();
        let turn_start = std::time::Instant::now();
        let (mut transcript, system) = self.open_turn(runner, session_id)?;
        let mut total_usage: Option<Usage> = None;
        let mut steps = 0;
        let mut memory = MemoryCounters::default();
        let mut jobs: Vec<DispatchedJob> = Vec::new();
        let mut continues: Vec<ContinuedJob> = Vec::new();
        let mut server_calls = ServerCalls::default();
        let mut grounding: Option<String> = None;

        let reply = loop {
            // the last step offers no tools, so the model has to answer
            let last_step = steps >= max_tool_steps(runner.role);
            let request = self.completion_request(
                runner,
                system.clone(),
                transcript.clone(),
                last_step,
                sources,
            );

            let (ending, text, reasoning, calls) = self
                .run_completion(
                    runner,
                    session_id,
                    turn_id,
                    request,
                    events,
                    &mut total_usage,
                    &mut server_calls,
                    &mut grounding,
                )
                .await?;

            match ending {
                Ending::Done(Stop::ToolCalls) if !last_step && !calls.is_empty() => {
                    steps += 1;
                    span.record("tool_steps", steps);
                    self.tool_step(
                        runner,
                        session_id,
                        turn_id,
                        text,
                        reasoning,
                        calls,
                        sources,
                        grants,
                        command_prefix,
                        &mut transcript,
                        &mut memory,
                        &mut jobs,
                        &mut continues,
                        events,
                    )
                    .await?;
                }
                Ending::Done(_) => {
                    let elapsed_ms = elapsed_ms_since(turn_start);
                    let seq = self.append_reply(
                        session_id,
                        turn_id,
                        &text,
                        false,
                        total_usage,
                        elapsed_ms,
                        grounding.clone().unwrap_or_default(),
                    )?;
                    span.record("outcome", "done");
                    span.record("assistant_seq", seq);
                    break Ok(Reply {
                        session_id: session_id.to_owned(),
                        seq,
                        usage: total_usage,
                        partial: false,
                        step_capped: last_step,
                        grounding_json: grounding.clone().unwrap_or_default(),
                        jobs,
                        continues,
                    });
                }
                Ending::Cut if text.is_empty() => {
                    span.record("outcome", "error");
                    break Err(Error::EmptyReply);
                }
                Ending::Cut => {
                    let elapsed_ms = elapsed_ms_since(turn_start);
                    let seq = self.append_reply(
                        session_id,
                        turn_id,
                        &text,
                        true,
                        total_usage,
                        elapsed_ms,
                        grounding.clone().unwrap_or_default(),
                    )?;
                    span.record("outcome", "partial");
                    span.record("assistant_seq", seq);
                    break Ok(Reply {
                        session_id: session_id.to_owned(),
                        seq,
                        usage: None,
                        partial: true,
                        step_capped: last_step,
                        grounding_json: grounding.clone().unwrap_or_default(),
                        jobs,
                        continues,
                    });
                }
                Ending::Failed(error) => {
                    if !text.is_empty() {
                        let elapsed_ms = elapsed_ms_since(turn_start);
                        let seq = self.append_reply(
                            session_id,
                            turn_id,
                            &text,
                            true,
                            total_usage,
                            elapsed_ms,
                            grounding.clone().unwrap_or_default(),
                        )?;
                        span.record("assistant_seq", seq);
                    }
                    span.record("outcome", "error");
                    break Err(error.into());
                }
            }
        };
        // a call the model saw but never got a response for still happened
        while let Some((_, name, payload_json)) = server_calls.open.pop_front() {
            self.record(
                Source::Model,
                session_event::Event::ServerCallRecorded(ServerCallRecorded {
                    session_id: session_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    name,
                    arguments_json: payload_json,
                    response_json: String::new(),
                    provider_roundtrip: Vec::new(),
                }),
            )?;
            server_calls.recorded += 1;
        }
        span.record("server_calls", server_calls.recorded);
        span.record("grounded", grounding.is_some());
        memory.record_on(&span);
        reply
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_completion(
        &self,
        runner: &Runner,
        session_id: &str,
        turn_id: &str,
        request: CompletionRequest,
        events: &mpsc::Sender<EngineEvent>,
        total_usage: &mut Option<Usage>,
        server_calls: &mut ServerCalls,
        grounding: &mut Option<String>,
    ) -> Result<(Ending, String, String, Vec<ToolCall>), Error> {
        let mut stream = runner.provider.complete(request).await?;
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut calls = Vec::new();
        let ending = loop {
            match stream.next().await {
                Some(Ok(CompletionDelta::Text(chunk))) => {
                    text.push_str(&chunk);
                    let _ = events.send(EngineEvent::Delta(chunk)).await;
                }
                Some(Ok(CompletionDelta::Reasoning(chunk))) => {
                    reasoning.push_str(&chunk);
                    let _ = events.send(EngineEvent::Reasoning(chunk)).await;
                }
                Some(Ok(CompletionDelta::ToolCall(call))) => calls.push(call),
                Some(Ok(CompletionDelta::ServerCall { name, payload_json })) => {
                    let call_id = server_calls.synthetic_id();
                    server_calls.open.push_back((
                        call_id.clone(),
                        name.clone(),
                        payload_json.clone(),
                    ));
                    let _ = events
                        .send(EngineEvent::ToolCallStarted {
                            call_id,
                            index: 0,
                            name,
                            arguments_json: payload_json,
                        })
                        .await;
                }
                Some(Ok(CompletionDelta::ServerResponse { name, payload_json })) => {
                    if let Some((call_id, call_name, call_payload)) = server_calls.open.pop_front()
                    {
                        self.record(
                            Source::Model,
                            session_event::Event::ServerCallRecorded(ServerCallRecorded {
                                session_id: session_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                name: call_name,
                                arguments_json: call_payload,
                                response_json: payload_json,
                                provider_roundtrip: Vec::new(),
                            }),
                        )?;
                        server_calls.recorded += 1;
                        let _ = events
                            .send(EngineEvent::ToolCallEnded {
                                call_id,
                                outcome: ToolOutcome::Ok,
                            })
                            .await;
                    } else {
                        tracing::warn!(
                            name = %name,
                            "a server response arrived with no open server call"
                        );
                        self.record(
                            Source::Model,
                            session_event::Event::ServerCallRecorded(ServerCallRecorded {
                                session_id: session_id.to_owned(),
                                turn_id: turn_id.to_owned(),
                                name,
                                arguments_json: String::new(),
                                response_json: payload_json,
                                provider_roundtrip: Vec::new(),
                            }),
                        )?;
                        server_calls.recorded += 1;
                    }
                }
                Some(Ok(CompletionDelta::Grounding(json))) => *grounding = Some(json),
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
        Ok((ending, text, reasoning, calls))
    }

    #[allow(clippy::too_many_arguments)]
    async fn tool_step(
        &self,
        runner: &Runner,
        session_id: &str,
        turn_id: &str,
        text: String,
        reasoning: String,
        mut calls: Vec<ToolCall>,
        sources: &[ToolSource],
        grants: Option<&Arc<Grants>>,
        command_prefix: &[String],
        transcript: &mut Vec<Message>,
        memory: &mut MemoryCounters,
        jobs: &mut Vec<DispatchedJob>,
        continues: &mut Vec<ContinuedJob>,
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
                    ..Default::default()
                }),
            )?;
            transcript.push(Message::Text {
                role: Role::Assistant,
                content: text,
                reasoning: None,
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
                    arguments_json: call.arguments.clone(),
                })
                .await;
        }

        let mut results = Vec::with_capacity(calls.len());
        for call in &calls {
            let ctx = TurnContext {
                session_id: session_id.to_owned(),
                turn_id: turn_id.to_owned(),
                grants: grants.cloned(),
                command_prefix: command_prefix.to_vec(),
            };
            let DispatchOutcome {
                content,
                ok,
                truncated,
                memory_events,
                job_request,
                continue_request,
            } = self
                .registry
                .dispatch(&call.name, call.arguments.clone(), ctx, sources)
                .await;
            for memory_event in memory_events {
                memory.observe_event(&memory_event);
                self.record_memory(Source::Model, memory_event)?;
            }
            let (outcome, content) = if let Some(job_request) = job_request {
                let (outcome, content, job) = self.dispatch_job(runner, session_id, job_request);
                if let Some(job) = job {
                    jobs.push(job);
                }
                (outcome, content)
            } else if let Some(continue_request) = continue_request {
                let (outcome, content, cont) =
                    self.continue_job(runner, session_id, continue_request);
                if let Some(cont) = cont {
                    continues.push(cont);
                }
                (outcome, content)
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

        transcript.push(Message::ToolCalls {
            calls,
            reasoning: (!reasoning.is_empty()).then_some(reasoning),
        });
        for (call_id, content) in results {
            transcript.push(Message::ToolResult { call_id, content });
        }
        Ok(())
    }

    fn enforce_pin(&self, runner: &Runner, session_id: &str) -> Result<(), Error> {
        match self.with_store(|store| store.projection().session_role(session_id))? {
            // sessions logged before roles exist stay unpinned
            Some(pinned) if pinned == SessionRole::Unspecified as i32 => return Ok(()),
            Some(pinned) if pinned != runner.role as i32 => {
                return Err(Error::RoleMismatch {
                    session_id: session_id.to_owned(),
                    pinned: role_name(pinned),
                    serving: provider::role_label(runner.role).to_owned(),
                });
            }
            Some(_) | None => {}
        }
        match self.identity_mismatch(runner, runner.role, session_id) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// The 3.2 pin's model half (row 6.32): `role`'s identity right now
    /// against what `session_id` was recorded on. `None` if they match, the
    /// session predates provider/model stamping, or the session is unknown —
    /// mirrors how an `Unspecified` recorded role stays unpinned.
    fn identity_mismatch(
        &self,
        runner: &Runner,
        role: SessionRole,
        session_id: &str,
    ) -> Option<Error> {
        let recorded = self
            .with_store(|store| store.projection().session_identity(session_id))
            .ok()
            .flatten()?;
        if recorded.0.is_empty() && recorded.1.is_empty() {
            return None;
        }
        let current = self
            .role_identities
            .get(&role)
            .cloned()
            .unwrap_or_else(|| (runner.provider.name().to_owned(), runner.model.clone()));
        if recorded == current {
            return None;
        }
        Some(Error::ModelMismatch {
            session_id: session_id.to_owned(),
            pinned: identity_label(&recorded.0, &recorded.1),
            role: provider::role_label(role).to_owned(),
            serving: identity_label(&current.0, &current.1),
        })
    }

    // stable first, volatile after: everything here is prefix the provider caches,
    // so anything that changes per turn has to go in the messages instead
    fn system_prompt(
        runner: &Runner,
        start_date: Option<&str>,
        memory_index: Option<&str>,
    ) -> Option<String> {
        let mut parts: Vec<&str> = Vec::new();
        if let Some(identity) = &runner.system {
            parts.push(identity);
        }
        if let Some(date) = start_date {
            parts.push(date);
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

    pub fn session_title(&self, session_id: &str) -> Result<Option<String>, Error> {
        Ok(self.with_store(|store| store.session_title(session_id))?)
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
        let session_id = session_id_of(&payload).to_owned();
        let payload = event::Payload::Session(SessionEvent {
            event: Some(payload),
        });
        let seq = self.with_store_mut(|store| store.append(source, Some(now_ts()), payload))?;
        self.notify_appended(session_id);
        Ok(seq)
    }

    fn notify_appended(&self, session_id: String) {
        let Some(notifier) = &self.notifier else {
            return;
        };
        let _ = notifier.send(Notification {
            event: Some(notification::Event::SessionAppended(SessionAppended {
                session_id,
            })),
        });
    }

    /// The review queue's current size, over the same lookback the `:review`
    /// pane defaults to, so a live count matches what opening it shows.
    pub fn review_pending(&self) -> Result<u32, Error> {
        let since = chrono::Utc::now().timestamp_micros() - projection::REVIEW_WINDOW_MICROS;
        let items = self.with_store(|store| store.projection().review_items(since))?;
        Ok(u32::try_from(items.len()).unwrap_or(u32::MAX))
    }

    fn notify_review_changed(&self) -> Result<(), Error> {
        let Some(notifier) = &self.notifier else {
            return Ok(());
        };
        let pending = self.review_pending()?;
        let _ = notifier.send(Notification {
            event: Some(notification::Event::ReviewChanged(ReviewChanged {
                pending,
            })),
        });
        Ok(())
    }

    fn record_memory(&self, source: Source, payload: memory_event::Event) -> Result<u64, Error> {
        let payload = event::Payload::Memory(MemoryEvent {
            event: Some(payload),
        });
        let seq = self.with_store_mut(|store| store.append(source, Some(now_ts()), payload))?;
        self.notify_review_changed()?;
        Ok(seq)
    }

    #[allow(clippy::too_many_arguments)]
    fn append_reply(
        &self,
        session_id: &str,
        turn_id: &str,
        text: &str,
        partial: bool,
        usage: Option<Usage>,
        elapsed_ms: u32,
        grounding_json: String,
    ) -> Result<u64, Error> {
        let usage = usage.unwrap_or_default();
        self.record(
            Source::Model,
            session_event::Event::MessageAppended(MessageAppended {
                session_id: session_id.to_owned(),
                role: Role::Assistant as i32,
                content: text.to_owned(),
                partial,
                turn_id: turn_id.to_owned(),
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                elapsed_ms,
                grounding_json,
            }),
        )
    }

    fn open_turn(
        &self,
        runner: &Runner,
        session_id: &str,
    ) -> Result<(Vec<Message>, Option<String>), Error> {
        let (rows, memory_index, started_at) = self.with_store(|store| {
            Ok::<_, Error>((
                store.projection().messages(session_id)?,
                store.projection().memory_index()?,
                store.projection().session_started_at(session_id)?,
            ))
        })?;
        let start_date = (runner.role == SessionRole::Concierge)
            .then(|| started_at.and_then(start_date_line))
            .flatten();
        let system = Self::system_prompt(
            runner,
            start_date.as_deref(),
            render_memory_index(&memory_index).as_deref(),
        );
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
            web: sources.contains(&ToolSource::Web),
        }
    }
}

// frozen at session start so the prefix stays byte-stable across turns
fn start_date_line(micros: i64) -> Option<String> {
    let date = chrono::DateTime::from_timestamp_micros(micros)?
        .with_timezone(&chrono::Local)
        .format("%Y-%m-%d");
    Some(format!("This conversation started on {date}."))
}

const MAX_HANDBACK_SUMMARY_BYTES: usize = 2048;

fn truncate_summary(summary: &str) -> String {
    if summary.len() <= MAX_HANDBACK_SUMMARY_BYTES {
        return summary.to_owned();
    }
    let mut cut = MAX_HANDBACK_SUMMARY_BYTES;
    while !summary.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{} [truncated]", &summary[..cut])
}

fn session_id_of(event: &session_event::Event) -> &str {
    match event {
        session_event::Event::SessionCreated(e) => &e.session_id,
        session_event::Event::MessageAppended(e) => &e.session_id,
        session_event::Event::ToolCallIssued(e) => &e.session_id,
        session_event::Event::ToolResultRecorded(e) => &e.session_id,
        session_event::Event::ServerCallRecorded(e) => &e.session_id,
        session_event::Event::SessionConsolidated(e) => &e.session_id,
        session_event::Event::SessionTitled(e) => &e.session_id,
    }
}

fn role_name(role: i32) -> String {
    match SessionRole::try_from(role) {
        Ok(role) => provider::role_label(role).to_owned(),
        Err(_) => format!("unknown role {role}"),
    }
}

fn identity_label(provider: &str, model: &str) -> String {
    if provider.is_empty() {
        model.to_owned()
    } else {
        format!("{provider}/{model}")
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
            MessageRow::Message { .. } | MessageRow::ServerCall { .. } => {}
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
                        reasoning: None,
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
                    messages.push(Message::ToolCalls {
                        calls: answered,
                        reasoning: None,
                    });
                    messages.append(&mut step_results);
                }
            }
            MessageRow::ToolResult { call_id, .. } => {
                if !issued.contains(call_id.as_str()) {
                    tracing::warn!(%call_id, "skipping a tool result no call claimed");
                }
                i += 1;
            }
            // provider-side and already resolved: replaying it as ours would
            // tell the model it issued a call it never did; the answer text
            // it produced carries what the search contributed
            MessageRow::ServerCall { .. } => {
                i += 1;
            }
        }
    }
    messages
}

// server-side calls a provider resolves inside its own turn (Gemini's
// google_search): tracked separately from `calls` because they never touch
// the tool-step counter or the orphan machinery
#[derive(Debug, Default)]
struct ServerCalls {
    open: VecDeque<(String, String, String)>, // (synthetic call_id, name, payload_json)
    next_id: u32,
    recorded: u32,
}

impl ServerCalls {
    fn synthetic_id(&mut self) -> String {
        let id = format!("web-{}", self.next_id);
        self.next_id += 1;
        id
    }
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
        HistoryEntry, HistoryMessage, HistoryToolCall, HistoryToolResult, MemoryRecord,
        MemoryRecordCreated, MemoryRecordSuperseded, Role, SessionRole, Source, ToolOutcome,
        history_entry, memory_event, memory_record, session_event,
    };
    use tempfile::TempDir;

    use super::{
        ContinuedJob, DispatchedJob, Engine, EngineEvent, Error, MAX_TOOL_STEPS, MemoryCounters,
        ProjectSpec, Runner,
    };
    use crate::log::Log;
    use crate::projection::Projection;
    use crate::provider::{
        CompletionDelta, Error as ProviderError, Message, Provider, Stop, Thinking, ToolCall, Usage,
    };
    use crate::store::{self, Store};
    use crate::testkit::{
        Canned, PrefixEcho, ScriptedProvider, Step, TraceCapture, appended, call, call_carrying,
        channel, counter_samples, done_reply, drain, engine, engine_with_role, engine_with_tools,
        engine_with_tools_at, issued, reopened_engine, replay_events, replay_log, resulted, runner,
        runner_with_role, seed_log, seed_memory_log, seed_memory_log_at, server_called, tool_stop,
        tools, turn, usage,
    };
    use crate::tool::builtin::dispatch::Dispatch;
    use crate::tool::workspace::{self, Grant, Mode, Workspace};
    use crate::tool::{Intent, JobRequest, Registry, Tool, ToolReply, ToolSource, TurnContext};

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
            dispatched_by: String::new(),
        })
    }

    fn seeded_message(role: Role, content: &str) -> session_event::Event {
        session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
            session_id: "s-01".to_owned(),
            role: role as i32,
            content: content.to_owned(),
            partial: false,
            turn_id: "t-01".to_owned(),
            ..Default::default()
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

    fn sourced_entry(role: i32, content: &str, source: Source) -> HistoryEntry {
        HistoryEntry {
            entry: Some(history_entry::Entry::Message(HistoryMessage {
                role,
                content: content.to_owned(),
                partial: false,
                source: source as i32,
                ..Default::default()
            })),
        }
    }

    fn prose_entry(role: i32, content: &str, partial: bool) -> HistoryEntry {
        let source = match Role::try_from(role) {
            Ok(Role::User) => Source::User,
            Ok(Role::Assistant) => Source::Model,
            _ => Source::Unspecified,
        };
        HistoryEntry {
            entry: Some(history_entry::Entry::Message(HistoryMessage {
                role,
                content: content.to_owned(),
                partial,
                source: source as i32,
                ..Default::default()
            })),
        }
    }

    fn assistant_entry_with_usage(
        content: &str,
        input_tokens: u32,
        output_tokens: u32,
    ) -> HistoryEntry {
        HistoryEntry {
            entry: Some(history_entry::Entry::Message(HistoryMessage {
                role: Role::Assistant as i32,
                content: content.to_owned(),
                partial: false,
                source: Source::Model as i32,
                input_tokens,
                output_tokens,
                ..Default::default()
            })),
        }
    }

    // elapsed_ms is wall time, not worth pinning down in a scripted test
    fn ignoring_elapsed(mut entries: Vec<HistoryEntry>) -> Vec<HistoryEntry> {
        for entry in &mut entries {
            if let Some(history_entry::Entry::Message(message)) = &mut entry.entry {
                message.elapsed_ms = 0;
            }
        }
        entries
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
            dispatched_by: String::new(),
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
        assert!(!reply.step_capped, "the model finished on its own");
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
    async fn a_notifier_receives_session_appended_for_every_durable_append() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello there")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (notifier, mut notifications) = tokio::sync::broadcast::channel(16);
        let engine = engine.with_notifier(notifier);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send_message");

        let mut session_ids = Vec::new();
        while let Ok(notification) = notifications.try_recv() {
            match notification.event {
                Some(arc_proto::v1::notification::Event::SessionAppended(appended)) => {
                    session_ids.push(appended.session_id);
                }
                other => panic!("expected SessionAppended, got {other:?}"),
            }
        }
        assert_eq!(
            session_ids,
            vec![reply.session_id.clone(); 3],
            "SessionCreated, the user message, and the assistant reply each notify"
        );
    }

    #[tokio::test]
    async fn with_no_notifier_configured_a_send_still_appends_normally() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello there")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send_message");

        assert_eq!(replay_log(dir.path()).len(), 3, "the append is unaffected");
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
        assert_eq!(
            requests[1].system,
            Some(format!("be terse\n\n{}", today_line()))
        );
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
                    dispatched_by: String::new(),
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
                    dispatched_by: String::new(),
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
                    ..Default::default()
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
        let Message::ToolCalls { calls, .. } = &requests[1].messages[1] else {
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
                    arguments_json: r#"{"q":1}"#.to_owned(),
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
    async fn a_server_call_records_durably_paints_the_client_and_never_touches_the_step_counter() {
        let provider = ScriptedProvider::scripted(vec![vec![
            Ok(CompletionDelta::ServerCall {
                name: "google_search".to_owned(),
                payload_json: r#"{"query":"arc release"}"#.to_owned(),
            }),
            Ok(CompletionDelta::ServerResponse {
                name: "google_search".to_owned(),
                payload_json: r#"{"results":["arc 3.5"]}"#.to_owned(),
            }),
            Ok(CompletionDelta::Text("arc shipped 3.5".to_owned())),
            Ok(CompletionDelta::Grounding(r#"{"chunks":["x"]}"#.to_owned())),
            // a stray ToolCalls stop with no real calls must not start a tool
            // step; the script has only one entry, so a second request panics
            Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::ToolCalls,
            }),
        ]]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, mut rx) = channel();

        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        assert!(!reply.step_capped, "server calls are not tool steps");

        let events = replay_log(dir.path());
        assert_eq!(events.len(), 4, "created, user message, server call, reply");
        let user = appended(&events[1]);
        let call = server_called(&events[2]);
        let assistant = appended(&events[3]);

        assert_eq!(call.turn_id, user.turn_id, "one turn, one id");
        assert_eq!(call.name, "google_search");
        assert_eq!(call.arguments_json, r#"{"query":"arc release"}"#);
        assert_eq!(call.response_json, r#"{"results":["arc 3.5"]}"#);

        assert_eq!(assistant.content, "arc shipped 3.5");
        assert_eq!(assistant.grounding_json, r#"{"chunks":["x"]}"#);

        assert_eq!(
            drain(&mut rx),
            [
                EngineEvent::Accepted {
                    session_id: reply.session_id.clone()
                },
                EngineEvent::ToolCallStarted {
                    call_id: "web-0".to_owned(),
                    index: 0,
                    name: "google_search".to_owned(),
                    arguments_json: r#"{"query":"arc release"}"#.to_owned(),
                },
                EngineEvent::ToolCallEnded {
                    call_id: "web-0".to_owned(),
                    outcome: ToolOutcome::Ok,
                },
                EngineEvent::Delta("arc shipped 3.5".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn an_unclosed_server_call_lands_with_an_empty_response_at_turn_end() {
        let provider = ScriptedProvider::scripted(vec![vec![
            Ok(CompletionDelta::ServerCall {
                name: "google_search".to_owned(),
                payload_json: r#"{"query":"arc release"}"#.to_owned(),
            }),
            Ok(CompletionDelta::Text("still searching".to_owned())),
            Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            }),
        ]]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let events = replay_log(dir.path());
        assert_eq!(
            events.len(),
            4,
            "created, user message, assistant reply, flushed server call"
        );
        let call = server_called(&events[3]);
        assert_eq!(call.name, "google_search");
        assert_eq!(call.arguments_json, r#"{"query":"arc release"}"#);
        assert!(
            call.response_json.is_empty(),
            "the search happened; losing it would un-replay what the model saw"
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
                Message::ToolCalls { calls, .. } => Some(calls.clone()),
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
        let Message::ToolCalls { calls, .. } = &requests[1].messages[1] else {
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
        let Message::ToolCalls { calls, .. } = &messages[1] else {
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
                    reasoning: None,
                },
                Message::Text {
                    role: Role::User,
                    content: "again".to_owned(),
                    reasoning: None,
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
                    ..Default::default()
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
                sourced_entry(Role::User as i32, "question", Source::System),
                sourced_entry(99, "from the future", Source::System),
                HistoryEntry {
                    entry: Some(history_entry::Entry::ToolCall(HistoryToolCall {
                        call_id: "c1".to_owned(),
                        name: "lookup".to_owned(),
                        arguments_json: "{}".to_owned(),
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
            ],
            "reasoning never survives a reopen: the log-rebuilt transcript has none"
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
    async fn a_multi_tool_step_turn_stamps_usage_only_on_the_final_append() {
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

        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        assert_eq!(
            reply.usage,
            Some(Usage {
                input_tokens: 6,
                output_tokens: 10
            }),
            "usage accumulates across both completion steps"
        );

        let events = replay_log(dir.path());
        let user = appended(&events[1]);
        let step_text = appended(&events[2]);
        let final_text = appended(&events[5]);

        assert_eq!(
            (user.input_tokens, user.output_tokens, user.elapsed_ms),
            (0, 0, 0)
        );
        assert_eq!(
            (
                step_text.input_tokens,
                step_text.output_tokens,
                step_text.elapsed_ms
            ),
            (0, 0, 0),
            "the intermediate tool-step assistant append stays zero"
        );
        assert_eq!(
            (final_text.input_tokens, final_text.output_tokens),
            (6, 10),
            "only the turn's final assistant append carries the accumulated usage"
        );
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
        assert!(
            reply.step_capped,
            "every step was spent wanting to keep going"
        );
    }

    #[tokio::test]
    async fn an_executor_turn_runs_past_the_concierge_step_cap() {
        let mut script: Vec<Vec<Result<CompletionDelta, ProviderError>>> = (0..MAX_TOOL_STEPS + 4)
            .map(|step| {
                vec![
                    Ok(call(&format!("c{step}"), 0, "alpha", "{}")),
                    Ok(tool_stop()),
                ]
            })
            .collect();
        script.push(done_reply("done"));
        let provider = ScriptedProvider::scripted(script);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine_with_tools(&provider, &dir, tools(&[("alpha", "A", true)]));
        let run = Runner {
            role: SessionRole::Executor,
            ..run
        };
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let requests = provider.requests();
        assert_eq!(requests.len(), MAX_TOOL_STEPS + 5);
        assert!(
            requests.iter().all(|r| !r.tools.is_empty()),
            "no forced tool-less completion before the executor cap"
        );
        assert!(!reply.partial);
        assert!(
            !reply.step_capped,
            "the executor's own cap is 256, not this"
        );
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
    async fn a_steps_reasoning_replays_on_the_next_steps_request() {
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(CompletionDelta::Reasoning("checking the time".to_owned())),
                Ok(call("t1", 0, "clock", "{}")),
                Ok(tool_stop()),
            ],
            done_reply("it is noon"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine_with_tools(&provider, &dir, tools(&[("clock", "noon", true)]));
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "what time is it?", tx)
            .await
            .expect("send");

        let requests = provider.requests();
        let Message::ToolCalls { reasoning, .. } = &requests[1].messages[1] else {
            panic!("expected the calls, got {:?}", requests[1].messages[1]);
        };
        assert_eq!(reasoning.as_deref(), Some("checking the time"));
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
            provider.requests()[0].system,
            Some(format!("be terse\n\n{}", today_line())),
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

    fn today_line() -> String {
        super::start_date_line(chrono::Local::now().timestamp_micros()).expect("valid now")
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
            Some(format!(
                "be terse\n\n{}\n\n{}",
                today_line(),
                seeded_block()
            ))
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
        let expected = Some(format!(
            "be terse\n\n{}\n\n{}",
            today_line(),
            seeded_block()
        ));
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

        assert_eq!(
            provider.requests()[0].system,
            Some(format!("be terse\n\n{}", today_line()))
        );
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

        assert_eq!(
            provider.requests()[0].system,
            Some(format!("{}\n\n{}", today_line(), seeded_block()))
        );
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
                    r#"{"kind":"preference","namespace":"global","title":"Terse replies",
                        "summary":"prefers short answers","body":"Prefers short answers."}"#,
                )),
                Ok(tool_stop()),
            ],
            done_reply("saved"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut registry = Registry::new(512);
        registry.register(Box::new(crate::tool::builtin::memory::MemoryWrite::new(
            vec!["global".to_owned(), "arc".to_owned()],
        )));
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
    async fn an_inline_memory_write_pushes_a_review_changed_with_the_grown_count() {
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "memory_write",
                    r#"{"kind":"preference","namespace":"global","title":"Terse replies",
                        "summary":"prefers short answers","body":"Prefers short answers."}"#,
                )),
                Ok(tool_stop()),
            ],
            done_reply("saved"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut registry = Registry::new(512);
        registry.register(Box::new(crate::tool::builtin::memory::MemoryWrite::new(
            vec!["global".to_owned(), "arc".to_owned()],
        )));
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let (notifier, mut notifications) = tokio::sync::broadcast::channel(16);
        let engine = engine.with_notifier(notifier);

        let (tx, _rx) = channel();
        engine
            .send_message(&run, None, "remember this", tx)
            .await
            .expect("send");

        let mut pending_seen = Vec::new();
        while let Ok(notification) = notifications.try_recv() {
            if let Some(arc_proto::v1::notification::Event::ReviewChanged(changed)) =
                notification.event
            {
                pending_seen.push(changed.pending);
            }
        }
        assert_eq!(
            pending_seen,
            [1],
            "the new record is the only one pending review"
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
                        continue_request: None,
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
            Some(format!(
                "be terse\n\n{}\n\n{}",
                today_line(),
                seeded_block()
            ))
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
    async fn a_review_verdict_pushes_a_review_changed_with_the_shrunk_count() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        seed_memory_log_at(
            &dir,
            seeded_records(),
            chrono::Utc::now().timestamp_micros(),
        );
        let (engine, _run) = reopened_engine(&provider, &dir, Registry::new(512));
        let (notifier, mut notifications) = tokio::sync::broadcast::channel(16);
        let engine = engine.with_notifier(notifier);

        engine.review_accept("mr-fact").expect("accept");

        let notification = notifications.try_recv().expect("a notification was pushed");
        match notification.event {
            Some(arc_proto::v1::notification::Event::ReviewChanged(changed)) => {
                assert_eq!(
                    changed.pending, 1,
                    "one of the two seeded records left the queue"
                );
            }
            other => panic!("expected ReviewChanged, got {other:?}"),
        }
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
                command_prefix: Vec::new(),
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
    async fn a_project_bound_session_gets_its_configured_command_prefix_in_turn_context() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(&dir, vec![seeded_session_with_project("arc")]);
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "prefix", "{}")), Ok(tool_stop())],
            done_reply("done"),
        ]);
        let mut registry = Registry::new(512);
        registry.register(Box::new(PrefixEcho {
            name: "prefix",
            source: ToolSource::Workspace,
        }));
        let mut projects = BTreeMap::new();
        projects.insert(
            "arc".to_owned(),
            ProjectSpec {
                sources: vec![ToolSource::Builtin, ToolSource::Workspace],
                grants: Vec::new(),
                command_prefix: vec!["nix".to_owned(), "develop".to_owned(), "-c".to_owned()],
            },
        );
        let (engine, run) = reopened_engine_with_projects(&provider, &dir, registry, projects);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, Some("s-01"), "run it", tx)
            .await
            .expect("send");

        let events = replay_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.content, "nix,develop,-c");
    }

    #[tokio::test]
    async fn an_unbound_session_gets_an_empty_command_prefix_in_turn_context() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "prefix", "{}")), Ok(tool_stop())],
            done_reply("done"),
        ]);
        let mut registry = Registry::new(512);
        registry.register(Box::new(PrefixEcho {
            name: "prefix",
            source: ToolSource::Builtin,
        }));
        let mut projects = BTreeMap::new();
        projects.insert(
            "arc".to_owned(),
            ProjectSpec {
                sources: vec![ToolSource::Builtin, ToolSource::Workspace],
                grants: Vec::new(),
                command_prefix: vec!["nix".to_owned(), "develop".to_owned(), "-c".to_owned()],
            },
        );
        let (engine, run) = reopened_engine_with_projects(&provider, &dir, registry, projects);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "run it", tx)
            .await
            .expect("send");

        let events = replay_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.content, "");
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
                command_prefix: Vec::new(),
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
                command_prefix: Vec::new(),
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

    fn expert_tool() -> Box<dyn crate::tool::Tool> {
        Box::new(Canned {
            name: "consult_expert",
            content: "",
            ok: true,
            source: ToolSource::Expert,
        })
    }

    fn engine_with_expert(dir: &TempDir, enabled: bool) -> Engine {
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let mut registry = Registry::new(512);
        registry.register(expert_tool());
        Engine::new(Store::new(log, projection), registry).with_expert_enabled(enabled)
    }

    #[tokio::test]
    async fn concierge_holds_consult_expert_when_counsel_is_configured() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let engine = engine_with_expert(&dir, true);
        let run = runner_with_role(&provider, SessionRole::Concierge);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let requests = provider.requests();
        assert!(
            requests[0]
                .tools
                .iter()
                .any(|def| def.name == "consult_expert"),
            "{:?}",
            requests[0].tools
        );
    }

    #[tokio::test]
    async fn executor_holds_consult_expert_when_counsel_is_configured() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let engine = engine_with_expert(&dir, true);
        let run = runner_with_role(&provider, SessionRole::Executor);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let requests = provider.requests();
        assert!(
            requests[0]
                .tools
                .iter()
                .any(|def| def.name == "consult_expert"),
            "{:?}",
            requests[0].tools
        );
    }

    #[tokio::test]
    async fn archivist_never_holds_consult_expert_even_when_counsel_is_configured() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let engine = engine_with_expert(&dir, true);
        let run = runner_with_role(&provider, SessionRole::Archivist);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let requests = provider.requests();
        assert!(
            requests[0]
                .tools
                .iter()
                .all(|def| def.name != "consult_expert"),
            "{:?}",
            requests[0].tools
        );
    }

    #[tokio::test]
    async fn nobody_holds_consult_expert_when_counsel_is_not_configured() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![done_reply("ok"), done_reply("ok")]);
        let engine = engine_with_expert(&dir, false);

        for role in [SessionRole::Concierge, SessionRole::Executor] {
            let run = runner_with_role(&provider, role);
            let (tx, _rx) = channel();
            engine
                .send_message(&run, None, "hi", tx)
                .await
                .expect("send");
        }

        let requests = provider.requests();
        assert!(
            requests
                .iter()
                .all(|request| request.tools.iter().all(|def| def.name != "consult_expert")),
            "{:?}",
            requests.iter().map(|r| &r.tools).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn a_concierge_session_asks_the_provider_for_web_grounding() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let (engine, run) = engine_with_role(&provider, &dir, SessionRole::Concierge);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        assert!(provider.requests()[0].web, "concierge is a web capability");
    }

    #[tokio::test]
    async fn executor_and_archivist_sessions_never_ask_for_web_grounding() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![done_reply("ok"), done_reply("ok")]);
        let (engine, _) = engine(&provider, &dir);

        for role in [SessionRole::Executor, SessionRole::Archivist] {
            let run = runner_with_role(&provider, role);
            let (tx, _rx) = channel();
            engine
                .send_message(&run, None, "hi", tx)
                .await
                .expect("send");
        }

        assert!(
            provider.requests().iter().all(|request| !request.web),
            "{:?}",
            provider
                .requests()
                .iter()
                .map(|r| r.web)
                .collect::<Vec<_>>()
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
        projects.insert(
            name.to_owned(),
            ProjectSpec {
                sources,
                grants,
                command_prefix: Vec::new(),
            },
        );
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
    async fn create_bound_session_with_role_identities_set_records_the_childs_own_provider_and_model()
     {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let provider = ScriptedProvider::scripted(vec![]);
        let (mut engine, run) = engine_with_tools(&provider, &dir, Registry::new(512));
        engine = engine
            .with_projects(projects_with(
                "arc",
                vec![ToolSource::Builtin, ToolSource::Workspace],
                vec![Grant::new(&root, Mode::ReadWrite)],
            ))
            .with_role_identities(BTreeMap::from([(
                SessionRole::Executor,
                ("opencode".to_owned(), "deepseek-v4-pro".to_owned()),
            )]));

        let session_id = engine
            .create_bound_session(&run, "arc", SessionRole::Executor, None)
            .expect("create a bound session");

        let events = replay_log(dir.path());
        let session_event::Event::SessionCreated(created) = &events[0] else {
            panic!("expected SessionCreated first, got {:?}", events[0]);
        };
        assert_eq!(created.session_id, session_id);
        assert_eq!(
            created.provider, "opencode",
            "the child role's own identity, not the dispatching runner's"
        );
        assert_eq!(created.model, "deepseek-v4-pro");
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
    async fn create_direct_session_records_source_user_and_no_dispatch_metadata() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let provider = ScriptedProvider::scripted(vec![]);
        let (mut engine, run) = engine_with_tools(&provider, &dir, Registry::new(512));
        engine = engine
            .with_projects(projects_with(
                "arc",
                vec![ToolSource::Builtin, ToolSource::Workspace],
                vec![Grant::new(&root, Mode::ReadWrite)],
            ))
            .with_role_identities(BTreeMap::from([(
                SessionRole::Executor,
                ("opencode".to_owned(), "deepseek-v4-pro".to_owned()),
            )]));

        let session_id = engine
            .create_direct_session(&run, "arc", SessionRole::Executor)
            .expect(":code opens a direct session");

        let events = replay_log(dir.path());
        assert_eq!(events.len(), 1, "no dispatch, so nothing else was appended");
        let session_event::Event::SessionCreated(created) = &events[0] else {
            panic!("expected SessionCreated, got {:?}", events[0]);
        };
        assert_eq!(created.session_id, session_id);
        assert_eq!(created.role, SessionRole::Executor as i32);
        assert_eq!(
            created.provider, "opencode",
            "the executor role's own identity, not the caller's"
        );
        assert_eq!(created.model, "deepseek-v4-pro");
        assert!(created.budget.is_none(), "the user is present; no budget");
        assert_eq!(
            created.dispatched_by, "",
            "a :code session is a root conversation, not a dispatched job"
        );
        assert_eq!(
            created.grants,
            [arc_proto::v1::WorkspaceGrant {
                root: root
                    .canonicalize()
                    .expect("canon")
                    .to_string_lossy()
                    .into_owned(),
                read_write: true,
            }],
            "implement-style: read-write, no downgrade"
        );

        let raw_events = replay_events(dir.path());
        assert_eq!(
            raw_events[0].source,
            Source::User as i32,
            "the user asked for this, not a model"
        );
    }

    #[tokio::test]
    async fn create_direct_session_names_an_unknown_project() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);

        let err = engine
            .create_direct_session(&run, "ghost", SessionRole::Executor)
            .expect_err("an unconfigured project must be refused");

        assert!(matches!(err, Error::UnknownProject { ref project } if project == "ghost"));
        assert!(err.to_string().contains("ghost"));
        assert_eq!(replay_log(dir.path()).len(), 0, "nothing was appended");
    }

    #[tokio::test]
    async fn a_turn_served_into_a_direct_session_resolves_its_grants() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        std::fs::write(root.join("inside.txt"), "hi").expect("write");

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
        let executor_run = runner_with_role(&provider, SessionRole::Executor);

        let session_id = engine
            .create_direct_session(&run, "arc", SessionRole::Executor)
            .expect("create a direct session");
        let (tx, _rx) = channel();
        engine
            .send_message(&executor_run, Some(&session_id), "read it", tx)
            .await
            .expect("send");

        let events = replay_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert_eq!(result.content, "hi");
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

    fn dispatch_args(role: &str, project: &str, brief: &str, intent: &str) -> String {
        serde_json::json!({
            "role": role,
            "project": project,
            "brief": brief,
            "intent": intent,
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
                    &dispatch_args("executor", "arc", "fix the bug", "implement"),
                )),
                Ok(tool_stop()),
            ],
            done_reply("dispatched"),
        ]);
        let mut registry = Registry::new(512);
        registry.register(Box::new(Dispatch::new(
            vec![("arc".to_owned(), String::new())],
            None,
        )));
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));
        let (tx, _rx) = channel();

        let reply = engine
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
            reply.jobs,
            [DispatchedJob {
                session_id: child.session_id.clone(),
                parent_session: reply.session_id.clone(),
                role: SessionRole::Executor,
                project: "arc".to_owned(),
                brief: "fix the bug".to_owned(),
                budget: None,
            }],
            "budgets are suspended; the dispatched job carries none"
        );
        assert_eq!(child.budget, None);
        assert_eq!(
            child.grants,
            [arc_proto::v1::WorkspaceGrant {
                root: root
                    .canonicalize()
                    .expect("canon")
                    .to_string_lossy()
                    .into_owned(),
                read_write: true,
            }],
            "implement records the root grant read-write, as before"
        );
        let child_id = child.session_id.clone();

        let result = resulted(&events[4]);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert!(result.content.contains(&child_id), "{}", result.content);
        assert!(result.content.contains("executor"), "{}", result.content);
        assert!(result.content.contains("arc"), "{}", result.content);
        assert!(
            result.content.contains("(implement: read-write)"),
            "{}",
            result.content
        );

        // the role-mismatch pin keys on the recorded role, not the runner
        // that created the session, so a same-identity executor runner
        // (matching what an unconfigured role_identities map recorded: the
        // dispatching runner's own provider and model) can continue it
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("on it")]);
        let executor_run = Runner {
            role: SessionRole::Executor,
            provider: Arc::clone(&executor_provider) as Arc<dyn Provider>,
            model: "test-model".to_owned(),
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
    async fn an_analyze_dispatch_records_the_root_grant_read_only_but_still_allows_read() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        std::fs::write(root.join("f.txt"), b"x").expect("write f.txt");

        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "dispatch",
                    &dispatch_args("executor", "arc", "check consistency", "analyze"),
                )),
                Ok(tool_stop()),
            ],
            done_reply("dispatched"),
        ]);
        let mut registry = Registry::new(512);
        registry.register(Box::new(Dispatch::new(
            vec![("arc".to_owned(), String::new())],
            None,
        )));
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
        let session_event::Event::SessionCreated(child) = &events[3] else {
            panic!("expected the child SessionCreated, got {:?}", events[3]);
        };
        assert_eq!(
            child.grants,
            [arc_proto::v1::WorkspaceGrant {
                root: root
                    .canonicalize()
                    .expect("canon")
                    .to_string_lossy()
                    .into_owned(),
                read_write: false,
            }],
            "analyze records the project root read-only, not the configured read-write"
        );

        let result = resulted(&events[4]);
        assert!(
            result
                .content
                .contains("(analyze: read-only, it reports but cannot edit)"),
            "{}",
            result.content
        );

        let grants = workspace::Grants::from_recorded(vec![(
            root.canonicalize().expect("canon"),
            Mode::ReadOnly,
        )]);
        let target = root.join("f.txt");
        let target = target.to_str().expect("utf8");
        grants
            .resolve(target, workspace::Access::Read)
            .expect("read is still allowed under an analyze grant");
        let err = grants
            .resolve(target, workspace::Access::Write)
            .expect_err("write is refused under an analyze grant");
        assert!(err.contains("read-only"), "{err}");
    }

    #[tokio::test]
    async fn a_resumed_analyze_job_still_carries_its_recorded_read_only_grant() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "dispatch",
                    &dispatch_args("executor", "arc", "check consistency", "analyze"),
                )),
                Ok(tool_stop()),
            ],
            done_reply("dispatched"),
        ]);
        let mut registry = Registry::new(512);
        registry.register(Box::new(Dispatch::new(
            vec![("arc".to_owned(), String::new())],
            None,
        )));
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
        let session_event::Event::SessionCreated(child) = &events[3] else {
            panic!("expected the child SessionCreated, got {:?}", events[3]);
        };
        let child_id = child.session_id.clone();

        // config here still says read-write; a resumed job goes by its own
        // recorded grant, not by re-resolving the project (5.1's rule)
        let (reopened, _) = reopened_engine(
            &ScriptedProvider::scripted(vec![]),
            &dir,
            Registry::new(512),
        );
        let reopened = reopened.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));
        let grants = reopened
            .grants(&child_id, false)
            .expect("grants lookup")
            .expect("a bound job carries grants");
        assert_eq!(
            grants.canonical_roots().to_vec(),
            vec![(root.canonicalize().expect("canon"), Mode::ReadOnly)]
        );
    }

    fn continue_job_args(session_id: &str, message: &str) -> String {
        serde_json::json!({
            "session_id": session_id,
            "message": message,
        })
        .to_string()
    }

    fn tool_result(events: &[session_event::Event]) -> &arc_proto::v1::ToolResultRecorded {
        events
            .iter()
            .find_map(|event| match event {
                session_event::Event::ToolResultRecorded(result) => Some(result),
                _ => None,
            })
            .expect("a recorded tool result")
    }

    #[tokio::test]
    async fn continue_job_on_an_existing_executor_child_lands_in_reply_continues() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let mut registry = Registry::new(512);
        registry.register(Box::new(crate::tool::builtin::continue_job::ContinueJob));
        // a throwaway provider: create_bound_session never drives it
        let (engine, bootstrap_run) =
            engine_with_tools(&ScriptedProvider::scripted(vec![]), &dir, registry);
        let engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));
        let child_id = engine
            .create_bound_session(&bootstrap_run, "arc", SessionRole::Executor, None)
            .expect("create the child durably");

        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "continue_job",
                    &continue_job_args(&child_id, "also check the linter"),
                )),
                Ok(tool_stop()),
            ],
            done_reply("continuing"),
        ]);
        let run = runner(&provider);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "continue the job", tx)
            .await
            .expect("send");

        assert_eq!(
            reply.continues,
            [ContinuedJob {
                session_id: child_id.clone(),
                parent_session: reply.session_id.clone(),
                message: "also check the linter".to_owned(),
                role: SessionRole::Executor,
                project: "arc".to_owned(),
            }]
        );

        let events = replay_log(dir.path());
        let result = tool_result(&events);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert!(result.content.contains("Continuing"), "{}", result.content);
        assert!(result.content.contains(&child_id), "{}", result.content);
    }

    #[tokio::test]
    async fn continue_job_on_an_unknown_session_is_an_actionable_error_and_the_turn_completes() {
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "continue_job",
                    &continue_job_args("s-ghost", "keep going"),
                )),
                Ok(tool_stop()),
            ],
            done_reply("noted"),
        ]);
        let mut registry = Registry::new(512);
        registry.register(Box::new(crate::tool::builtin::continue_job::ContinueJob));
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "continue it", tx)
            .await
            .expect("a bad continue_job fails the call, not the turn");

        assert!(reply.continues.is_empty());
        let events = replay_log(dir.path());
        let result = tool_result(&events);
        assert_eq!(result.outcome, ToolOutcome::Error as i32);
        assert!(result.content.contains("s-ghost"), "{}", result.content);
    }

    #[tokio::test]
    async fn continue_job_on_a_non_job_session_is_an_actionable_error() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let mut registry = Registry::new(512);
        registry.register(Box::new(crate::tool::builtin::continue_job::ContinueJob));
        // a throwaway provider: create_bound_session never drives it
        let (engine, bootstrap_run) =
            engine_with_tools(&ScriptedProvider::scripted(vec![]), &dir, registry);
        let engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));
        let other_concierge = engine
            .create_bound_session(&bootstrap_run, "arc", SessionRole::Concierge, None)
            .expect("create a non-job session durably");

        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "continue_job",
                    &continue_job_args(&other_concierge, "keep going"),
                )),
                Ok(tool_stop()),
            ],
            done_reply("noted"),
        ]);
        let run = runner(&provider);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "continue it", tx)
            .await
            .expect("a bad continue_job fails the call, not the turn");

        assert!(reply.continues.is_empty());
        let events = replay_log(dir.path());
        let result = tool_result(&events);
        assert_eq!(result.outcome, ToolOutcome::Error as i32);
        assert!(result.content.contains("concierge"), "{}", result.content);
        assert!(result.content.contains("not a job"), "{}", result.content);
    }

    fn unstamped_session(id: &str, role: SessionRole) -> session_event::Event {
        session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
            session_id: id.to_owned(),
            title: String::new(),
            provider: String::new(),
            model: String::new(),
            role: role as i32,
            project: String::new(),
            budget: None,
            grants: Vec::new(),
            dispatched_by: String::new(),
        })
    }

    fn session_recorded_on(
        id: &str,
        role: SessionRole,
        provider: &str,
        model: &str,
    ) -> session_event::Event {
        session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
            session_id: id.to_owned(),
            title: String::new(),
            provider: provider.to_owned(),
            model: model.to_owned(),
            role: role as i32,
            project: String::new(),
            budget: None,
            grants: Vec::new(),
            dispatched_by: String::new(),
        })
    }

    #[tokio::test]
    async fn continue_job_refuses_when_the_recorded_model_no_longer_matches_the_role() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        let mut registry = Registry::new(512);
        registry.register(Box::new(crate::tool::builtin::continue_job::ContinueJob));
        let (engine, bootstrap_run) =
            engine_with_tools(&ScriptedProvider::scripted(vec![]), &dir, registry);
        let engine = engine
            .with_projects(projects_with(
                "arc",
                vec![ToolSource::Builtin],
                vec![Grant::new(&root, Mode::ReadWrite)],
            ))
            .with_role_identities(BTreeMap::from([(
                SessionRole::Executor,
                ("scripted".to_owned(), "model-a".to_owned()),
            )]));
        let child_id = engine
            .create_bound_session(&bootstrap_run, "arc", SessionRole::Executor, None)
            .expect("create the child durably, recorded on model-a");

        // config changed since: the executor role now runs a different model
        let engine = engine.with_role_identities(BTreeMap::from([(
            SessionRole::Executor,
            ("scripted".to_owned(), "model-b".to_owned()),
        )]));

        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "continue_job",
                    &continue_job_args(&child_id, "keep going"),
                )),
                Ok(tool_stop()),
            ],
            done_reply("noted"),
        ]);
        let run = runner(&provider);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "continue it", tx)
            .await
            .expect("a refused continue_job fails the call, not the turn");

        assert!(
            reply.continues.is_empty(),
            "the mismatch refuses the resume"
        );
        let events = replay_log(dir.path());
        let result = tool_result(&events);
        assert_eq!(result.outcome, ToolOutcome::Error as i32);
        assert!(result.content.contains("model-a"), "{}", result.content);
        assert!(result.content.contains("model-b"), "{}", result.content);
        assert!(result.content.contains("executor"), "{}", result.content);
        assert!(result.content.contains("fresh job"), "{}", result.content);
    }

    #[tokio::test]
    async fn send_message_refuses_a_session_recorded_on_a_different_model() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![session_recorded_on(
                "s-01",
                SessionRole::Executor,
                "scripted",
                "model-a",
            )],
        );
        let provider = ScriptedProvider::scripted(vec![]);
        let (engine, _) = reopened_engine(&provider, &dir, Registry::new(512));
        let executor_run = Runner {
            role: SessionRole::Executor,
            provider: Arc::clone(&provider) as Arc<dyn Provider>,
            model: "model-b".to_owned(),
            thinking: Thinking::Default,
            system: None,
        };
        let (tx, _rx) = channel();

        let error = engine
            .send_message(&executor_run, Some("s-01"), "resume", tx)
            .await
            .expect_err("a model mismatch refuses the resume");

        assert!(
            matches!(error, Error::ModelMismatch { .. }),
            "expected ModelMismatch, got {error:?}"
        );
        let message = error.to_string();
        assert!(message.contains("model-a"), "{message}");
        assert!(message.contains("model-b"), "{message}");
        assert!(
            provider.requests().is_empty(),
            "a refused turn never reaches the provider"
        );
    }

    #[tokio::test]
    async fn a_session_recorded_before_model_stamping_stays_resumable() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(&dir, vec![unstamped_session("s-01", SessionRole::Executor)]);
        let provider = ScriptedProvider::scripted(vec![done_reply("hi")]);
        let (engine, _) = reopened_engine(&provider, &dir, Registry::new(512));
        let executor_run = Runner {
            role: SessionRole::Executor,
            provider: Arc::clone(&provider) as Arc<dyn Provider>,
            model: "model-b".to_owned(),
            thinking: Thinking::Default,
            system: None,
        };
        let (tx, _rx) = channel();

        engine
            .send_message(&executor_run, Some("s-01"), "resume", tx)
            .await
            .expect("a session logged before provider/model stamping stays unpinned");
    }

    #[tokio::test]
    async fn send_message_on_a_matching_model_resumes_untouched() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![session_recorded_on(
                "s-01",
                SessionRole::Executor,
                "scripted",
                "model-a",
            )],
        );
        let provider = ScriptedProvider::scripted(vec![done_reply("hi")]);
        let (engine, _) = reopened_engine(&provider, &dir, Registry::new(512));
        let executor_run = Runner {
            role: SessionRole::Executor,
            provider: Arc::clone(&provider) as Arc<dyn Provider>,
            model: "model-a".to_owned(),
            thinking: Thinking::Default,
            system: None,
        };
        let (tx, _rx) = channel();

        engine
            .send_message(&executor_run, Some("s-01"), "resume", tx)
            .await
            .expect("the same recorded identity resumes without a refusal");
    }

    #[tokio::test]
    async fn record_handback_appends_a_system_sourced_message_visible_in_the_parents_log() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hi")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        engine
            .record_handback(&reply.session_id, "child-1", None, "all done")
            .await
            .expect("record_handback");

        let events = replay_events(dir.path());
        let last = events.last().expect("an event was appended");
        assert_eq!(
            last.source,
            Source::System as i32,
            "the daemon wrote the handback, not the user's turn"
        );
        let arc_proto::v1::event::Payload::Session(arc_proto::v1::SessionEvent {
            event: Some(session_event::Event::MessageAppended(handback)),
        }) = last.payload.clone().expect("payload")
        else {
            panic!("expected a MessageAppended, got {:?}", last.payload);
        };
        assert_eq!(handback.session_id, reply.session_id);
        assert_eq!(
            handback.role,
            Role::User as i32,
            "rebuild_transcript only carries User/Assistant rows to the model"
        );
        assert!(!handback.partial);
        assert!(handback.content.contains("child-1"));
        assert!(handback.content.contains("all done"));

        let earlier_turn_id = match &events[1].payload {
            Some(arc_proto::v1::event::Payload::Session(arc_proto::v1::SessionEvent {
                event: Some(session_event::Event::MessageAppended(m)),
            })) => m.turn_id.clone(),
            other => panic!("expected the user's message, got {other:?}"),
        };
        assert!(!handback.turn_id.is_empty());
        assert_ne!(
            handback.turn_id, earlier_turn_id,
            "the handback is its own turn, not part of the conversation turn"
        );

        let entries = engine.transcript(&reply.session_id).expect("transcript");
        match &entries.last().expect("an entry").entry {
            Some(history_entry::Entry::Message(HistoryMessage { role, content, .. })) => {
                assert_eq!(*role, Role::User as i32);
                assert!(content.contains("child-1"));
            }
            other => panic!("expected a message entry, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn record_handback_truncates_a_long_summary_on_a_char_boundary() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hi")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        // two-byte characters straddle the 2 KiB cap, exercising the
        // char-boundary walk-back as well as the cap itself
        let long_summary = "é".repeat(2000);
        engine
            .record_handback(&reply.session_id, "child-1", None, &long_summary)
            .await
            .expect("record_handback");

        let entries = engine.transcript(&reply.session_id).expect("transcript");
        let content = match &entries.last().expect("an entry").entry {
            Some(history_entry::Entry::Message(HistoryMessage { content, .. })) => content.clone(),
            other => panic!("expected a message entry, got {other:?}"),
        };
        assert!(content.contains(" [truncated]\n"), "{content}");
        assert!(
            content.ends_with("a new dispatch starts from nothing."),
            "the continue_job affordance stays the closing line: {content}"
        );
        assert!(content.len() < long_summary.len());
    }

    #[tokio::test]
    async fn record_handback_for_an_analyze_child_warns_that_continuing_stays_read_only() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let provider = ScriptedProvider::scripted(vec![]);
        let (mut engine, run) = engine_with_tools(&provider, &dir, Registry::new(512));
        engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));
        let parent_id = engine
            .create_bound_session(&run, "arc", SessionRole::Concierge, None)
            .expect("create the parent");
        let child_id = engine
            .create_bound_session_with_intent(
                &run,
                "arc",
                SessionRole::Executor,
                None,
                Intent::Analyze,
                Some(&parent_id),
                Source::Model,
            )
            .expect("create an analyze child");

        engine
            .record_handback(&parent_id, &child_id, None, "found the bug")
            .await
            .expect("record_handback");

        let entries = engine.transcript(&parent_id).expect("transcript");
        let content = match &entries.last().expect("an entry").entry {
            Some(history_entry::Entry::Message(HistoryMessage { content, .. })) => content.clone(),
            other => panic!("expected a message entry, got {other:?}"),
        };
        assert_eq!(
            content,
            format!(
                "Job {child_id} finished.\nfound the bug\nFor follow-ups about anything this \
                 job read, continue_job {child_id} keeps its context but stays read-only — a \
                 change needs a fresh implement dispatch; a new dispatch starts from nothing."
            )
        );
    }

    #[tokio::test]
    async fn record_handback_for_an_implement_child_keeps_the_plain_tail() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let provider = ScriptedProvider::scripted(vec![]);
        let (mut engine, run) = engine_with_tools(&provider, &dir, Registry::new(512));
        engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));
        let parent_id = engine
            .create_bound_session(&run, "arc", SessionRole::Concierge, None)
            .expect("create the parent");
        let child_id = engine
            .create_bound_session(&run, "arc", SessionRole::Executor, None)
            .expect("create an implement child");

        engine
            .record_handback(&parent_id, &child_id, None, "fixed it")
            .await
            .expect("record_handback");

        let entries = engine.transcript(&parent_id).expect("transcript");
        let content = match &entries.last().expect("an entry").entry {
            Some(history_entry::Entry::Message(HistoryMessage { content, .. })) => content.clone(),
            other => panic!("expected a message entry, got {other:?}"),
        };
        assert_eq!(
            content,
            format!(
                "Job {child_id} finished.\nfixed it\nFor follow-ups about anything this job \
                 read or did, continue_job {child_id} keeps its context; a new dispatch starts \
                 from nothing."
            )
        );
    }

    #[tokio::test]
    async fn record_handback_for_a_grantless_child_keeps_the_plain_tail() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hi")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        // "child-2" was never created durably: its session_grants is empty,
        // the same shape a non-workspace job leaves behind
        engine
            .record_handback(&reply.session_id, "child-2", None, "no workspace here")
            .await
            .expect("record_handback");

        let entries = engine.transcript(&reply.session_id).expect("transcript");
        let content = match &entries.last().expect("an entry").entry {
            Some(history_entry::Entry::Message(HistoryMessage { content, .. })) => content.clone(),
            other => panic!("expected a message entry, got {other:?}"),
        };
        assert_eq!(
            content,
            "Job child-2 finished.\nno workspace here\nFor follow-ups about anything this job \
             read or did, continue_job child-2 keeps its context; a new dispatch starts from \
             nothing."
        );
    }

    async fn wait_for_event_count(dir: &std::path::Path, want: usize) {
        for _ in 0..400 {
            if replay_log(dir).len() >= want {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for {want} events in {}", dir.display());
    }

    #[tokio::test]
    async fn record_handback_waits_for_the_parents_turn_guard() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(&dir, vec![seeded_session()]);

        let notify = Arc::new(tokio::sync::Notify::new());
        let provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("working".to_owned()))],
            notify: Arc::clone(&notify),
            after: vec![Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            })],
        }]);
        let (engine, run) = reopened_engine(&provider, &dir, Registry::new(512));
        let engine = Arc::new(engine);

        let turn_engine = Arc::clone(&engine);
        let turn_run = run.clone();
        let turn = tokio::spawn(async move {
            let (tx, _rx) = channel();
            turn_engine
                .send_message(&turn_run, Some("s-01"), "go", tx)
                .await
                .expect("send")
        });

        // the user message lands before the provider stalls on the gate:
        // waiting for it proves the turn is genuinely in flight, holding the
        // guard, when record_handback is asked to run below
        wait_for_event_count(dir.path(), 2).await;

        let handback_engine = Arc::clone(&engine);
        let handback = tokio::spawn(async move {
            handback_engine
                .record_handback("s-01", "child-1", None, "the child's report")
                .await
                .expect("record_handback");
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            replay_log(dir.path()).len(),
            2,
            "the handback stays blocked while the parent's turn guard is held"
        );

        notify.notify_one();
        turn.await.expect("turn task");
        handback.await.expect("handback task");

        let events = replay_log(dir.path());
        assert_eq!(events.len(), 4, "the turn's events, then the handback");
        let turn_id = appended(&events[1]).turn_id.clone();
        assert_eq!(
            appended(&events[2]).turn_id,
            turn_id,
            "the gated turn's own reply"
        );
        let handback_msg = appended(&events[3]);
        assert_ne!(
            handback_msg.turn_id, turn_id,
            "the handback landed after the turn released the guard, in its own turn"
        );
        assert!(handback_msg.content.contains("child-1"));
    }

    #[tokio::test]
    async fn continue_session_runs_a_scripted_turn_over_the_existing_transcript_without_a_user_message()
     {
        let provider = ScriptedProvider::scripted(vec![
            done_reply("first"),
            done_reply("the concierge reacts"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let (tx, mut rx) = channel();
        let continued = engine
            .continue_session(&run, &reply.session_id, tx)
            .await
            .expect("continue_session");

        assert_eq!(continued.session_id, reply.session_id);
        assert!(!continued.partial);

        let events = replay_log(dir.path());
        assert_eq!(
            events.len(),
            4,
            "SessionCreated, the user message, the first reply, then only the continued reply"
        );
        let last = appended(&events[3]);
        assert_eq!(last.role, Role::Assistant as i32);
        assert_eq!(last.content, "the concierge reacts");

        assert_eq!(
            ignoring_elapsed(engine.transcript(&reply.session_id).expect("transcript")),
            [
                prose_entry(Role::User as i32, "hi", false),
                assistant_entry_with_usage("first", 3, 5),
                assistant_entry_with_usage("the concierge reacts", 3, 5),
            ],
            "no user message was appended for the handback turn"
        );

        assert_eq!(
            drain(&mut rx),
            [
                EngineEvent::Accepted {
                    session_id: reply.session_id.clone()
                },
                EngineEvent::Delta("the concierge reacts".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn continue_session_over_a_transcript_with_no_messages_still_runs_a_turn() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(&dir, vec![seeded_session()]);
        let provider =
            ScriptedProvider::scripted(vec![done_reply("nothing to react to, but here I am")]);
        let (engine, run) = reopened_engine(&provider, &dir, Registry::new(512));
        let (tx, _rx) = channel();

        let reply = engine
            .continue_session(&run, "s-01", tx)
            .await
            .expect("continue_session");

        assert!(!reply.partial);
        let events = replay_log(dir.path());
        assert_eq!(
            events.len(),
            2,
            "SessionCreated, then only the assistant reply"
        );
        assert_eq!(
            appended(&events[1]).content,
            "nothing to react to, but here I am"
        );
        let requests = provider.requests();
        assert!(
            requests[0].messages.is_empty(),
            "no user message and no history: the model saw an empty transcript"
        );
    }

    #[tokio::test]
    async fn continue_session_on_a_session_pinned_to_another_role_refuses_and_appends_nothing() {
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
                    dispatched_by: String::new(),
                }),
                seeded_message(Role::User, "earlier"),
            ],
        );
        let provider = ScriptedProvider::scripted(vec![done_reply("never sent")]);
        let (engine, run) = reopened_engine(&provider, &dir, Registry::new(512));
        let (tx, _rx) = channel();

        let err = engine
            .continue_session(&run, "s-01", tx)
            .await
            .expect_err("a concierge engine must refuse an executor session");

        assert!(matches!(err, Error::RoleMismatch { .. }), "got: {err:?}");
        assert_eq!(
            replay_log(dir.path()).len(),
            2,
            "the refusal appended nothing"
        );
        assert!(provider.requests().is_empty(), "the provider never ran");
    }

    #[tokio::test]
    async fn continue_session_waits_for_a_pending_user_turns_guard_on_the_same_session() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(&dir, vec![seeded_session()]);

        let notify = Arc::new(tokio::sync::Notify::new());
        let provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("working".to_owned()))],
                notify: Arc::clone(&notify),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Immediate(done_reply("the concierge reacts")),
        ]);
        let (engine, run) = reopened_engine(&provider, &dir, Registry::new(512));
        let engine = Arc::new(engine);

        let turn_engine = Arc::clone(&engine);
        let turn_run = run.clone();
        let turn = tokio::spawn(async move {
            let (tx, _rx) = channel();
            turn_engine
                .send_message(&turn_run, Some("s-01"), "go", tx)
                .await
                .expect("send")
        });

        // the user message lands before the provider stalls on the gate:
        // waiting for it proves the user turn genuinely holds the guard
        // when continue_session is asked to run below
        wait_for_event_count(dir.path(), 2).await;

        let continue_engine = Arc::clone(&engine);
        let continue_run = run.clone();
        let continue_handle = tokio::spawn(async move {
            let (tx, _rx) = channel();
            continue_engine
                .continue_session(&continue_run, "s-01", tx)
                .await
                .expect("continue_session")
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            replay_log(dir.path()).len(),
            2,
            "continue_session stays blocked while the user turn holds the guard"
        );

        notify.notify_one();
        let sent = turn.await.expect("turn task");
        let continued = continue_handle.await.expect("continue_session task");

        let events = replay_log(dir.path());
        assert_eq!(
            events.len(),
            4,
            "the user turn's events, then the handback turn's reply"
        );
        let turn_id = appended(&events[1]).turn_id.clone();
        assert_eq!(
            appended(&events[2]).turn_id,
            turn_id,
            "the gated turn's own reply"
        );
        let continued_msg = appended(&events[3]);
        assert_ne!(
            continued_msg.turn_id, turn_id,
            "continue_session ran in its own turn, after the guard released"
        );
        assert_eq!(continued_msg.content, "the concierge reacts");
        assert_eq!(sent.session_id, "s-01");
        assert_eq!(continued.session_id, "s-01");
    }

    #[tokio::test]
    async fn last_assistant_message_returns_the_most_recent_non_empty_reply() {
        let provider = ScriptedProvider::scripted(vec![done_reply("first"), done_reply("second")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "one", tx)
            .await
            .expect("send");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "two", tx)
            .await
            .expect("send");

        assert_eq!(
            engine
                .last_assistant_message(&reply.session_id)
                .expect("last"),
            Some("second".to_owned())
        );
    }

    #[tokio::test]
    async fn last_assistant_message_is_none_for_a_session_with_no_assistant_text() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, _run) = engine(&provider, &dir);

        assert_eq!(
            engine.last_assistant_message("s-ghost").expect("last"),
            None
        );
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
                            intent: Intent::Implement,
                        }),
                        continue_request: None,
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

        let reply = engine
            .send_message(&run, None, "start a job", tx)
            .await
            .expect("a bad dispatch fails the call, not the turn");

        let events = replay_log(dir.path());
        assert_eq!(events.len(), 5, "no child session was created");
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Error as i32);
        assert!(result.content.contains("ghost"), "{}", result.content);
        assert!(reply.jobs.is_empty(), "a failed dispatch spawns no job");
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
            ignoring_elapsed(
                engine
                    .transcript(&fast_reply.session_id)
                    .expect("transcript")
            ),
            [
                prose_entry(Role::User as i32, "fast please", false),
                assistant_entry_with_usage("fast done", 3, 5),
            ]
        );
        assert_eq!(
            ignoring_elapsed(
                engine
                    .transcript(&slow_reply.session_id)
                    .expect("transcript")
            ),
            [
                prose_entry(Role::User as i32, "slow please", false),
                assistant_entry_with_usage("slow startslow end", 3, 5),
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
            ignoring_elapsed(engine.transcript(&session_id).expect("transcript")),
            [
                prose_entry(Role::User as i32, "one", false),
                assistant_entry_with_usage("first startfirst end", 3, 5),
                prose_entry(Role::User as i32, "two", false),
                assistant_entry_with_usage("second reply", 3, 5),
            ],
            "the two turns land whole, in order, with nothing interleaved"
        );
    }
}
