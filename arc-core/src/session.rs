use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

use arc_proto::v1::{
    BranchMarked, Budget, ImageAttachment, MemoryEvent, MessageAppended,
    ModelChoice as WireModelChoice, ModelList, Notification, ReviewChanged, Role, RoleEvent,
    RoleModelSelected, ServerCallRecorded, SessionAppended, SessionCompacted, SessionCreated,
    SessionEvent, SessionRole, Source, ToolCallIssued, ToolOutcome, ToolResultRecorded,
    WorkspaceGrant, branch_marked,
};
use arc_proto::v1::{event, memory_event, notification, role_event, session_event};
use futures::StreamExt as _;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::memory::render_memory_index;
use crate::projection::{self, MessageRow, SessionSummary};
use crate::provider::{
    self, CompletionDelta, CompletionRequest, Message, Provider, Stop, Thinking, ToolCall, Usage,
};
use crate::store::{self, Store, now_ts};
use crate::tool::workspace::{Grant, Grants, Mode};
use crate::tool::{ContinueRequest, DispatchOutcome, Intent, Registry, ToolSource, TurnContext};

const MAX_EXECUTOR_TOOL_STEPS: usize = 256;

fn max_tool_steps(role: SessionRole) -> usize {
    match role {
        SessionRole::Chat | SessionRole::Executor => MAX_EXECUTOR_TOOL_STEPS,
        _ => 8,
    }
}

fn elapsed_ms_since(start: std::time::Instant) -> u32 {
    u32::try_from(start.elapsed().as_millis()).unwrap_or(u32::MAX)
}

#[derive(Debug, Clone, Default)]
pub struct ProjectSpec {
    pub sources: Vec<ToolSource>,
    pub grants: Vec<Grant>,
    pub command_prefix: Vec<String>,
    pub description: String,
}

#[derive(Clone, Debug)]
pub struct Runner {
    pub role: SessionRole,
    pub provider: Arc<dyn Provider>,
    pub model: String,
    pub thinking: Thinking,
    pub system: Option<String>,
    pub compact_at: Option<u32>,
    pub context_window: Option<u32>,
    pub editing: crate::tool::Editing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelChoice {
    pub name: String,
    pub provider: String,
    pub model: String,
    pub thinking: Thinking,
    pub editing: crate::tool::Editing,
}

pub struct Engine {
    store: StdMutex<Store>,
    registry: Registry,
    projects: BTreeMap<String, ProjectSpec>,
    role_choices: BTreeMap<SessionRole, Vec<ModelChoice>>,
    compaction_runners: Vec<(String, Runner)>,
    turns: StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    live_turns: StdMutex<HashMap<String, LiveTurn>>,
    notifier: Option<broadcast::Sender<Notification>>,
    completed_turns: broadcast::Sender<String>,
}

#[derive(Debug, Clone)]
pub struct Inbound {
    pub content: String,
    pub source: Source,
    pub attachments: Vec<ImageAttachment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    JobsReady {
        jobs: Vec<DispatchedJob>,
        continues: Vec<ContinuedJob>,
        cancels: Vec<String>,
    },
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
        content: String,
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
    pub cancels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedJob {
    pub session_id: String,
    pub parent_session: String,
    pub role: SessionRole,
    pub project: String,
    pub brief: String,
    pub budget: Option<Budget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinuedJob {
    pub session_id: String,
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

    #[error("image attachment: {0}")]
    Attachment(#[from] crate::attachment::Error),

    #[error("the {provider} provider does not support picture attachments")]
    AttachmentsUnsupported { provider: String },

    #[error("no runner is configured for the {role} role")]
    NoRunner { role: String },

    #[error("session {session_id} is pinned to the {pinned} role; this engine serves {serving}")]
    RoleMismatch {
        session_id: String,
        pinned: String,
        serving: String,
    },

    #[error(
        "session {session_id} was recorded on {pinned}; the {role} turn tried {serving}. \
         Use its recorded model, or fork to switch."
    )]
    ModelMismatch {
        session_id: String,
        pinned: String,
        role: String,
        serving: String,
    },

    #[error("the model produced no reply")]
    EmptyReply,

    #[error("thinking update position exceeds transcript length")]
    InvalidThinkingPosition,

    #[error("compaction failed: {reason}; no summary was saved")]
    CompactionFailed { reason: String },

    #[error("the turn was cancelled before it produced anything durable")]
    Cancelled,

    #[error("project {project} is not configured")]
    UnknownProject { project: String },

    #[error("`{choice}` is not one of the {role} role's model choices")]
    UnknownChoice { role: String, choice: String },

    #[error("session {session_id} needs its recorded model choice {choice}; restore it or fork")]
    MissingChoice { session_id: String, choice: String },

    #[error("session {session_id} has ambiguous old model {provider}/{model}; fork it")]
    AmbiguousChoice {
        session_id: String,
        provider: String,
        model: String,
    },

    #[error("project {project}: could not resolve its granted roots: {source}")]
    Grants {
        project: String,
        #[source]
        source: std::io::Error,
    },

    #[error("session {session_id} does not exist; nothing to fork")]
    UnknownSession { session_id: String },

    #[error("unsupported thinking level {thinking} for {provider}/{model}")]
    UnsupportedThinking {
        thinking: String,
        provider: String,
        model: String,
    },

    #[error("session {session_id} has an active turn")]
    ThinkingWhileBusy { session_id: String },

    #[error("session {session_id} uses the retired code role; start a new assistant session")]
    HistoricalSession { session_id: String },

    #[error(
        "fork_point {fork_point} is not a message in session {session_id}; fork from a \
         message, not a tool call or another session's sequence"
    )]
    InvalidForkPoint { session_id: String, fork_point: u64 },

    #[error("session {session_id} is a root conversation; only a fork has a disposition to mark")]
    NotABranch { session_id: String },
}

impl Engine {
    pub fn new(store: Store, registry: Registry) -> Self {
        Self {
            store: StdMutex::new(store),
            registry,
            projects: BTreeMap::new(),
            role_choices: BTreeMap::new(),
            compaction_runners: Vec::new(),
            turns: StdMutex::new(HashMap::new()),
            live_turns: StdMutex::new(HashMap::new()),
            notifier: None,
            completed_turns: broadcast::channel(256).0,
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

    pub fn review_accept(&self, record_id: &str) -> Result<(), Error> {
        self.with_store_mut(|store| store.review_accept(record_id))?;
        self.notify_review_changed()
    }

    pub fn review_delete(&self, record_id: &str) -> Result<(), Error> {
        self.with_store_mut(|store| store.review_delete(record_id))?;
        self.notify_review_changed()
    }

    pub fn completed_turns(&self) -> broadcast::Receiver<String> {
        self.completed_turns.subscribe()
    }

    pub(crate) fn commit_title(
        &self,
        snapshot: &crate::store::SessionSnapshot,
        title: &str,
    ) -> Result<bool, store::Error> {
        let committed = self.with_store_mut(|store| store.commit_title(snapshot, title))?;
        if committed {
            self.notify_appended(snapshot.session_id.clone());
        }
        Ok(committed)
    }

    pub fn cancel_turn(&self, session_id: &str) -> bool {
        let live = self.live_turns.lock().expect("live turns lock poisoned");
        match live.get(session_id) {
            Some(turn) => {
                let _ = turn.cancel.send(true);
                true
            }
            None => false,
        }
    }

    pub fn turn_is_live(&self, session_id: &str) -> bool {
        self.live_turns
            .lock()
            .expect("live turns lock poisoned")
            .contains_key(session_id)
    }

    pub fn queue_message(&self, session_id: &str, content: &str, source: Source) -> bool {
        self.queue_message_with_attachments(session_id, content, source, Vec::new())
    }

    pub fn queue_message_with_attachments(
        &self,
        session_id: &str,
        content: &str,
        source: Source,
        attachments: Vec<ImageAttachment>,
    ) -> bool {
        let live = self.live_turns.lock().expect("live turns lock poisoned");
        match live.get(session_id) {
            Some(turn) => turn
                .inbox
                .send(Inbound {
                    content: content.to_owned(),
                    source,
                    attachments,
                })
                .is_ok(),
            None => false,
        }
    }

    pub(crate) fn turn_guard(&self, session_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut turns = self.turns.lock().expect("turns lock poisoned");
        Arc::clone(
            turns
                .entry(session_id.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    #[must_use]
    pub fn with_projects(mut self, projects: BTreeMap<String, ProjectSpec>) -> Self {
        self.projects = projects;
        self
    }

    #[must_use]
    pub fn with_role_choices(
        mut self,
        role_choices: BTreeMap<SessionRole, Vec<ModelChoice>>,
    ) -> Self {
        self.role_choices = role_choices;
        self
    }

    #[must_use]
    pub fn with_compaction_runners(mut self, runners: Vec<(String, Runner)>) -> Self {
        self.compaction_runners = runners;
        self
    }

    #[must_use]
    pub fn with_role_identities(
        self,
        role_identities: BTreeMap<SessionRole, (String, String)>,
    ) -> Self {
        self.with_role_choices(
            role_identities
                .into_iter()
                .map(|(role, (provider, model))| {
                    (
                        role,
                        vec![ModelChoice {
                            name: model.clone(),
                            editing: crate::tool::Editing::for_provider(&provider),
                            provider,
                            model,
                            thinking: Thinking::Default,
                        }],
                    )
                })
                .collect(),
        )
    }

    pub fn selected_choice(&self, role: SessionRole) -> Result<Option<String>, Error> {
        Ok(self.current_choice(role)?.map(|choice| choice.name))
    }

    fn current_choice(&self, role: SessionRole) -> Result<Option<ModelChoice>, Error> {
        let Some(choices) = self.role_choices.get(&role) else {
            return Ok(None);
        };
        let selected = self.with_store(|store| store.projection().role_selection(role))?;
        Ok(selected
            .and_then(|name| choices.iter().find(|choice| choice.name == name))
            .or_else(|| choices.first())
            .cloned())
    }

    fn choice_for(&self, role: SessionRole, choice: &str) -> Result<Option<ModelChoice>, Error> {
        if choice.is_empty() {
            return self.current_choice(role);
        }
        self.role_choices
            .get(&role)
            .and_then(|menu| menu.iter().find(|entry| entry.name == choice))
            .cloned()
            .map(Some)
            .ok_or_else(|| Error::UnknownChoice {
                role: provider::role_label(role).to_owned(),
                choice: choice.to_owned(),
            })
    }

    pub fn model_list(&self) -> Result<ModelList, Error> {
        let mut choices = Vec::new();
        for (role, menu) in &self.role_choices {
            let selected = self.selected_choice(*role)?;
            for choice in menu {
                choices.push(WireModelChoice {
                    role: *role as i32,
                    name: choice.name.clone(),
                    provider: choice.provider.clone(),
                    model: choice.model.clone(),
                    thinking: choice.thinking.label().to_owned(),
                    selected: selected.as_deref() == Some(choice.name.as_str()),
                });
            }
        }
        Ok(ModelList { choices })
    }

    #[tracing::instrument(name = "engine.select_model", skip_all, fields(role = provider::role_label(role), choice))]
    pub fn select_model(&self, role: SessionRole, choice: &str) -> Result<(), Error> {
        let on_menu = self
            .role_choices
            .get(&role)
            .is_some_and(|menu| menu.iter().any(|entry| entry.name == choice));
        if !on_menu {
            return Err(Error::UnknownChoice {
                role: provider::role_label(role).to_owned(),
                choice: choice.to_owned(),
            });
        }
        let payload = event::Payload::Role(RoleEvent {
            event: Some(role_event::Event::ModelSelected(RoleModelSelected {
                role: role as i32,
                choice: choice.to_owned(),
            })),
        });
        self.with_store_mut(|store| store.append(Source::User, Some(now_ts()), payload))?;
        if let Some(notifier) = &self.notifier {
            let _ = notifier.send(Notification {
                event: Some(notification::Event::ModelsChanged(self.model_list()?)),
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn with_notifier(mut self, notifier: broadcast::Sender<Notification>) -> Self {
        self.notifier = Some(notifier);
        self
    }

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
            "",
        )
    }

    pub fn create_session(&self, runner: &Runner) -> Result<String, Error> {
        self.create_session_with_choice(runner, "")
    }

    pub fn create_session_with_choice(
        &self,
        runner: &Runner,
        choice: &str,
    ) -> Result<String, Error> {
        self.create_session_with_directory(runner, choice, "")
    }

    pub fn create_session_with_directory(
        &self,
        runner: &Runner,
        choice: &str,
        directory: &str,
    ) -> Result<String, Error> {
        let selected = self.choice_for(runner.role, choice)?;
        let session_id = uuid::Uuid::new_v4().to_string();
        let editing = selected
            .as_ref()
            .map_or(runner.editing, |pick| pick.editing);
        let directory = if directory.is_empty() {
            String::new()
        } else {
            std::fs::canonicalize(directory)
                .map_err(|source| Error::Grants {
                    project: directory.to_owned(),
                    source,
                })?
                .to_string_lossy()
                .into_owned()
        };
        self.record_unbound_session(&session_id, runner, selected.as_ref(), editing, directory)?;
        Ok(session_id)
    }

    pub fn session_thinking(
        &self,
        session_id: &str,
        fallback: Thinking,
    ) -> Result<Thinking, Error> {
        let (value, known) = self.with_store(|store| {
            let projection = store.projection();
            Ok::<_, Error>((
                projection.session_thinking(session_id)?,
                projection.session_identity(session_id)?.is_some(),
            ))
        })?;
        match value {
            None if known => Ok(fallback),
            None => Err(Error::UnknownSession {
                session_id: session_id.to_owned(),
            }),
            Some(label) => Ok(Thinking::from_label(&label).unwrap_or(fallback)),
        }
    }

    pub fn set_session_thinking(&self, session_id: &str, label: &str) -> Result<(), Error> {
        let thinking = Thinking::from_label(label).ok_or_else(|| Error::UnsupportedThinking {
            thinking: label.to_owned(),
            provider: String::new(),
            model: String::new(),
        })?;
        let (provider, model) = self
            .with_store(|store| {
                let projection = store.projection();
                let identity = projection.session_identity(session_id)?;
                Ok::<_, Error>(identity)
            })?
            .ok_or_else(|| Error::UnknownSession {
                session_id: session_id.to_owned(),
            })?;
        if !provider::supported_thinking(&provider, &model).contains(&thinking) {
            return Err(Error::UnsupportedThinking {
                thinking: label.to_owned(),
                provider,
                model,
            });
        }
        let guard = self.turn_guard(session_id);
        let _turn = guard.try_lock().map_err(|_| Error::ThinkingWhileBusy {
            session_id: session_id.to_owned(),
        })?;
        self.record(
            Source::User,
            session_event::Event::SessionThinkingSelected(arc_proto::v1::SessionThinkingSelected {
                session_id: session_id.to_owned(),
                thinking: label.to_owned(),
            }),
        )?;
        Ok(())
    }

    fn record_unbound_session(
        &self,
        session_id: &str,
        runner: &Runner,
        selected: Option<&ModelChoice>,
        editing: crate::tool::Editing,
        working_directory: String,
    ) -> Result<(), Error> {
        self.record(
            Source::User,
            session_event::Event::SessionCreated(SessionCreated {
                session_id: session_id.to_owned(),
                provider: selected.map_or_else(
                    || runner.provider.name().to_owned(),
                    |pick| pick.provider.clone(),
                ),
                model: selected.map_or_else(|| runner.model.clone(), |pick| pick.model.clone()),
                role: runner.role as i32,
                editing: editing.as_str().to_owned(),
                choice: selected.map_or_else(String::new, |pick| pick.name.clone()),
                working_directory,
                thinking: selected
                    .as_ref()
                    .map_or(runner.thinking, |pick| pick.thinking)
                    .label()
                    .to_owned(),
                ..Default::default()
            }),
        )?;
        Ok(())
    }

    pub fn create_direct_session(
        &self,
        runner: &Runner,
        project: &str,
        role: SessionRole,
    ) -> Result<String, Error> {
        self.create_direct_session_with_choice(runner, project, role, "")
    }

    pub fn create_direct_session_with_choice(
        &self,
        runner: &Runner,
        project: &str,
        role: SessionRole,
        choice: &str,
    ) -> Result<String, Error> {
        self.create_bound_session_with_intent(
            runner,
            project,
            role,
            None,
            Intent::Implement,
            None,
            Source::User,
            choice,
        )
    }

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
        _intent: Intent,
        dispatched_by: Option<&str>,
        source: Source,
        choice: &str,
    ) -> Result<String, Error> {
        let spec = self
            .projects
            .get(project)
            .ok_or_else(|| Error::UnknownProject {
                project: project.to_owned(),
            })?;
        let grants = Grants::new(spec.grants.clone()).map_err(|source| Error::Grants {
            project: project.to_owned(),
            source,
        })?;

        let session_id = uuid::Uuid::new_v4().to_string();
        tracing::Span::current().record("session_id", session_id.as_str());
        let selected = self.choice_for(role, choice)?;
        let (provider, model) = selected.as_ref().map_or_else(
            || (runner.provider.name().to_owned(), runner.model.clone()),
            |pick| (pick.provider.clone(), pick.model.clone()),
        );
        let thinking = selected
            .as_ref()
            .map_or(runner.thinking, |pick| pick.thinking);
        self.record(
            source,
            session_event::Event::SessionCreated(SessionCreated {
                session_id: session_id.clone(),
                parent_session: String::new(),
                fork_point: 0,
                title: String::new(),
                provider,
                model,
                role: role as i32,
                project: project.to_owned(),
                budget,
                grants: Vec::new(),
                working_directory: grants
                    .project_root()
                    .map_or_else(String::new, |root| root.to_string_lossy().into_owned()),
                dispatched_by: dispatched_by.unwrap_or_default().to_owned(),
                editing: selected
                    .as_ref()
                    .map_or(runner.editing, |pick| pick.editing)
                    .as_str()
                    .to_owned(),
                choice: selected.map_or_else(String::new, |pick| pick.name),
                thinking: thinking.label().to_owned(),
            }),
        )?;
        Ok(session_id)
    }

    pub fn fork_session(&self, parent_id: &str, fork_point: u64) -> Result<String, Error> {
        self.fork_session_with_choice(parent_id, fork_point, "")
    }

    pub fn fork_session_with_choice(
        &self,
        parent_id: &str,
        fork_point: u64,
        choice: &str,
    ) -> Result<String, Error> {
        let (
            parent_id,
            role,
            identity,
            recorded_editing,
            project,
            grants,
            working_directory,
            inherited_thinking,
        ) = self.with_store(|store| -> Result<_, Error> {
            let projection = store.projection();
            let role =
                projection
                    .session_role(parent_id)?
                    .ok_or_else(|| Error::UnknownSession {
                        session_id: parent_id.to_owned(),
                    })?;
            if role == provider::LEGACY_DIRECT_ROLE {
                return Err(Error::HistoricalSession {
                    session_id: parent_id.to_owned(),
                });
            }
            let owner = projection.message_owner(fork_point)?;
            let owner_id = match owner {
                Some((owner_id, projection::KIND_MESSAGE))
                    if owner_id == parent_id
                        || projection.ancestors(parent_id)?.contains(&owner_id) =>
                {
                    owner_id
                }
                _ => {
                    return Err(Error::InvalidForkPoint {
                        session_id: parent_id.to_owned(),
                        fork_point,
                    });
                }
            };
            let role = if owner_id == parent_id {
                role
            } else {
                projection
                    .session_role(&owner_id)?
                    .ok_or_else(|| Error::UnknownSession {
                        session_id: owner_id.clone(),
                    })?
            };
            let identity = projection.session_identity(&owner_id)?;
            let editing = projection.session_editing(&owner_id)?;
            let project = projection.session_project(&owner_id)?.unwrap_or_default();
            let grants = projection.session_grants(&owner_id)?;
            let working_directory = projection
                .session_working_directory(&owner_id)?
                .unwrap_or_default();
            let inherited_thinking = projection.session_thinking_at(&owner_id, fork_point)?;
            Ok((
                owner_id,
                role,
                identity,
                editing,
                project,
                grants,
                working_directory,
                inherited_thinking,
            ))
        })?;
        if role == provider::LEGACY_DIRECT_ROLE {
            return Err(Error::HistoricalSession {
                session_id: parent_id,
            });
        }
        let selected = self.choice_for(
            SessionRole::try_from(role).unwrap_or(SessionRole::Unspecified),
            choice,
        )?;
        let (provider, model) = match &selected {
            Some(pick) => (pick.provider.clone(), pick.model.clone()),
            None => identity.clone().unwrap_or_default(),
        };
        let editing = match &selected {
            Some(pick) => pick.editing.as_str().to_owned(),
            None => recorded_editing.unwrap_or_else(|| {
                crate::tool::Editing::for_provider(&provider)
                    .as_str()
                    .to_owned()
            }),
        };
        let thinking = match &selected {
            Some(pick)
                if choice.is_empty()
                    && identity
                        .as_ref()
                        .is_some_and(|(p, m)| p == &pick.provider && m == &pick.model) =>
            {
                inherited_thinking
                    .and_then(|label| Thinking::from_label(&label))
                    .unwrap_or(pick.thinking)
            }
            Some(pick) => pick.thinking,
            None => inherited_thinking
                .and_then(|label| Thinking::from_label(&label))
                .unwrap_or(Thinking::Default),
        };

        let session_id = uuid::Uuid::new_v4().to_string();
        self.record(
            Source::User,
            session_event::Event::SessionCreated(SessionCreated {
                session_id: session_id.clone(),
                parent_session: parent_id,
                fork_point,
                title: String::new(),
                provider,
                model,
                role,
                project,
                budget: None,
                grants: grants
                    .into_iter()
                    .map(|(root, read_write)| WorkspaceGrant { root, read_write })
                    .collect(),
                working_directory,
                dispatched_by: String::new(),
                editing,
                choice: selected.map_or_else(String::new, |pick| pick.name),
                thinking: thinking.label().to_owned(),
            }),
        )?;
        Ok(session_id)
    }

    pub fn mark_branch(
        &self,
        session_id: &str,
        disposition: branch_marked::Disposition,
    ) -> Result<(), Error> {
        self.with_store(|store| store.projection().session_role(session_id))?
            .ok_or_else(|| Error::UnknownSession {
                session_id: session_id.to_owned(),
            })?;
        let ancestors = self.with_store(|store| store.projection().ancestors(session_id))?;
        if ancestors.is_empty() {
            return Err(Error::NotABranch {
                session_id: session_id.to_owned(),
            });
        }
        self.record(
            Source::User,
            session_event::Event::BranchMarked(BranchMarked {
                session_id: session_id.to_owned(),
                disposition: disposition as i32,
            }),
        )?;
        Ok(())
    }

    fn dispatch_job(
        &self,
        runner: &Runner,
        parent_session: &str,
        job_request: crate::tool::JobRequest,
    ) -> (ToolOutcome, String, Option<DispatchedJob>) {
        let role = job_request.role;
        let brief = job_request.brief;
        let budget = job_request.budget;
        let intent = job_request.intent;
        let project = if job_request.project == "none" {
            match self.session_project(parent_session) {
                Ok(Some(name)) if !name.is_empty() => name,
                _ => {
                    return (
                        ToolOutcome::Error,
                        format!(
                            "ERROR: this session has no project of its own. Name one of \
                             the configured projects instead: {}.",
                            self.projects.keys().cloned().collect::<Vec<_>>().join(", ")
                        ),
                        None,
                    );
                }
            }
        } else {
            job_request.project
        };
        match self.create_bound_session_with_intent(
            runner,
            &project,
            role,
            budget,
            intent,
            Some(parent_session),
            Source::Model,
            "",
        ) {
            Ok(child_id) => (
                ToolOutcome::Ok,
                format!(
                    "Dispatched {} into {project} as session {child_id} ({}). \
                     Its result will arrive automatically as a handback.",
                    provider::role_label(role),
                    match intent {
                        Intent::Analyze => "analyze",
                        Intent::Implement => "implement",
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

    fn continue_job(
        &self,
        _runner: &Runner,
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
        let recorded_choice = match self.session_choice(&request.session_id) {
            Ok(choice) => choice,
            Err(error) => return (ToolOutcome::Error, format!("ERROR: {error}"), None),
        };
        if let Some(choice) = recorded_choice {
            let selected = self
                .role_choices
                .get(&role)
                .and_then(|menu| menu.iter().find(|entry| entry.name == choice));
            if let Some(selected) = selected {
                if let Ok(Some((provider, model))) = self.session_identity(&request.session_id) {
                    if (provider.as_str(), model.as_str())
                        != (selected.provider.as_str(), selected.model.as_str())
                    {
                        let error = Error::ModelMismatch {
                            session_id: request.session_id,
                            pinned: identity_label(&provider, &model),
                            role: provider::role_label(role).to_owned(),
                            serving: identity_label(&selected.provider, &selected.model),
                        };
                        return (ToolOutcome::Error, format!("ERROR: {error}"), None);
                    }
                }
            } else {
                let error = Error::MissingChoice {
                    session_id: request.session_id,
                    choice,
                };
                return (ToolOutcome::Error, format!("ERROR: {error}"), None);
            }
        }
        let project = self
            .with_store(|store| store.projection().session_project(&request.session_id))
            .ok()
            .flatten()
            .unwrap_or_default();
        (
            ToolOutcome::Ok,
            format!(
                "Continuing job {}. Its reply will arrive as a handback.",
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

    pub fn compose_handback(
        &self,
        child_session: &str,
        reason: Option<&str>,
        summary: &str,
        footprint: Option<&str>,
    ) -> Result<String, Error> {
        let summary = truncate_summary(summary, child_session);
        let footprint = footprint
            .map(|text| format!("\n{text}"))
            .unwrap_or_default();
        let content = match reason {
            None => format!("Job {child_session} finished.\n{summary}{footprint}"),
            Some(reason) => format!("Job {child_session} stopped: {reason}.\n{summary}{footprint}"),
        };
        Ok(content)
    }

    pub fn append_message(
        &self,
        session_id: &str,
        content: &str,
        source: Source,
    ) -> Result<(), Error> {
        self.record(
            source,
            session_event::Event::MessageAppended(MessageAppended {
                session_id: session_id.to_owned(),
                role: Role::User as i32,
                content: content.to_owned(),
                partial: false,
                turn_id: uuid::Uuid::new_v4().to_string(),
                ..Default::default()
            }),
        )?;
        Ok(())
    }

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

    pub fn session_role(&self, session_id: &str) -> Result<Option<SessionRole>, Error> {
        let raw = self.with_store(|store| store.projection().session_role(session_id))?;
        if raw == Some(provider::LEGACY_DIRECT_ROLE) {
            return Err(Error::HistoricalSession {
                session_id: session_id.to_owned(),
            });
        }
        Ok(raw.and_then(|role| SessionRole::try_from(role).ok()))
    }

    #[tracing::instrument(
        name = "footprint.confirmed_paths",
        skip_all,
        fields(session_id, reply_seq)
    )]
    pub fn changed_paths_for_reply(
        &self,
        session_id: &str,
        reply_seq: u64,
    ) -> Result<Option<Vec<String>>, Error> {
        Ok(self.with_store(|store| {
            store
                .projection()
                .changed_paths_for_reply(session_id, reply_seq)
        })?)
    }

    pub fn session_project(&self, session_id: &str) -> Result<Option<String>, Error> {
        Ok(self.with_store(|store| store.projection().session_project(session_id))?)
    }

    pub fn session_working_directory(&self, session_id: &str) -> Result<Option<String>, Error> {
        Ok(self.with_store(|store| store.projection().session_working_directory(session_id))?)
    }

    pub fn project_description(&self, name: &str) -> Option<&str> {
        self.projects
            .get(name)
            .map(|spec| spec.description.as_str())
    }

    pub fn session_usage_tokens(&self, session_id: &str) -> Result<u64, Error> {
        Ok(self.with_store(|store| store.projection().session_token_total(session_id))?)
    }

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

    fn tool_setup(
        &self,
        session_id: &str,
        new_session: bool,
        runner: &Runner,
    ) -> Result<(Vec<ToolSource>, Vec<String>), Error> {
        let role = runner.role;
        let (project, source) = if new_session {
            (None, None)
        } else {
            self.with_store(|store| -> Result<_, Error> {
                Ok((
                    store.projection().session_project(session_id)?,
                    store.projection().session_source(session_id)?,
                ))
            })?
        };
        let mut sources = vec![ToolSource::Builtin, ToolSource::Workspace];
        let command_prefix = project
            .as_deref()
            .and_then(|name| self.projects.get(name))
            .map_or_else(Vec::new, |spec| spec.command_prefix.clone());
        {
            let editing = if new_session {
                runner.editing
            } else {
                let recorded =
                    self.with_store(|store| store.projection().session_editing(session_id))?;
                match recorded.as_deref() {
                    Some("patch") => crate::tool::Editing::Patch,
                    Some("replacement") => crate::tool::Editing::Replacement,
                    _ => {
                        let provider = self
                            .with_store(|store| store.projection().session_identity(session_id))?
                            .map_or_else(
                                || runner.provider.name().to_owned(),
                                |pin| {
                                    if pin.0.is_empty() {
                                        runner.provider.name().to_owned()
                                    } else {
                                        pin.0
                                    }
                                },
                            );
                        crate::tool::Editing::for_provider(&provider)
                    }
                }
            };
            sources.push(editing.source());
        }
        if source != Some(Source::Model as i32) {
            sources.push(ToolSource::Jobs);
        }
        if role == SessionRole::Chat {
            sources.push(ToolSource::Web);
        }
        Ok((sources, command_prefix))
    }

    fn grants(&self, session_id: &str, new_session: bool) -> Result<Option<Arc<Grants>>, Error> {
        let project = (!new_session)
            .then(|| self.with_store(|store| store.projection().session_project(session_id)))
            .transpose()?
            .flatten();
        if let Some(spec) = project.as_deref().and_then(|name| self.projects.get(name)) {
            return Ok(Some(Arc::new(Grants::new(spec.grants.clone()).map_err(
                |source| Error::Grants {
                    project: project.unwrap(),
                    source,
                },
            )?)));
        }
        if !new_session {
            if let Some(directory) =
                self.with_store(|store| store.projection().session_working_directory(session_id))?
            {
                return Ok(Some(Arc::new(Grants::from_recorded(vec![(
                    PathBuf::from(directory),
                    Mode::ReadWrite,
                )]))));
            }
        }
        let recorded = if new_session {
            Vec::new()
        } else {
            self.with_store(|store| store.projection().session_grants(session_id))?
        };
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
        self.send_message_from(runner, session_id, content, Source::User, events)
            .await
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
    pub async fn send_message_from(
        &self,
        runner: &Runner,
        session_id: Option<&str>,
        content: &str,
        source: Source,
        events: mpsc::Sender<EngineEvent>,
    ) -> Result<Reply, Error> {
        self.send_message_from_with_attachments(
            runner,
            session_id,
            content,
            source,
            Vec::new(),
            events,
        )
        .await
    }

    pub async fn send_message_from_with_attachments(
        &self,
        runner: &Runner,
        session_id: Option<&str>,
        content: &str,
        source: Source,
        mut attachments: Vec<ImageAttachment>,
        events: mpsc::Sender<EngineEvent>,
    ) -> Result<Reply, Error> {
        if content.trim().is_empty() && attachments.is_empty() {
            return Err(Error::EmptyMessage);
        }
        crate::attachment::validate(&mut attachments)?;
        if !attachments.is_empty() && !runner.provider.supports_images() {
            return Err(Error::AttachmentsUnsupported {
                provider: runner.provider.name().to_owned(),
            });
        }
        let (session_id, new_session) = match session_id {
            Some(id) => (id.to_owned(), false),
            None => (uuid::Uuid::new_v4().to_string(), true),
        };
        self.run_session_turn(
            runner,
            session_id,
            new_session,
            Some(Inbound {
                content: content.to_owned(),
                source,
                attachments,
            }),
            events,
        )
        .await
    }

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
        self.run_session_turn(runner, session_id.to_owned(), false, None, events)
            .await
    }

    async fn run_session_turn(
        &self,
        runner: &Runner,
        session_id: String,
        new_session: bool,
        message: Option<Inbound>,
        events: mpsc::Sender<EngineEvent>,
    ) -> Result<Reply, Error> {
        let span = tracing::Span::current();
        span.record("session_id", session_id.as_str());
        span.record("new_session", new_session);
        let turn_id = uuid::Uuid::new_v4().to_string();

        let guard = self.turn_guard(&session_id);
        let turn = guard.lock().await;

        if !new_session {
            self.enforce_pin(runner, &session_id)?;
        }
        let (sources, command_prefix) = self.tool_setup(&session_id, new_session, runner)?;
        let grants = self.grants(&session_id, new_session)?;

        if new_session {
            let selected = self.role_choices.get(&runner.role).and_then(|menu| {
                menu.iter().find(|choice| {
                    choice.provider == runner.provider.name()
                        && choice.model == runner.model
                        && choice.thinking == runner.thinking
                })
            });
            self.record_unbound_session(
                &session_id,
                runner,
                selected,
                runner.editing,
                String::new(),
            )?;
        }
        if let Some(message) = message {
            self.record(
                message.source,
                session_event::Event::MessageAppended(MessageAppended {
                    session_id: session_id.clone(),
                    role: Role::User as i32,
                    content: message.content,
                    partial: false,
                    turn_id: turn_id.clone(),
                    attachments: message.attachments,
                    ..Default::default()
                }),
            )?;
        }

        let reply = self
            .drive_turn(
                runner,
                &session_id,
                &turn_id,
                &sources,
                grants.as_ref(),
                &command_prefix,
                &events,
            )
            .await;
        drop(turn);
        if reply.as_ref().is_ok_and(|reply| !reply.partial) {
            let _ = self.completed_turns.send(session_id);
        }
        reply
    }

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
        let mut cancel_requests: Vec<String> = Vec::new();
        let mut server_calls = ServerCalls::default();
        let mut grounding: Option<String> = None;

        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let (inbox_tx, mut inbox_rx) = mpsc::unbounded_channel::<Inbound>();
        self.live_turns
            .lock()
            .expect("live turns lock poisoned")
            .insert(
                session_id.to_owned(),
                LiveTurn {
                    cancel: cancel_tx,
                    inbox: inbox_tx,
                },
            );
        let _turn_guard = TurnRegistration {
            live: &self.live_turns,
            session_id,
        };

        let mut attempted_compaction = None;
        let reply = loop {
            // the last step offers no tools, so the model has to answer
            let last_step = steps >= max_tool_steps(runner.role);
            let request = self.completion_request(
                runner,
                session_id,
                system.clone(),
                transcript.clone(),
                last_step,
                sources,
            )?;

            let (ending, text, reasoning, calls, step_usage) = self
                .run_completion(
                    runner,
                    session_id,
                    turn_id,
                    request,
                    events,
                    &mut total_usage,
                    &mut server_calls,
                    &mut grounding,
                    &mut cancel_rx,
                )
                .await?;
            if let Some(usage) = step_usage {
                self.record(
                    Source::System,
                    session_event::Event::ContextMeasured(arc_proto::v1::ContextMeasured {
                        session_id: session_id.to_owned(),
                        input_tokens: usage.input_tokens,
                        context_window: runner.context_window,
                        compact_at: runner.compact_at,
                    }),
                )?;
            }
            let over_budget = runner
                .compact_at
                .is_some_and(|limit| step_usage.is_some_and(|usage| usage.input_tokens >= limit));

            let completed_seq = match ending {
                Ending::Done(Stop::ToolCalls) if !last_step && !calls.is_empty() => {
                    steps += 1;
                    span.record("tool_steps", steps);
                    let cancelled = self
                        .tool_step(
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
                            &mut cancel_requests,
                            events,
                            &mut cancel_rx,
                        )
                        .await?;
                    if cancelled {
                        span.record("outcome", "cancelled");
                        // the step's own text, if any, already landed as a
                        // durable message above tool_step; nothing new to add
                        break Ok(Reply {
                            session_id: session_id.to_owned(),
                            seq: 0,
                            usage: total_usage,
                            partial: true,
                            step_capped: false,
                            grounding_json: grounding.clone().unwrap_or_default(),
                            jobs,
                            continues,
                            cancels: cancel_requests,
                        });
                    }
                    self.drain_inbox(session_id, turn_id, &mut inbox_rx, &mut transcript)?;
                    None
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
                    transcript.push(Message::Text {
                        role: Role::Assistant,
                        content: text,
                        reasoning: None,
                    });
                    // a message that lands right as the model stops keeps
                    // the turn going instead of ending it unanswered
                    let steered =
                        self.drain_inbox(session_id, turn_id, &mut inbox_rx, &mut transcript)?;
                    (!steered).then_some(seq)
                }
                Ending::Cut { cancelled } if text.is_empty() => {
                    span.record("outcome", if cancelled { "cancelled" } else { "error" });
                    break Err(if cancelled {
                        Error::Cancelled
                    } else {
                        Error::EmptyReply
                    });
                }
                Ending::Cut { cancelled } => {
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
                    span.record("outcome", if cancelled { "cancelled" } else { "partial" });
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
                        cancels: cancel_requests,
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
            };
            if over_budget {
                let cutoff = self.compaction_cutoff(session_id)?;
                if cutoff > attempted_compaction {
                    attempted_compaction = cutoff;
                    if self.compact(runner, session_id, turn_id).await? && completed_seq.is_none() {
                        let rows = self
                            .with_store(|store| store.projection().lineage_messages(session_id))?;
                        transcript = rebuild_transcript(&rows);
                    }
                }
            }
            if let Some(seq) = completed_seq {
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
                    cancels: cancel_requests,
                });
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
        cancel_rx: &mut watch::Receiver<bool>,
    ) -> Result<(Ending, String, String, Vec<ToolCall>, Option<Usage>), Error> {
        let mut stream = runner.provider.complete(request).await?;
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut calls = Vec::new();
        let mut step_usage: Option<Usage> = None;
        let ending = loop {
            let delta = tokio::select! {
                _ = cancel_rx.changed() => break Ending::Cut { cancelled: true },
                delta = stream.next() => delta,
            };
            match delta {
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
                                response_json: payload_json.clone(),
                                provider_roundtrip: Vec::new(),
                            }),
                        )?;
                        server_calls.recorded += 1;
                        let _ = events
                            .send(EngineEvent::ToolCallEnded {
                                call_id,
                                outcome: ToolOutcome::Ok,
                                content: payload_json,
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
                Some(Ok(CompletionDelta::UnmeasuredDone { stop })) => break Ending::Done(stop),
                Some(Ok(CompletionDelta::Done { usage, stop })) => {
                    let total = total_usage.get_or_insert(Usage::default());
                    total.input_tokens = total.input_tokens.saturating_add(usage.input_tokens);
                    total.output_tokens = total.output_tokens.saturating_add(usage.output_tokens);
                    step_usage = Some(usage);
                    break Ending::Done(stop);
                }
                Some(Err(error)) => break Ending::Failed(error),
                None => break Ending::Cut { cancelled: false },
            }
        };
        let (text, reasoning) = split_leaked_thinking(text, reasoning);
        Ok((ending, text, reasoning, calls, step_usage))
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
        cancels: &mut Vec<String>,
        events: &mpsc::Sender<EngineEvent>,
        cancel_rx: &mut watch::Receiver<bool>,
    ) -> Result<bool, Error> {
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
        let mut cancelled = false;
        for call in &calls {
            let ctx = TurnContext {
                session_id: session_id.to_owned(),
                turn_id: turn_id.to_owned(),
                grants: grants.cloned(),
                command_prefix: command_prefix.to_vec(),
            };
            let dispatch = self
                .registry
                .dispatch(&call.name, call.arguments.clone(), ctx, sources);
            // dropping `dispatch` here abandons whatever the tool was doing;
            // a bash child keeps running to its own timeout regardless —
            // threading cancel into tools themselves is later work
            let outcome = tokio::select! {
                outcome = dispatch => outcome,
                _ = cancel_rx.changed() => {
                    cancelled = true;
                    break;
                }
            };
            let DispatchOutcome {
                content,
                changed_paths,
                ok,
                truncated,
                memory_events,
                job_request,
                continue_request,
                cancel_request,
            } = outcome;
            for memory_event in memory_events {
                memory.observe_event(&memory_event);
                self.record_memory(Source::Model, memory_event)?;
            }
            let ready_from = (jobs.len(), continues.len(), cancels.len());
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
            } else if let Some(cancel_request) = cancel_request {
                cancels.push(cancel_request);
                (ToolOutcome::Ok, content)
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
                    changed_paths,
                }),
            )?;
            let _ = events
                .send(EngineEvent::ToolCallEnded {
                    call_id: call.id.clone(),
                    outcome,
                    content: content.clone(),
                })
                .await;
            if ready_from != (jobs.len(), continues.len(), cancels.len()) {
                let _ = events
                    .send(EngineEvent::JobsReady {
                        jobs: jobs[ready_from.0..].to_vec(),
                        continues: continues[ready_from.1..].to_vec(),
                        cancels: cancels[ready_from.2..].to_vec(),
                    })
                    .await;
            }
            results.push((call.id.clone(), content));
        }

        if cancelled {
            for call in calls.iter().skip(results.len()) {
                let content = "cancelled by the user".to_owned();
                self.record(
                    Source::System,
                    session_event::Event::ToolResultRecorded(ToolResultRecorded {
                        changed_paths: Vec::new(),
                        session_id: session_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        call_id: call.id.clone(),
                        outcome: ToolOutcome::Error as i32,
                        content: content.clone(),
                        truncated: false,
                    }),
                )?;
                let _ = events
                    .send(EngineEvent::ToolCallEnded {
                        call_id: call.id.clone(),
                        outcome: ToolOutcome::Error,
                        content: content.clone(),
                    })
                    .await;
            }
            return Ok(true);
        }

        transcript.push(Message::ToolCalls {
            calls,
            reasoning: (!reasoning.is_empty()).then_some(reasoning),
        });
        for (call_id, content) in results {
            transcript.push(Message::ToolResult { call_id, content });
        }
        Ok(false)
    }

    fn enforce_pin(&self, runner: &Runner, session_id: &str) -> Result<(), Error> {
        match self.with_store(|store| store.projection().session_role(session_id))? {
            Some(provider::LEGACY_DIRECT_ROLE) => {
                return Err(Error::HistoricalSession {
                    session_id: session_id.to_owned(),
                });
            }
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
        match self.identity_mismatch(runner, session_id) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn identity_mismatch(&self, runner: &Runner, session_id: &str) -> Option<Error> {
        let recorded = self
            .with_store(|store| store.projection().session_identity(session_id))
            .ok()
            .flatten()?;
        if recorded.0.is_empty() && recorded.1.is_empty() {
            return None;
        }
        let current = (runner.provider.name().to_owned(), runner.model.clone());
        if recorded == current {
            return None;
        }
        Some(Error::ModelMismatch {
            session_id: session_id.to_owned(),
            pinned: identity_label(&recorded.0, &recorded.1),
            role: provider::role_label(runner.role).to_owned(),
            serving: identity_label(&current.0, &current.1),
        })
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

    pub fn session_choice(&self, session_id: &str) -> Result<Option<String>, Error> {
        Ok(self.with_store(|store| store.projection().session_choice(session_id))?)
    }

    pub fn session_identity(&self, session_id: &str) -> Result<Option<(String, String)>, Error> {
        Ok(self.with_store(|store| store.projection().session_identity(session_id))?)
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

    // arrival order: a message sent while the turn is between steps lands
    // in the transcript exactly where it was queued
    fn drain_inbox(
        &self,
        session_id: &str,
        turn_id: &str,
        inbox_rx: &mut mpsc::UnboundedReceiver<Inbound>,
        transcript: &mut Vec<Message>,
    ) -> Result<bool, Error> {
        let mut delivered = false;
        while let Ok(inbound) = inbox_rx.try_recv() {
            self.record(
                inbound.source,
                session_event::Event::MessageAppended(MessageAppended {
                    session_id: session_id.to_owned(),
                    role: Role::User as i32,
                    content: inbound.content.clone(),
                    partial: false,
                    turn_id: turn_id.to_owned(),
                    attachments: inbound.attachments.clone(),
                    ..Default::default()
                }),
            )?;
            if inbound.attachments.is_empty() {
                transcript.push(Message::Text {
                    role: Role::User,
                    content: inbound.content,
                    reasoning: None,
                });
            } else {
                transcript.push(Message::UserWithAttachments {
                    content: inbound.content,
                    attachments: inbound.attachments,
                });
            }
            tracing::info!(
                session_id,
                source = ?inbound.source,
                "delivered a queued message at a turn step boundary"
            );
            delivered = true;
        }
        Ok(delivered)
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
                attachments: Vec::new(),
            }),
        )
    }

    /// Replaces the session's transcript through a cutoff seq with one
    /// summary the runner's own model writes. Returns whether a
    /// `SessionCompacted` event was appended; `false` covers every reason it
    /// wasn't — nothing to compact, a provider error, a summary that failed
    /// validation — and the turn (or the hand-triggered `:compact`) is meant
    /// to carry on uncompacted in every one of them.
    #[tracing::instrument(
        name = "session.compact",
        skip_all,
        fields(
            role = provider::role_label(runner.role),
            session_id,
            turn_id = %turn_id,
            through_seq = tracing::field::Empty,
            model = tracing::field::Empty,
            input_tokens = tracing::field::Empty,
            output_tokens = tracing::field::Empty,
            outcome = tracing::field::Empty,
        )
    )]
    pub async fn compact(
        &self,
        runner: &Runner,
        session_id: &str,
        turn_id: &str,
    ) -> Result<bool, Error> {
        let span = tracing::Span::current();

        let Some(through_seq) = self.compaction_cutoff(session_id)? else {
            tracing::info!(session_id, "nothing to compact");
            span.record("outcome", "nothing_to_compact");
            return Ok(false);
        };
        span.record("through_seq", through_seq);

        let rows =
            self.with_store(|store| store.projection().lineage_messages_with_seq(session_id))?;
        let prefix: Vec<MessageRow> = rows
            .iter()
            .filter(|(seq, _)| *seq <= through_seq)
            .map(|(_, row)| row.clone())
            .collect();
        if prefix.is_empty() {
            tracing::info!(session_id, "nothing to compact");
            span.record("outcome", "nothing_to_compact");
            return Ok(false);
        }
        let original_rows =
            self.with_store(|store| store.projection().original_lineage_rows(session_id))?;
        let users_words = recent_user_words(&original_rows, through_seq);
        let preserved_bytes = render_compaction_summary("", &users_words).len();

        let choice = self
            .current_choice(SessionRole::Archivist)?
            .ok_or_else(|| Error::NoRunner {
                role: "archivist".to_owned(),
            })?;
        let compact_runner = self
            .compaction_runners
            .iter()
            .find(|(name, _)| *name == choice.name)
            .map(|(_, runner)| runner)
            .ok_or_else(|| Error::NoRunner {
                role: "archivist compaction".to_owned(),
            })?;
        span.record("model", &compact_runner.model);
        let request = CompletionRequest {
            model: compact_runner.model.clone(),
            role: compact_runner.role,
            thinking: compact_runner.thinking,
            thinking_updates: Vec::new(),
            system: Some(COMPACTION_PROMPT_V3.to_owned()),
            messages: rebuild_transcript(&prefix),
            tools: Vec::new(),
            seed: None,
            web: false,
            cache_key: Some(session_id.to_owned()),
        };

        let (mut model_text, usage) = self
            .compaction_completion(compact_runner, request)
            .await
            .map_err(|reason| {
                span.record("outcome", "provider_error");
                tracing::warn!(session_id, %reason, "compaction call failed");
                Error::CompactionFailed { reason }
            })?;
        span.record("input_tokens", usage.input_tokens);
        span.record("output_tokens", usage.output_tokens);
        tracing::info!(draft_bytes = model_text.len(), "compaction draft received");

        let mut summary = render_compaction_summary(&model_text, &users_words);
        if let Err(reason) = validate_compaction_summary(&model_text, &summary) {
            if model_text.trim().is_empty() {
                span.record("outcome", "empty_draft");
                return Err(Error::CompactionFailed {
                    reason: reason.to_owned(),
                });
            }
            let mut draft_end = model_text.len().min(MAX_COMPACTION_SUMMARY_BYTES);
            while !model_text.is_char_boundary(draft_end) {
                draft_end -= 1;
            }
            let repair = CompletionRequest {
                model: compact_runner.model.clone(),
                role: compact_runner.role,
                thinking: compact_runner.thinking,
                thinking_updates: Vec::new(),
                system: Some(COMPACTION_REPAIR_PROMPT_V2.to_owned()),
                messages: vec![Message::Text {
                    role: Role::User,
                    content: format!(
                        "Validation error: {reason}\n\
                         Maximum summary bytes before ARC appends user messages: {}\n\n\
                         Draft:\n{}",
                        MAX_COMPACTION_SUMMARY_BYTES - preserved_bytes,
                        &model_text[..draft_end]
                    ),
                    reasoning: None,
                }],
                tools: Vec::new(),
                seed: None,
                web: false,
                cache_key: Some(session_id.to_owned()),
            };
            (model_text, _) = self
                .compaction_completion(compact_runner, repair)
                .await
                .map_err(|reason| {
                    span.record("outcome", "repair_error");
                    tracing::warn!(session_id, %reason, "compaction repair failed");
                    Error::CompactionFailed { reason }
                })?;
            tracing::info!(
                repair_bytes = model_text.len(),
                "compaction repair received"
            );
            summary = render_compaction_summary(&model_text, &users_words);
            validate_compaction_summary(&model_text, &summary).map_err(|reason| {
                span.record("outcome", "invalid_repair");
                tracing::warn!(session_id, reason, "compaction repair failed validation");
                Error::CompactionFailed {
                    reason: reason.to_owned(),
                }
            })?;
        }

        self.record(
            Source::Model,
            session_event::Event::SessionCompacted(SessionCompacted {
                session_id: session_id.to_owned(),
                through_seq,
                summary,
                prompt_version: COMPACTION_PROMPT_VERSION.to_owned(),
                model: compact_runner.model.clone(),
            }),
        )?;
        span.record("outcome", "compacted");
        Ok(true)
    }

    fn compaction_cutoff(&self, session_id: &str) -> Result<Option<u64>, Error> {
        let (own_user_seqs, rows) = self.with_store(|store| {
            Ok::<_, Error>((
                store.projection().own_user_message_seqs(session_id)?,
                store.projection().lineage_messages_with_seq(session_id)?,
            ))
        })?;
        let exchange_cutoff = if own_user_seqs.len() >= 3 {
            own_user_seqs.get(own_user_seqs.len() - 2)
        } else {
            own_user_seqs.last()
        }
        .and_then(|seq| seq.checked_sub(1));
        let mut open = std::collections::HashSet::new();
        let mut batches = Vec::new();
        for (seq, row) in &rows {
            match row {
                MessageRow::ToolCall { call_id, .. } => {
                    open.insert(call_id);
                }
                MessageRow::ToolResult { call_id, .. }
                    if open.remove(call_id) && open.is_empty() =>
                {
                    batches.push(*seq);
                }
                _ => {}
            }
        }
        let batch_cutoff = batches.len().checked_sub(3).map(|i| batches[i]);
        let cutoff = exchange_cutoff.max(batch_cutoff);
        Ok(cutoff.filter(|cutoff| {
            rows.iter().any(|(seq, row)| {
                *seq <= *cutoff
                    && !matches!(row, MessageRow::Message { turn_id, source, .. }
                        if turn_id.is_empty() && *source == Source::Model as i32)
            })
        }))
    }

    /// A single non-streamed completion for the compaction pass: no tools
    /// offered, so a tool call back is a provider that ignored the system
    /// prompt, not a turn to drive.
    async fn compaction_completion(
        &self,
        runner: &Runner,
        request: CompletionRequest,
    ) -> Result<(String, Usage), String> {
        let mut stream = runner
            .provider
            .complete(request)
            .await
            .map_err(|error| format!("provider refused the request: {error}"))?;
        let mut text = String::new();
        let mut usage = Usage::default();
        let mut finished = false;
        while let Some(item) = stream.next().await {
            match item.map_err(|error| format!("stream failed: {error}"))? {
                CompletionDelta::Text(chunk) => text.push_str(&chunk),
                CompletionDelta::Reasoning(_)
                | CompletionDelta::ServerCall { .. }
                | CompletionDelta::ServerResponse { .. }
                | CompletionDelta::Grounding(_) => {}
                CompletionDelta::ToolCall(call) => {
                    return Err(format!(
                        "the model called {} with no tools offered",
                        call.name
                    ));
                }
                CompletionDelta::Done {
                    usage: step_usage,
                    stop: Stop::EndTurn,
                } => {
                    usage = step_usage;
                    finished = true;
                }
                CompletionDelta::Done {
                    stop: Stop::ToolCalls,
                    ..
                }
                | CompletionDelta::UnmeasuredDone {
                    stop: Stop::ToolCalls,
                } => {
                    return Err("the model stopped for tool calls with no tools offered".to_owned());
                }
                CompletionDelta::UnmeasuredDone {
                    stop: Stop::EndTurn,
                } => finished = true,
            }
        }
        if !finished {
            return Err("stream cut before the model finished".to_owned());
        }
        Ok((text, usage))
    }

    fn open_turn(
        &self,
        runner: &Runner,
        session_id: &str,
    ) -> Result<(Vec<Message>, Option<String>), Error> {
        let (rows, memory_index) = self.with_store(|store| {
            Ok::<_, Error>((
                store.projection().lineage_messages(session_id)?,
                store.projection().memory_index()?,
            ))
        })?;
        let system = Self::system_prompt(runner, render_memory_index(&memory_index).as_deref());
        Ok((rebuild_transcript(&rows), system))
    }

    fn completion_request(
        &self,
        runner: &Runner,
        session_id: &str,
        system: Option<String>,
        messages: Vec<Message>,
        last_step: bool,
        sources: &[ToolSource],
    ) -> Result<CompletionRequest, Error> {
        let effective = self.session_thinking(session_id, runner.thinking)?;
        let codex = runner.provider.name() == "codex";
        let (thinking, thinking_updates, messages) = if codex {
            let (rows, changes, initial) = self.with_store(|store| {
                Ok::<_, projection::Error>((
                    store.projection().lineage_messages_with_seq(session_id)?,
                    store
                        .projection()
                        .lineage_thinking_changes_with_seq(session_id)?,
                    store.projection().session_thinking_at(session_id, 0)?,
                ))
            })?;
            let messages =
                rebuild_transcript(&rows.iter().map(|(_, row)| row.clone()).collect::<Vec<_>>());
            let thinking = match initial.as_deref() {
                Some(label) => {
                    Thinking::from_label(label).ok_or_else(|| Error::UnsupportedThinking {
                        thinking: label.to_owned(),
                        provider: runner.provider.name().to_owned(),
                        model: runner.model.clone(),
                    })?
                }
                None => runner.thinking,
            };
            let summary_seq = rows.iter().find_map(|(seq, row)| match row {
                MessageRow::Message {
                    role,
                    source,
                    turn_id,
                    ..
                } if *role == Role::User as i32
                    && *source == Source::Model as i32
                    && turn_id.is_empty() =>
                {
                    Some(*seq)
                }
                _ => None,
            });
            let mut updates = Vec::new();
            let mut current = thinking;
            let mut summary_effort = thinking;
            for (seq, label) in changes {
                let level =
                    Thinking::from_label(&label).ok_or_else(|| Error::UnsupportedThinking {
                        thinking: label,
                        provider: runner.provider.name().to_owned(),
                        model: runner.model.clone(),
                    })?;
                if summary_seq.is_some_and(|summary_seq| seq <= summary_seq) {
                    summary_effort = level;
                    current = level;
                    continue;
                }
                let prefix_rows: Vec<_> = rows
                    .iter()
                    .take_while(|(row_seq, _)| *row_seq < seq)
                    .map(|(_, row)| row.clone())
                    .collect();
                let position = rebuild_transcript(&prefix_rows).len();
                if position > messages.len() {
                    return Err(Error::InvalidThinkingPosition);
                }
                if current != level {
                    updates.push((position, level));
                }
                current = level;
            }
            if summary_seq.is_some() && summary_effort != thinking {
                let summary_position = rows
                    .iter()
                    .position(|(seq, _)| Some(*seq) == summary_seq)
                    .map_or(0, |index| {
                        rebuild_transcript(
                            &rows[..=index]
                                .iter()
                                .map(|(_, row)| row.clone())
                                .collect::<Vec<_>>(),
                        )
                        .len()
                    });
                updates.insert(0, (summary_position, summary_effort));
            }
            (thinking, updates, messages)
        } else {
            (effective, Vec::new(), messages)
        };
        Ok(CompletionRequest {
            model: runner.model.clone(),
            role: runner.role,
            thinking,
            thinking_updates,
            system,
            messages,
            tools: if last_step {
                Vec::new()
            } else {
                self.registry.definitions(sources)
            },
            seed: None,
            web: sources.contains(&ToolSource::Web),
            cache_key: Some(session_id.to_owned()),
        })
    }
}

pub const COMPACTION_PROMPT_VERSION: &str = "v3";

pub const COMPACTION_PROMPT_V3: &str = r#"You are ARC's compaction pass. A conversation has outgrown its context
window; your summary replaces everything before it, and the rest of the
conversation continues after it. Whatever you leave out is gone.

Summarize the user's goal, what has been completed, what remains open,
and the facts needed to continue: decisions, corrections, file paths,
commands, and errors. Include older forks. Do not assume any user message
is copied: ARC may append up to two recent messages verbatim when they fit.
Summarize tool output; never quote it. Answer directly in under 24 KiB.
Organize the summary as you see fit."#;

const COMPACTION_REPAIR_PROMPT_V2: &str = "Shorten this compaction draft to fit \
the byte limit supplied with it. Preserve decisions, corrections, and open \
work. Return only the summary; ARC may append a recent user tail separately.";

const USERS_WORDS_HEADING: &str = "Recent user's words";

const MAX_COMPACTION_SUMMARY_BYTES: usize = 32 * 1024;
const MAX_COPIED_USER_BYTES: usize = 8 * 1024;

fn recent_user_words(rows: &[(u64, MessageRow)], through_seq: u64) -> Vec<&str> {
    let mut words = Vec::new();
    let mut bytes = 0;
    for (_, row) in rows.iter().filter(|(seq, _)| *seq <= through_seq).rev() {
        let MessageRow::Message {
            role,
            content,
            source,
            ..
        } = row
        else {
            continue;
        };
        if *role != Role::User as i32 || *source != Source::User as i32 {
            continue;
        }
        let next = content.len() + 2;
        if bytes + next > MAX_COPIED_USER_BYTES {
            break;
        }
        words.push(content.as_str());
        bytes += next;
        if words.len() == 2 {
            break;
        }
    }
    words.reverse();
    words
}

fn render_compaction_summary(model_text: &str, users_words: &[&str]) -> String {
    let mut summary = model_text.trim().to_owned();
    summary.push_str("\n\n");
    summary.push_str(USERS_WORDS_HEADING);
    summary.push('\n');
    if users_words.is_empty() {
        summary.push_str("(none)\n");
    } else {
        for message in users_words {
            summary.push('\n');
            summary.push_str(message);
            summary.push('\n');
        }
    }
    summary
}

fn validate_compaction_summary(model_text: &str, summary: &str) -> Result<(), &'static str> {
    if model_text.trim().is_empty() {
        return Err("empty model summary");
    }
    if summary.len() > MAX_COMPACTION_SUMMARY_BYTES {
        return Err("exceeds the 32 KiB cap");
    }
    Ok(())
}

const MAX_HANDBACK_SUMMARY_BYTES: usize = 16 * 1024;

fn truncate_summary(summary: &str, child_session: &str) -> String {
    if summary.len() <= MAX_HANDBACK_SUMMARY_BYTES {
        return summary.to_owned();
    }
    let mut cut = MAX_HANDBACK_SUMMARY_BYTES;
    while !summary.is_char_boundary(cut) {
        cut -= 1;
    }
    format!(
        "{} [truncated; use session_read for session {child_session} to read the rest]",
        &summary[..cut]
    )
}

// DeepSeek at long context sometimes thinks in the content channel and
// closes with a bare tag; the head is reasoning, never a reply
fn split_leaked_thinking(text: String, mut reasoning: String) -> (String, String) {
    let Some((head, tail)) = text.split_once("</think>") else {
        return (text, reasoning);
    };
    let head = head.strip_prefix("<think>").unwrap_or(head).trim();
    if !head.is_empty() {
        if !reasoning.is_empty() {
            reasoning.push('\n');
        }
        reasoning.push_str(head);
    }
    (tail.trim_start().to_owned(), reasoning)
}

fn session_id_of(event: &session_event::Event) -> &str {
    match event {
        session_event::Event::SessionCreated(e) => &e.session_id,
        session_event::Event::MessageAppended(e) => &e.session_id,
        session_event::Event::ToolCallIssued(e) => &e.session_id,
        session_event::Event::ToolResultRecorded(e) => &e.session_id,
        session_event::Event::ServerCallRecorded(e) => &e.session_id,
        session_event::Event::BranchMarked(e) => &e.session_id,
        session_event::Event::SessionConsolidated(e) => &e.session_id,
        session_event::Event::SessionTitled(e) => &e.session_id,
        session_event::Event::SessionCompacted(e) => &e.session_id,
        session_event::Event::ContextMeasured(e) => &e.session_id,
        session_event::Event::SessionThinkingSelected(e) => &e.session_id,
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
            MessageRow::Message {
                role,
                content,
                attachments,
                ..
            } => {
                if let Ok(mapped @ (Role::User | Role::Assistant)) = Role::try_from(*role) {
                    if mapped == Role::User && !attachments.is_empty() {
                        messages.push(Message::UserWithAttachments {
                            content: content.clone(),
                            attachments: attachments.clone(),
                        });
                    } else {
                        messages.push(Message::Text {
                            role: mapped,
                            content: content.clone(),
                            reasoning: None,
                        });
                    }
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
    Cut { cancelled: bool },
    Failed(provider::Error),
}

struct LiveTurn {
    cancel: watch::Sender<bool>,
    inbox: mpsc::UnboundedSender<Inbound>,
}

struct TurnRegistration<'a> {
    live: &'a StdMutex<HashMap<String, LiveTurn>>,
    session_id: &'a str,
}

impl Drop for TurnRegistration<'_> {
    fn drop(&mut self) {
        self.live
            .lock()
            .expect("live turns lock poisoned")
            .remove(self.session_id);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arc_proto::v1::{
        HistoryEntry, HistoryMessage, ImageAttachment, MemoryRecord, MemoryRecordCreated,
        MemoryRecordSuperseded, Role, SessionCompacted, SessionRole, Source, ToolOutcome,
        history_entry, memory_event, memory_record, session_event,
    };
    use tempfile::TempDir;

    use super::{
        ContinuedJob, DispatchedJob, Engine, EngineEvent, Error, ProjectSpec, Runner,
        max_tool_steps,
    };
    use crate::log::Log;
    use crate::projection::Projection;
    use crate::provider::{
        CompletionDelta, Error as ProviderError, Message, Provider, Stop, Thinking, ToolCall, Usage,
    };
    use crate::store::Store;
    use crate::testkit::{
        Canned, Gated, ScriptedProvider, Step, TraceCapture, appended, call, call_carrying,
        channel, counter_samples, done_reply, drain, engine, engine_with_role, engine_with_tools,
        engine_with_tools_at, issued, reopened_engine, replay_events, replay_log, resulted, runner,
        runner_with_role, seed_log, seed_memory_log, seed_memory_log_at, server_called, tool_stop,
        tools, turn, usage,
    };
    use crate::tool::builtin::dispatch::Dispatch;
    use crate::tool::workspace::{self, Grant, Mode, Workspace};
    use crate::tool::{Registry, ToolSource, TurnContext};

    fn conversation_log(path: &std::path::Path) -> Vec<session_event::Event> {
        replay_log(path)
            .into_iter()
            .filter(|event| !matches!(event, session_event::Event::ContextMeasured(_)))
            .collect()
    }

    #[test]
    fn created_sessions_snapshot_initial_thinking_and_legacy_sessions_use_fallback() {
        let dir = TempDir::new().unwrap();
        let provider = ScriptedProvider::scripted(Vec::new());
        let (engine, run) = engine(&provider, &dir);
        let id = engine.create_session(&run).unwrap();
        let created = conversation_log(dir.path())
            .into_iter()
            .find_map(|event| {
                if let session_event::Event::SessionCreated(created) = event {
                    Some(created)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(created.thinking, run.thinking.label());
        assert_eq!(
            engine.session_thinking(&id, Thinking::High).unwrap(),
            run.thinking
        );
        assert!(matches!(
            engine.session_thinking("missing", Thinking::High),
            Err(Error::UnknownSession { .. })
        ));
        engine
            .record(
                Source::User,
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: "legacy-thinking".to_owned(),
                    provider: "scripted".to_owned(),
                    model: "test-model".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        assert_eq!(
            engine
                .session_thinking("legacy-thinking", Thinking::High)
                .unwrap(),
            Thinking::High
        );
    }

    #[test]
    fn thinking_selection_rejects_invalid_and_unsupported_values() {
        let dir = TempDir::new().unwrap();
        let provider = ScriptedProvider::scripted(Vec::new());
        let (engine, run) = engine(&provider, &dir);
        let id = engine.create_session(&run).unwrap();
        assert!(matches!(
            engine.set_session_thinking(&id, "nonsense"),
            Err(Error::UnsupportedThinking { .. })
        ));
        assert!(matches!(
            engine.set_session_thinking(&id, "high"),
            Err(Error::UnsupportedThinking { .. })
        ));
    }

    #[test]
    fn thinking_updates_replay_and_forks_inherit_the_fork_point() {
        let dir = TempDir::new().unwrap();
        let provider = ScriptedProvider::scripted(Vec::new());
        let (engine, _run) = engine(&provider, &dir);
        let id = "thinking-fork";
        engine
            .record(
                Source::User,
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: id.to_owned(),
                    provider: "codex".to_owned(),
                    model: "gpt-6-astra".to_owned(),
                    role: SessionRole::Chat as i32,
                    thinking: "high".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        let user = engine
            .record(
                Source::User,
                session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
                    session_id: id.to_owned(),
                    role: Role::User as i32,
                    content: "fork here".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        engine.set_session_thinking(id, "low").unwrap();
        let fork = engine.fork_session(id, user).unwrap();
        assert_eq!(
            engine.session_thinking(&fork, Thinking::Default).unwrap(),
            Thinking::High
        );
        assert_eq!(
            engine.session_thinking(id, Thinking::Default).unwrap(),
            Thinking::Low
        );
    }

    #[test]
    fn codex_request_keeps_initial_effort_and_places_session_update() {
        let dir = TempDir::new().unwrap();
        let scripted = ScriptedProvider::scripted(Vec::new());
        let (engine, mut run) = engine(&scripted, &dir);
        run.thinking = Thinking::Low;
        let id = "thinking-request";
        engine
            .record(
                Source::User,
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: id.to_owned(),
                    provider: "codex".to_owned(),
                    model: "gpt-6-astra".to_owned(),
                    role: SessionRole::Chat as i32,
                    thinking: "low".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        engine.set_session_thinking(id, "high").unwrap();
        engine
            .record(
                Source::User,
                session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
                    session_id: id.to_owned(),
                    role: Role::User as i32,
                    content: "next".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        run.provider = Arc::new(CodexThinkingProvider);
        let request = engine
            .completion_request(&run, id, None, Vec::new(), true, &[])
            .unwrap();

        assert_eq!(request.thinking, Thinking::Low);
        assert_eq!(request.thinking_updates, vec![(0, Thinking::High)]);
    }

    #[test]
    fn codex_thinking_updates_survive_compaction_and_later_changes() {
        let dir = TempDir::new().unwrap();
        let scripted = ScriptedProvider::scripted(Vec::new());
        let (engine, mut run) = engine(&scripted, &dir);
        run.thinking = Thinking::Low;
        let id = "thinking-compaction";
        engine
            .record(
                Source::User,
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: id.to_owned(),
                    provider: "codex".to_owned(),
                    model: "gpt-6-astra".to_owned(),
                    role: SessionRole::Chat as i32,
                    thinking: "low".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        engine
            .record(
                Source::User,
                session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
                    session_id: id.to_owned(),
                    role: Role::User as i32,
                    content: "first".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        engine.set_session_thinking(id, "high").unwrap();
        assert_eq!(
            engine
                .with_store(|store| store.projection().session_thinking_at(id, 2))
                .unwrap()
                .as_deref(),
            Some("high"),
        );
        engine
            .record(
                Source::User,
                session_event::Event::SessionCompacted(SessionCompacted {
                    session_id: id.to_owned(),
                    through_seq: 2,
                    summary: "summary".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        engine
            .record(
                Source::User,
                session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
                    session_id: id.to_owned(),
                    role: Role::User as i32,
                    content: "after summary".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        engine.set_session_thinking(id, "max").unwrap();
        engine
            .record(
                Source::User,
                session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
                    session_id: id.to_owned(),
                    role: Role::User as i32,
                    content: "last".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        run.provider = Arc::new(CodexThinkingProvider);
        let check = |engine: &Engine, run: &Runner| {
            engine
                .completion_request(run, id, None, Vec::new(), true, &[])
                .unwrap()
        };
        let request = check(&engine, &run);
        assert_eq!(request.thinking, Thinking::Low);
        assert!(matches!(
            &request.messages[0],
            Message::Text { content, .. } if content == "summary"
        ));
        assert_eq!(
            request.thinking_updates,
            vec![(1, Thinking::High), (2, Thinking::Max)]
        );

        let (reopened, _) = reopened_engine(&scripted, &dir, Registry::new(4));
        let reopened_request = check(&reopened, &run);
        assert_eq!(reopened_request.thinking, request.thinking);
        assert_eq!(reopened_request.messages, request.messages);
        assert_eq!(reopened_request.thinking_updates, request.thinking_updates);

        run.provider = Arc::clone(&scripted) as Arc<dyn Provider>;
        let non_codex = check(&engine, &run);
        assert_eq!(non_codex.thinking, Thinking::Max);
        assert!(non_codex.thinking_updates.is_empty());
    }

    #[derive(Debug)]
    struct CodexThinkingProvider;

    impl Provider for CodexThinkingProvider {
        fn name(&self) -> &'static str {
            "codex"
        }

        fn complete(
            &self,
            _request: crate::provider::CompletionRequest,
        ) -> futures::future::BoxFuture<'_, Result<crate::provider::CompletionStream, ProviderError>>
        {
            Box::pin(async { panic!("test provider must not make requests") })
        }
    }

    #[tokio::test]
    async fn thinking_selection_rejects_an_active_turn() {
        let dir = TempDir::new().unwrap();
        let provider = ScriptedProvider::scripted(Vec::new());
        let (engine, _) = engine(&provider, &dir);
        let id = "thinking-busy";
        engine
            .record(
                Source::User,
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: id.to_owned(),
                    provider: "codex".to_owned(),
                    model: "gpt-6-astra".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        let guard = engine.turn_guard(id);
        let _held = guard.lock().await;
        assert!(matches!(
            engine.set_session_thinking(id, "high"),
            Err(Error::ThinkingWhileBusy { .. })
        ));
    }

    #[derive(Debug)]
    struct TextOnlyProvider;

    impl Provider for TextOnlyProvider {
        fn name(&self) -> &'static str {
            "text-only"
        }

        fn complete(
            &self,
            _request: crate::provider::CompletionRequest,
        ) -> futures::future::BoxFuture<'_, Result<crate::provider::CompletionStream, ProviderError>>
        {
            Box::pin(async { panic!("an unsupported picture must not reach the provider") })
        }
    }

    #[tokio::test]
    async fn confirmed_writes_survive_capped_results_and_engine_replay() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("project");
        std::fs::create_dir(&root).unwrap();
        let path = root.join("child.txt");
        let mut registry = Registry::new(4);
        for tool in workspace::tools(Arc::new(Workspace::new())) {
            registry.register(tool);
        }
        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "write-1",
                    0,
                    "write",
                    &serde_json::json!({"path": path, "content": "child"}).to_string(),
                )),
                Ok(tool_stop()),
            ],
            done_reply("done"),
        ]);
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Workspace],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));
        let id = engine
            .create_bound_session(&run, "arc", SessionRole::Chat, None)
            .unwrap();
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, Some(&id), "write", tx)
            .await
            .unwrap();
        std::fs::write(root.join("external.txt"), "someone else").unwrap();
        let paths = engine
            .changed_paths_for_reply(&id, reply.seq)
            .unwrap()
            .unwrap();
        assert_eq!(paths, [path.canonicalize().unwrap().to_string_lossy()]);
        let events = replay_log(dir.path());
        assert!(events.iter().any(
            |event| matches!(event, session_event::Event::ToolResultRecorded(result)
            if result.truncated && result.changed_paths == paths)
        ));
        drop(engine);
        let (reopened, _) = reopened_engine(&provider, &dir, Registry::new(4));
        assert_eq!(
            reopened.changed_paths_for_reply(&id, reply.seq).unwrap(),
            Some(paths)
        );
    }

    #[tokio::test]
    async fn context_records_each_measured_step_not_turn_totals_or_missing_usage() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "lookup", "{}")), Ok(tool_stop())],
            vec![
                Ok(CompletionDelta::Text("done".to_owned())),
                Ok(CompletionDelta::Done {
                    usage: Usage {
                        input_tokens: 84_000,
                        output_tokens: 7,
                    },
                    stop: Stop::EndTurn,
                }),
            ],
            vec![
                Ok(CompletionDelta::Text("unmetered".to_owned())),
                Ok(CompletionDelta::UnmeasuredDone {
                    stop: Stop::EndTurn,
                }),
            ],
        ]);
        let dir = TempDir::new().unwrap();
        let (engine, mut run) =
            engine_with_tools(&provider, &dir, tools(&[("lookup", "found", true)]));
        run.context_window = Some(272_000);
        run.compact_at = Some(217_600);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "question", tx)
            .await
            .unwrap();
        assert_eq!(
            reply.usage.unwrap().input_tokens,
            84_000 + usage().input_tokens
        );
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "again", tx)
            .await
            .unwrap();
        let measurements: Vec<_> = replay_log(dir.path())
            .into_iter()
            .filter_map(|event| {
                if let session_event::Event::ContextMeasured(measured) = event {
                    Some(measured)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(measurements.len(), 2);
        assert_eq!(measurements[0].input_tokens, usage().input_tokens);
        assert_eq!(measurements[1].input_tokens, 84_000);
        assert_eq!(measurements[1].context_window, Some(272_000));
        assert_eq!(measurements[1].compact_at, Some(217_600));
    }

    #[tokio::test]
    async fn picture_bytes_survive_replay_and_return_to_the_provider() {
        let dir = TempDir::new().unwrap();
        let first = ScriptedProvider::scripted(vec![done_reply("a diagram")]);
        let (engine, run) = engine(&first, &dir);
        let image = ImageAttachment {
            name: "diagram.png".to_owned(),
            media_type: String::new(),
            data: b"\x89PNG\r\n\x1a\nstable bytes".to_vec(),
        };
        let (tx, _rx) = channel();
        let reply = engine
            .send_message_from_with_attachments(
                &run,
                None,
                "describe it",
                Source::User,
                vec![image.clone()],
                tx,
            )
            .await
            .unwrap();

        assert!(matches!(
            &first.requests()[0].messages[0],
            Message::UserWithAttachments { content, attachments }
                if content == "describe it"
                    && attachments[0].media_type == "image/png"
                    && attachments[0].data == image.data
        ));
        drop(engine);

        let second = ScriptedProvider::scripted(vec![done_reply("still a diagram")]);
        let (reopened, reopened_run) = reopened_engine(&second, &dir, Registry::new(4));
        let (tx, _rx) = channel();
        reopened
            .send_message(&reopened_run, Some(&reply.session_id), "again", tx)
            .await
            .unwrap();

        let requests = second.requests();
        assert!(matches!(
            &requests[0].messages[0],
            Message::UserWithAttachments { content, attachments }
                if content == "describe it"
                    && attachments[0].name == "diagram.png"
                    && attachments[0].data == image.data
        ));
        let history = reopened.transcript(&reply.session_id).unwrap();
        let Some(history_entry::Entry::Message(message)) = &history[0].entry else {
            panic!("expected the durable user message");
        };
        assert_eq!(message.content, "[image: diagram.png]\ndescribe it");
    }

    #[tokio::test]
    async fn a_text_only_provider_refuses_pictures_before_appending() {
        let dir = TempDir::new().unwrap();
        let log = Log::open(dir.path()).unwrap();
        let projection = Projection::in_memory().unwrap();
        let engine = Engine::new(Store::new(log, projection), Registry::new(4));
        let run = Runner {
            role: SessionRole::Chat,
            provider: Arc::new(TextOnlyProvider),
            model: "text-only".to_owned(),
            thinking: Thinking::Default,
            system: None,
            compact_at: None,
            context_window: None,
            editing: crate::tool::Editing::Replacement,
        };
        let (tx, _rx) = channel();

        let error = engine
            .send_message_from_with_attachments(
                &run,
                None,
                "look",
                Source::User,
                vec![ImageAttachment {
                    name: "screen.png".to_owned(),
                    media_type: String::new(),
                    data: b"\x89PNG\r\n\x1a\nbody".to_vec(),
                }],
                tx,
            )
            .await
            .expect_err("text-only provider");

        assert!(matches!(
            error,
            Error::AttachmentsUnsupported { provider } if provider == "text-only"
        ));
        assert!(replay_log(dir.path()).is_empty());
    }

    fn seeded_session() -> session_event::Event {
        session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
            session_id: "s-01".to_owned(),
            parent_session: String::new(),
            fork_point: 0,
            title: String::new(),
            provider: "scripted".to_owned(),
            model: "test-model".to_owned(),
            role: arc_proto::v1::SessionRole::Unspecified as i32,
            project: String::new(),
            budget: None,
            grants: Vec::new(),
            dispatched_by: String::new(),
            choice: String::new(),
            editing: String::new(),
            working_directory: String::new(),
            thinking: String::new(),
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
            seq: 0,
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
            seq: 0,
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
            parent_session: String::new(),
            fork_point: 0,
            title: String::new(),
            provider: "scripted".to_owned(),
            model: "test-model".to_owned(),
            role: SessionRole::Unspecified as i32,
            project: project.to_owned(),
            budget: None,
            grants: Vec::new(),
            dispatched_by: String::new(),
            choice: String::new(),
            editing: String::new(),
            working_directory: String::new(),
            thinking: String::new(),
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
        assert_eq!(reply.seq, 3);

        let events = conversation_log(dir.path());
        assert_eq!(events.len(), 3);
        let session_event::Event::SessionCreated(created) = &events[0] else {
            panic!("expected SessionCreated first, got {:?}", events[0]);
        };
        assert_eq!(created.session_id, reply.session_id);
        assert_eq!(created.provider, "scripted");
        assert_eq!(created.model, "test-model");
        assert_eq!(created.role, SessionRole::Chat as i32);
        assert!(created.project.is_empty());

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
            Some(3)
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
    async fn a_session_pinned_to_another_role_refuses_and_logs_nothing() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: "s-01".to_owned(),
                    parent_session: String::new(),
                    fork_point: 0,
                    title: String::new(),
                    provider: "scripted".to_owned(),
                    model: "test-model".to_owned(),
                    role: SessionRole::Executor as i32,
                    project: String::new(),
                    budget: None,
                    grants: Vec::new(),
                    dispatched_by: String::new(),
                    choice: String::new(),
                    editing: String::new(),
                    working_directory: String::new(),
                    thinking: String::new(),
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
            .expect_err("a chat engine must refuse an executor session");

        assert!(matches!(err, Error::RoleMismatch { .. }), "got: {err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains("executor") && msg.contains("assistant"),
            "the refusal names both roles: {msg}"
        );
        assert_eq!(
            conversation_log(dir.path()).len(),
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
        let events = conversation_log(dir.path());
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
        let events = conversation_log(dir.path());
        assert_eq!(events.len(), 3, "the partial text was still appended");
        let assistant = appended(&events[2]);
        assert!(assistant.partial);
        assert_eq!(assistant.content, "some tex");
    }

    #[tokio::test]
    async fn a_message_queued_during_a_tool_step_lands_after_the_result_and_before_the_reply() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(call("c1", 0, "slow", "{}")), Ok(tool_stop())],
            done_reply("final reply"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let mut registry = Registry::new(512);
        registry.register(Box::new(Gated {
            name: "slow",
            notify: Arc::clone(&gate),
        }));
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let engine = Arc::new(engine);

        let (tx, mut rx) = channel();
        let handle = tokio::spawn({
            let engine = Arc::clone(&engine);
            let run = run.clone();
            async move { engine.send_message(&run, None, "go", tx).await }
        });

        let session_id = loop {
            if let EngineEvent::Accepted { session_id } =
                rx.recv().await.expect("events channel stays open")
            {
                break session_id;
            }
        };
        loop {
            if let EngineEvent::ToolCallStarted { .. } =
                rx.recv().await.expect("events channel stays open")
            {
                break;
            }
        }

        assert!(
            engine.queue_message(&session_id, "queued mid-tool", Source::System),
            "the turn is live"
        );
        gate.notify_one();

        let reply = handle.await.expect("task").expect("a clean reply");
        assert!(!reply.partial);

        let events = conversation_log(dir.path());
        let issued_call = issued(&events[2]);
        assert_eq!(issued_call.call_id, "c1");
        let result = resulted(&events[3]);
        assert_eq!(result.call_id, "c1");
        let injected = appended(&events[4]);
        assert_eq!(injected.content, "queued mid-tool");
        let raw_events = replay_events(dir.path());
        assert_eq!(raw_events[4].source, Source::System as i32);
        let final_reply = appended(&events[5]);
        assert_eq!(final_reply.content, "final reply");
        assert_eq!(events.len(), 6);

        let requests = provider.requests();
        assert_eq!(
            requests[1].messages.last(),
            Some(&Message::Text {
                role: Role::User,
                content: "queued mid-tool".to_owned(),
                reasoning: None,
            }),
            "the next completion request ends with the injected message"
        );
    }

    #[tokio::test]
    async fn a_message_queued_as_the_model_stops_continues_the_turn_for_a_second_completion() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("stopped".to_owned()))],
                notify: Arc::clone(&gate),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Immediate(done_reply("second reply")),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let engine = Arc::new(engine);

        let (tx, mut rx) = channel();
        let handle = tokio::spawn({
            let engine = Arc::clone(&engine);
            let run = run.clone();
            async move { engine.send_message(&run, None, "go", tx).await }
        });

        let session_id = loop {
            if let EngineEvent::Accepted { session_id } =
                rx.recv().await.expect("events channel stays open")
            {
                break session_id;
            }
        };
        loop {
            if let EngineEvent::Delta(text) = rx.recv().await.expect("events channel stays open") {
                assert_eq!(text, "stopped");
                break;
            }
        }

        assert!(
            engine.queue_message(&session_id, "queued at the finish", Source::User),
            "the turn is live"
        );
        gate.notify_one();

        let reply = handle.await.expect("task").expect("a clean reply");
        assert!(!reply.partial);

        let events = conversation_log(dir.path());
        assert_eq!(events.len(), 5);
        let first_reply = appended(&events[2]);
        assert_eq!(first_reply.content, "stopped");
        let injected = appended(&events[3]);
        assert_eq!(injected.content, "queued at the finish");
        let second_reply = appended(&events[4]);
        assert_eq!(second_reply.content, "second reply");
        assert_eq!(reply.seq, 6, "the reply points at the second completion");

        assert_eq!(
            provider.requests().len(),
            2,
            "the queued message forced a second completion"
        );

        assert!(
            !engine.queue_message(&session_id, "too late", Source::User),
            "the turn ended; the inbox is deregistered"
        );
    }

    #[tokio::test]
    async fn cancelling_mid_stream_lands_the_partial_text_durably_and_ends_the_turn() {
        // the first turn only exists to learn a session id ahead of time;
        // the second is the one a background task gets cancelled mid-flight
        let provider = ScriptedProvider::scripted_steps(vec![
            Step::Immediate(done_reply("hi")),
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("partial".to_owned()))],
                notify: Arc::new(tokio::sync::Notify::new()),
                after: Vec::new(),
            },
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let engine = Arc::new(engine);

        let (tx, _rx) = channel();
        let session_id = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("first turn")
            .session_id;

        let (tx, mut rx) = channel();
        let handle = tokio::spawn({
            let engine = Arc::clone(&engine);
            let run = run.clone();
            let session_id = session_id.clone();
            async move { engine.send_message(&run, Some(&session_id), "go", tx).await }
        });

        loop {
            if let EngineEvent::Delta(text) = rx.recv().await.expect("events channel stays open") {
                assert_eq!(text, "partial");
                break;
            }
        }
        assert!(engine.cancel_turn(&session_id), "the turn is live");

        let reply = handle
            .await
            .expect("task")
            .expect("a clean partial reply, not an error");
        assert!(reply.partial);

        let events = conversation_log(dir.path());
        let assistant = appended(events.last().expect("an appended event"));
        assert!(assistant.partial);
        assert_eq!(assistant.content, "partial");

        assert!(
            !engine.cancel_turn(&session_id),
            "the turn ended; the registry is clean"
        );
        assert!(!engine.turn_is_live(&session_id));
        assert!(!engine.queue_message(&session_id, "late", Source::User));
    }

    #[tokio::test]
    async fn cancelling_a_tool_step_keeps_completed_results_and_closes_remaining_calls() {
        // never notified: the tool call stalls until the cancel drops it
        let gate = Arc::new(tokio::sync::Notify::new());
        let provider = ScriptedProvider::scripted(vec![vec![
            Ok(call("fast", 0, "fast", "{}")),
            Ok(call("slow", 1, "slow", "{}")),
            Ok(call("later", 2, "fast", "{}")),
            Ok(tool_stop()),
        ]]);
        let dir = TempDir::new().expect("temp dir");
        let mut registry = Registry::new(512);
        registry.register(Box::new(Canned {
            name: "fast",
            content: "done",
            ok: true,
            source: ToolSource::Builtin,
        }));
        registry.register(Box::new(Gated {
            name: "slow",
            notify: Arc::clone(&gate),
        }));
        let (engine, run) = engine_with_tools(&provider, &dir, registry);
        let engine = Arc::new(engine);

        let (tx, mut rx) = channel();
        let handle = tokio::spawn({
            let engine = Arc::clone(&engine);
            let run = run.clone();
            async move { engine.send_message(&run, None, "go", tx).await }
        });

        let session_id = loop {
            if let EngineEvent::Accepted { session_id } =
                rx.recv().await.expect("events channel stays open")
            {
                break session_id;
            }
        };
        loop {
            if matches!(
                rx.recv().await.expect("events channel stays open"),
                EngineEvent::ToolCallEnded { call_id, .. } if call_id == "fast"
            ) {
                break;
            }
        }
        assert!(engine.cancel_turn(&session_id), "the turn is live");

        let reply = handle
            .await
            .expect("task")
            .expect("a clean partial reply, not an error");
        assert!(reply.partial);

        let events = conversation_log(dir.path());
        let results: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                session_event::Event::ToolResultRecorded(r) if r.session_id == session_id => {
                    Some(r)
                }
                _ => None,
            })
            .collect();
        assert_eq!(results.len(), 3, "every issued call has one result");
        assert_eq!(results[0].call_id, "fast");
        assert_eq!(results[0].outcome, ToolOutcome::Ok as i32);
        assert_eq!(results[0].content, "done");
        for (result, call_id) in results[1..].iter().zip(["slow", "later"]) {
            assert_eq!(result.call_id, call_id);
            assert_eq!(result.outcome, ToolOutcome::Error as i32);
            assert_eq!(result.content, "cancelled by the user");
            assert!(!result.truncated);
        }
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
        let events = conversation_log(dir.path());
        assert_eq!(appended(&events[2]).content, "nobody watched");
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

        let events = conversation_log(dir.path());
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
                    content: "found it".to_owned(),
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

        let events = conversation_log(dir.path());
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
                    content: r#"{"results":["arc 3.5"]}"#.to_owned(),
                },
                EngineEvent::Delta("arc shipped 3.5".to_owned()),
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

        let logged = conversation_log(dir.path())
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

        let events = conversation_log(dir.path());
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

        let events = conversation_log(dir.path());
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

        let ids: Vec<String> = conversation_log(dir.path())
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

        let events = conversation_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Error as i32);
        assert_eq!(result.content, "ERROR: nope");
        assert_eq!(appended(&events[4]).content, "recovered");
        assert!(drain(&mut rx).contains(&EngineEvent::ToolCallEnded {
            call_id: "x".to_owned(),
            outcome: ToolOutcome::Error,
            content: "ERROR: nope".to_owned(),
        }));
    }

    #[tokio::test]
    async fn the_step_cap_forces_a_final_completion_without_tools() {
        let steps = max_tool_steps(SessionRole::Archivist);
        let mut script: Vec<Vec<Result<CompletionDelta, ProviderError>>> = (0..steps)
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
        let (engine, mut run) = engine_with_tools(&provider, &dir, tools(&[("alpha", "A", true)]));
        run.role = SessionRole::Archivist;
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let requests = provider.requests();
        assert_eq!(requests.len(), steps + 1);
        assert!(requests[..steps].iter().all(|r| !r.tools.is_empty()));
        assert!(
            requests[steps].tools.is_empty(),
            "the final completion offers no tools"
        );
        let events = conversation_log(dir.path());
        assert_eq!(appended(events.last().expect("events")).content, "enough");
        assert!(!reply.partial);
        assert!(
            reply.step_capped,
            "every step was spent wanting to keep going"
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
        let events = conversation_log(dir.path());
        assert_eq!(events.len(), 3);
        assert_eq!(appended(&events[2]).content, "hi there");
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

    const SEARCH_HIT: &str = r#"{"records":[{"id":"mr-pal","namespace":"global","kind":"fact","title":"Gruvbox","summary":"the palette"}]}"#;

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
                        changed_paths: Vec::new(),
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
                        cancel_request: None,
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
                description: String::new(),
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
        let events = conversation_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert_eq!(result.content, "workspace file");
    }

    #[tokio::test]
    async fn an_unbound_session_can_use_a_workspace_tool() {
        let dir = TempDir::new().expect("temp dir");
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
                description: String::new(),
            },
        );
        let (engine, run) = reopened_engine_with_projects(&provider, &dir, registry, projects);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, None, "read it", tx)
            .await
            .expect("unbound workspace tool");

        assert!(!reply.partial);
        let requests = provider.requests();
        assert!(
            requests[0].tools.iter().any(|def| def.name == "ws_read"),
            "an unbound session has workspace tools: {:?}",
            requests[0].tools
        );
        let events = conversation_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert_eq!(result.content, "workspace file");
    }

    #[tokio::test]
    async fn a_session_naming_an_unconfigured_project_keeps_workspace_tools() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(&dir, vec![seeded_session_with_project("vanished")]);
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
                description: String::new(),
            },
        );
        let (engine, run) = reopened_engine_with_projects(&provider, &dir, registry, projects);
        let (tx, _rx) = channel();

        let reply = engine
            .send_message(&run, Some("s-01"), "read it", tx)
            .await
            .expect("project metadata is not a tool permission");

        assert!(!reply.partial);
        let events = conversation_log(dir.path());
        let result = resulted(&events[3]);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert_eq!(result.content, "workspace file");
    }

    #[tokio::test]
    async fn a_direct_session_holds_the_job_tools_and_a_dispatched_job_never_does() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        let provider = ScriptedProvider::scripted(vec![done_reply("ok"), done_reply("ok")]);
        let mut registry = Registry::new(512);
        for name in ["dispatch", "continue_job", "cancel_job"] {
            registry.register(Box::new(Canned {
                name,
                content: "",
                ok: true,
                source: ToolSource::Jobs,
            }));
        }
        let (engine, _) = engine_with_tools(&provider, &dir, registry);
        let engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));
        let run = runner_with_role(&provider, SessionRole::Executor);

        let direct = engine
            .create_direct_session(&run, "arc", SessionRole::Executor)
            .expect("direct session");
        let job = engine
            .create_bound_session(&run, "arc", SessionRole::Executor, None)
            .expect("job session");
        for session in [&direct, &job] {
            let (tx, _rx) = channel();
            engine
                .send_message(&run, Some(session), "hi", tx)
                .await
                .expect("send");
        }

        let requests = provider.requests();
        let names = |index: usize| -> Vec<String> {
            requests[index]
                .tools
                .iter()
                .map(|def| def.name.clone())
                .collect()
        };
        assert_eq!(names(0), ["cancel_job", "continue_job", "dispatch"]);
        assert!(
            names(1).is_empty(),
            "a job holds no job tools: {:?}",
            names(1)
        );
    }

    #[tokio::test]
    async fn a_chat_session_asks_the_provider_for_web_grounding() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![done_reply("ok")]);
        let (engine, run) = engine_with_role(&provider, &dir, SessionRole::Chat);
        let (tx, _rx) = channel();

        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        assert!(provider.requests()[0].web, "chat is a web capability");
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
                description: String::new(),
            },
        );
        projects
    }

    #[tokio::test]
    async fn create_bound_session_records_the_project_without_permissions() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let provider = ScriptedProvider::scripted(vec![]);
        let (mut engine, run) = engine_with_tools(&provider, &dir, Registry::new(512));
        engine = engine.with_projects(projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![Grant::new(&root, Mode::ReadWrite)],
        ));

        let session_id = engine
            .create_bound_session(&run, "arc", SessionRole::Chat, None)
            .expect("create a bound session");

        let events = conversation_log(dir.path());
        let session_event::Event::SessionCreated(created) = &events[0] else {
            panic!("expected SessionCreated first, got {:?}", events[0]);
        };
        assert_eq!(created.session_id, session_id);
        assert_eq!(created.project, "arc");
        assert_eq!(created.role, SessionRole::Chat as i32);
        assert_eq!(created.provider, "scripted");
        assert_eq!(created.model, "test-model");
        assert!(created.grants.is_empty());
    }

    #[tokio::test]
    async fn historical_code_sessions_replay_but_cannot_resume_or_fork() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![]);
        let (engine, _run) = engine_with_tools(&provider, &dir, Registry::new(512));
        let code = "historical-code".to_owned();
        engine
            .record(
                Source::User,
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: code.clone(),
                    role: crate::provider::LEGACY_DIRECT_ROLE,
                    provider: "scripted".to_owned(),
                    model: "test-model".to_owned(),
                    ..Default::default()
                }),
            )
            .expect("record historical session");
        drop(engine);
        let (reopened, run) = reopened_engine(&provider, &dir, Registry::new(512));
        let summary = reopened
            .with_store(|store| store.projection().sessions())
            .expect("replay");
        assert_eq!(summary[0].role, crate::provider::LEGACY_DIRECT_ROLE);
        assert!(matches!(
            reopened.session_role(&code),
            Err(super::Error::HistoricalSession { .. })
        ));
        let (tx, _rx) = channel();
        let before = conversation_log(dir.path()).len();
        assert_eq!(
            reopened
                .send_message(&run, Some(&code), "continue", tx)
                .await
                .unwrap_err()
                .to_string(),
            format!("session {code} uses the retired code role; start a new assistant session")
        );
        assert!(matches!(
            reopened.fork_session(&code, 0),
            Err(super::Error::HistoricalSession { .. })
        ));
        assert_eq!(
            conversation_log(dir.path()).len(),
            before,
            "refused operations append nothing"
        );
    }

    #[tokio::test]
    async fn explicit_fork_choice_records_its_own_pin_without_changing_the_default() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let choices = ["first", "second"]
            .into_iter()
            .map(|name| super::ModelChoice {
                name: name.to_owned(),
                provider: "scripted".to_owned(),
                model: name.to_owned(),
                thinking: Thinking::Default,
                editing: crate::tool::Editing::Replacement,
            })
            .collect();
        let engine = engine.with_role_choices(BTreeMap::from([(SessionRole::Chat, choices)]));
        let parent = engine.create_session(&run).expect("parent");
        let fork_point = engine
            .record(
                Source::User,
                session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
                    session_id: parent.clone(),
                    role: Role::User as i32,
                    content: "context".to_owned(),
                    ..Default::default()
                }),
            )
            .expect("message");
        let fork = engine
            .fork_session_with_choice(&parent, fork_point, "second")
            .expect("fork");
        assert_eq!(
            engine
                .selected_choice(SessionRole::Chat)
                .unwrap()
                .as_deref(),
            Some("first")
        );
        assert_eq!(
            engine.session_choice(&fork).unwrap().as_deref(),
            Some("second")
        );
        assert_eq!(
            engine.session_identity(&fork).unwrap(),
            Some(("scripted".to_owned(), "second".to_owned()))
        );
        let err = engine
            .fork_session_with_choice(&parent, fork_point, "missing")
            .expect_err("unknown choice");
        assert!(matches!(err, Error::UnknownChoice { .. }));
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
            .create_bound_session(&run, "arc", SessionRole::Chat, None)
            .expect_err("a missing root must fail at creation");

        assert!(matches!(err, Error::Grants { ref project, .. } if project == "arc"));
        assert_eq!(
            conversation_log(dir.path()).len(),
            0,
            "nothing was appended"
        );
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
            .expect("create a direct session");

        let events = conversation_log(dir.path());
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
            "a direct session is not a dispatched job"
        );
        assert!(created.grants.is_empty());

        let raw_events = replay_events(dir.path());
        assert_eq!(
            raw_events[0].source,
            Source::User as i32,
            "the user asked for this, not a model"
        );
    }

    #[tokio::test]
    async fn fork_session_records_the_roles_current_identity_and_project() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let creating_provider = ScriptedProvider::scripted(vec![]);
        let (creating_engine, _run) =
            engine_with_tools(&creating_provider, &dir, Registry::new(512));
        let creating_engine = creating_engine
            .with_projects(projects_with(
                "arc",
                vec![ToolSource::Builtin, ToolSource::Workspace],
                vec![Grant::new(&root, Mode::ReadWrite)],
            ))
            .with_role_identities(BTreeMap::from([(
                SessionRole::Executor,
                ("codex-legacy".to_owned(), "gpt-5-legacy".to_owned()),
            )]));
        let creating_run = runner_with_role(&creating_provider, SessionRole::Executor);
        let parent_id = creating_engine
            .create_bound_session(&creating_run, "arc", SessionRole::Executor, None)
            .expect("create a bound session");
        let fork_point = creating_engine
            .record(
                Source::User,
                session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
                    session_id: parent_id.clone(),
                    role: Role::User as i32,
                    content: "hi".to_owned(),
                    ..Default::default()
                }),
            )
            .expect("message");
        drop(creating_engine);

        // reconfigured before the fork: a changed root and a different executor identity
        let changed_root = TempDir::new().expect("temp dir 2");
        let provider = ScriptedProvider::scripted(vec![]);
        let projects = projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![Grant::new(changed_root.path(), Mode::ReadWrite)],
        );
        let (engine, _run) =
            reopened_engine_with_projects(&provider, &dir, Registry::new(512), projects);
        let engine = engine.with_role_identities(BTreeMap::from([(
            SessionRole::Executor,
            ("codex-new".to_owned(), "gpt-6-new".to_owned()),
        )]));

        let fork_id = engine
            .fork_session(&parent_id, fork_point)
            .expect("fork_session");

        assert_ne!(fork_id, parent_id);
        let events = conversation_log(dir.path());
        let session_event::Event::SessionCreated(created) = events.last().expect("an event") else {
            panic!(
                "expected the fork's SessionCreated last, got {:?}",
                events.last()
            );
        };
        assert_eq!(created.session_id, fork_id);
        assert_eq!(created.parent_session, parent_id);
        assert_eq!(created.fork_point, fork_point);
        assert_eq!(created.role, SessionRole::Executor as i32);
        assert_eq!(created.project, "arc");
        assert_eq!(
            (created.provider.as_str(), created.model.as_str()),
            ("codex-new", "gpt-6-new"),
            "the role's current identity, so the fork runs under today's model"
        );
        assert!(created.grants.is_empty());
        assert_eq!(created.dispatched_by, "");
        assert_eq!(created.budget, None);

        let raw_events = replay_events(dir.path());
        assert_eq!(
            raw_events.last().expect("an event").source,
            Source::User as i32,
            "a person forked, not a model"
        );
    }

    #[tokio::test]
    async fn fork_session_rejects_a_foreign_sessions_seq() {
        let provider = ScriptedProvider::scripted(vec![done_reply("a"), done_reply("b")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let first = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");
        let (tx, _rx) = channel();
        let second = engine
            .send_message(&run, None, "yo", tx)
            .await
            .expect("send");

        let err = engine
            .fork_session(&first.session_id, second.seq)
            .expect_err("a foreign session's seq must be refused");

        assert!(matches!(err, Error::InvalidForkPoint { .. }));
    }

    #[tokio::test]
    async fn a_fork_at_an_inherited_seq_forks_the_ancestor_that_owns_it() {
        let provider = ScriptedProvider::scripted(vec![
            done_reply("one"),
            done_reply("two"),
            done_reply("three"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);

        let (tx, _rx) = channel();
        let first = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");
        let root = first.session_id.clone();
        let root_user_seq = first.seq - 2;

        let branch = engine
            .fork_session(&root, first.seq)
            .expect("fork at the root's reply");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&branch), "branch turn", tx)
            .await
            .expect("send on the branch");

        // rewinding past the fork boundary: the selected message belongs to
        // the root, so the new branch's parent is the root, not the branch
        let rewound = engine
            .fork_session(&branch, root_user_seq)
            .expect("a rewind through the fork boundary forks the owner");
        let parent = engine
            .with_store(|store| {
                store
                    .projection()
                    .ancestors(&rewound)
                    .map(|chain| chain.first().cloned())
            })
            .expect("ancestors")
            .expect("a parent");
        assert_eq!(
            parent, root,
            "the inherited seq's owner became the recorded parent"
        );
    }

    #[tokio::test]
    async fn a_turn_on_a_forked_session_sends_the_parents_prefix_transcript() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![
            done_reply("first answer"),
            done_reply("second answer"),
            done_reply("branch answer"),
        ]);
        let (engine, run) = engine(&provider, &dir);

        let (tx, _rx) = channel();
        let first = engine
            .send_message(&run, None, "question one", tx)
            .await
            .expect("send");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&first.session_id), "question two", tx)
            .await
            .expect("send");

        // forked right after the first exchange: the branch must not see "question two"
        let fork_id = engine
            .fork_session(&first.session_id, first.seq)
            .expect("fork_session");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&fork_id), "branch question", tx)
            .await
            .expect("send");

        let requests = provider.requests();
        let turns: Vec<(Role, &str)> = requests[2].messages.iter().map(turn).collect();
        assert_eq!(
            turns,
            [
                (Role::User, "question one"),
                (Role::Assistant, "first answer"),
                (Role::User, "branch question"),
            ],
            "the branch inherits the parent prefix through its fork point, not what came after"
        );
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

        let events = conversation_log(dir.path());
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
        assert!(child.grants.is_empty());
        let child_id = child.session_id.clone();

        let result = resulted(&events[4]);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert!(result.content.contains(&child_id), "{}", result.content);
        assert!(result.content.contains("executor"), "{}", result.content);
        assert!(result.content.contains("arc"), "{}", result.content);
        assert!(
            result
                .content
                .contains("arrive automatically as a handback")
        );
        assert!(!result.content.contains("End this reply"));
        assert!(!result.content.contains("wait"));
        assert!(result.content.contains("(implement)"), "{}", result.content);

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
            compact_at: None,
            context_window: None,
            editing: crate::tool::Editing::Replacement,
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
    async fn an_analyze_dispatch_is_not_a_permission_mode() {
        let dir = TempDir::new_in(env!("CARGO_MANIFEST_DIR")).expect("temp dir outside /tmp");
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

        let events = conversation_log(dir.path());
        let session_event::Event::SessionCreated(child) = &events[3] else {
            panic!("expected the child SessionCreated, got {:?}", events[3]);
        };
        assert!(child.grants.is_empty());

        let result = resulted(&events[4]);
        assert!(result.content.contains("(analyze)"), "{}", result.content);
    }

    #[tokio::test]
    async fn a_dispatch_into_a_project_with_a_finished_job_creates_a_new_child() {
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
            vec![
                Ok(call(
                    "c2",
                    0,
                    "dispatch",
                    &dispatch_args("executor", "arc", "now check the docs", "implement"),
                )),
                Ok(tool_stop()),
            ],
            done_reply("second try"),
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
        let child_id = reply.jobs[0].session_id.clone();
        record_handback(
            &engine,
            &reply.session_id,
            &child_id,
            None,
            "fixed it",
            None,
        );

        let (tx, _rx) = channel();
        let second = engine
            .send_message(&run, Some(&reply.session_id), "more work", tx)
            .await
            .expect("a second dispatch creates a new child");

        assert_eq!(second.jobs.len(), 1);
        assert_ne!(second.jobs[0].session_id, child_id);
        assert_eq!(second.jobs[0].parent_session, reply.session_id);
        assert_eq!(second.jobs[0].brief, "now check the docs");
        let events = conversation_log(dir.path());
        let result = events
            .iter()
            .find_map(|event| match event {
                session_event::Event::ToolResultRecorded(result) if result.call_id == "c2" => {
                    Some(result)
                }
                _ => None,
            })
            .expect("second dispatch result");
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert!(result.content.contains(&second.jobs[0].session_id));
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

        let events = conversation_log(dir.path());
        let result = tool_result(&events);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert!(result.content.contains("Continuing"), "{}", result.content);
        assert!(result.content.contains(&child_id), "{}", result.content);
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
        let other_chat = engine
            .create_bound_session(&bootstrap_run, "arc", SessionRole::Chat, None)
            .expect("create a non-job session durably");

        let provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c1",
                    0,
                    "continue_job",
                    &continue_job_args(&other_chat, "keep going"),
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
        let events = conversation_log(dir.path());
        let result = tool_result(&events);
        assert_eq!(result.outcome, ToolOutcome::Error as i32);
        assert!(result.content.contains("assistant"), "{}", result.content);
        assert!(result.content.contains("not a job"), "{}", result.content);
    }

    fn session_recorded_on(
        id: &str,
        role: SessionRole,
        provider: &str,
        model: &str,
    ) -> session_event::Event {
        session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
            session_id: id.to_owned(),
            parent_session: String::new(),
            fork_point: 0,
            title: String::new(),
            provider: provider.to_owned(),
            model: model.to_owned(),
            role: role as i32,
            project: String::new(),
            budget: None,
            grants: Vec::new(),
            dispatched_by: String::new(),
            choice: String::new(),
            editing: String::new(),
            working_directory: String::new(),
            thinking: String::new(),
        })
    }

    #[tokio::test]
    async fn continue_job_keeps_its_recorded_model_when_the_role_default_changes() {
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
            .with_role_choices(BTreeMap::from([(
                SessionRole::Executor,
                ["model-a", "model-b"]
                    .into_iter()
                    .map(|name| super::ModelChoice {
                        name: name.to_owned(),
                        provider: "scripted".to_owned(),
                        model: name.to_owned(),
                        thinking: Thinking::Default,
                        editing: crate::tool::Editing::Replacement,
                    })
                    .collect(),
            )]));
        let child_id = engine
            .create_bound_session(&bootstrap_run, "arc", SessionRole::Executor, None)
            .expect("create the child durably, recorded on model-a");

        engine
            .select_model(SessionRole::Executor, "model-b")
            .expect("change default, keep the old preset configured");

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
            .expect("the continue request completes");

        assert!(
            reply.continues.len() == 1,
            "the session's recorded model, not the changed default, governs the resume"
        );
        let events = conversation_log(dir.path());
        let result = tool_result(&events);
        assert_eq!(result.outcome, ToolOutcome::Ok as i32);
        assert!(
            result.content.contains("Continuing job"),
            "{}",
            result.content
        );
    }

    #[tokio::test]
    async fn a_pinned_session_refuses_a_different_model_but_resumes_on_its_own() {
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
            model: "model-b".to_owned(),
            thinking: Thinking::Default,
            system: None,
            compact_at: None,
            context_window: None,
            editing: crate::tool::Editing::Replacement,
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
        assert_eq!(conversation_log(dir.path()).len(), 1);

        let matching_run = Runner {
            model: "model-a".to_owned(),
            ..executor_run
        };
        let (tx, _rx) = channel();
        engine
            .send_message(&matching_run, Some("s-01"), "resume", tx)
            .await
            .expect("the recorded identity resumes");
        assert_eq!(provider.requests().len(), 1);
    }

    #[tokio::test]
    async fn a_session_recorded_before_model_stamping_stays_resumable() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![session_recorded_on("s-01", SessionRole::Executor, "", "")],
        );
        let provider = ScriptedProvider::scripted(vec![done_reply("hi")]);
        let (engine, _) = reopened_engine(&provider, &dir, Registry::new(512));
        let executor_run = Runner {
            role: SessionRole::Executor,
            provider: Arc::clone(&provider) as Arc<dyn Provider>,
            model: "model-b".to_owned(),
            thinking: Thinking::Default,
            system: None,
            compact_at: None,
            context_window: None,
            editing: crate::tool::Editing::Replacement,
        };
        let (tx, _rx) = channel();

        engine
            .send_message(&executor_run, Some("s-01"), "resume", tx)
            .await
            .expect("a session logged before provider/model stamping stays unpinned");
    }

    #[tokio::test]
    async fn a_handback_truncates_a_long_summary_on_a_char_boundary() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hi")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        // two-byte characters straddle the 16 KiB cap, exercising the
        // char-boundary walk-back as well as the cap itself
        let long_summary = "é".repeat(9000);
        record_handback(
            &engine,
            &reply.session_id,
            "child-1",
            None,
            &long_summary,
            None,
        );

        let entries = engine.transcript(&reply.session_id).expect("transcript");
        let content = match &entries.last().expect("an entry").entry {
            Some(history_entry::Entry::Message(HistoryMessage { content, .. })) => content.clone(),
            other => panic!("expected a message entry, got {other:?}"),
        };
        assert!(
            content.contains(" [truncated; use session_read for session child-1 to read the rest]"),
            "{content}"
        );
        assert!(content.len() < long_summary.len());
    }

    fn record_handback(
        engine: &Engine,
        parent_session: &str,
        child_session: &str,
        reason: Option<&str>,
        summary: &str,
        footprint: Option<&str>,
    ) {
        let content = engine
            .compose_handback(child_session, reason, summary, footprint)
            .expect("compose the handback");
        engine
            .append_message(parent_session, &content, Source::System)
            .expect("append the handback");
    }

    async fn wait_for_event_count(dir: &std::path::Path, want: usize) {
        for _ in 0..400 {
            if conversation_log(dir).len() >= want {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for {want} events in {}", dir.display());
    }

    #[tokio::test]
    async fn continue_session_runs_a_scripted_turn_over_the_existing_transcript_without_a_user_message()
     {
        let provider =
            ScriptedProvider::scripted(vec![done_reply("first"), done_reply("the chat reacts")]);
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

        let events = conversation_log(dir.path());
        assert_eq!(
            events.len(),
            4,
            "SessionCreated, the user message, the first reply, then only the continued reply"
        );
        let last = appended(&events[3]);
        assert_eq!(last.role, Role::Assistant as i32);
        assert_eq!(last.content, "the chat reacts");

        assert_eq!(
            ignoring_elapsed(engine.transcript(&reply.session_id).expect("transcript")),
            [
                prose_entry(Role::User as i32, "hi", false),
                assistant_entry_with_usage("first", 3, 5),
                assistant_entry_with_usage("the chat reacts", 3, 5),
            ],
            "no user message was appended for the handback turn"
        );

        assert_eq!(
            drain(&mut rx),
            [
                EngineEvent::Accepted {
                    session_id: reply.session_id.clone()
                },
                EngineEvent::Delta("the chat reacts".to_owned()),
            ]
        );
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
            Step::Immediate(done_reply("the chat reacts")),
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
            conversation_log(dir.path()).len(),
            2,
            "continue_session stays blocked while the user turn holds the guard"
        );

        notify.notify_one();
        let sent = turn.await.expect("turn task");
        let continued = continue_handle.await.expect("continue_session task");

        let events = conversation_log(dir.path());
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
        assert_eq!(continued_msg.content, "the chat reacts");
        assert_eq!(sent.session_id, "s-01");
        assert_eq!(continued.session_id, "s-01");
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

    fn compacted_event(
        events: &[session_event::Event],
    ) -> Option<&arc_proto::v1::SessionCompacted> {
        events.iter().find_map(|event| match event {
            session_event::Event::SessionCompacted(compacted) => Some(compacted),
            _ => None,
        })
    }

    async fn hi_then_more(engine: &Engine, run: &Runner) -> String {
        let (tx, _rx) = channel();
        let reply = engine.send_message(run, None, "hi", tx).await.unwrap();
        let (tx, _rx) = channel();
        engine
            .send_message(run, Some(&reply.session_id), "more", tx)
            .await
            .unwrap();
        reply.session_id
    }

    #[tokio::test]
    async fn a_fork_with_oversized_user_history_compacts_and_replays() {
        let provider = ScriptedProvider::scripted(vec![
            done_reply("root 1"),
            done_reply("root 2"),
            done_reply("root 3"),
            done_reply("root 4"),
            done_reply("branch 1"),
            done_reply("branch 2"),
            done_reply(
                "Goal\nOlder requests and decisions summarized.\nDone so far\nOpen\nFacts to keep",
            ),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let mut root = String::new();
        let mut fork_point = 0;
        for index in 0..4 {
            let (tx, _rx) = channel();
            let reply = engine
                .send_message(
                    &run,
                    (!root.is_empty()).then_some(root.as_str()),
                    &format!("old request {index}: {}", "x".repeat(24 * 1024)),
                    tx,
                )
                .await
                .expect("root turn");
            root = reply.session_id;
            fork_point = reply.seq;
        }
        let branch = engine.fork_session(&root, fork_point).expect("fork");
        for message in ["keep this", "and this"] {
            let (tx, _rx) = channel();
            engine
                .send_message(&run, Some(&branch), message, tx)
                .await
                .expect("branch turn");
        }
        assert!(
            engine.compact(&run, &branch, "manual").await.unwrap(),
            "large inherited user history no longer blocks compaction"
        );
        assert_eq!(provider.requests().len(), 7);

        let logged = replay_events(dir.path());
        let compacted = logged
            .iter()
            .find_map(|event| match &event.payload {
                Some(arc_proto::v1::event::Payload::Session(session)) => {
                    match session.event.as_ref() {
                        Some(session_event::Event::SessionCompacted(compacted)) => Some(compacted),
                        _ => None,
                    }
                }
                _ => None,
            })
            .expect("compaction event");
        assert_eq!(compacted.prompt_version, super::COMPACTION_PROMPT_VERSION);
        assert!(compacted.summary.contains("keep this"));
        assert!(
            !compacted.summary.contains("old request 0:"),
            "older user text is summarized rather than copied"
        );
        assert!(compacted.summary.len() <= super::MAX_COMPACTION_SUMMARY_BYTES);
        let mut replay = Projection::in_memory().expect("projection");
        for event in logged {
            replay.apply(&event).expect("replay");
        }
        assert_eq!(
            replay.lineage_messages(&branch).unwrap(),
            engine
                .with_store(|store| store.projection().lineage_messages(&branch))
                .unwrap()
        );
    }

    #[tokio::test]
    async fn one_user_request_compacts_twice_without_losing_instructions_or_tool_pairs() {
        const LIMIT: u32 = 42;
        let mut script = Vec::new();
        for step in 0..5 {
            script.push(vec![
                Ok(call(&format!("c{step}-a"), 0, "lookup", "{}")),
                Ok(call(&format!("c{step}-b"), 1, "lookup", "{}")),
                Ok(CompletionDelta::Done {
                    usage: Usage {
                        input_tokens: if step == 2 || step == 4 { LIMIT } else { 1 },
                        output_tokens: 5,
                    },
                    stop: Stop::ToolCalls,
                }),
            ]);
            if step == 2 || step == 4 {
                script.push(done_reply("Goal\nContinue the work."));
            }
        }
        script.push(done_reply("finished"));
        let provider = ScriptedProvider::scripted(script);
        let dir = TempDir::new().expect("temp dir");
        let (engine, _) =
            engine_with_tools(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let run = Runner {
            compact_at: Some(LIMIT),
            context_window: None,
            ..runner_with_role(&provider, SessionRole::Executor)
        };
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "Keep the public API unchanged.", tx)
            .await
            .expect("long turn");
        let events = conversation_log(dir.path());
        let compactions: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                session_event::Event::SessionCompacted(event) => Some(event),
                _ => None,
            })
            .collect();
        assert_eq!(compactions.len(), 2);
        assert!(compactions[0].through_seq < compactions[1].through_seq);
        for compacted in &compactions {
            assert_eq!(
                compacted
                    .summary
                    .matches("Keep the public API unchanged.")
                    .count(),
                1
            );
        }
        let requests = provider.requests();
        for request in &requests {
            let mut open = std::collections::HashSet::new();
            for message in &request.messages {
                match message {
                    Message::ToolCalls { calls, .. } => {
                        open.extend(calls.iter().map(|call| call.id.as_str()));
                    }
                    Message::ToolResult { call_id, .. } => {
                        assert!(open.remove(call_id.as_str()), "result without call");
                    }
                    Message::Text { .. } | Message::UserWithAttachments { .. } => {}
                }
            }
            assert!(open.is_empty(), "call without result");
        }
        let mut rebuilt = Projection::in_memory().expect("projection");
        for event in replay_events(dir.path()) {
            rebuilt.apply(&event).expect("replay");
        }
        let actual = rebuilt
            .lineage_messages(&reply.session_id)
            .expect("lineage");
        let expected = engine
            .with_store(|store| store.projection().lineage_messages(&reply.session_id))
            .expect("live lineage");
        assert_eq!(actual, expected);
        assert_eq!(requests.len(), 8);
    }

    #[tokio::test]
    async fn a_failed_compaction_repairs_once_then_surfaces_an_error() {
        const LIMIT: u32 = 42;
        let provider = ScriptedProvider::scripted(vec![
            done_reply("reply1"),
            done_reply("reply2"),
            vec![
                Ok(call("c1", 0, "lookup", "{}")),
                Ok(CompletionDelta::Done {
                    usage: Usage {
                        input_tokens: LIMIT,
                        output_tokens: 5,
                    },
                    stop: Stop::ToolCalls,
                }),
            ],
            done_reply(&"x".repeat(super::MAX_COMPACTION_SUMMARY_BYTES + 1)),
            done_reply(&"x".repeat(super::MAX_COMPACTION_SUMMARY_BYTES + 1)),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, _) =
            engine_with_tools(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let run = Runner {
            compact_at: Some(LIMIT),
            context_window: None,
            ..runner_with_role(&provider, SessionRole::Executor)
        };

        let session_id = hi_then_more(&engine, &run).await;
        let (tx, mut rx) = channel();
        let error = engine
            .send_message(&run, Some(&session_id), "third question", tx)
            .await
            .expect_err("the turn stops rather than retrying an expensive prefix");
        drain(&mut rx);

        assert!(matches!(error, Error::CompactionFailed { .. }));
        let requests = provider.requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(
            requests[4].system.as_deref(),
            Some(super::COMPACTION_REPAIR_PROMPT_V2)
        );
        assert_eq!(requests[4].messages.len(), 1);
        let (_, repair_input) = turn(&requests[4].messages[0]);
        assert!(
            repair_input.len() < super::MAX_COMPACTION_SUMMARY_BYTES + 256,
            "the repair request never repeats the history or an unbounded draft"
        );
        assert!(
            compacted_event(&conversation_log(dir.path())).is_none(),
            "an invalid summary writes nothing to the log"
        );
    }

    #[tokio::test]
    async fn archivist_repairs_compaction_without_changing_the_session_model() {
        const LIMIT: u32 = 42;
        let provider = ScriptedProvider::scripted(vec![
            done_reply("reply1"),
            done_reply("reply2"),
            vec![
                Ok(call("c1", 0, "lookup", "{}")),
                Ok(CompletionDelta::Done {
                    usage: Usage {
                        input_tokens: LIMIT,
                        output_tokens: 5,
                    },
                    stop: Stop::ToolCalls,
                }),
            ],
            done_reply("final answer"),
        ]);
        let archivist = ScriptedProvider::scripted(vec![
            done_reply(&"x".repeat(super::MAX_COMPACTION_SUMMARY_BYTES + 1)),
            done_reply("Keep working on the open tasks."),
        ]);
        let unused = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, _) =
            engine_with_tools(&provider, &dir, tools(&[("lookup", "found it", true)]));
        let engine = engine
            .with_role_choices(BTreeMap::from([(
                SessionRole::Archivist,
                vec![
                    super::ModelChoice {
                        name: "unused".to_owned(),
                        provider: unused.name().to_owned(),
                        model: "unused-model".to_owned(),
                        thinking: Thinking::Default,
                        editing: crate::tool::Editing::Replacement,
                    },
                    super::ModelChoice {
                        name: "cheap".to_owned(),
                        provider: archivist.name().to_owned(),
                        model: "archivist-model".to_owned(),
                        thinking: Thinking::Low,
                        editing: crate::tool::Editing::Replacement,
                    },
                ],
            )]))
            .with_compaction_runners(vec![
                (
                    "unused".to_owned(),
                    runner_with_role(&unused, SessionRole::Archivist),
                ),
                (
                    "cheap".to_owned(),
                    Runner {
                        model: "archivist-model".to_owned(),
                        thinking: Thinking::Low,
                        ..runner_with_role(&archivist, SessionRole::Archivist)
                    },
                ),
            ]);
        engine
            .select_model(SessionRole::Archivist, "cheap")
            .expect("selected archivist is used at compaction time");
        let run = Runner {
            compact_at: Some(LIMIT),
            ..runner_with_role(&provider, SessionRole::Executor)
        };
        let session_id = hi_then_more(&engine, &run).await;
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&session_id), "third", tx)
            .await
            .expect("repair succeeded");

        assert_eq!(provider.requests().len(), 4, "only turns use executor");
        assert!(unused.requests().is_empty());
        assert_eq!(archivist.requests().len(), 2);
        let repair = &archivist.requests()[1];
        assert_eq!(repair.role, SessionRole::Archivist);
        assert_eq!(repair.model, "archivist-model");
        assert_eq!(repair.messages.len(), 1, "repair does not resend history");
        let compacted = compacted_event(&conversation_log(dir.path()))
            .expect("repaired compaction is durable")
            .clone();
        assert_eq!(compacted.model, "archivist-model");
        assert!(compacted.summary.contains("hi"));
        let (provider_name, model) = engine.session_identity(&session_id).unwrap().unwrap();
        assert_eq!(provider_name, provider.name());
        assert_eq!(model, "test-model");
    }

    #[tokio::test]
    async fn editing_interface_is_pinned_and_filters_both_schemas_and_dispatch() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("project");
        std::fs::create_dir(&root).unwrap();
        let provider = ScriptedProvider::scripted(vec![]);
        let mut registry = Registry::new(512);
        for tool in workspace::tools(Arc::new(Workspace::new())) {
            registry.register(tool);
        }
        let projects = projects_with(
            "arc",
            vec![ToolSource::Builtin, ToolSource::Workspace],
            vec![Grant::new(&root, Mode::ReadWrite)],
        );
        let (engine, mut run) = engine_with_tools(&provider, &dir, registry);
        let engine = engine.with_projects(projects.clone());
        run.editing = crate::tool::Editing::Patch;
        let session = engine
            .create_bound_session(&run, "arc", SessionRole::Chat, None)
            .unwrap();
        let patch_sources = engine.tool_setup(&session, false, &run).unwrap().0;
        let names = engine
            .registry
            .definitions(&patch_sources)
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        assert!(names.contains(&"bash".to_owned()));
        assert!(names.contains(&"read".to_owned()));
        assert!(names.contains(&"apply_patch".to_owned()));
        assert!(!names.contains(&"edit".to_owned()));
        assert!(!names.contains(&"write".to_owned()));
        let denied = engine
            .registry
            .dispatch(
                "write",
                serde_json::json!({"path": root.join("not-created"), "content": "no"}).to_string(),
                TurnContext::default(),
                &patch_sources,
            )
            .await;
        assert!(!denied.ok && denied.content.contains("not available"));
        assert!(!root.join("not-created").exists());
        let applied = engine
            .registry
            .dispatch(
                "apply_patch",
                serde_json::json!({"input": format!(
                    "*** Begin Patch\n*** Add File: {}\n+patch\n*** End Patch",
                    root.join("patched").display()
                )})
                .to_string(),
                TurnContext {
                    session_id: session.clone(),
                    grants: engine.grants(&session, false).unwrap(),
                    ..Default::default()
                },
                &patch_sources,
            )
            .await;
        assert!(applied.ok, "{}", applied.content);
        assert_eq!(
            std::fs::read_to_string(root.join("patched")).unwrap(),
            "patch\n"
        );
        drop(engine);

        let mut registry = Registry::new(512);
        for tool in workspace::tools(Arc::new(Workspace::new())) {
            registry.register(tool);
        }
        let (reopened, mut changed_default) = reopened_engine(&provider, &dir, registry);
        changed_default.editing = crate::tool::Editing::Replacement;
        let reopened = reopened.with_projects(projects);
        assert_eq!(
            reopened
                .with_store(|store| store.projection().session_editing(&session))
                .unwrap(),
            Some("patch".to_owned())
        );
        let resumed = reopened
            .tool_setup(&session, false, &changed_default)
            .unwrap()
            .0;
        assert!(resumed.contains(&ToolSource::Patch));
        assert!(!resumed.contains(&ToolSource::Replacement));

        let legacy = reopened
            .create_bound_session(&changed_default, "arc", SessionRole::Chat, None)
            .unwrap();
        let replacement = reopened
            .tool_setup(&legacy, false, &changed_default)
            .unwrap()
            .0;
        assert!(replacement.contains(&ToolSource::Replacement));
        assert!(!replacement.contains(&ToolSource::Patch));
        let names = reopened
            .registry
            .definitions(&replacement)
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        assert!(names.contains(&"read".to_owned()));
        assert!(names.contains(&"bash".to_owned()));
        assert!(names.contains(&"write".to_owned()));
        assert!(names.contains(&"edit".to_owned()));
        assert!(!names.contains(&"apply_patch".to_owned()));
        let denied = reopened
            .registry
            .dispatch(
                "apply_patch",
                "{}".to_owned(),
                TurnContext::default(),
                &replacement,
            )
            .await;
        assert!(!denied.ok && denied.content.contains("not available"));
        let written = reopened
            .registry
            .dispatch(
                "write",
                serde_json::json!({"path": root.join("written"), "content": "replacement"})
                    .to_string(),
                TurnContext {
                    session_id: legacy.clone(),
                    grants: reopened.grants(&legacy, false).unwrap(),
                    ..Default::default()
                },
                &replacement,
            )
            .await;
        assert!(written.ok, "{}", written.content);
        assert_eq!(
            std::fs::read_to_string(root.join("written")).unwrap(),
            "replacement"
        );

        reopened
            .record(
                Source::User,
                session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
                    session_id: "unpinned-editing".to_owned(),
                    provider: "codex".to_owned(),
                    model: "older".to_owned(),
                    role: SessionRole::Chat as i32,
                    project: "arc".to_owned(),
                    ..Default::default()
                }),
            )
            .unwrap();
        let old_sources = reopened
            .tool_setup("unpinned-editing", false, &changed_default)
            .unwrap()
            .0;
        assert!(old_sources.contains(&ToolSource::Patch));
        assert!(!old_sources.contains(&ToolSource::Replacement));
    }
}
