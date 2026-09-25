mod handback;
pub(crate) mod prompt;
mod status;
#[cfg(test)]
mod tests_common;
mod turn;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_core::provider::role_label;
use arc_core::session::{
    ContinuedJob, DispatchedJob, Engine, EngineEvent, Error as SessionError, Inbound, Reply, Runner,
};
use arc_proto::v1::{
    ImageAttachment, JobInfo, Notification, ProjectInfo, SessionRole, Source, job_info,
};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{info, warn};

use handback::{Autonomy, handback_crashed, job_title, record_handback};
use status::{JobStatuses, notify_job_changed};
use turn::{EVENT_BUFFER, Task, run_task};

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct Project {
    pub root: PathBuf,
    pub command_prefix: Vec<String>,
}

impl From<PathBuf> for Project {
    fn from(root: PathBuf) -> Self {
        Self {
            root,
            command_prefix: Vec::new(),
        }
    }
}

struct LiveSession {
    inbox: mpsc::UnboundedSender<Inbound>,
    cancel: watch::Sender<bool>,
    drop_tx: mpsc::UnboundedSender<()>,
}

type LiveMap = Mutex<HashMap<String, LiveSession>>;
type Handles = Mutex<Vec<JoinHandle<()>>>;

pub enum TurnEvent {
    Engine(EngineEvent),
    Ended(Result<Reply, SessionError>),
}

pub enum SendOutcome {
    Started {
        session_id: String,
        events: Option<mpsc::Receiver<TurnEvent>>,
    },
    Queued {
        session_id: String,
    },
}

#[derive(Clone)]
pub(crate) struct Shared {
    engine: Arc<Engine>,
    // every configured choice per role; the engine's recorded selection picks
    menus: BTreeMap<SessionRole, Vec<(String, Runner)>>,
    projects: BTreeMap<String, Project>,
    identity: Option<String>,
    live: Arc<LiveMap>,
    statuses: Arc<JobStatuses>,
    notifier: Option<broadcast::Sender<Notification>>,
    handles: Arc<Handles>,
    autonomy: Arc<Autonomy>,
}

pub struct Supervisor {
    shared: Shared,
    project_list: Vec<ProjectInfo>,
}

impl Supervisor {
    pub fn new(engine: Arc<Engine>, menus: BTreeMap<SessionRole, Vec<(String, Runner)>>) -> Self {
        Self {
            shared: Shared {
                engine,
                menus,
                projects: BTreeMap::new(),
                identity: None,
                live: Arc::new(Mutex::new(HashMap::new())),
                statuses: Arc::new(JobStatuses::new()),
                notifier: None,
                handles: Arc::new(Mutex::new(Vec::new())),
                autonomy: Arc::new(Autonomy::new()),
            },
            project_list: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_projects(mut self, projects: BTreeMap<String, Project>) -> Self {
        self.shared.projects = projects;
        self
    }

    #[must_use]
    pub fn with_notifier(mut self, notifier: broadcast::Sender<Notification>) -> Self {
        self.shared.notifier = Some(notifier);
        self
    }

    #[cfg(test)]
    pub fn with_chat(mut self, runner: Runner) -> Self {
        self.shared
            .menus
            .insert(SessionRole::Chat, vec![(runner.model.clone(), runner)]);
        self
    }

    #[cfg(test)]
    pub fn for_test(engine: Arc<Engine>, runners: BTreeMap<SessionRole, Runner>) -> Self {
        Self::new(
            engine,
            runners
                .into_iter()
                .map(|(role, runner)| (role, vec![(runner.model.clone(), runner)]))
                .collect(),
        )
    }

    #[must_use]
    pub fn with_identity(mut self, identity: Option<String>) -> Self {
        self.shared.identity = identity;
        self
    }

    #[must_use]
    pub fn with_project_list(mut self, projects: Vec<ProjectInfo>) -> Self {
        self.project_list = projects;
        self
    }

    pub fn send(
        &self,
        session_id: Option<&str>,
        content: &str,
        source: Source,
        attachments: Vec<ImageAttachment>,
        attach: bool,
    ) -> Result<SendOutcome, SessionError> {
        send_into(
            &self.shared,
            session_id,
            content,
            source,
            attachments,
            attach,
        )
    }

    #[cfg(test)]
    pub fn spawn(&self, job: DispatchedJob) {
        spawn_job(&self.shared, job, 0);
    }

    #[cfg(test)]
    pub fn continue_job(&self, cont: ContinuedJob) {
        route_continue(&self.shared, cont);
    }

    pub fn list(&self) -> Vec<JobInfo> {
        let mut jobs = self.shared.statuses.list();
        for job in &mut jobs {
            job.title = job_title(&self.shared.engine, &job.session_id);
        }
        jobs
    }

    pub fn cancel(&self, session_id: &str) -> bool {
        cancel_live(&self.shared.live, session_id)
    }

    pub fn drop_steers(&self, session_id: &str) -> bool {
        let live = self.shared.live.lock().expect("live");
        let Some(session) = live.get(session_id) else {
            return false;
        };
        let _ = session.drop_tx.send(());
        true
    }

    pub(crate) fn turn_runner(&self, session_id: &str) -> Result<Runner, SessionError> {
        turn_runner(&self.shared, session_id)
    }

    pub(crate) fn role_runner(&self, role: SessionRole) -> Option<Runner> {
        selected_runner(&self.shared, role)
            .or_else(|| selected_runner(&self.shared, SessionRole::Chat))
    }

    pub(crate) fn project_list(&self) -> &[ProjectInfo] {
        &self.project_list
    }

    pub(crate) fn status_runner(
        &self,
        role: SessionRole,
        provider: &str,
        model: &str,
    ) -> Option<Runner> {
        let mut matches = self
            .shared
            .menus
            .get(&role)?
            .iter()
            .map(|(_, runner)| runner)
            .filter(|runner| runner.provider.name() == provider && runner.model == model);
        let runner = matches.next()?;
        if matches.any(|other| !Arc::ptr_eq(&runner.provider, &other.provider)) {
            return None;
        }
        Some(runner.clone())
    }

    pub fn repair_restart_handbacks(&self) {
        let unfinished = match self.shared.engine.unfinished_jobs() {
            Ok(unfinished) => unfinished,
            Err(error) => {
                warn!(%error, "could not scan for unconcluded jobs at startup; skipping restart repair");
                return;
            }
        };
        for job in unfinished {
            info!(
                session_id = %job.session_id,
                parent_session = %job.parent_session,
                "handing back a job left unfinished by the last restart"
            );
            record_handback(&self.shared, &job, Some("the daemon restarted"));
        }
    }

    pub async fn shutdown(&self) {
        let draining = async {
            loop {
                let handles: Vec<_> = self
                    .shared
                    .handles
                    .lock()
                    .expect("handles")
                    .drain(..)
                    .collect();
                if handles.is_empty() {
                    return;
                }
                futures::future::join_all(handles).await;
            }
        };
        if tokio::time::timeout(SHUTDOWN_GRACE, draining)
            .await
            .is_err()
        {
            warn!("shutdown grace expired; abandoning outstanding tasks");
        }
    }
}

fn send_into(
    shared: &Shared,
    session_id: Option<&str>,
    content: &str,
    source: Source,
    mut attachments: Vec<ImageAttachment>,
    attach: bool,
) -> Result<SendOutcome, SessionError> {
    if content.trim().is_empty() && attachments.is_empty() {
        return Err(SessionError::EmptyMessage);
    }
    arc_core::attachment::validate(&mut attachments)?;
    let runner = match session_id {
        Some(session_id) => turn_runner(shared, session_id)?,
        None => chat_runner(shared)?,
    };
    if !attachments.is_empty() && !runner.provider.supports_images() {
        return Err(SessionError::AttachmentsUnsupported {
            provider: runner.provider.name().to_owned(),
        });
    }
    // a user message ends whatever chain of system-started turns was running
    if source == Source::User {
        if let Some(session_id) = session_id {
            shared.autonomy.reset(session_id);
        }
    }

    let mut live = shared.live.lock().expect("live");
    if let Some(session_id) = session_id {
        if let Some(queued) = deliver_live(shared, &live, session_id, content, source, &attachments)
        {
            return Ok(queued);
        }
    }

    let session_id = match session_id {
        Some(session_id) => session_id.to_owned(),
        None => shared.engine.create_session(&runner)?,
    };
    if !autonomy_allows(shared, &session_id, content, source) {
        return Ok(SendOutcome::Queued { session_id });
    }

    let (events, receiver) = if attach {
        let (events, receiver) = mpsc::channel(EVENT_BUFFER);
        (Some(events), Some(receiver))
    } else {
        (None, None)
    };
    let task = Task {
        job: DispatchedJob {
            session_id: session_id.clone(),
            parent_session: String::new(),
            role: runner.role,
            project: String::new(),
            brief: content.to_owned(),
            budget: None,
        },
        dispatched: false,
        source,
        attachments,
        attached: events,
        spent_tokens: 0,
    };
    spawn_task(shared, &mut live, runner, task);
    Ok(SendOutcome::Started {
        session_id,
        events: receiver,
    })
}

fn deliver_live(
    shared: &Shared,
    live: &HashMap<String, LiveSession>,
    session_id: &str,
    content: &str,
    source: Source,
    attachments: &[ImageAttachment],
) -> Option<SendOutcome> {
    let session = live.get(session_id)?;
    if shared.engine.queue_message_with_attachments(
        session_id,
        content,
        source,
        attachments.to_vec(),
    ) {
        return Some(SendOutcome::Queued {
            session_id: session_id.to_owned(),
        });
    }
    if !autonomy_allows(shared, session_id, content, source) {
        return Some(SendOutcome::Queued {
            session_id: session_id.to_owned(),
        });
    }
    let sent = session
        .inbox
        .send(Inbound {
            content: content.to_owned(),
            source,
            attachments: attachments.to_vec(),
        })
        .is_ok();
    if !sent {
        return None;
    }
    if let Some(info) = shared.statuses.record_steer_queued(session_id) {
        notify_job_changed(shared.notifier.as_ref(), &shared.engine, info);
    }
    Some(SendOutcome::Queued {
        session_id: session_id.to_owned(),
    })
}

fn autonomy_allows(shared: &Shared, session_id: &str, content: &str, source: Source) -> bool {
    if source != Source::System || shared.autonomy.claim(session_id) {
        return true;
    }
    warn!(
        session_id,
        "consecutive system-started turns hit the autonomy cap; appending without a turn"
    );
    if let Err(error) = shared.engine.append_message(session_id, content, source) {
        warn!(session_id, %error, "could not append the capped message");
    }
    false
}

fn selected_runner(shared: &Shared, role: SessionRole) -> Option<Runner> {
    let menu = shared.menus.get(&role)?;
    let picked = shared.engine.selected_choice(role).ok().flatten();
    picked
        .and_then(|name| menu.iter().find(|(choice, _)| *choice == name))
        .or_else(|| menu.first())
        .map(|(_, runner)| runner.clone())
}

fn chat_runner(shared: &Shared) -> Result<Runner, SessionError> {
    selected_runner(shared, SessionRole::Chat).ok_or_else(|| SessionError::NoRunner {
        role: role_label(SessionRole::Chat).to_owned(),
    })
}

fn turn_runner(shared: &Shared, session_id: &str) -> Result<Runner, SessionError> {
    let Some(role) = shared.engine.session_role(session_id)? else {
        return chat_runner(shared);
    };
    if role == SessionRole::Unspecified {
        return chat_runner(shared);
    }
    let choice = shared.engine.session_choice(session_id)?;
    let identity = shared.engine.session_identity(session_id)?;
    let menu = shared.menus.get(&role).or_else(|| {
        (choice.is_none())
            .then(|| shared.menus.get(&SessionRole::Chat))
            .flatten()
    });
    let candidates = || menu.into_iter().flat_map(|menu| menu.iter());
    let mut runner = if let Some(choice) = choice {
        candidates()
            .find(|(name, runner)| {
                name == &choice
                    && identity.as_ref().is_none_or(|(provider, model)| {
                        provider.is_empty()
                            || (runner.provider.name(), runner.model.as_str())
                                == (provider.as_str(), model.as_str())
                    })
            })
            .map(|(_, runner)| runner.clone())
            .ok_or_else(|| SessionError::MissingChoice {
                session_id: session_id.to_owned(),
                choice,
            })?
    } else if let Some((provider, model)) = identity {
        if provider.is_empty() && model.is_empty() {
            selected_runner(shared, role)
                .or_else(|| selected_runner(shared, SessionRole::Chat))
                .ok_or_else(|| SessionError::NoRunner {
                    role: role_label(role).to_owned(),
                })?
        } else {
            let mut matches = candidates()
                .filter(|(_, runner)| runner.provider.name() == provider && runner.model == model);
            let runner = matches.next().map(|(_, runner)| runner.clone());
            if matches.next().is_some() {
                return Err(SessionError::AmbiguousChoice {
                    session_id: session_id.to_owned(),
                    provider,
                    model,
                });
            }
            runner.ok_or_else(|| SessionError::MissingChoice {
                session_id: session_id.to_owned(),
                choice: format!("{provider}/{model}"),
            })?
        }
    } else {
        return Err(SessionError::UnknownSession {
            session_id: session_id.to_owned(),
        });
    };
    if let Some(project) = shared.engine.session_project(session_id)? {
        if let Some(configured) = shared.projects.get(&project) {
            let description = shared.engine.project_description(&project).unwrap_or("");
            runner.system = Some(prompt::project_context(
                &project,
                description,
                &configured.root,
                shared.identity.as_deref(),
            ));
        }
    } else if let Some(directory) = shared.engine.session_working_directory(session_id)? {
        runner.system = Some(prompt::directory_context(
            std::path::Path::new(&directory),
            shared.identity.as_deref(),
        ));
    }
    Ok(runner)
}

fn spawn_dispatched(shared: &Shared, jobs: Vec<DispatchedJob>) {
    for job in jobs {
        spawn_job(shared, job, 0);
    }
}

fn route_continues(shared: &Shared, continues: Vec<ContinuedJob>) {
    for cont in continues {
        route_continue(shared, cont);
    }
}

fn route_cancels(shared: &Shared, cancels: Vec<String>) {
    for session_id in cancels {
        if !cancel_live(&shared.live, &session_id) {
            warn!(session_id, "cancel_job named a job that wasn't live");
        }
    }
}

fn cancel_live(live: &LiveMap, session_id: &str) -> bool {
    let live = live.lock().expect("live");
    let Some(session) = live.get(session_id) else {
        return false;
    };
    let _ = session.cancel.send(true);
    true
}

fn spawn_job(shared: &Shared, job: DispatchedJob, initial_spent_tokens: u64) -> bool {
    let mut runner = match turn_runner(shared, &job.session_id) {
        Ok(runner) => runner,
        Err(error) => {
            warn!(session_id = %job.session_id, %error, "job cannot use its recorded model");
            return false;
        }
    };
    if let Some(project) = shared.projects.get(&job.project) {
        runner.system = Some(prompt::project_context(
            &job.project,
            shared
                .engine
                .project_description(&job.project)
                .unwrap_or(""),
            &project.root,
            None,
        ));
    }
    let mut live = shared.live.lock().expect("live");
    if live.contains_key(&job.session_id) {
        warn!(
            session_id = %job.session_id,
            "continue_job raced with another resume of the same job; skipping"
        );
        return false;
    }
    let info = shared.statuses.start(&job, initial_spent_tokens);
    notify_job_changed(shared.notifier.as_ref(), &shared.engine, info);
    let task = Task {
        job,
        dispatched: true,
        source: Source::User,
        attachments: Vec::new(),
        attached: None,
        spent_tokens: initial_spent_tokens,
    };
    spawn_task(shared, &mut live, runner, task);
    true
}

fn spawn_task(
    shared: &Shared,
    live: &mut HashMap<String, LiveSession>,
    runner: Runner,
    task: Task,
) {
    let (inbox, inbox_rx) = mpsc::unbounded_channel();
    let (cancel, cancel_rx) = watch::channel(false);
    let (drop_tx, drop_rx) = mpsc::unbounded_channel();
    live.insert(
        task.job.session_id.clone(),
        LiveSession {
            inbox,
            cancel,
            drop_tx,
        },
    );
    let handle = spawn_watched(shared.clone(), runner, task, inbox_rx, cancel_rx, drop_rx);
    let mut handles = shared.handles.lock().expect("handles");
    // reap finished wrappers so a long-lived daemon's history stays bounded
    handles.retain(|held| !held.is_finished());
    handles.push(handle);
}

fn spawn_watched(
    shared: Shared,
    runner: Runner,
    task: Task,
    inbox_rx: mpsc::UnboundedReceiver<Inbound>,
    cancel_rx: watch::Receiver<bool>,
    drop_rx: mpsc::UnboundedReceiver<()>,
) -> JoinHandle<()> {
    let recovery = task.job.clone();
    let dispatched = task.dispatched;
    let start = Instant::now();
    tokio::spawn(async move {
        let inner = tokio::spawn(run_task(
            shared.clone(),
            runner,
            task,
            inbox_rx,
            cancel_rx,
            drop_rx,
        ));
        if let Err(join_error) = inner.await {
            if !join_error.is_panic() {
                return;
            }
            warn!(
                session_id = %recovery.session_id,
                "session task panicked; forcing it to failed"
            );
            shared
                .live
                .lock()
                .expect("live")
                .remove(&recovery.session_id);
            if !dispatched {
                return;
            }
            if let Some(info) = shared.statuses.finish(
                &recovery.session_id,
                job_info::State::Failed,
                start.elapsed(),
            ) {
                notify_job_changed(shared.notifier.as_ref(), &shared.engine, info);
            }
            handback_crashed(&shared, &recovery);
        }
    })
}

fn route_continue(shared: &Shared, cont: ContinuedJob) {
    {
        let live = shared.live.lock().expect("live");
        if deliver_live(
            shared,
            &live,
            &cont.session_id,
            &cont.message,
            Source::Model,
            &[],
        )
        .is_some()
        {
            info!(session_id = %cont.session_id, "continue_job queued into the live job");
            return;
        }
    }
    let session_id = cont.session_id.clone();
    // a resume's strip counter seeds from durable usage, not zero (row 6.37)
    let initial_spent_tokens = shared.engine.session_usage_tokens(&session_id).unwrap_or_else(|error| {
        warn!(session_id, %error, "could not read the job's durable usage; resuming its counter at zero");
        0
    });
    let resumed = spawn_job(
        shared,
        DispatchedJob {
            session_id: cont.session_id,
            parent_session: cont.parent_session,
            role: cont.role,
            project: cont.project,
            brief: cont.message,
            budget: None,
        },
        initial_spent_tokens,
    );
    if resumed {
        info!(session_id, "continue_job resumed a finished job");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arc_core::log::Log;
    use arc_core::projection::Projection;
    use arc_core::provider::{CompletionDelta, Stop, Thinking};
    use arc_core::session::ProjectSpec;
    use arc_core::store::Store;
    use arc_core::testkit::{
        ScriptedProvider, Step, call, channel, done_reply, replay_log, runner, seed_log, tool_stop,
        usage,
    };
    use arc_core::tool::workspace::{Grant, Mode};
    use arc_core::tool::{Registry, ToolSource};
    use arc_proto::v1::{Role, job_info};
    use tempfile::TempDir;

    use super::handback::NO_REPLY;
    use super::tests_common::testkit::{
        child_session, child_user_messages, engine_for_project, engine_for_project_notified,
        executor_runner, job_changed, only_job, parent_session, steer, wait_for_message_count,
    };

    #[tokio::test]
    async fn open_sessions_keep_distinct_presets_after_the_role_default_changes() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![done_reply("still first")]);
        let mut first = runner(&provider);
        first.model = "first-model".to_owned();
        let mut second = first.clone();
        second.model = "second-model".to_owned();
        let menu = vec![
            ("first".to_owned(), first.clone()),
            ("second".to_owned(), second),
        ];
        let choices = menu
            .iter()
            .map(|(name, runner)| arc_core::session::ModelChoice {
                name: name.clone(),
                provider: runner.provider.name().to_owned(),
                model: runner.model.clone(),
                thinking: runner.thinking,
                editing: arc_core::tool::Editing::Replacement,
            })
            .collect();
        let engine = Arc::new(
            Engine::new(
                Store::new(
                    Log::open(dir.path()).expect("log"),
                    Projection::in_memory().expect("projection"),
                ),
                Registry::new(512),
            )
            .with_role_choices(BTreeMap::from([(SessionRole::Chat, choices)])),
        );
        let supervisor = Supervisor::new(
            Arc::clone(&engine),
            BTreeMap::from([(SessionRole::Chat, menu)]),
        );
        let first_id = engine.create_session(&first).expect("first session");
        engine
            .select_model(SessionRole::Chat, "second")
            .expect("change default");
        let second_id = engine.create_session(&first).expect("second session");
        assert_eq!(
            supervisor.turn_runner(&first_id).expect("first pin").model,
            "first-model"
        );
        assert_eq!(
            supervisor
                .turn_runner(&second_id)
                .expect("second pin")
                .model,
            "second-model"
        );
        let third_id = engine
            .create_session_with_choice(&first, "first")
            .expect("explicit first");
        assert_eq!(
            engine
                .selected_choice(SessionRole::Chat)
                .unwrap()
                .as_deref(),
            Some("second"),
            "choosing for one session leaves the default alone"
        );
        assert_eq!(
            supervisor.turn_runner(&third_id).expect("third pin").model,
            "first-model"
        );
        let (tx, _rx) = channel();
        engine
            .send_message(
                &supervisor.turn_runner(&first_id).expect("first runner"),
                Some(&first_id),
                "keep working",
                tx,
            )
            .await
            .expect("first session remains continuable");
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn a_missing_recorded_preset_never_falls_back_to_the_new_default() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![]);
        let first = runner(&provider);
        let engine = Arc::new(
            Engine::new(
                Store::new(
                    Log::open(dir.path()).expect("log"),
                    Projection::in_memory().expect("projection"),
                ),
                Registry::new(512),
            )
            .with_role_choices(BTreeMap::from([(
                SessionRole::Chat,
                vec![arc_core::session::ModelChoice {
                    name: "old".to_owned(),
                    provider: first.provider.name().to_owned(),
                    model: first.model.clone(),
                    thinking: first.thinking,
                    editing: arc_core::tool::Editing::Replacement,
                }],
            )])),
        );
        let session_id = engine
            .create_session_with_choice(&first, "old")
            .expect("session");
        let supervisor = Supervisor::new(
            Arc::clone(&engine),
            BTreeMap::from([(SessionRole::Chat, vec![("new".to_owned(), first)])]),
        );
        assert!(
            matches!(
                supervisor.turn_runner(&session_id),
                Err(SessionError::MissingChoice { .. })
            ),
            "a removed preset must not silently run another"
        );
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn legacy_pin_refuses_ambiguous_thinking_presets() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![]);
        let first = runner(&provider);
        let session = Engine::new(
            Store::new(
                Log::open(dir.path()).expect("log"),
                Projection::in_memory().expect("projection"),
            ),
            Registry::new(512),
        );
        let id = session.create_session(&first).expect("legacy session");
        let engine = Arc::new(session);
        let mut high = first.clone();
        high.thinking = Thinking::High;
        let supervisor = Supervisor::new(
            Arc::clone(&engine),
            BTreeMap::from([(
                SessionRole::Chat,
                vec![("default".to_owned(), first), ("high".to_owned(), high)],
            )]),
        );
        assert!(matches!(
            supervisor.turn_runner(&id),
            Err(SessionError::AmbiguousChoice { .. })
        ));
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn an_unsupported_picture_does_not_create_a_session() {
        let dir = TempDir::new().expect("temp dir");
        let provider = ScriptedProvider::scripted(vec![]);
        let mut chat_runner = runner(&provider);
        chat_runner.provider = Arc::new(arc_core::provider::openai::OpenAiCompat::new(
            "http://127.0.0.1",
        ));
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(Engine::new(Store::new(log, projection), Registry::new(512)));
        let supervisor = Supervisor::for_test(
            Arc::clone(&engine),
            BTreeMap::from([(SessionRole::Chat, chat_runner)]),
        );
        let image = ImageAttachment {
            name: "picture.png".to_owned(),
            media_type: String::new(),
            data: b"\x89PNG\r\n\x1a\nbody".to_vec(),
        };

        let Err(error) = supervisor.send(None, "", Source::User, vec![image], false) else {
            panic!("the provider has no image support");
        };

        assert!(matches!(
            error,
            SessionError::AttachmentsUnsupported { provider } if provider == "openai-compat"
        ));
        assert!(replay_log(dir.path()).is_empty());
        supervisor.shutdown().await;
    }

    fn seeded_session(
        id: &str,
        role: SessionRole,
        dispatched_by: &str,
    ) -> arc_proto::v1::session_event::Event {
        arc_proto::v1::session_event::Event::SessionCreated(arc_proto::v1::SessionCreated {
            session_id: id.to_owned(),
            parent_session: String::new(),
            fork_point: 0,
            title: String::new(),
            provider: "scripted".to_owned(),
            model: "test-model".to_owned(),
            role: role as i32,
            project: "arc".to_owned(),
            budget: None,
            grants: Vec::new(),
            dispatched_by: dispatched_by.to_owned(),
            choice: String::new(),
            editing: String::new(),
            working_directory: String::new(),
        })
    }

    fn seeded_message(
        session_id: &str,
        role: Role,
        content: &str,
    ) -> arc_proto::v1::session_event::Event {
        arc_proto::v1::session_event::Event::MessageAppended(arc_proto::v1::MessageAppended {
            session_id: session_id.to_owned(),
            role: role as i32,
            content: content.to_owned(),
            partial: false,
            turn_id: "t-01".to_owned(),
            ..Default::default()
        })
    }

    fn reopened_arc_engine(dir: &TempDir) -> Arc<Engine> {
        let log = Log::open(dir.path()).expect("open log");
        let mut projection = Projection::in_memory().expect("open projection");
        arc_core::projection::replay(log.reader().expect("reader"), &mut projection)
            .expect("replay");
        Arc::new(Engine::new(Store::new(log, projection), Registry::new(512)))
    }

    #[tokio::test]
    async fn restart_repair_hands_back_an_unconcluded_dispatched_job_once() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                seeded_session("s-parent", SessionRole::Chat, ""),
                seeded_session("s-child", SessionRole::Executor, "s-parent"),
                seeded_message("s-child", Role::User, "fix the bug"),
                seeded_message("s-child", Role::Assistant, "half done"),
            ],
        );

        let engine = reopened_arc_engine(&dir);
        let supervisor = Supervisor::for_test(Arc::clone(&engine), BTreeMap::new());
        supervisor.repair_restart_handbacks();

        assert_eq!(
            child_user_messages(dir.path(), "s-parent"),
            [(
                Role::User,
                "Job s-child stopped: the daemon restarted.\nhalf done".to_owned()
            )],
            "the unconcluded job gets exactly one restart handback"
        );

        let engine = reopened_arc_engine(&dir);
        let supervisor = Supervisor::for_test(Arc::clone(&engine), BTreeMap::new());
        supervisor.repair_restart_handbacks();

        assert_eq!(
            child_user_messages(dir.path(), "s-parent").len(),
            1,
            "idempotent: a repaired job must not hand back twice"
        );
    }

    #[tokio::test]
    async fn a_job_holds_no_dispatch_so_no_grandchild_is_ever_created() {
        let dispatch_args = serde_json::json!({
            "role": "executor",
            "project": "arc",
            "brief": "grandchild work",
            "intent": "implement",
        })
        .to_string();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let executor_provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("g1", 0, "dispatch", &dispatch_args)),
                Ok(tool_stop()),
            ],
            done_reply("did it myself"),
        ]);

        let mut registry = Registry::new(512);
        registry.register(Box::new(arc_core::tool::builtin::dispatch::Dispatch::new(
            vec![("arc".to_owned(), String::new())],
            None,
        )));
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(
            Engine::new(Store::new(log, projection), registry).with_projects(BTreeMap::from([(
                "arc".to_owned(),
                ProjectSpec {
                    sources: vec![ToolSource::Builtin],
                    grants: vec![Grant::new(&root, Mode::ReadWrite)],
                    command_prefix: Vec::new(),
                    description: String::new(),
                },
            )])),
        );

        let parent_id = engine
            .create_bound_session(&runner(&executor_provider), "arc", SessionRole::Chat, None)
            .expect("create the parent durably");
        let child = engine
            .create_bound_session(
                &runner(&executor_provider),
                "arc",
                SessionRole::Executor,
                None,
            )
            .expect("create the child durably");

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child.clone(),
            parent_session: parent_id,
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "do the work; delegate the rest".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        let requests = executor_provider.requests();
        assert!(
            requests[0].tools.iter().all(|def| def.name != "dispatch"),
            "{:?}",
            requests[0].tools
        );
        let events = replay_log(dir.path());
        let refused = events
            .iter()
            .find_map(|event| match event {
                arc_proto::v1::session_event::Event::ToolResultRecorded(result)
                    if result.call_id == "g1" =>
                {
                    Some(result.clone())
                }
                _ => None,
            })
            .expect("the dispatch call got a result");
        assert_eq!(refused.outcome, arc_proto::v1::ToolOutcome::Error as i32);
        assert!(
            refused.content.contains("not available in this session"),
            "{}",
            refused.content
        );
        let created = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    arc_proto::v1::session_event::Event::SessionCreated(_)
                )
            })
            .count();
        assert_eq!(
            created, 2,
            "the parent and the child: no grandchild session exists"
        );
    }

    #[tokio::test]
    async fn an_executor_finishes_while_its_code_parent_is_busy_and_steering_survives() {
        let dispatch_args = serde_json::json!({
            "role": "executor",
            "project": "arc",
            "brief": "grandchild work",
            "intent": "implement",
        })
        .to_string();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let notify = Arc::new(tokio::sync::Notify::new());
        let code_provider = ScriptedProvider::scripted_steps(vec![
            Step::Immediate(vec![
                Ok(call("g1", 0, "dispatch", &dispatch_args)),
                Ok(tool_stop()),
            ]),
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("independent work".to_owned()))],
                notify: Arc::clone(&notify),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Immediate(done_reply("checked report and correction")),
        ]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("grandchild done")]);
        let code_runner = Runner {
            role: SessionRole::Chat,
            model: "astra".to_owned(),
            ..executor_runner(&code_provider)
        };
        let worker_runner = Runner {
            model: "sol".to_owned(),
            ..executor_runner(&executor_provider)
        };

        let mut registry = Registry::new(512);
        registry.register(Box::new(arc_core::tool::builtin::dispatch::Dispatch::new(
            vec![("arc".to_owned(), String::new())],
            None,
        )));
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(
            Engine::new(Store::new(log, projection), registry)
                .with_projects(BTreeMap::from([(
                    "arc".to_owned(),
                    ProjectSpec {
                        sources: vec![ToolSource::Builtin],
                        grants: vec![Grant::new(&root, Mode::ReadWrite)],
                        command_prefix: Vec::new(),
                        description: String::new(),
                    },
                )]))
                .with_role_identities(BTreeMap::from([
                    (
                        SessionRole::Chat,
                        ("scripted".to_owned(), "astra".to_owned()),
                    ),
                    (
                        SessionRole::Executor,
                        ("scripted".to_owned(), "sol".to_owned()),
                    ),
                ])),
        );

        let child = engine
            .create_direct_session(&code_runner, "arc", SessionRole::Chat)
            .expect("create the direct session durably");

        let runners = BTreeMap::from([
            (SessionRole::Chat, code_runner),
            (SessionRole::Executor, worker_runner),
        ]);
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners);

        let _stream = supervisor
            .send(
                Some(&child),
                "do the work; delegate the rest",
                Source::User,
                Vec::new(),
                false,
            )
            .expect("start direct turn");
        for _ in 0..400 {
            if supervisor
                .list()
                .iter()
                .any(|job| job.session_id != child && job.state == job_info::State::Finished as i32)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(supervisor.list().iter().any(|job| job.session_id != child
            && job.state == job_info::State::Finished as i32),
            "the child must finish before the parent is released");
        assert!(matches!(
            supervisor
                .send(
                    Some(&child),
                    "keep the API",
                    Source::User,
                    Vec::new(),
                    false
                )
                .expect("steer"),
            SendOutcome::Queued { .. }
        ));
        notify.notify_one();
        supervisor.shutdown().await;
        let requests = code_provider.requests();
        assert_eq!(requests.len(), 3, "no duplicate parent turn");
        assert!(
            requests
                .iter()
                .all(|r| r.role == SessionRole::Chat && r.model == "astra")
        );
        let worker_requests = executor_provider.requests();
        assert_eq!(worker_requests.len(), 1);
        assert_eq!(worker_requests[0].role, SessionRole::Executor);
        assert_eq!(worker_requests[0].model, "sol");
        let final_context = format!("{:?}", requests[2].messages);
        assert!(final_context.contains("grandchild done"));
        assert!(final_context.contains("keep the API"));

        let events = replay_log(dir.path());
        let grandchild = events
            .iter()
            .find_map(|event| match event {
                arc_proto::v1::session_event::Event::SessionCreated(created)
                    if created.role == SessionRole::Executor as i32
                        && created.session_id != child =>
                {
                    Some(created.session_id.clone())
                }
                _ => None,
            })
            .expect("the job's own dispatch created a grandchild durably");

        let ran: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                arc_proto::v1::session_event::Event::MessageAppended(m)
                    if m.session_id == grandchild =>
                {
                    Some(m.content.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            ran,
            ["grandchild work", "grandchild done"],
            "the grandchild actually ran, not just existed"
        );
    }

    #[tokio::test]
    async fn continue_job_on_a_finished_job_resumes_it_and_the_handback_lands_in_the_parent() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let chat_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider =
            ScriptedProvider::scripted(vec![done_reply("on it"), done_reply("linted too")]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &chat_provider);
        let child_id = child_session(&engine, &chat_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        assert!(
            !steer(&supervisor, &child_id, "not live anymore"),
            "the job already finished; nothing is left to steer"
        );

        supervisor.continue_job(ContinuedJob {
            session_id: child_id.clone(),
            parent_session: parent_id.clone(),
            message: "also check the linter".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
        });
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "on it".to_owned()),
                (Role::User, "also check the linter".to_owned()),
                (Role::Assistant, "linted too".to_owned()),
            ],
            "the resume ran as a fresh task over the same session, transcript intact"
        );
        assert_eq!(
            child_user_messages(dir.path(), &parent_id),
            [
                (
                    Role::User,
                    format!(
                        "Job {child_id} finished.\non it\n{footprint}",
                        footprint = arc_core::footprint::report(Some(&[]), None)
                    )
                ),
                (
                    Role::User,
                    format!(
                        "Job {child_id} finished.\nlinted too\n{footprint}",
                        footprint = arc_core::footprint::report(Some(&[]), None)
                    )
                ),
            ],
            "both the original finish and the resume's finish handed back to the same parent"
        );
    }

    #[tokio::test]
    async fn a_resumed_jobs_first_status_push_seeds_spent_tokens_from_durable_usage() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let chat_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider =
            ScriptedProvider::scripted(vec![done_reply("on it"), done_reply("linted too")]);

        let (notifier, mut notifications) = broadcast::channel(64);
        let engine = engine_for_project_notified(&dir, &root, notifier.clone());
        let parent_id = parent_session(&engine, &chat_provider);
        let child_id = child_session(&engine, &chat_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor =
            Supervisor::for_test(Arc::clone(&engine), runners).with_notifier(notifier.clone());

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;
        // usage() reports 8 tokens combined, spent by the first (only) turn
        assert_eq!(
            engine.session_usage_tokens(&child_id).expect("usage"),
            8,
            "the durable usage this resume should seed from"
        );
        while notifications.try_recv().is_ok() {}

        supervisor.continue_job(ContinuedJob {
            session_id: child_id.clone(),
            parent_session: parent_id.clone(),
            message: "also check the linter".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
        });
        let first_push = job_changed(&mut notifications).await;

        assert_eq!(first_push.session_id, child_id);
        assert_eq!(first_push.state, job_info::State::Running as i32);
        assert_eq!(
            first_push.spent_tokens, 8,
            "the resume's first push carries the summed durable usage, not zero"
        );

        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn spawning_an_already_live_session_keeps_its_inbox() {
        let dir = TempDir::new().expect("temp dir");
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(Engine::new(Store::new(log, projection), Registry::new(512)));
        let provider = ScriptedProvider::scripted(vec![]);
        let child_id = engine
            .create_session(&executor_runner(&provider))
            .expect("session");
        let runners = BTreeMap::from([(SessionRole::Executor, executor_runner(&provider))]);
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners);
        let shared = &supervisor.shared;

        // stands in for a first resume already holding this session's slot
        let (inbox, _inbox_rx) = mpsc::unbounded_channel();
        let (cancel, _cancel_rx) = watch::channel(false);
        let (drop_tx, _drop_rx) = mpsc::unbounded_channel();
        shared.live.lock().expect("live").insert(
            child_id.clone(),
            LiveSession {
                inbox,
                cancel,
                drop_tx,
            },
        );

        let spawned = spawn_job(
            shared,
            DispatchedJob {
                session_id: child_id,
                parent_session: "s-parent".to_owned(),
                role: SessionRole::Executor,
                project: "arc".to_owned(),
                brief: "second resume".to_owned(),
                budget: None,
            },
            0,
        );

        assert!(
            !spawned,
            "a racing resume must not clobber the first one's inbox"
        );
        assert_eq!(
            shared.live.lock().expect("live").len(),
            1,
            "still exactly the first resume's entry"
        );
        assert!(
            shared.handles.lock().expect("handles").is_empty(),
            "no task was spawned for the loser of the race"
        );
    }

    #[tokio::test]
    async fn a_panicking_job_task_ends_failed_with_a_crashed_handback_and_no_daemon_panic() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let chat_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Panics]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &chat_provider);
        let child_id = child_session(&engine, &chat_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        let job = only_job(supervisor.list());
        assert_eq!(
            job.state,
            job_info::State::Failed as i32,
            "the watchdog forced the panicked task to failed"
        );
        assert!(
            !steer(&supervisor, &child_id, "too late"),
            "the panicking task's live entry was removed"
        );
        assert_eq!(
            child_user_messages(dir.path(), &parent_id),
            [(
                Role::User,
                format!("Job {child_id} stopped: the job crashed.\n{NO_REPLY}")
            )],
            "the crash reads as a normal stopped handback"
        );
    }

    #[tokio::test]
    async fn cancelling_a_job_drops_its_queued_steers_with_a_warning() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let chat_provider = ScriptedProvider::scripted(vec![]);
        let gate = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: Vec::new(),
            notify: gate,
            after: Vec::new(),
        }]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &chat_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });

        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(steer(&supervisor, &child_id, "too late"));
        assert!(supervisor.cancel(&child_id));
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [(Role::User, "fix the failing test".to_owned())],
            "the queued steer never ran"
        );
        assert!(
            !steer(&supervisor, &child_id, "still too late"),
            "the job already ended"
        );
    }

    #[tokio::test]
    async fn a_jobs_own_cancel_job_is_refused_and_the_sibling_keeps_running() {
        use arc_core::provider::{Provider, Thinking};

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        // the sibling stays live until this test releases it
        let gate = Arc::new(tokio::sync::Notify::new());
        let archivist_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: Vec::new(),
            notify: Arc::clone(&gate),
            after: Vec::new(),
        }]);

        let mut registry = Registry::new(512);
        registry.register(Box::new(arc_core::tool::builtin::cancel_job::CancelJob));
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(
            Engine::new(Store::new(log, projection), registry).with_projects(BTreeMap::from([(
                "arc".to_owned(),
                ProjectSpec {
                    sources: vec![ToolSource::Builtin],
                    grants: vec![Grant::new(&root, Mode::ReadWrite)],
                    command_prefix: Vec::new(),
                    description: String::new(),
                },
            )])),
        );

        let bootstrap_provider = ScriptedProvider::scripted(vec![]);
        let parent_id = engine
            .create_direct_session(&runner(&bootstrap_provider), "arc", SessionRole::Chat)
            .expect("create the parent durably");
        let sibling = engine
            .create_bound_session(
                &runner(&bootstrap_provider),
                "arc",
                SessionRole::Archivist,
                None,
            )
            .expect("create the sibling durably");
        let canceller = engine
            .create_bound_session(
                &runner(&bootstrap_provider),
                "arc",
                SessionRole::Executor,
                None,
            )
            .expect("create the canceller durably");

        let cancel_args = serde_json::json!({ "session_id": sibling }).to_string();
        let executor_provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "cancel_job", &cancel_args)),
                Ok(tool_stop()),
            ],
            done_reply("stopped it"),
        ]);

        let runners = BTreeMap::from([
            (SessionRole::Executor, executor_runner(&executor_provider)),
            (
                SessionRole::Archivist,
                Runner {
                    role: SessionRole::Archivist,
                    provider: Arc::clone(&archivist_provider) as Arc<dyn Provider>,
                    model: "test-model".to_owned(),
                    thinking: Thinking::Default,
                    system: None,
                    compact_at: None,
                    context_window: None,
                    editing: arc_core::tool::Editing::Replacement,
                },
            ),
        ]);
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: sibling.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Archivist,
            project: "arc".to_owned(),
            brief: "sit and wait".to_owned(),
            budget: None,
        });
        wait_for_message_count(dir.path(), &sibling, 1).await;

        supervisor.spawn(DispatchedJob {
            session_id: canceller.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "stop the sibling".to_owned(),
            budget: None,
        });
        for _ in 0..400 {
            if supervisor.list().iter().any(|job| {
                job.session_id == canceller && job.state == job_info::State::Finished as i32
            }) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            supervisor.list().iter().any(
                |job| job.session_id == sibling && job.state == job_info::State::Running as i32
            ),
            "the sibling is still running: a job cannot cancel it"
        );
        gate.notify_one();
        supervisor.shutdown().await;

        let requests = executor_provider.requests();
        assert!(
            requests[0].tools.iter().all(|def| def.name != "cancel_job"),
            "{:?}",
            requests[0].tools
        );
        assert!(
            child_user_messages(dir.path(), &parent_id)
                .into_iter()
                .all(|(_, content)| !content.contains("cancelled")),
            "no cancelled handback reached the parent"
        );
    }
}
