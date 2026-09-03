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
use arc_proto::v1::{JobInfo, Notification, ProjectInfo, SessionRole, Source};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{info, warn};

use handback::{Autonomy, handback_crashed, job_title, record_handback};
use prompt::job_system_prompt;
use status::{JobState, JobStatuses, notify_job_changed};
use turn::{EVENT_BUFFER, Task, run_task};

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// The root a job works in and the wrapper its commands run under.
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

/// A session with a task running its turns. The inbox holds messages that
/// arrived while the task was between turns; a message that arrives while a
/// turn is actually live goes to the engine instead, which delivers it at
/// the turn's next step boundary.
struct LiveSession {
    inbox: mpsc::UnboundedSender<Inbound>,
    cancel: watch::Sender<bool>,
    drop_tx: mpsc::UnboundedSender<()>,
}

type LiveMap = Mutex<HashMap<String, LiveSession>>;
type Handles = Mutex<Vec<JoinHandle<()>>>;

/// What a turn's task streams to the connection that started it.
pub enum TurnEvent {
    Engine(EngineEvent),
    Ended(Result<Reply, SessionError>),
}

pub enum SendOutcome {
    /// The message started a turn. `events` carries it when the caller
    /// asked to be attached.
    Started {
        session_id: String,
        events: Option<mpsc::Receiver<TurnEvent>>,
    },
    /// The session was already working: the message lands in the running
    /// turn, or as the task's next turn.
    Queued { session_id: String },
}

#[derive(Clone)]
pub(crate) struct Shared {
    engine: Arc<Engine>,
    runners: BTreeMap<SessionRole, Runner>,
    concierge: Option<Runner>,
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
    pub fn new(engine: Arc<Engine>, runners: BTreeMap<SessionRole, Runner>) -> Self {
        Self {
            shared: Shared {
                engine,
                runners,
                concierge: None,
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

    #[must_use]
    pub fn with_concierge(mut self, runner: Runner) -> Self {
        self.shared.concierge = Some(runner);
        self
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

    /// The one way a message reaches a session. A live turn takes it at its
    /// next step boundary, a task between turns runs it next, and an idle
    /// session gets a task of its own — for a user message always, for a
    /// system message until the autonomy cap.
    pub fn send(
        &self,
        session_id: Option<&str>,
        content: &str,
        source: Source,
        attach: bool,
    ) -> Result<SendOutcome, SessionError> {
        send_into(&self.shared, session_id, content, source, attach)
    }

    /// Dispatch and resume, for tests. A turn that dispatches routes its
    /// own children as it ends; nothing else starts a job.
    #[cfg(test)]
    pub fn spawn(&self, job: DispatchedJob) {
        spawn_job(&self.shared, job);
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

    /// What a new session of this role would run under: the role's own
    /// runner, or the concierge's when the role has none configured.
    pub(crate) fn role_runner(&self, role: SessionRole) -> Option<Runner> {
        self.shared
            .runners
            .get(&role)
            .cloned()
            .or_else(|| self.shared.concierge.clone())
    }

    pub(crate) fn project_list(&self) -> &[ProjectInfo] {
        &self.project_list
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
    attach: bool,
) -> Result<SendOutcome, SessionError> {
    if content.trim().is_empty() {
        return Err(SessionError::EmptyMessage);
    }
    // a user message ends whatever chain of system-started turns was running
    if source == Source::User {
        if let Some(session_id) = session_id {
            shared.autonomy.reset(session_id);
        }
    }

    let mut live = shared.live.lock().expect("live");
    if let Some(session_id) = session_id {
        if let Some(queued) = deliver_live(shared, &live, session_id, content, source) {
            return Ok(queued);
        }
    }

    let (session_id, runner) = if let Some(session_id) = session_id {
        (session_id.to_owned(), turn_runner(shared, session_id)?)
    } else {
        let runner = concierge_runner(shared)?;
        (shared.engine.create_session(&runner)?, runner)
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
        attached: events,
        spent_tokens: 0,
    };
    spawn_task(shared, &mut live, runner, task);
    Ok(SendOutcome::Started {
        session_id,
        events: receiver,
    })
}

/// Hands a message to the session's live task, if it has one: into the
/// running turn, or into the task's inbox as its next turn. `None` means
/// there is no task to take it.
fn deliver_live(
    shared: &Shared,
    live: &HashMap<String, LiveSession>,
    session_id: &str,
    content: &str,
    source: Source,
) -> Option<SendOutcome> {
    let session = live.get(session_id)?;
    if shared.engine.queue_message(session_id, content, source) {
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

/// A handback that would start a turn of its own is capped once a parent
/// has narrated `MAX_HANDBACK_TURNS` of them with no user message between:
/// past that it lands in the log and stops there (DESIGN.md §4.1).
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

fn concierge_runner(shared: &Shared) -> Result<Runner, SessionError> {
    shared
        .concierge
        .clone()
        .ok_or_else(|| SessionError::NoRunner {
            role: role_label(SessionRole::Concierge).to_owned(),
        })
}

/// The runner a session the user (or a handback) writes into runs under,
/// fixed for the task's lifetime so its prompt prefix stays byte-stable.
fn turn_runner(shared: &Shared, session_id: &str) -> Result<Runner, SessionError> {
    let role = match shared.engine.session_role(session_id) {
        Ok(role) => role.unwrap_or(SessionRole::Unspecified),
        Err(error) => {
            warn!(session_id, %error, "could not read the session's role; serving it as a concierge");
            SessionRole::Unspecified
        }
    };
    let (SessionRole::Executor | SessionRole::Archivist) = role else {
        return concierge_runner(shared);
    };
    let mut runner = match shared.runners.get(&role) {
        Some(runner) => runner.clone(),
        None => concierge_runner(shared)?,
    };
    if role == SessionRole::Executor {
        if let Some(prompt) = direct_system_prompt_for(shared, session_id) {
            runner.system = Some(prompt);
        }
    }
    Ok(runner)
}

fn direct_system_prompt_for(shared: &Shared, session_id: &str) -> Option<String> {
    let project = shared.engine.session_project(session_id).ok().flatten()?;
    let root = &shared.projects.get(&project)?.root;
    Some(prompt::direct_system_prompt(
        root,
        shared.identity.as_deref(),
    ))
}

fn spawn_dispatched(shared: &Shared, jobs: Vec<DispatchedJob>) {
    for job in jobs {
        spawn_job(shared, job);
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

fn spawn_job(shared: &Shared, job: DispatchedJob) {
    spawn_job_checked(shared, job, false, 0);
}

fn spawn_job_checked(
    shared: &Shared,
    job: DispatchedJob,
    guard_absent: bool,
    initial_spent_tokens: u64,
) -> bool {
    let Some(mut runner) = shared.runners.get(&job.role).cloned() else {
        warn!(
            session_id = %job.session_id,
            role = role_label(job.role),
            "dispatched job names a role with no runner; skipping"
        );
        return false;
    };
    if let Some(project) = shared.projects.get(&job.project) {
        runner.system = Some(job_system_prompt(&project.root));
    }
    let mut live = shared.live.lock().expect("live");
    if guard_absent && live.contains_key(&job.session_id) {
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
            if let Some(info) =
                shared
                    .statuses
                    .finish(&recovery.session_id, JobState::Failed, start.elapsed())
            {
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
    let resumed = spawn_job_checked(
        shared,
        DispatchedJob {
            session_id: cont.session_id,
            parent_session: cont.parent_session,
            role: cont.role,
            project: cont.project,
            brief: cont.message,
            budget: None,
        },
        true,
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
    use arc_core::provider::{CompletionDelta, Stop};
    use arc_core::session::ProjectSpec;
    use arc_core::store::Store;
    use arc_core::testkit::{
        ScriptedProvider, Step, appended, call, done_reply, replay_log, runner, seed_log,
        tool_stop, usage,
    };
    use arc_core::tool::workspace::{Grant, Mode};
    use arc_core::tool::{Registry, ToolSource};
    use arc_proto::v1::{Budget, Role, job_info};
    use tempfile::TempDir;

    use super::handback::NO_REPLY;
    use super::tests_common::testkit::{
        child_session, child_user_messages, engine_for_project, engine_for_project_notified,
        executor_runner, job_changed, only_job, parent_session, steer, wait_for_message_count,
    };

    #[tokio::test]
    async fn a_job_with_no_runner_for_its_role_logs_and_skips_without_a_panic() {
        let dir = TempDir::new().expect("temp dir");
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(Engine::new(Store::new(log, projection), Registry::new(512)));
        let supervisor = Supervisor::new(Arc::clone(&engine), BTreeMap::new());

        supervisor.spawn(DispatchedJob {
            session_id: "s-ghost".to_owned(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Concierge,
            project: "arc".to_owned(),
            brief: "never runs".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        assert_eq!(replay_log(dir.path()), Vec::new(), "no child turn ran");
    }

    #[tokio::test]
    async fn a_spawned_job_runs_the_brief_as_the_childs_first_message() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("on it")]);

        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(
            Engine::new(Store::new(log, projection), Registry::new(512)).with_projects(
                BTreeMap::from([(
                    "arc".to_owned(),
                    ProjectSpec {
                        sources: Vec::new(),
                        grants: vec![Grant::new(&root, Mode::ReadWrite)],
                        command_prefix: Vec::new(),
                    },
                )]),
            ),
        );

        let child_id = engine
            .create_bound_session(
                &runner(&concierge_provider),
                "arc",
                SessionRole::Executor,
                None,
            )
            .expect("create the child durably, as dispatch already does");

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        let events = replay_log(dir.path());
        let child_messages: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                arc_proto::v1::session_event::Event::MessageAppended(m)
                    if m.session_id == child_id =>
                {
                    Some(appended(event))
                }
                _ => None,
            })
            .collect();

        assert_eq!(child_messages.len(), 2, "the job's user turn and its reply");
        assert_eq!(child_messages[0].role, Role::User as i32);
        assert_eq!(child_messages[0].content, "fix the failing test");
        assert_eq!(child_messages[1].role, Role::Assistant as i32);
        assert_eq!(child_messages[1].content, "on it");
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
                seeded_session("s-parent", SessionRole::Concierge, ""),
                seeded_session("s-child", SessionRole::Executor, "s-parent"),
                seeded_message("s-child", Role::User, "fix the bug"),
                seeded_message("s-child", Role::Assistant, "half done"),
            ],
        );

        let engine = reopened_arc_engine(&dir);
        let supervisor = Supervisor::new(Arc::clone(&engine), BTreeMap::new());
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
        let supervisor = Supervisor::new(Arc::clone(&engine), BTreeMap::new());
        supervisor.repair_restart_handbacks();

        assert_eq!(
            child_user_messages(dir.path(), "s-parent").len(),
            1,
            "idempotent: a repaired job must not hand back twice"
        );
    }

    #[tokio::test]
    async fn restart_repair_leaves_a_cleanly_concluded_job_untouched() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                seeded_session("s-parent", SessionRole::Concierge, ""),
                seeded_session("s-child", SessionRole::Executor, "s-parent"),
                seeded_message("s-child", Role::User, "fix the bug"),
                seeded_message("s-child", Role::Assistant, "all done"),
                seeded_message("s-parent", Role::User, "Job s-child finished.\nall done"),
            ],
        );

        let engine = reopened_arc_engine(&dir);
        let supervisor = Supervisor::new(Arc::clone(&engine), BTreeMap::new());
        supervisor.repair_restart_handbacks();

        assert_eq!(
            child_user_messages(dir.path(), "s-parent"),
            [(Role::User, "Job s-child finished.\nall done".to_owned())],
            "a cleanly concluded job gets no extra handback"
        );
    }

    #[tokio::test]
    async fn restart_repair_skips_a_parentless_job_session() {
        let dir = TempDir::new().expect("temp dir");
        seed_log(
            &dir,
            vec![
                seeded_session("s-child", SessionRole::Executor, ""),
                seeded_message("s-child", Role::User, "fix the bug"),
            ],
        );

        let engine = reopened_arc_engine(&dir);
        let supervisor = Supervisor::new(Arc::clone(&engine), BTreeMap::new());
        supervisor.repair_restart_handbacks();

        assert_eq!(
            child_user_messages(dir.path(), "s-child"),
            [(Role::User, "fix the bug".to_owned())],
            "a parentless job session predates 6.34 and cannot be repaired"
        );
    }

    #[tokio::test]
    async fn a_steer_to_a_live_job_lands_in_the_turn_it_is_already_running() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("working".to_owned()))],
                notify: Arc::clone(&notify),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Immediate(done_reply("steer reply")),
        ]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });

        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(
            steer(&supervisor, &child_id, "also check the linter"),
            "the job is live"
        );
        notify.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "working".to_owned()),
                (Role::User, "also check the linter".to_owned()),
                (Role::Assistant, "steer reply".to_owned()),
            ],
            "the steer landed at the running turn's next step boundary"
        );
    }

    #[tokio::test]
    async fn two_steers_into_one_live_turn_both_land_in_it_in_order() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("on it".to_owned()))],
                notify: Arc::clone(&notify),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Immediate(done_reply("both steers done")),
        ]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: Some(Budget {
                total_tokens: 100_000,
                wall_clock_seconds: 0,
            }),
        });

        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(steer(&supervisor, &child_id, "first steer"));
        assert!(steer(&supervisor, &child_id, "second steer"));
        notify.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "on it".to_owned()),
                (Role::User, "first steer".to_owned()),
                (Role::User, "second steer".to_owned()),
                (Role::Assistant, "both steers done".to_owned()),
            ],
            "both landed at the same step boundary, in the order they arrived"
        );
    }

    #[tokio::test]
    async fn a_jobs_own_dispatch_spawns_and_runs_the_grandchild() {
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
            done_reply("parent job done"),
            done_reply("grandchild done"),
            // the grandchild's report reaches the job that dispatched it
            done_reply("noted the grandchild"),
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
                },
            )])),
        );

        let parent_id = engine
            .create_bound_session(
                &runner(&executor_provider),
                "arc",
                SessionRole::Concierge,
                None,
            )
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
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child.clone(),
            parent_session: parent_id,
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "do the work; delegate the rest".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

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
    async fn continue_job_on_a_live_job_queues_into_its_steer_channel_instead_of_resuming() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("working".to_owned()))],
                notify: Arc::clone(&notify),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Immediate(done_reply("continued reply")),
        ]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });

        wait_for_message_count(dir.path(), &child_id, 1).await;
        supervisor.continue_job(ContinuedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            message: "also check the linter".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
        });
        notify.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "working".to_owned()),
                (Role::User, "also check the linter".to_owned()),
                (Role::Assistant, "continued reply".to_owned()),
            ],
            "the live job's steer channel got it, not a resumed task"
        );

        let created = replay_log(dir.path())
            .into_iter()
            .filter(|event| {
                matches!(
                    event,
                    arc_proto::v1::session_event::Event::SessionCreated(created)
                        if created.role == SessionRole::Executor as i32
                )
            })
            .count();
        assert_eq!(created, 1, "no second session was created for a live job");
    }

    #[tokio::test]
    async fn continue_job_on_a_finished_job_resumes_it_and_the_handback_lands_in_the_parent() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider =
            ScriptedProvider::scripted(vec![done_reply("on it"), done_reply("linted too")]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

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
                        "Job {child_id} finished.\non it\nFor follow-ups about anything this job read or did, continue_job {child_id} keeps its context; a new dispatch starts from nothing."
                    )
                ),
                (
                    Role::User,
                    format!(
                        "Job {child_id} finished.\nlinted too\nFor follow-ups about anything this job read or did, continue_job {child_id} keeps its context; a new dispatch starts from nothing."
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

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider =
            ScriptedProvider::scripted(vec![done_reply("on it"), done_reply("linted too")]);

        let (notifier, mut notifications) = broadcast::channel(64);
        let engine = engine_for_project_notified(&dir, &root, notifier.clone());
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor =
            Supervisor::new(Arc::clone(&engine), runners).with_notifier(notifier.clone());

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
    async fn spawn_job_checked_with_guard_absent_skips_a_session_already_live() {
        let dir = TempDir::new().expect("temp dir");
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(Engine::new(Store::new(log, projection), Registry::new(512)));
        let provider = ScriptedProvider::scripted(vec![]);
        let runners = BTreeMap::from([(SessionRole::Executor, executor_runner(&provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);
        let shared = &supervisor.shared;

        // stands in for a first resume already holding this session's slot
        let (inbox, _inbox_rx) = mpsc::unbounded_channel();
        let (cancel, _cancel_rx) = watch::channel(false);
        let (drop_tx, _drop_rx) = mpsc::unbounded_channel();
        shared.live.lock().expect("live").insert(
            "s-child".to_owned(),
            LiveSession {
                inbox,
                cancel,
                drop_tx,
            },
        );

        let spawned = spawn_job_checked(
            shared,
            DispatchedJob {
                session_id: "s-child".to_owned(),
                parent_session: "s-parent".to_owned(),
                role: SessionRole::Executor,
                project: "arc".to_owned(),
                brief: "second resume".to_owned(),
                budget: None,
            },
            true,
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

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Panics]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

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
    async fn cancelling_a_live_job_hands_back_cancelled_exactly_once_and_goes_terminal() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        // never notified: the turn stalls until the cancel drops it
        let gate = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: Vec::new(),
            notify: gate,
            after: Vec::new(),
        }]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });

        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(supervisor.cancel(&child_id), "the job is live");
        supervisor.shutdown().await;

        let job = only_job(supervisor.list());
        assert_eq!(
            job.state,
            job_info::State::Failed as i32,
            "cancel is a failed-like terminal state"
        );
        assert_eq!(
            child_user_messages(dir.path(), &parent_id),
            [(
                Role::User,
                format!(
                    "Job {child_id} stopped: cancelled by the user. The user chose to stop \
                     this work — do not dispatch or continue it again unless they ask.\n{NO_REPLY}"
                )
            )],
            "the cancelled handback lands exactly once"
        );
        assert!(
            !supervisor.cancel(&child_id),
            "already terminal; nothing left to cancel"
        );
    }

    #[tokio::test]
    async fn cancelling_a_job_drops_its_queued_steers_with_a_warning() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let gate = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: Vec::new(),
            notify: gate,
            after: Vec::new(),
        }]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

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
    async fn cancelling_a_finished_job_is_an_honest_no_op() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("on it")]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        assert!(
            !supervisor.cancel(&child_id),
            "the job already finished cleanly"
        );
        assert!(!supervisor.cancel("s-never-existed"), "an unknown session");
    }

    #[tokio::test]
    async fn a_jobs_own_cancel_job_stops_a_live_sibling_and_its_handback_lands() {
        use arc_core::provider::{Provider, Thinking};

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        // the sibling: never notified, so it stays live until cancel_job stops it
        let gate = Arc::new(tokio::sync::Notify::new());
        let archivist_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: Vec::new(),
            notify: gate,
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
                },
            )])),
        );

        let bootstrap_provider = ScriptedProvider::scripted(vec![]);
        let parent_id = engine
            .create_bound_session(
                &runner(&bootstrap_provider),
                "arc",
                SessionRole::Concierge,
                None,
            )
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
                },
            ),
        ]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

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
            session_id: canceller,
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "stop the sibling".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &parent_id)
                .into_iter()
                .filter(|(_, content)| content.contains("stopped: cancelled by the user"))
                .count(),
            1,
            "the sibling's cancelled handback landed in the shared parent"
        );
    }
}
