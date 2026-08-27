mod handback;
mod prompt;
mod status;
#[cfg(test)]
mod tests_common;
mod turn;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_core::provider::role_label;
use arc_core::session::{ContinuedJob, DispatchedJob, Engine, Runner};
use arc_proto::v1::{JobInfo, Notification, SessionRole};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{info, warn};

use handback::{Handback, HandbackCtx, handback_crashed, job_title, record_handback};
use prompt::job_system_prompt;
use status::{JobState, JobStatuses, notify_job_changed};
use turn::run_job;

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// A live job's handles: the steer queue's write end, a cancel signal
/// (row 6.39), and a drop-queued-steers signal (row 6.33). All three share
/// the entry's lifetime — inserted together on spawn, removed together
/// when the job task itself finishes.
struct LiveJob {
    steer_tx: mpsc::UnboundedSender<String>,
    cancel: watch::Sender<bool>,
    drop_tx: mpsc::UnboundedSender<()>,
}

type LiveMap = Mutex<HashMap<String, LiveJob>>;
type Handles = Mutex<Vec<JoinHandle<()>>>;

/// Runs the jobs `dispatch` created. One tokio task per job, driven to
/// completion with `send_message`; nothing restarts a job that fails.
pub struct Supervisor {
    engine: Arc<Engine>,
    runners: BTreeMap<SessionRole, Runner>,
    /// Project name to root path, so a dispatched job's system prompt can
    /// read `<root>/AGENTS.md`. A project absent here gets no system prompt.
    projects: BTreeMap<String, PathBuf>,
    handles: Arc<Handles>,
    /// `session_id` -> steer sender, for jobs currently running. `steer`
    /// sends under this lock; a job task removes its own entry under the
    /// same lock, so an enqueue can never land in a channel nobody will read.
    live: Arc<LiveMap>,
    /// Outlives `live`: a finished job keeps its status entry after its
    /// steer sender is torn down, until eviction or a daemon restart.
    statuses: Arc<JobStatuses>,
    notifier: Option<broadcast::Sender<Notification>>,
    handback: Option<Arc<Handback>>,
}

impl Supervisor {
    pub fn new(engine: Arc<Engine>, runners: BTreeMap<SessionRole, Runner>) -> Self {
        Self {
            engine,
            runners,
            projects: BTreeMap::new(),
            handles: Arc::new(Mutex::new(Vec::new())),
            live: Arc::new(Mutex::new(HashMap::new())),
            statuses: Arc::new(JobStatuses::new()),
            notifier: None,
            handback: None,
        }
    }

    /// Project roots a dispatched job's system prompt is built from. Absent,
    /// job runners get no system prompt at all.
    #[must_use]
    pub fn with_projects(mut self, projects: BTreeMap<String, PathBuf>) -> Self {
        self.projects = projects;
        self
    }

    /// Wires the daemon's broadcast spine: job state transitions then also
    /// fan out as `job_changed` pushes. Absent, nothing is sent.
    #[must_use]
    pub fn with_notifier(mut self, notifier: broadcast::Sender<Notification>) -> Self {
        self.notifier = Some(notifier);
        self
    }

    /// Wires the concierge runner a handback turn drives (DESIGN.md §4.1):
    /// once a job's summary lands in a concierge parent, the supervisor
    /// runs one turn over it so the concierge reacts. Absent, `record_handback`
    /// still runs but no turn follows.
    #[must_use]
    pub fn with_concierge(mut self, runner: Runner) -> Self {
        self.handback = Some(Arc::new(Handback::new(runner)));
        self
    }

    pub fn spawn(&self, job: DispatchedJob) {
        spawn_job(
            job,
            &self.engine,
            &self.runners,
            &self.projects,
            &self.live,
            &self.statuses,
            self.notifier.as_ref(),
            self.handback.as_ref(),
            &self.handles,
        );
    }

    /// Ends whatever chain of handback turns was running for `session_id`:
    /// called from the server's send path, since a user message is what
    /// ends autonomous narration (DESIGN.md §4.1). A no-op without a
    /// concierge wired, or if the session has no counter yet.
    pub fn reset_autonomy(&self, session_id: &str) {
        if let Some(handback) = &self.handback {
            handback.reset_autonomy(session_id);
        }
    }

    /// The daemon's live view of every job it remembers: running jobs first
    /// (newest first), then terminal ones (newest first).
    pub fn list(&self) -> Vec<JobInfo> {
        let mut jobs = self.statuses.list();
        for job in &mut jobs {
            job.title = self.title(&job.session_id);
        }
        jobs
    }

    fn title(&self, session_id: &str) -> String {
        job_title(&self.engine, session_id)
    }

    /// Enqueues a steering message for a live job. `true` if the job was
    /// live and the message was enqueued; `false` otherwise, meaning the
    /// caller should fall through to a normal turn.
    pub fn steer(&self, session_id: &str, text: &str) -> bool {
        let queued = steer_live(&self.live, session_id, text);
        if queued {
            if let Some(info) = self.statuses.record_steer_queued(session_id) {
                notify_job_changed(self.notifier.as_ref(), &self.engine, info);
            }
        }
        queued
    }

    /// Cancels a live job (row 6.39): drops its running turn, drains
    /// whatever's queued, and hands back that the user stopped it. `false`
    /// if the job isn't live — the same honest signal `steer` gives for an
    /// unknown or already-finished job.
    pub fn cancel(&self, session_id: &str) -> bool {
        let live = self.live.lock().expect("live");
        let Some(job) = live.get(session_id) else {
            return false;
        };
        let _ = job.cancel.send(true);
        true
    }

    /// Empties a live job's queued steers (row 6.33) without touching its
    /// running turn. Only signals the request here: the job's own task owns
    /// the queue, so it drains it and reports the fresh count itself — a
    /// steer that queues between this call and that drain must never be
    /// silently discarded. `false` if the job isn't live.
    pub fn drop_steers(&self, session_id: &str) -> bool {
        let live = self.live.lock().expect("live");
        let Some(job) = live.get(session_id) else {
            return false;
        };
        let _ = job.drop_tx.send(());
        true
    }

    /// The runner a job of this role is dispatched with — the same map,
    /// shared rather than rebuilt, so the server can serve a follow-up turn
    /// on a job's own session with its own role instead of the concierge's.
    pub fn job_runner(&self, role: SessionRole) -> Option<&Runner> {
        self.runners.get(&role)
    }

    /// Routes a `continue_job` request (DESIGN.md §6.16): queued into the
    /// job's steer channel if it's still running, or resumed as a fresh
    /// task over the same child session, its full transcript intact, if it
    /// already finished.
    pub fn continue_job(&self, cont: ContinuedJob) {
        let ctx = HandbackCtx {
            engine: &self.engine,
            runners: &self.runners,
            projects: &self.projects,
            live: &self.live,
            statuses: &self.statuses,
            notifier: self.notifier.as_ref(),
            handles: &self.handles,
            handback: self.handback.as_ref(),
        };
        route_continue(&ctx, cont);
    }

    /// Startup repair (row 6.34): every job session left dispatched-but-
    /// unconcluded when the daemon last died gets a "stopped: restarted"
    /// handback now, through the same `record_handback` path a live job's
    /// own ending uses — parent turn guard, narration turn, all of it. Runs
    /// once, after orphan repair and before serving; idempotent across
    /// repeated restarts, since a job already handed back is found
    /// concluded and skipped. A job session that predates 6.34 has no
    /// recorded parent and cannot be found here.
    pub async fn repair_restart_handbacks(&self) {
        let unfinished = match self.engine.unfinished_jobs() {
            Ok(unfinished) => unfinished,
            Err(error) => {
                warn!(%error, "could not scan for unconcluded jobs at startup; skipping restart repair");
                return;
            }
        };
        let ctx = HandbackCtx {
            engine: &self.engine,
            runners: &self.runners,
            projects: &self.projects,
            live: &self.live,
            statuses: &self.statuses,
            notifier: self.notifier.as_ref(),
            handles: &self.handles,
            handback: self.handback.as_ref(),
        };
        for job in unfinished {
            info!(
                session_id = %job.session_id,
                parent_session = %job.parent_session,
                "handing back a job left unfinished by the last restart"
            );
            record_handback(&ctx, &job, Some("the daemon restarted")).await;
        }
    }

    /// Gives outstanding jobs a grace period to finish, then abandons them.
    /// Loops the drain to a fixed point: a job's own turn, or a handback
    /// turn it triggers, can spawn further jobs (chains, DESIGN.md §4.1),
    /// so one drain pass is not enough to wait out a whole chain.
    pub async fn shutdown(&self) {
        let draining = async {
            loop {
                let handles: Vec<_> = self.handles.lock().expect("handles").drain(..).collect();
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
            warn!("shutdown grace expired; abandoning outstanding jobs");
        }
    }
}

/// Spawns one job's task and registers its handle, whether the job came
/// from a client's `dispatch` or from a handback turn's own `dispatch`.
#[allow(clippy::too_many_arguments)]
fn spawn_dispatched(ctx: &HandbackCtx<'_>, jobs: Vec<DispatchedJob>) {
    for job in jobs {
        spawn_job(
            job,
            ctx.engine,
            ctx.runners,
            ctx.projects,
            ctx.live,
            ctx.statuses,
            ctx.notifier,
            ctx.handback,
            ctx.handles,
        );
    }
}

/// Routes every `continue_job` a turn produced, alongside whatever it
/// dispatched.
fn route_continues(ctx: &HandbackCtx<'_>, continues: Vec<ContinuedJob>) {
    for cont in continues {
        route_continue(ctx, cont);
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_job(
    job: DispatchedJob,
    engine: &Arc<Engine>,
    runners: &BTreeMap<SessionRole, Runner>,
    projects: &BTreeMap<String, PathBuf>,
    live: &Arc<LiveMap>,
    statuses: &Arc<JobStatuses>,
    notifier: Option<&broadcast::Sender<Notification>>,
    handback: Option<&Arc<Handback>>,
    handles: &Arc<Handles>,
) {
    spawn_job_checked(
        job, engine, runners, projects, live, statuses, notifier, handback, handles, false, 0,
    );
}

/// Spawns a job's task and registers its handle. `guard_absent` gates the
/// `live` insert on `session_id` not already being there: a fresh dispatch
/// never needs this (the `session_id` is brand new), but a `continue_job`
/// resume does — two resumes racing on the same finished job must not both
/// spawn a task and clobber each other's steer channel. Returns whether it
/// actually spawned.
#[allow(clippy::too_many_arguments)]
fn spawn_job_checked(
    job: DispatchedJob,
    engine: &Arc<Engine>,
    runners: &BTreeMap<SessionRole, Runner>,
    projects: &BTreeMap<String, PathBuf>,
    live: &Arc<LiveMap>,
    statuses: &Arc<JobStatuses>,
    notifier: Option<&broadcast::Sender<Notification>>,
    handback: Option<&Arc<Handback>>,
    handles: &Arc<Handles>,
    guard_absent: bool,
    initial_spent_tokens: u64,
) -> bool {
    let Some(mut runner) = runners.get(&job.role).cloned() else {
        warn!(
            session_id = %job.session_id,
            role = role_label(job.role),
            "dispatched job names a role with no runner; skipping"
        );
        return false;
    };
    if let Some(root) = projects.get(&job.project) {
        runner.system = Some(job_system_prompt(root));
    }
    let (steer_tx, steer_rx) = mpsc::unbounded_channel();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (drop_tx, drop_rx) = mpsc::unbounded_channel();
    {
        let mut live_guard = live.lock().expect("live");
        if guard_absent && live_guard.contains_key(&job.session_id) {
            warn!(
                session_id = %job.session_id,
                "continue_job raced with another resume of the same job; skipping"
            );
            return false;
        }
        live_guard.insert(
            job.session_id.clone(),
            LiveJob {
                steer_tx,
                cancel: cancel_tx,
                drop_tx,
            },
        );
    }
    let info = statuses.start(&job, initial_spent_tokens);
    notify_job_changed(notifier, engine, info);
    let handle = spawn_watched(
        job,
        Arc::clone(engine),
        runner,
        steer_rx,
        cancel_rx,
        drop_rx,
        Arc::clone(live),
        Arc::clone(statuses),
        notifier.cloned(),
        runners.clone(),
        projects.clone(),
        handback.cloned(),
        Arc::clone(handles),
    );
    let mut handles = handles.lock().expect("handles");
    // reap finished wrappers so a long-lived daemon's history stays bounded
    handles.retain(|held| !held.is_finished());
    handles.push(handle);
    true
}

/// Spawns `run_job` under a wrapper task that watches for it panicking
/// (any bare `expect`, any bug) instead of reaching a terminal state. The
/// wrapper, not `run_job`, is what `handles` tracks, so `shutdown` still
/// waits out the recovery path too.
#[allow(clippy::too_many_arguments)]
fn spawn_watched(
    job: DispatchedJob,
    engine: Arc<Engine>,
    runner: Runner,
    steer_rx: mpsc::UnboundedReceiver<String>,
    cancel_rx: watch::Receiver<bool>,
    drop_rx: mpsc::UnboundedReceiver<()>,
    live: Arc<LiveMap>,
    statuses: Arc<JobStatuses>,
    notifier: Option<broadcast::Sender<Notification>>,
    runners: BTreeMap<SessionRole, Runner>,
    projects: BTreeMap<String, PathBuf>,
    handback: Option<Arc<Handback>>,
    handles: Arc<Handles>,
) -> JoinHandle<()> {
    let recovery_job = job.clone();
    let start = Instant::now();
    tokio::spawn(async move {
        let inner = tokio::spawn(run_job(
            Arc::clone(&engine),
            runner,
            job,
            steer_rx,
            cancel_rx,
            drop_rx,
            Arc::clone(&live),
            Arc::clone(&statuses),
            notifier.clone(),
            runners.clone(),
            projects.clone(),
            handback.clone(),
            Arc::clone(&handles),
        ));
        if let Err(join_error) = inner.await {
            if !join_error.is_panic() {
                return;
            }
            warn!(
                session_id = %recovery_job.session_id,
                "job task panicked; forcing it to failed"
            );
            live.lock().expect("live").remove(&recovery_job.session_id);
            if let Some(info) =
                statuses.finish(&recovery_job.session_id, JobState::Failed, start.elapsed())
            {
                notify_job_changed(notifier.as_ref(), &engine, info);
            }
            let ctx = HandbackCtx {
                engine: &engine,
                runners: &runners,
                projects: &projects,
                live: &live,
                statuses: &statuses,
                notifier: notifier.as_ref(),
                handles: &handles,
                handback: handback.as_ref(),
            };
            handback_crashed(&ctx, &recovery_job).await;
        }
    })
}

fn steer_live(live: &LiveMap, session_id: &str, text: &str) -> bool {
    let live = live.lock().expect("live");
    live.get(session_id)
        .is_some_and(|job| job.steer_tx.send(text.to_owned()).is_ok())
}

/// Routes one `ContinuedJob`: a steer if the job is still live, or a resume
/// — a fresh task over the same child session, its `message` as the next
/// turn — if it already finished. Shared by every turn driver that can
/// produce one: a user turn, a handback turn, and a job's own turn.
fn route_continue(ctx: &HandbackCtx<'_>, cont: ContinuedJob) {
    if steer_live(ctx.live, &cont.session_id, &cont.message) {
        if let Some(info) = ctx.statuses.record_steer_queued(&cont.session_id) {
            notify_job_changed(ctx.notifier, ctx.engine, info);
        }
        info!(session_id = %cont.session_id, "continue_job queued into the live job");
        return;
    }
    let session_id = cont.session_id.clone();
    // a resume's strip counter seeds from durable usage, not zero (row 6.37)
    let initial_spent_tokens = ctx.engine.session_usage_tokens(&session_id).unwrap_or_else(|error| {
        warn!(session_id, %error, "could not read the job's durable usage; resuming its counter at zero");
        0
    });
    let resumed = spawn_job_checked(
        DispatchedJob {
            session_id: cont.session_id,
            parent_session: cont.parent_session,
            role: cont.role,
            project: cont.project,
            brief: cont.message,
            budget: None,
        },
        ctx.engine,
        ctx.runners,
        ctx.projects,
        ctx.live,
        ctx.statuses,
        ctx.notifier,
        ctx.handback,
        ctx.handles,
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
        executor_runner, job_changed, only_job, parent_session, wait_for_message_count,
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

    /// An engine over a log seeded before this call, as a restart sees it:
    /// opened fresh and replayed into a fresh index, no engine-driven append
    /// in between.
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
        supervisor.repair_restart_handbacks().await;

        assert_eq!(
            child_user_messages(dir.path(), "s-parent"),
            [(
                Role::User,
                "Job s-child stopped: the daemon restarted.\nhalf done".to_owned()
            )],
            "the unconcluded job gets exactly one restart handback"
        );

        // a second restart replays the same seeded log plus the handback
        // just appended; it must not hand back a second time
        let engine = reopened_arc_engine(&dir);
        let supervisor = Supervisor::new(Arc::clone(&engine), BTreeMap::new());
        supervisor.repair_restart_handbacks().await;

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
        supervisor.repair_restart_handbacks().await;

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
                // predates 6.34: no recorded dispatched_by
                seeded_session("s-child", SessionRole::Executor, ""),
                seeded_message("s-child", Role::User, "fix the bug"),
            ],
        );

        let engine = reopened_arc_engine(&dir);
        let supervisor = Supervisor::new(Arc::clone(&engine), BTreeMap::new());
        supervisor.repair_restart_handbacks().await;

        assert_eq!(
            child_user_messages(dir.path(), "s-child"),
            [(Role::User, "fix the bug".to_owned())],
            "a parentless job session predates 6.34 and cannot be repaired"
        );
    }

    #[tokio::test]
    async fn a_steer_to_a_live_job_is_queued_and_runs_after_the_initial_turn() {
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

        // the brief's user message lands as soon as the turn opens, before
        // the provider stalls on the gate: waiting for it proves turn 1 is
        // genuinely in flight when the steer below gets enqueued
        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(
            supervisor.steer(&child_id, "also check the linter"),
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
            "the brief turn ran to completion before the steered turn started"
        );
    }

    #[tokio::test]
    async fn two_steers_run_as_two_turns_in_the_order_they_arrived() {
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
            Step::Immediate(done_reply("first steer done")),
            Step::Immediate(done_reply("second steer done")),
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

        // both steers are enqueued while the brief turn is still gated, so
        // the job's post-turn drain finds them already queued in order
        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(supervisor.steer(&child_id, "first steer"));
        assert!(supervisor.steer(&child_id, "second steer"));
        notify.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "on it".to_owned()),
                (Role::User, "first steer".to_owned()),
                (Role::Assistant, "first steer done".to_owned()),
                (Role::User, "second steer".to_owned()),
                (Role::Assistant, "second steer done".to_owned()),
            ]
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
            !supervisor.steer(&child_id, "not live anymore"),
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
                (Role::User, format!("Job {child_id} finished.\non it")),
                (Role::User, format!("Job {child_id} finished.\nlinted too")),
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
        let live: Arc<LiveMap> = Arc::new(Mutex::new(HashMap::new()));
        let statuses = Arc::new(JobStatuses::new());
        let handles: Arc<Handles> = Arc::new(Mutex::new(Vec::new()));

        // stands in for a first resume already holding this session's slot
        let (steer_tx, _steer_rx) = mpsc::unbounded_channel();
        let (cancel_tx, _cancel_rx) = watch::channel(false);
        let (drop_tx, _drop_rx) = mpsc::unbounded_channel();
        live.lock().expect("live").insert(
            "s-child".to_owned(),
            LiveJob {
                steer_tx,
                cancel: cancel_tx,
                drop_tx,
            },
        );

        let spawned = spawn_job_checked(
            DispatchedJob {
                session_id: "s-child".to_owned(),
                parent_session: "s-parent".to_owned(),
                role: SessionRole::Executor,
                project: "arc".to_owned(),
                brief: "second resume".to_owned(),
                budget: None,
            },
            &engine,
            &runners,
            &BTreeMap::new(),
            &live,
            &statuses,
            None,
            None,
            &handles,
            true,
            0,
        );

        assert!(
            !spawned,
            "a racing resume must not clobber the first one's steer channel"
        );
        assert_eq!(
            live.lock().expect("live").len(),
            1,
            "still exactly the first resume's entry"
        );
        assert!(
            handles.lock().expect("handles").is_empty(),
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
            !supervisor.steer(&child_id, "too late"),
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
        assert!(supervisor.steer(&child_id, "too late"));
        assert!(supervisor.cancel(&child_id));
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [(Role::User, "fix the failing test".to_owned())],
            "the queued steer never ran"
        );
        assert!(
            !supervisor.steer(&child_id, "still too late"),
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
}
