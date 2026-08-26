use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_core::provider::{Usage, role_label};
use arc_core::session::{ContinuedJob, DispatchedJob, Engine, Runner};
use arc_proto::v1::{Budget, JobInfo, Notification, SessionRole, job_info, notification};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info, warn};

const EVENT_BUFFER: usize = 64;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
const NO_REPLY: &str = "(the job produced no reply)";

/// Terminal job statuses retained after a job's task ends. Live daemon
/// memory only: rebuilt empty on restart, unlike the child session itself.
const MAX_TERMINAL_JOBS: usize = 50;

/// Consecutive handback turns run for one parent since its last user
/// message, before the daemon stops narrating and just leaves the handback
/// itself in the transcript (DESIGN.md §4.1).
const MAX_HANDBACK_TURNS: u32 = 50;

type LiveMap = Mutex<HashMap<String, mpsc::UnboundedSender<String>>>;
type Handles = Mutex<Vec<JoinHandle<()>>>;

/// The handback turn's collapse and autonomy state, shared by every job's
/// task since jobs finishing into the same parent contend on it together.
/// Absent from a `Supervisor` that never called `with_concierge`, in which
/// case a handback is recorded but no turn ever follows it.
struct Handback {
    runner: Runner,
    /// Parents with a handback turn in flight right now: a second handback
    /// for the same parent collapses into `dirty` instead of its own turn.
    pending: Mutex<HashSet<String>>,
    /// Parents that got another handback while their turn was pending: the
    /// running turn loops once more per flag once it finishes, bounded by
    /// `autonomy`, not by a count of its own.
    dirty: Mutex<HashSet<String>>,
    /// Consecutive handback turns per parent since its last user message.
    autonomy: Mutex<HashMap<String, u32>>,
}

impl Handback {
    fn new(runner: Runner) -> Self {
        Self {
            runner,
            pending: Mutex::new(HashSet::new()),
            dirty: Mutex::new(HashSet::new()),
            autonomy: Mutex::new(HashMap::new()),
        }
    }

    fn reset_autonomy(&self, session_id: &str) {
        self.autonomy.lock().expect("autonomy").remove(session_id);
    }
}

/// Everything a handback turn needs to run and, if it dispatches, to spawn
/// the child itself: bundled so `run_job` can build it once and pass it to
/// all three of its finish paths.
struct HandbackCtx<'a> {
    engine: &'a Arc<Engine>,
    runners: &'a BTreeMap<SessionRole, Runner>,
    live: &'a Arc<LiveMap>,
    statuses: &'a Arc<JobStatuses>,
    notifier: Option<&'a broadcast::Sender<Notification>>,
    handles: &'a Arc<Handles>,
    handback: Option<&'a Arc<Handback>>,
}

/// Runs the jobs `dispatch` created. One tokio task per job, driven to
/// completion with `send_message`; nothing restarts a job that fails.
pub struct Supervisor {
    engine: Arc<Engine>,
    runners: BTreeMap<SessionRole, Runner>,
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
            handles: Arc::new(Mutex::new(Vec::new())),
            live: Arc::new(Mutex::new(HashMap::new())),
            statuses: Arc::new(JobStatuses::new()),
            notifier: None,
            handback: None,
        }
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
        steer_live(&self.live, session_id, text)
    }

    /// Routes a `continue_job` request (DESIGN.md §6.16): queued into the
    /// job's steer channel if it's still running, or resumed as a fresh
    /// task over the same child session, its full transcript intact, if it
    /// already finished.
    pub fn continue_job(&self, cont: ContinuedJob) {
        let ctx = HandbackCtx {
            engine: &self.engine,
            runners: &self.runners,
            live: &self.live,
            statuses: &self.statuses,
            notifier: self.notifier.as_ref(),
            handles: &self.handles,
            handback: self.handback.as_ref(),
        };
        route_continue(&ctx, cont);
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

fn spawn_job(
    job: DispatchedJob,
    engine: &Arc<Engine>,
    runners: &BTreeMap<SessionRole, Runner>,
    live: &Arc<LiveMap>,
    statuses: &Arc<JobStatuses>,
    notifier: Option<&broadcast::Sender<Notification>>,
    handback: Option<&Arc<Handback>>,
    handles: &Arc<Handles>,
) {
    spawn_job_checked(
        job, engine, runners, live, statuses, notifier, handback, handles, false,
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
    live: &Arc<LiveMap>,
    statuses: &Arc<JobStatuses>,
    notifier: Option<&broadcast::Sender<Notification>>,
    handback: Option<&Arc<Handback>>,
    handles: &Arc<Handles>,
    guard_absent: bool,
) -> bool {
    let Some(runner) = runners.get(&job.role).cloned() else {
        warn!(
            session_id = %job.session_id,
            role = role_label(job.role),
            "dispatched job names a role with no runner; skipping"
        );
        return false;
    };
    let (steer_tx, steer_rx) = mpsc::unbounded_channel();
    {
        let mut live_guard = live.lock().expect("live");
        if guard_absent && live_guard.contains_key(&job.session_id) {
            warn!(
                session_id = %job.session_id,
                "continue_job raced with another resume of the same job; skipping"
            );
            return false;
        }
        live_guard.insert(job.session_id.clone(), steer_tx);
    }
    let info = statuses.start(&job);
    notify_job_changed(notifier, engine, info);
    let handle = tokio::spawn(run_job(
        Arc::clone(engine),
        runner,
        job,
        steer_rx,
        Arc::clone(live),
        Arc::clone(statuses),
        notifier.cloned(),
        runners.clone(),
        handback.cloned(),
        Arc::clone(handles),
    ));
    handles.lock().expect("handles").push(handle);
    true
}

fn steer_live(live: &LiveMap, session_id: &str, text: &str) -> bool {
    let live = live.lock().expect("live");
    live.get(session_id)
        .is_some_and(|sender| sender.send(text.to_owned()).is_ok())
}

/// Routes one `ContinuedJob`: a steer if the job is still live, or a resume
/// — a fresh task over the same child session, its `message` as the next
/// turn — if it already finished. Shared by every turn driver that can
/// produce one: a user turn, a handback turn, and a job's own turn.
fn route_continue(ctx: &HandbackCtx<'_>, cont: ContinuedJob) {
    if steer_live(ctx.live, &cont.session_id, &cont.message) {
        info!(session_id = %cont.session_id, "continue_job queued into the live job");
        return;
    }
    let session_id = cont.session_id.clone();
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
        ctx.live,
        ctx.statuses,
        ctx.notifier,
        ctx.handback,
        ctx.handles,
        true,
    );
    if resumed {
        info!(session_id, "continue_job resumed a finished job");
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_job(
    engine: Arc<Engine>,
    runner: Runner,
    job: DispatchedJob,
    mut steer_rx: mpsc::UnboundedReceiver<String>,
    live: Arc<LiveMap>,
    statuses: Arc<JobStatuses>,
    notifier: Option<broadcast::Sender<Notification>>,
    runners: BTreeMap<SessionRole, Runner>,
    handback: Option<Arc<Handback>>,
    handles: Arc<Handles>,
) {
    let session_id = job.session_id.clone();
    let start = Instant::now();
    let mut spent_tokens: u64 = 0;
    let ctx = HandbackCtx {
        engine: &engine,
        runners: &runners,
        live: &live,
        statuses: &statuses,
        notifier: notifier.as_ref(),
        handles: &handles,
        handback: handback.as_ref(),
    };

    match run_turn(&engine, &runner, &session_id, &job.brief).await {
        TurnOutcome::Success {
            usage,
            jobs,
            continues,
        } => {
            spent_tokens += usage_tokens(usage);
            if let Some(info) = statuses.record_tokens(&session_id, spent_tokens) {
                notify_job_changed(notifier.as_ref(), &engine, info);
            }
            spawn_dispatched(&ctx, jobs);
            route_continues(&ctx, continues);
        }
        TurnOutcome::Failure => {
            finish_now(&live, &mut steer_rx, &session_id);
            if let Some(info) = statuses.finish(&session_id, JobState::Failed, start.elapsed()) {
                notify_job_changed(notifier.as_ref(), &engine, info);
            }
            handback_failed(&ctx, &job).await;
            return;
        }
    }

    loop {
        if let Some(breach) = budget_breach(job.budget.as_ref(), spent_tokens, start.elapsed()) {
            warn_over_budget(&session_id, &breach);
            finish_now(&live, &mut steer_rx, &session_id);
            if let Some(info) = statuses.finish(&session_id, JobState::OverBudget, start.elapsed())
            {
                notify_job_changed(notifier.as_ref(), &engine, info);
            }
            handback_over_budget(&ctx, &job, &breach).await;
            return;
        }

        let steered = match steer_rx.try_recv() {
            Ok(text) => Some(text),
            Err(mpsc::error::TryRecvError::Disconnected) => None,
            Err(mpsc::error::TryRecvError::Empty) => {
                let mut live = live.lock().expect("live");
                if let Ok(text) = steer_rx.try_recv() {
                    Some(text)
                } else {
                    live.remove(&session_id);
                    None
                }
            }
        };
        let Some(text) = steered else { break };
        match run_turn(&engine, &runner, &session_id, &text).await {
            TurnOutcome::Success {
                usage,
                jobs,
                continues,
            } => {
                spent_tokens += usage_tokens(usage);
                if let Some(info) = statuses.record_tokens(&session_id, spent_tokens) {
                    notify_job_changed(notifier.as_ref(), &engine, info);
                }
                spawn_dispatched(&ctx, jobs);
                route_continues(&ctx, continues);
            }
            TurnOutcome::Failure => {
                finish_now(&live, &mut steer_rx, &session_id);
                if let Some(info) = statuses.finish(&session_id, JobState::Failed, start.elapsed())
                {
                    notify_job_changed(notifier.as_ref(), &engine, info);
                }
                handback_failed(&ctx, &job).await;
                return;
            }
        }
    }

    if let Some(info) = statuses.finish(&session_id, JobState::Finished, start.elapsed()) {
        notify_job_changed(notifier.as_ref(), &engine, info);
    }
    handback_clean(&ctx, &job).await;
}

fn job_title(engine: &Engine, session_id: &str) -> String {
    match engine.session_title(session_id) {
        Ok(title) => title.unwrap_or_default(),
        Err(error) => {
            warn!(session_id, %error, "could not read the job's title; leaving it blank");
            String::new()
        }
    }
}

fn notify_job_changed(
    notifier: Option<&broadcast::Sender<Notification>>,
    engine: &Engine,
    mut info: JobInfo,
) {
    let Some(notifier) = notifier else {
        return;
    };
    info.title = job_title(engine, &info.session_id);
    let _ = notifier.send(Notification {
        event: Some(notification::Event::JobChanged(info)),
    });
}

/// The job's last assistant reply, or the fixed no-reply line: shared by
/// every handback path, since an empty or missing reply reads the same way
/// regardless of how the job ended.
fn job_summary(engine: &Engine, job: &DispatchedJob) -> String {
    match engine.last_assistant_message(&job.session_id) {
        Ok(Some(text)) => text,
        Ok(None) => NO_REPLY.to_owned(),
        Err(error) => {
            warn!(
                session_id = %job.session_id,
                %error,
                "could not read the job's last reply for its handback; using the no-reply line"
            );
            NO_REPLY.to_owned()
        }
    }
}

/// Appends the job's handback and, if that succeeds, requests the turn
/// that reads it (DESIGN.md §4.1's "the daemon drives one model turn").
async fn record_handback(ctx: &HandbackCtx<'_>, job: &DispatchedJob, reason: Option<&str>) {
    let summary = job_summary(ctx.engine, job);
    match ctx
        .engine
        .record_handback(&job.parent_session, &job.session_id, reason, &summary)
        .await
    {
        Ok(()) => maybe_run_handback_turn(ctx, &job.parent_session).await,
        Err(error) => warn!(
            parent_session = %job.parent_session,
            session_id = %job.session_id,
            %error,
            "failed to record the job's handback into the parent session"
        ),
    }
}

async fn handback_clean(ctx: &HandbackCtx<'_>, job: &DispatchedJob) {
    record_handback(ctx, job, None).await;
}

async fn handback_failed(ctx: &HandbackCtx<'_>, job: &DispatchedJob) {
    record_handback(ctx, job, Some("the turn failed")).await;
}

async fn handback_over_budget(ctx: &HandbackCtx<'_>, job: &DispatchedJob, breach: &BudgetBreach) {
    let reason = match breach {
        BudgetBreach::Tokens { spent, allowed } => {
            format!("token budget exhausted ({spent}/{allowed})")
        }
        BudgetBreach::WallClock { elapsed, allowed } => {
            format!("time budget exhausted ({elapsed}s/{allowed}s)")
        }
    };
    record_handback(ctx, job, Some(&reason)).await;
}

/// Requests a handback turn on `parent_session`, collapsing into an
/// already-pending one instead of queuing a second (DESIGN.md §4.1). Exact
/// shape: a `pending` set claimed before the turn runs and cleared after;
/// a handback arriving while claimed sets `dirty` instead of running its
/// own turn; once the running turn finishes, a set `dirty` flag reruns the
/// turn once more (looping until nothing landed meanwhile), bounded by the
/// autonomy cap below rather than by a catch-up count of its own.
async fn maybe_run_handback_turn(ctx: &HandbackCtx<'_>, parent_session: &str) {
    let Some(handback) = ctx.handback else {
        return;
    };

    if !claim_pending(handback, parent_session) {
        mark_dirty(handback, parent_session);
        return;
    }

    loop {
        run_one_handback_turn(ctx, handback, parent_session).await;
        if !release_or_rerun(handback, parent_session) {
            return;
        }
    }
}

fn claim_pending(handback: &Handback, parent_session: &str) -> bool {
    let mut pending = handback.pending.lock().expect("pending");
    if pending.contains(parent_session) {
        return false;
    }
    pending.insert(parent_session.to_owned());
    true
}

fn mark_dirty(handback: &Handback, parent_session: &str) {
    handback
        .dirty
        .lock()
        .expect("dirty")
        .insert(parent_session.to_owned());
}

/// `true` if another handback collapsed into this parent while its turn
/// ran: clears `dirty` and leaves `pending` set for one more turn. `false`
/// releases `pending`: this parent's chain of turns is done for now.
fn release_or_rerun(handback: &Handback, parent_session: &str) -> bool {
    if handback.dirty.lock().expect("dirty").remove(parent_session) {
        return true;
    }
    handback
        .pending
        .lock()
        .expect("pending")
        .remove(parent_session);
    false
}

/// Runs one handback turn, if the parent is (still) a concierge session and
/// under the autonomy cap, and spawns whatever it dispatches.
async fn run_one_handback_turn(
    ctx: &HandbackCtx<'_>,
    handback: &Arc<Handback>,
    parent_session: &str,
) {
    match ctx.engine.session_role(parent_session) {
        Ok(None | Some(SessionRole::Unspecified | SessionRole::Concierge)) => {}
        Ok(Some(other)) => {
            warn!(
                parent_session,
                role = role_label(other),
                "the handback's parent is not a concierge session; skipping the auto-turn"
            );
            return;
        }
        Err(error) => {
            warn!(
                parent_session,
                %error,
                "could not read the handback parent's role; skipping the auto-turn"
            );
            return;
        }
    }

    let capped = {
        let mut autonomy = handback.autonomy.lock().expect("autonomy");
        let count = autonomy.entry(parent_session.to_owned()).or_insert(0);
        if *count >= MAX_HANDBACK_TURNS {
            true
        } else {
            *count += 1;
            false
        }
    };
    if capped {
        warn!(
            parent_session,
            cap = MAX_HANDBACK_TURNS,
            "consecutive handback turns hit the autonomy cap; skipping the concierge turn"
        );
        return;
    }

    let (events, mut rx) = mpsc::channel(EVENT_BUFFER);
    let (result, ()) = tokio::join!(
        ctx.engine
            .continue_session(&handback.runner, parent_session, events),
        async {
            while let Some(event) = rx.recv().await {
                debug!(parent_session, ?event, "handback turn event");
            }
        },
    );
    match result {
        Ok(reply) => {
            info!(parent_session, "handback turn completed");
            for job in reply.jobs {
                spawn_job(
                    job,
                    ctx.engine,
                    ctx.runners,
                    ctx.live,
                    ctx.statuses,
                    ctx.notifier,
                    Some(handback),
                    ctx.handles,
                );
            }
            route_continues(ctx, reply.continues);
        }
        Err(error) => warn!(parent_session, %error, "handback turn failed"),
    }
}

/// A completed turn's outcome. A failed turn ends the job, so the caller
/// stops draining steers; a successful turn carries whatever usage the
/// provider reported, which may itself be absent.
enum TurnOutcome {
    Success {
        usage: Option<Usage>,
        jobs: Vec<DispatchedJob>,
        continues: Vec<ContinuedJob>,
    },
    Failure,
}

fn usage_tokens(usage: Option<Usage>) -> u64 {
    usage.map_or(0, |usage| {
        u64::from(usage.input_tokens) + u64::from(usage.output_tokens)
    })
}

/// Which dimension of a job's budget it went over, and by how much.
enum BudgetBreach {
    Tokens { spent: u64, allowed: u64 },
    WallClock { elapsed: u64, allowed: u64 },
}

/// A zero field means that dimension is unlimited.
fn budget_breach(
    budget: Option<&Budget>,
    spent_tokens: u64,
    elapsed: Duration,
) -> Option<BudgetBreach> {
    let budget = budget?;
    if budget.total_tokens > 0 && spent_tokens >= budget.total_tokens {
        return Some(BudgetBreach::Tokens {
            spent: spent_tokens,
            allowed: budget.total_tokens,
        });
    }
    if budget.wall_clock_seconds > 0 && elapsed.as_secs() >= u64::from(budget.wall_clock_seconds) {
        return Some(BudgetBreach::WallClock {
            elapsed: elapsed.as_secs(),
            allowed: u64::from(budget.wall_clock_seconds),
        });
    }
    None
}

fn warn_over_budget(session_id: &str, breach: &BudgetBreach) {
    match breach {
        BudgetBreach::Tokens { spent, allowed } => warn!(
            session_id,
            spent, allowed, "job over its token budget; stopping at the next turn boundary"
        ),
        BudgetBreach::WallClock { elapsed, allowed } => warn!(
            session_id,
            elapsed, allowed, "job over its wall-clock budget; stopping at the next turn boundary"
        ),
    }
}

/// Runs one turn to completion.
async fn run_turn(engine: &Engine, runner: &Runner, session_id: &str, text: &str) -> TurnOutcome {
    let (events, mut rx) = mpsc::channel(EVENT_BUFFER);
    let (result, ()) = tokio::join!(
        engine.send_message(runner, Some(session_id), text, events),
        async {
            while let Some(event) = rx.recv().await {
                debug!(session_id = %session_id, ?event, "job event");
            }
        },
    );
    match result {
        Ok(reply) => {
            info!(
                session_id = %session_id,
                input_tokens = reply.usage.map_or(0, |usage| usage.input_tokens),
                output_tokens = reply.usage.map_or(0, |usage| usage.output_tokens),
                "job turn completed"
            );
            TurnOutcome::Success {
                usage: reply.usage,
                jobs: reply.jobs,
                continues: reply.continues,
            }
        }
        Err(error) => {
            warn!(session_id = %session_id, %error, "job turn failed");
            TurnOutcome::Failure
        }
    }
}

/// Ends a job now: removes its live entry and drops whatever steers are
/// still queued, exactly as a failed turn or an over-budget job must.
fn finish_now(live: &LiveMap, steer_rx: &mut mpsc::UnboundedReceiver<String>, session_id: &str) {
    live.lock().expect("live").remove(session_id);
    let mut dropped = 0_usize;
    while steer_rx.try_recv().is_ok() {
        dropped += 1;
    }
    if dropped > 0 {
        warn!(
            session_id,
            dropped, "dropping queued steers as the job finishes"
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobState {
    Running,
    Finished,
    Failed,
    OverBudget,
}

struct JobStatus {
    role: SessionRole,
    project: String,
    state: JobState,
    spent_tokens: u64,
    budget: Option<Budget>,
    started: Instant,
    /// Frozen at the terminal transition; `None` while the job is running,
    /// when `list` reports `started.elapsed()` instead.
    elapsed: Option<Duration>,
    /// Last write wins: bumped on start and again on the terminal
    /// transition, so "newest first" within a state needs no extra clock.
    ordinal: u64,
}

impl JobStatus {
    fn to_job_info(&self, session_id: &str) -> JobInfo {
        let elapsed = self.elapsed.unwrap_or_else(|| self.started.elapsed());
        let state = match self.state {
            JobState::Running => job_info::State::Running,
            JobState::Finished => job_info::State::Finished,
            JobState::Failed => job_info::State::Failed,
            JobState::OverBudget => job_info::State::OverBudget,
        };
        JobInfo {
            session_id: session_id.to_owned(),
            role: self.role as i32,
            project: self.project.clone(),
            state: state as i32,
            spent_tokens: self.spent_tokens,
            budget_tokens: self.budget.as_ref().map_or(0, |budget| budget.total_tokens),
            elapsed_seconds: u32::try_from(elapsed.as_secs()).unwrap_or(u32::MAX),
            budget_seconds: self
                .budget
                .as_ref()
                .map_or(0, |budget| budget.wall_clock_seconds),
            title: String::new(),
        }
    }
}

/// Job status, kept separate from `live`: a finished job's steer channel is
/// torn down immediately, but its status survives until eviction, so the
/// two have different lifetimes over the same job.
struct JobStatuses {
    entries: Mutex<HashMap<String, JobStatus>>,
    /// Insertion order of terminal jobs only, for the eviction cap: each
    /// job reaches a terminal state at most once, so this never double-counts.
    terminal_order: Mutex<VecDeque<String>>,
    ordinal: AtomicU64,
}

impl JobStatuses {
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            terminal_order: Mutex::new(VecDeque::new()),
            ordinal: AtomicU64::new(0),
        }
    }

    fn next_ordinal(&self) -> u64 {
        self.ordinal.fetch_add(1, Ordering::Relaxed)
    }

    fn start(&self, job: &DispatchedJob) -> JobInfo {
        let entry = JobStatus {
            role: job.role,
            project: job.project.clone(),
            state: JobState::Running,
            spent_tokens: 0,
            budget: job.budget,
            started: Instant::now(),
            elapsed: None,
            ordinal: self.next_ordinal(),
        };
        let info = entry.to_job_info(&job.session_id);
        self.entries
            .lock()
            .expect("statuses")
            .insert(job.session_id.clone(), entry);
        info
    }

    fn record_tokens(&self, session_id: &str, spent_tokens: u64) -> Option<JobInfo> {
        let mut entries = self.entries.lock().expect("statuses");
        let entry = entries.get_mut(session_id)?;
        entry.spent_tokens = spent_tokens;
        Some(entry.to_job_info(session_id))
    }

    fn finish(&self, session_id: &str, state: JobState, elapsed: Duration) -> Option<JobInfo> {
        let ordinal = self.next_ordinal();
        let info = {
            let mut entries = self.entries.lock().expect("statuses");
            let entry = entries.get_mut(session_id)?;
            entry.state = state;
            entry.elapsed = Some(elapsed);
            entry.ordinal = ordinal;
            entry.to_job_info(session_id)
        };

        let mut terminal_order = self.terminal_order.lock().expect("terminal_order");
        terminal_order.push_back(session_id.to_owned());
        if terminal_order.len() > MAX_TERMINAL_JOBS {
            if let Some(oldest) = terminal_order.pop_front() {
                self.entries.lock().expect("statuses").remove(&oldest);
            }
        }
        Some(info)
    }

    fn list(&self) -> Vec<JobInfo> {
        let entries = self.entries.lock().expect("statuses");
        let mut listed: Vec<(&String, &JobStatus)> = entries.iter().collect();
        listed.sort_by(|(_, a), (_, b)| {
            let a_terminal = a.state != JobState::Running;
            let b_terminal = b.state != JobState::Running;
            a_terminal
                .cmp(&b_terminal)
                .then_with(|| b.ordinal.cmp(&a.ordinal))
        });
        listed
            .into_iter()
            .map(|(session_id, status)| status.to_job_info(session_id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arc_core::log::Log;
    use arc_core::projection::Projection;
    use arc_core::provider::{CompletionDelta, Error as ProviderError, Provider, Stop, Thinking};
    use arc_core::session::ProjectSpec;
    use arc_core::store::Store;
    use arc_core::testkit::{
        ScriptedProvider, Step, appended, call, done_reply, replay_log, runner, tool_stop, usage,
    };
    use arc_core::tool::workspace::{Grant, Mode};
    use arc_core::tool::{Registry, ToolSource};
    use arc_proto::v1::Role;
    use tempfile::TempDir;

    fn executor_runner(provider: &Arc<ScriptedProvider>) -> Runner {
        Runner {
            role: SessionRole::Executor,
            provider: Arc::clone(provider) as Arc<dyn Provider>,
            model: "exec-model".to_owned(),
            thinking: Thinking::Default,
            system: None,
        }
    }

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

    fn child_session(engine: &Engine, concierge: &Arc<ScriptedProvider>) -> String {
        engine
            .create_bound_session(&runner(concierge), "arc", SessionRole::Executor, None)
            .expect("create the child durably, as dispatch already does")
    }

    /// A stand-in for whatever session dispatched the job, so a test can
    /// read its handback the same way `child_session` stands in for the job.
    fn parent_session(engine: &Engine, concierge: &Arc<ScriptedProvider>) -> String {
        engine
            .create_bound_session(&runner(concierge), "arc", SessionRole::Concierge, None)
            .expect("create the parent durably")
    }

    fn engine_for_project(dir: &TempDir, root: &std::path::Path) -> Arc<Engine> {
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        Arc::new(
            Engine::new(Store::new(log, projection), Registry::new(512)).with_projects(
                BTreeMap::from([(
                    "arc".to_owned(),
                    ProjectSpec {
                        sources: Vec::new(),
                        grants: vec![Grant::new(root, Mode::ReadWrite)],
                    },
                )]),
            ),
        )
    }

    fn engine_for_project_notified(
        dir: &TempDir,
        root: &std::path::Path,
        notifier: broadcast::Sender<Notification>,
    ) -> Arc<Engine> {
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        Arc::new(
            Engine::new(Store::new(log, projection), Registry::new(512))
                .with_projects(BTreeMap::from([(
                    "arc".to_owned(),
                    ProjectSpec {
                        sources: Vec::new(),
                        grants: vec![Grant::new(root, Mode::ReadWrite)],
                    },
                )]))
                .with_notifier(notifier),
        )
    }

    /// The next `job_changed` notification, skipping any `session_appended`
    /// pushes (e.g. from `child_session`'s own durable creation) in between.
    async fn job_changed(notifications: &mut broadcast::Receiver<Notification>) -> JobInfo {
        loop {
            let received = tokio::time::timeout(Duration::from_secs(5), notifications.recv())
                .await
                .expect("a notification within the timeout")
                .expect("the notifier stays open");
            if let Some(notification::Event::JobChanged(info)) = received.event {
                return info;
            }
        }
    }

    fn child_user_messages(dir: &std::path::Path, session_id: &str) -> Vec<(Role, String)> {
        replay_log(dir)
            .into_iter()
            .filter_map(|event| match event {
                arc_proto::v1::session_event::Event::MessageAppended(m)
                    if m.session_id == session_id =>
                {
                    Some((Role::try_from(m.role).expect("a known role"), m.content))
                }
                _ => None,
            })
            .collect()
    }

    /// Polls the log rather than sleeping a fixed duration: waits for actual
    /// state, not a guessed timing window.
    async fn wait_for_message_count(dir: &std::path::Path, session_id: &str, want: usize) {
        for _ in 0..400 {
            if child_user_messages(dir, session_id).len() >= want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for {want} messages in session {session_id}");
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
    async fn a_failed_turn_drops_queued_steers_and_removes_the_live_entry() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: Vec::new(),
            notify: Arc::clone(&notify),
            after: vec![Err(ProviderError::InvalidRequest("boom".to_owned()))],
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

        // queued while the failing turn is still gated open
        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(supervisor.steer(&child_id, "too late"));
        notify.notify_one();
        supervisor.shutdown().await;

        assert!(
            !supervisor.steer(&child_id, "later still"),
            "a failed job removed its live entry; nothing is left to read a steer"
        );
        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [(Role::User, "fix the failing test".to_owned())],
            "the queued steer was dropped, not processed, after the failed turn"
        );
    }

    #[tokio::test]
    async fn a_token_budget_smaller_than_the_brief_turns_usage_stops_the_job_after_that_turn() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("working".to_owned()))],
            notify: Arc::clone(&notify),
            after: vec![Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            })],
        }]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        // usage() reports 8 tokens combined; a cap of 5 is over budget as
        // soon as the brief turn lands, before any steer is even queued
        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: Some(Budget {
                total_tokens: 5,
                wall_clock_seconds: 0,
            }),
        });

        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(
            supervisor.steer(&child_id, "too late"),
            "the job is still live when queued"
        );
        notify.notify_one();
        supervisor.shutdown().await;

        assert!(
            !supervisor.steer(&child_id, "later still"),
            "the over-budget job removed its live entry"
        );
        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "working".to_owned()),
            ],
            "the brief turn ran to completion; the queued steer was dropped, not processed"
        );
    }

    #[tokio::test]
    async fn a_generous_token_budget_does_not_trip_and_steers_run_normally() {
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
            budget: Some(Budget {
                total_tokens: 100_000,
                wall_clock_seconds: 3600,
            }),
        });

        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(supervisor.steer(&child_id, "also check the linter"));
        notify.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "on it".to_owned()),
                (Role::User, "also check the linter".to_owned()),
                (Role::Assistant, "steer reply".to_owned()),
            ],
            "a budget with plenty of headroom never trips"
        );
    }

    #[tokio::test]
    async fn a_wall_clock_only_budget_stops_the_job_once_elapsed_time_exceeds_it() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        tokio::time::pause();

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("first".to_owned()))],
            notify: Arc::clone(&notify),
            after: vec![Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            })],
        }]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        // total_tokens: 0 means the token dimension is unlimited; only
        // wall-clock is enforced
        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: Some(Budget {
                total_tokens: 0,
                wall_clock_seconds: 1,
            }),
        });

        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(
            supervisor.steer(&child_id, "too late"),
            "the job is still live when queued"
        );
        // the job task is gated on a Notify, not a timer, so advancing the
        // paused clock here only changes what its Instant::elapsed() later
        // reports, it does not let the task run ahead
        tokio::time::advance(Duration::from_secs(2)).await;
        notify.notify_one();
        supervisor.shutdown().await;

        assert!(
            !supervisor.steer(&child_id, "later still"),
            "the over-budget job removed its live entry"
        );
        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "first".to_owned()),
            ],
            "the brief turn ran to completion; the queued steer was dropped, not processed"
        );
    }

    #[tokio::test]
    async fn usage_accumulates_across_turns_and_stops_the_job_once_the_combined_total_crosses_the_cap()
     {
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
        ]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        // usage() reports 8 tokens per turn: the brief alone (8) stays under
        // a cap of 10, but the brief plus the first steer (16) crosses it,
        // so the check before the second steer is what stops the job
        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: Some(Budget {
                total_tokens: 10,
                wall_clock_seconds: 0,
            }),
        });

        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(supervisor.steer(&child_id, "first steer"));
        assert!(supervisor.steer(&child_id, "second steer"));
        notify.notify_one();
        supervisor.shutdown().await;

        assert!(
            !supervisor.steer(&child_id, "third steer"),
            "the over-budget job removed its live entry"
        );
        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "on it".to_owned()),
                (Role::User, "first steer".to_owned()),
                (Role::Assistant, "first steer done".to_owned()),
            ],
            "the first steer ran; the second never did once the combined usage crossed the cap"
        );
    }

    #[tokio::test]
    async fn a_clean_finish_records_a_handback_with_the_childs_final_reply() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("all fixed")]);

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

        assert_eq!(
            child_user_messages(dir.path(), &parent_id),
            [(Role::User, format!("Job {child_id} finished.\nall fixed"))],
            "the handback names the child and carries its final reply"
        );
    }

    #[tokio::test]
    async fn a_failed_turn_records_a_stopped_handback_naming_the_turn_failure() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![vec![Err(
            ProviderError::InvalidRequest("boom".to_owned()),
        )]]);

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

        assert_eq!(
            child_user_messages(dir.path(), &parent_id),
            [(
                Role::User,
                format!("Job {child_id} stopped: the turn failed.\n{NO_REPLY}")
            )],
            "the failure never produced any assistant text, so the summary falls back"
        );
    }

    #[tokio::test]
    async fn an_over_budget_finish_records_a_stopped_handback_naming_the_numbers() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("partial progress")]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        // usage() reports 8 tokens combined; a cap of 5 is over budget as
        // soon as the brief turn lands
        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: Some(Budget {
                total_tokens: 5,
                wall_clock_seconds: 0,
            }),
        });
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &parent_id),
            [(
                Role::User,
                "Job ".to_owned()
                    + &child_id
                    + " stopped: token budget exhausted (8/5).\npartial progress"
            )],
            "the reason names the spent and allowed token counts"
        );
    }

    #[tokio::test]
    async fn a_clean_finish_with_no_assistant_text_falls_back_to_the_no_reply_line() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![vec![Ok(CompletionDelta::Done {
            usage: usage(),
            stop: Stop::EndTurn,
        })]]);

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

        assert_eq!(
            child_user_messages(dir.path(), &parent_id),
            [(Role::User, format!("Job {child_id} finished.\n{NO_REPLY}"))],
            "an empty assistant reply reads the same as no reply at all"
        );
    }

    fn only_job(listed: Vec<JobInfo>) -> JobInfo {
        assert_eq!(listed.len(), 1, "got: {listed:?}");
        listed.into_iter().next().expect("one job")
    }

    #[tokio::test]
    async fn list_shows_a_running_job_with_its_live_elapsed_and_the_tokens_spent_so_far() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let first_gate = Arc::new(tokio::sync::Notify::new());
        let second_gate = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("on it".to_owned()))],
                notify: Arc::clone(&first_gate),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Gated {
                before: Vec::new(),
                notify: Arc::clone(&second_gate),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
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
        assert!(supervisor.steer(&child_id, "also check the linter"));
        first_gate.notify_one();
        // the steer is now its own gated turn: the brief turn's reply plus
        // the steer's own user message have both landed, so its usage is
        // counted and the job is provably still running, not yet finished
        wait_for_message_count(dir.path(), &child_id, 3).await;

        let job = only_job(supervisor.list());
        assert_eq!(job.session_id, child_id);
        assert_eq!(job.role, SessionRole::Executor as i32);
        assert_eq!(job.project, "arc");
        assert_eq!(job.state, job_info::State::Running as i32);
        assert_eq!(
            job.spent_tokens,
            u64::from(usage().input_tokens) + u64::from(usage().output_tokens)
        );
        assert_eq!(
            job.budget_tokens, 0,
            "no budget means unlimited, not zero spent"
        );

        second_gate.notify_one();
        supervisor.shutdown().await;
    }

    struct FixedTitle;

    impl arc_core::consolidation::Extractor for FixedTitle {
        async fn extract(
            &self,
            _session: &arc_core::consolidation::SessionSnapshot,
        ) -> Result<Vec<arc_proto::v1::memory_event::Event>, arc_core::consolidation::ExtractError>
        {
            Ok(Vec::new())
        }

        async fn title(
            &self,
            _session: &arc_core::consolidation::SessionSnapshot,
        ) -> Result<Option<String>, arc_core::consolidation::ExtractError> {
            Ok(Some("Fix the failing test".to_owned()))
        }
    }

    #[tokio::test]
    async fn list_carries_the_title_once_the_projection_has_one() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("all fixed")]);

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

        assert_eq!(only_job(supervisor.list()).title, "", "not titled yet");

        arc_core::consolidation::run_pass(
            &engine,
            &FixedTitle,
            i64::MAX,
            "",
            &std::collections::HashSet::new(),
        )
        .await
        .expect("pass");

        assert_eq!(only_job(supervisor.list()).title, "Fix the failing test");
    }

    #[tokio::test]
    async fn a_finished_job_retains_its_state_with_a_frozen_elapsed() {
        tokio::time::pause();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("all fixed")]);

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

        let just_after = only_job(supervisor.list());
        assert_eq!(just_after.state, job_info::State::Finished as i32);
        assert_eq!(
            just_after.spent_tokens,
            u64::from(usage().input_tokens) + u64::from(usage().output_tokens)
        );

        tokio::time::advance(Duration::from_secs(30)).await;
        let later = only_job(supervisor.list());
        assert_eq!(
            later.elapsed_seconds, just_after.elapsed_seconds,
            "a finished job's elapsed time is frozen, not still ticking"
        );
    }

    #[tokio::test]
    async fn a_failed_job_reports_the_failed_state() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![vec![Err(
            ProviderError::InvalidRequest("boom".to_owned()),
        )]]);

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

        let job = only_job(supervisor.list());
        assert_eq!(job.state, job_info::State::Failed as i32);
        assert_eq!(job.spent_tokens, 0, "the failed turn reported no usage");
    }

    #[tokio::test]
    async fn an_over_budget_job_reports_the_over_budget_state() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("partial progress")]);

        let engine = engine_for_project(&dir, &root);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        // usage() reports 8 tokens combined; a cap of 5 is over budget as
        // soon as the brief turn lands
        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: Some(Budget {
                total_tokens: 5,
                wall_clock_seconds: 0,
            }),
        });
        supervisor.shutdown().await;

        let job = only_job(supervisor.list());
        assert_eq!(job.state, job_info::State::OverBudget as i32);
        assert_eq!(job.spent_tokens, 8);
        assert_eq!(job.budget_tokens, 5);
    }

    #[tokio::test]
    async fn eviction_keeps_at_most_the_terminal_cap_of_finished_jobs() {
        const SPAWNED: usize = MAX_TERMINAL_JOBS + 5;

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider =
            ScriptedProvider::scripted((0..SPAWNED).map(|_| done_reply("ok")).collect());

        let engine = engine_for_project(&dir, &root);
        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        for i in 0..SPAWNED {
            let child_id = child_session(&engine, &concierge_provider);
            supervisor.spawn(DispatchedJob {
                session_id: child_id,
                parent_session: "s-parent".to_owned(),
                role: SessionRole::Executor,
                project: "arc".to_owned(),
                brief: format!("job {i}"),
                budget: None,
            });
        }
        supervisor.shutdown().await;

        let listed = supervisor.list();
        assert_eq!(
            listed.len(),
            MAX_TERMINAL_JOBS,
            "eviction caps retained terminal entries at the const, not the number spawned"
        );
        assert!(
            listed
                .iter()
                .all(|job| job.state == job_info::State::Finished as i32),
            "every spawned job in this test finishes cleanly"
        );
    }

    #[tokio::test]
    async fn spawning_a_job_broadcasts_a_running_job_changed_notification() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("on it")]);
        let (notifier, mut notifications) = broadcast::channel(16);

        let engine = engine_for_project_notified(&dir, &root, notifier.clone());
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners).with_notifier(notifier);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });

        let job = job_changed(&mut notifications).await;
        assert_eq!(job.session_id, child_id);
        assert_eq!(job.state, job_info::State::Running as i32);

        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn a_clean_finish_broadcasts_a_finished_job_changed_notification() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("all fixed")]);
        let (notifier, mut notifications) = broadcast::channel(16);

        let engine = engine_for_project_notified(&dir, &root, notifier.clone());
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners).with_notifier(notifier);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        let mut job = job_changed(&mut notifications).await;
        while job.state != job_info::State::Finished as i32 {
            job = job_changed(&mut notifications).await;
        }
        assert_eq!(job.session_id, child_id);
    }

    #[tokio::test]
    async fn a_failed_turn_broadcasts_a_failed_job_changed_notification() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![vec![Err(
            ProviderError::InvalidRequest("boom".to_owned()),
        )]]);
        let (notifier, mut notifications) = broadcast::channel(16);

        let engine = engine_for_project_notified(&dir, &root, notifier.clone());
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners).with_notifier(notifier);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        let mut job = job_changed(&mut notifications).await;
        while job.state != job_info::State::Failed as i32 {
            job = job_changed(&mut notifications).await;
        }
        assert_eq!(job.session_id, child_id);
    }

    #[tokio::test]
    async fn an_over_budget_finish_broadcasts_an_over_budget_job_changed_notification() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("partial progress")]);
        let (notifier, mut notifications) = broadcast::channel(16);

        let engine = engine_for_project_notified(&dir, &root, notifier.clone());
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners).with_notifier(notifier);

        // usage() reports 8 tokens combined; a cap of 5 is over budget as
        // soon as the brief turn lands
        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: Some(Budget {
                total_tokens: 5,
                wall_clock_seconds: 0,
            }),
        });
        supervisor.shutdown().await;

        let mut job = job_changed(&mut notifications).await;
        while job.state != job_info::State::OverBudget as i32 {
            job = job_changed(&mut notifications).await;
        }
        assert_eq!(job.session_id, child_id);
        assert_eq!(job.spent_tokens, 8);
    }

    fn last_assistant(dir: &std::path::Path, session_id: &str) -> Option<String> {
        child_user_messages(dir, session_id)
            .into_iter()
            .rev()
            .find(|(role, _)| *role == Role::Assistant)
            .map(|(_, content)| content)
    }

    #[tokio::test]
    async fn a_clean_finish_triggers_a_concierge_turn_that_reacts_to_the_handback() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![done_reply("the job did X")]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("all fixed")]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_concierge(runner(&concierge_provider));

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &parent_id),
            [
                (Role::User, format!("Job {child_id} finished.\nall fixed")),
                (Role::Assistant, "the job did X".to_owned()),
            ],
            "the handback lands, then the concierge's own turn reacts to it"
        );
    }

    #[tokio::test]
    async fn a_failed_jobs_handback_also_triggers_a_concierge_turn() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![done_reply("noted the failure")]);
        let executor_provider = ScriptedProvider::scripted(vec![vec![Err(
            ProviderError::InvalidRequest("boom".to_owned()),
        )]]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_concierge(runner(&concierge_provider));

        supervisor.spawn(DispatchedJob {
            session_id: child_id,
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        assert_eq!(
            last_assistant(dir.path(), &parent_id),
            Some("noted the failure".to_owned()),
            "a failed job's handback gets a concierge turn too"
        );
    }

    #[tokio::test]
    async fn a_handback_whose_parent_is_not_a_concierge_session_gets_no_auto_turn() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        // must never be called: the parent here stands in for a future
        // nested job, not a concierge
        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("done")]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = engine
            .create_bound_session(
                &runner(&concierge_provider),
                "arc",
                SessionRole::Executor,
                None,
            )
            .expect("create the parent durably");
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_concierge(runner(&concierge_provider));

        supervisor.spawn(DispatchedJob {
            session_id: child_id,
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &parent_id).len(),
            1,
            "the handback landed but no concierge turn followed"
        );
        assert!(
            concierge_provider.requests().is_empty(),
            "an executor parent never drives the concierge provider"
        );
    }

    // record_handback's append and continue_session's turn share the
    // parent's guard, so a second handback can only race for `pending` in
    // the instant between them, not while a gated turn is stalled; this
    // drives the same `claim_pending` a real second handback would call.
    #[tokio::test]
    async fn a_handback_arriving_while_one_is_pending_collapses_and_marks_dirty() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]); // must never be called
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("job done")]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_concierge(runner(&concierge_provider));
        let handback = supervisor.handback.clone().expect("concierge wired");

        assert!(
            claim_pending(&handback, &parent_id),
            "simulates a handback turn already running for this parent"
        );

        supervisor.spawn(DispatchedJob {
            session_id: child_id,
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &parent_id).len(),
            1,
            "the handback still lands durably even though its turn collapsed"
        );
        assert!(
            concierge_provider.requests().is_empty(),
            "the collapsed handback never ran its own turn"
        );
        assert!(
            handback.dirty.lock().expect("dirty").contains(&parent_id),
            "the collapse set the catch-up flag instead of running a second turn"
        );
        assert!(
            release_or_rerun(&handback, &parent_id),
            "a dirty flag means the pending turn's own task reruns once more"
        );
        assert!(
            !release_or_rerun(&handback, &parent_id),
            "nothing landed since: the second release ends the chain"
        );
    }

    #[tokio::test]
    async fn a_forced_autonomy_cap_skips_the_concierge_turn_and_appends_nothing_extra() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]); // must never be called
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("done")]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_concierge(runner(&concierge_provider));
        supervisor
            .handback
            .as_ref()
            .expect("concierge wired")
            .autonomy
            .lock()
            .expect("autonomy")
            .insert(parent_id.clone(), MAX_HANDBACK_TURNS);

        supervisor.spawn(DispatchedJob {
            session_id: child_id,
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &parent_id).len(),
            1,
            "only the handback landed; the capped concierge turn never ran"
        );
        assert!(
            concierge_provider.requests().is_empty(),
            "the capped provider was never called"
        );
    }

    #[tokio::test]
    async fn reset_autonomy_lets_the_next_handback_run_after_a_forced_cap() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider =
            ScriptedProvider::scripted(vec![done_reply("the concierge reacts")]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("done")]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_concierge(runner(&concierge_provider));
        supervisor
            .handback
            .as_ref()
            .expect("concierge wired")
            .autonomy
            .lock()
            .expect("autonomy")
            .insert(parent_id.clone(), MAX_HANDBACK_TURNS);

        supervisor.reset_autonomy(&parent_id);

        supervisor.spawn(DispatchedJob {
            session_id: child_id,
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        assert_eq!(
            last_assistant(dir.path(), &parent_id),
            Some("the concierge reacts".to_owned()),
            "resetting the counter let the handback turn run again"
        );
    }

    #[tokio::test]
    async fn a_handback_turn_that_dispatches_spawns_the_chained_child() {
        let dispatch_args = serde_json::json!({
            "role": "executor",
            "project": "arc",
            "brief": "second link",
            "budget_tokens": 0,
            "budget_minutes": 0,
        })
        .to_string();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("d2", 0, "dispatch", &dispatch_args)),
                Ok(tool_stop()),
            ],
            done_reply("chained"),
        ]);
        let executor_provider = ScriptedProvider::scripted(vec![
            done_reply("first job done"),
            done_reply("second job done"),
        ]);

        let mut registry = Registry::new(512);
        registry.register(Box::new(arc_core::tool::builtin::dispatch::Dispatch::new(
            vec!["arc".to_owned()],
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
                },
            )])),
        );

        let parent_id = engine
            .create_bound_session(
                &runner(&concierge_provider),
                "arc",
                SessionRole::Concierge,
                None,
            )
            .expect("create the parent durably");
        let first_child = engine
            .create_bound_session(
                &runner(&concierge_provider),
                "arc",
                SessionRole::Executor,
                None,
            )
            .expect("create the first child durably");

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_concierge(runner(&concierge_provider));

        supervisor.spawn(DispatchedJob {
            session_id: first_child.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        let events = replay_log(dir.path());
        let chained_child = events
            .iter()
            .find_map(|event| match event {
                arc_proto::v1::session_event::Event::SessionCreated(created)
                    if created.role == SessionRole::Executor as i32
                        && created.session_id != first_child =>
                {
                    Some(created.session_id.clone())
                }
                _ => None,
            })
            .expect("the handback turn's dispatch created a second child durably");

        let chained_messages: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                arc_proto::v1::session_event::Event::MessageAppended(m)
                    if m.session_id == chained_child =>
                {
                    Some(m)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            chained_messages.len(),
            2,
            "the chained job ran its own turn"
        );
        assert_eq!(chained_messages[0].content, "second link");
        assert_eq!(chained_messages[1].content, "second job done");

        assert_eq!(
            last_assistant(dir.path(), &parent_id),
            Some("chained".to_owned()),
            "the handback turn's own reply landed after it dispatched"
        );
    }

    #[tokio::test]
    async fn a_jobs_own_dispatch_spawns_and_runs_the_grandchild() {
        let dispatch_args = serde_json::json!({
            "role": "executor",
            "project": "arc",
            "brief": "grandchild work",
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
            vec!["arc".to_owned()],
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

    fn continue_job_args(session_id: &str, message: &str) -> String {
        serde_json::json!({
            "session_id": session_id,
            "message": message,
        })
        .to_string()
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
        live.lock()
            .expect("live")
            .insert("s-child".to_owned(), steer_tx);

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
            &live,
            &statuses,
            None,
            None,
            &handles,
            true,
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
    async fn a_handback_turn_that_calls_continue_job_resumes_the_finished_job() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let mut registry = Registry::new(512);
        registry.register(Box::new(arc_core::tool::builtin::continue_job::ContinueJob));
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(
            Engine::new(Store::new(log, projection), registry).with_projects(BTreeMap::from([(
                "arc".to_owned(),
                ProjectSpec {
                    sources: vec![ToolSource::Builtin],
                    grants: vec![Grant::new(&root, Mode::ReadWrite)],
                },
            )])),
        );

        // a throwaway provider: session creation never drives it
        let bootstrap_provider = ScriptedProvider::scripted(vec![]);
        let parent_id = engine
            .create_bound_session(
                &runner(&bootstrap_provider),
                "arc",
                SessionRole::Concierge,
                None,
            )
            .expect("create the parent durably");
        let first_child = engine
            .create_bound_session(
                &runner(&bootstrap_provider),
                "arc",
                SessionRole::Executor,
                None,
            )
            .expect("create the first child durably");

        let concierge_provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call(
                    "c2",
                    0,
                    "continue_job",
                    &continue_job_args(&first_child, "second link"),
                )),
                Ok(tool_stop()),
            ],
            done_reply("chained"),
            // the resumed job's own finish drives a second, independent
            // handback turn on the same parent
            done_reply("noted"),
        ]);
        let executor_provider = ScriptedProvider::scripted(vec![
            done_reply("first job done"),
            done_reply("second job done"),
        ]);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_concierge(runner(&concierge_provider));

        supervisor.spawn(DispatchedJob {
            session_id: first_child.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &first_child),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "first job done".to_owned()),
                (Role::User, "second link".to_owned()),
                (Role::Assistant, "second job done".to_owned()),
            ],
            "the handback turn's continue_job resumed the finished job in place"
        );
        assert_eq!(
            last_assistant(dir.path(), &parent_id),
            Some("noted".to_owned()),
            "the resumed job's own handback drove one more concierge turn"
        );
    }
}
