use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_core::provider::{Usage, role_label};
use arc_core::session::{DispatchedJob, Engine, Runner};
use arc_proto::v1::{Budget, SessionRole};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info, warn};

const EVENT_BUFFER: usize = 64;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
const NO_REPLY: &str = "(the job produced no reply)";

type LiveMap = Mutex<HashMap<String, mpsc::UnboundedSender<String>>>;

/// Runs the jobs `dispatch` created. One tokio task per job, driven to
/// completion with `send_message`; nothing restarts a job that fails.
pub struct Supervisor {
    engine: Arc<Engine>,
    runners: BTreeMap<SessionRole, Runner>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    /// `session_id` -> steer sender, for jobs currently running. `steer`
    /// sends under this lock; a job task removes its own entry under the
    /// same lock, so an enqueue can never land in a channel nobody will read.
    live: Arc<LiveMap>,
}

impl Supervisor {
    pub fn new(engine: Arc<Engine>, runners: BTreeMap<SessionRole, Runner>) -> Self {
        Self {
            engine,
            runners,
            handles: Mutex::new(Vec::new()),
            live: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn spawn(&self, job: DispatchedJob) {
        let Some(runner) = self.runners.get(&job.role).cloned() else {
            warn!(
                session_id = %job.session_id,
                role = role_label(job.role),
                "dispatched job names a role with no runner; skipping"
            );
            return;
        };
        let (steer_tx, steer_rx) = mpsc::unbounded_channel();
        self.live
            .lock()
            .expect("live")
            .insert(job.session_id.clone(), steer_tx);
        let engine = Arc::clone(&self.engine);
        let live = Arc::clone(&self.live);
        let handle = tokio::spawn(run_job(engine, runner, job, steer_rx, live));
        self.handles.lock().expect("handles").push(handle);
    }

    /// Enqueues a steering message for a live job. `true` if the job was
    /// live and the message was enqueued; `false` otherwise, meaning the
    /// caller should fall through to a normal turn.
    pub fn steer(&self, session_id: &str, text: &str) -> bool {
        let live = self.live.lock().expect("live");
        live.get(session_id)
            .is_some_and(|sender| sender.send(text.to_owned()).is_ok())
    }

    /// Gives outstanding jobs a grace period to finish, then abandons them.
    pub async fn shutdown(&self) {
        let handles: Vec<_> = self.handles.lock().expect("handles").drain(..).collect();
        if handles.is_empty() {
            return;
        }
        let draining = futures::future::join_all(handles);
        if tokio::time::timeout(SHUTDOWN_GRACE, draining)
            .await
            .is_err()
        {
            warn!("shutdown grace expired; abandoning outstanding jobs");
        }
    }
}

async fn run_job(
    engine: Arc<Engine>,
    runner: Runner,
    job: DispatchedJob,
    mut steer_rx: mpsc::UnboundedReceiver<String>,
    live: Arc<LiveMap>,
) {
    let session_id = job.session_id.clone();
    let start = Instant::now();
    let mut spent_tokens: u64 = 0;

    match run_turn(&engine, &runner, &session_id, &job.brief).await {
        TurnOutcome::Success(usage) => spent_tokens += usage_tokens(usage),
        TurnOutcome::Failure => {
            finish_now(&live, &mut steer_rx, &session_id);
            handback_failed(&engine, &job).await;
            return;
        }
    }

    loop {
        if let Some(breach) = budget_breach(job.budget.as_ref(), spent_tokens, start.elapsed()) {
            warn_over_budget(&session_id, &breach);
            finish_now(&live, &mut steer_rx, &session_id);
            handback_over_budget(&engine, &job, &breach).await;
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
            TurnOutcome::Success(usage) => spent_tokens += usage_tokens(usage),
            TurnOutcome::Failure => {
                finish_now(&live, &mut steer_rx, &session_id);
                handback_failed(&engine, &job).await;
                return;
            }
        }
    }

    handback_clean(&engine, &job).await;
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

async fn record_handback(engine: &Engine, job: &DispatchedJob, reason: Option<&str>) {
    let summary = job_summary(engine, job);
    if let Err(error) = engine
        .record_handback(&job.parent_session, &job.session_id, reason, &summary)
        .await
    {
        warn!(
            parent_session = %job.parent_session,
            session_id = %job.session_id,
            %error,
            "failed to record the job's handback into the parent session"
        );
    }
}

async fn handback_clean(engine: &Engine, job: &DispatchedJob) {
    record_handback(engine, job, None).await;
}

async fn handback_failed(engine: &Engine, job: &DispatchedJob) {
    record_handback(engine, job, Some("the turn failed")).await;
}

async fn handback_over_budget(engine: &Engine, job: &DispatchedJob, breach: &BudgetBreach) {
    let reason = match breach {
        BudgetBreach::Tokens { spent, allowed } => {
            format!("token budget exhausted ({spent}/{allowed})")
        }
        BudgetBreach::WallClock { elapsed, allowed } => {
            format!("time budget exhausted ({elapsed}s/{allowed}s)")
        }
    };
    record_handback(engine, job, Some(&reason)).await;
}

/// A completed turn's outcome. A failed turn ends the job, so the caller
/// stops draining steers; a successful turn carries whatever usage the
/// provider reported, which may itself be absent.
enum TurnOutcome {
    Success(Option<Usage>),
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
            TurnOutcome::Success(reply.usage)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arc_core::log::Log;
    use arc_core::projection::Projection;
    use arc_core::provider::{CompletionDelta, Error as ProviderError, Provider, Stop, Thinking};
    use arc_core::session::ProjectSpec;
    use arc_core::store::Store;
    use arc_core::testkit::{
        ScriptedProvider, Step, appended, done_reply, replay_log, runner, usage,
    };
    use arc_core::tool::Registry;
    use arc_core::tool::workspace::{Grant, Mode};
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
}
