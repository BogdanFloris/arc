use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_core::session::{DispatchedJob, Engine};
use arc_proto::v1::{Budget, JobInfo, Notification, SessionRole, job_info, notification};
use tokio::sync::broadcast;
use tokio::time::Instant;

use super::handback::job_title;

/// Terminal job statuses retained after a job's task ends. Live daemon
/// memory only: rebuilt empty on restart, unlike the child session itself.
const MAX_TERMINAL_JOBS: usize = 50;

/// How long a terminal job stays in `list()` after it finishes, so the
/// list reads as "what's going on", not an ever-growing history.
const TERMINAL_TTL: Duration = Duration::from_secs(600);

pub(super) fn notify_job_changed(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JobState {
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
    elapsed: Option<Duration>,
    /// Last write wins: bumped on start and again on the terminal
    /// transition, so "newest first" within a state needs no extra clock.
    ordinal: u64,
    finished_at: Option<Instant>,
    /// Tool calls issued so far, one per started call; 0 reads as
    /// "thinking" in the strip until the first tool step.
    tool_steps: u32,
    /// The latest engine event — delta, reasoning, or a tool call — that
    /// the strip's idle readout counts from; the client ticks it forward
    /// locally between pushes.
    last_engine_event: Instant,
    parent_session: String,
    queued_steers: u32,
    last_call: String,
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
            tool_steps: self.tool_steps,
            idle_seconds: u32::try_from(self.last_engine_event.elapsed().as_secs())
                .unwrap_or(u32::MAX),
            parent_session: self.parent_session.clone(),
            queued_steers: self.queued_steers,
            last_call: self.last_call.clone(),
        }
    }
}

/// Job status, kept separate from `live`: a finished job's steer channel is
/// torn down immediately, but its status survives until eviction, so the
/// two have different lifetimes over the same job.
pub(super) struct JobStatuses {
    entries: Mutex<HashMap<String, JobStatus>>,
    /// Insertion order of terminal jobs only, for the eviction cap: each
    /// job reaches a terminal state at most once, so this never double-counts.
    terminal_order: Mutex<VecDeque<String>>,
    ordinal: AtomicU64,
}

impl JobStatuses {
    pub(super) fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            terminal_order: Mutex::new(VecDeque::new()),
            ordinal: AtomicU64::new(0),
        }
    }

    fn next_ordinal(&self) -> u64 {
        self.ordinal.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn start(&self, job: &DispatchedJob, initial_spent_tokens: u64) -> JobInfo {
        let entry = JobStatus {
            role: job.role,
            project: job.project.clone(),
            state: JobState::Running,
            spent_tokens: initial_spent_tokens,
            budget: job.budget,
            started: Instant::now(),
            elapsed: None,
            ordinal: self.next_ordinal(),
            finished_at: None,
            tool_steps: 0,
            last_engine_event: Instant::now(),
            parent_session: job.parent_session.clone(),
            queued_steers: 0,
            last_call: String::new(),
        };
        let info = entry.to_job_info(&job.session_id);
        self.entries
            .lock()
            .expect("statuses")
            .insert(job.session_id.clone(), entry);
        info
    }

    pub(super) fn record_tokens(&self, session_id: &str, spent_tokens: u64) -> Option<JobInfo> {
        let mut entries = self.entries.lock().expect("statuses");
        let entry = entries.get_mut(session_id)?;
        entry.spent_tokens = spent_tokens;
        Some(entry.to_job_info(session_id))
    }

    pub(super) fn record_tool_step(
        &self,
        session_id: &str,
        name: &str,
        arguments_json: &str,
    ) -> Option<JobInfo> {
        let mut entries = self.entries.lock().expect("statuses");
        let entry = entries.get_mut(session_id)?;
        entry.tool_steps += 1;
        entry.last_call = compose_last_call(name, arguments_json);
        entry.last_engine_event = Instant::now();
        Some(entry.to_job_info(session_id))
    }

    pub(super) fn record_steer_queued(&self, session_id: &str) -> Option<JobInfo> {
        let mut entries = self.entries.lock().expect("statuses");
        let entry = entries.get_mut(session_id)?;
        entry.queued_steers += 1;
        Some(entry.to_job_info(session_id))
    }

    pub(super) fn record_steer_consumed(&self, session_id: &str) -> Option<JobInfo> {
        let mut entries = self.entries.lock().expect("statuses");
        let entry = entries.get_mut(session_id)?;
        entry.queued_steers = entry.queued_steers.saturating_sub(1);
        Some(entry.to_job_info(session_id))
    }

    pub(super) fn drop_queued(&self, session_id: &str) -> Option<JobInfo> {
        let mut entries = self.entries.lock().expect("statuses");
        let entry = entries.get_mut(session_id)?;
        entry.queued_steers = 0;
        Some(entry.to_job_info(session_id))
    }

    pub(super) fn touch_engine(&self, session_id: &str) {
        if let Some(entry) = self.entries.lock().expect("statuses").get_mut(session_id) {
            entry.last_engine_event = Instant::now();
        }
    }

    pub(super) fn finish(
        &self,
        session_id: &str,
        state: JobState,
        elapsed: Duration,
    ) -> Option<JobInfo> {
        let ordinal = self.next_ordinal();
        let info = {
            let mut entries = self.entries.lock().expect("statuses");
            let entry = entries.get_mut(session_id)?;
            entry.state = state;
            entry.elapsed = Some(elapsed);
            entry.ordinal = ordinal;
            entry.finished_at = Some(Instant::now());
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

    pub(super) fn list(&self) -> Vec<JobInfo> {
        let now = Instant::now();
        let mut entries = self.entries.lock().expect("statuses");
        entries.retain(|_, status| {
            status
                .finished_at
                .is_none_or(|finished_at| finished_at + TERMINAL_TTL > now)
        });
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

// the first meaningful string value, mirroring the client's tool lines;
// flattened and capped because it rides every job_changed push
fn compose_last_call(name: &str, arguments_json: &str) -> String {
    let summary = serde_json::from_str::<serde_json::Value>(arguments_json)
        .ok()
        .and_then(|value| match value {
            serde_json::Value::Object(map) => map.into_iter().find_map(|(_, value)| match value {
                serde_json::Value::String(s) if !s.trim().is_empty() => Some(s),
                _ => None,
            }),
            _ => None,
        })
        .unwrap_or_default();
    format!("{name} {}", summary.trim())
        .trim_end()
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .take(120)
        .collect()
}

#[cfg(test)]
mod compose_tests {
    use super::compose_last_call;

    #[test]
    fn the_first_string_argument_rides_along_flattened_and_capped() {
        assert_eq!(
            compose_last_call("bash", r#"{"command":"cargo\ntest"}"#),
            "bash cargo test"
        );
        assert_eq!(compose_last_call("get_time", "{}"), "get_time");
        assert_eq!(compose_last_call("edit", "not json"), "edit");
        let long = format!(r#"{{"command":"{}"}}"#, "x".repeat(300));
        assert_eq!(compose_last_call("bash", &long).chars().count(), 120);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arc_core::log::Log;
    use arc_core::projection::Projection;
    use arc_core::provider::{CompletionDelta, Error as ProviderError, Stop};
    use arc_core::session::ProjectSpec;
    use arc_core::store::Store;
    use arc_core::testkit::{ScriptedProvider, Step, call, done_reply, tool_stop, tools, usage};
    use arc_core::tool::Registry;
    use arc_core::tool::workspace::{Grant, Mode};
    use tempfile::TempDir;

    use arc_proto::v1::Role;

    use crate::jobs::Supervisor;
    use crate::jobs::tests_common::testkit::{
        GatedTool, child_session, child_user_messages, engine_for_project,
        engine_for_project_notified, executor_runner, job_changed, only_job, steer,
        wait_for_message_count,
    };

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

        // no wait: the steer lands before the first turn is live, so it
        // runs as the task's next turn instead of joining this one
        assert!(steer(&supervisor, &child_id, "also check the linter"));
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
    async fn a_terminal_job_ages_out_of_the_list_once_the_terminal_ttl_passes() {
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

        assert_eq!(only_job(supervisor.list()).session_id, child_id);

        tokio::time::advance(TERMINAL_TTL + Duration::from_secs(1)).await;
        assert_eq!(
            supervisor.list(),
            Vec::new(),
            "the terminal entry aged out once past the TTL"
        );
    }

    #[tokio::test]
    async fn a_running_job_is_never_aged_out_of_the_list() {
        tokio::time::pause();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let gate = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("still working".to_owned()))],
            notify: Arc::clone(&gate),
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

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "fix the failing test".to_owned(),
            budget: None,
        });

        wait_for_message_count(dir.path(), &child_id, 1).await;
        tokio::time::advance(TERMINAL_TTL + Duration::from_secs(1)).await;
        assert_eq!(
            only_job(supervisor.list()).state,
            job_info::State::Running as i32,
            "a running job stays listed no matter how long it's been running"
        );

        gate.notify_one();
        supervisor.shutdown().await;
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

    #[tokio::test]
    async fn each_tool_step_broadcasts_a_job_changed_that_counts_it() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        // two issued calls in one round: Started hits for both land before
        // either tool runs, so the pushes count 1 then 2
        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![
            vec![
                Ok(call("c1", 0, "lookup", "{}")),
                Ok(call("c2", 1, "lookup", "{}")),
                Ok(tool_stop()),
            ],
            done_reply("done"),
        ]);
        let (notifier, mut notifications) = broadcast::channel(16);

        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(
            Engine::new(
                Store::new(log, projection),
                tools(&[("lookup", "found it", true)]),
            )
            .with_projects(BTreeMap::from([(
                "arc".to_owned(),
                ProjectSpec {
                    sources: Vec::new(),
                    grants: vec![Grant::new(&root, Mode::ReadWrite)],
                    command_prefix: Vec::new(),
                },
            )]))
            .with_notifier(notifier.clone()),
        );
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners).with_notifier(notifier);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "run two tools".to_owned(),
            budget: None,
        });

        let mut job = job_changed(&mut notifications).await;
        assert_eq!(job.tool_steps, 0, "the spawn push is still thinking");
        let mut step_pushes = Vec::new();
        loop {
            job = job_changed(&mut notifications).await;
            if job.tool_steps > 0 {
                step_pushes.push(job.tool_steps);
                assert_eq!(job.idle_seconds, 0, "the push follows the step event");
            }
            if job.state == job_info::State::Finished as i32 {
                break;
            }
        }
        assert!(
            step_pushes.contains(&1) && step_pushes.contains(&2),
            "{step_pushes:?}"
        );
        assert_eq!(job.tool_steps, 2, "the job's final state keeps the count");
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn idle_seconds_count_the_wall_clock_since_the_last_engine_event() {
        tokio::time::pause();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let tool_gate = Arc::new(tokio::sync::Notify::new());
        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Immediate(vec![Ok(call("c1", 0, "slow_tool", "{}")), Ok(tool_stop())]),
            Step::Immediate(done_reply("done after the slow tool")),
        ]);
        let (notifier, mut notifications) = broadcast::channel(16);

        let mut registry = Registry::new(512);
        registry.register(Box::new(GatedTool {
            notify: Arc::clone(&tool_gate),
        }));
        let log = Log::open(dir.path()).expect("open log");
        let projection = Projection::in_memory().expect("open projection");
        let engine = Arc::new(
            Engine::new(Store::new(log, projection), registry)
                .with_projects(BTreeMap::from([(
                    "arc".to_owned(),
                    ProjectSpec {
                        sources: Vec::new(),
                        grants: vec![Grant::new(&root, Mode::ReadWrite)],
                        command_prefix: Vec::new(),
                    },
                )]))
                .with_notifier(notifier.clone()),
        );
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners).with_notifier(notifier);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "run the slow tool".to_owned(),
            budget: None,
        });

        // the step push lands while the tool is still gated, then the clock
        // runs on with no engine events: idle must grow with it
        let mut job = job_changed(&mut notifications).await;
        while job.tool_steps == 0 {
            job = job_changed(&mut notifications).await;
        }
        assert_eq!(job.idle_seconds, 0);
        tokio::time::advance(Duration::from_secs(8)).await;
        assert_eq!(
            only_job(supervisor.list()).idle_seconds,
            8,
            "no engine events for eight seconds reads as eight idle"
        );

        tool_gate.notify_one();
        loop {
            job = job_changed(&mut notifications).await;
            if job.state == job_info::State::Finished as i32 {
                break;
            }
        }
        assert_eq!(
            job.idle_seconds, 0,
            "the tool's end event reset the idle clock"
        );
        assert_eq!(
            only_job(supervisor.list()).state,
            job_info::State::Finished as i32
        );
        supervisor.shutdown().await;
    }

    #[tokio::test]
    async fn queued_steers_count_tracks_queueing_and_consuming() {
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
            budget: None,
        });

        // no wait: the steer lands before the first turn is live, so it
        // waits for a turn of its own and the count can see it
        assert!(steer(&supervisor, &child_id, "also check the linter"));
        assert_eq!(
            only_job(supervisor.list()).queued_steers,
            1,
            "the count tracks the queue"
        );

        notify.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            only_job(supervisor.list()).queued_steers,
            0,
            "consuming the queued steer cleared it"
        );
    }

    #[tokio::test]
    async fn dropping_steers_zeroes_the_count_immediately_and_pushes_job_changed() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![Step::Gated {
            before: vec![Ok(CompletionDelta::Text("on it".to_owned()))],
            notify: Arc::clone(&notify),
            after: vec![Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            })],
        }]);
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

        assert!(steer(&supervisor, &child_id, "first"));
        assert!(steer(&supervisor, &child_id, "second"));
        let mut job = job_changed(&mut notifications).await;
        while job.queued_steers != 2 {
            job = job_changed(&mut notifications).await;
        }

        // the job is still gated mid-turn: the drop's count and push land
        // without waiting for the turn to finish
        assert!(supervisor.drop_steers(&child_id));
        let dropped_push = job_changed(&mut notifications).await;
        assert_eq!(
            dropped_push.queued_steers, 0,
            "the drop pushed the zeroed count right away"
        );

        notify.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            child_user_messages(dir.path(), &child_id),
            [
                (Role::User, "fix the failing test".to_owned()),
                (Role::Assistant, "on it".to_owned()),
            ],
            "neither dropped steer ever ran"
        );
    }

    #[tokio::test]
    async fn dropping_steers_on_an_unknown_job_is_an_honest_no_op() {
        let dir = TempDir::new().expect("temp dir");
        let engine = Arc::new(Engine::new(
            Store::new(
                Log::open(dir.path()).expect("open log"),
                Projection::in_memory().expect("open projection"),
            ),
            Registry::new(512),
        ));
        let supervisor = Supervisor::new(engine, BTreeMap::new());

        assert!(!supervisor.drop_steers("s-never-existed"));
    }
}
