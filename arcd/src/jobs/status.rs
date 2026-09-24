use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use arc_core::session::{DispatchedJob, Engine};
use arc_proto::v1::{JobInfo, Notification, job_info, notification};
use tokio::sync::broadcast;
use tokio::time::Instant;

use super::handback::job_title;

const MAX_TERMINAL_JOBS: usize = 50;
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

struct JobStatus {
    info: JobInfo,
    started: Instant,
    ordinal: u64,
    finished_at: Option<Instant>,
    last_engine_event: Instant,
}

impl JobStatus {
    fn to_job_info(&self) -> JobInfo {
        let mut info = self.info.clone();
        if self.finished_at.is_none() {
            info.elapsed_seconds =
                u32::try_from(self.started.elapsed().as_secs()).unwrap_or(u32::MAX);
        }
        info.idle_seconds =
            u32::try_from(self.last_engine_event.elapsed().as_secs()).unwrap_or(u32::MAX);
        info
    }
}

pub(super) struct JobStatuses {
    inner: Mutex<StatusStore>,
}

struct StatusStore {
    entries: HashMap<String, JobStatus>,
    ordinal: u64,
}

impl StatusStore {
    fn next_ordinal(&mut self) -> u64 {
        let ordinal = self.ordinal;
        self.ordinal += 1;
        ordinal
    }

    fn update(&mut self, session_id: &str, change: impl FnOnce(&mut JobStatus)) -> Option<JobInfo> {
        let entry = self.entries.get_mut(session_id)?;
        change(entry);
        Some(entry.to_job_info())
    }
}

impl JobStatuses {
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(StatusStore {
                entries: HashMap::new(),
                ordinal: 0,
            }),
        }
    }

    pub(super) fn start(&self, job: &DispatchedJob, initial_spent_tokens: u64) -> JobInfo {
        let mut store = self.inner.lock().expect("statuses");
        let entry = JobStatus {
            info: JobInfo {
                session_id: job.session_id.clone(),
                role: job.role as i32,
                project: job.project.clone(),
                state: job_info::State::Running as i32,
                spent_tokens: initial_spent_tokens,
                budget_tokens: job.budget.as_ref().map_or(0, |budget| budget.total_tokens),
                budget_seconds: job
                    .budget
                    .as_ref()
                    .map_or(0, |budget| budget.wall_clock_seconds),
                parent_session: job.parent_session.clone(),
                ..Default::default()
            },
            started: Instant::now(),
            ordinal: store.next_ordinal(),
            finished_at: None,
            last_engine_event: Instant::now(),
        };
        let info = entry.to_job_info();
        store.entries.insert(job.session_id.clone(), entry);
        info
    }

    pub(super) fn record_tokens(&self, session_id: &str, spent_tokens: u64) -> Option<JobInfo> {
        self.inner
            .lock()
            .expect("statuses")
            .update(session_id, |entry| entry.info.spent_tokens = spent_tokens)
    }

    pub(super) fn record_tool_step(
        &self,
        session_id: &str,
        name: &str,
        arguments_json: &str,
    ) -> Option<JobInfo> {
        self.inner
            .lock()
            .expect("statuses")
            .update(session_id, |entry| {
                entry.info.tool_steps += 1;
                entry.info.last_call = compose_last_call(name, arguments_json);
                entry.last_engine_event = Instant::now();
            })
    }

    pub(super) fn record_steer_queued(&self, session_id: &str) -> Option<JobInfo> {
        self.inner
            .lock()
            .expect("statuses")
            .update(session_id, |entry| entry.info.queued_steers += 1)
    }

    pub(super) fn record_steer_consumed(&self, session_id: &str) -> Option<JobInfo> {
        self.inner
            .lock()
            .expect("statuses")
            .update(session_id, |entry| {
                entry.info.queued_steers -= 1;
            })
    }

    pub(super) fn drop_queued(&self, session_id: &str) -> Option<JobInfo> {
        self.inner
            .lock()
            .expect("statuses")
            .update(session_id, |entry| entry.info.queued_steers = 0)
    }

    pub(super) fn touch_engine(&self, session_id: &str) {
        if let Some(entry) = self
            .inner
            .lock()
            .expect("statuses")
            .entries
            .get_mut(session_id)
        {
            entry.last_engine_event = Instant::now();
        }
    }

    pub(super) fn finish(
        &self,
        session_id: &str,
        state: job_info::State,
        elapsed: Duration,
    ) -> Option<JobInfo> {
        let mut store = self.inner.lock().expect("statuses");
        let ordinal = store.next_ordinal();
        let info = store.update(session_id, |entry| {
            entry.info.state = state as i32;
            entry.info.elapsed_seconds = u32::try_from(elapsed.as_secs()).unwrap_or(u32::MAX);
            entry.ordinal = ordinal;
            entry.finished_at = Some(Instant::now());
        })?;

        let terminal_count = store
            .entries
            .values()
            .filter(|entry| entry.finished_at.is_some())
            .count();
        if terminal_count > MAX_TERMINAL_JOBS {
            if let Some(oldest) = store
                .entries
                .iter()
                .filter(|(_, entry)| entry.finished_at.is_some())
                .min_by_key(|(_, entry)| entry.ordinal)
                .map(|(id, _)| id.clone())
            {
                store.entries.remove(&oldest);
            }
        }
        Some(info)
    }

    pub(super) fn list(&self) -> Vec<JobInfo> {
        let now = Instant::now();
        let mut store = self.inner.lock().expect("statuses");
        store.entries.retain(|_, status| {
            status
                .finished_at
                .is_none_or(|finished_at| finished_at + TERMINAL_TTL > now)
        });
        let mut listed: Vec<(&String, &JobStatus)> = store.entries.iter().collect();
        listed.sort_by(|(_, a), (_, b)| {
            let a_terminal = a.finished_at.is_some();
            let b_terminal = b.finished_at.is_some();
            a_terminal
                .cmp(&b_terminal)
                .then_with(|| b.ordinal.cmp(&a.ordinal))
        });
        listed
            .into_iter()
            .map(|(_, status)| status.to_job_info())
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
    use arc_core::provider::{CompletionDelta, Stop};
    use arc_core::session::ProjectSpec;
    use arc_core::store::Store;
    use arc_core::testkit::{ScriptedProvider, Step, call, done_reply, tool_stop, tools, usage};
    use arc_core::tool::Registry;
    use arc_core::tool::workspace::{Grant, Mode};
    use tempfile::TempDir;

    use arc_proto::v1::SessionRole;

    use crate::jobs::Supervisor;
    use crate::jobs::tests_common::testkit::{
        GatedTool, child_session, engine_for_project, executor_runner, job_changed, only_job, steer,
    };

    #[tokio::test]
    async fn a_finished_job_retains_its_state_with_a_frozen_elapsed() {
        tokio::time::pause();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let chat_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("all fixed")]);

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

        let chat_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("all fixed")]);

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
        supervisor.shutdown().await;

        assert_eq!(only_job(supervisor.list()).session_id, child_id);

        tokio::time::advance(TERMINAL_TTL + Duration::from_secs(1)).await;
        assert_eq!(
            supervisor.list(),
            Vec::new(),
            "the terminal entry aged out once past the TTL"
        );
    }

    #[test]
    fn resumed_job_does_not_count_as_terminal_or_get_evicted() {
        let statuses = JobStatuses::new();
        let job = |id: String| DispatchedJob {
            session_id: id,
            parent_session: "parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "work".to_owned(),
            budget: None,
        };
        let resumed = job("resumed".to_owned());
        statuses.start(&resumed, 0);
        statuses.finish(
            &resumed.session_id,
            job_info::State::Finished,
            Duration::ZERO,
        );
        statuses.start(&resumed, 42);
        for n in 0..=MAX_TERMINAL_JOBS {
            let other = job(format!("other-{n}"));
            statuses.start(&other, 0);
            statuses.finish(&other.session_id, job_info::State::Finished, Duration::ZERO);
        }
        let listed = statuses.list();
        assert_eq!(listed.len(), MAX_TERMINAL_JOBS + 1);
        let active = listed
            .iter()
            .find(|info| info.session_id == resumed.session_id)
            .expect("resumed job remains live");
        assert_eq!(active.state, job_info::State::Running as i32);
        assert_eq!(active.spent_tokens, 42);
    }

    #[tokio::test]
    async fn each_tool_step_broadcasts_a_job_changed_that_counts_it() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        // two issued calls in one round: Started hits for both land before
        // either tool runs, so the pushes count 1 then 2
        let chat_provider = ScriptedProvider::scripted(vec![]);
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
        let child_id = child_session(&engine, &chat_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners).with_notifier(notifier);

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
        let chat_provider = ScriptedProvider::scripted(vec![]);
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
        let child_id = child_session(&engine, &chat_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::for_test(Arc::clone(&engine), runners).with_notifier(notifier);

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

        let chat_provider = ScriptedProvider::scripted(vec![]);
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
}
