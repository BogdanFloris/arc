use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_core::provider::role_label;
use arc_core::session::{DispatchedJob, Engine, Runner};
use arc_proto::v1::SessionRole;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

const EVENT_BUFFER: usize = 64;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Runs the jobs `dispatch` created. One tokio task per job, driven to
/// completion with `send_message`; nothing restarts a job that fails.
pub struct Supervisor {
    engine: Arc<Engine>,
    runners: BTreeMap<SessionRole, Runner>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl Supervisor {
    pub fn new(engine: Arc<Engine>, runners: BTreeMap<SessionRole, Runner>) -> Self {
        Self {
            engine,
            runners,
            handles: Mutex::new(Vec::new()),
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
        let engine = Arc::clone(&self.engine);
        let handle = tokio::spawn(run_job(engine, runner, job));
        self.handles.lock().expect("handles").push(handle);
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

async fn run_job(engine: Arc<Engine>, runner: Runner, job: DispatchedJob) {
    let (events, mut rx) = mpsc::channel(EVENT_BUFFER);
    let session_id = job.session_id.clone();
    let (result, ()) = tokio::join!(
        engine.send_message(&runner, Some(&session_id), &job.brief, events),
        async {
            while let Some(event) = rx.recv().await {
                debug!(session_id = %session_id, ?event, "job event");
            }
        },
    );
    match result {
        Ok(reply) => info!(
            session_id = %session_id,
            output_tokens = reply.usage.map_or(0, |usage| usage.output_tokens),
            "job completed"
        ),
        Err(error) => warn!(session_id = %session_id, %error, "job failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arc_core::log::Log;
    use arc_core::projection::Projection;
    use arc_core::provider::{Provider, Thinking};
    use arc_core::session::ProjectSpec;
    use arc_core::store::Store;
    use arc_core::testkit::{ScriptedProvider, appended, done_reply, replay_log, runner};
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
            role: SessionRole::Concierge,
            brief: "never runs".to_owned(),
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
            role: SessionRole::Executor,
            brief: "fix the failing test".to_owned(),
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
}
