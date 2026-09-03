use std::collections::HashMap;
use std::sync::Mutex;

use arc_core::session::{DispatchedJob, Engine};
use arc_proto::v1::Source;
use tracing::warn;

use super::turn::BudgetBreach;
use super::{Shared, send_into};

pub(super) const NO_REPLY: &str = "(the job produced no reply)";

/// Consecutive system-started turns run for one session since its last user
/// message, before the daemon stops narrating and just leaves the handback
/// itself in the transcript (DESIGN.md §4.1).
const MAX_HANDBACK_TURNS: u32 = 50;

/// The backstop on a chain of handbacks answering handbacks.
pub(super) struct Autonomy(Mutex<HashMap<String, u32>>);

impl Autonomy {
    pub(super) fn new() -> Self {
        Self(Mutex::new(HashMap::new()))
    }

    /// Counts one system-started turn for this session, or refuses once the
    /// session has had `MAX_HANDBACK_TURNS` of them with no user message.
    pub(super) fn claim(&self, session_id: &str) -> bool {
        let mut counts = self.0.lock().expect("autonomy");
        let count = counts.entry(session_id.to_owned()).or_insert(0);
        if *count >= MAX_HANDBACK_TURNS {
            return false;
        }
        *count += 1;
        true
    }

    pub(super) fn reset(&self, session_id: &str) {
        self.0.lock().expect("autonomy").remove(session_id);
    }
}

pub(super) fn job_title(engine: &Engine, session_id: &str) -> String {
    match engine.session_title(session_id) {
        Ok(title) => title.unwrap_or_default(),
        Err(error) => {
            warn!(session_id, %error, "could not read the job's title; leaving it blank");
            String::new()
        }
    }
}

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

pub(super) fn record_handback(shared: &Shared, job: &DispatchedJob, reason: Option<&str>) {
    let summary = job_summary(&shared.engine, job);
    record_handback_with(shared, job, reason, &summary, None);
}

/// The report goes to the parent as an ordinary system message: read at the
/// parent's next step boundary if it is working, on a turn of its own if it
/// is idle. Only when no turn can carry it does it land in the log alone.
fn record_handback_with(
    shared: &Shared,
    job: &DispatchedJob,
    reason: Option<&str>,
    summary: &str,
    footprint: Option<&str>,
) {
    if job.parent_session.is_empty() {
        warn!(session_id = %job.session_id, "a job with no parent has nowhere to report to");
        return;
    }
    let content = match shared
        .engine
        .compose_handback(&job.session_id, reason, summary, footprint)
    {
        Ok(content) => content,
        Err(error) => {
            warn!(
                session_id = %job.session_id,
                %error,
                "could not compose the job's handback"
            );
            return;
        }
    };
    let Err(error) = send_into(
        shared,
        Some(&job.parent_session),
        &content,
        Source::System,
        false,
    ) else {
        return;
    };
    warn!(
        parent_session = %job.parent_session,
        session_id = %job.session_id,
        %error,
        "no turn could carry this handback; leaving the report in the parent's log"
    );
    if let Err(error) = shared
        .engine
        .append_message(&job.parent_session, &content, Source::System)
    {
        warn!(
            parent_session = %job.parent_session,
            session_id = %job.session_id,
            %error,
            "failed to record the job's handback into the parent session"
        );
    }
}

pub(super) fn handback_clean(shared: &Shared, job: &DispatchedJob, footprint: Option<&str>) {
    let summary = job_summary(&shared.engine, job);
    record_handback_with(shared, job, None, &summary, footprint);
}

pub(super) const USER_REPLY_NOTE: &str = "This reply answers a message the user sent the job directly; the report before it still stands.";

pub(super) fn handback_user_reply(shared: &Shared, job: &DispatchedJob, footprint: Option<&str>) {
    let summary = format!("{USER_REPLY_NOTE}\n{}", job_summary(&shared.engine, job));
    record_handback_with(shared, job, None, &summary, footprint);
}

pub(super) fn handback_failed(shared: &Shared, job: &DispatchedJob) {
    record_handback(shared, job, Some("the turn failed"));
}

pub(super) fn handback_crashed(shared: &Shared, job: &DispatchedJob) {
    record_handback(shared, job, Some("the job crashed"));
}

pub(super) fn handback_cancelled(shared: &Shared, job: &DispatchedJob) {
    record_handback(
        shared,
        job,
        Some(
            "cancelled by the user. The user chose to stop this work — do not dispatch or continue it again unless they ask",
        ),
    );
}

pub(super) fn handback_over_budget(shared: &Shared, job: &DispatchedJob, breach: &BudgetBreach) {
    let reason = match breach {
        BudgetBreach::Tokens { spent, allowed } => {
            format!("token budget exhausted ({spent}/{allowed})")
        }
        BudgetBreach::WallClock { elapsed, allowed } => {
            format!("time budget exhausted ({elapsed}s/{allowed}s)")
        }
    };
    // the turn that crossed the budget already handed its reply back
    record_handback_with(
        shared,
        job,
        Some(&reason),
        "(its last reply was handed back when that turn ended)",
        None,
    );
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
    use arc_core::testkit::{
        ScriptedProvider, Step, call, done_reply, replay_log, runner, tool_stop, usage,
    };
    use arc_core::tool::Registry;
    use arc_core::tool::ToolSource;
    use arc_core::tool::workspace::{Grant, Mode};
    use arc_proto::v1::{Budget, Role, SessionRole};
    use tempfile::TempDir;

    use crate::jobs::Supervisor;
    use crate::jobs::tests_common::testkit::{
        child_session, child_user_messages, engine_for_project, executor_runner, parent_session,
        steer,
    };

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
            [(
                Role::User,
                format!(
                    "Job {child_id} finished.\nall fixed\nFor follow-ups about anything this job read or did, continue_job {child_id} keeps its context; a new dispatch starts from nothing."
                )
            )],
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

        let handbacks = child_user_messages(dir.path(), &parent_id);
        assert_eq!(handbacks.len(), 2, "the turn's own handback, then the stop");
        assert!(
            handbacks[0].1.contains("partial progress"),
            "the turn that crossed the budget still hands its reply back: {:?}",
            handbacks[0]
        );
        assert_eq!(
            handbacks[1],
            (
                Role::User,
                "Job ".to_owned()
                    + &child_id
                    + " stopped: token budget exhausted (8/5).\n(its last reply was handed back when that turn ended)"
            ),
            "the reason names the spent and allowed token counts"
        );
    }

    #[tokio::test]
    async fn a_handback_from_a_jj_project_carries_the_daemons_footprint() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");
        let init = std::process::Command::new("jj")
            .args(["git", "init"])
            .current_dir(&root)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("jj runs");
        assert!(init.success());

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("changed nothing")]);

        let engine = engine_for_project(&dir, &root);
        let parent_id = parent_session(&engine, &concierge_provider);
        let child_id = child_session(&engine, &concierge_provider);

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners)
            .with_projects(BTreeMap::from([("arc".to_owned(), root.clone().into())]));

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: parent_id.clone(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "look around".to_owned(),
            budget: None,
        });
        supervisor.shutdown().await;

        let handbacks = child_user_messages(dir.path(), &parent_id);
        assert_eq!(handbacks.len(), 1, "{handbacks:?}");
        assert!(
            handbacks[0].1.contains(
                "changed nothing\nFootprint since this turn began, counted by the daemon: nothing in the project changed.\nFor follow-ups"
            ),
            "the daemon's count sits between the report and the continue line: {:?}",
            handbacks[0]
        );
    }

    #[tokio::test]
    async fn every_turn_hands_back_and_a_users_own_steer_says_so() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let notify = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("the report".to_owned()))],
                notify: Arc::clone(&notify),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Immediate(done_reply("agreed, jj split")),
        ]);

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
            brief: "make two commits".to_owned(),
            budget: None,
        });
        // queued before the brief turn is live, so it runs as its own turn
        // and hands back on its own
        assert!(steer(&supervisor, &child_id, "you can use jj split"));
        notify.notify_one();
        supervisor.shutdown().await;

        let handbacks = child_user_messages(dir.path(), &parent_id);
        assert_eq!(handbacks.len(), 2, "one handback per turn: {handbacks:?}");
        assert!(
            handbacks[0]
                .1
                .starts_with(&format!("Job {child_id} finished.\nthe report\n")),
            "the steer queued behind the brief turn does not swallow its report: {:?}",
            handbacks[0]
        );
        assert!(
            handbacks[1].1.starts_with(&format!(
                "Job {child_id} finished.\n{USER_REPLY_NOTE}\nagreed, jj split\n"
            )),
            "a reply to the user's own message says so: {:?}",
            handbacks[1]
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
            [(
                Role::User,
                format!(
                    "Job {child_id} finished.\n{NO_REPLY}\nFor follow-ups about anything this job read or did, continue_job {child_id} keeps its context; a new dispatch starts from nothing."
                )
            )],
            "an empty assistant reply reads the same as no reply at all"
        );
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
                (
                    Role::User,
                    format!(
                        "Job {child_id} finished.\nall fixed\nFor follow-ups about anything this job read or did, continue_job {child_id} keeps its context; a new dispatch starts from nothing."
                    )
                ),
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
    async fn a_handback_into_an_idle_executor_parent_starts_a_turn_there_too() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]); // must never be called
        let executor_provider =
            ScriptedProvider::scripted(vec![done_reply("done"), done_reply("the parent reacts")]);

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
            last_assistant(dir.path(), &parent_id),
            Some("the parent reacts".to_owned()),
            "a code session reads its children's reports like the concierge does"
        );
        assert!(
            concierge_provider.requests().is_empty(),
            "the parent's own role runs its turn, not the concierge"
        );
    }

    /// Polls rather than sleeping: the gate only proves the turn is live
    /// once the provider has actually been asked for a completion.
    async fn wait_for_requests(provider: &Arc<ScriptedProvider>, want: usize) {
        for _ in 0..400 {
            if provider.requests().len() >= want {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("timed out waiting for {want} completion requests");
    }

    #[tokio::test]
    async fn a_second_handback_during_a_parents_turn_lands_in_that_turn() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let notify = Arc::new(tokio::sync::Notify::new());
        let concierge_provider = ScriptedProvider::scripted_steps(vec![
            Step::Gated {
                before: vec![Ok(CompletionDelta::Text("reacting".to_owned()))],
                notify: Arc::clone(&notify),
                after: vec![Ok(CompletionDelta::Done {
                    usage: usage(),
                    stop: Stop::EndTurn,
                })],
            },
            Step::Immediate(done_reply("and to the second")),
        ]);
        let executor_provider = ScriptedProvider::scripted(vec![done_reply("job done")]);

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
        wait_for_requests(&concierge_provider, 1).await;

        // exactly what a second child finishing would send
        let second = supervisor
            .send(
                Some(&parent_id),
                "Job child-2 finished.\nalso done",
                Source::System,
                false,
            )
            .expect("the parent takes it");
        assert!(
            matches!(second, crate::jobs::SendOutcome::Queued { .. }),
            "a live parent queues it instead of starting a second turn"
        );

        notify.notify_one();
        supervisor.shutdown().await;

        let messages = child_user_messages(dir.path(), &parent_id);
        assert_eq!(
            messages
                .iter()
                .map(|(role, content)| (*role, content.as_str()))
                .collect::<Vec<_>>(),
            [
                (Role::User, messages[0].1.as_str()),
                (Role::Assistant, "reacting"),
                (Role::User, "Job child-2 finished.\nalso done"),
                (Role::Assistant, "and to the second"),
            ],
            "both reports landed in the one turn"
        );
        assert!(
            messages[0]
                .1
                .starts_with(&format!("Job {child_id} finished.")),
            "the first message is the first child's report: {}",
            messages[0].1
        );
        assert_eq!(
            concierge_provider.requests().len(),
            2,
            "one turn, two completions: the second report joined it at a step boundary"
        );
    }

    #[tokio::test]
    async fn a_forced_autonomy_cap_skips_the_parents_turn_and_appends_nothing_extra() {
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
        for _ in 0..MAX_HANDBACK_TURNS {
            assert!(supervisor.shared.autonomy.claim(&parent_id));
        }

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
        for _ in 0..MAX_HANDBACK_TURNS {
            assert!(supervisor.shared.autonomy.claim(&parent_id));
        }

        supervisor.shared.autonomy.reset(&parent_id);

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
            "intent": "implement",
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
            // the chained child hands back in turn, and that is read too
            done_reply("the chain ends here"),
        ]);
        let executor_provider = ScriptedProvider::scripted(vec![
            done_reply("first job done"),
            done_reply("second job done"),
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
            Some("the chain ends here".to_owned()),
            "the chain ran on: dispatch, then the chained child's own report"
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
                    command_prefix: Vec::new(),
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
