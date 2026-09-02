use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arc_core::provider::role_label;
use arc_core::session::{DispatchedJob, Engine, Runner};
use arc_proto::v1::{Notification, SessionRole};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use super::status::JobStatuses;
use super::turn::{BudgetBreach, EVENT_BUFFER};
use super::{Handles, LiveMap, route_cancels, route_continues, spawn_job};

pub(super) const NO_REPLY: &str = "(the job produced no reply)";

/// Consecutive handback turns run for one parent since its last user
/// message, before the daemon stops narrating and just leaves the handback
/// itself in the transcript (DESIGN.md §4.1).
const MAX_HANDBACK_TURNS: u32 = 50;

pub(super) struct Handback {
    runner: Runner,
    /// Parents with a handback turn in flight right now: a second handback
    /// for the same parent collapses into `dirty` instead of its own turn.
    pending: Mutex<HashSet<String>>,
    /// Parents that got another handback while their turn was pending: the
    /// running turn loops once more per flag once it finishes, bounded by
    /// `autonomy`, not by a count of its own.
    dirty: Mutex<HashSet<String>>,
    autonomy: Mutex<HashMap<String, u32>>,
}

impl Handback {
    pub(super) fn new(runner: Runner) -> Self {
        Self {
            runner,
            pending: Mutex::new(HashSet::new()),
            dirty: Mutex::new(HashSet::new()),
            autonomy: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn reset_autonomy(&self, session_id: &str) {
        self.autonomy.lock().expect("autonomy").remove(session_id);
    }
}

pub(super) struct HandbackCtx<'a> {
    pub(super) engine: &'a Arc<Engine>,
    pub(super) runners: &'a BTreeMap<SessionRole, Runner>,
    pub(super) projects: &'a BTreeMap<String, PathBuf>,
    pub(super) live: &'a Arc<LiveMap>,
    pub(super) statuses: &'a Arc<JobStatuses>,
    pub(super) notifier: Option<&'a broadcast::Sender<Notification>>,
    pub(super) handles: &'a Arc<Handles>,
    pub(super) handback: Option<&'a Arc<Handback>>,
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

pub(super) async fn record_handback(
    ctx: &HandbackCtx<'_>,
    job: &DispatchedJob,
    reason: Option<&str>,
) {
    let summary = job_summary(ctx.engine, job);
    record_handback_with(ctx, job, reason, summary).await;
}

async fn record_handback_with(
    ctx: &HandbackCtx<'_>,
    job: &DispatchedJob,
    reason: Option<&str>,
    summary: String,
) {
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

pub(super) async fn handback_clean(ctx: &HandbackCtx<'_>, job: &DispatchedJob) {
    record_handback(ctx, job, None).await;
}

pub(super) const USER_REPLY_NOTE: &str = "This reply answers a message the user sent the job directly; the report before it still stands.";

pub(super) async fn handback_user_reply(ctx: &HandbackCtx<'_>, job: &DispatchedJob) {
    let summary = format!("{USER_REPLY_NOTE}\n{}", job_summary(ctx.engine, job));
    record_handback_with(ctx, job, None, summary).await;
}

pub(super) async fn handback_failed(ctx: &HandbackCtx<'_>, job: &DispatchedJob) {
    record_handback(ctx, job, Some("the turn failed")).await;
}

pub(super) async fn handback_crashed(ctx: &HandbackCtx<'_>, job: &DispatchedJob) {
    record_handback(ctx, job, Some("the job crashed")).await;
}

pub(super) async fn handback_cancelled(ctx: &HandbackCtx<'_>, job: &DispatchedJob) {
    record_handback(
        ctx,
        job,
        Some("cancelled by the user. The user chose to stop this work — do not dispatch or continue it again unless they ask"),
    )
    .await;
}

pub(super) async fn handback_over_budget(
    ctx: &HandbackCtx<'_>,
    job: &DispatchedJob,
    breach: &BudgetBreach,
) {
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
        ctx,
        job,
        Some(&reason),
        "(its last reply was handed back when that turn ended)".to_owned(),
    )
    .await;
}

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
                    ctx.projects,
                    ctx.live,
                    ctx.statuses,
                    ctx.notifier,
                    Some(handback),
                    ctx.handles,
                );
            }
            route_continues(ctx, reply.continues);
            route_cancels(ctx, reply.cancels);
        }
        Err(error) => warn!(parent_session, %error, "handback turn failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    use arc_proto::v1::{Budget, Role};
    use tempfile::TempDir;

    use crate::jobs::Supervisor;
    use crate::jobs::tests_common::testkit::{
        child_session, child_user_messages, engine_for_project, executor_runner, parent_session,
        wait_for_message_count,
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
        wait_for_message_count(dir.path(), &child_id, 1).await;
        assert!(supervisor.steer(&child_id, "you can use jj split"));
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
            Some("chained".to_owned()),
            "the handback turn's own reply landed after it dispatched"
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
