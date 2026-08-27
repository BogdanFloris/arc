use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_core::provider::Usage;
use arc_core::session::{ContinuedJob, DispatchedJob, Engine, EngineEvent, Runner};
use arc_proto::v1::{Budget, Notification, SessionRole};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use super::handback::{
    Handback, HandbackCtx, handback_cancelled, handback_clean, handback_failed,
    handback_over_budget,
};
use super::status::{JobState, JobStatuses, notify_job_changed};
use super::{Handles, LiveMap, route_continues, spawn_dispatched};

pub(super) const EVENT_BUFFER: usize = 64;
/// How long a turn may go without an engine event (a delta, reasoning, or
/// tool activity) before it's failed outright. A streaming HTTP body that
/// goes quiet without closing would otherwise hold the job open forever.
const JOB_SILENCE_TIMEOUT: Duration = Duration::from_secs(600);

/// Why a job's turn loop stopped short of a clean finish: both read the
/// same terminal `JobState::Failed`, so only the handback text tells them
/// apart (the caller wired to `k` gets a distinct reason from a genuine
/// provider failure).
enum EndReason {
    Failed,
    Cancelled,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_job(
    engine: Arc<Engine>,
    runner: Runner,
    job: DispatchedJob,
    mut steer_rx: mpsc::UnboundedReceiver<String>,
    mut cancel_rx: watch::Receiver<bool>,
    mut drop_rx: mpsc::UnboundedReceiver<()>,
    live: Arc<LiveMap>,
    statuses: Arc<JobStatuses>,
    notifier: Option<broadcast::Sender<Notification>>,
    runners: BTreeMap<SessionRole, Runner>,
    projects: BTreeMap<String, PathBuf>,
    handback: Option<Arc<Handback>>,
    handles: Arc<Handles>,
) {
    let session_id = job.session_id.clone();
    let start = Instant::now();
    let mut spent_tokens: u64 = 0;
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

    match run_turn(
        &ctx,
        &runner,
        &session_id,
        &job.brief,
        &mut steer_rx,
        &mut drop_rx,
        &mut cancel_rx,
    )
    .await
    {
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
            end_job(&ctx, &job, &mut steer_rx, start, EndReason::Failed).await;
            return;
        }
        TurnOutcome::Cancelled => {
            end_job(&ctx, &job, &mut steer_rx, start, EndReason::Cancelled).await;
            return;
        }
    }

    loop {
        drain_dropped_steers(&mut drop_rx, &mut steer_rx, &ctx, &session_id);

        if *cancel_rx.borrow() {
            end_job(&ctx, &job, &mut steer_rx, start, EndReason::Cancelled).await;
            return;
        }

        if let Some(breach) = budget_breach(job.budget.as_ref(), spent_tokens, start.elapsed()) {
            warn_over_budget(&session_id, &breach);
            finish_now(&live, &mut steer_rx, &statuses, &session_id);
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
        if let Some(info) = statuses.record_steer_consumed(&session_id) {
            notify_job_changed(notifier.as_ref(), &engine, info);
        }
        match run_turn(
            &ctx,
            &runner,
            &session_id,
            &text,
            &mut steer_rx,
            &mut drop_rx,
            &mut cancel_rx,
        )
        .await
        {
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
                end_job(&ctx, &job, &mut steer_rx, start, EndReason::Failed).await;
                return;
            }
            TurnOutcome::Cancelled => {
                end_job(&ctx, &job, &mut steer_rx, start, EndReason::Cancelled).await;
                return;
            }
        }
    }

    if let Some(info) = statuses.finish(&session_id, JobState::Finished, start.elapsed()) {
        notify_job_changed(notifier.as_ref(), &engine, info);
    }
    handback_clean(&ctx, &job).await;
}

/// Shared tail for a turn loop stopping short of a clean finish: drains
/// whatever's queued, marks the job terminal, and hands back the reason.
async fn end_job(
    ctx: &HandbackCtx<'_>,
    job: &DispatchedJob,
    steer_rx: &mut mpsc::UnboundedReceiver<String>,
    start: Instant,
    reason: EndReason,
) {
    finish_now(ctx.live, steer_rx, ctx.statuses, &job.session_id);
    if let Some(info) = ctx
        .statuses
        .finish(&job.session_id, JobState::Failed, start.elapsed())
    {
        notify_job_changed(ctx.notifier, ctx.engine, info);
    }
    match reason {
        EndReason::Failed => handback_failed(ctx, job).await,
        EndReason::Cancelled => handback_cancelled(ctx, job).await,
    }
}

/// A completed turn's outcome. A failed or cancelled turn ends the job, so
/// the caller stops draining steers; a successful turn carries whatever
/// usage the provider reported, which may itself be absent.
enum TurnOutcome {
    Success {
        usage: Option<Usage>,
        jobs: Vec<DispatchedJob>,
        continues: Vec<ContinuedJob>,
    },
    Failure,
    Cancelled,
}

fn usage_tokens(usage: Option<Usage>) -> u64 {
    usage.map_or(0, |usage| {
        u64::from(usage.input_tokens) + u64::from(usage.output_tokens)
    })
}

/// Which dimension of a job's budget it went over, and by how much.
pub(super) enum BudgetBreach {
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

/// One engine event for a job's running turn: a started tool call counts a
/// step and broadcasts the strip's new state; every event refreshes the
/// idle clock and the silence deadline. Shared by the live recv and the
/// post-send drain, which exists because a fully-scripted turn can finish
/// with events still buffered.
fn handle_job_event(
    event: &EngineEvent,
    pending_tool_calls: &mut u32,
    deadline: &mut Instant,
    ctx: &HandbackCtx<'_>,
    session_id: &str,
) {
    match event {
        EngineEvent::ToolCallStarted { .. } => {
            *pending_tool_calls += 1;
            if let Some(info) = ctx.statuses.record_tool_step(session_id) {
                notify_job_changed(ctx.notifier, ctx.engine, info);
            }
        }
        EngineEvent::ToolCallEnded { .. } => {
            *pending_tool_calls = pending_tool_calls.saturating_sub(1);
            ctx.statuses.touch_engine(session_id);
        }
        _ => ctx.statuses.touch_engine(session_id),
    }
    debug!(session_id = %session_id, ?event, "job event");
    *deadline = Instant::now() + JOB_SILENCE_TIMEOUT;
}

/// One completed poll of `run_turn`'s select loop: either the send future
/// resolved, or it timed out silent, or a cancel landed (row 6.39). Kept
/// distinct from `TurnOutcome` because the caller still needs to log each
/// case with its own message before collapsing them.
enum RawOutcome {
    Sent(Result<arc_core::session::Reply, arc_core::session::Error>),
    SilentTimeout,
    Cancelled,
}

/// Runs one turn to completion, failing it if the engine goes quiet for
/// `JOB_SILENCE_TIMEOUT` with no events, or if `cancel_rx` fires. A pending
/// tool call suspends the timeout instead of tripping it: bash alone is
/// allowed to run silent for up to its own 600s cap, so a tool call in
/// flight is activity, not stall. A cancel drops `send` mid-await — the
/// same shape a crash leaves — instead of waiting the turn out. A
/// `DropSteers` request lands here too, so it empties the queue without
/// waiting for a long turn to finish first.
#[allow(clippy::too_many_arguments)]
async fn run_turn(
    ctx: &HandbackCtx<'_>,
    runner: &Runner,
    session_id: &str,
    text: &str,
    steer_rx: &mut mpsc::UnboundedReceiver<String>,
    drop_rx: &mut mpsc::UnboundedReceiver<()>,
    cancel_rx: &mut watch::Receiver<bool>,
) -> TurnOutcome {
    let (events, mut rx) = mpsc::channel(EVENT_BUFFER);
    let send = ctx
        .engine
        .send_message(runner, Some(session_id), text, events);
    tokio::pin!(send);

    let mut deadline = Instant::now() + JOB_SILENCE_TIMEOUT;
    let mut pending_tool_calls: u32 = 0;
    // once a sender's gone, stop polling it: an already-closed channel
    // would otherwise resolve immediately forever and spin the select loop
    let mut cancel_live = true;
    let mut drop_live = true;

    let outcome = loop {
        tokio::select! {
            result = &mut send => {
                // the engine can complete a fully-scripted turn with events
                // still buffered; drain them so fast tools count as steps
                while let Ok(event) = rx.try_recv() {
                    handle_job_event(&event, &mut pending_tool_calls, &mut deadline, ctx, session_id);
                }
                break RawOutcome::Sent(result);
            }
            event = rx.recv() => {
                // send_message drops its sender exactly as it returns, so a
                // `None` here means `send` is already ready on the next poll
                let Some(event) = event else { continue };
                handle_job_event(&event, &mut pending_tool_calls, &mut deadline, ctx, session_id);
            }
            () = tokio::time::sleep_until(deadline), if pending_tool_calls == 0 => break RawOutcome::SilentTimeout,
            changed = cancel_rx.changed(), if cancel_live => match changed {
                Ok(()) => break RawOutcome::Cancelled,
                Err(_) => cancel_live = false,
            },
            dropped = drop_rx.recv(), if drop_live => match dropped {
                Some(()) => drop_queued_steers(steer_rx, ctx, session_id),
                None => drop_live = false,
            },
        }
    };

    match outcome {
        RawOutcome::Sent(Ok(reply)) => {
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
        RawOutcome::Sent(Err(error)) => {
            warn!(session_id = %session_id, %error, "job turn failed");
            TurnOutcome::Failure
        }
        // dropping `send` here abandons the turn mid-flight, the same shape
        // as a crash: any durable but unresolved tool call is left for the
        // orphan-repair a restart already needs, not fixed up here
        RawOutcome::SilentTimeout => {
            warn!(
                session_id = %session_id,
                timeout_secs = JOB_SILENCE_TIMEOUT.as_secs(),
                "job turn silent past the timeout; failing it"
            );
            TurnOutcome::Failure
        }
        RawOutcome::Cancelled => {
            info!(session_id = %session_id, "job turn cancelled by the user");
            TurnOutcome::Cancelled
        }
    }
}

/// Physically empties the steer queue and reports the fresh (zero) count.
/// Draining and counting happen together — the job's own task is the only
/// reader of `steer_rx` — so a steer that queues after the request is never
/// caught in the same net.
fn drop_queued_steers(
    steer_rx: &mut mpsc::UnboundedReceiver<String>,
    ctx: &HandbackCtx<'_>,
    session_id: &str,
) {
    let mut dropped = 0_usize;
    while steer_rx.try_recv().is_ok() {
        dropped += 1;
    }
    if dropped > 0 {
        warn!(session_id, dropped, "dropping queued steers on request");
    }
    if let Some(info) = ctx.statuses.drop_queued(session_id) {
        notify_job_changed(ctx.notifier, ctx.engine, info);
    }
}

/// Whether any `DropSteers` request has landed since the last check
/// (row 6.33); a no-op unless one has. The mid-turn select handles a
/// request seen while a turn is in flight — this is only for the window
/// between turns, in `run_job`'s own loop.
fn drain_dropped_steers(
    drop_rx: &mut mpsc::UnboundedReceiver<()>,
    steer_rx: &mut mpsc::UnboundedReceiver<String>,
    ctx: &HandbackCtx<'_>,
    session_id: &str,
) {
    let mut requested = false;
    while drop_rx.try_recv().is_ok() {
        requested = true;
    }
    if requested {
        drop_queued_steers(steer_rx, ctx, session_id);
    }
}

/// Ends a job now: removes its live entry, drops whatever steers are still
/// queued, and zeroes the visible count — exactly as a failed, cancelled,
/// or over-budget job must.
fn finish_now(
    live: &LiveMap,
    steer_rx: &mut mpsc::UnboundedReceiver<String>,
    statuses: &JobStatuses,
    session_id: &str,
) {
    live.lock().expect("live").remove(session_id);
    let mut dropped = 0_usize;
    while steer_rx.try_recv().is_ok() {
        dropped += 1;
    }
    statuses.drop_queued(session_id);
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
    use std::sync::Arc;

    use arc_core::log::Log;
    use arc_core::projection::Projection;
    use arc_core::provider::{CompletionDelta, Error as ProviderError, Stop};
    use arc_core::session::ProjectSpec;
    use arc_core::store::Store;
    use arc_core::testkit::{ScriptedProvider, Step, call, done_reply, runner, tool_stop, usage};
    use arc_core::tool::Registry;
    use arc_core::tool::ToolSource;
    use arc_core::tool::workspace::{Grant, Mode};
    use arc_proto::v1::{Role, job_info};
    use tempfile::TempDir;

    use crate::jobs::Supervisor;
    use crate::jobs::handback::NO_REPLY;
    use crate::jobs::tests_common::testkit::{
        GatedTool, child_session, child_user_messages, engine_for_project, executor_runner,
        only_job, parent_session, wait_for_message_count, wait_for_tool_call_issued,
    };

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
    async fn a_provider_silent_past_the_timeout_fails_the_turn_and_hands_back_that_it_failed() {
        tokio::time::pause();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        // never notified: the stream never yields anything at all
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
        tokio::time::advance(JOB_SILENCE_TIMEOUT + Duration::from_secs(1)).await;
        supervisor.shutdown().await;

        let job = only_job(supervisor.list());
        assert_eq!(
            job.state,
            job_info::State::Failed as i32,
            "a stream silent past the timeout fails the turn"
        );
        assert_eq!(
            child_user_messages(dir.path(), &parent_id),
            [(
                Role::User,
                format!("Job {child_id} stopped: the turn failed.\n{NO_REPLY}")
            )],
            "the silence timeout hands back like any other failed turn"
        );
    }

    #[tokio::test]
    async fn steady_events_past_the_timeout_duration_never_trip_the_silence_timeout() {
        tokio::time::pause();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let concierge_provider = ScriptedProvider::scripted(vec![]);
        let gate1 = Arc::new(tokio::sync::Notify::new());
        let gate2 = Arc::new(tokio::sync::Notify::new());
        // two silent gaps, each under JOB_SILENCE_TIMEOUT but summing well
        // past it: only a single gap that long should ever trip the job
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Immediate(vec![
                Ok(CompletionDelta::Text("chunk1".to_owned())),
                Ok(call("c1", 0, "missing_tool", "{}")),
                Ok(tool_stop()),
            ]),
            Step::Gated {
                before: Vec::new(),
                notify: Arc::clone(&gate1),
                after: vec![
                    Ok(CompletionDelta::Text("chunk2".to_owned())),
                    Ok(call("c2", 0, "missing_tool", "{}")),
                    Ok(tool_stop()),
                ],
            },
            Step::Gated {
                before: Vec::new(),
                notify: Arc::clone(&gate2),
                after: vec![
                    Ok(CompletionDelta::Text("chunk3".to_owned())),
                    Ok(CompletionDelta::Done {
                        usage: usage(),
                        stop: Stop::EndTurn,
                    }),
                ],
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

        // each gap is well under JOB_SILENCE_TIMEOUT on its own; only their
        // sum (800s) exceeds it
        let gap = Duration::from_secs(400);
        wait_for_message_count(dir.path(), &child_id, 2).await;
        tokio::time::advance(gap).await;
        gate1.notify_one();
        wait_for_message_count(dir.path(), &child_id, 3).await;
        tokio::time::advance(gap).await;
        gate2.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            only_job(supervisor.list()).state,
            job_info::State::Finished as i32,
            "events kept resetting the deadline, so the long total turn never tripped"
        );
    }

    #[tokio::test]
    async fn a_pending_tool_call_suspends_the_silence_timeout_while_it_runs() {
        tokio::time::pause();

        let dir = TempDir::new().expect("temp dir");
        let root = dir.path().join("proj");
        std::fs::create_dir_all(&root).expect("mkdir proj");

        let tool_gate = Arc::new(tokio::sync::Notify::new());
        let executor_provider = ScriptedProvider::scripted_steps(vec![
            Step::Immediate(vec![Ok(call("c1", 0, "slow_tool", "{}")), Ok(tool_stop())]),
            Step::Immediate(done_reply("finished after the slow tool")),
        ]);

        let mut registry = Registry::new(512);
        registry.register(Box::new(GatedTool {
            notify: Arc::clone(&tool_gate),
        }));
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
        let child_id = engine
            .create_bound_session(
                &runner(&bootstrap_provider),
                "arc",
                SessionRole::Executor,
                None,
            )
            .expect("create the child durably");

        let runners =
            BTreeMap::from([(SessionRole::Executor, executor_runner(&executor_provider))]);
        let supervisor = Supervisor::new(Arc::clone(&engine), runners);

        supervisor.spawn(DispatchedJob {
            session_id: child_id.clone(),
            parent_session: "s-parent".to_owned(),
            role: SessionRole::Executor,
            project: "arc".to_owned(),
            brief: "run the slow tool".to_owned(),
            budget: None,
        });

        wait_for_tool_call_issued(dir.path(), &child_id).await;
        // well past JOB_SILENCE_TIMEOUT, but the tool call is still pending
        tokio::time::advance(JOB_SILENCE_TIMEOUT * 2).await;
        tool_gate.notify_one();
        supervisor.shutdown().await;

        assert_eq!(
            only_job(supervisor.list()).state,
            job_info::State::Finished as i32,
            "a pending tool call is activity, not silence"
        );
    }
}
