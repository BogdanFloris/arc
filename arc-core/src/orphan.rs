//! Orphaned tool calls: detection, and the startup repair.
//!
//! The engine appends every `ToolCallIssued` before dispatching it, so a
//! daemon that dies mid-step leaves a durable call with no durable result.
//! The bytes cannot say whether the tool ran: the outcome is unknown, and the
//! call must be neither silently re-dispatched nor silently dropped.
//!
//! [`scan`] reads a log and returns the calls still open at its end. Only
//! arcd at startup may act on that list — an in-flight call in a live daemon
//! is byte-identical on disk to an abandoned one, so "orphaned" is the log
//! plus the fact that nobody is running. Every other reader treats an open
//! call as unknown-outcome and appends nothing; that is why the repair is an
//! appended event rather than something each reader synthesizes.
//!
//! [`close`] is the repair: one `ToolResultRecorded { outcome: UNKNOWN }` per
//! orphan, carrying [`CLOSER_CONTENT`], appended to the log and applied to
//! the projection in lockstep the same way `Engine::record` keeps them. The
//! next replay is then clean, and every call in a rebuilt transcript has an
//! answer. The closer lands at the log tail, possibly long after its call —
//! correlation is by `call_id`, never adjacency. arcd does not re-drive the
//! model afterward; the turn resumes on the next user message.

use std::collections::HashMap;

use arc_proto::v1::{Event, SessionEvent, Source, ToolOutcome, ToolResultRecorded};
use arc_proto::v1::{event, session_event};

use crate::log::{self, Log, LogReader};
use crate::projection::{self, Projection};
use crate::session::now_ts;

/// What a closer says. Fixed so tests can pin it and readers can trust it.
pub const CLOSER_CONTENT: &str = "The daemon restarted before this call's result was recorded; the call may or may not have run.";

/// A durable call with no durable result: everything a closer needs to name it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanCall {
    pub session_id: String,
    pub turn_id: String,
    pub call_id: String,
}

/// Everything the orphan pass can fail with.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Reading the log during the scan, or appending a closer, failed.
    /// Durable state is not in a known condition; a daemon must not start.
    #[error("orphan pass log: {0}")]
    Log(#[from] log::Error),

    /// The index refused a closer the log accepted — log and projection are
    /// out of step, which is a bug, not a runtime condition.
    #[error("orphan pass projection: {0}")]
    Projection(#[from] projection::Error),
}

/// Replays `reader` and returns the calls still open at the end of the log,
/// in log order.
///
/// A call is open from its `ToolCallIssued` until a `ToolResultRecorded`
/// names its `call_id`. Tracking is by `(session_id, call_id)` — `call_id`
/// is unique per session, not globally — and by id only, never adjacency:
/// events from other turns and sessions between a call and its result change
/// nothing.
///
/// # Errors
///
/// [`log::Error`] if the log cannot be read to its end. A partial scan says
/// nothing about what is open, so nothing is returned.
pub fn scan(reader: LogReader) -> Result<Vec<OrphanCall>, log::Error> {
    let mut slots: Vec<Option<OrphanCall>> = Vec::new();
    let mut open: HashMap<(String, String), usize> = HashMap::new();
    for read in reader {
        let event = read?;
        let Some(event::Payload::Session(session)) = event.payload else {
            continue;
        };
        match session.event {
            Some(session_event::Event::ToolCallIssued(call)) => {
                open.insert((call.session_id.clone(), call.call_id.clone()), slots.len());
                slots.push(Some(OrphanCall {
                    session_id: call.session_id,
                    turn_id: call.turn_id,
                    call_id: call.call_id,
                }));
            }
            Some(session_event::Event::ToolResultRecorded(result)) => {
                if let Some(slot) = open.remove(&(result.session_id, result.call_id)) {
                    slots[slot] = None;
                }
            }
            _ => {}
        }
    }
    Ok(slots.into_iter().flatten().collect())
}

/// Appends one UNKNOWN closer per orphan and applies each to the projection,
/// keeping log and index in lockstep (log stamps seq, then the projection
/// applies).
///
/// The closer copies the call's `session_id`, `turn_id`, and `call_id`, says
/// [`CLOSER_CONTENT`], and carries `Event.source = SYSTEM`: arcd concluded
/// this, not the tool.
///
/// # Errors
///
/// [`Error::Log`] if an append fails, [`Error::Projection`] if the index
/// refuses what the log accepted. Closers appended before the failure are
/// durable and stay; the pass is idempotent, so running it again closes only
/// what remains.
pub fn close(
    orphans: &[OrphanCall],
    log: &mut Log,
    projection: &mut Projection,
) -> Result<(), Error> {
    for orphan in orphans {
        let mut event = Event {
            seq: 0, // added by the log
            ts: Some(now_ts()),
            source: Source::System as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::ToolResultRecorded(
                    ToolResultRecorded {
                        session_id: orphan.session_id.clone(),
                        turn_id: orphan.turn_id.clone(),
                        call_id: orphan.call_id.clone(),
                        outcome: ToolOutcome::Unknown as i32,
                        content: CLOSER_CONTENT.to_owned(),
                        truncated: false,
                    },
                )),
            })),
        };
        let seq = log.append(event.clone())?;
        event.seq = seq;
        projection.apply(&event)?;
    }
    Ok(())
}

/// The whole startup pass: [`scan`], then [`close`], returning what was
/// closed. Zero orphans is the common case and costs one span, not silence.
///
/// # Errors
///
/// See [`scan`] and [`close`].
#[tracing::instrument(
    level = "info",
    name = "orphan.close_pass",
    skip_all,
    fields(orphans = tracing::field::Empty)
)]
pub fn close_orphans(
    reader: LogReader,
    log: &mut Log,
    projection: &mut Projection,
) -> Result<Vec<OrphanCall>, Error> {
    let orphans = scan(reader)?;
    tracing::Span::current().record("orphans", orphans.len());
    close(&orphans, log, projection)?;
    Ok(orphans)
}

#[cfg(test)]
mod tests {
    use arc_proto::v1::{
        Event, MessageAppended, Role, SessionEvent, Source, ToolCallIssued, ToolOutcome,
        ToolResultRecorded,
    };
    use arc_proto::v1::{event, session_event};
    use tempfile::TempDir;

    use super::{CLOSER_CONTENT, OrphanCall, close, close_orphans, scan};
    use crate::log::Log;
    use crate::projection::{self, Projection};

    fn wrap(source: Source, payload: session_event::Event) -> Event {
        Event {
            seq: 0,
            ts: None,
            source: source as i32,
            payload: Some(event::Payload::Session(SessionEvent {
                event: Some(payload),
            })),
        }
    }

    fn issued(session: &str, turn: &str, call: &str) -> Event {
        wrap(
            Source::Model,
            session_event::Event::ToolCallIssued(ToolCallIssued {
                session_id: session.to_owned(),
                turn_id: turn.to_owned(),
                call_id: call.to_owned(),
                index: 0,
                name: "lookup".to_owned(),
                arguments_json: "{}".to_owned(),
            }),
        )
    }

    fn resulted(session: &str, turn: &str, call: &str) -> Event {
        wrap(
            Source::System,
            session_event::Event::ToolResultRecorded(ToolResultRecorded {
                session_id: session.to_owned(),
                turn_id: turn.to_owned(),
                call_id: call.to_owned(),
                outcome: ToolOutcome::Ok as i32,
                content: "found".to_owned(),
                truncated: false,
            }),
        )
    }

    fn message(session: &str, turn: &str, content: &str) -> Event {
        wrap(
            Source::User,
            session_event::Event::MessageAppended(MessageAppended {
                session_id: session.to_owned(),
                role: Role::User as i32,
                content: content.to_owned(),
                partial: false,
                turn_id: turn.to_owned(),
            }),
        )
    }

    fn orphan(session: &str, turn: &str, call: &str) -> OrphanCall {
        OrphanCall {
            session_id: session.to_owned(),
            turn_id: turn.to_owned(),
            call_id: call.to_owned(),
        }
    }

    /// A log holding `events`, appended in order.
    fn log_with(dir: &TempDir, events: Vec<Event>) -> Log {
        let mut log = Log::open(dir.path()).expect("open log");
        for event in events {
            log.append(event).expect("append");
        }
        log
    }

    /// A projection caught up with `log`, the way the daemon starts.
    fn replayed(log: &Log) -> Projection {
        let mut projection = Projection::open(":memory:").expect("open projection");
        projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
        projection
    }

    fn scan_log(log: &Log) -> Vec<OrphanCall> {
        scan(log.reader().expect("reader")).expect("scan")
    }

    fn replay_events(log: &Log) -> Vec<Event> {
        log.reader()
            .expect("reader")
            .map(|read| read.expect("replay"))
            .collect()
    }

    fn closer_of(event: &Event) -> &ToolResultRecorded {
        let Some(event::Payload::Session(session)) = event.payload.as_ref() else {
            panic!("expected a session event, got {event:?}");
        };
        match session.event.as_ref() {
            Some(session_event::Event::ToolResultRecorded(result)) => result,
            other => panic!("expected a tool result, got {other:?}"),
        }
    }

    #[test]
    fn an_unanswered_call_is_closed_as_unknown() {
        let dir = TempDir::new().expect("temp dir");
        let mut log = log_with(
            &dir,
            vec![message("s1", "t1", "hi"), issued("s1", "t1", "c1")],
        );
        let mut projection = replayed(&log);

        let orphans = scan_log(&log);
        assert_eq!(orphans, [orphan("s1", "t1", "c1")]);

        close(&orphans, &mut log, &mut projection).expect("close");

        let events = replay_events(&log);
        let last = events.last().expect("events");
        assert_eq!(last.source, Source::System as i32);
        let result = closer_of(last);
        assert_eq!(result.session_id, "s1");
        assert_eq!(result.turn_id, "t1");
        assert_eq!(result.call_id, "c1");
        assert_eq!(result.outcome, ToolOutcome::Unknown as i32);
        assert_eq!(result.content, CLOSER_CONTENT);
        assert!(!result.truncated);

        // The projection kept pace: the closer went through apply, not replay.
        assert_eq!(
            projection.last_seq().expect("last_seq"),
            Some(last.seq),
            "log and index in lockstep"
        );
    }

    #[test]
    fn the_pass_is_idempotent_across_restarts() {
        let dir = TempDir::new().expect("temp dir");
        let mut log = log_with(&dir, vec![issued("s1", "t1", "c1")]);
        let mut projection = replayed(&log);
        let reader = log.reader().expect("reader");
        let closed = close_orphans(reader, &mut log, &mut projection).expect("close pass");
        assert_eq!(closed.len(), 1);

        // The next startup: reopen the log, scan again.
        drop(log);
        let mut log = Log::open(dir.path()).expect("reopen log");
        let mut projection = replayed(&log);
        let before = log.next_seq();
        let reader = log.reader().expect("reader");
        let closed = close_orphans(reader, &mut log, &mut projection).expect("close pass");

        assert_eq!(closed, [], "the first pass left no open call");
        assert_eq!(log.next_seq(), before, "nothing appended");
    }

    #[test]
    fn an_answered_call_is_not_an_orphan() {
        let dir = TempDir::new().expect("temp dir");
        let log = log_with(
            &dir,
            vec![issued("s1", "t1", "c1"), resulted("s1", "t1", "c1")],
        );

        assert_eq!(scan_log(&log), []);
    }

    #[test]
    fn only_the_unanswered_parallel_call_closes() {
        let dir = TempDir::new().expect("temp dir");
        let mut log = log_with(
            &dir,
            vec![
                issued("s1", "t1", "a"),
                issued("s1", "t1", "b"),
                resulted("s1", "t1", "b"),
            ],
        );
        let mut projection = replayed(&log);

        let orphans = scan_log(&log);
        assert_eq!(orphans, [orphan("s1", "t1", "a")]);

        close(&orphans, &mut log, &mut projection).expect("close");

        let events = replay_events(&log);
        let unknowns: Vec<&ToolResultRecorded> = events
            .iter()
            .filter_map(|event| match event.payload.as_ref() {
                Some(event::Payload::Session(session)) => match session.event.as_ref() {
                    Some(session_event::Event::ToolResultRecorded(result))
                        if result.outcome == ToolOutcome::Unknown as i32 =>
                    {
                        Some(result)
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect();
        assert_eq!(unknowns.len(), 1, "exactly the unanswered call closes");
        assert_eq!(unknowns[0].call_id, "a");
    }

    #[test]
    fn the_same_call_id_in_two_sessions_is_tracked_independently() {
        let dir = TempDir::new().expect("temp dir");
        let log = log_with(
            &dir,
            vec![
                issued("s1", "t1", "c1"),
                issued("s2", "t9", "c1"),
                resulted("s1", "t1", "c1"),
            ],
        );

        assert_eq!(
            scan_log(&log),
            [orphan("s2", "t9", "c1")],
            "only the truly open call is orphaned"
        );
    }

    #[test]
    fn later_events_in_the_turn_do_not_hide_the_orphan() {
        let dir = TempDir::new().expect("temp dir");
        let log = log_with(
            &dir,
            vec![
                issued("s1", "t1", "c1"),
                message("s2", "t2", "elsewhere"),
                message("s1", "t1", "continue"),
            ],
        );

        assert_eq!(
            scan_log(&log),
            [orphan("s1", "t1", "c1")],
            "correlation is by id, not adjacency"
        );
    }

    #[test]
    fn zero_orphans_leaves_the_log_untouched() {
        let dir = TempDir::new().expect("temp dir");
        let mut log = log_with(
            &dir,
            vec![
                message("s1", "t1", "hi"),
                issued("s1", "t1", "c1"),
                resulted("s1", "t1", "c1"),
            ],
        );
        let mut projection = replayed(&log);
        let before = log.next_seq();

        let reader = log.reader().expect("reader");
        let closed = close_orphans(reader, &mut log, &mut projection).expect("close pass");

        assert_eq!(closed, []);
        assert_eq!(log.next_seq(), before, "no event appended");
    }

    #[test]
    fn orphans_close_in_log_order() {
        let dir = TempDir::new().expect("temp dir");
        let mut log = log_with(
            &dir,
            vec![issued("s1", "t1", "first"), issued("s2", "t2", "second")],
        );
        let mut projection = replayed(&log);

        let reader = log.reader().expect("reader");
        let closed = close_orphans(reader, &mut log, &mut projection).expect("close pass");

        assert_eq!(
            closed,
            [orphan("s1", "t1", "first"), orphan("s2", "t2", "second")]
        );
        let events = replay_events(&log);
        let tail = &events[events.len() - 2..];
        assert_eq!(closer_of(&tail[0]).call_id, "first");
        assert_eq!(closer_of(&tail[1]).call_id, "second");
    }
}
