use std::collections::HashMap;

use arc_proto::v1::{SessionEvent, Source, ToolOutcome, ToolResultRecorded};
use arc_proto::v1::{event, session_event};

use crate::log::{self, LogReader};
use crate::projection;
use crate::store::Store;
use crate::store::now_ts;

pub(crate) const CLOSER_CONTENT: &str = "The daemon restarted before this call's result was recorded; the call may or may not have run.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanCall {
    pub session_id: String,
    pub turn_id: String,
    pub call_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("orphan pass log: {0}")]
    Log(#[from] log::Error),

    #[error("orphan pass projection: {0}")]
    Projection(#[from] projection::Error),

    #[error("orphan pass store: {0}")]
    Store(#[from] crate::store::Error),
}

pub(crate) fn scan(reader: LogReader) -> Result<Vec<OrphanCall>, log::Error> {
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

pub(crate) fn close(orphans: &[OrphanCall], store: &mut Store) -> Result<(), Error> {
    for orphan in orphans {
        let payload = event::Payload::Session(SessionEvent {
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
        });
        store.append(Source::System, Some(now_ts()), payload)?;
    }
    Ok(())
}

#[tracing::instrument(
    level = "info",
    name = "orphan.close_pass",
    skip_all,
    fields(orphans = tracing::field::Empty)
)]
pub fn close_orphans(reader: LogReader, store: &mut Store) -> Result<Vec<OrphanCall>, Error> {
    let orphans = scan(reader)?;
    tracing::Span::current().record("orphans", orphans.len());
    close(&orphans, store)?;
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
    use crate::store::Store;

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
                provider_roundtrip: Vec::new(),
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

    fn log_with(dir: &TempDir, events: Vec<Event>) -> Log {
        let mut log = Log::open(dir.path()).expect("open log");
        for event in events {
            log.append(event).expect("append");
        }
        log
    }

    fn store_with(dir: &TempDir, events: Vec<Event>) -> Store {
        let log = log_with(dir, events);
        let mut projection = Projection::in_memory().expect("open projection");
        projection::replay(log.reader().expect("reader"), &mut projection).expect("replay");
        Store::new(log, projection)
    }

    fn scan_store(store: &Store) -> Vec<OrphanCall> {
        scan(store.reader().expect("reader")).expect("scan")
    }

    fn replay_events(store: &Store) -> Vec<Event> {
        store
            .reader()
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
        let mut store = store_with(
            &dir,
            vec![message("s1", "t1", "hi"), issued("s1", "t1", "c1")],
        );

        let orphans = scan_store(&store);
        assert_eq!(orphans, [orphan("s1", "t1", "c1")]);

        close(&orphans, &mut store).expect("close");

        let events = replay_events(&store);
        let last = events.last().expect("events");
        assert_eq!(last.source, Source::System as i32);
        let result = closer_of(last);
        assert_eq!(result.session_id, "s1");
        assert_eq!(result.turn_id, "t1");
        assert_eq!(result.call_id, "c1");
        assert_eq!(result.outcome, ToolOutcome::Unknown as i32);
        assert_eq!(result.content, CLOSER_CONTENT);
        assert!(!result.truncated);

        assert_eq!(
            store.projection().last_seq().expect("last_seq"),
            Some(last.seq),
            "log and index in lockstep"
        );
    }

    #[test]
    fn the_pass_is_idempotent_across_restarts() {
        let dir = TempDir::new().expect("temp dir");
        let mut store = store_with(&dir, vec![issued("s1", "t1", "c1")]);
        let reader = store.reader().expect("reader");
        let closed = close_orphans(reader, &mut store).expect("close pass");
        assert_eq!(closed.len(), 1);

        drop(store);
        let mut store = store_with(&dir, Vec::new());
        let before = store.next_seq();
        let reader = store.reader().expect("reader");
        let closed = close_orphans(reader, &mut store).expect("close pass");

        assert_eq!(closed, [], "the first pass left no open call");
        assert_eq!(store.next_seq(), before, "nothing appended");
    }

    #[test]
    fn an_answered_call_is_not_an_orphan() {
        let dir = TempDir::new().expect("temp dir");
        let store = store_with(
            &dir,
            vec![issued("s1", "t1", "c1"), resulted("s1", "t1", "c1")],
        );

        assert_eq!(scan_store(&store), []);
    }

    #[test]
    fn only_the_unanswered_parallel_call_closes() {
        let dir = TempDir::new().expect("temp dir");
        let mut store = store_with(
            &dir,
            vec![
                issued("s1", "t1", "a"),
                issued("s1", "t1", "b"),
                resulted("s1", "t1", "b"),
            ],
        );

        let orphans = scan_store(&store);
        assert_eq!(orphans, [orphan("s1", "t1", "a")]);

        close(&orphans, &mut store).expect("close");

        let events = replay_events(&store);
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
        let store = store_with(
            &dir,
            vec![
                issued("s1", "t1", "c1"),
                issued("s2", "t9", "c1"),
                resulted("s1", "t1", "c1"),
            ],
        );

        assert_eq!(
            scan_store(&store),
            [orphan("s2", "t9", "c1")],
            "only the truly open call is orphaned"
        );
    }

    #[test]
    fn later_events_in_the_turn_do_not_hide_the_orphan() {
        let dir = TempDir::new().expect("temp dir");
        let store = store_with(
            &dir,
            vec![
                issued("s1", "t1", "c1"),
                message("s2", "t2", "elsewhere"),
                message("s1", "t1", "continue"),
            ],
        );

        assert_eq!(
            scan_store(&store),
            [orphan("s1", "t1", "c1")],
            "correlation is by id, not adjacency"
        );
    }

    #[test]
    fn zero_orphans_leaves_the_log_untouched() {
        let dir = TempDir::new().expect("temp dir");
        let mut store = store_with(
            &dir,
            vec![
                message("s1", "t1", "hi"),
                issued("s1", "t1", "c1"),
                resulted("s1", "t1", "c1"),
            ],
        );
        let before = store.next_seq();

        let reader = store.reader().expect("reader");
        let closed = close_orphans(reader, &mut store).expect("close pass");

        assert_eq!(closed, []);
        assert_eq!(store.next_seq(), before, "no event appended");
    }

    #[test]
    fn orphans_close_in_log_order() {
        let dir = TempDir::new().expect("temp dir");
        let mut store = store_with(
            &dir,
            vec![issued("s1", "t1", "first"), issued("s2", "t2", "second")],
        );

        let reader = store.reader().expect("reader");
        let closed = close_orphans(reader, &mut store).expect("close pass");

        assert_eq!(
            closed,
            [orphan("s1", "t1", "first"), orphan("s2", "t2", "second")]
        );
        let events = replay_events(&store);
        let tail = &events[events.len() - 2..];
        assert_eq!(closer_of(&tail[0]).call_id, "first");
        assert_eq!(closer_of(&tail[1]).call_id, "second");
    }
}
