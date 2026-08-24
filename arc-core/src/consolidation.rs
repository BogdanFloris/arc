pub mod extract;
pub mod replay;

use std::collections::HashSet;
use std::future::Future;

use arc_proto::v1::memory_event;
use tokio::sync::Mutex;

use crate::session::Engine;
use crate::store;

pub use crate::store::SessionSnapshot;

pub trait Extractor: Send + Sync {
    fn extract(
        &self,
        session: &SessionSnapshot,
    ) -> impl Future<Output = Result<Vec<memory_event::Event>, ExtractError>> + Send;
}

#[derive(Debug, thiserror::Error)]
#[error("extractor: {0}")]
pub struct ExtractError(pub String);

#[cfg(test)]
pub(crate) struct NoopExtractor;

#[cfg(test)]
impl Extractor for NoopExtractor {
    async fn extract(
        &self,
        _session: &SessionSnapshot,
    ) -> Result<Vec<memory_event::Event>, ExtractError> {
        Ok(Vec::new())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("consolidation store: {0}")]
    Store(#[from] store::Error),

    #[error("consolidation extractor for {session_id}: {source}")]
    Extractor {
        session_id: String,
        source: ExtractError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    NothingDue,
    Consolidated {
        session_id: String,
        through_seq: u64,
        records: usize,
        records_created: usize,
        records_superseded: usize,
    },
    Raced {
        session_id: String,
    },
}

#[tracing::instrument(
    name = "consolidation.pass",
    skip_all,
    fields(
        session_id = tracing::field::Empty,
        through_seq = tracing::field::Empty,
        records = tracing::field::Empty,
        outcome = tracing::field::Empty,
        counter.records_created = tracing::field::Empty,
        counter.records_superseded = tracing::field::Empty,
    )
)]
pub async fn run_pass<E: Extractor>(
    engine: &Mutex<Engine>,
    extractor: &E,
    idle_cutoff_micros: i64,
    prompt_version: &str,
    skip: &HashSet<String>,
) -> Result<Outcome, Error> {
    let span = tracing::Span::current();
    let result = pass(engine, extractor, idle_cutoff_micros, prompt_version, skip).await;
    match &result {
        Ok(Outcome::NothingDue) => {
            span.record("outcome", "nothing_due");
        }
        Ok(Outcome::Consolidated {
            session_id,
            through_seq,
            records,
            records_created,
            records_superseded,
        }) => {
            span.record("session_id", session_id.as_str());
            span.record("through_seq", through_seq);
            span.record("records", records);
            if *records_created > 0 {
                span.record("counter.records_created", records_created);
            }
            if *records_superseded > 0 {
                span.record("counter.records_superseded", records_superseded);
            }
            span.record("outcome", "consolidated");
        }
        Ok(Outcome::Raced { session_id }) => {
            span.record("session_id", session_id.as_str());
            span.record("outcome", "raced");
        }
        Err(_) => {
            span.record("outcome", "failed");
        }
    }
    result
}

async fn pass<E: Extractor>(
    engine: &Mutex<Engine>,
    extractor: &E,
    idle_cutoff_micros: i64,
    prompt_version: &str,
    skip: &HashSet<String>,
) -> Result<Outcome, Error> {
    let snapshot = {
        let engine = engine.lock().await;
        engine
            .store()
            .snapshot_for_consolidation(idle_cutoff_micros, skip)?
    };
    let Some(snapshot) = snapshot else {
        return Ok(Outcome::NothingDue);
    };

    let events = extractor
        .extract(&snapshot)
        .await
        .map_err(|source| Error::Extractor {
            session_id: snapshot.session_id.clone(),
            source,
        })?;
    let records = events.len();
    let records_created = events
        .iter()
        .filter(|event| matches!(event, memory_event::Event::RecordCreated(_)))
        .count();
    let records_superseded = events
        .iter()
        .filter(|event| matches!(event, memory_event::Event::RecordSuperseded(_)))
        .count();

    // re-locked, not held: extraction can take minutes
    let committed = {
        let mut engine = engine.lock().await;
        engine
            .store_mut()
            .commit_consolidation(&snapshot, events, prompt_version)?
    };
    Ok(if committed {
        Outcome::Consolidated {
            session_id: snapshot.session_id,
            through_seq: snapshot.latest_seq,
            records,
            records_created,
            records_superseded,
        }
    } else {
        Outcome::Raced {
            session_id: snapshot.session_id,
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arc_proto::v1::{
        MemoryRecord, MemoryRecordCreated, MemoryRecordSuperseded, Source, event, memory_event,
        memory_record, session_event,
    };
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    use super::{ExtractError, Extractor, NoopExtractor, Outcome, SessionSnapshot, run_pass};
    use crate::projection::Projection;
    use crate::testkit::{
        ScriptedProvider, TraceCapture, channel, counter_samples, done_reply, engine, replay_events,
    };

    const ALL_IDLE: i64 = i64::MAX;

    struct Scripted(Vec<memory_event::Event>);

    impl Extractor for Scripted {
        async fn extract(
            &self,
            _session: &SessionSnapshot,
        ) -> Result<Vec<memory_event::Event>, ExtractError> {
            Ok(self.0.clone())
        }
    }

    struct Failing;

    impl Extractor for Failing {
        async fn extract(
            &self,
            _session: &SessionSnapshot,
        ) -> Result<Vec<memory_event::Event>, ExtractError> {
            Err(ExtractError("boom".to_owned()))
        }
    }

    fn created_record(id: &str) -> memory_event::Event {
        memory_event::Event::RecordCreated(MemoryRecordCreated {
            record: Some(MemoryRecord {
                id: id.to_owned(),
                kind: memory_record::Kind::Fact as i32,
                namespace: "global".to_owned(),
                title: "extracted".to_owned(),
                summary: "an extracted fact".to_owned(),
                body: "the body".to_owned(),
                links: Vec::new(),
                provenance: None,
                status: memory_record::Status::Active as i32,
            }),
        })
    }

    fn marker(event: &arc_proto::v1::Event) -> &arc_proto::v1::SessionConsolidated {
        let Some(event::Payload::Session(session)) = &event.payload else {
            panic!("expected a session payload, got {event:?}");
        };
        let Some(session_event::Event::SessionConsolidated(marker)) = &session.event else {
            panic!("expected SessionConsolidated, got {session:?}");
        };
        marker
    }

    #[tokio::test]
    async fn a_pass_marks_the_session_and_a_second_finds_nothing_due() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let engine = Mutex::new(engine);
        let (tx, _rx) = channel();
        let reply = engine
            .lock()
            .await
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let outcome = run_pass(&engine, &NoopExtractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("pass");

        assert_eq!(
            outcome,
            Outcome::Consolidated {
                session_id: reply.session_id.clone(),
                through_seq: 2,
                records: 0,
                records_created: 0,
                records_superseded: 0,
            }
        );
        let events = replay_events(dir.path());
        assert_eq!(events.len(), 4, "the turn plus exactly one marker");
        let last = events.last().expect("events");
        assert_eq!(last.source, Source::System as i32, "arcd initiated this");
        assert_eq!(marker(last).session_id, reply.session_id);
        assert_eq!(marker(last).through_seq, 2);
        assert_eq!(marker(last).prompt_version, "");

        let outcome = run_pass(&engine, &NoopExtractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("second pass");
        assert_eq!(outcome, Outcome::NothingDue);
    }

    #[tokio::test]
    async fn a_recent_session_is_not_due() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let engine = Mutex::new(engine);
        let (tx, _rx) = channel();
        engine
            .lock()
            .await
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let outcome = run_pass(&engine, &NoopExtractor, 0, "", &HashSet::new())
            .await
            .expect("pass");

        assert_eq!(outcome, Outcome::NothingDue);
        assert_eq!(replay_events(dir.path()).len(), 3, "no marker appended");
    }

    #[tokio::test]
    async fn activity_during_the_pass_discards_it_whole() {
        let provider = ScriptedProvider::scripted(vec![done_reply("first"), done_reply("second")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let engine = Mutex::new(engine);
        let (tx, _rx) = channel();
        let reply = engine
            .lock()
            .await
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let snapshot = engine
            .lock()
            .await
            .store()
            .snapshot_for_consolidation(ALL_IDLE, &HashSet::new())
            .expect("snapshot")
            .expect("the session is due");
        assert_eq!(snapshot.session_id, reply.session_id);
        assert_eq!(snapshot.latest_seq, 2);
        assert_eq!(snapshot.rows.len(), 2, "both prose rows, for 7.2");

        let (tx, _rx) = channel();
        engine
            .lock()
            .await
            .send_message(&run, Some(&reply.session_id), "more", tx)
            .await
            .expect("send");

        let committed = engine
            .lock()
            .await
            .store_mut()
            .commit_consolidation(&snapshot, vec![created_record("mr-x")], "")
            .expect("commit");

        assert!(!committed, "the stale snapshot must not commit");
        for event in replay_events(dir.path()) {
            let Some(event::Payload::Session(session)) = &event.payload else {
                panic!("a memory event leaked from the discarded pass");
            };
            assert!(
                !matches!(
                    session.event,
                    Some(session_event::Event::SessionConsolidated(_))
                ),
                "a marker leaked from the discarded pass"
            );
        }
        let due = engine
            .lock()
            .await
            .store()
            .due_for_consolidation(ALL_IDLE)
            .expect("due");
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].latest_seq, 4, "coverage will span the new turn");
    }

    #[tokio::test]
    async fn extracted_records_land_as_system_before_the_marker() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let engine = Mutex::new(engine);
        let (tx, _rx) = channel();
        let reply = engine
            .lock()
            .await
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let outcome = run_pass(
            &engine,
            &Scripted(vec![created_record("mr-x")]),
            ALL_IDLE,
            "",
            &HashSet::new(),
        )
        .await
        .expect("pass");

        assert_eq!(
            outcome,
            Outcome::Consolidated {
                session_id: reply.session_id.clone(),
                through_seq: 2,
                records: 1,
                records_created: 1,
                records_superseded: 0,
            }
        );
        let events = replay_events(dir.path());
        assert_eq!(events.len(), 5);
        let record_event = &events[3];
        assert_eq!(record_event.source, Source::System as i32);
        assert!(
            matches!(record_event.payload, Some(event::Payload::Memory(_))),
            "the record precedes the marker"
        );
        assert_eq!(marker(&events[4]).through_seq, 2);

        let mut fresh = Projection::in_memory().expect("open");
        for event in &events {
            fresh.apply(event).expect("apply");
        }
        assert!(
            fresh
                .memory_record("mr-x")
                .expect("memory_record")
                .is_some()
        );
        assert_eq!(
            fresh.due_for_consolidation(ALL_IDLE).expect("due"),
            [],
            "replayed coverage keeps the session out"
        );
    }

    #[tokio::test]
    async fn a_create_and_a_supersede_show_both_counters() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let engine = Mutex::new(engine);
        let (tx, _rx) = channel();
        engine
            .lock()
            .await
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let extractor = Scripted(vec![
            created_record("mr-x"),
            memory_event::Event::RecordSuperseded(MemoryRecordSuperseded {
                superseded_id: "mr-x".to_owned(),
                record: Some(MemoryRecord {
                    id: "mr-y".to_owned(),
                    kind: memory_record::Kind::Fact as i32,
                    namespace: "global".to_owned(),
                    title: "corrected".to_owned(),
                    summary: "the corrected fact".to_owned(),
                    body: "the corrected body".to_owned(),
                    links: Vec::new(),
                    provenance: None,
                    status: memory_record::Status::Active as i32,
                }),
            }),
        ]);
        let capture = TraceCapture::start();
        let outcome = run_pass(&engine, &extractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("pass");
        let trace = capture.finish();

        assert!(
            matches!(
                outcome,
                Outcome::Consolidated {
                    records: 2,
                    records_created: 1,
                    records_superseded: 1,
                    ..
                }
            ),
            "got: {outcome:?}"
        );
        assert_eq!(counter_samples(&trace, "records_created"), [1.0]);
        assert_eq!(counter_samples(&trace, "records_superseded"), [1.0]);
    }

    #[tokio::test]
    async fn a_zero_yield_pass_emits_no_counters() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let engine = Mutex::new(engine);
        let (tx, _rx) = channel();
        engine
            .lock()
            .await
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let capture = TraceCapture::start();
        run_pass(&engine, &NoopExtractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("pass");
        let trace = capture.finish();

        for name in ["records_created", "records_superseded"] {
            assert!(
                counter_samples(&trace, name).is_empty(),
                "{name} must be absent on a zero-yield pass"
            );
        }
    }

    #[tokio::test]
    async fn a_failed_extraction_appends_nothing_and_names_its_session() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let engine = Mutex::new(engine);
        let (tx, _rx) = channel();
        let reply = engine
            .lock()
            .await
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let err = run_pass(&engine, &Failing, ALL_IDLE, "", &HashSet::new())
            .await
            .expect_err("the extractor's failure must surface");

        let super::Error::Extractor { session_id, .. } = err else {
            panic!("got: {err:?}");
        };
        assert_eq!(session_id, reply.session_id);
        assert_eq!(replay_events(dir.path()).len(), 3, "log untouched");
    }

    #[tokio::test]
    async fn a_skipped_session_yields_to_the_next_due() {
        let provider = ScriptedProvider::scripted(vec![done_reply("one"), done_reply("two")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let engine = Mutex::new(engine);
        for text in ["hi", "yo"] {
            let (tx, _rx) = channel();
            engine
                .lock()
                .await
                .send_message(&run, None, text, tx)
                .await
                .expect("send");
        }
        let due = engine
            .lock()
            .await
            .store()
            .due_for_consolidation(ALL_IDLE)
            .expect("due");
        assert_eq!(due.len(), 2);

        let mut skip = HashSet::new();
        skip.insert(due[0].session_id.clone());
        let outcome = run_pass(&engine, &NoopExtractor, ALL_IDLE, "", &skip)
            .await
            .expect("pass");
        assert!(
            matches!(
                &outcome,
                Outcome::Consolidated { session_id, .. } if *session_id == due[1].session_id
            ),
            "the pass must take the next due session, got: {outcome:?}"
        );

        assert_eq!(
            run_pass(&engine, &NoopExtractor, ALL_IDLE, "", &skip)
                .await
                .expect("pass"),
            Outcome::NothingDue
        );
    }
}
