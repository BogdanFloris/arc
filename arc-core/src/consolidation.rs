//! The consolidation pass: end-of-session extraction's trigger and plumbing
//! (DESIGN.md §5.4).
//!
//! [`run_pass`] is one pass, lock-shaped for a shared sidecar: it snapshots
//! the first due session under the engine lock, runs the [`Extractor`] with
//! nobody blocked, then re-checks under the lock that the session is still
//! idle before appending the extractor's records and the coverage marker
//! together. New activity since the snapshot discards the pass whole — a
//! later idle timeout re-runs it over the longer history. The locked halves
//! live on the engine (`snapshot_for_consolidation`, `commit_consolidation`):
//! arc-core owns the invariants, the daemon owns scheduling.
//!
//! What extraction *is* — the prompt, merging, superseding — lives in
//! [`extract`] behind the [`Extractor`] seam. [`NoopExtractor`] remains for
//! tests of the pass itself; a finished pass always appends its marker:
//! "looked and found nothing durable" is a decision, and coverage is what
//! makes the next due-query honest.

pub mod extract;
pub mod replay;

use std::collections::HashSet;
use std::future::Future;

use arc_proto::v1::memory_event;
use tokio::sync::Mutex;

use crate::projection::{MemoryIndexEntry, MessageRow};
use crate::provider::Provider;
use crate::session::{self, Engine};

/// One session as the pass read it: the rows the extractor works over, and
/// the seq the whole pass is pinned to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSnapshot {
    /// The session being consolidated.
    pub session_id: String,
    /// Every projected row, all kinds, in seq order.
    pub rows: Vec<MessageRow>,
    /// Seq of the session's last event at snapshot time. Becomes the
    /// marker's `through_seq`; the commit re-check compares against it.
    pub latest_seq: u64,
    /// The ACTIVE records at snapshot time. The extractor cannot call tools;
    /// this index is its entire view of existing memory, and the summaries
    /// are its merge signal (DESIGN.md §5.4).
    pub memory_index: Vec<MemoryIndexEntry>,
}

/// The extraction seam (task 7.2 fills it): turn a session snapshot into
/// memory events for the engine to append.
///
/// The returned future is `Send` for the same reason `Provider::complete`'s
/// is: the daemon drives passes from a spawned task.
pub trait Extractor: Send + Sync {
    /// Extracts durable facts from `session`. An empty vec is a valid
    /// answer — the pass still appends its marker.
    fn extract(
        &self,
        session: &SessionSnapshot,
    ) -> impl Future<Output = Result<Vec<memory_event::Event>, ExtractError>> + Send;
}

/// An extraction failure, as the seam speaks it. One string arm on purpose:
/// every failure — timeout, bad JSON, bad op — means the same thing to the
/// pass (nothing is appended), and the text is for a person reading the log.
#[derive(Debug, thiserror::Error)]
#[error("extractor: {0}")]
pub struct ExtractError(pub String);

/// The extractor that extracts nothing and never calls a model. Kept for
/// tests of the pass itself — trigger, coverage, race handling.
pub struct NoopExtractor;

impl Extractor for NoopExtractor {
    async fn extract(
        &self,
        _session: &SessionSnapshot,
    ) -> Result<Vec<memory_event::Event>, ExtractError> {
        Ok(Vec::new())
    }
}

/// Everything one pass can fail with.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The engine refused a read or an append; see [`session::Error`].
    #[error("consolidation engine: {0}")]
    Engine(#[from] session::Error),

    /// The extractor failed. Nothing was appended: a pass whose output is in
    /// doubt writes nothing (docs/prior-art-hermes.md §3). Carries the
    /// session so the caller can keep strikes per session.
    #[error("consolidation extractor for {session_id}: {source}")]
    Extractor {
        /// The session whose extraction failed.
        session_id: String,
        /// What went wrong.
        source: ExtractError,
    },
}

/// How one pass ended. Every arm is also the `outcome` field of its
/// `consolidation.pass` span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// No session was idle past the cutoff with events left to cover.
    NothingDue,
    /// The pass finished a session: records appended, marker appended.
    Consolidated {
        /// The session covered.
        session_id: String,
        /// The marker's coverage.
        through_seq: u64,
        /// Extractor events appended before the marker.
        records: usize,
    },
    /// The session spoke while the extractor ran; the pass was discarded
    /// whole and the log holds nothing from it.
    Raced {
        /// The session that got away.
        session_id: String,
    },
}

/// One consolidation pass over the first due session, if any.
///
/// One session at a time — bounded concurrency, hermes-style: the bound
/// exists, and v1 sets it to one. The caller ticks; a backlog drains one
/// pass per tick.
///
/// # Errors
///
/// - [`Error::Engine`] if a read or append failed. Appends are sequential,
///   so a mid-commit failure can leave records without their marker; the
///   next pass sees the session still due and re-covers it (the projection's
///   guarded writes make replaying the log safe regardless).
/// - [`Error::Extractor`] if extraction failed; nothing was appended.
// Not generalized over hashers: `skip` is a plain in-process set, and the
// signature stays readable.
#[allow(clippy::implicit_hasher)]
#[tracing::instrument(
    name = "consolidation.pass",
    skip_all,
    fields(
        session_id = tracing::field::Empty,
        through_seq = tracing::field::Empty,
        records = tracing::field::Empty,
        outcome = tracing::field::Empty,
    )
)]
pub async fn run_pass<P: Provider, E: Extractor>(
    engine: &Mutex<Engine<P>>,
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
        }) => {
            span.record("session_id", session_id.as_str());
            span.record("through_seq", through_seq);
            span.record("records", records);
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

/// [`run_pass`] minus the span bookkeeping: the three steps, in order.
async fn pass<P: Provider, E: Extractor>(
    engine: &Mutex<Engine<P>>,
    extractor: &E,
    idle_cutoff_micros: i64,
    prompt_version: &str,
    skip: &HashSet<String>,
) -> Result<Outcome, Error> {
    // Step 1 — under the lock: pick the first due session, snapshot it.
    let snapshot = engine
        .lock()
        .await
        .snapshot_for_consolidation(idle_cutoff_micros, skip)?;
    let Some(snapshot) = snapshot else {
        return Ok(Outcome::NothingDue);
    };

    // Step 2 — lock released: the model runs with nobody blocked.
    let events = extractor
        .extract(&snapshot)
        .await
        .map_err(|source| Error::Extractor {
            session_id: snapshot.session_id.clone(),
            source,
        })?;
    let records = events.len();

    // Step 3 — under the lock again: commit only if the session stayed idle.
    let committed = engine
        .lock()
        .await
        .commit_consolidation(&snapshot, events, prompt_version)?;
    Ok(if committed {
        Outcome::Consolidated {
            session_id: snapshot.session_id,
            through_seq: snapshot.latest_seq,
            records,
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
        MemoryRecord, MemoryRecordCreated, Source, event, memory_event, memory_record,
        session_event,
    };
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    use super::{ExtractError, Extractor, NoopExtractor, Outcome, SessionSnapshot, run_pass};
    use crate::projection::Projection;
    use crate::testkit::{ScriptedProvider, channel, done_reply, engine, replay_events};

    /// A cutoff after any clock this test run reads: every session is idle.
    const ALL_IDLE: i64 = i64::MAX;

    /// An extractor with a canned answer.
    struct Scripted(Vec<memory_event::Event>);

    impl Extractor for Scripted {
        async fn extract(
            &self,
            _session: &SessionSnapshot,
        ) -> Result<Vec<memory_event::Event>, ExtractError> {
            Ok(self.0.clone())
        }
    }

    /// An extractor that always fails.
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

    /// The marker inside a whole `Event`, or a panic.
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
        let engine = Mutex::new(engine(&provider, &dir));
        let (tx, _rx) = channel();
        let reply = engine
            .lock()
            .await
            .send_message(None, "hi", tx)
            .await
            .expect("send");

        let outcome = run_pass(&engine, &NoopExtractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("pass");

        // Seqs 0..=2 are the turn; the pass covered through its last event.
        assert_eq!(
            outcome,
            Outcome::Consolidated {
                session_id: reply.session_id.clone(),
                through_seq: 2,
                records: 0,
            }
        );
        let events = replay_events(&dir);
        assert_eq!(events.len(), 4, "the turn plus exactly one marker");
        let last = events.last().expect("events");
        assert_eq!(last.source, Source::System as i32, "arcd initiated this");
        assert_eq!(marker(last).session_id, reply.session_id);
        assert_eq!(marker(last).through_seq, 2);
        assert_eq!(marker(last).prompt_version, "");

        // Coverage holds: the next tick has nothing to do.
        let outcome = run_pass(&engine, &NoopExtractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("second pass");
        assert_eq!(outcome, Outcome::NothingDue);
    }

    #[tokio::test]
    async fn a_recent_session_is_not_due() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let engine = Mutex::new(engine(&provider, &dir));
        let (tx, _rx) = channel();
        engine
            .lock()
            .await
            .send_message(None, "hi", tx)
            .await
            .expect("send");

        // Cutoff at the epoch: nothing has been idle since before then.
        let outcome = run_pass(&engine, &NoopExtractor, 0, "", &HashSet::new())
            .await
            .expect("pass");

        assert_eq!(outcome, Outcome::NothingDue);
        assert_eq!(replay_events(&dir).len(), 3, "no marker appended");
    }

    /// The race path, step by step: activity between the snapshot and the
    /// commit discards the pass whole — no records, no marker.
    #[tokio::test]
    async fn activity_during_the_pass_discards_it_whole() {
        let provider = ScriptedProvider::scripted(vec![done_reply("first"), done_reply("second")]);
        let dir = TempDir::new().expect("temp dir");
        let engine = Mutex::new(engine(&provider, &dir));
        let (tx, _rx) = channel();
        let reply = engine
            .lock()
            .await
            .send_message(None, "hi", tx)
            .await
            .expect("send");

        let snapshot = engine
            .lock()
            .await
            .snapshot_for_consolidation(ALL_IDLE, &HashSet::new())
            .expect("snapshot")
            .expect("the session is due");
        assert_eq!(snapshot.session_id, reply.session_id);
        assert_eq!(snapshot.latest_seq, 2);
        assert_eq!(snapshot.rows.len(), 2, "both prose rows, for 7.2");

        // The user comes back mid-extraction.
        let (tx, _rx) = channel();
        engine
            .lock()
            .await
            .send_message(Some(&reply.session_id), "more", tx)
            .await
            .expect("send");

        let committed = engine
            .lock()
            .await
            .commit_consolidation(&snapshot, vec![created_record("mr-x")], "")
            .expect("commit");

        assert!(!committed, "the stale snapshot must not commit");
        // Nothing consolidation-shaped in the log: no memory event, no marker.
        for event in replay_events(&dir) {
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
        // And the longer history is still due for a later pass.
        let due = engine
            .lock()
            .await
            .due_for_consolidation(ALL_IDLE)
            .expect("due");
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].latest_seq, 4, "coverage will span the new turn");
    }

    /// Extracted records land with `Source::System` — arcd initiated the
    /// write — and before the marker, in one uninterrupted run under the lock.
    #[tokio::test]
    async fn extracted_records_land_as_system_before_the_marker() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let engine = Mutex::new(engine(&provider, &dir));
        let (tx, _rx) = channel();
        let reply = engine
            .lock()
            .await
            .send_message(None, "hi", tx)
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
            }
        );
        let events = replay_events(&dir);
        assert_eq!(events.len(), 5);
        let record_event = &events[3];
        assert_eq!(record_event.source, Source::System as i32);
        assert!(
            matches!(record_event.payload, Some(event::Payload::Memory(_))),
            "the record precedes the marker"
        );
        assert_eq!(marker(&events[4]).through_seq, 2);

        // A fresh replay agrees: the record is in the distilled tier and the
        // session's coverage is set — the pass is deterministic history now.
        let mut fresh = Projection::open(":memory:").expect("open");
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
    async fn a_failed_extraction_appends_nothing_and_names_its_session() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let engine = Mutex::new(engine(&provider, &dir));
        let (tx, _rx) = channel();
        let reply = engine
            .lock()
            .await
            .send_message(None, "hi", tx)
            .await
            .expect("send");

        let err = run_pass(&engine, &Failing, ALL_IDLE, "", &HashSet::new())
            .await
            .expect_err("the extractor's failure must surface");

        // The error names the session, so the caller's strikes map has a key.
        let super::Error::Extractor { session_id, .. } = err else {
            panic!("got: {err:?}");
        };
        assert_eq!(session_id, reply.session_id);
        assert_eq!(replay_events(&dir).len(), 3, "log untouched");
    }

    /// The strikes seam: a skipped session yields the slot to the next due
    /// one, and skipping everything due reads as nothing due.
    #[tokio::test]
    async fn a_skipped_session_yields_to_the_next_due() {
        let provider = ScriptedProvider::scripted(vec![done_reply("one"), done_reply("two")]);
        let dir = TempDir::new().expect("temp dir");
        let engine = Mutex::new(engine(&provider, &dir));
        for text in ["hi", "yo"] {
            let (tx, _rx) = channel();
            engine
                .lock()
                .await
                .send_message(None, text, tx)
                .await
                .expect("send");
        }
        let due = engine
            .lock()
            .await
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

        // The skipped session stays due but never gets the slot.
        assert_eq!(
            run_pass(&engine, &NoopExtractor, ALL_IDLE, "", &skip)
                .await
                .expect("pass"),
            Outcome::NothingDue
        );
    }
}
