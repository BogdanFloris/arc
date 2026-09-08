pub mod extract;
pub mod replay;

use std::collections::{HashMap, HashSet};
use std::future::Future;

use arc_proto::v1::{SessionRole, Source, memory_event};

use crate::session::Engine;
use crate::store;

pub use crate::store::SessionSnapshot;

pub trait Extractor: Send + Sync {
    fn extract(
        &self,
        session: &SessionSnapshot,
    ) -> impl Future<Output = Result<Vec<memory_event::Event>, ExtractError>> + Send;

    fn title(
        &self,
        _session: &SessionSnapshot,
    ) -> impl Future<Output = Result<Option<String>, ExtractError>> + Send {
        async { Ok(None) }
    }
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
    engine: &Engine,
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
    engine: &Engine,
    extractor: &E,
    idle_cutoff_micros: i64,
    prompt_version: &str,
    skip: &HashSet<String>,
) -> Result<Outcome, Error> {
    let snapshot =
        engine.with_store(|store| store.snapshot_for_consolidation(idle_cutoff_micros, skip))?;
    let Some(snapshot) = snapshot else {
        return Ok(Outcome::NothingDue);
    };

    // the store is not locked during extraction: it can take minutes
    let events = if extracts_user_facts(snapshot.source, snapshot.role) {
        extractor
            .extract(&snapshot)
            .await
            .map_err(|source| Error::Extractor {
                session_id: snapshot.session_id.clone(),
                source,
            })?
    } else {
        tracing::info!(
            session_id = %snapshot.session_id,
            source = source_name(snapshot.source),
            role = role_name(snapshot.role),
            "session source holds no user facts; skipping extraction"
        );
        Vec::new()
    };
    let records = events.len();
    let records_created = events
        .iter()
        .filter(|event| matches!(event, memory_event::Event::RecordCreated(_)))
        .count();
    let records_superseded = events
        .iter()
        .filter(|event| matches!(event, memory_event::Event::RecordSuperseded(_)))
        .count();

    // one store-lock scope: the re-check and the append are atomic together
    let committed = engine
        .with_store_mut(|store| store.commit_consolidation(&snapshot, events, prompt_version))?;
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

#[derive(Default)]
pub struct Titles {
    attempted: HashMap<String, String>,
}

impl Titles {
    #[tracing::instrument(name = "session.titles", skip_all)]
    pub async fn run<E: Extractor>(&mut self, engine: &Engine, extractor: &E) -> Result<(), Error> {
        let sessions = engine
            .with_store(|store| store.projection().untitled_sessions())
            .map_err(store::Error::from)?;
        for session_id in sessions {
            if let Err(error) = self.run_session(engine, extractor, &session_id).await {
                tracing::warn!(%error, %session_id, "title generation failed");
            }
        }
        Ok(())
    }

    pub async fn run_session<E: Extractor>(
        &mut self,
        engine: &Engine,
        extractor: &E,
        session_id: &str,
    ) -> Result<(), Error> {
        let snapshot = {
            let Ok(_guard) = engine.turn_guard(session_id).try_lock_owned() else {
                return Ok(());
            };
            engine.with_store(|store| store.snapshot_for_title(session_id))?
        };
        let Some(snapshot) = snapshot else {
            self.attempted.remove(session_id);
            return Ok(());
        };
        let Some(input) = extract::title_prompt(&snapshot) else {
            return Ok(());
        };
        if self.attempted.get(session_id) == Some(&input) {
            return Ok(());
        }
        self.attempt(engine, extractor, &snapshot, input).await
    }

    #[tracing::instrument(name = "session.title", skip_all, fields(session_id = %snapshot.session_id))]
    async fn attempt<E: Extractor>(
        &mut self,
        engine: &Engine,
        extractor: &E,
        snapshot: &SessionSnapshot,
        input: String,
    ) -> Result<(), Error> {
        let title = extractor
            .title(snapshot)
            .await
            .map_err(|source| Error::Extractor {
                session_id: snapshot.session_id.clone(),
                source,
            })?;
        let Ok(_guard) = engine.turn_guard(&snapshot.session_id).try_lock_owned() else {
            return Ok(());
        };
        let current = engine
            .with_store(|store| store.projection().latest_seq(&snapshot.session_id))
            .map_err(store::Error::from)?;
        if current != Some(snapshot.latest_seq) {
            return Ok(());
        }
        self.attempted.insert(snapshot.session_id.clone(), input);
        if let Some(title) = title.filter(|title| !title.trim().is_empty()) {
            if engine.commit_title(snapshot, &title)? {
                self.attempted.remove(&snapshot.session_id);
            }
        }
        Ok(())
    }
}

/// The gate is presence, not role. A source-less legacy session falls
/// back to the old role rule.
fn extracts_user_facts(source: i32, role: i32) -> bool {
    match Source::try_from(source) {
        Ok(Source::User) => true,
        Ok(Source::Model) => false,
        _ => matches!(
            SessionRole::try_from(role),
            Ok(SessionRole::Unspecified | SessionRole::Concierge)
        ),
    }
}

fn role_name(role: i32) -> &'static str {
    SessionRole::try_from(role).map_or("unknown", crate::provider::role_label)
}

fn source_name(source: i32) -> &'static str {
    match Source::try_from(source) {
        Ok(Source::Unspecified) => "unspecified",
        Ok(Source::Model) => "model",
        Ok(Source::User) => "user",
        Ok(Source::System) => "system",
        Err(_) => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arc_proto::v1::{
        MemoryRecord, MemoryRecordCreated, MemoryRecordSuperseded, MessageAppended, Role,
        SessionCreated, SessionEvent, SessionRole, Source, event, memory_event, memory_record,
        session_event,
    };
    use tempfile::TempDir;

    use super::{
        ExtractError, Extractor, NoopExtractor, Outcome, SessionSnapshot, Titles, run_pass,
    };
    use crate::projection::Projection;
    use crate::session::Engine;
    use crate::testkit::{
        ScriptedProvider, TraceCapture, channel, counter_samples, done_reply, engine,
        engine_with_role, engine_with_role_and_project, replay_events,
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

    struct Titling(Option<String>);

    impl Extractor for Titling {
        async fn extract(
            &self,
            _session: &SessionSnapshot,
        ) -> Result<Vec<memory_event::Event>, ExtractError> {
            Ok(Vec::new())
        }

        async fn title(&self, _session: &SessionSnapshot) -> Result<Option<String>, ExtractError> {
            Ok(self.0.clone())
        }
    }

    struct PanicsIfTitled;

    impl Extractor for PanicsIfTitled {
        async fn extract(
            &self,
            _session: &SessionSnapshot,
        ) -> Result<Vec<memory_event::Event>, ExtractError> {
            Ok(Vec::new())
        }

        async fn title(&self, _session: &SessionSnapshot) -> Result<Option<String>, ExtractError> {
            panic!("an already-titled session must not be retitled");
        }
    }

    struct CountingExtractor {
        calls: Arc<AtomicUsize>,
        records: Vec<memory_event::Event>,
    }

    impl Extractor for CountingExtractor {
        async fn extract(
            &self,
            _session: &SessionSnapshot,
        ) -> Result<Vec<memory_event::Event>, ExtractError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.records.clone())
        }

        async fn title(&self, _session: &SessionSnapshot) -> Result<Option<String>, ExtractError> {
            Ok(Some("Job transcript".to_owned()))
        }
    }

    // the only way to reproduce a session logged with an arbitrary source
    fn seed_session(engine: &Engine, session_id: &str, role: SessionRole, source: Source) {
        let ts = Some(prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        });
        engine
            .with_store_mut(|store| {
                store.append(
                    source,
                    ts,
                    event::Payload::Session(SessionEvent {
                        event: Some(session_event::Event::SessionCreated(SessionCreated {
                            session_id: session_id.to_owned(),
                            role: role as i32,
                            ..Default::default()
                        })),
                    }),
                )
            })
            .expect("seed SessionCreated");
        engine
            .with_store_mut(|store| {
                store.append(
                    source,
                    ts,
                    event::Payload::Session(SessionEvent {
                        event: Some(session_event::Event::MessageAppended(MessageAppended {
                            session_id: session_id.to_owned(),
                            role: Role::User as i32,
                            content: "hi".to_owned(),
                            turn_id: "t-1".to_owned(),
                            ..Default::default()
                        })),
                    }),
                )
            })
            .expect("seed MessageAppended");
    }

    fn titled_events(dir: &std::path::Path) -> Vec<arc_proto::v1::SessionTitled> {
        replay_events(dir)
            .into_iter()
            .filter_map(|event| match event.payload {
                Some(event::Payload::Session(session)) => match session.event {
                    Some(session_event::Event::SessionTitled(titled)) => Some(titled),
                    _ => None,
                },
                _ => None,
            })
            .collect()
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
        let (tx, _rx) = channel();
        let reply = engine
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
                through_seq: 3,
                records: 0,
                records_created: 0,
                records_superseded: 0,
            }
        );
        let events = replay_events(dir.path());
        assert_eq!(events.len(), 5, "the turn plus exactly one marker");
        let last = events.last().expect("events");
        assert_eq!(last.source, Source::System as i32, "arcd initiated this");
        assert_eq!(marker(last).session_id, reply.session_id);
        assert_eq!(marker(last).through_seq, 3);
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
        let (tx, _rx) = channel();
        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let outcome = run_pass(&engine, &NoopExtractor, 0, "", &HashSet::new())
            .await
            .expect("pass");

        assert_eq!(outcome, Outcome::NothingDue);
        assert_eq!(replay_events(dir.path()).len(), 4, "no marker appended");
    }

    #[tokio::test]
    async fn a_completed_exchange_is_titled_before_the_idle_gate() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        Titles::default()
            .run(&engine, &Titling(Some("Palette bikeshed".to_owned())))
            .await
            .expect("title");
        assert_eq!(
            run_pass(&engine, &PanicsIfTitled, 0, "", &HashSet::new())
                .await
                .expect("idle gate"),
            Outcome::NothingDue
        );

        let titled = titled_events(dir.path());
        assert_eq!(titled.len(), 1);
        assert_eq!(titled[0].session_id, reply.session_id);
        assert_eq!(titled[0].title, "Palette bikeshed");
    }

    #[tokio::test]
    async fn a_greeting_retries_only_after_new_completed_input_even_if_consolidated() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello"), done_reply("fixed")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("greeting");
        let mut titles = Titles::default();
        titles
            .run(&engine, &Titling(None))
            .await
            .expect("blank title");
        titles.run(&engine, &PanicsIfTitled).await.expect("dedup");
        run_pass(&engine, &PanicsIfTitled, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("consolidate");
        titles
            .run(&engine, &PanicsIfTitled)
            .await
            .expect("watermark is not input");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "fix title refresh", tx)
            .await
            .expect("task");
        titles
            .run(&engine, &Titling(Some("Title refresh".to_owned())))
            .await
            .expect("retry");
        assert_eq!(titled_events(dir.path())[0].title, "Title refresh");
    }

    struct AdvancesDuringTitle<'a> {
        engine: &'a Engine,
        runner: &'a crate::session::Runner,
    }

    impl Extractor for AdvancesDuringTitle<'_> {
        async fn extract(
            &self,
            _: &SessionSnapshot,
        ) -> Result<Vec<memory_event::Event>, ExtractError> {
            panic!("title generation must not extract");
        }

        async fn title(&self, snapshot: &SessionSnapshot) -> Result<Option<String>, ExtractError> {
            let (tx, _rx) = channel();
            self.engine
                .send_message(self.runner, Some(&snapshot.session_id), "new task", tx)
                .await
                .expect("turn is not blocked by title generation");
            Ok(Some("Stale".to_owned()))
        }
    }

    #[tokio::test]
    async fn a_title_racing_a_turn_is_discarded_and_retried() {
        let provider = ScriptedProvider::scripted(vec![done_reply("first"), done_reply("second")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        engine
            .send_message(&run, None, "first task", tx)
            .await
            .expect("send");
        let mut titles = Titles::default();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            titles.run(
                &engine,
                &AdvancesDuringTitle {
                    engine: &engine,
                    runner: &run,
                },
            ),
        )
        .await
        .expect("not blocking turns")
        .expect("raced");
        assert!(titled_events(dir.path()).is_empty());
        titles
            .run(&engine, &Titling(Some("Current".to_owned())))
            .await
            .expect("retry");
        assert_eq!(titled_events(dir.path())[0].title, "Current");
    }

    struct FailsFirstTitle(AtomicUsize);

    impl Extractor for FailsFirstTitle {
        async fn extract(
            &self,
            _: &SessionSnapshot,
        ) -> Result<Vec<memory_event::Event>, ExtractError> {
            panic!("title generation must not extract");
        }

        async fn title(&self, _: &SessionSnapshot) -> Result<Option<String>, ExtractError> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(ExtractError("temporary provider failure".to_owned()))
            } else {
                Ok(Some("Recovered title".to_owned()))
            }
        }
    }

    #[tokio::test]
    async fn a_failed_title_can_retry_unchanged_input() {
        let provider = ScriptedProvider::scripted(vec![done_reply("done")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "fix titles", tx)
            .await
            .expect("send");
        let extractor = FailsFirstTitle(AtomicUsize::new(0));
        let mut titles = Titles::default();
        assert!(matches!(
            titles
                .run_session(&engine, &extractor, &reply.session_id)
                .await,
            Err(super::Error::Extractor { .. })
        ));
        assert!(titled_events(dir.path()).is_empty());
        titles
            .run_session(&engine, &extractor, &reply.session_id)
            .await
            .expect("retry unchanged input");
        assert_eq!(extractor.0.load(Ordering::SeqCst), 2);
        assert_eq!(titled_events(dir.path())[0].title, "Recovered title");
    }

    #[tokio::test]
    async fn recovery_continues_after_one_title_fails() {
        let provider = ScriptedProvider::scripted(vec![done_reply("one"), done_reply("two")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        for content in ["first task", "second task"] {
            let (tx, _rx) = channel();
            engine
                .send_message(&run, None, content, tx)
                .await
                .expect("send");
        }
        let extractor = FailsFirstTitle(AtomicUsize::new(0));
        let mut titles = Titles::default();
        titles.run(&engine, &extractor).await.expect("recovery");
        assert_eq!(extractor.0.load(Ordering::SeqCst), 2);
        assert_eq!(titled_events(dir.path()).len(), 1);
        titles
            .run(&engine, &extractor)
            .await
            .expect("retry failed candidate");
        assert_eq!(extractor.0.load(Ordering::SeqCst), 3);
        assert_eq!(titled_events(dir.path()).len(), 2);
    }

    struct SimultaneousTitles(tokio::sync::Barrier);

    impl Extractor for SimultaneousTitles {
        async fn extract(
            &self,
            _: &SessionSnapshot,
        ) -> Result<Vec<memory_event::Event>, ExtractError> {
            panic!("title generation must not extract");
        }

        async fn title(&self, _: &SessionSnapshot) -> Result<Option<String>, ExtractError> {
            self.0.wait().await;
            Ok(Some("One winner".to_owned()))
        }
    }

    #[tokio::test]
    async fn simultaneous_title_generations_append_only_one_winner() {
        let provider = ScriptedProvider::scripted(vec![done_reply("done")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        engine
            .send_message(&run, None, "fix titles", tx)
            .await
            .expect("send");
        let extractor = SimultaneousTitles(tokio::sync::Barrier::new(2));
        let mut first = Titles::default();
        let mut second = Titles::default();
        let (first, second) = tokio::join!(
            first.run(&engine, &extractor),
            second.run(&engine, &extractor)
        );
        first.expect("first generation");
        second.expect("second generation");
        assert_eq!(titled_events(dir.path()).len(), 1);
    }

    #[tokio::test]
    async fn partial_and_live_exchanges_are_not_title_candidates() {
        let provider = ScriptedProvider::scripted(vec![
            vec![Ok(crate::provider::CompletionDelta::Text(
                "partial".to_owned(),
            ))],
            done_reply("done"),
        ]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let mut completions = engine.completed_turns();
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "fix titles", tx)
            .await
            .expect("partial reply");
        assert!(reply.partial);
        assert!(completions.try_recv().is_err());
        Titles::default()
            .run(&engine, &PanicsIfTitled)
            .await
            .expect("partial skipped");
        let (tx, _rx) = channel();
        engine
            .continue_session(&run, &reply.session_id, tx)
            .await
            .expect("finish");
        assert_eq!(
            completions.try_recv().expect("completion"),
            reply.session_id
        );
        let guard = engine
            .turn_guard(&reply.session_id)
            .try_lock_owned()
            .expect("completion released the guard");
        Titles::default()
            .run(&engine, &PanicsIfTitled)
            .await
            .expect("live skipped");
        drop(guard);
        Titles::default()
            .run(&engine, &Titling(Some("Finished".to_owned())))
            .await
            .expect("finished title");
        assert_eq!(titled_events(dir.path()).len(), 1);
    }

    #[tokio::test]
    async fn concurrent_title_commits_notify_once_and_replay_preserves_the_pin() {
        let provider = ScriptedProvider::scripted(vec![done_reply("done")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (notifier, mut notifications) = tokio::sync::broadcast::channel(16);
        let engine = engine.with_notifier(notifier);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "fix titles", tx)
            .await
            .expect("send");
        while notifications.try_recv().is_ok() {}
        let before = engine.sessions().expect("sessions");
        let snapshot = engine
            .with_store(|store| store.snapshot_for_title(&reply.session_id))
            .expect("snapshot")
            .expect("eligible");
        let (first, second) =
            tokio::join!(async { engine.commit_title(&snapshot, "First") }, async {
                engine.commit_title(&snapshot, "Second")
            });
        assert!(first.expect("first"));
        assert!(!second.expect("second"));
        assert!(
            matches!(notifications.try_recv().expect("notification").event,
            Some(arc_proto::v1::notification::Event::SessionAppended(appended)) if appended.session_id == reply.session_id)
        );
        assert!(notifications.try_recv().is_err());
        let mut rebuilt = Projection::in_memory().expect("projection");
        for event in replay_events(dir.path()) {
            rebuilt.apply(&event).expect("apply");
        }
        let after = rebuilt.sessions().expect("sessions");
        assert_eq!(after[0].title, "First");
        assert_eq!(after[0].provider, before[0].provider);
        assert_eq!(after[0].model, before[0].model);
        assert_eq!(after[0].role, before[0].role);
    }

    #[tokio::test]
    async fn an_already_titled_session_is_not_retitled() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello"), done_reply("again")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        Titles::default()
            .run(&engine, &Titling(Some("First".to_owned())))
            .await
            .expect("first title");

        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "more", tx)
            .await
            .expect("send more");

        Titles::default()
            .run(&engine, &PanicsIfTitled)
            .await
            .expect("retained");

        let titled = titled_events(dir.path());
        assert_eq!(
            titled.iter().map(|t| t.title.as_str()).collect::<Vec<_>>(),
            ["First"],
            "one title event, never overwritten"
        );
    }

    #[tokio::test]
    async fn activity_during_the_pass_discards_it_whole() {
        let provider = ScriptedProvider::scripted(vec![done_reply("first"), done_reply("second")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let snapshot = engine
            .with_store(|store| store.snapshot_for_consolidation(ALL_IDLE, &HashSet::new()))
            .expect("snapshot")
            .expect("the session is due");
        assert_eq!(snapshot.session_id, reply.session_id);
        assert_eq!(snapshot.latest_seq, 3);
        assert_eq!(snapshot.rows.len(), 2, "both prose rows, for 7.2");

        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&reply.session_id), "more", tx)
            .await
            .expect("send");

        let titled = engine
            .with_store_mut(|store| store.commit_title(&snapshot, "a stale title"))
            .expect("commit_title");
        assert!(!titled, "the stale snapshot must not title either");

        let committed = engine
            .with_store_mut(|store| {
                store.commit_consolidation(&snapshot, vec![created_record("mr-x")], "")
            })
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
            assert!(
                !matches!(session.event, Some(session_event::Event::SessionTitled(_))),
                "a title leaked from the discarded pass"
            );
        }
        let due = engine
            .with_store(|store| store.due_for_consolidation(ALL_IDLE))
            .expect("due");
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].latest_seq, 6, "coverage will span the new turn");
    }

    #[tokio::test]
    async fn extracted_records_land_as_system_before_the_marker() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        let (tx, _rx) = channel();
        let reply = engine
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
                through_seq: 3,
                records: 1,
                records_created: 1,
                records_superseded: 0,
            }
        );
        let events = replay_events(dir.path());
        assert_eq!(events.len(), 6);
        let record_event = &events[4];
        assert_eq!(record_event.source, Source::System as i32);
        assert!(
            matches!(record_event.payload, Some(event::Payload::Memory(_))),
            "the record precedes the marker"
        );
        assert_eq!(marker(&events[5]).through_seq, 3);

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
        let (tx, _rx) = channel();
        engine
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
        let (tx, _rx) = channel();
        engine
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
        let (tx, _rx) = channel();
        let reply = engine
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
        assert_eq!(replay_events(dir.path()).len(), 4, "log untouched");
    }

    #[tokio::test]
    async fn a_skipped_session_yields_to_the_next_due() {
        let provider = ScriptedProvider::scripted(vec![done_reply("one"), done_reply("two")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine(&provider, &dir);
        for text in ["hi", "yo"] {
            let (tx, _rx) = channel();
            engine
                .send_message(&run, None, text, tx)
                .await
                .expect("send");
        }
        let due = engine
            .with_store(|store| store.due_for_consolidation(ALL_IDLE))
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

    #[tokio::test]
    async fn a_dispatched_executor_session_is_titled_but_never_extracted() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine_with_role_and_project(&provider, &dir, SessionRole::Executor);
        // source Model, as dispatch_job records it
        let session_id = engine
            .create_bound_session(&run, "arc", SessionRole::Executor, None)
            .expect("create bound session");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&session_id), "hi", tx)
            .await
            .expect("send");

        let calls = Arc::new(AtomicUsize::new(0));
        let extractor = CountingExtractor {
            calls: Arc::clone(&calls),
            records: Vec::new(),
        };
        Titles::default()
            .run(&engine, &extractor)
            .await
            .expect("title");
        let outcome = run_pass(&engine, &extractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("pass");

        assert_eq!(
            outcome,
            Outcome::Consolidated {
                session_id: session_id.clone(),
                through_seq: 3,
                records: 0,
                records_created: 0,
                records_superseded: 0,
            }
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "a dispatched executor session must never reach the extractor"
        );

        let titled = titled_events(dir.path());
        assert_eq!(
            titled.len(),
            1,
            "an eligible dispatched session still gets a title"
        );
        assert_eq!(titled[0].session_id, session_id);
    }

    #[tokio::test]
    async fn a_direct_executor_session_extracts() {
        direct_session_extracts(SessionRole::Executor).await;
    }

    #[tokio::test]
    async fn a_direct_code_session_extracts() {
        direct_session_extracts(SessionRole::Code).await;
    }

    async fn direct_session_extracts(role: SessionRole) {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine_with_role_and_project(&provider, &dir, role);
        let session_id = engine
            .create_direct_session(&run, "arc", role)
            .expect("create direct session");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&session_id), "hi", tx)
            .await
            .expect("send");

        let calls = Arc::new(AtomicUsize::new(0));
        let extractor = CountingExtractor {
            calls: Arc::clone(&calls),
            records: vec![created_record("mr-code")],
        };
        let outcome = run_pass(&engine, &extractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("pass");

        assert!(
            matches!(outcome, Outcome::Consolidated { records: 1, .. }),
            "got: {outcome:?}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a :code session the user opened must reach the extractor"
        );
    }

    #[tokio::test]
    async fn an_executor_session_with_unspecified_source_does_not_extract() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, _run) = engine_with_role(&provider, &dir, SessionRole::Executor);
        seed_session(
            &engine,
            "s-legacy",
            SessionRole::Executor,
            Source::Unspecified,
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let extractor = CountingExtractor {
            calls: Arc::clone(&calls),
            records: Vec::new(),
        };
        let outcome = run_pass(&engine, &extractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("pass");

        assert!(
            matches!(outcome, Outcome::Consolidated { records: 0, .. }),
            "got: {outcome:?}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "an unspecified-source executor session keeps the old role rule"
        );
    }

    #[tokio::test]
    async fn a_concierge_session_still_extracts() {
        let provider = ScriptedProvider::scripted(vec![done_reply("hello")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine_with_role(&provider, &dir, SessionRole::Concierge);
        let (tx, _rx) = channel();
        engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");

        let calls = Arc::new(AtomicUsize::new(0));
        let extractor = CountingExtractor {
            calls: Arc::clone(&calls),
            records: vec![created_record("mr-x")],
        };
        let outcome = run_pass(&engine, &extractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("pass");

        assert!(
            matches!(outcome, Outcome::Consolidated { records: 1, .. }),
            "got: {outcome:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_real_branch_extracts_its_own_rows_and_a_real_executor_branch_never_extracts() {
        use arc_proto::v1::branch_marked::Disposition;
        // concierge: a REAL branch is due and mines like any main line
        let provider = ScriptedProvider::scripted(vec![done_reply("one"), done_reply("two")]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, run) = engine_with_role(&provider, &dir, SessionRole::Concierge);
        let (tx, _rx) = channel();
        let first = engine
            .send_message(&run, None, "hi", tx)
            .await
            .expect("send");
        let branch = engine
            .fork_session(&first.session_id, first.seq)
            .expect("fork");
        let (tx, _rx) = channel();
        engine
            .send_message(&run, Some(&branch), "branch turn", tx)
            .await
            .expect("send on branch");
        engine
            .mark_branch(&branch, Disposition::Real)
            .expect("mark real");

        let calls = Arc::new(AtomicUsize::new(0));
        let extractor = CountingExtractor {
            calls: Arc::clone(&calls),
            records: vec![created_record("mr-branch")],
        };
        // both the root and the REAL branch are due; the scratch default
        // would have kept the branch out (pinned by the projection tests)
        let outcome = run_pass(&engine, &extractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("pass");
        assert!(
            matches!(outcome, Outcome::Consolidated { .. }),
            "got: {outcome:?}"
        );
        let extractor_two = CountingExtractor {
            calls: Arc::clone(&calls),
            records: vec![created_record("mr-branch-2")],
        };
        let second = run_pass(&engine, &extractor_two, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("pass");
        assert!(
            matches!(second, Outcome::Consolidated { .. }),
            "got: {second:?}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "root and REAL branch both mined, each once"
        );
    }

    #[tokio::test]
    async fn a_role_less_legacy_session_still_extracts() {
        let provider = ScriptedProvider::scripted(vec![]);
        let dir = TempDir::new().expect("temp dir");
        let (engine, _run) = engine_with_role(&provider, &dir, SessionRole::Unspecified);
        seed_session(
            &engine,
            "s-legacy",
            SessionRole::Unspecified,
            Source::Unspecified,
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let extractor = CountingExtractor {
            calls: Arc::clone(&calls),
            records: vec![created_record("mr-x")],
        };
        let outcome = run_pass(&engine, &extractor, ALL_IDLE, "", &HashSet::new())
            .await
            .expect("pass");

        assert!(
            matches!(outcome, Outcome::Consolidated { records: 1, .. }),
            "got: {outcome:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
