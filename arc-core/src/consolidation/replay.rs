//! `arcd memory-replay`'s core (DESIGN.md §5.4, tuning loop): re-run a
//! prompt version over the historical sessions in the log, against a
//! scratch in-memory projection, and diff the resulting memory states.
//!
//! Replay never touches durable state. The log is read once, read-only —
//! the reader tolerates a torn tail, so a running daemon is fine — and
//! extraction results land only in the scratch projection. Semantics mirror
//! the live pass, evolving state included: sessions run in first-event
//! order, each snapshot is rows plus the current ACTIVE index exactly as
//! `snapshot_for_consolidation` builds it, and a session's results apply
//! before the next session runs — session N+1 sees what N wrote, because
//! that accumulation is the merge behavior under test. The log's own
//! consolidation output (SYSTEM-sourced memory events) is excluded from the
//! scratch: replay replaces the live pass's writes with its own.
//!
//! Each session's extraction runs with the same session-derived seed under
//! every version, so differences in the diff are attributable to the
//! prompt. Minted ids differ between runs by construction; [`diff`] keys on
//! content, never ids.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arc_proto::v1::{Event, MemoryEvent, Source, event, memory_event, session_event};

use super::extract::ModelExtractor;
use super::{ExtractError, Extractor as _, SessionSnapshot};
use crate::log::{LogReader, discover_segments};
use crate::memory::kind_name;
use crate::projection::Projection;
use crate::provider::Provider;

/// Everything one replay can fail with. Extraction failures fail the run
/// whole: a report over a partially-extracted history would diff noise.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// The log could not be read.
    #[error("reading the log: {0}")]
    Log(#[from] crate::log::Error),

    /// The scratch projection refused an event or a query.
    #[error("scratch projection: {0}")]
    Projection(#[from] crate::projection::Error),

    /// One session's extraction failed under one version.
    #[error("extraction for {session_id} under {version}: {source}")]
    Extraction {
        /// The version whose run failed.
        version: String,
        /// The session whose extraction failed.
        session_id: String,
        /// What went wrong.
        source: ExtractError,
    },
}

/// One version's run: every processed session with its operations, and the
/// ACTIVE memory state the run ended on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    /// The prompt version that ran.
    pub version: String,
    /// Processed sessions in run order, zero-operation ones included.
    pub sessions: Vec<SessionReplay>,
    /// The ACTIVE index after the last session, in the index's own order.
    pub final_state: Vec<ReplayRecord>,
}

/// What one session's extraction did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionReplay {
    /// The session replayed.
    pub session_id: String,
    /// Its operations, in extraction order.
    pub operations: Vec<ReplayOperation>,
}

/// One extraction operation, rendered down to what a report shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayOperation {
    /// "write" or "supersede".
    pub op: &'static str,
    /// Lowercase kind name.
    pub kind: String,
    /// Record title.
    pub title: String,
    /// Record summary.
    pub summary: String,
}

/// One record of a run's final state — content only, no ids: ids are minted
/// fresh every run and carry no meaning across them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayRecord {
    /// Lowercase kind name.
    pub kind: String,
    /// "global" or a project id.
    pub namespace: String,
    /// Record title.
    pub title: String,
    /// The one-line summary.
    pub summary: String,
}

/// How two runs' final states differ, keyed on content — (kind,
/// case-insensitive title) — never on ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayDiff {
    /// Records A ended with and B did not.
    pub only_in_a: Vec<ReplayRecord>,
    /// Records B ended with and A did not.
    pub only_in_b: Vec<ReplayRecord>,
    /// Same-(kind, title) pairs whose summaries differ.
    pub changed: Vec<ChangedSummary>,
}

/// One record both runs kept whose summary drifted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedSummary {
    /// Lowercase kind name.
    pub kind: String,
    /// The title, as run A spelled it.
    pub title: String,
    /// Run A's summary.
    pub summary_a: String,
    /// Run B's summary.
    pub summary_b: String,
}

pub use super::extract::session_seed;

/// Runs every version in `versions` — `(version, prompt)` pairs — over the
/// log at `log_dir`, one full pass each. `session_filter` empty means all
/// sessions; sessions with no message rows are skipped silently.
///
/// # Errors
///
/// Any [`ReplayError`]: an unreadable log, a scratch projection failure, or
/// a failed extraction (which fails the run whole).
pub async fn run<P: Provider>(
    provider: &Arc<P>,
    model: &str,
    timeout: Duration,
    log_dir: &Path,
    versions: &[(&str, &str)],
    session_filter: &[String],
) -> Result<Vec<ReplayReport>, ReplayError> {
    // One read serves every version: the log is immutable history.
    let mut events = Vec::new();
    for item in LogReader::new(discover_segments(log_dir)?) {
        events.push(item?);
    }
    let mut reports = Vec::with_capacity(versions.len());
    for (version, prompt) in versions {
        reports.push(
            run_version(
                provider,
                model,
                timeout,
                &events,
                version,
                prompt,
                session_filter,
            )
            .await?,
        );
    }
    Ok(reports)
}

/// One version's full pass over the scratch projection.
#[tracing::instrument(
    name = "memory.replay",
    skip_all,
    fields(
        version = version,
        sessions = tracing::field::Empty,
        records = tracing::field::Empty,
    )
)]
async fn run_version<P: Provider>(
    provider: &Arc<P>,
    model: &str,
    timeout: Duration,
    events: &[Event],
    version: &str,
    prompt: &str,
    session_filter: &[String],
) -> Result<ReplayReport, ReplayError> {
    let mut projection = Projection::open(":memory:")?;
    let mut session_order = Vec::new();
    let mut next_seq = 0_u64;
    for event in events {
        next_seq = next_seq.max(event.seq + 1);
        // The live pass's own output is exactly what this run replaces.
        if event.source == Source::System as i32
            && matches!(event.payload, Some(event::Payload::Memory(_)))
        {
            continue;
        }
        if let Some(event::Payload::Session(session)) = &event.payload {
            if let Some(session_event::Event::SessionCreated(created)) = &session.event {
                session_order.push(created.session_id.clone());
            }
        }
        projection.apply(event)?;
    }
    if !session_filter.is_empty() {
        session_order.retain(|id| session_filter.contains(id));
    }

    let mut sessions = Vec::new();
    let mut records = 0_usize;
    for session_id in session_order {
        let rows = projection.messages(&session_id)?;
        let latest_seq = projection.latest_seq(&session_id)?;
        let (Some(latest_seq), false) = (latest_seq, rows.is_empty()) else {
            continue;
        };
        let snapshot = SessionSnapshot {
            session_id: session_id.clone(),
            rows,
            latest_seq,
            memory_index: projection.memory_index()?,
        };
        let extractor = ModelExtractor::new(Arc::clone(provider), model, timeout)
            .with_prompt(prompt)
            .with_seed(session_seed(&session_id));
        let extracted =
            extractor
                .extract(&snapshot)
                .await
                .map_err(|source| ReplayError::Extraction {
                    version: version.to_owned(),
                    session_id: session_id.clone(),
                    source,
                })?;
        let mut operations = Vec::with_capacity(extracted.len());
        for memory in extracted {
            operations.push(operation(&memory));
            projection.apply(&Event {
                seq: next_seq,
                ts: None,
                source: Source::System as i32,
                payload: Some(event::Payload::Memory(MemoryEvent {
                    event: Some(memory),
                })),
            })?;
            next_seq += 1;
        }
        records += operations.len();
        sessions.push(SessionReplay {
            session_id,
            operations,
        });
    }

    let final_state = projection
        .memory_index()?
        .into_iter()
        .map(|entry| ReplayRecord {
            kind: kind_name(entry.kind),
            namespace: entry.namespace,
            title: entry.title,
            summary: entry.summary,
        })
        .collect();
    let span = tracing::Span::current();
    span.record("sessions", sessions.len());
    span.record("records", records);
    Ok(ReplayReport {
        version: version.to_owned(),
        sessions,
        final_state,
    })
}

/// What a report shows of one extraction event.
fn operation(event: &memory_event::Event) -> ReplayOperation {
    // The extractor mints only these two arms; the others stay total for
    // safety, not because a run can produce them.
    let (op, record) = match event {
        memory_event::Event::RecordCreated(created) => ("write", created.record.as_ref()),
        memory_event::Event::RecordSuperseded(superseded) => {
            ("supersede", superseded.record.as_ref())
        }
        memory_event::Event::RecordUpdated(updated) => ("update", updated.record.as_ref()),
        memory_event::Event::RecordDeleted(_) => ("delete", None),
        memory_event::Event::RecordReviewed(_) => ("review", None),
    };
    record.map_or(
        ReplayOperation {
            op,
            kind: String::new(),
            title: String::new(),
            summary: String::new(),
        },
        |record| ReplayOperation {
            op,
            kind: kind_name(record.kind),
            title: record.title.clone(),
            summary: record.summary.clone(),
        },
    )
}

/// Diffs two runs' final states by content: (kind, case-insensitive title)
/// is the key, matched pairwise in state order.
#[must_use]
pub fn diff(a: &ReplayReport, b: &ReplayReport) -> ReplayDiff {
    let mut remaining_a: Vec<&ReplayRecord> = a.final_state.iter().collect();
    let mut only_in_b = Vec::new();
    let mut changed = Vec::new();
    for record_b in &b.final_state {
        let matched = remaining_a.iter().position(|record_a| {
            record_a.kind == record_b.kind
                && record_a.title.to_lowercase() == record_b.title.to_lowercase()
        });
        match matched {
            Some(index) => {
                let record_a = remaining_a.remove(index);
                if record_a.summary != record_b.summary {
                    changed.push(ChangedSummary {
                        kind: record_a.kind.clone(),
                        title: record_a.title.clone(),
                        summary_a: record_a.summary.clone(),
                        summary_b: record_b.summary.clone(),
                    });
                }
            }
            None => only_in_b.push(record_b.clone()),
        }
    }
    ReplayDiff {
        only_in_a: remaining_a.into_iter().cloned().collect(),
        only_in_b,
        changed,
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use arc_proto::v1::{
        MemoryEvent, MemoryRecord, MemoryRecordCreated, MessageAppended, Role, SessionCreated,
        SessionEvent, event, memory_event, memory_record, session_event,
    };
    use tempfile::TempDir;

    use super::{ReplayOperation, ReplayRecord, ReplayReport, diff, run, session_seed};
    use crate::provider::{CompletionDelta, Error as ProviderError, Message, Stop};
    use crate::testkit::{ScriptedProvider, seed_log_payloads, usage};

    fn extraction_reply(json: &str) -> Vec<Result<CompletionDelta, ProviderError>> {
        vec![
            Ok(CompletionDelta::Text(json.to_owned())),
            Ok(CompletionDelta::Done {
                usage: usage(),
                stop: Stop::EndTurn,
            }),
        ]
    }

    fn created(session_id: &str) -> event::Payload {
        event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::SessionCreated(SessionCreated {
                session_id: session_id.to_owned(),
                title: String::new(),
                provider: "scripted".to_owned(),
                model: "test-model".to_owned(),
            })),
        })
    }

    fn message(session_id: &str, content: &str) -> event::Payload {
        event::Payload::Session(SessionEvent {
            event: Some(session_event::Event::MessageAppended(MessageAppended {
                session_id: session_id.to_owned(),
                role: Role::User as i32,
                content: content.to_owned(),
                partial: false,
                turn_id: format!("{session_id}-t1"),
            })),
        })
    }

    /// A record as the live pass would have written it: seeded with
    /// `Source::System`, so replay must exclude it.
    fn live_record() -> event::Payload {
        event::Payload::Memory(MemoryEvent {
            event: Some(memory_event::Event::RecordCreated(MemoryRecordCreated {
                record: Some(MemoryRecord {
                    id: "mr-live".to_owned(),
                    kind: memory_record::Kind::Fact as i32,
                    namespace: "global".to_owned(),
                    title: "Stale live record".to_owned(),
                    summary: "written by the live pass".to_owned(),
                    body: "x".to_owned(),
                    links: Vec::new(),
                    provenance: None,
                    status: memory_record::Status::Active as i32,
                }),
            })),
        })
    }

    /// Two sessions with prose, one empty session, and one live-pass record.
    fn seeded(dir: &TempDir) {
        seed_log_payloads(
            dir,
            vec![
                created("s-1"),
                message("s-1", "My name is Bogdan"),
                live_record(),
                created("s-2"),
                message("s-2", "Tell me a big story"),
                created("s-3"),
            ],
        );
    }

    /// Every file in `dir`, name and bytes, sorted — the whole log verbatim.
    fn log_bytes(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
            .expect("read dir")
            .map(|entry| entry.expect("entry").path())
            .collect();
        files.sort();
        files
            .into_iter()
            .map(|path| {
                let bytes = std::fs::read(&path).expect("read segment");
                (path, bytes)
            })
            .collect()
    }

    const WRITE_NAME_A: &str = r#"{"operations":[{"op":"write","kind":"fact",
        "title":"User name","summary":"named Bogdan",
        "body":"The user is named Bogdan.","links":[]}]}"#;
    const WRITE_NAME_B: &str = r#"{"operations":[{"op":"write","kind":"fact",
        "title":"User name","summary":"goes by Bogdan",
        "body":"The user goes by Bogdan.","links":[]}]}"#;
    const WRITE_STORY_B: &str = r#"{"operations":[{"op":"write","kind":"preference",
        "title":"Storytelling","summary":"likes big stories",
        "body":"The user likes big stories.","links":[]}]}"#;
    const EMPTY: &str = r#"{"operations":[]}"#;

    /// The whole loop, two versions: evolving state, live-output exclusion,
    /// per-session seeds identical across versions, a content-keyed diff,
    /// and a log whose bytes replay never touches.
    #[tokio::test]
    async fn two_versions_replay_with_evolving_state_and_a_content_diff() {
        let dir = TempDir::new().expect("temp dir");
        seeded(&dir);
        let before = log_bytes(dir.path());

        // Call order: version A over s-1 then s-2, version B the same.
        let provider = ScriptedProvider::scripted(vec![
            extraction_reply(WRITE_NAME_A),
            extraction_reply(EMPTY),
            extraction_reply(WRITE_NAME_B),
            extraction_reply(WRITE_STORY_B),
        ]);
        let reports = run(
            &provider,
            "test-model",
            Duration::from_secs(5),
            dir.path(),
            &[("va", "PROMPT A"), ("vb", "PROMPT B")],
            &[],
        )
        .await
        .expect("replay");

        // Version A's report: two processed sessions (s-3 has no rows), one
        // operation, and a final state of exactly the replay's own record.
        let a = &reports[0];
        assert_eq!(a.version, "va");
        assert_eq!(
            a.sessions.iter().map(|s| &s.session_id).collect::<Vec<_>>(),
            ["s-1", "s-2"],
            "s-3 is skipped silently"
        );
        assert_eq!(
            a.sessions[0].operations,
            [ReplayOperation {
                op: "write",
                kind: "fact".to_owned(),
                title: "User name".to_owned(),
                summary: "named Bogdan".to_owned(),
            }]
        );
        assert!(a.sessions[1].operations.is_empty());
        assert_eq!(
            a.final_state,
            [ReplayRecord {
                kind: "fact".to_owned(),
                namespace: "global".to_owned(),
                title: "User name".to_owned(),
                summary: "named Bogdan".to_owned(),
            }]
        );

        // The captured requests: each version's own prompt, and the evolving
        // index — session 2 sees what session 1 wrote, never the live
        // pass's record.
        let requests = provider.requests();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0].system.as_deref(), Some("PROMPT A"));
        assert_eq!(requests[2].system.as_deref(), Some("PROMPT B"));
        let content = |index: usize| {
            let [Message::Text { content, .. }] = requests[index].messages.as_slice() else {
                panic!("expected one user message, got {:?}", requests[index]);
            };
            content
        };
        assert!(content(0).contains("(none yet)"), "{}", content(0));
        assert!(
            content(1).contains("User name \u{2014} named Bogdan"),
            "session 2 must see session 1's record: {}",
            content(1)
        );
        for index in 0..4 {
            assert!(
                !content(index).contains("Stale live record"),
                "the live pass's output must be excluded: {}",
                content(index)
            );
        }

        // Seeds: derived from the session id, identical across versions.
        assert_eq!(requests[0].seed, Some(session_seed("s-1")));
        assert_eq!(requests[1].seed, Some(session_seed("s-2")));
        assert_eq!(requests[2].seed, requests[0].seed);
        assert_eq!(requests[3].seed, requests[1].seed);
        assert_ne!(requests[0].seed, requests[1].seed);

        // The diff: a changed summary and an only-in-B record, by content.
        let diffed = diff(&reports[0], &reports[1]);
        assert!(diffed.only_in_a.is_empty());
        assert_eq!(
            diffed.only_in_b,
            [ReplayRecord {
                kind: "preference".to_owned(),
                namespace: "global".to_owned(),
                title: "Storytelling".to_owned(),
                summary: "likes big stories".to_owned(),
            }]
        );
        assert_eq!(diffed.changed.len(), 1);
        assert_eq!(diffed.changed[0].title, "User name");
        assert_eq!(diffed.changed[0].summary_a, "named Bogdan");
        assert_eq!(diffed.changed[0].summary_b, "goes by Bogdan");

        assert_eq!(log_bytes(dir.path()), before, "the log must be untouched");
    }

    #[tokio::test]
    async fn the_session_filter_limits_the_run() {
        let dir = TempDir::new().expect("temp dir");
        seeded(&dir);
        let provider = ScriptedProvider::scripted(vec![extraction_reply(EMPTY)]);

        let reports = run(
            &provider,
            "test-model",
            Duration::from_secs(5),
            dir.path(),
            &[("va", "PROMPT A")],
            &["s-2".to_owned()],
        )
        .await
        .expect("replay");

        assert_eq!(reports[0].sessions.len(), 1);
        assert_eq!(reports[0].sessions[0].session_id, "s-2");
        assert_eq!(provider.requests().len(), 1, "one session, one extraction");
    }

    fn state(records: Vec<ReplayRecord>) -> ReplayReport {
        ReplayReport {
            version: "x".to_owned(),
            sessions: Vec::new(),
            final_state: records,
        }
    }

    fn record(kind: &str, title: &str, summary: &str) -> ReplayRecord {
        ReplayRecord {
            kind: kind.to_owned(),
            namespace: "global".to_owned(),
            title: title.to_owned(),
            summary: summary.to_owned(),
        }
    }

    /// The diff key: kind plus case-insensitive title. A same-title record
    /// of another kind is a different record, not a changed one.
    #[test]
    fn the_diff_keys_on_kind_and_case_insensitive_title() {
        let a = state(vec![
            record("fact", "User Name", "named Bogdan"),
            record("fact", "Editor", "uses vim"),
        ]);
        let b = state(vec![
            record("fact", "user name", "goes by Bogdan"),
            record("preference", "Editor", "uses vim"),
        ]);

        let diffed = diff(&a, &b);

        assert_eq!(diffed.only_in_a, [record("fact", "Editor", "uses vim")]);
        assert_eq!(
            diffed.only_in_b,
            [record("preference", "Editor", "uses vim")]
        );
        assert_eq!(diffed.changed.len(), 1);
        assert_eq!(diffed.changed[0].title, "User Name", "A's spelling");
        assert_eq!(diffed.changed[0].summary_b, "goes by Bogdan");
    }

    #[test]
    fn the_session_seed_is_stable() {
        // Pinned: a moved seed silently unpins every recorded replay.
        assert_eq!(session_seed("s-1"), 0x817c_da19_5c3f_bf24);
        assert_ne!(session_seed("s-1"), session_seed("s-2"));
    }
}
