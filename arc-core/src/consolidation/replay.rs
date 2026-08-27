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

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("reading the log: {0}")]
    Log(#[from] crate::log::Error),

    #[error("scratch projection: {0}")]
    Projection(#[from] crate::projection::Error),

    #[error("extraction for {session_id} under {version}: {source}")]
    Extraction {
        version: String,
        session_id: String,
        source: ExtractError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    pub version: String,
    pub sessions: Vec<SessionReplay>,
    pub final_state: Vec<ReplayRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionReplay {
    pub session_id: String,
    pub operations: Vec<ReplayOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayOperation {
    pub op: &'static str,
    pub kind: String,
    pub title: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayRecord {
    pub kind: String,
    pub namespace: String,
    pub title: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayDiff {
    pub only_in_a: Vec<ReplayRecord>,
    pub only_in_b: Vec<ReplayRecord>,
    pub changed: Vec<ChangedSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedSummary {
    pub kind: String,
    pub title: String,
    pub summary_a: String,
    pub summary_b: String,
}

use super::extract::session_seed;

pub async fn run(
    provider: &Arc<dyn Provider>,
    model: &str,
    timeout: Duration,
    log_dir: &Path,
    versions: &[(&str, &str)],
    session_filter: &[String],
) -> Result<Vec<ReplayReport>, ReplayError> {
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

#[tracing::instrument(
    name = "memory.replay",
    skip_all,
    fields(
        version = version,
        sessions = tracing::field::Empty,
        records = tracing::field::Empty,
    )
)]
async fn run_version(
    provider: &Arc<dyn Provider>,
    model: &str,
    timeout: Duration,
    events: &[Event],
    version: &str,
    prompt: &str,
    session_filter: &[String],
) -> Result<ReplayReport, ReplayError> {
    let mut projection = Projection::in_memory()?;
    let mut session_order = Vec::new();
    let mut next_seq = 0_u64;
    for event in events {
        next_seq = next_seq.max(event.seq + 1);
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
        let extractor = ModelExtractor::pinned(
            Arc::clone(provider),
            model,
            timeout,
            prompt,
            session_seed(&session_id),
        );
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

fn operation(event: &memory_event::Event) -> ReplayOperation {
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
    use std::sync::Arc;
    use std::time::Duration;

    use arc_proto::v1::{
        MemoryEvent, MemoryRecord, MemoryRecordCreated, MessageAppended, Role, SessionCreated,
        SessionEvent, SessionRole, event, memory_event, memory_record, session_event,
    };
    use tempfile::TempDir;

    use super::{ReplayOperation, ReplayRecord, ReplayReport, diff, run, session_seed};
    use crate::provider::{CompletionDelta, Error as ProviderError, Message, Provider, Stop};
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
                role: SessionRole::Unspecified as i32,
                project: String::new(),
                budget: None,
                grants: Vec::new(),
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
                ..Default::default()
            })),
        })
    }

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

    #[tokio::test]
    async fn two_versions_replay_with_evolving_state_and_a_content_diff() {
        let dir = TempDir::new().expect("temp dir");
        seeded(&dir);
        let before = log_bytes(dir.path());

        let provider = ScriptedProvider::scripted(vec![
            extraction_reply(WRITE_NAME_A),
            extraction_reply(EMPTY),
            extraction_reply(WRITE_NAME_B),
            extraction_reply(WRITE_STORY_B),
        ]);
        let reports = run(
            &(Arc::clone(&provider) as Arc<dyn Provider>),
            "test-model",
            Duration::from_secs(5),
            dir.path(),
            &[("va", "PROMPT A"), ("vb", "PROMPT B")],
            &[],
        )
        .await
        .expect("replay");

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

        assert_eq!(requests[0].seed, Some(session_seed("s-1")));
        assert_eq!(requests[1].seed, Some(session_seed("s-2")));
        assert_eq!(requests[2].seed, requests[0].seed);
        assert_eq!(requests[3].seed, requests[1].seed);
        assert_ne!(requests[0].seed, requests[1].seed);

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
            &(Arc::clone(&provider) as Arc<dyn Provider>),
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
        assert_eq!(session_seed("s-1"), 0x817c_da19_5c3f_bf24);
        assert_ne!(session_seed("s-1"), session_seed("s-2"));
    }
}
