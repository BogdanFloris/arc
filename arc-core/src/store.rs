use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use arc_proto::v1::{
    Event, MemoryEvent, MemoryRecordDeleted, MemoryRecordReviewed, SessionConsolidated,
    SessionEvent, SessionTitled, Source, event, memory_event, session_event,
};
use prost_types::Timestamp;
use tracing::info;

use crate::log::{self, Log, LogReader};
use crate::projection::{self, MemoryIndexEntry, MessageRow, Projection};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("store log: {0}")]
    Log(#[from] log::Error),

    #[error("store projection: {0}")]
    Projection(#[from] projection::Error),

    #[error("no memory record {id} to review")]
    UnknownRecord { id: String },
}

// the only writer: an append that does not project is a torn invariant, not a fast path
#[derive(Debug)]
pub struct Store {
    log: Log,
    projection: Projection,
}

pub struct SessionSnapshot {
    pub session_id: String,
    pub rows: Vec<MessageRow>,
    pub latest_seq: u64,
    pub memory_index: Vec<MemoryIndexEntry>,
}

impl Store {
    pub fn new(log: Log, projection: Projection) -> Self {
        Self { log, projection }
    }

    pub fn append(
        &mut self,
        source: Source,
        ts: Option<Timestamp>,
        payload: event::Payload,
    ) -> Result<u64, Error> {
        let mut event = Event {
            seq: 0,
            ts,
            source: source as i32,
            payload: Some(payload),
        };
        let seq = self.log.append(event.clone())?;
        event.seq = seq;
        self.projection.apply(&event)?;
        Ok(seq)
    }

    pub fn projection(&self) -> &Projection {
        &self.projection
    }

    pub fn next_seq(&self) -> u64 {
        self.log.next_seq()
    }

    pub fn reader(&self) -> Result<LogReader, log::Error> {
        self.log.reader()
    }

    pub(crate) fn due_for_consolidation(
        &self,
        idle_cutoff_micros: i64,
    ) -> Result<Vec<projection::DueSession>, Error> {
        Ok(self.projection.due_for_consolidation(idle_cutoff_micros)?)
    }

    pub(crate) fn session_title(&self, session_id: &str) -> Result<Option<String>, Error> {
        Ok(self.projection.session_title(session_id)?)
    }

    pub(crate) fn snapshot_for_consolidation(
        &self,
        idle_cutoff_micros: i64,
        skip: &HashSet<String>,
    ) -> Result<Option<SessionSnapshot>, Error> {
        let Some(first) = self
            .due_for_consolidation(idle_cutoff_micros)?
            .into_iter()
            .find(|due| !skip.contains(&due.session_id))
        else {
            return Ok(None);
        };
        let rows = self.projection.messages(&first.session_id)?;
        let memory_index = self.projection.memory_index()?;
        Ok(Some(SessionSnapshot {
            session_id: first.session_id,
            rows,
            latest_seq: first.latest_seq,
            memory_index,
        }))
    }

    pub(crate) fn commit_consolidation(
        &mut self,
        snapshot: &SessionSnapshot,
        events: Vec<memory_event::Event>,
        prompt_version: &str,
    ) -> Result<bool, Error> {
        let latest = self.projection.latest_seq(&snapshot.session_id)?;
        if latest != Some(snapshot.latest_seq) {
            info!(
                session_id = %snapshot.session_id,
                snapshot_seq = snapshot.latest_seq,
                latest_seq = latest,
                "session grew during consolidation; discarding the pass"
            );
            return Ok(false);
        }
        for event in events {
            self.append_memory(Source::System, event)?;
        }
        self.append(
            Source::System,
            Some(now_ts()),
            event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::SessionConsolidated(
                    SessionConsolidated {
                        session_id: snapshot.session_id.clone(),
                        through_seq: snapshot.latest_seq,
                        prompt_version: prompt_version.to_owned(),
                    },
                )),
            }),
        )?;
        Ok(true)
    }

    /// Its own idle re-check, separate from `commit_consolidation`: titling
    /// runs before extraction, in a lock scope of its own.
    pub(crate) fn commit_title(
        &mut self,
        snapshot: &SessionSnapshot,
        title: &str,
    ) -> Result<bool, Error> {
        let latest = self.projection.latest_seq(&snapshot.session_id)?;
        if latest != Some(snapshot.latest_seq) {
            info!(
                session_id = %snapshot.session_id,
                snapshot_seq = snapshot.latest_seq,
                latest_seq = latest,
                "session grew before titling; discarding the title"
            );
            return Ok(false);
        }
        if self
            .projection
            .session_title(&snapshot.session_id)?
            .is_some()
        {
            info!(
                session_id = %snapshot.session_id,
                "session was titled by another pass; discarding the title"
            );
            return Ok(false);
        }
        self.append(
            Source::System,
            Some(now_ts()),
            event::Payload::Session(SessionEvent {
                event: Some(session_event::Event::SessionTitled(SessionTitled {
                    session_id: snapshot.session_id.clone(),
                    title: title.to_owned(),
                })),
            }),
        )?;
        Ok(true)
    }

    #[tracing::instrument(name = "memory.review_accept", skip(self), fields(record_id))]
    pub fn review_accept(&mut self, record_id: &str) -> Result<(), Error> {
        self.reviewable(record_id)?;
        self.append_memory(
            Source::User,
            memory_event::Event::RecordReviewed(MemoryRecordReviewed {
                record_id: record_id.to_owned(),
            }),
        )?;
        Ok(())
    }

    #[tracing::instrument(name = "memory.review_delete", skip(self), fields(record_id))]
    pub fn review_delete(&mut self, record_id: &str) -> Result<(), Error> {
        self.reviewable(record_id)?;
        self.append_memory(
            Source::User,
            memory_event::Event::RecordDeleted(MemoryRecordDeleted {
                id: record_id.to_owned(),
            }),
        )?;
        Ok(())
    }

    fn reviewable(&self, record_id: &str) -> Result<(), Error> {
        if self.projection.memory_record(record_id)?.is_none() {
            return Err(Error::UnknownRecord {
                id: record_id.to_owned(),
            });
        }
        Ok(())
    }

    fn append_memory(
        &mut self,
        source: Source,
        payload: memory_event::Event,
    ) -> Result<u64, Error> {
        let payload = event::Payload::Memory(MemoryEvent {
            event: Some(payload),
        });
        self.append(source, Some(now_ts()), payload)
    }
}

pub(crate) fn now_ts() -> Timestamp {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp {
        seconds: i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(elapsed.subsec_nanos()).unwrap_or(0),
    }
}
