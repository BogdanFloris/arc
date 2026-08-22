use arc_proto::v1::{Event, Source, event};
use prost_types::Timestamp;

use crate::log::{self, Log, LogReader};
use crate::projection::{self, Projection};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("store log: {0}")]
    Log(#[from] log::Error),

    #[error("store projection: {0}")]
    Projection(#[from] projection::Error),
}

// the only writer: an append that does not project is a torn invariant, not a fast path
#[derive(Debug)]
pub struct Store {
    log: Log,
    projection: Projection,
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
}
