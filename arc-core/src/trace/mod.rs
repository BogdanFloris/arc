//! Perfetto tracing (DESIGN.md §8).
//!
//! ARC's `tracing` spans are its debugging surface, not decoration: the two
//! subsystems that will be hard to get right — consolidation quality and
//! retrieval behavior — are only debuggable against traces of real behavior.
//! This module is the half that renders spans into Perfetto's `TracePacket`
//! protos; the spans themselves live with the code they instrument.
//!
//! ```no_run
//! use tracing_subscriber::prelude::*;
//!
//! let (layer, path) = arc_core::trace::perfetto("data/traces", "arcd")?;
//! tracing_subscriber::registry().with(layer).init();
//! println!("tracing to {}", path.display());
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! The file opens directly in <https://ui.perfetto.dev>, or in
//! `trace_processor_shell` (in the dev shell) for SQL over it.

mod layer;
mod writer;

use std::{
    io,
    path::{Path, PathBuf},
};

pub use layer::PerfettoLayer;

/// Opens a trace file in `dir` and returns the layer writing to it.
///
/// One file per call, named for the second it was opened, so a run's spans
/// stay together. `process_name` labels the process track — the top row in
/// the UI.
///
/// # Errors
///
/// If `dir` cannot be created, or the trace file cannot be opened.
pub fn perfetto(dir: impl AsRef<Path>, process_name: &str) -> io::Result<(PerfettoLayer, PathBuf)> {
    PerfettoLayer::create(dir.as_ref(), process_name)
}
