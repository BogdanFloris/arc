//! Tracing setup.
//!
//! Two layers over one filter: a human-readable one on stderr, and the
//! Perfetto layer DESIGN.md §8 calls for, writing a `.pftrace` under
//! `data/traces/`. This function is the single seam either of them layers
//! into — so the daemon never grows a second place that decides what gets
//! recorded.

use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt as _, util::SubscriberInitExt as _};

/// Filter used when `RUST_LOG` is unset: the daemon's own story at `info`, and
/// the engine room — log recovery, replays, provider calls — at `debug`.
const DEFAULT_FILTER: &str = "info,arc_core=debug";

/// Installs the global subscriber, and says where the trace is being written.
///
/// # Errors
///
/// If a subscriber is already installed, `RUST_LOG` is not a valid filter — a
/// typo there silences logs, which is worth failing over at startup — or the
/// trace file cannot be opened.
pub fn init(traces: &Path) -> Result<PathBuf> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_FILTER))
        .context("building the tracing filter (check RUST_LOG)")?;

    let (perfetto, path) = arc_core::trace::perfetto(traces, "arcd")
        .with_context(|| format!("opening a trace file in {}", traces.display()))?;

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_writer(std::io::stderr))
        .with(perfetto)
        .try_init()
        .map_err(|err| anyhow::anyhow!("installing the tracing subscriber: {err}"))?;

    tracing::info!(trace = %path.display(), "writing a perfetto trace");
    Ok(path)
}
