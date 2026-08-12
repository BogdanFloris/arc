//! Tracing setup.
//!
//! One human-readable layer on stderr today. Task 7.1 adds the Perfetto layer
//! that DESIGN.md §8 calls for, and this function is the single seam it layers
//! into — so the daemon never grows a second place that decides what gets
//! recorded.

use anyhow::{Context as _, Result};
use tracing_subscriber::EnvFilter;

/// Filter used when `RUST_LOG` is unset: the daemon's own story at `info`, and
/// the engine room — log recovery, replays, provider calls — at `debug`.
const DEFAULT_FILTER: &str = "info,arc_core=debug";

/// Installs the global subscriber.
///
/// stderr, not stdout: `arcd login` prints a URL the user has to act on, and
/// log lines must not be interleaved into it.
///
/// # Errors
///
/// If a subscriber is already installed, or `RUST_LOG` is not a valid filter —
/// a typo there silences logs, which is worth failing over at startup.
pub fn init() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_FILTER))
        .context("building the tracing filter (check RUST_LOG)")?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|err| anyhow::anyhow!("installing the tracing subscriber: {err}"))
}
