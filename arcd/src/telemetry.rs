use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt as _, util::SubscriberInitExt as _};

const DEFAULT_FILTER: &str = "info,arc_core=debug";

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
