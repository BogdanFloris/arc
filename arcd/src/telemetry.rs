use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

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
    let pruned = prune_old_traces(traces, TRACE_RETENTION);
    if pruned > 0 {
        tracing::info!(pruned, "old traces removed");
    }
    Ok(path)
}

/// Traces are rebuildable diagnostics, excluded from backup; two weeks of
/// them is plenty (task 7.4's retention policy).
const TRACE_RETENTION: Duration = Duration::from_secs(14 * 24 * 60 * 60);

fn prune_old_traces(traces: &Path, retention: Duration) -> usize {
    let Some(cutoff) = SystemTime::now().checked_sub(retention) else {
        return 0;
    };
    let Ok(entries) = std::fs::read_dir(traces) else {
        return 0;
    };
    let mut pruned = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "pftrace") {
            continue;
        }
        let old = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .is_ok_and(|modified| modified < cutoff);
        if old && std::fs::remove_file(&path).is_ok() {
            pruned += 1;
        }
    }
    pruned
}

#[cfg(test)]
mod tests {
    use super::{TRACE_RETENTION, prune_old_traces};
    use std::time::Duration;

    #[test]
    fn prune_removes_only_old_pftrace_files() {
        let dir = tempfile::TempDir::new().expect("dir");
        let old_trace = dir.path().join("arc-1.pftrace");
        let fresh_trace = dir.path().join("arc-2.pftrace");
        let other = dir.path().join("notes.txt");
        for path in [&old_trace, &fresh_trace, &other] {
            std::fs::write(path, b"x").expect("write");
        }
        let stale = std::time::SystemTime::now() - TRACE_RETENTION - Duration::from_secs(60);
        for path in [&old_trace, &other] {
            let file = std::fs::File::options()
                .append(true)
                .open(path)
                .expect("open");
            file.set_modified(stale).expect("age the file");
        }

        assert_eq!(prune_old_traces(dir.path(), TRACE_RETENTION), 1);
        assert!(!old_trace.exists(), "the stale trace is gone");
        assert!(fresh_trace.exists(), "the fresh trace stays");
        assert!(other.exists(), "non-trace files are never touched");
    }

    #[test]
    fn prune_on_a_missing_dir_is_a_quiet_zero() {
        let dir = tempfile::TempDir::new().expect("dir");
        let missing = dir.path().join("nope");
        assert_eq!(prune_old_traces(&missing, TRACE_RETENTION), 0);
    }
}
