//! `arcd run` — startup composition and lifecycle.
//!
//! [`Daemon::start`] is the whole of the daemon's wiring: open the log,
//! catch the index up to it, build the provider, and hand back a value that
//! owns all of it. [`Daemon::serve`] is the lifecycle: announce readiness, wait
//! for a shutdown signal, stop.
//!
//! Nothing here decides anything — every rule lives in `arc-core` (DESIGN.md
//! §2). What this file owns is the order things happen in, and the property
//! that all of it happens before the daemon claims to be ready. A half-started
//! daemon is worse than one that refused to start, so any failure below comes
//! straight back out to `main`.
//!
//! Startup I/O is synchronous — the log and projection layers are deliberately
//! blocking — and runs before anything is being served, so it does not matter
//! that it holds the runtime thread.

use anyhow::{Context as _, Result};
use arc_core::log::Log;
use arc_core::projection::{self, Projection};
use arc_core::provider::antigravity::Antigravity;
use arc_core::provider::oauth::{OauthConfig, TokenManager};
use std::sync::Arc;
use tracing::info;

use crate::config::Config;
use crate::dirs::DataDirs;

/// A started daemon: everything durable is open, nothing is being served yet.
pub struct Daemon {
    config: Config,
    dirs: DataDirs,

    /// The event log. `mut` from 5.2 on — every durable change is an append.
    log: Log,

    /// The `SQLite` index, replayed up to the log's head.
    #[allow(dead_code, reason = "task 5.2 reads sessions and messages from here")]
    projection: Projection,

    /// The provider. Constructed without a network call: auth is lazy, so the
    /// daemon starts on a machine with no connectivity and fails, if it must,
    /// on the first completion instead.
    #[allow(dead_code, reason = "task 5.2 drives completions through this")]
    provider: Antigravity,
}

impl Daemon {
    /// Opens everything the daemon needs, in dependency order.
    ///
    /// # Errors
    ///
    /// If the directories cannot be created, the log cannot be opened or
    /// recovered, or the index cannot be opened or replayed. All of these mean
    /// durable state is not in a known condition, which is a refusal to start.
    #[tracing::instrument(name = "daemon.start", skip_all, fields(data_dir = %dirs.root().display()))]
    pub fn start(config: Config, dirs: DataDirs) -> Result<Self> {
        dirs.create()
            .with_context(|| format!("preparing {}", dirs.root().display()))?;

        // Recovery happens inside `open`: a torn tail is sealed, never
        // truncated, and the append point comes back with it (DESIGN.md §3).
        let log = Log::open(dirs.log())
            .with_context(|| format!("opening the event log at {}", dirs.log().display()))?;
        info!(
            next_seq = log.next_seq(),
            segment = %log.current_segment().display(),
            "event log ready"
        );

        let mut projection = Projection::open(dirs.index())
            .with_context(|| format!("opening the index at {}", dirs.index().display()))?;
        let reader = log.reader().context("listing log segments for replay")?;
        let stats = projection::replay(reader, &mut projection)
            .context("replaying the event log into the index")?;
        info!(
            applied = stats.applied,
            skipped = stats.skipped,
            "index caught up with the log"
        );

        let tokens = TokenManager::new(OauthConfig::default(), dirs.tokens());
        let provider = Antigravity::new(Arc::new(tokens));

        Ok(Self {
            config,
            dirs,
            log,
            projection,
            provider,
        })
    }

    /// Announces readiness and runs until a shutdown signal.
    ///
    /// # Errors
    ///
    /// If the signal handler cannot be installed.
    pub async fn serve(self) -> Result<()> {
        // 5.4 plugs in here: bind `config.bind` and serve `wire.proto`, with
        // 5.2's session engine behind it and 5.3's identity file in its
        // context. Until then the daemon holds its state open and waits.
        info!(
            model = self.config.model,
            bind = %self.config.bind,
            data_dir = %self.dirs.root().display(),
            next_seq = self.log.next_seq(),
            version = env!("CARGO_PKG_VERSION"),
            "arcd ready"
        );

        tokio::signal::ctrl_c()
            .await
            .context("waiting for the shutdown signal")?;

        info!("shutting down");
        Ok(())
    }
}
