//! The llama.cpp sidecar: `arcd` owns the local model server's process.
//!
//! `llama-server` is spawned at startup, polled on `/health` until the model
//! is loaded, and killed when the daemon stops (DESIGN.md §6, amendment
//! 2026-08-13 — the daemon supervises, rather than a systemd unit). A monitor
//! task holds the child: an exit nobody asked for is logged loudly, and
//! dropping the [`Sidecar`] is itself the kill signal, so no path out of the
//! daemon — error or clean — leaves a stray model server holding gigabytes of
//! VRAM.
//!
//! No auto-restart in Phase 1. A sidecar that dies mid-run makes completions
//! fail with a transport error the client renders as a fault, which is
//! honest; respawning in a loop over a bad flag or an OOM would not be.
//! Restart policy is a decision for later, noted in TASKS.md, not improvised
//! here.
//!
//! The kill is SIGKILL, deliberately: `llama-server` holds no state worth
//! flushing, and a graceful-shutdown dance would buy nothing but a slower
//! exit.

use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::config::LlamaConfig;

/// How long a cold start may take: a model load reads gigabytes from disk.
const READY_TIMEOUT: Duration = Duration::from_secs(180);

/// Gap between `/health` polls while loading.
const POLL: Duration = Duration::from_millis(500);

/// A supervised `llama-server`, ready to answer completions.
pub struct Sidecar {
    /// Where the server listens. `OpenAiCompat`'s endpoint.
    endpoint: String,

    /// Tells the monitor the daemon wants the child dead. Dropped unsent, it
    /// says the same thing — see [`monitor`].
    kill: Option<oneshot::Sender<()>>,

    /// The monitor task, joined by [`stop`](Self::stop) so shutdown does not
    /// return before the process is gone.
    monitor: JoinHandle<()>,
}

impl Sidecar {
    /// Spawns `llama-server` for `config` and waits until it answers.
    ///
    /// `model` becomes the server's `--alias`, so the name completions run as
    /// is also the name the server reports.
    ///
    /// # Errors
    ///
    /// If the model file is missing, the binary cannot be spawned, the
    /// process exits before becoming ready, or [`READY_TIMEOUT`] passes
    /// first. All of these are startup refusals: a daemon whose default
    /// provider cannot answer should not claim to be ready.
    #[tracing::instrument(name = "llama.start", skip_all, fields(model_file = %config.model_file.display(), port = config.port))]
    pub async fn start(config: &LlamaConfig, model: &str) -> Result<Self> {
        if !config.model_file.is_file() {
            bail!(
                "llama.model_file {} does not exist — download the default with `just model`, \
                 or point it at a GGUF you have",
                config.model_file.display()
            );
        }

        // Resolved fresh at every start: device names are stable across
        // reboots, backend indexes are not — pinning an index is how the
        // model once silently landed on the iGPU.
        let device = match &config.device {
            Some(wanted) => Some(resolve_device(config, wanted).await?),
            None => None,
        };

        let endpoint = format!("http://127.0.0.1:{}", config.port);
        let mut command = Command::new(&config.server);
        command
            .arg("--model")
            .arg(&config.model_file)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(config.port.to_string())
            .arg("--alias")
            .arg(model)
            .args(&config.args);
        if let Some(id) = &device {
            command.arg("--device").arg(id);
        }
        let mut child = command
            .stdin(Stdio::null())
            // stdout/stderr inherited: the server's log is part of the
            // daemon's, and hiding it would hide every model-load error.
            .kill_on_drop(true)
            .spawn()
            .with_context(|| {
                format!(
                    "spawning {} — is llama.cpp installed and on PATH?",
                    config.server.display()
                )
            })?;

        wait_ready(&mut child, &endpoint).await?;
        info!(%endpoint, model, "llama-server ready");

        let (kill, killed) = oneshot::channel();
        let monitor = tokio::spawn(monitor(child, killed));
        Ok(Self {
            endpoint,
            kill: Some(kill),
            monitor,
        })
    }

    /// Where the server listens.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Kills the sidecar and waits until it is gone.
    pub async fn stop(mut self) {
        // Dropping the sender is the signal; `send` would say nothing more.
        self.kill.take();
        if let Err(error) = (&mut self.monitor).await {
            warn!(%error, "the sidecar monitor task failed");
        }
    }
}

/// Asks `llama-server --list-devices` which device matches `wanted` and
/// returns its current backend id (e.g. `Vulkan0`).
///
/// No match is a startup refusal, with the listing in the error so the fix
/// is one config edit away: starting on whatever device happens to hold the
/// wanted index is the silent failure this resolution exists to prevent.
async fn resolve_device(config: &LlamaConfig, wanted: &str) -> Result<String> {
    let output = Command::new(&config.server)
        .arg("--list-devices")
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("running {} --list-devices", config.server.display()))?;
    // The listing prints regardless of exit status on current builds, but an
    // empty stdout with a failure is a real refusal.
    let listing = String::from_utf8_lossy(&output.stdout);
    let Some((id, name)) = find_device(&listing, wanted) else {
        bail!(
            "no model device matching {wanted:?} — `{} --list-devices` says:\n{}",
            config.server.display(),
            listing.trim_end()
        );
    };
    info!(device = %id, %name, "pinned the model device by name");
    Ok(id)
}

/// The first `<Id>: <name> (...)` line of a `--list-devices` listing whose
/// name contains `wanted`, case-insensitive. The id is the token before the
/// first colon and never holds whitespace, which is what separates a device
/// line from prose.
fn find_device(listing: &str, wanted: &str) -> Option<(String, String)> {
    let wanted = wanted.to_lowercase();
    for line in listing.lines() {
        let Some((id, rest)) = line.trim().split_once(':') else {
            continue;
        };
        let (id, name) = (id.trim(), rest.trim());
        if id.is_empty() || id.contains(char::is_whitespace) || name.is_empty() {
            continue;
        }
        if name.to_lowercase().contains(&wanted) {
            return Some((id.to_owned(), name.to_owned()));
        }
    }
    None
}

/// Owns the child until it exits or the daemon asks for it to be killed.
///
/// The kill signal is the channel closing, not a message: an explicit
/// [`Sidecar::stop`] and a `Sidecar` dropped on an error path both close it,
/// so every way out of the daemon kills the child the same way.
async fn monitor(mut child: Child, killed: oneshot::Receiver<()>) {
    tokio::select! {
        status = child.wait() => match status {
            // No one asked: turns will fail with a transport error until the
            // daemon is restarted. Loud, and deliberately not auto-healed.
            Ok(status) => error!(%status, "llama-server exited unexpectedly"),
            Err(error) => error!(%error, "waiting on llama-server failed"),
        },
        _ = killed => {
            if let Err(error) = child.kill().await {
                warn!(%error, "killing llama-server failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::find_device;

    const LISTING: &str = "Available devices:\n  \
        Vulkan0: NVIDIA GeForce RTX 5070 (12227 MiB, 8861 MiB free)\n  \
        Vulkan1: AMD Ryzen 9 9900X 12-Core Processor (RADV RAPHAEL_MENDOCINO) (16137 MiB, 10371 MiB free)\n";

    #[test]
    fn finds_a_device_by_case_insensitive_substring() {
        assert_eq!(
            find_device(LISTING, "rtx 5070"),
            Some((
                "Vulkan0".to_owned(),
                "NVIDIA GeForce RTX 5070 (12227 MiB, 8861 MiB free)".to_owned()
            ))
        );
        assert_eq!(
            find_device(LISTING, "RADV").map(|(id, _)| id),
            Some("Vulkan1".to_owned())
        );
    }

    #[test]
    fn no_match_is_none_not_a_guess() {
        assert_eq!(find_device(LISTING, "RTX 4090"), None);
        assert_eq!(find_device("", "RTX 5070"), None);
    }

    #[test]
    fn the_first_of_several_matches_wins() {
        // Both names contain "MiB"; the listing is ordered, so first wins.
        assert_eq!(
            find_device(LISTING, "MiB").map(|(id, _)| id),
            Some("Vulkan0".to_owned())
        );
    }

    #[test]
    fn prose_lines_are_not_devices() {
        // "Available devices:" has an empty rest; noise with spaces in the
        // would-be id is skipped too.
        let noisy =
            "warning: something: happened here\nAvailable devices:\n  CUDA0: NVIDIA T4 (16 GiB)\n";
        assert_eq!(
            find_device(noisy, "t4").map(|(id, _)| id),
            Some("CUDA0".to_owned())
        );
    }
}

/// Polls `/health` until the server answers 200, the child exits, or the
/// timeout passes.
///
/// `llama-server` answers 503 while the model is loading and 200 once it can
/// serve; anything transport-shaped just means "not yet".
async fn wait_ready(child: &mut Child, endpoint: &str) -> Result<()> {
    let http = reqwest::Client::new();
    let url = format!("{endpoint}/health");
    let deadline = Instant::now() + READY_TIMEOUT;

    loop {
        if let Some(status) = child.try_wait().context("checking on llama-server")? {
            bail!(
                "llama-server exited with {status} before becoming ready — \
                 its log above says why"
            );
        }
        if let Ok(response) = http.get(&url).send().await {
            if response.status().is_success() {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            let _ = child.start_kill();
            bail!(
                "llama-server did not become ready within {}s",
                READY_TIMEOUT.as_secs()
            );
        }
        tokio::time::sleep(POLL).await;
    }
}
