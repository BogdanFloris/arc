use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::config::LlamaConfig;

const READY_TIMEOUT: Duration = Duration::from_secs(180);

const POLL: Duration = Duration::from_millis(500);

pub struct Sidecar {
    endpoint: String,

    kill: Option<oneshot::Sender<()>>,

    monitor: JoinHandle<()>,
}

impl Sidecar {
    #[tracing::instrument(name = "llama.start", skip_all, fields(model_file = %config.model_file.display(), port = config.port))]
    pub async fn start(config: &LlamaConfig, model: &str) -> Result<Self> {
        if !config.model_file.is_file() {
            bail!(
                "llama.model_file {} does not exist — download the default with `just model`, \
                 or point it at a GGUF you have",
                config.model_file.display()
            );
        }

        let endpoint = format!("http://127.0.0.1:{}", config.port);
        let child = spawn_ready(config, model, &endpoint).await?;
        info!(%endpoint, model, "llama-server ready");

        let (kill, killed) = oneshot::channel();
        let monitor = tokio::spawn(monitor(
            child,
            killed,
            config.clone(),
            model.to_owned(),
            endpoint.clone(),
        ));
        Ok(Self {
            endpoint,
            kill: Some(kill),
            monitor,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn stop(mut self) {
        // dropping the sender is the kill signal
        self.kill.take();
        if let Err(error) = (&mut self.monitor).await {
            warn!(%error, "the sidecar monitor task failed");
        }
    }
}

async fn resolve_device(config: &LlamaConfig, wanted: &str) -> Result<String> {
    let output = Command::new(&config.server)
        .arg("--list-devices")
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("running {} --list-devices", config.server.display()))?;
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

async fn spawn_ready(config: &LlamaConfig, model: &str, endpoint: &str) -> Result<Child> {
    // re-resolved on every spawn: enumeration order is not stable across a driver reset
    let device = match &config.device {
        Some(wanted) => Some(resolve_device(config, wanted).await?),
        None => None,
    };

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
        .kill_on_drop(true)
        .spawn()
        .with_context(|| {
            format!(
                "spawning {} — is llama.cpp installed and on PATH?",
                config.server.display()
            )
        })?;

    wait_ready(&mut child, endpoint).await?;
    Ok(child)
}

const RESTART_LIMIT: u32 = 3;

// a sidecar that stayed up this long counts as recovered
const HEALTHY_FOR: Duration = Duration::from_secs(60);

#[derive(Default)]
struct Restarts {
    consecutive: u32,
}

impl Restarts {
    fn next_backoff(&mut self, uptime: Duration) -> Option<Duration> {
        if uptime >= HEALTHY_FOR {
            self.consecutive = 0;
        }
        self.consecutive += 1;
        (self.consecutive <= RESTART_LIMIT)
            .then(|| Duration::from_secs(1 << (self.consecutive - 1)))
    }
}

async fn monitor(
    mut child: Child,
    mut killed: oneshot::Receiver<()>,
    config: LlamaConfig,
    model: String,
    endpoint: String,
) {
    let mut restarts = Restarts::default();
    loop {
        let started = Instant::now();
        tokio::select! {
            status = child.wait() => {
                match status {
                    Ok(status) => error!(%status, "llama-server exited unexpectedly"),
                    Err(error) => {
                        error!(%error, "waiting on llama-server failed; leaving it down");
                        return;
                    }
                }
                let Some(backoff) = restarts.next_backoff(started.elapsed()) else {
                    error!(
                        limit = RESTART_LIMIT,
                        "llama-server keeps failing; leaving it down, the archivist will error"
                    );
                    return;
                };
                warn!(seconds = backoff.as_secs(), "restarting llama-server");
                tokio::select! {
                    () = tokio::time::sleep(backoff) => {}
                    _ = &mut killed => return,
                }
                match spawn_ready(&config, &model, &endpoint).await {
                    Ok(next) => {
                        info!(%endpoint, "llama-server back up");
                        child = next;
                    }
                    Err(error) => {
                        error!(%error, "restarting llama-server failed; leaving it down");
                        return;
                    }
                }
            }
            _ = &mut killed => {
                if let Err(error) = child.kill().await {
                    warn!(%error, "killing llama-server failed");
                }
                return;
            }
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::{HEALTHY_FOR, RESTART_LIMIT, Restarts, find_device};
    use std::time::Duration;

    const CRASHED: Duration = Duration::from_secs(1);

    #[test]
    fn a_crash_loop_backs_off_and_then_gives_up() {
        let mut restarts = Restarts::default();

        let waits: Vec<u64> = (0..RESTART_LIMIT)
            .map(|_| {
                restarts
                    .next_backoff(CRASHED)
                    .expect("under the limit")
                    .as_secs()
            })
            .collect();
        assert_eq!(waits, [1, 2, 4], "each failure waits twice as long");
        assert!(
            restarts.next_backoff(CRASHED).is_none(),
            "past the limit the sidecar stays down instead of spinning"
        );
    }

    #[test]
    fn a_sidecar_that_ran_a_while_starts_its_count_over() {
        let mut restarts = Restarts::default();
        for _ in 0..RESTART_LIMIT {
            restarts.next_backoff(CRASHED).expect("under the limit");
        }

        assert_eq!(
            restarts.next_backoff(HEALTHY_FOR).map(|w| w.as_secs()),
            Some(1),
            "an exit after a healthy run is a first failure, not a fourth"
        );
    }

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
        assert_eq!(
            find_device(LISTING, "MiB").map(|(id, _)| id),
            Some("Vulkan0".to_owned())
        );
    }

    #[test]
    fn prose_lines_are_not_devices() {
        let noisy =
            "warning: something: happened here\nAvailable devices:\n  CUDA0: NVIDIA T4 (16 GiB)\n";
        assert_eq!(
            find_device(noisy, "t4").map(|(id, _)| id),
            Some("CUDA0".to_owned())
        );
    }
}
