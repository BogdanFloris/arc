use std::path::PathBuf;

use serde_json::json;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::sync::mpsc;

const SOURCE: &str = "custom:arc";
const AGENT: &str = "arc";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentState {
    Idle,
    Working,
    Blocked,
    Done,
}

impl AgentState {
    fn wire(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Working => "working",
            Self::Blocked => "blocked",
            Self::Done => "done",
        }
    }
}

/// Pushes lifecycle state to a surrounding herdr pane so its sidebar reads
/// arc's real state instead of screen-scraping. Display only, never blocks,
/// inert outside herdr.
pub struct Reporter {
    tx: Option<mpsc::UnboundedSender<String>>,
    task: Option<tokio::task::JoinHandle<()>>,
    pane_id: String,
    seq: u64,
    state: Option<AgentState>,
    meta: Option<(String, usize)>,
}

impl Reporter {
    pub fn from_env() -> Self {
        let pane_id = std::env::var("HERDR_PANE_ID")
            .ok()
            .filter(|v| !v.is_empty());
        let path = socket_path(
            std::env::var("HERDR_SOCKET_PATH").ok(),
            std::env::var("HERDR_SESSION").ok(),
            std::env::var("XDG_CONFIG_HOME").ok(),
            std::env::var("HOME").ok(),
        );
        let (tx, task) = match pane_id.as_ref().zip(path) {
            Some((_, path)) => {
                let (tx, rx) = mpsc::unbounded_channel();
                (Some(tx), Some(tokio::spawn(pump(path, rx))))
            }
            None => (None, None),
        };
        Self {
            tx,
            task,
            pane_id: pane_id.unwrap_or_default(),
            // herdr orders reports per source by seq and remembers it across
            // processes; a restart counting from 0 would be dropped as stale
            seq: unix_millis(),
            state: None,
            meta: None,
        }
    }

    /// A report outlives its process in herdr, so quitting without this
    /// leaves the pane wearing arc's last state forever.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.tx.take() {
            let mut seq = self.seq;
            let _ = tx.send(report_metadata(&self.pane_id, "", 0, seq));
            seq += 1;
            let _ = tx.send(release_agent(&self.pane_id, seq));
        }
        if let Some(task) = self.task.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_millis(500), task).await;
        }
    }

    pub fn state(&mut self, state: AgentState) {
        if self.tx.is_none() || self.state == Some(state) {
            return;
        }
        self.state = Some(state);
        let line = report_agent(&self.pane_id, state, self.seq);
        self.send(line);
    }

    pub fn metadata(&mut self, title: &str, running_jobs: usize) {
        if self.tx.is_none() {
            return;
        }
        let next = (title.to_owned(), running_jobs);
        if self.meta.as_ref() == Some(&next) {
            return;
        }
        self.meta = Some(next);
        let line = report_metadata(&self.pane_id, title, running_jobs, self.seq);
        self.send(line);
    }

    fn send(&mut self, line: String) {
        self.seq += 1;
        if let Some(tx) = &self.tx {
            let _ = tx.send(line);
        }
    }
}

fn report_agent(pane_id: &str, state: AgentState, seq: u64) -> String {
    ndjson(&json!({
        "id": format!("arc-{seq}"),
        "method": "pane.report_agent",
        "params": {
            "pane_id": pane_id,
            "source": SOURCE,
            "agent": AGENT,
            "state": state.wire(),
            "seq": seq,
        },
    }))
}

fn report_metadata(pane_id: &str, title: &str, running_jobs: usize, seq: u64) -> String {
    ndjson(&json!({
        "id": format!("arc-{seq}"),
        "method": "pane.report_metadata",
        "params": {
            "pane_id": pane_id,
            "source": SOURCE,
            "agent": AGENT,
            "title": (!title.is_empty()).then_some(title),
            "clear_title": title.is_empty(),
            "tokens": {
                "jobs": (running_jobs > 0).then(|| format!("{running_jobs} running")),
            },
            "seq": seq,
        },
    }))
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(0))
}

fn release_agent(pane_id: &str, seq: u64) -> String {
    ndjson(&json!({
        "id": format!("arc-{seq}"),
        "method": "pane.release_agent",
        "params": {
            "pane_id": pane_id,
            "source": SOURCE,
            "agent": AGENT,
            "seq": seq,
        },
    }))
}

fn ndjson(request: &serde_json::Value) -> String {
    let mut line = request.to_string();
    line.push('\n');
    line
}

fn socket_path(
    explicit: Option<String>,
    session: Option<String>,
    xdg_config: Option<String>,
    home: Option<String>,
) -> Option<PathBuf> {
    if let Some(path) = explicit.filter(|v| !v.is_empty()) {
        return Some(path.into());
    }
    let config = match xdg_config.filter(|v| !v.is_empty()) {
        Some(xdg) => PathBuf::from(xdg),
        None => PathBuf::from(home.filter(|v| !v.is_empty())?).join(".config"),
    }
    .join("herdr");
    Some(match session.filter(|v| !v.is_empty()) {
        Some(session) => config.join("sessions").join(session).join("herdr.sock"),
        None => config.join("herdr.sock"),
    })
}

async fn pump(path: PathBuf, mut rx: mpsc::UnboundedReceiver<String>) {
    while let Some(line) = rx.recv().await {
        request(&path, &line).await;
    }
}

// the server answers one request per connection, then hangs up
async fn request(path: &std::path::Path, line: &str) {
    let mut stream = match UnixStream::connect(path).await {
        Ok(stream) => stream,
        Err(error) => {
            tracing::debug!(%error, "herdr socket connect failed");
            return;
        }
    };
    if stream.write_all(line.as_bytes()).await.is_err() {
        return;
    }
    let mut reply = String::new();
    let mut reader = tokio::io::BufReader::new(stream);
    let read = reader.read_line(&mut reply);
    if tokio::time::timeout(std::time::Duration::from_secs(1), read)
        .await
        .is_ok()
        && reply.contains("\"error\"")
    {
        tracing::debug!(%reply, "herdr rejected a report");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(line: &str) -> serde_json::Value {
        assert!(line.ends_with('\n'));
        serde_json::from_str(line.trim_end()).expect("valid json")
    }

    #[test]
    fn report_agent_matches_the_wire_shape() {
        let line = parsed(&report_agent("w1:p2", AgentState::Working, 7));
        assert_eq!(line["id"], "arc-7");
        assert_eq!(line["method"], "pane.report_agent");
        let params = &line["params"];
        assert_eq!(params["pane_id"], "w1:p2");
        assert_eq!(params["source"], "custom:arc");
        assert_eq!(params["agent"], "arc");
        assert_eq!(params["state"], "working");
        assert_eq!(params["seq"], 7);
    }

    #[test]
    fn every_state_serializes_to_a_herdr_status() {
        for (state, wire) in [
            (AgentState::Idle, "idle"),
            (AgentState::Working, "working"),
            (AgentState::Blocked, "blocked"),
            (AgentState::Done, "done"),
        ] {
            assert_eq!(state.wire(), wire);
        }
    }

    #[test]
    fn metadata_carries_title_and_jobs_and_clears_both_when_empty() {
        let line = parsed(&report_metadata("w1:p2", "the quiet week", 2, 3));
        assert_eq!(line["params"]["title"], "the quiet week");
        assert_eq!(line["params"]["clear_title"], false);
        assert_eq!(line["params"]["tokens"]["jobs"], "2 running");

        let line = parsed(&report_metadata("w1:p2", "", 0, 4));
        assert!(line["params"]["title"].is_null());
        assert_eq!(line["params"]["clear_title"], true);
        assert!(line["params"]["tokens"]["jobs"].is_null());
    }

    #[test]
    fn release_matches_the_wire_shape() {
        let line = parsed(&release_agent("w1:p2", 9));
        assert_eq!(line["method"], "pane.release_agent");
        assert_eq!(line["params"]["pane_id"], "w1:p2");
        assert_eq!(line["params"]["source"], "custom:arc");
        assert_eq!(line["params"]["agent"], "arc");
    }

    #[test]
    fn the_socket_resolves_explicit_then_session_then_default() {
        let explicit = socket_path(
            Some("/run/h.sock".into()),
            None,
            None,
            Some("/home/b".into()),
        );
        assert_eq!(explicit, Some(PathBuf::from("/run/h.sock")));

        let session = socket_path(None, Some("work".into()), None, Some("/home/b".into()));
        assert_eq!(
            session,
            Some(PathBuf::from(
                "/home/b/.config/herdr/sessions/work/herdr.sock"
            ))
        );

        let default = socket_path(None, None, Some("/xdg".into()), Some("/home/b".into()));
        assert_eq!(default, Some(PathBuf::from("/xdg/herdr/herdr.sock")));

        assert_eq!(socket_path(None, None, None, None), None);
    }
}
