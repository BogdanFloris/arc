use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use super::Workspace;
use crate::provider::ToolDefinition;
use crate::tool::{Tool, ToolReply, ToolSource, TurnContext};

const DRAIN_GRACE: Duration = Duration::from_millis(500);
const MAX_CAPTURE_BYTES: usize = 16 * 1024;
const DEFAULT_TIMEOUT_SECS: u64 = 120;
const MIN_TIMEOUT_SECS: u64 = 1;
const MAX_TIMEOUT_SECS: u64 = 600;
const ENV_ALLOWLIST: [&str; 5] = ["PATH", "HOME", "USER", "TMPDIR", "LANG"];

pub struct Bash {
    workspace: Arc<Workspace>,
}

impl Bash {
    pub fn new(workspace: Arc<Workspace>) -> Self {
        Self { workspace }
    }
}

#[derive(Deserialize)]
struct BashArgs {
    command: String,
    timeout_secs: Option<u64>,
}

impl Tool for Bash {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "bash".to_owned(),
            description: "Run a shell command with bash in the project's writable root. The \
                          environment is scrubbed to a small allowlist; no secrets pass \
                          through. Output is capped at 16 KiB per stream. Commands default to \
                          a 120s timeout (max 600); raise timeout_secs for one that \
                          legitimately runs long."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "The shell command to run."},
                    "timeout_secs": {
                        "type": "integer",
                        "description": "Seconds before the command is killed. Default 120, max 600."
                    }
                },
                "required": ["command"]
            }),
        }
    }

    fn source(&self) -> ToolSource {
        ToolSource::Workspace
    }

    fn execute(
        &self,
        arguments_json: String,
        _ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async move {
            let args: BashArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad bash arguments ({error}). Pass {{\"command\": \"...\"}}."
                    ));
                }
            };

            let Some(root) = self.workspace.grants.read_write_root() else {
                return ToolReply::error(
                    "ERROR: this session has no writable project root to run in.".to_owned(),
                );
            };

            if args.command.trim().is_empty() {
                return ToolReply::error(
                    "ERROR: command is empty. Pass a non-empty shell command to run.".to_owned(),
                );
            }

            let timeout_secs = args
                .timeout_secs
                .unwrap_or(DEFAULT_TIMEOUT_SECS)
                .clamp(MIN_TIMEOUT_SECS, MAX_TIMEOUT_SECS);

            run(&args.command, root, timeout_secs).await
        })
    }
}

async fn run(command: &str, cwd: &Path, timeout_secs: u64) -> ToolReply {
    let mut cmd = Command::new("bash");
    cmd.args(["--noprofile", "--norc", "-c", command]);
    cmd.current_dir(cwd);
    scrub_env(&mut cmd);
    // CLIs block on an open stdin.
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    // own process group so a timeout kill reaches every grandchild.
    cmd.process_group(0);

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            return ToolReply::error(format!("ERROR: could not start bash ({error})."));
        }
    };
    let pgid = child.id().and_then(|id| i32::try_from(id).ok());
    let stdout_pipe = child.stdout.take().expect("stdout is piped");
    let stderr_pipe = child.stderr.take().expect("stderr is piped");
    let mut stdout_task = tokio::spawn(drain(stdout_pipe));
    let mut stderr_task = tokio::spawn(drain(stderr_pipe));

    let Ok(wait_result) =
        tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait()).await
    else {
        if let Some(pgid) = pgid {
            // negative pid signals the whole process group.
            unsafe {
                libc::kill(-pgid, libc::SIGKILL);
            }
        }
        let _ = child.wait().await;
        let stdout = stdout_task.await.unwrap_or_default();
        let stderr = stderr_task.await.unwrap_or_default();
        let header = format!("ERROR: timed out after {timeout_secs}s.");
        return ToolReply::error(compose(Some(&header), &stdout, &stderr));
    };

    // a background child can hold the pipes open past bash's own exit; after a
    // short grace the group is killed so EOF arrives and captured bytes survive.
    let drains = async { ((&mut stdout_task).await, (&mut stderr_task).await) };
    let (stdout, stderr) =
        if let Ok((stdout, stderr)) = tokio::time::timeout(DRAIN_GRACE, drains).await {
            (stdout.unwrap_or_default(), stderr.unwrap_or_default())
        } else {
            if let Some(pgid) = pgid {
                unsafe {
                    libc::kill(-pgid, libc::SIGKILL);
                }
            }
            (
                stdout_task.await.unwrap_or_default(),
                stderr_task.await.unwrap_or_default(),
            )
        };
    match wait_result {
        Ok(status) => reply_for(status, &stdout, &stderr),
        Err(error) => ToolReply::error(format!("ERROR: bash did not run to completion ({error}).")),
    }
}

fn scrub_env(cmd: &mut Command) {
    cmd.env_clear();
    // nix and cargo need HOME/USER/XDG_*; scrubbed isn't empty.
    for key in ENV_ALLOWLIST {
        if let Ok(value) = std::env::var(key) {
            cmd.env(key, value);
        }
    }
    for (key, value) in std::env::vars() {
        if key.starts_with("XDG_") {
            cmd.env(key, value);
        }
    }
}

#[derive(Default)]
struct Captured {
    text: String,
    truncated: bool,
}

async fn drain(mut reader: impl tokio::io::AsyncRead + Unpin + Send + 'static) -> Captured {
    let mut buf = Vec::new();
    let mut truncated = false;
    let mut chunk = [0u8; 8192];
    loop {
        let n = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if buf.len() < MAX_CAPTURE_BYTES {
            let take = (MAX_CAPTURE_BYTES - buf.len()).min(n);
            buf.extend_from_slice(&chunk[..take]);
            if take < n {
                truncated = true;
            }
        } else {
            truncated = true;
        }
    }
    let text = match std::str::from_utf8(&buf) {
        Ok(text) => text.to_owned(),
        Err(error) => {
            truncated = true;
            std::str::from_utf8(&buf[..error.valid_up_to()])
                .expect("valid_up_to bounds valid utf8")
                .to_owned()
        }
    };
    Captured { text, truncated }
}

fn reply_for(status: std::process::ExitStatus, stdout: &Captured, stderr: &Captured) -> ToolReply {
    let code = status.code();
    if code == Some(0) && stderr.text.is_empty() {
        let content = if stdout.text.is_empty() {
            "(no output)".to_owned()
        } else {
            mark(&stdout.text, stdout.truncated)
        };
        return ToolReply::ok(content);
    }

    let header = match code {
        Some(0) => None,
        Some(code) => Some(format!("exit {code}")),
        None => Some("exit signal".to_owned()),
    };
    let content = compose(header.as_deref(), stdout, stderr);
    if code == Some(0) {
        ToolReply::ok(content)
    } else {
        ToolReply::error(content)
    }
}

fn compose(header: Option<&str>, stdout: &Captured, stderr: &Captured) -> String {
    let mut parts = Vec::new();
    if let Some(header) = header {
        parts.push(header.to_owned());
    }
    parts.push(mark(&stdout.text, stdout.truncated));
    if !stderr.text.is_empty() {
        parts.push("--- stderr ---".to_owned());
        parts.push(mark(&stderr.text, stderr.truncated));
    }
    parts.join("\n")
}

fn mark(text: &str, truncated: bool) -> String {
    if truncated {
        format!("{text} [truncated]")
    } else {
        text.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tempfile::TempDir;

    use super::Bash;
    use crate::tool::workspace::{Grant, Grants, Mode, Workspace};
    use crate::tool::{Tool as _, TurnContext};

    fn workspace_rw(root: &std::path::Path) -> Arc<Workspace> {
        let grants = Grants::new(vec![Grant::new(root, Mode::ReadWrite)]).expect("grants");
        Arc::new(Workspace::new(grants))
    }

    fn args(command: &str) -> String {
        serde_json::json!({ "command": command }).to_string()
    }

    fn args_with_timeout(command: &str, timeout_secs: u64) -> String {
        serde_json::json!({ "command": command, "timeout_secs": timeout_secs }).to_string()
    }

    #[tokio::test]
    async fn an_echo_command_returns_its_stdout_verbatim() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));

        let reply = tool
            .execute(args("echo hello"), TurnContext::default())
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.content, "hello\n");
    }

    #[tokio::test]
    async fn a_command_with_no_output_reports_no_output() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));

        let reply = tool.execute(args("true"), TurnContext::default()).await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.content, "(no output)");
    }

    #[tokio::test]
    async fn a_nonzero_exit_is_an_error_naming_the_code_with_both_streams() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));

        let reply = tool
            .execute(
                args("echo out; echo err >&2; exit 3"),
                TurnContext::default(),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("exit 3"), "{}", reply.content);
        assert!(reply.content.contains("out"), "{}", reply.content);
        assert!(
            reply.content.contains("--- stderr ---"),
            "{}",
            reply.content
        );
        assert!(reply.content.contains("err"), "{}", reply.content);
    }

    #[tokio::test]
    async fn stderr_noise_with_a_clean_exit_is_ok_but_carries_the_stderr_section() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));

        let reply = tool
            .execute(args("echo warn >&2"), TurnContext::default())
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert!(
            reply.content.contains("--- stderr ---"),
            "{}",
            reply.content
        );
        assert!(reply.content.contains("warn"), "{}", reply.content);
        assert!(!reply.content.contains("exit 0"), "{}", reply.content);
    }

    #[tokio::test]
    async fn the_environment_is_scrubbed_of_everything_but_the_allowlist() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));
        unsafe {
            std::env::set_var("ARC_TEST_SECRET", "shh");
        }

        let reply = tool.execute(args("env"), TurnContext::default()).await;

        assert!(reply.ok, "{}", reply.content);
        assert!(
            !reply.content.contains("ARC_TEST_SECRET"),
            "{}",
            reply.content
        );
        assert!(reply.content.contains("HOME="), "{}", reply.content);
    }

    #[tokio::test]
    async fn xdg_prefixed_variables_pass_through() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));
        unsafe {
            std::env::set_var("XDG_ARC_TEST", "1");
        }

        let reply = tool.execute(args("env"), TurnContext::default()).await;

        assert!(reply.ok, "{}", reply.content);
        assert!(
            reply.content.contains("XDG_ARC_TEST=1"),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn the_command_runs_in_the_read_write_root() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));

        let reply = tool.execute(args("pwd"), TurnContext::default()).await;

        assert!(reply.ok, "{}", reply.content);
        let canonical = dir.path().canonicalize().expect("canonicalize");
        assert_eq!(reply.content.trim_end(), canonical.to_str().expect("utf8"));
    }

    #[tokio::test]
    async fn stdout_over_the_cap_is_truncated_and_marked() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));

        let reply = tool
            .execute(
                args("head -c 20000 /dev/zero | tr '\\0' 'x'"),
                TurnContext::default(),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert!(reply.content.contains("[truncated]"), "{}", reply.content);
        assert!(reply.content.len() < 20_000, "{}", reply.content.len());
    }

    #[tokio::test]
    async fn a_command_past_its_timeout_is_an_error_naming_the_deadline() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));

        let reply = tool
            .execute(args_with_timeout("sleep 5", 1), TurnContext::default())
            .await;

        assert!(!reply.ok);
        assert!(
            reply.content.starts_with("ERROR: timed out after"),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn a_timeout_kills_the_whole_process_group_and_returns_promptly() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));

        let start = Instant::now();
        let reply = tool
            .execute(
                args_with_timeout("sleep 5; echo late", 1),
                TurnContext::default(),
            )
            .await;
        let elapsed = start.elapsed();

        assert!(!reply.ok);
        assert!(!reply.content.contains("late"), "{}", reply.content);
        assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    }

    #[tokio::test]
    async fn no_writable_root_is_a_named_error() {
        let dir = TempDir::new().expect("tmp");
        let grants = Grants::new(vec![Grant::new(dir.path(), Mode::ReadOnly)]).expect("grants");
        let tool = Bash::new(Arc::new(Workspace::new(grants)));

        let reply = tool.execute(args("echo hi"), TurnContext::default()).await;

        assert!(!reply.ok);
        assert!(reply.content.contains("writable"), "{}", reply.content);
    }

    #[tokio::test]
    async fn a_missing_command_argument_is_an_actionable_error() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));

        let reply = tool.execute("{}".to_owned(), TurnContext::default()).await;

        assert!(!reply.ok);
        assert!(reply.content.contains("command"), "{}", reply.content);
    }

    #[tokio::test]
    async fn an_empty_command_is_an_actionable_error() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));

        let reply = tool.execute(args("   "), TurnContext::default()).await;

        assert!(!reply.ok);
        assert!(reply.content.contains("empty"), "{}", reply.content);
    }

    #[tokio::test]
    async fn a_background_child_holding_the_pipe_does_not_hang_the_call() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new(workspace_rw(dir.path()));
        let started = std::time::Instant::now();

        let reply = tool
            .execute(args("sleep 30 & echo up"), TurnContext::default())
            .await;

        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the call must return once bash exits, not when the orphan dies"
        );
        assert!(reply.content.contains("up"), "{}", reply.content);
    }
}
