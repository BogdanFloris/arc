use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::provider::ToolDefinition;
use crate::tool::{Tool, ToolReply, ToolSource, TurnContext};

const DRAIN_GRACE: Duration = Duration::from_millis(500);
const HEAD_BYTES: usize = 4 * 1024;
const TAIL_BYTES: usize = 12 * 1024;
const DEFAULT_TIMEOUT_SECS: u64 = 120;
const MIN_TIMEOUT_SECS: u64 = 1;
const MAX_TIMEOUT_SECS: u64 = 600;
const ENV_ALLOWLIST: [&str; 5] = ["PATH", "HOME", "USER", "TMPDIR", "LANG"];

#[derive(Default)]
pub struct Bash;

impl Bash {
    pub fn new() -> Self {
        Self
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
            description: "Run Bash in the session's working directory, falling back to the \
                          daemon's directory. The environment is scrubbed; daemon credentials \
                          are not inherited. Output keeps the first 4 KiB and the last 12 KiB \
                          of each stream, with a marker naming the bytes left out. \
                          If the call is cancelled or dropped, the whole process group is \
                          killed; a background job that redirects its output survives a \
                          normal return. Prefer narrow queries and plain output."
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
        ctx: TurnContext,
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

            if args.command.trim().is_empty() {
                return ToolReply::error(
                    "ERROR: command is empty. Pass a non-empty shell command to run.".to_owned(),
                );
            }

            let timeout_secs = args
                .timeout_secs
                .unwrap_or(DEFAULT_TIMEOUT_SECS)
                .clamp(MIN_TIMEOUT_SECS, MAX_TIMEOUT_SECS);

            let cwd = ctx
                .grants
                .as_ref()
                .and_then(|grants| grants.project_root().map(Path::to_path_buf))
                .map_or_else(std::env::current_dir, Ok);
            let cwd = match cwd {
                Ok(cwd) => cwd,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: could not determine working directory ({error})."
                    ));
                }
            };
            run(&args.command, &cwd, timeout_secs, &ctx.command_prefix).await
        })
    }
}

async fn run(command: &str, cwd: &Path, timeout_secs: u64, command_prefix: &[String]) -> ToolReply {
    if patches_via_bash(command) {
        tracing::warn!("the command starts apply_patch or applypatch; prefer the apply_patch tool");
    }
    let (program, mut cmd) = prefixed(
        cwd,
        command_prefix,
        &["bash", "--noprofile", "--norc", "-c", command],
    );
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    // own process group so a timeout kill reaches every grandchild.
    cmd.process_group(0);

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            return ToolReply::error(format!("ERROR: could not start {program} ({error})."));
        }
    };
    let pgid = child.id().and_then(|id| i32::try_from(id).ok());
    let mut group = KillGroup(pgid);
    let stdout_pipe = child.stdout.take().expect("stdout is piped");
    let stderr_pipe = child.stderr.take().expect("stderr is piped");
    let mut drains =
        tokio::spawn(async move { tokio::join!(drain(stdout_pipe), drain(stderr_pipe)) });

    let wait_result = tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait()).await;
    if wait_result.is_err() {
        kill_group(pgid);
        let _ = child.wait().await;
    }

    // Background children can hold pipes after bash exits.
    let (stdout, stderr) =
        if let Ok(outputs) = tokio::time::timeout(DRAIN_GRACE, &mut drains).await {
            outputs
        } else {
            kill_group(pgid);
            drains.await
        }
        .unwrap_or_default();
    group.disarm();
    match wait_result {
        Err(_) => {
            let header = format!("ERROR: timed out after {timeout_secs}s.");
            ToolReply::error(compose(Some(&header), &stdout, &stderr))
        }
        Ok(Ok(status)) => reply_for(status, &stdout, &stderr),
        Ok(Err(error)) => {
            ToolReply::error(format!("ERROR: bash did not run to completion ({error})."))
        }
    }
}

fn kill_group(pgid: Option<i32>) {
    if let Some(pgid) = pgid {
        // A negative PID signals the whole process group.
        unsafe {
            libc::kill(-pgid, libc::SIGKILL);
        }
    }
}

fn patches_via_bash(command: &str) -> bool {
    let mut rest = command.trim_start();
    if rest.starts_with("cd ") || rest.starts_with("cd\t") {
        match after_cd(rest) {
            Some(tail) => rest = tail.trim_start(),
            None => return false,
        }
    }
    matches!(head_word(rest), Some("apply_patch" | "applypatch"))
}

// `apply_patch<<EOF` and `apply_patch; true` name the same command
fn head_word(command: &str) -> Option<&str> {
    let end = command
        .find(|c: char| c.is_whitespace() || ";|&<>()".contains(c))
        .unwrap_or(command.len());
    (!command[..end].is_empty()).then(|| &command[..end])
}

fn after_cd(command: &str) -> Option<&str> {
    let mut quote = None;
    let mut chars = command.char_indices();
    while let Some((index, c)) = chars.next() {
        match (quote, c) {
            (None, '\'' | '"') => quote = Some(c),
            (Some(opened), c) if c == opened => quote = None,
            (None | Some('"'), '\\') => {
                chars.next();
            }
            (None, '&') if command[index..].starts_with("&&") => {
                return Some(&command[index + 2..]);
            }
            _ => {}
        }
    }
    None
}

struct KillGroup(Option<i32>);

impl KillGroup {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for KillGroup {
    fn drop(&mut self) {
        kill_group(self.0);
    }
}

// the project's wrapper around argv, with the environment every child tool gets
pub(crate) fn prefixed<'a>(
    cwd: &Path,
    command_prefix: &'a [String],
    argv: &[&'a str],
) -> (&'a str, Command) {
    let program = command_prefix.first().map_or(argv[0], String::as_str);
    let mut cmd = Command::new(program);
    if command_prefix.is_empty() {
        cmd.args(&argv[1..]);
    } else {
        cmd.args(&command_prefix[1..]).args(argv);
    }
    cmd.current_dir(cwd);
    scrub_env(&mut cmd);
    // CLIs block on an open stdin.
    cmd.stdin(Stdio::null());
    (program, cmd)
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
    cmd.env("NO_COLOR", "1");
    cmd.env("CLICOLOR", "0");
    cmd.env("TERM", "dumb");
}

#[derive(Default)]
struct Captured {
    text: String,
}

// not str::is_char_boundary: the buffer as a whole may not be valid UTF-8
// (binary output, or a multi-byte char split across a chunk), so this only
// asks whether `byte` could start a new UTF-8 sequence.
fn starts_a_char(byte: u8) -> bool {
    byte & 0b1100_0000 != 0b1000_0000
}

async fn drain(mut reader: impl tokio::io::AsyncRead + Unpin + Send + 'static) -> Captured {
    let mut head: Vec<u8> = Vec::new();
    let mut tail: Vec<u8> = Vec::new();
    let mut total: usize = 0;
    let mut chunk = [0u8; 8192];
    loop {
        let n = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        total += n;
        let room = HEAD_BYTES.saturating_sub(head.len());
        head.extend_from_slice(&chunk[..n.min(room)]);
        tail.extend_from_slice(&chunk[..n]);
        if tail.len() > TAIL_BYTES {
            let overflow = tail.len() - TAIL_BYTES;
            tail.drain(..overflow);
        }
    }

    let tail_start = total - tail.len();
    if tail_start <= head.len() {
        let overlap = head.len() - tail_start;
        let mut bytes = head;
        bytes.extend_from_slice(&tail[overlap..]);
        return Captured {
            text: decode(&bytes),
        };
    }

    let head_end = intact_len(&head);
    let tail_skip = (0..tail.len())
        .find(|&i| starts_a_char(tail[i]))
        .unwrap_or(tail.len());
    let omitted = tail_start + tail_skip - head_end;
    Captured {
        text: format!(
            "{}\n[{omitted} bytes omitted]\n{}",
            decode(&head[..head_end]),
            decode(&tail[tail_skip..])
        ),
    }
}

fn intact_len(bytes: &[u8]) -> usize {
    match std::str::from_utf8(bytes) {
        Ok(_) => bytes.len(),
        Err(error) => error.valid_up_to(),
    }
}

fn decode(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_owned(),
        Err(error) => std::str::from_utf8(&bytes[..error.valid_up_to()])
            .expect("valid_up_to bounds valid utf8")
            .to_owned(),
    }
}

fn reply_for(status: std::process::ExitStatus, stdout: &Captured, stderr: &Captured) -> ToolReply {
    let code = status.code();
    let content = if code == Some(0) && stderr.text.is_empty() {
        if stdout.text.is_empty() {
            "(no output)".to_owned()
        } else {
            stdout.text.clone()
        }
    } else {
        let header = match code {
            Some(0) => None,
            Some(code) => Some(format!("exit {code}")),
            None => Some("exit signal".to_owned()),
        };
        compose(header.as_deref(), stdout, stderr)
    };
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
    parts.push(stdout.text.clone());
    if !stderr.text.is_empty() {
        parts.push("--- stderr ---".to_owned());
        parts.push(stderr.text.clone());
    }
    parts.join("\n")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tempfile::TempDir;

    use super::Bash;
    use crate::testkit::WarningCapture;
    use crate::tool::workspace::{Grant, Grants, Mode};
    use crate::tool::{Tool as _, TurnContext};

    fn ctx(root: &std::path::Path, mode: Mode) -> TurnContext {
        ctx_with_prefix(root, mode, Vec::new())
    }

    fn ctx_with_prefix(
        root: &std::path::Path,
        mode: Mode,
        command_prefix: Vec<String>,
    ) -> TurnContext {
        let grants = Grants::new(vec![Grant::new(root, mode)]).expect("grants");
        TurnContext {
            session_id: String::new(),
            turn_id: String::new(),
            grants: Some(Arc::new(grants)),
            command_prefix,
        }
    }

    fn ctx_rw(root: &std::path::Path) -> TurnContext {
        ctx(root, Mode::ReadWrite)
    }

    fn ctx_rw_with_prefix(root: &std::path::Path, command_prefix: Vec<String>) -> TurnContext {
        ctx_with_prefix(root, Mode::ReadWrite, command_prefix)
    }

    fn args(command: &str) -> String {
        serde_json::json!({ "command": command }).to_string()
    }

    fn args_with_timeout(command: &str, timeout_secs: u64) -> String {
        serde_json::json!({ "command": command, "timeout_secs": timeout_secs }).to_string()
    }

    // Process names may contain parentheses.
    fn process_alive(pid: &str) -> bool {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .ok()
            .and_then(|stat| {
                stat.rsplit(')')
                    .next()
                    .and_then(|rest| rest.split_whitespace().next().map(str::to_owned))
            })
            .is_some_and(|state| state != "Z")
    }

    fn kill(pid: &str) {
        let _ = std::process::Command::new("kill")
            .args(["-9", pid])
            .status();
    }

    #[tokio::test]
    async fn an_echo_command_returns_its_stdout_verbatim() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();

        let reply = tool.execute(args("echo hello"), ctx_rw(dir.path())).await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.content, "hello\n");
    }

    #[tokio::test]
    async fn a_nonzero_exit_is_an_error_naming_the_code_with_both_streams() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();

        let reply = tool
            .execute(args("echo out; echo err >&2; exit 3"), ctx_rw(dir.path()))
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
    async fn the_environment_is_scrubbed_of_everything_but_the_allowlist() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();
        unsafe {
            std::env::set_var("ARC_TEST_SECRET", "shh");
        }

        let reply = tool.execute(args("env"), ctx_rw(dir.path())).await;

        assert!(reply.ok, "{}", reply.content);
        assert!(
            !reply.content.contains("ARC_TEST_SECRET"),
            "{}",
            reply.content
        );
        assert!(reply.content.contains("HOME="), "{}", reply.content);
        for setting in ["NO_COLOR=1", "CLICOLOR=0", "TERM=dumb"] {
            assert!(
                reply.content.lines().any(|line| line == setting),
                "missing {setting}: {}",
                reply.content
            );
        }
    }

    #[tokio::test]
    async fn xdg_prefixed_variables_pass_through() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();
        unsafe {
            std::env::set_var("XDG_ARC_TEST", "1");
        }

        let reply = tool.execute(args("env"), ctx_rw(dir.path())).await;

        assert!(reply.ok, "{}", reply.content);
        assert!(
            reply.content.contains("XDG_ARC_TEST=1"),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn truncation_keeps_the_tail_so_a_trailing_error_survives() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();

        let reply = tool
            .execute(
                args("head -c 20000 /dev/zero | tr '\\0' 'x'; echo; echo ERROR: something broke"),
                ctx_rw(dir.path()),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert!(reply.content.contains("bytes omitted"), "{}", reply.content);
        assert!(
            reply.content.trim_end().ends_with("ERROR: something broke"),
            "the tail survives truncation: {}",
            reply.content
        );
    }

    #[tokio::test]
    async fn an_error_at_the_start_survives_a_large_output() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();

        let reply = tool
            .execute(
                args("echo ERROR: at the start; head -c 20000 /dev/zero | tr '\\0' 'y'; echo"),
                ctx_rw(dir.path()),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert!(
            reply.content.starts_with("ERROR: at the start\n"),
            "the head survives truncation: {}",
            reply.content
        );
        assert!(reply.content.contains("bytes omitted"), "{}", reply.content);
        assert!(reply.content.trim_end().ends_with('y'), "{}", reply.content);
    }

    async fn drained(input: Vec<u8>) -> String {
        super::drain(Box::leak(input.into_boxed_slice()) as &[u8])
            .await
            .text
    }

    #[tokio::test]
    async fn drain_never_splits_a_multibyte_character_at_either_cut() {
        // the accent straddles the head's cut at 4096
        let mut input = vec![b'a'; 4095];
        input.extend_from_slice("é".repeat(9_999).as_bytes());
        let total = input.len();

        let text = drained(input).await;

        assert!(!text.contains('\u{FFFD}'), "{text:?}");
        let mut lines = text.split('\n');
        let head_line = lines.next().expect("a head");
        assert_eq!(head_line.len(), 4095, "the head stops on a char boundary");
        assert!(head_line.chars().all(|c| c == 'a'), "{head_line:?}");
        let marker = lines.next().expect("a marker");
        let omitted: usize = marker
            .strip_prefix('[')
            .and_then(|it| it.strip_suffix(" bytes omitted]"))
            .expect("a count")
            .parse()
            .expect("a number");
        let tail: String = lines.collect::<Vec<_>>().join("\n");
        assert!(
            tail.chars().all(|c| c == 'é'),
            "the tail starts on a char boundary"
        );
        assert!(
            !tail.is_empty() && head_line.len() + omitted + tail.len() == total,
            "{marker} does not account for {total} bytes"
        );
    }

    #[tokio::test]
    async fn an_output_within_the_budget_is_whole_and_unmarked() {
        for size in [16 * 1024, 15 * 1024, 12 * 1024 + 1, 1024] {
            let input = vec![b'z'; size];
            let text = drained(input.clone()).await;
            assert_eq!(text.len(), size, "{size} bytes come back whole");
            assert_eq!(text.as_bytes(), input.as_slice());
            assert!(!text.contains("omitted"), "no marker at {size}");
        }
    }

    #[tokio::test]
    async fn an_output_within_the_budget_survives_a_character_across_the_head_cut() {
        let mut input = vec![b'a'; 4095];
        input.extend_from_slice("é".as_bytes());
        input.resize(16 * 1024, b'b');
        let expected = input.clone();

        let text = drained(input).await;

        let whole = text.as_bytes() == expected.as_slice();
        assert!(
            whole,
            "the whole stream comes back unharmed; got {} of {} bytes",
            text.len(),
            expected.len()
        );
    }

    #[tokio::test]
    async fn undecodable_output_still_reports_no_output() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();

        let reply = tool
            .execute(args("printf '\\xff'"), ctx_rw(dir.path()))
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.content, "(no output)");
    }

    #[tokio::test]
    async fn a_command_that_runs_apply_patch_through_bash_is_warned_about() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();
        let capture = WarningCapture::start();

        for command in [
            "apply_patch",
            "applypatch < patch.txt",
            "cd '/a && b' && apply_patch",
            "cd \"/a b\" && applypatch < patch.txt",
            "apply_patch<<EOF",
            "apply_patch; true",
            "apply_patch&&true",
            "cd /tmp && applypatch<<EOF",
            "cd \"/a\\\" && b\" && apply_patch",
        ] {
            tool.execute(args(command), ctx_rw(dir.path())).await;
        }

        let warnings = capture.warnings();
        assert_eq!(
            warnings.matches("prefer the apply_patch tool").count(),
            9,
            "{warnings}"
        );
    }

    #[tokio::test]
    async fn a_command_that_only_mentions_apply_patch_is_not_warned_about() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();
        let capture = WarningCapture::start();

        for command in [
            "echo apply_patch",
            "echo 'cd /tmp && applypatch'",
            "applypatch_extra",
            "cd \"x\\\" && apply_patch && echo hi\"",
        ] {
            tool.execute(args(command), ctx_rw(dir.path())).await;
        }

        assert!(capture.warnings().is_empty(), "{}", capture.warnings());
    }

    #[tokio::test]
    async fn a_timeout_kills_the_whole_process_group_and_returns_promptly() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();

        let start = Instant::now();
        let reply = tool
            .execute(
                args_with_timeout("sleep 5; echo late", 1),
                ctx_rw(dir.path()),
            )
            .await;
        let elapsed = start.elapsed();

        assert!(!reply.ok);
        assert!(
            reply.content.starts_with("ERROR: timed out after 1s."),
            "{}",
            reply.content
        );
        assert!(!reply.content.contains("late"), "{}", reply.content);
        assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    }

    #[tokio::test]
    async fn a_read_only_root_is_still_where_bash_runs() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();

        let reply = tool
            .execute(args("pwd"), ctx(dir.path(), Mode::ReadOnly))
            .await;

        assert!(reply.ok, "{}", reply.content);
        let canonical = dir.path().canonicalize().expect("canonicalize");
        assert_eq!(reply.content.trim_end(), canonical.to_str().expect("utf8"));
    }

    #[tokio::test]
    async fn an_unbound_session_runs_in_the_daemons_current_directory() {
        let tool = Bash::new();

        let reply = tool.execute(args("pwd"), TurnContext::default()).await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(
            reply.content.trim_end(),
            std::env::current_dir()
                .unwrap()
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn a_background_child_holding_the_pipe_does_not_hang_the_call() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();
        for command in ["sleep 30 & echo up", "sleep 30 >&2 & echo up"] {
            let started = Instant::now();
            let reply = tool.execute(args(command), ctx_rw(dir.path())).await;
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the call must return once bash exits, not when the orphan dies"
            );
            assert!(reply.ok, "{}", reply.content);
            assert!(reply.content.contains("up"), "{}", reply.content);
        }
    }

    #[tokio::test]
    async fn a_dropped_call_kills_its_process_group() {
        let dir = TempDir::new().expect("tmp");
        let pid_file = dir.path().join("pid");
        let command = format!("echo $$ > {}; exec sleep 30", pid_file.display());
        let tool = Bash::new();

        let _ = tokio::time::timeout(
            Duration::from_millis(500),
            tool.execute(args(&command), ctx_rw(dir.path())),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let pid = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .to_owned();
        let alive = process_alive(&pid);
        if alive {
            kill(&pid);
        }
        assert!(!alive, "sleep {pid} outlived the dropped call");
    }

    #[tokio::test]
    async fn a_redirected_background_job_survives_a_normal_return() {
        let dir = TempDir::new().expect("tmp");
        let pid_file = dir.path().join("pid");
        let command = format!(
            "nohup sleep 30 > /dev/null 2>&1 & echo $! > {}",
            pid_file.display()
        );
        let tool = Bash::new();

        let reply = tool.execute(args(&command), ctx_rw(dir.path())).await;

        let pid = std::fs::read_to_string(&pid_file)
            .expect("the background job wrote its pid")
            .trim()
            .to_owned();
        let alive = process_alive(&pid);
        kill(&pid);
        assert!(reply.ok, "{}", reply.content);
        assert!(alive, "sleep {pid} did not survive a normal return");
    }

    #[tokio::test]
    async fn a_prefix_wraps_the_child_and_produces_the_same_output() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();

        let reply = tool
            .execute(
                args("echo hello"),
                ctx_rw_with_prefix(dir.path(), vec!["env".to_owned()]),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.content, "hello\n");
    }

    #[tokio::test]
    async fn a_timeout_kills_a_sleeping_child_under_a_prefix() {
        let dir = TempDir::new().expect("tmp");
        let tool = Bash::new();

        let start = Instant::now();
        let reply = tool
            .execute(
                args_with_timeout("sleep 5; echo late", 1),
                ctx_rw_with_prefix(dir.path(), vec!["env".to_owned()]),
            )
            .await;
        let elapsed = start.elapsed();

        assert!(!reply.ok);
        assert!(!reply.content.contains("late"), "{}", reply.content);
        assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    }
}
