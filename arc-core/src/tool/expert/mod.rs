use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::provider::ToolDefinition;
use crate::tool::{Tool, ToolReply, ToolSource, TurnContext};

const TIMEOUT: Duration = Duration::from_secs(600);
const DRAIN_GRACE: Duration = Duration::from_millis(500);
const MAX_CAPTURE_BYTES: usize = 1024 * 1024;

/// What `[roles.counsel]` resolves to: a command template, never a
/// `Provider` — the CLI runs its own read-only loop over the workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CounselSpec {
    pub command: CounselCommand,
    pub model: String,
    pub fallback_model: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounselCommand {
    Claude,
    Codex,
}

impl CounselCommand {
    fn label(self) -> &'static str {
        match self {
            CounselCommand::Claude => "claude",
            CounselCommand::Codex => "codex",
        }
    }
}

/// `consult_expert`: spawns a read-only `claude -p` or `codex exec` over a
/// project root and returns its answer. Constructed with the resolved
/// `[roles.counsel]` spec and the same project map dispatch gets.
pub struct Expert {
    spec: CounselSpec,
    projects: Vec<(String, PathBuf)>,
}

impl Expert {
    pub fn new(spec: CounselSpec, mut projects: Vec<(String, PathBuf)>) -> Self {
        projects.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        Self { spec, projects }
    }

    fn names_joined(&self) -> String {
        self.projects
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn project_description(&self) -> String {
        format!(
            "The configured project the expert reads: {}. \"none\" reads this session's own \
             workspace when it is bound to one; an unbound session must name one of the \
             configured projects instead.",
            self.names_joined()
        )
    }

    // an early-return-on-error reads best here; ToolReply is not on a hot path
    #[allow(clippy::result_large_err)]
    fn resolve_root(&self, project: &str, ctx: &TurnContext) -> Result<PathBuf, ToolReply> {
        let bound_root = ctx.grants.as_ref().and_then(|grants| grants.project_root());
        if project == "none" {
            return bound_root.map(Path::to_path_buf).ok_or_else(|| {
                ToolReply::error(format!(
                    "ERROR: this session has no workspace of its own. Name one of the \
                     configured projects instead: {}.",
                    self.names_joined()
                ))
            });
        }
        let Some((_, configured_root)) = self.projects.iter().find(|(name, _)| name == project)
        else {
            return Err(ToolReply::error(format!(
                "ERROR: unknown project {project:?}. Use one of the configured projects ({}), \
                 or \"none\" for this session's own workspace.",
                self.names_joined()
            )));
        };
        if let Some(bound_root) = bound_root {
            if !same_root(bound_root, configured_root) {
                return Err(ToolReply::error(format!(
                    "ERROR: this session is bound to its own workspace; consult_expert reads \
                     the caller's workspace, not {project:?}. Use \"none\" instead."
                )));
            }
        }
        Ok(configured_root.clone())
    }
}

fn same_root(a: &Path, b: &Path) -> bool {
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

#[derive(Deserialize)]
struct ExpertArgs {
    question: String,
    project: String,
}

impl Tool for Expert {
    fn definition(&self) -> ToolDefinition {
        let mut project_enum: Vec<String> =
            self.projects.iter().map(|(name, _)| name.clone()).collect();
        project_enum.push("none".to_owned());
        ToolDefinition {
            name: "consult_expert".to_owned(),
            description: "Consult a stronger, read-only expert over a project's workspace — \
                          for a plan, a review, or when stuck. Use it sparingly: a few calls \
                          per job at most. The expert reads the workspace itself, so the \
                          question should say what to look at, not paste file contents."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "What to ask the expert. Say what to look at — it \
                            reads the workspace itself."
                    },
                    "project": {
                        "type": "string",
                        "enum": project_enum,
                        "description": self.project_description(),
                    },
                },
                "required": ["question", "project"]
            }),
        }
    }

    fn source(&self) -> ToolSource {
        ToolSource::Expert
    }

    fn execute(
        &self,
        arguments_json: String,
        ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async move {
            let args: ExpertArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad consult_expert arguments ({error}). Pass question and \
                         project."
                    ));
                }
            };
            if args.question.trim().is_empty() {
                return ToolReply::error(
                    "ERROR: question must not be empty. Say what the expert should look at."
                        .to_owned(),
                );
            }
            let root = match self.resolve_root(&args.project, &ctx) {
                Ok(root) => root,
                Err(reply) => return reply,
            };
            run(&self.spec, &args.question, &root, TIMEOUT).await
        })
    }
}

async fn run(spec: &CounselSpec, question: &str, root: &Path, timeout: Duration) -> ToolReply {
    let start = std::time::Instant::now();
    let reply = match spec.command {
        CounselCommand::Claude => run_claude(spec, question, root, timeout).await,
        CounselCommand::Codex => run_codex(question, root, timeout).await,
    };
    tracing::info!(
        command = spec.command.label(),
        latency_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
        ok = reply.ok,
        "consult_expert finished"
    );
    reply
}

fn claude_argv(spec: &CounselSpec, question: &str) -> Vec<String> {
    let mut argv = vec![
        "claude".to_owned(),
        "-p".to_owned(),
        question.to_owned(),
        "--model".to_owned(),
        spec.model.clone(),
    ];
    if let Some(fallback) = &spec.fallback_model {
        argv.push("--fallback-model".to_owned());
        argv.push(fallback.clone());
    }
    argv.extend(
        [
            "--tools",
            "Read,Glob,Grep",
            "--strict-mcp-config",
            "--setting-sources",
            "",
            "--permission-mode",
            "manual",
            "--no-session-persistence",
            "--output-format",
            "json",
        ]
        .map(str::to_owned),
    );
    argv
}

fn codex_argv(question: &str, root: &Path, result_file: &Path) -> Vec<String> {
    vec![
        "codex".to_owned(),
        "exec".to_owned(),
        question.to_owned(),
        "-s".to_owned(),
        "read-only".to_owned(),
        "--json".to_owned(),
        "--ephemeral".to_owned(),
        "-C".to_owned(),
        root.to_string_lossy().into_owned(),
        "-o".to_owned(),
        result_file.to_string_lossy().into_owned(),
    ]
}

async fn run_claude(
    spec: &CounselSpec,
    question: &str,
    root: &Path,
    timeout: Duration,
) -> ToolReply {
    let argv = claude_argv(spec, question);
    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.current_dir(root);
    // deliberate exception to bash's env scrub: subscription auth lives in HOME/keychain
    match spawn_capture(cmd, timeout).await {
        RunOutcome::Finished {
            status,
            stdout,
            stderr,
        } => claude_reply(status, &stdout, &stderr),
        RunOutcome::TimedOut => timeout_error(timeout),
        RunOutcome::SpawnFailed(error) => {
            ToolReply::error(format!("ERROR: could not start claude ({error})."))
        }
    }
}

async fn run_codex(question: &str, root: &Path, timeout: Duration) -> ToolReply {
    let temp = match tempfile::tempdir() {
        Ok(temp) => temp,
        Err(error) => {
            return ToolReply::error(format!(
                "ERROR: could not create a temp dir for the expert's result ({error})."
            ));
        }
    };
    let result_file = temp.path().join("result.json");
    let argv = codex_argv(question, root, &result_file);
    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.current_dir(root);
    // deliberate exception to bash's env scrub: subscription auth lives in HOME/keychain
    // codex runs its commands through `zsh -lc`, sourcing the user's profile; the given
    // argv carries no flag to pin shell_environment_policy, so that residue stands
    match spawn_capture(cmd, timeout).await {
        RunOutcome::Finished {
            status,
            stdout: _,
            stderr,
        } => {
            if !status.success() {
                return exit_error("codex", status, &stderr);
            }
            match std::fs::read_to_string(&result_file) {
                Ok(text) => ToolReply::ok(text),
                Err(error) => ToolReply::error(format!(
                    "ERROR: codex exited cleanly but its result file could not be read ({error})."
                )),
            }
        }
        RunOutcome::TimedOut => timeout_error(timeout),
        RunOutcome::SpawnFailed(error) => {
            ToolReply::error(format!("ERROR: could not start codex ({error})."))
        }
    }
}

fn timeout_error(timeout: Duration) -> ToolReply {
    ToolReply::error(format!(
        "ERROR: the expert timed out after {}s.",
        timeout.as_secs()
    ))
}

fn claude_reply(status: ExitStatus, stdout: &Captured, stderr: &Captured) -> ToolReply {
    if !status.success() {
        return exit_error("claude", status, stderr);
    }
    let Ok(envelope) = serde_json::from_str::<serde_json::Value>(&stdout.text) else {
        return ToolReply::ok(format!(
            "[consult_expert: claude's output did not parse as JSON; returning it raw]\n{}",
            mark(&stdout.text, stdout.dropped)
        ));
    };
    let usage = envelope.get("usage");
    let cost_usd = envelope
        .get("total_cost_usd")
        .and_then(serde_json::Value::as_f64);
    let input_tokens = usage
        .and_then(|usage| usage.get("input_tokens"))
        .and_then(serde_json::Value::as_u64);
    let output_tokens = usage
        .and_then(|usage| usage.get("output_tokens"))
        .and_then(serde_json::Value::as_u64);
    if cost_usd.is_some() || input_tokens.is_some() || output_tokens.is_some() {
        tracing::info!(
            cost_usd = ?cost_usd,
            input_tokens = ?input_tokens,
            output_tokens = ?output_tokens,
            "consult_expert usage"
        );
    }
    let content = envelope
        .get("result")
        .and_then(serde_json::Value::as_str)
        .map_or_else(|| mark(&stdout.text, stdout.dropped), str::to_owned);
    ToolReply::ok(content)
}

fn exit_error(program: &str, status: ExitStatus, stderr: &Captured) -> ToolReply {
    let code = status
        .code()
        .map_or_else(|| "by signal".to_owned(), |code| code.to_string());
    let tail = if stderr.text.is_empty() {
        String::new()
    } else {
        format!("\n{}", mark(&stderr.text, stderr.dropped))
    };
    ToolReply::error(format!("ERROR: {program} exited {code}.{tail}"))
}

fn mark(text: &str, dropped: usize) -> String {
    if dropped > 0 {
        format!("[first {dropped} bytes dropped]\n{text}")
    } else {
        text.to_owned()
    }
}

enum RunOutcome {
    Finished {
        status: ExitStatus,
        stdout: Captured,
        stderr: Captured,
    },
    TimedOut,
    SpawnFailed(String),
}

async fn spawn_capture(mut cmd: Command, timeout: Duration) -> RunOutcome {
    // CLIs block on an open stdin.
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    // own process group so a timeout kill reaches every grandchild.
    cmd.process_group(0);

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => return RunOutcome::SpawnFailed(error.to_string()),
    };
    let pgid = child.id().and_then(|id| i32::try_from(id).ok());
    let stdout_pipe = child.stdout.take().expect("stdout is piped");
    let stderr_pipe = child.stderr.take().expect("stderr is piped");
    let mut stdout_task = tokio::spawn(drain(stdout_pipe));
    let mut stderr_task = tokio::spawn(drain(stderr_pipe));

    let Ok(wait_result) = tokio::time::timeout(timeout, child.wait()).await else {
        kill_group(pgid);
        let _ = child.wait().await;
        let _ = stdout_task.await;
        let _ = stderr_task.await;
        return RunOutcome::TimedOut;
    };

    // a background child can hold the pipes open past the parent's own exit; after a
    // short grace the group is killed so EOF arrives and captured bytes survive.
    let drains = async { ((&mut stdout_task).await, (&mut stderr_task).await) };
    let (stdout, stderr) =
        if let Ok((stdout, stderr)) = tokio::time::timeout(DRAIN_GRACE, drains).await {
            (stdout.unwrap_or_default(), stderr.unwrap_or_default())
        } else {
            kill_group(pgid);
            (
                stdout_task.await.unwrap_or_default(),
                stderr_task.await.unwrap_or_default(),
            )
        };

    match wait_result {
        Ok(status) => RunOutcome::Finished {
            status,
            stdout,
            stderr,
        },
        Err(error) => RunOutcome::SpawnFailed(format!("did not run to completion ({error})")),
    }
}

fn kill_group(pgid: Option<i32>) {
    if let Some(pgid) = pgid {
        // negative pid signals the whole process group.
        unsafe {
            libc::kill(-pgid, libc::SIGKILL);
        }
    }
}

#[derive(Default)]
struct Captured {
    text: String,
    dropped: usize,
}

// not str::is_char_boundary: the buffer as a whole may not be valid UTF-8
// (binary output, or a multi-byte char split across a chunk), so this only
// asks whether `byte` could start a new UTF-8 sequence.
fn starts_a_char(byte: u8) -> bool {
    byte & 0b1100_0000 != 0b1000_0000
}

async fn drain(mut reader: impl tokio::io::AsyncRead + Unpin + Send + 'static) -> Captured {
    let mut buf: Vec<u8> = Vec::new();
    let mut dropped: usize = 0;
    let mut chunk = [0u8; 8192];
    loop {
        let n = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > MAX_CAPTURE_BYTES {
            let overflow = buf.len() - MAX_CAPTURE_BYTES;
            let cut = (overflow..buf.len())
                .find(|&i| starts_a_char(buf[i]))
                .unwrap_or(buf.len());
            dropped += cut;
            buf.drain(..cut);
        }
    }
    let text = match std::str::from_utf8(&buf) {
        Ok(text) => text.to_owned(),
        Err(error) => std::str::from_utf8(&buf[..error.valid_up_to()])
            .expect("valid_up_to bounds valid utf8")
            .to_owned(),
    };
    Captured { text, dropped }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tempfile::TempDir;

    use super::{
        CounselCommand, CounselSpec, Expert, claude_argv, codex_argv, run_claude, run_codex,
    };
    use crate::tool::workspace::{Grant, Grants, Mode};
    use crate::tool::{Tool as _, ToolSource, TurnContext};

    fn spec(command: CounselCommand, fallback: Option<&str>) -> CounselSpec {
        CounselSpec {
            command,
            model: "opus".to_owned(),
            fallback_model: fallback.map(str::to_owned),
        }
    }

    fn args(question: &str, project: &str) -> String {
        serde_json::json!({ "question": question, "project": project }).to_string()
    }

    fn ctx_bound(root: &std::path::Path) -> TurnContext {
        let grants = Grants::new(vec![Grant::new(root, Mode::ReadWrite)]).expect("grants");
        TurnContext {
            session_id: String::new(),
            turn_id: String::new(),
            grants: Some(Arc::new(grants)),
            command_prefix: Vec::new(),
        }
    }

    // --- argv golden tests ---

    #[test]
    fn claude_argv_carries_the_load_bearing_flags() {
        let argv = claude_argv(&spec(CounselCommand::Claude, None), "what is broken?");
        assert_eq!(
            argv,
            [
                "claude",
                "-p",
                "what is broken?",
                "--model",
                "opus",
                "--tools",
                "Read,Glob,Grep",
                "--strict-mcp-config",
                "--setting-sources",
                "",
                "--permission-mode",
                "manual",
                "--no-session-persistence",
                "--output-format",
                "json",
            ]
        );
    }

    #[test]
    fn claude_argv_includes_the_fallback_model_only_when_set() {
        let argv = claude_argv(&spec(CounselCommand::Claude, Some("sonnet")), "q");
        assert_eq!(
            &argv[5..7],
            ["--fallback-model", "sonnet"],
            "fallback lands right after --model: {argv:?}"
        );

        let without = claude_argv(&spec(CounselCommand::Claude, None), "q");
        assert!(
            !without.contains(&"--fallback-model".to_owned()),
            "{without:?}"
        );
    }

    #[test]
    fn codex_argv_carries_the_load_bearing_flags() {
        let root = std::path::Path::new("/tmp/proj");
        let result_file = std::path::Path::new("/tmp/scratch/result.json");
        let argv = codex_argv("what is broken?", root, result_file);
        assert_eq!(
            argv,
            [
                "codex",
                "exec",
                "what is broken?",
                "-s",
                "read-only",
                "--json",
                "--ephemeral",
                "-C",
                "/tmp/proj",
                "-o",
                "/tmp/scratch/result.json",
            ]
        );
    }

    // --- project resolution ---

    // `ToolReply` carries no `Debug`, so `.expect()` on the `Err` side won't compile
    fn ok_root(result: Result<std::path::PathBuf, crate::tool::ToolReply>) -> std::path::PathBuf {
        match result {
            Ok(root) => root,
            Err(reply) => panic!("expected a resolved root, got: {}", reply.content),
        }
    }

    #[test]
    fn none_in_a_bound_session_resolves_the_session_root() {
        let dir = TempDir::new().expect("tmp");
        let tool = Expert::new(
            spec(CounselCommand::Claude, None),
            vec![("arc".to_owned(), dir.path().to_path_buf())],
        );

        let root = ok_root(tool.resolve_root("none", &ctx_bound(dir.path())));
        assert_eq!(root, dir.path().canonicalize().expect("canon"));
    }

    #[test]
    fn none_in_an_unbound_session_names_the_configured_projects() {
        let tool = Expert::new(
            spec(CounselCommand::Claude, None),
            vec![("arc".to_owned(), std::path::PathBuf::from("/tmp/arc"))],
        );

        let error = tool
            .resolve_root("none", &TurnContext::default())
            .unwrap_err();
        assert!(!error.ok);
        assert!(error.content.contains("arc"), "{}", error.content);
    }

    #[test]
    fn a_bound_session_naming_a_foreign_project_is_an_error() {
        let dir = TempDir::new().expect("tmp");
        let other = TempDir::new().expect("tmp2");
        let tool = Expert::new(
            spec(CounselCommand::Claude, None),
            vec![
                ("mine".to_owned(), dir.path().to_path_buf()),
                ("theirs".to_owned(), other.path().to_path_buf()),
            ],
        );

        let error = tool
            .resolve_root("theirs", &ctx_bound(dir.path()))
            .unwrap_err();
        assert!(!error.ok);
        assert!(error.content.contains("theirs"), "{}", error.content);
    }

    #[test]
    fn a_bound_session_naming_its_own_project_resolves() {
        let dir = TempDir::new().expect("tmp");
        let tool = Expert::new(
            spec(CounselCommand::Claude, None),
            vec![("mine".to_owned(), dir.path().to_path_buf())],
        );

        let root = ok_root(tool.resolve_root("mine", &ctx_bound(dir.path())));
        assert_eq!(root, dir.path().to_path_buf());
    }

    #[test]
    fn an_unbound_session_can_name_a_configured_project_directly() {
        let dir = TempDir::new().expect("tmp");
        let tool = Expert::new(
            spec(CounselCommand::Claude, None),
            vec![("arc".to_owned(), dir.path().to_path_buf())],
        );

        let root = ok_root(tool.resolve_root("arc", &TurnContext::default()));
        assert_eq!(root, dir.path().to_path_buf());
    }

    #[test]
    fn an_unknown_project_name_is_an_error() {
        let tool = Expert::new(
            spec(CounselCommand::Claude, None),
            vec![("arc".to_owned(), std::path::PathBuf::from("/tmp/arc"))],
        );

        let error = tool
            .resolve_root("ghost", &TurnContext::default())
            .unwrap_err();
        assert!(!error.ok);
        assert!(error.content.contains("ghost"), "{}", error.content);
    }

    // --- tool-level argument validation ---

    #[tokio::test]
    async fn an_empty_question_is_an_error() {
        let tool = Expert::new(spec(CounselCommand::Claude, None), vec![]);

        let reply = tool
            .execute(args("   ", "none"), TurnContext::default())
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("question"), "{}", reply.content);
    }

    #[tokio::test]
    async fn bad_json_arguments_are_an_actionable_error() {
        let tool = Expert::new(spec(CounselCommand::Claude, None), vec![]);

        let reply = tool
            .execute("not json".to_owned(), TurnContext::default())
            .await;

        assert!(!reply.ok);
        assert!(
            reply.content.contains("consult_expert"),
            "{}",
            reply.content
        );
    }

    #[test]
    fn the_definition_requires_question_and_project_and_carries_the_escape_value() {
        let tool = Expert::new(
            spec(CounselCommand::Claude, None),
            vec![("arc".to_owned(), std::path::PathBuf::from("/tmp/arc"))],
        );

        let definition = tool.definition();
        assert_eq!(definition.name, "consult_expert");
        assert_eq!(
            definition.parameters["required"],
            serde_json::json!(["question", "project"])
        );
        let project_enum = definition.parameters["properties"]["project"]["enum"]
            .as_array()
            .expect("project enum")
            .iter()
            .map(|v| v.as_str().expect("string").to_owned())
            .collect::<Vec<_>>();
        assert_eq!(project_enum, ["arc", "none"]);
    }

    #[test]
    fn the_tool_source_is_expert() {
        let tool = Expert::new(spec(CounselCommand::Claude, None), vec![]);
        assert_eq!(tool.source(), ToolSource::Expert);
    }

    // --- stub subprocess runs ---

    fn stub(dir: &TempDir, name: &str, script: &str) {
        let path = dir.path().join(name);
        std::fs::write(&path, script).expect("write stub");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }

    // every stub test spawns "claude" or "codex" by bare name, resolved through
    // PATH; this serializes them so concurrent tests cannot see each other's stub
    static PATH_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct PathGuard {
        original: Option<String>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl PathGuard {
        fn set(dir: &TempDir) -> Self {
            let lock = PATH_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let original = std::env::var("PATH").ok();
            let joined = match &original {
                Some(existing) => format!("{}:{existing}", dir.path().display()),
                None => dir.path().display().to_string(),
            };
            // SAFETY: serialized by PATH_LOCK, restored on drop
            unsafe { std::env::set_var("PATH", joined) };
            Self {
                original,
                _lock: lock,
            }
        }
    }

    impl Drop for PathGuard {
        fn drop(&mut self) {
            // SAFETY: serialized by PATH_LOCK
            unsafe {
                match &self.original {
                    Some(value) => std::env::set_var("PATH", value),
                    None => std::env::remove_var("PATH"),
                }
            }
        }
    }

    #[tokio::test]
    async fn a_canned_claude_envelope_becomes_the_reply_text() {
        let stub_dir = TempDir::new().expect("tmp");
        stub(
            &stub_dir,
            "claude",
            "#!/bin/sh\necho '{\"result\":\"looks fine\",\"total_cost_usd\":0.05,\"usage\":{\"input_tokens\":10,\"output_tokens\":20}}'\n",
        );
        let _path_guard = PathGuard::set(&stub_dir);
        let root = TempDir::new().expect("root");

        let reply = run_claude(
            &spec(CounselCommand::Claude, None),
            "what is broken?",
            root.path(),
            Duration::from_secs(5),
        )
        .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.content, "looks fine");
    }

    #[tokio::test]
    async fn unparseable_claude_stdout_returns_it_raw_with_a_note() {
        let stub_dir = TempDir::new().expect("tmp");
        stub(&stub_dir, "claude", "#!/bin/sh\necho 'not json at all'\n");
        let _path_guard = PathGuard::set(&stub_dir);
        let root = TempDir::new().expect("root");

        let reply = run_claude(
            &spec(CounselCommand::Claude, None),
            "q",
            root.path(),
            Duration::from_secs(5),
        )
        .await;

        assert!(reply.ok, "an unparseable answer still beats a lost one");
        assert!(reply.content.contains("did not parse"), "{}", reply.content);
        assert!(
            reply.content.contains("not json at all"),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn a_nonzero_claude_exit_is_an_error_naming_the_code() {
        let stub_dir = TempDir::new().expect("tmp");
        stub(
            &stub_dir,
            "claude",
            "#!/bin/sh\necho 'bad key' >&2\nexit 2\n",
        );
        let _path_guard = PathGuard::set(&stub_dir);
        let root = TempDir::new().expect("root");

        let reply = run_claude(
            &spec(CounselCommand::Claude, None),
            "q",
            root.path(),
            Duration::from_secs(5),
        )
        .await;

        assert!(!reply.ok);
        assert!(reply.content.contains('2'), "{}", reply.content);
        assert!(reply.content.contains("bad key"), "{}", reply.content);
    }

    #[tokio::test]
    async fn a_claude_stub_past_its_timeout_is_killed_and_errors() {
        let stub_dir = TempDir::new().expect("tmp");
        stub(&stub_dir, "claude", "#!/bin/sh\nsleep 5\necho late\n");
        let _path_guard = PathGuard::set(&stub_dir);
        let root = TempDir::new().expect("root");

        let start = Instant::now();
        let reply = run_claude(
            &spec(CounselCommand::Claude, None),
            "q",
            root.path(),
            Duration::from_secs(1),
        )
        .await;
        let elapsed = start.elapsed();

        assert!(!reply.ok);
        assert!(reply.content.contains("timed out"), "{}", reply.content);
        assert!(!reply.content.contains("late"), "{}", reply.content);
        assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    }

    #[tokio::test]
    async fn a_canned_codex_result_file_becomes_the_reply_text() {
        let stub_dir = TempDir::new().expect("tmp");
        stub(
            &stub_dir,
            "codex",
            "#!/bin/sh\n\
             # the -o path is the last argument\n\
             for a in \"$@\"; do out=\"$a\"; done\n\
             echo 'looks fine from codex' > \"$out\"\n",
        );
        let _path_guard = PathGuard::set(&stub_dir);
        let root = TempDir::new().expect("root");

        let reply = run_codex("what is broken?", root.path(), Duration::from_secs(5)).await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.content.trim_end(), "looks fine from codex");
    }

    #[tokio::test]
    async fn a_nonzero_codex_exit_is_an_error_naming_the_code() {
        let stub_dir = TempDir::new().expect("tmp");
        stub(
            &stub_dir,
            "codex",
            "#!/bin/sh\necho 'broken sandbox' >&2\nexit 7\n",
        );
        let _path_guard = PathGuard::set(&stub_dir);
        let root = TempDir::new().expect("root");

        let reply = run_codex("q", root.path(), Duration::from_secs(5)).await;

        assert!(!reply.ok);
        assert!(reply.content.contains('7'), "{}", reply.content);
        assert!(
            reply.content.contains("broken sandbox"),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn a_codex_stub_past_its_timeout_is_killed_and_errors() {
        let stub_dir = TempDir::new().expect("tmp");
        stub(&stub_dir, "codex", "#!/bin/sh\nsleep 5\n");
        let _path_guard = PathGuard::set(&stub_dir);
        let root = TempDir::new().expect("root");

        let start = Instant::now();
        let reply = run_codex("q", root.path(), Duration::from_secs(1)).await;
        let elapsed = start.elapsed();

        assert!(!reply.ok);
        assert!(reply.content.contains("timed out"), "{}", reply.content);
        assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    }
}
