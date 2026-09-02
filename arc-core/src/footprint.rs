use std::fmt::Write as _;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tracing::warn;

use crate::tool::workspace::bash::prefixed;

const TIMEOUT: Duration = Duration::from_secs(60);
const MAX_LINES: usize = 40;

/// Where a project's working copy stood when a turn began.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mark {
    Jj { commit: String, change: String },
    Git { head: String },
}

pub async fn mark(root: &Path, command_prefix: &[String]) -> Option<Mark> {
    if root.join(".jj").is_dir() {
        // `jj log` snapshots the working copy, so the commit id names the tree as it is now
        let out = capture(
            root,
            command_prefix,
            &[
                "jj",
                "--quiet",
                "log",
                "-r",
                "@",
                "--no-graph",
                "-T",
                "commit_id ++ \" \" ++ change_id",
            ],
        )
        .await?;
        let (commit, change) = out.trim().split_once(' ')?;
        return Some(Mark::Jj {
            commit: commit.to_owned(),
            change: change.to_owned(),
        });
    }
    if root.join(".git").exists() {
        let head = capture(root, command_prefix, &["git", "rev-parse", "HEAD"]).await?;
        return Some(Mark::Git {
            head: head.trim().to_owned(),
        });
    }
    None
}

/// What changed in the project since `mark`: new commits by id and message,
/// then the files, as the tools count them rather than as the job reports them.
pub async fn since(mark: &Mark, root: &Path, command_prefix: &[String]) -> Option<String> {
    let (commits, files) = match mark {
        Mark::Jj { commit, change } => {
            let revset = format!("({change}::@) ~ @");
            let commits = capture(
                root,
                command_prefix,
                &[
                    "jj",
                    "--quiet",
                    "log",
                    "-r",
                    &revset,
                    "--no-graph",
                    "-T",
                    "change_id.short() ++ \" \" ++ description.first_line() ++ \"\\n\"",
                ],
            )
            .await?;
            let files = capture(
                root,
                command_prefix,
                &[
                    "jj", "--quiet", "diff", "--from", commit, "--to", "@", "--stat",
                ],
            )
            .await?;
            (commits, files)
        }
        Mark::Git { head } => {
            let range = format!("{head}..HEAD");
            let commits = capture(
                root,
                command_prefix,
                &["git", "log", "--format=%h %s", &range],
            )
            .await?;
            let files = capture(
                root,
                command_prefix,
                &["git", "diff", "--stat", "--no-color", head],
            )
            .await?;
            (commits, files)
        }
    };
    Some(render(&commits, &files))
}

fn render(commits: &str, files: &str) -> String {
    let commits: Vec<&str> = commits
        .lines()
        .map(str::trim_end)
        .filter(|l| !l.is_empty())
        .collect();
    let files: Vec<&str> = files
        .lines()
        .map(str::trim_end)
        .filter(|l| !l.is_empty() && !l.trim_start().starts_with("0 files changed"))
        .collect();
    let mut out = String::from("Footprint since this turn began, counted by the daemon:");
    if commits.is_empty() && files.is_empty() {
        out.push_str(" nothing in the project changed.");
        return out;
    }
    out.push_str("\ncommits:");
    if commits.is_empty() {
        out.push_str(" none");
    }
    for line in commits {
        out.push_str("\n  ");
        out.push_str(line);
    }
    out.push_str("\nfiles:");
    if files.is_empty() {
        out.push_str(" none");
    }
    for line in files.iter().take(MAX_LINES) {
        out.push_str("\n  ");
        out.push_str(line.trim_start());
    }
    if files.len() > MAX_LINES {
        let _ = write!(out, "\n  … {} more lines", files.len() - MAX_LINES);
    }
    out
}

async fn capture(root: &Path, command_prefix: &[String], argv: &[&str]) -> Option<String> {
    let (program, mut cmd) = prefixed(root, command_prefix, argv);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.kill_on_drop(true);
    let output = match tokio::time::timeout(TIMEOUT, cmd.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            warn!(program, tool = argv[0], %error, "footprint command could not start");
            return None;
        }
        Err(_) => {
            warn!(program, tool = argv[0], "footprint command timed out");
            return None;
        }
    };
    if !output.status.success() {
        warn!(
            program,
            tool = argv[0],
            status = %output.status,
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "footprint command failed"
        );
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    use tempfile::TempDir;

    fn sh(dir: &Path, argv: &[&str]) {
        let status = Command::new(argv[0])
            .args(&argv[1..])
            .current_dir(dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("the tool runs");
        assert!(status.success(), "{argv:?}");
    }

    #[tokio::test]
    async fn a_jj_footprint_names_the_turns_commits_and_files() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path();
        sh(root, &["jj", "git", "init"]);
        std::fs::write(root.join("a.txt"), "a\n").expect("write");
        sh(root, &["jj", "commit", "-m", "base"]);

        let mark = mark(root, &[]).await.expect("a jj repo marks");
        assert!(matches!(mark, Mark::Jj { .. }));
        assert_eq!(
            since(&mark, root, &[]).await.as_deref(),
            Some(
                "Footprint since this turn began, counted by the daemon: nothing in the project changed."
            ),
        );

        std::fs::write(root.join("a.txt"), "a\na\n").expect("write");
        std::fs::write(root.join("b.txt"), "bee bee bee\n").expect("write");
        sh(root, &["jj", "commit", "-m", "arc: the job's commit"]);
        std::fs::write(root.join("c.txt"), "sea sea sea\n").expect("write");

        let text = since(&mark, root, &[]).await.expect("a footprint");
        assert!(text.contains("commits:\n  "), "{text}");
        assert!(text.contains(" arc: the job's commit"), "{text}");
        for name in ["a.txt", "b.txt", "c.txt"] {
            assert!(text.contains(name), "{name} is missing: {text}");
        }
        assert!(text.contains("3 files changed"), "{text}");
    }

    #[tokio::test]
    async fn a_git_footprint_reads_the_same_way() {
        let dir = TempDir::new().expect("temp dir");
        let root = dir.path();
        sh(root, &["git", "init", "-q"]);
        sh(
            root,
            &[
                "git",
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "--allow-empty",
                "-q",
                "-m",
                "base",
            ],
        );

        let mark = mark(root, &[]).await.expect("a git repo marks");
        assert!(matches!(mark, Mark::Git { .. }));

        std::fs::write(root.join("a.txt"), "a\n").expect("write");
        sh(root, &["git", "add", "a.txt"]);
        sh(
            root,
            &[
                "git",
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-q",
                "-m",
                "the job's commit",
            ],
        );

        let text = since(&mark, root, &[]).await.expect("a footprint");
        assert!(text.contains(" the job's commit"), "{text}");
        assert!(text.contains("a.txt"), "{text}");
        assert!(text.contains("1 file changed"), "{text}");
    }

    #[tokio::test]
    async fn a_directory_without_a_repo_has_no_footprint() {
        let dir = TempDir::new().expect("temp dir");
        assert_eq!(mark(dir.path(), &[]).await, None);
    }

    #[test]
    fn a_long_file_list_is_capped() {
        let mut files = String::new();
        for i in 0..50 {
            let _ = writeln!(files, "f{i}.rs | 1 +");
        }
        let text = render("", &files);
        assert!(text.contains("commits: none"), "{text}");
        assert!(text.contains("… 10 more lines"), "{text}");
    }
}
