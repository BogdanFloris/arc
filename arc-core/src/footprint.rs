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

#[tracing::instrument(name = "footprint.mark", skip_all)]
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

/// A repository comparison, not evidence of a particular writer.
#[tracing::instrument(name = "footprint.since", skip_all)]
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
    let mut observation = render(&commits, &files);
    if matches!(mark, Mark::Git { .. }) {
        observation.push_str("\nGit compares against the starting HEAD; pre-existing edits may be included and untracked files omitted.");
    }
    Some(observation)
}

pub fn report(confirmed_paths: Option<&[String]>, workspace: Option<&str>) -> String {
    let mut out = String::from(
        "Turn footprint:\nConfirmed file operations by this turn (write/edit/apply_patch):",
    );
    match confirmed_paths {
        None => out.push_str(" unavailable"),
        Some([]) => out.push_str(" none recorded"),
        Some(paths) => {
            for path in paths.iter().take(MAX_LINES) {
                let _ = write!(out, "\n  {path:?}");
            }
            if paths.len() > MAX_LINES {
                let _ = write!(out, "\n  … {} more paths", paths.len() - MAX_LINES);
            }
        }
    }
    out.push_str("\nThese operations are not exclusive authorship. Bash, external writers, and operations without a durable result remain unattributed.\n");
    out.push_str(workspace.unwrap_or("Workspace-wide repository comparison: unavailable."));
    out
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
    let mut out =
        String::from("Workspace-wide repository comparison (all writers; not job attribution):");
    if commits.is_empty() && files.is_empty() {
        out.push_str(" no changes reported.");
        return out;
    }
    out.push_str("\ncommits:");
    if commits.is_empty() {
        out.push_str(" none");
    }
    for line in commits.iter().take(MAX_LINES) {
        out.push_str("\n  ");
        out.push_str(line);
    }
    if commits.len() > MAX_LINES {
        let _ = write!(out, "\n  … {} more commits", commits.len() - MAX_LINES);
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
                "Workspace-wide repository comparison (all writers; not job attribution): no changes reported."
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

    #[tokio::test]
    async fn another_writers_changes_are_observed_but_not_attributed_to_the_child() {
        let dir = TempDir::new().unwrap();
        sh(dir.path(), &["jj", "git", "init"]);
        let mark = mark(dir.path(), &[]).await.unwrap();
        std::fs::write(dir.path().join("parent.txt"), "parent work").unwrap();
        let observation = since(&mark, dir.path(), &[]).await.unwrap();
        let text = report(Some(&["/child.txt".to_owned()]), Some(&observation));
        let (confirmed, workspace) = text
            .split_once("Workspace-wide repository comparison")
            .unwrap();
        assert!(confirmed.contains("/child.txt"));
        assert!(!confirmed.contains("parent.txt"));
        assert!(workspace.contains("parent.txt"));
        assert!(workspace.contains("all writers; not job attribution"));
        assert!(text.contains("Bash, external writers"));
    }

    #[test]
    fn missing_observations_and_unsafe_path_characters_are_explicit() {
        let text = report(Some(&["/a\nforged label".to_owned()]), None);
        assert!(text.contains("\"/a\\nforged label\""));
        assert!(text.contains("repository comparison: unavailable"));
        assert!(report(None, None).contains("write/edit/apply_patch): unavailable"));
        assert!(report(Some(&[]), None).contains("none recorded"));
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
