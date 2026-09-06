use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;

use super::{Access, Grants, Workspace, ensure_fresh};
use crate::provider::ToolDefinition;
use crate::tool::{Tool, ToolReply, ToolSource, TurnContext};

pub const NAME: &str = "apply_patch";

/// The freeform patch tool GPT-5 class models are trained on, in Codex's
/// grammar: one `*** Begin Patch` block of add, delete, and update hunks.
pub struct ApplyPatch {
    workspace: Arc<Workspace>,
}

impl ApplyPatch {
    pub fn new(workspace: Arc<Workspace>) -> Self {
        Self { workspace }
    }
}

#[derive(Deserialize)]
struct PatchArgs {
    input: String,
}

impl Tool for ApplyPatch {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: NAME.to_owned(),
            description: "Edit files with a patch. The patch starts with `*** Begin Patch` and \
                          ends with `*** End Patch`; each hunk is `*** Add File: path` followed \
                          by `+` lines, `*** Delete File: path`, or `*** Update File: path` \
                          (optionally `*** Move to: path`) followed by `@@ context` headers and \
                          ` `, `-`, `+` lines. Paths are relative to the project root or \
                          absolute. Updating or deleting a file requires having read it in this \
                          session, with no changes since. A patch applies whole or not at all."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "input": {"type": "string", "description": "The patch text."}
                },
                "required": ["input"]
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
            let args: PatchArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad apply_patch arguments ({error}). Pass {{\"input\": \"*** Begin Patch ... *** End Patch\"}}."
                    ));
                }
            };
            let Some(grants) = &ctx.grants else {
                return ToolReply::error(
                    "ERROR: no workspace is granted in this session.".to_owned(),
                );
            };
            let hunks = match parse(&args.input) {
                Ok(hunks) => hunks,
                Err(reason) => return ToolReply::error(format!("ERROR: {reason}")),
            };

            let mut planned = Vec::with_capacity(hunks.len());
            for hunk in hunks {
                match self.plan(hunk, grants, &ctx.session_id) {
                    Ok(change) => planned.push(change),
                    Err(reason) => return ToolReply::error(format!("ERROR: {reason}")),
                }
            }

            let mut report = String::from("Applied the patch:");
            for change in planned {
                if let Err(reason) = self.commit(&change, &ctx.session_id) {
                    return ToolReply::error(format!(
                        "ERROR: {reason} Earlier hunks of this patch were already applied; \
                         read the files again before retrying."
                    ));
                }
                report.push('\n');
                report.push_str(&change.line());
            }
            ToolReply::ok(report)
        })
    }
}

enum Change {
    Add {
        path: PathBuf,
        content: Vec<u8>,
    },
    Delete {
        path: PathBuf,
    },
    Update {
        path: PathBuf,
        moved_to: Option<PathBuf>,
        content: Vec<u8>,
    },
}

impl Change {
    fn line(&self) -> String {
        match self {
            Change::Add { path, .. } => format!("A {}", path.display()),
            Change::Delete { path } => format!("D {}", path.display()),
            Change::Update {
                path,
                moved_to: Some(dest),
                ..
            } => format!("R {} -> {}", path.display(), dest.display()),
            Change::Update { path, .. } => format!("M {}", path.display()),
        }
    }
}

impl ApplyPatch {
    fn plan(&self, hunk: Hunk, grants: &Grants, session_id: &str) -> Result<Change, String> {
        let resolve = |path: &str, access: Access| {
            let absolute = if Path::new(path).is_absolute() {
                PathBuf::from(path)
            } else {
                let root = grants.project_root().ok_or_else(|| {
                    "no project root to resolve a relative path against.".to_owned()
                })?;
                root.join(path)
            };
            resolve_creating(grants, &absolute, access)
        };
        match hunk {
            Hunk::Add { path, content } => {
                let resolved = resolve(&path, Access::Write)?;
                if resolved.exists() {
                    return Err(format!(
                        "{} already exists; use an Update hunk.",
                        resolved.display()
                    ));
                }
                Ok(Change::Add {
                    path: resolved,
                    content: content.into_bytes(),
                })
            }
            Hunk::Delete { path } => {
                let resolved = resolve(&path, Access::Write)?;
                let bytes = existing(&resolved)?;
                ensure_fresh(&self.workspace, session_id, &resolved, &bytes)?;
                Ok(Change::Delete { path: resolved })
            }
            Hunk::Update {
                path,
                move_to,
                chunks,
            } => {
                let resolved = resolve(&path, Access::Write)?;
                let bytes = existing(&resolved)?;
                let text = std::str::from_utf8(&bytes).map_err(|_| {
                    format!("{} is not text (not valid UTF-8).", resolved.display())
                })?;
                ensure_fresh(&self.workspace, session_id, &resolved, &bytes)?;
                let updated = apply_chunks(text, &chunks, &path)?;
                let moved_to = move_to
                    .as_deref()
                    .map(|dest| {
                        let dest = resolve(dest, Access::Write)?;
                        if dest.exists() {
                            return Err(format!(
                                "cannot move to {}: it already exists.",
                                dest.display()
                            ));
                        }
                        Ok(dest)
                    })
                    .transpose()?;
                Ok(Change::Update {
                    path: resolved,
                    moved_to,
                    content: updated.into_bytes(),
                })
            }
        }
    }

    fn commit(&self, change: &Change, session_id: &str) -> Result<(), String> {
        let write = |path: &Path, bytes: &[u8]| {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|error| format!("could not create {} ({error}).", parent.display()))?;
            }
            std::fs::write(path, bytes)
                .map_err(|error| format!("could not write {} ({error}).", path.display()))?;
            self.workspace.record_read(session_id, path, bytes);
            Ok::<(), String>(())
        };
        match change {
            Change::Add { path, content }
            | Change::Update {
                path,
                moved_to: None,
                content,
            } => write(path, content),
            Change::Delete { path } => std::fs::remove_file(path)
                .map_err(|error| format!("could not delete {} ({error}).", path.display())),
            Change::Update {
                path,
                moved_to: Some(dest),
                content,
            } => {
                write(dest, content)?;
                std::fs::remove_file(path).map_err(|error| {
                    format!(
                        "could not remove {} after moving it ({error}).",
                        path.display()
                    )
                })
            }
        }
    }
}

// a new file may sit under directories that do not exist yet: the deepest
// existing ancestor is what containment checks, the rest is plain names
fn resolve_creating(grants: &Grants, path: &Path, access: Access) -> Result<PathBuf, String> {
    let mut existing = path;
    let mut remainder = Vec::new();
    while !existing.exists() {
        let name = existing
            .file_name()
            .ok_or_else(|| format!("{} does not name a file.", path.display()))?;
        remainder.push(name.to_owned());
        existing = existing
            .parent()
            .ok_or_else(|| format!("{} has no existing ancestor.", path.display()))?;
    }
    if remainder.iter().any(|name| name == ".." || name == ".") {
        return Err(format!(
            "{} does not name a file; \".\" and \"..\" are not allowed here.",
            path.display()
        ));
    }
    let mut resolved = grants.resolve(&existing.to_string_lossy(), access)?;
    for name in remainder.iter().rev() {
        resolved.push(name);
    }
    Ok(resolved)
}

fn existing(path: &Path) -> Result<Vec<u8>, String> {
    if path.is_dir() {
        return Err(format!("{} is a directory, not a file.", path.display()));
    }
    if !path.exists() {
        return Err(format!("{} does not exist.", path.display()));
    }
    std::fs::read(path).map_err(|error| format!("could not read {} ({error}).", path.display()))
}

const BEGIN: &str = "*** Begin Patch";
const END: &str = "*** End Patch";
const ADD: &str = "*** Add File: ";
const DELETE: &str = "*** Delete File: ";
const UPDATE: &str = "*** Update File: ";
const MOVE_TO: &str = "*** Move to: ";
const END_OF_FILE: &str = "*** End of File";

#[derive(Debug, PartialEq, Eq)]
enum Hunk {
    Add {
        path: String,
        content: String,
    },
    Delete {
        path: String,
    },
    Update {
        path: String,
        move_to: Option<String>,
        chunks: Vec<Chunk>,
    },
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Chunk {
    context: Option<String>,
    old: Vec<String>,
    new: Vec<String>,
    at_end_of_file: bool,
}

fn parse(patch: &str) -> Result<Vec<Hunk>, String> {
    let lines: Vec<&str> = patch.trim().lines().collect();
    let (Some(first), Some(last)) = (lines.first(), lines.last()) else {
        return Err("the patch is empty.".to_owned());
    };
    if first.trim_end() != BEGIN {
        return Err(format!("the patch must start with `{BEGIN}`."));
    }
    if last.trim_end() != END {
        return Err(format!("the patch must end with `{END}`."));
    }
    let body = &lines[1..lines.len() - 1];

    let mut hunks = Vec::new();
    let mut i = 0;
    while i < body.len() {
        let line = body[i];
        i += 1;
        if let Some(path) = line.strip_prefix(ADD) {
            let mut content = String::new();
            while let Some(added) = body.get(i).and_then(|l| l.strip_prefix('+')) {
                content.push_str(added);
                content.push('\n');
                i += 1;
            }
            hunks.push(Hunk::Add {
                path: path.trim().to_owned(),
                content,
            });
        } else if let Some(path) = line.strip_prefix(DELETE) {
            hunks.push(Hunk::Delete {
                path: path.trim().to_owned(),
            });
        } else if let Some(path) = line.strip_prefix(UPDATE) {
            let move_to = body
                .get(i)
                .and_then(|l| l.strip_prefix(MOVE_TO))
                .map(|dest| dest.trim().to_owned());
            if move_to.is_some() {
                i += 1;
            }
            let mut chunks: Vec<Chunk> = Vec::new();
            while let Some(&line) = body.get(i) {
                if line.starts_with("*** ") && line != END_OF_FILE {
                    break;
                }
                i += 1;
                if line == "@@" || line == "@@ " {
                    chunks.push(Chunk::default());
                    continue;
                }
                if let Some(context) = line.strip_prefix("@@ ") {
                    chunks.push(Chunk {
                        context: Some(context.to_owned()),
                        ..Chunk::default()
                    });
                    continue;
                }
                if chunks.is_empty() {
                    chunks.push(Chunk::default());
                }
                let chunk = chunks.last_mut().expect("a chunk was just ensured");
                if line == END_OF_FILE {
                    chunk.at_end_of_file = true;
                } else if let Some(removed) = line.strip_prefix('-') {
                    chunk.old.push(removed.to_owned());
                } else if let Some(added) = line.strip_prefix('+') {
                    chunk.new.push(added.to_owned());
                } else if let Some(kept) = line.strip_prefix(' ') {
                    chunk.old.push(kept.to_owned());
                    chunk.new.push(kept.to_owned());
                } else if line.is_empty() {
                    // a blank context line usually arrives without its leading space
                    chunk.old.push(String::new());
                    chunk.new.push(String::new());
                } else {
                    return Err(format!(
                        "line {} of the patch is not a hunk line: {line}",
                        i + 1
                    ));
                }
            }
            if chunks.is_empty() {
                return Err(format!("the update for {path} has no changes."));
            }
            hunks.push(Hunk::Update {
                path: path.trim().to_owned(),
                move_to,
                chunks,
            });
        } else {
            return Err(format!(
                "line {} of the patch is not a hunk header: {line}",
                i + 1
            ));
        }
    }
    if hunks.is_empty() {
        return Err("the patch has no hunks.".to_owned());
    }
    Ok(hunks)
}

fn apply_chunks(text: &str, chunks: &[Chunk], path: &str) -> Result<String, String> {
    let mut lines: Vec<String> = text.split('\n').map(str::to_owned).collect();
    // the split's trailing empty element stands for the final newline
    let ends_with_newline = text.ends_with('\n');
    if ends_with_newline {
        lines.pop();
    }

    let mut replacements: Vec<(usize, usize, Vec<String>)> = Vec::new();
    let mut cursor = 0;
    for chunk in chunks {
        if let Some(context) = &chunk.context {
            let found = seek(&lines, std::slice::from_ref(context), cursor, false)
                .ok_or_else(|| format!("could not find the context line `{context}` in {path}."))?;
            cursor = found + 1;
        }
        if chunk.old.is_empty() {
            replacements.push((lines.len(), 0, chunk.new.clone()));
            continue;
        }
        let mut old: &[String] = &chunk.old;
        let mut new: &[String] = &chunk.new;
        let mut found = seek(&lines, old, cursor, chunk.at_end_of_file);
        if found.is_none() && old.last().is_some_and(String::is_empty) {
            old = &old[..old.len() - 1];
            if new.last().is_some_and(String::is_empty) {
                new = &new[..new.len() - 1];
            }
            found = seek(&lines, old, cursor, chunk.at_end_of_file);
        }
        let start = found.ok_or_else(|| {
            format!(
                "could not find these lines in {path}:\n{}",
                chunk.old.join("\n")
            )
        })?;
        replacements.push((start, old.len(), new.to_vec()));
        cursor = start + old.len();
    }

    replacements.sort_by_key(|(start, _, _)| *start);
    for (start, removed, added) in replacements.into_iter().rev() {
        let end = (start + removed).min(lines.len());
        lines.splice(start..end, added);
    }
    let mut out = lines.join("\n");
    if ends_with_newline || text.is_empty() {
        out.push('\n');
    }
    Ok(out)
}

// exact first, then ignoring trailing whitespace, then ignoring all edge whitespace
fn seek(lines: &[String], pattern: &[String], start: usize, at_end: bool) -> Option<usize> {
    if pattern.is_empty() {
        return Some(start);
    }
    if pattern.len() > lines.len() {
        return None;
    }
    let last_start = lines.len() - pattern.len();
    let from = if at_end { last_start } else { start };
    let matches = |equal: &dyn Fn(&str, &str) -> bool| {
        (from..=last_start).find(|&i| {
            pattern
                .iter()
                .enumerate()
                .all(|(k, want)| equal(&lines[i + k], want))
        })
    };
    matches(&|a, b| a == b)
        .or_else(|| matches(&|a, b| a.trim_end() == b.trim_end()))
        .or_else(|| matches(&|a, b| a.trim() == b.trim()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::{ApplyPatch, Chunk, Hunk, apply_chunks, parse};
    use crate::tool::workspace::read::Read;
    use crate::tool::workspace::{Grant, Grants, Mode, Workspace};
    use crate::tool::{Tool as _, TurnContext};

    fn ctx(root: &std::path::Path, mode: Mode) -> TurnContext {
        let grants = Grants::new(vec![Grant::new(root, mode)]).expect("grants");
        TurnContext {
            session_id: "s-1".to_owned(),
            turn_id: String::new(),
            grants: Some(Arc::new(grants)),
            command_prefix: Vec::new(),
        }
    }

    fn args(patch: &str) -> String {
        serde_json::json!({ "input": patch }).to_string()
    }

    async fn read(ws: &Arc<Workspace>, root: &std::path::Path, path: &std::path::Path) {
        let reply = Read::new(Arc::clone(ws))
            .execute(
                serde_json::json!({ "path": path }).to_string(),
                ctx(root, Mode::ReadWrite),
            )
            .await;
        assert!(reply.ok, "{}", reply.content);
    }

    #[test]
    fn a_patch_parses_into_its_hunks() {
        let hunks = parse(
            &[
                "*** Begin Patch",
                "*** Add File: new.txt",
                "+line one",
                "+line two",
                "*** Delete File: old.txt",
                "*** Update File: src/lib.rs",
                "*** Move to: src/main.rs",
                "@@ fn main() {",
                "-    old();",
                "+    new();",
                "     keep();",
                "",
                "@@",
                "+trailing",
                "*** End of File",
                "*** End Patch",
            ]
            .join("\n"),
        )
        .expect("parses");

        assert_eq!(
            hunks,
            [
                Hunk::Add {
                    path: "new.txt".to_owned(),
                    content: "line one\nline two\n".to_owned(),
                },
                Hunk::Delete {
                    path: "old.txt".to_owned(),
                },
                Hunk::Update {
                    path: "src/lib.rs".to_owned(),
                    move_to: Some("src/main.rs".to_owned()),
                    chunks: vec![
                        Chunk {
                            context: Some("fn main() {".to_owned()),
                            old: vec![
                                "    old();".to_owned(),
                                "    keep();".to_owned(),
                                String::new()
                            ],
                            new: vec![
                                "    new();".to_owned(),
                                "    keep();".to_owned(),
                                String::new()
                            ],
                            at_end_of_file: false,
                        },
                        Chunk {
                            context: None,
                            old: vec![],
                            new: vec!["trailing".to_owned()],
                            at_end_of_file: true,
                        },
                    ],
                },
            ]
        );
    }

    #[test]
    fn a_patch_without_its_markers_or_hunks_is_refused() {
        assert!(parse("").unwrap_err().contains("empty"));
        assert!(
            parse("*** Update File: x\n-a\n+b")
                .unwrap_err()
                .contains("Begin Patch")
        );
        assert!(
            parse("*** Begin Patch\n*** End Patch")
                .unwrap_err()
                .contains("no hunks")
        );
        assert!(
            parse("*** Begin Patch\nhello\n*** End Patch")
                .unwrap_err()
                .contains("hunk header")
        );
        assert!(
            parse("*** Begin Patch\n*** Update File: x\n?what\n*** End Patch")
                .unwrap_err()
                .contains("hunk line")
        );
    }

    fn chunk(context: Option<&str>, old: &[&str], new: &[&str]) -> Chunk {
        Chunk {
            context: context.map(str::to_owned),
            old: old.iter().map(|s| (*s).to_owned()).collect(),
            new: new.iter().map(|s| (*s).to_owned()).collect(),
            at_end_of_file: false,
        }
    }

    #[test]
    fn chunks_apply_after_their_context_and_in_order() {
        let text = "fn a() {\n    x();\n}\n\nfn b() {\n    x();\n}\n";
        let chunks = [chunk(Some("fn b() {"), &["    x();"], &["    y();"])];

        assert_eq!(
            apply_chunks(text, &chunks, "f.rs").expect("applies"),
            "fn a() {\n    x();\n}\n\nfn b() {\n    y();\n}\n",
            "the context line steers the match past the first x()"
        );
    }

    #[test]
    fn an_insertion_without_old_lines_appends_and_trailing_whitespace_is_forgiven() {
        let text = "one  \ntwo\n";
        let chunks = [
            chunk(None, &["one"], &["uno"]),
            chunk(None, &[], &["three"]),
        ];

        assert_eq!(
            apply_chunks(text, &chunks, "f").expect("applies"),
            "uno\ntwo\nthree\n"
        );
    }

    #[test]
    fn a_file_without_a_final_newline_stays_that_way() {
        let text = "a\nb";
        let chunks = [chunk(None, &["b"], &["c"])];

        assert_eq!(apply_chunks(text, &chunks, "f").expect("applies"), "a\nc");
    }

    #[test]
    fn lines_that_are_not_in_the_file_name_the_file_and_the_lines() {
        let err = apply_chunks("a\n", &[chunk(None, &["zzz"], &["y"])], "f.txt").unwrap_err();
        assert!(err.contains("f.txt") && err.contains("zzz"), "{err}");
        let err = apply_chunks("a\n", &[chunk(Some("nope"), &["a"], &["b"])], "f.txt").unwrap_err();
        assert!(err.contains("context line `nope`"), "{err}");
    }

    #[tokio::test]
    async fn a_patch_adds_updates_moves_and_deletes_under_the_root() {
        let dir = TempDir::new().expect("tmp");
        let root = dir.path();
        fs::write(root.join("keep.txt"), "hello world\n").expect("write");
        fs::write(root.join("gone.txt"), "bye\n").expect("write");
        fs::write(root.join("old.txt"), "moving\n").expect("write");
        let ws = Arc::new(Workspace::new());
        for name in ["keep.txt", "gone.txt", "old.txt"] {
            read(&ws, root, &root.join(name)).await;
        }
        let tool = ApplyPatch::new(Arc::clone(&ws));
        let patch = format!(
            "*** Begin Patch\n\
             *** Add File: sub/new.txt\n\
             +fresh\n\
             *** Update File: keep.txt\n\
             -hello world\n\
             +hello there\n\
             *** Update File: {}\n\
             *** Move to: moved.txt\n\
             -moving\n\
             +moved\n\
             *** Delete File: gone.txt\n\
             *** End Patch",
            root.join("old.txt").display()
        );

        let reply = tool.execute(args(&patch), ctx(root, Mode::ReadWrite)).await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(
            fs::read_to_string(root.join("sub/new.txt")).unwrap(),
            "fresh\n"
        );
        assert_eq!(
            fs::read_to_string(root.join("keep.txt")).unwrap(),
            "hello there\n"
        );
        assert_eq!(
            fs::read_to_string(root.join("moved.txt")).unwrap(),
            "moved\n"
        );
        assert!(!root.join("old.txt").exists());
        assert!(!root.join("gone.txt").exists());
        assert!(
            reply.content.starts_with("Applied the patch:"),
            "{}",
            reply.content
        );
        assert!(
            reply.content.contains("A ")
                && reply.content.contains("M ")
                && reply.content.contains("R ")
                && reply.content.contains("D "),
            "{}",
            reply.content
        );

        let again = tool
            .execute(
                args("*** Begin Patch\n*** Update File: keep.txt\n-hello there\n+hello again\n*** End Patch"),
                ctx(root, Mode::ReadWrite),
            )
            .await;
        assert!(
            again.ok,
            "a patch counts as a fresh read: {}",
            again.content
        );
    }

    #[tokio::test]
    async fn an_unread_file_is_refused_and_nothing_is_written() {
        let dir = TempDir::new().expect("tmp");
        let root = dir.path();
        fs::write(root.join("a.txt"), "a\n").expect("write");
        let ws = Arc::new(Workspace::new());
        let tool = ApplyPatch::new(Arc::clone(&ws));

        let reply = tool
            .execute(
                args("*** Begin Patch\n*** Add File: b.txt\n+b\n*** Update File: a.txt\n-a\n+A\n*** End Patch"),
                ctx(root, Mode::ReadWrite),
            )
            .await;

        assert!(!reply.ok);
        assert!(
            reply.content.contains("has not been read"),
            "{}",
            reply.content
        );
        assert!(
            !root.join("b.txt").exists(),
            "the add hunk before it was not applied"
        );
    }

    #[tokio::test]
    async fn a_read_only_root_an_escape_and_an_existing_add_target_are_refused() {
        let dir = TempDir::new().expect("tmp");
        let root = dir.path();
        fs::write(root.join("a.txt"), "a\n").expect("write");
        let ws = Arc::new(Workspace::new());
        read(&ws, root, &root.join("a.txt")).await;
        let tool = ApplyPatch::new(Arc::clone(&ws));

        let reply = tool
            .execute(
                args("*** Begin Patch\n*** Update File: a.txt\n-a\n+A\n*** End Patch"),
                ctx(root, Mode::ReadOnly),
            )
            .await;
        assert!(
            !reply.ok && reply.content.contains("read-only"),
            "{}",
            reply.content
        );

        let reply = tool
            .execute(
                args("*** Begin Patch\n*** Add File: ../escape.txt\n+x\n*** End Patch"),
                ctx(root, Mode::ReadWrite),
            )
            .await;
        assert!(
            !reply.ok && reply.content.contains("outside"),
            "{}",
            reply.content
        );

        let reply = tool
            .execute(
                args("*** Begin Patch\n*** Add File: a.txt\n+x\n*** End Patch"),
                ctx(root, Mode::ReadWrite),
            )
            .await;
        assert!(
            !reply.ok && reply.content.contains("already exists"),
            "{}",
            reply.content
        );
        assert_eq!(fs::read_to_string(root.join("a.txt")).unwrap(), "a\n");
    }

    #[tokio::test]
    async fn an_unbound_session_and_bad_arguments_are_named_errors() {
        let tool = ApplyPatch::new(Arc::new(Workspace::new()));

        let reply = tool
            .execute(
                args("*** Begin Patch\n*** Delete File: x\n*** End Patch"),
                TurnContext::default(),
            )
            .await;
        assert!(
            !reply.ok && reply.content.contains("no workspace"),
            "{}",
            reply.content
        );

        let reply = tool
            .execute("not json".to_owned(), TurnContext::default())
            .await;
        assert!(
            !reply.ok && reply.content.contains("bad apply_patch arguments"),
            "{}",
            reply.content
        );
    }
}
