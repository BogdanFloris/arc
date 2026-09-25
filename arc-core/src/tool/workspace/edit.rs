use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;

use super::{Workspace, ensure_fresh, resolve_path};
use crate::provider::ToolDefinition;
use crate::tool::{Tool, ToolReply, ToolSource, TurnContext};

pub struct Edit {
    workspace: Arc<Workspace>,
}

impl Edit {
    pub fn new(workspace: Arc<Workspace>) -> Self {
        Self { workspace }
    }
}

#[derive(Deserialize)]
struct EditArgs {
    path: String,
    replacements: Option<Vec<Replacement>>,
    old: Option<String>,
    new: Option<String>,
}

#[derive(Deserialize)]
struct Replacement {
    old: String,
    new: String,
}

impl Tool for Edit {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "edit".to_owned(),
            description: "Replace several non-overlapping spans in one file. path must be \
                          absolute. Each replacement's old text must appear exactly once in \
                          the original file; include context to make it unique. All replacements \
                          are validated before writing. Requires having read the file using \
                          the `read` tool in this session, with no changes since. Reading \
                          through Bash does not count."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path to the file."},
                    "replacements": {
                        "type": "array",
                        "minItems": 1,
                        "description": "Non-overlapping replacements against the original file.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "old": {"type": "string", "description": "Text appearing exactly once in the original file."},
                                "new": {"type": "string", "description": "Replacement text."}
                            },
                            "required": ["old", "new"]
                        }
                    }
                },
                "required": ["path", "replacements"]
            }),
        }
    }

    fn source(&self) -> ToolSource {
        ToolSource::Replacement
    }

    fn execute(
        &self,
        arguments_json: String,
        ctx: TurnContext,
    ) -> Pin<Box<dyn Future<Output = ToolReply> + Send + '_>> {
        Box::pin(async move {
            let args: EditArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad edit arguments ({error}). Pass {{\"path\": \"/abs/path\", \
                         \"replacements\": [{{\"old\": \"...\", \"new\": \"...\"}}]}}."
                    ));
                }
            };
            let replacements =
                match (args.replacements, args.old, args.new) {
                    (Some(replacements), None, None) if !replacements.is_empty() => replacements,
                    (None, Some(old), Some(new)) => vec![Replacement { old, new }],
                    _ => return ToolReply::error(
                        "ERROR: provide nonempty replacements, or legacy old and new, not both."
                            .to_owned(),
                    ),
                };

            let resolved = match resolve_path(&args.path) {
                Ok(path) => path,
                Err(reason) => return ToolReply::error(format!("ERROR: {reason}")),
            };

            if resolved.is_dir() {
                return ToolReply::error(format!(
                    "ERROR: {} is a directory, not a file.",
                    resolved.display()
                ));
            }
            if !resolved.exists() {
                return ToolReply::error(format!("ERROR: {} does not exist.", resolved.display()));
            }

            let bytes = match std::fs::read(&resolved) {
                Ok(bytes) => bytes,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: could not read {} ({error}).",
                        resolved.display()
                    ));
                }
            };

            let Ok(text) = std::str::from_utf8(&bytes) else {
                return ToolReply::error(format!(
                    "ERROR: {} is not text (not valid UTF-8).",
                    resolved.display()
                ));
            };

            if let Err(reason) = ensure_fresh(&self.workspace, &ctx.session_id, &resolved, &bytes) {
                return ToolReply::error(format!("ERROR: {reason}"));
            }

            let mut spans = Vec::with_capacity(replacements.len());
            for replacement in &replacements {
                if replacement.old.is_empty() {
                    return ToolReply::error("ERROR: old must not be empty.".to_owned());
                }
                if replacement.old == replacement.new {
                    return ToolReply::error("ERROR: old and new must be different.".to_owned());
                }
                let mut matches = text.match_indices(&replacement.old);
                let Some((start, _)) = matches.next() else {
                    return ToolReply::error(format!(
                        "ERROR: old text was not found in {}.",
                        resolved.display()
                    ));
                };
                if matches.next().is_some() {
                    let occurrences = 2 + matches.count();
                    return ToolReply::error(format!(
                        "ERROR: old text appears {occurrences} times in {}; include more \
                         surrounding context to make the match unique.",
                        resolved.display()
                    ));
                }
                spans.push((
                    start,
                    start + replacement.old.len(),
                    replacement.new.as_str(),
                ));
            }
            spans.sort_unstable_by_key(|span| span.0);
            if spans.windows(2).any(|pair| pair[0].1 > pair[1].0) {
                return ToolReply::error(
                    "ERROR: replacements overlap in the original file.".to_owned(),
                );
            }
            let mut updated = String::with_capacity(text.len());
            let mut end = 0;
            for (start, next, replacement) in spans {
                updated.push_str(&text[end..start]);
                updated.push_str(replacement);
                end = next;
            }
            updated.push_str(&text[end..]);
            let updated_bytes = updated.into_bytes();
            if let Err(error) = std::fs::write(&resolved, &updated_bytes) {
                return ToolReply::error(format!(
                    "ERROR: could not write {} ({error}).",
                    resolved.display()
                ));
            }

            self.workspace
                .record_read(&ctx.session_id, &resolved, &updated_bytes);
            ToolReply {
                changed_paths: vec![resolved.to_string_lossy().into_owned()],
                ..ToolReply::ok(format!(
                    "Edited {} ({} bytes).",
                    resolved.display(),
                    updated_bytes.len()
                ))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::Edit;
    use crate::tool::workspace::read::Read;
    use crate::tool::workspace::{Grant, Grants, Mode, Workspace};
    use crate::tool::{Tool as _, TurnContext};

    fn workspace() -> Arc<Workspace> {
        Arc::new(Workspace::new())
    }

    fn ctx(session_id: &str, root: &std::path::Path, mode: Mode) -> TurnContext {
        let grants = Grants::new(vec![Grant::new(root, mode)]).expect("grants");
        TurnContext {
            session_id: session_id.to_owned(),
            turn_id: String::new(),
            grants: Some(Arc::new(grants)),
            command_prefix: Vec::new(),
        }
    }

    fn read_args(path: &std::path::Path) -> String {
        serde_json::json!({ "path": path }).to_string()
    }

    fn edit_args(path: &std::path::Path, old: &str, new: &str) -> String {
        serde_json::json!({ "path": path, "old": old, "new": new }).to_string()
    }

    fn batch(path: &std::path::Path, replacements: &[(&str, &str)]) -> String {
        let replacements = replacements
            .iter()
            .map(|(old, new)| serde_json::json!({"old": old, "new": new}))
            .collect::<Vec<_>>();
        serde_json::json!({"path": path, "replacements": replacements}).to_string()
    }

    #[tokio::test]
    async fn batch_replaces_against_original_and_records_one_path() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "alpha beta gamma").unwrap();
        let ws = workspace();
        Read::new(Arc::clone(&ws))
            .execute(read_args(&path), ctx("s", dir.path(), Mode::ReadWrite))
            .await;
        let reply = Edit::new(ws)
            .execute(
                batch(&path, &[("gamma", "G"), ("alpha", "A"), ("beta", "B")]),
                ctx("s", dir.path(), Mode::ReadWrite),
            )
            .await;
        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.changed_paths, [path.to_string_lossy()]);
        assert_eq!(fs::read_to_string(path).unwrap(), "A B G");
    }

    #[tokio::test]
    async fn failed_batches_never_write_any_replacement() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        for (content, replacements, error) in [
            ("abcd", vec![("abc", "X"), ("bcd", "Y")], "overlap"),
            ("abcd", vec![("ab", "X"), ("missing", "Y")], "not found"),
            ("ab ab", vec![("ab", "X"), (" ", "_")], "unique"),
            ("ab ab ab", vec![("ab", "X")], "3 times"),
            ("abcd", vec![("ab", "X"), ("cd", "cd")], "different"),
            ("abcd", vec![("", "X")], "empty"),
        ] {
            fs::write(&path, content).unwrap();
            let ws = workspace();
            Read::new(Arc::clone(&ws))
                .execute(read_args(&path), ctx("s", dir.path(), Mode::ReadWrite))
                .await;
            let reply = Edit::new(ws)
                .execute(
                    batch(&path, &replacements),
                    ctx("s", dir.path(), Mode::ReadWrite),
                )
                .await;
            assert!(
                !reply.ok && reply.content.contains(error),
                "{}",
                reply.content
            );
            assert!(reply.changed_paths.is_empty());
            assert_eq!(fs::read_to_string(&path).unwrap(), content);
        }
    }

    #[tokio::test]
    async fn batch_keeps_freshness_checks_without_grants() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "alpha beta").unwrap();
        let ws = workspace();
        let request = batch(&path, &[("alpha", "A"), ("beta", "B")]);
        let tool = Edit::new(Arc::clone(&ws));
        let unread = tool
            .execute(request.clone(), ctx("s", dir.path(), Mode::ReadWrite))
            .await;
        assert!(unread.content.contains("has not been read"));
        Read::new(ws)
            .execute(read_args(&path), ctx("s", dir.path(), Mode::ReadWrite))
            .await;
        fs::write(&path, "alpha beta!").unwrap();
        let stale = tool
            .execute(request.clone(), ctx("s", dir.path(), Mode::ReadWrite))
            .await;
        assert!(stale.content.contains("changed since"));
        Read::new(Arc::clone(&tool.workspace))
            .execute(
                read_args(&path),
                TurnContext {
                    session_id: "s".to_owned(),
                    ..TurnContext::default()
                },
            )
            .await;
        let edited = tool
            .execute(
                request,
                TurnContext {
                    session_id: "s".to_owned(),
                    ..TurnContext::default()
                },
            )
            .await;
        assert!(edited.ok, "{}", edited.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A B!");
    }

    #[tokio::test]
    async fn happy_path_reads_edits_and_a_second_edit_also_succeeds() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "hello world").expect("write");
        let ws = workspace();
        let read_tool = Read::new(Arc::clone(&ws));
        let edit_tool = Edit::new(Arc::clone(&ws));

        let read_reply = read_tool
            .execute(read_args(&path), ctx("s-1", dir.path(), Mode::ReadWrite))
            .await;
        assert!(read_reply.ok, "{}", read_reply.content);

        let first = edit_tool
            .execute(
                edit_args(&path, "world", "there"),
                ctx("s-1", dir.path(), Mode::ReadWrite),
            )
            .await;
        assert!(first.ok, "{}", first.content);
        assert_eq!(
            first.changed_paths,
            [path.canonicalize().unwrap().to_string_lossy()]
        );
        assert_eq!(fs::read_to_string(&path).expect("read back"), "hello there");

        let second = edit_tool
            .execute(
                edit_args(&path, "hello", "hi"),
                ctx("s-1", dir.path(), Mode::ReadWrite),
            )
            .await;
        assert!(second.ok, "{}", second.content);
        assert_eq!(second.changed_paths, first.changed_paths);
        assert_eq!(fs::read_to_string(&path).expect("read back"), "hi there");
    }

    #[tokio::test]
    async fn a_read_under_a_different_session_id_does_not_count() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "hello world").expect("write");
        let ws = workspace();
        let read_tool = Read::new(Arc::clone(&ws));
        let edit_tool = Edit::new(Arc::clone(&ws));

        read_tool
            .execute(
                read_args(&path),
                ctx("s-other", dir.path(), Mode::ReadWrite),
            )
            .await;

        let reply = edit_tool
            .execute(
                edit_args(&path, "world", "there"),
                ctx("s-1", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(!reply.ok);
        assert!(
            reply.content.contains("has not been read"),
            "{}",
            reply.content
        );
    }
}
