use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;

use super::{Access, Workspace, ensure_fresh};
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
    old: String,
    new: String,
}

impl Tool for Edit {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "edit".to_owned(),
            description: "Replace one exact occurrence of old with new in a file. path must \
                          be absolute. old must match exactly once; include enough \
                          surrounding context to make it unique. Requires having read the \
                          file in this session, with no changes since."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path to the file."},
                    "old": {"type": "string", "description": "Text to find; must be unique in the file."},
                    "new": {"type": "string", "description": "Text to replace it with."}
                },
                "required": ["path", "old", "new"]
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
            let args: EditArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad edit arguments ({error}). Pass {{\"path\": \"/abs/path\", \
                         \"old\": \"...\", \"new\": \"...\"}}."
                    ));
                }
            };

            if args.old.is_empty() {
                return ToolReply::error("ERROR: old must not be empty.".to_owned());
            }
            if args.old == args.new {
                return ToolReply::error("ERROR: old and new must be different.".to_owned());
            }

            let resolved = match self.workspace.grants.resolve(&args.path, Access::Write) {
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

            let occurrences = text.matches(args.old.as_str()).count();
            if occurrences == 0 {
                return ToolReply::error(format!(
                    "ERROR: old text was not found in {}.",
                    resolved.display()
                ));
            }
            if occurrences > 1 {
                return ToolReply::error(format!(
                    "ERROR: old text appears {occurrences} times in {}; include more \
                     surrounding context to make the match unique.",
                    resolved.display()
                ));
            }

            let updated = text.replacen(args.old.as_str(), &args.new, 1);
            let updated_bytes = updated.into_bytes();
            if let Err(error) = std::fs::write(&resolved, &updated_bytes) {
                return ToolReply::error(format!(
                    "ERROR: could not write {} ({error}).",
                    resolved.display()
                ));
            }

            self.workspace
                .record_read(&ctx.session_id, &resolved, &updated_bytes);
            ToolReply::ok(format!(
                "Edited {} ({} bytes).",
                resolved.display(),
                updated_bytes.len()
            ))
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
    use crate::tool::{Registry, Tool as _, ToolSource, TurnContext};

    fn workspace(root: &std::path::Path, mode: Mode) -> Arc<Workspace> {
        let grants = Grants::new(vec![Grant::new(root, mode)]).expect("grants");
        Arc::new(Workspace::new(grants))
    }

    fn ctx(session_id: &str) -> TurnContext {
        TurnContext {
            session_id: session_id.to_owned(),
            turn_id: String::new(),
        }
    }

    fn read_args(path: &std::path::Path) -> String {
        serde_json::json!({ "path": path }).to_string()
    }

    fn edit_args(path: &std::path::Path, old: &str, new: &str) -> String {
        serde_json::json!({ "path": path, "old": old, "new": new }).to_string()
    }

    #[tokio::test]
    async fn happy_path_reads_edits_and_a_second_edit_also_succeeds() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "hello world").expect("write");
        let ws = workspace(dir.path(), Mode::ReadWrite);
        let read_tool = Read::new(Arc::clone(&ws));
        let edit_tool = Edit::new(Arc::clone(&ws));

        let read_reply = read_tool.execute(read_args(&path), ctx("s-1")).await;
        assert!(read_reply.ok, "{}", read_reply.content);

        let first = edit_tool
            .execute(edit_args(&path, "world", "there"), ctx("s-1"))
            .await;
        assert!(first.ok, "{}", first.content);
        assert_eq!(fs::read_to_string(&path).expect("read back"), "hello there");

        let second = edit_tool
            .execute(edit_args(&path, "hello", "hi"), ctx("s-1"))
            .await;
        assert!(second.ok, "{}", second.content);
        assert_eq!(fs::read_to_string(&path).expect("read back"), "hi there");
    }

    #[tokio::test]
    async fn editing_without_a_prior_read_is_refused_with_the_read_first_message() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "hello world").expect("write");
        let ws = workspace(dir.path(), Mode::ReadWrite);
        let edit_tool = Edit::new(ws);

        let reply = edit_tool
            .execute(edit_args(&path, "world", "there"), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(
            reply.content.contains("has not been read"),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn editing_a_file_changed_underneath_is_refused_with_the_changed_since_message() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "hello world").expect("write");
        let ws = workspace(dir.path(), Mode::ReadWrite);
        let read_tool = Read::new(Arc::clone(&ws));
        let edit_tool = Edit::new(Arc::clone(&ws));

        let read_reply = read_tool.execute(read_args(&path), ctx("s-1")).await;
        assert!(read_reply.ok, "{}", read_reply.content);

        fs::write(&path, "changed underneath").expect("write underneath");

        let reply = edit_tool
            .execute(edit_args(&path, "world", "there"), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("changed since"), "{}", reply.content);
    }

    #[tokio::test]
    async fn zero_matches_is_a_not_found_error() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "hello world").expect("write");
        let ws = workspace(dir.path(), Mode::ReadWrite);
        let read_tool = Read::new(Arc::clone(&ws));
        let edit_tool = Edit::new(Arc::clone(&ws));

        read_tool.execute(read_args(&path), ctx("s-1")).await;

        let reply = edit_tool
            .execute(edit_args(&path, "nope", "there"), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("not found"), "{}", reply.content);
    }

    #[tokio::test]
    async fn two_matches_names_the_count_in_the_error() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "ab ab").expect("write");
        let ws = workspace(dir.path(), Mode::ReadWrite);
        let read_tool = Read::new(Arc::clone(&ws));
        let edit_tool = Edit::new(Arc::clone(&ws));

        read_tool.execute(read_args(&path), ctx("s-1")).await;

        let reply = edit_tool
            .execute(edit_args(&path, "ab", "cd"), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains('2'), "{}", reply.content);
        assert!(reply.content.contains("unique"), "{}", reply.content);
    }

    #[tokio::test]
    async fn old_equal_to_new_is_an_error() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "hello world").expect("write");
        let ws = workspace(dir.path(), Mode::ReadWrite);
        let read_tool = Read::new(Arc::clone(&ws));
        let edit_tool = Edit::new(Arc::clone(&ws));

        read_tool.execute(read_args(&path), ctx("s-1")).await;

        let reply = edit_tool
            .execute(edit_args(&path, "world", "world"), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("different"), "{}", reply.content);
    }

    #[tokio::test]
    async fn empty_old_is_an_error() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "hello world").expect("write");
        let ws = workspace(dir.path(), Mode::ReadWrite);
        let read_tool = Read::new(Arc::clone(&ws));
        let edit_tool = Edit::new(Arc::clone(&ws));

        read_tool.execute(read_args(&path), ctx("s-1")).await;

        let reply = edit_tool
            .execute(edit_args(&path, "", "x"), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("empty"), "{}", reply.content);
    }

    #[tokio::test]
    async fn editing_in_a_read_only_grant_is_the_gates_refusal() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "hello world").expect("write");
        let ws = workspace(dir.path(), Mode::ReadOnly);
        let edit_tool = Edit::new(ws);

        let reply = edit_tool
            .execute(edit_args(&path, "world", "there"), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("read-only"), "{}", reply.content);
    }

    #[tokio::test]
    async fn a_read_under_a_different_session_id_does_not_count() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "hello world").expect("write");
        let ws = workspace(dir.path(), Mode::ReadWrite);
        let read_tool = Read::new(Arc::clone(&ws));
        let edit_tool = Edit::new(Arc::clone(&ws));

        read_tool.execute(read_args(&path), ctx("s-other")).await;

        let reply = edit_tool
            .execute(edit_args(&path, "world", "there"), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(
            reply.content.contains("has not been read"),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn a_non_utf8_file_is_a_named_error() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.bin");
        fs::write(&path, [0xff, 0xfe, 0x00, 0xff]).expect("write");
        let ws = workspace(dir.path(), Mode::ReadWrite);
        let edit_tool = Edit::new(ws);

        let reply = edit_tool
            .execute(edit_args(&path, "a", "b"), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("text"), "{}", reply.content);
    }

    #[tokio::test]
    async fn a_missing_file_is_a_named_error() {
        let dir = TempDir::new().expect("tmp");
        let ws = workspace(dir.path(), Mode::ReadWrite);
        let edit_tool = Edit::new(ws);

        let path = dir.path().join("nope.txt");
        let reply = edit_tool
            .execute(edit_args(&path, "a", "b"), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(
            reply.content.contains(path.to_str().expect("utf8")),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn the_edit_tool_dispatches_through_the_registry_by_source() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "hello world").expect("write");
        let ws = workspace(dir.path(), Mode::ReadWrite);
        let read_tool = Read::new(Arc::clone(&ws));
        read_tool.execute(read_args(&path), ctx("s-1")).await;

        let mut registry = Registry::new(32 * 1024);
        registry.register(Box::new(Edit::new(ws)));

        let request = edit_args(&path, "world", "there");
        let present = registry
            .dispatch(
                "edit",
                request.clone(),
                ctx("s-1"),
                &[ToolSource::Workspace],
            )
            .await;
        assert!(present.ok, "{}", present.content);

        let absent = registry
            .dispatch("edit", request, ctx("s-1"), &[ToolSource::Builtin])
            .await;
        assert!(!absent.ok);
        assert_eq!(
            absent.content,
            "ERROR: Tool edit is not available in this session."
        );
    }
}
