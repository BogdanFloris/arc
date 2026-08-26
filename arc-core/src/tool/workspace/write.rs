use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;

use super::{Access, Workspace, ensure_fresh};
use crate::provider::ToolDefinition;
use crate::tool::{Tool, ToolReply, ToolSource, TurnContext};

pub struct Write {
    workspace: Arc<Workspace>,
}

impl Write {
    pub fn new(workspace: Arc<Workspace>) -> Self {
        Self { workspace }
    }
}

#[derive(Deserialize)]
struct WriteArgs {
    path: String,
    content: String,
}

impl Tool for Write {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "write".to_owned(),
            description: "Write a file's full contents, creating it if it does not exist. \
                          path must be absolute. Overwriting a file that already exists \
                          requires having read it in this session, with no changes since."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path to the file."},
                    "content": {"type": "string", "description": "The file's new full content."}
                },
                "required": ["path", "content"]
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
            let args: WriteArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad write arguments ({error}). Pass {{\"path\": \"/abs/path\", \
                         \"content\": \"...\"}}."
                    ));
                }
            };

            let Some(grants) = &ctx.grants else {
                return ToolReply::error(
                    "ERROR: no workspace is granted in this session.".to_owned(),
                );
            };
            let resolved = match grants.resolve(&args.path, Access::Write) {
                Ok(path) => path,
                Err(reason) => return ToolReply::error(format!("ERROR: {reason}")),
            };

            if resolved.is_dir() {
                return ToolReply::error(format!(
                    "ERROR: {} is a directory, not a file.",
                    resolved.display()
                ));
            }

            if resolved.exists() {
                let existing = match std::fs::read(&resolved) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        return ToolReply::error(format!(
                            "ERROR: could not read {} ({error}).",
                            resolved.display()
                        ));
                    }
                };
                if let Err(reason) =
                    ensure_fresh(&self.workspace, &ctx.session_id, &resolved, &existing)
                {
                    return ToolReply::error(format!("ERROR: {reason}"));
                }
            }

            let bytes = args.content.into_bytes();
            if let Err(error) = std::fs::write(&resolved, &bytes) {
                return ToolReply::error(format!(
                    "ERROR: could not write {} ({error}).",
                    resolved.display()
                ));
            }

            self.workspace
                .record_read(&ctx.session_id, &resolved, &bytes);
            ToolReply::ok(format!(
                "Wrote {} bytes to {}.",
                bytes.len(),
                resolved.display()
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::Write;
    use crate::tool::workspace::read::Read;
    use crate::tool::workspace::{Grant, Grants, Mode, Workspace};
    use crate::tool::{Registry, Tool as _, ToolSource, TurnContext};

    fn workspace() -> Arc<Workspace> {
        Arc::new(Workspace::new())
    }

    fn ctx(session_id: &str, root: &std::path::Path, mode: Mode) -> TurnContext {
        let grants = Grants::new(vec![Grant::new(root, mode)]).expect("grants");
        TurnContext {
            session_id: session_id.to_owned(),
            turn_id: String::new(),
            grants: Some(Arc::new(grants)),
        }
    }

    fn write_args(path: &std::path::Path, content: &str) -> String {
        serde_json::json!({ "path": path, "content": content }).to_string()
    }

    fn read_args(path: &std::path::Path) -> String {
        serde_json::json!({ "path": path }).to_string()
    }

    #[tokio::test]
    async fn writing_a_new_file_succeeds_and_its_hash_lets_an_immediate_edit_proceed() {
        let dir = TempDir::new().expect("tmp");
        let ws = workspace();
        let tool = Write::new(Arc::clone(&ws));

        let path = dir.path().join("new.txt");
        let reply = tool
            .execute(
                write_args(&path, "hello"),
                ctx("s-1", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert!(reply.content.contains("5 bytes"), "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).expect("read back"), "hello");

        let canonical = path.canonicalize().expect("canonicalize");
        assert!(ws.recorded_hash("s-1", &canonical).is_some());
    }

    #[tokio::test]
    async fn an_unbound_session_is_a_named_error() {
        let dir = TempDir::new().expect("tmp");
        let tool = Write::new(workspace());

        let path = dir.path().join("new.txt");
        let reply = tool
            .execute(write_args(&path, "hello"), TurnContext::default())
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("granted"), "{}", reply.content);
    }

    #[tokio::test]
    async fn overwriting_without_a_prior_read_is_refused_with_the_read_first_message() {
        let dir = TempDir::new().expect("tmp");
        fs::write(dir.path().join("f.txt"), "original").expect("write");
        let ws = workspace();
        let tool = Write::new(ws);

        let path = dir.path().join("f.txt");
        let reply = tool
            .execute(
                write_args(&path, "new"),
                ctx("s-1", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(!reply.ok);
        assert!(
            reply.content.contains("has not been read"),
            "{}",
            reply.content
        );
        assert_eq!(fs::read_to_string(&path).expect("unchanged"), "original");
    }

    #[tokio::test]
    async fn overwriting_after_a_read_succeeds() {
        let dir = TempDir::new().expect("tmp");
        fs::write(dir.path().join("f.txt"), "original").expect("write");
        let ws = workspace();
        let read_tool = Read::new(Arc::clone(&ws));
        let write_tool = Write::new(Arc::clone(&ws));

        let path = dir.path().join("f.txt");
        let read_reply = read_tool
            .execute(read_args(&path), ctx("s-1", dir.path(), Mode::ReadWrite))
            .await;
        assert!(read_reply.ok, "{}", read_reply.content);

        let write_reply = write_tool
            .execute(
                write_args(&path, "updated"),
                ctx("s-1", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(write_reply.ok, "{}", write_reply.content);
        assert_eq!(fs::read_to_string(&path).expect("read back"), "updated");
    }

    #[tokio::test]
    async fn overwriting_after_a_read_but_the_file_changed_underneath_is_refused() {
        let dir = TempDir::new().expect("tmp");
        let path = dir.path().join("f.txt");
        fs::write(&path, "original").expect("write");
        let ws = workspace();
        let read_tool = Read::new(Arc::clone(&ws));
        let write_tool = Write::new(Arc::clone(&ws));

        let read_reply = read_tool
            .execute(read_args(&path), ctx("s-1", dir.path(), Mode::ReadWrite))
            .await;
        assert!(read_reply.ok, "{}", read_reply.content);

        fs::write(&path, "changed by someone else").expect("write underneath");

        let write_reply = write_tool
            .execute(
                write_args(&path, "my update"),
                ctx("s-1", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(!write_reply.ok);
        assert!(
            write_reply.content.contains("changed since"),
            "{}",
            write_reply.content
        );
        assert_eq!(
            fs::read_to_string(&path).expect("unchanged"),
            "changed by someone else"
        );
    }

    #[tokio::test]
    async fn writing_into_a_read_only_grant_is_the_gates_refusal() {
        let dir = TempDir::new().expect("tmp");
        fs::write(dir.path().join("f.txt"), "x").expect("write");
        let ws = workspace();
        let tool = Write::new(ws);

        let path = dir.path().join("f.txt");
        let reply = tool
            .execute(
                write_args(&path, "y"),
                ctx("s-1", dir.path(), Mode::ReadOnly),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("read-only"), "{}", reply.content);
    }

    #[tokio::test]
    async fn writing_outside_all_grants_is_refused() {
        let dir = TempDir::new().expect("tmp");
        let elsewhere = TempDir::new().expect("tmp2");
        let ws = workspace();
        let tool = Write::new(ws);

        let path = elsewhere.path().join("f.txt");
        let reply = tool
            .execute(
                write_args(&path, "y"),
                ctx("s-1", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("outside"), "{}", reply.content);
    }

    #[tokio::test]
    async fn writing_with_a_nonexistent_parent_is_the_gates_error() {
        let dir = TempDir::new().expect("tmp");
        let ws = workspace();
        let tool = Write::new(ws);

        let path = dir.path().join("missing_dir").join("f.txt");
        let reply = tool
            .execute(
                write_args(&path, "y"),
                ctx("s-1", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("parent"), "{}", reply.content);
    }

    #[tokio::test]
    async fn writing_then_immediately_editing_succeeds_off_the_re_recorded_hash() {
        let dir = TempDir::new().expect("tmp");
        let ws = workspace();
        let write_tool = Write::new(Arc::clone(&ws));
        let edit_tool = crate::tool::workspace::edit::Edit::new(Arc::clone(&ws));

        let path = dir.path().join("f.txt");
        let write_reply = write_tool
            .execute(
                write_args(&path, "hello"),
                ctx("s-1", dir.path(), Mode::ReadWrite),
            )
            .await;
        assert!(write_reply.ok, "{}", write_reply.content);

        let edit_request = serde_json::json!({
            "path": path,
            "old": "hello",
            "new": "goodbye"
        })
        .to_string();
        let edit_reply = edit_tool
            .execute(edit_request, ctx("s-1", dir.path(), Mode::ReadWrite))
            .await;

        assert!(edit_reply.ok, "{}", edit_reply.content);
        assert_eq!(fs::read_to_string(&path).expect("read back"), "goodbye");
    }

    #[tokio::test]
    async fn writing_empty_content_is_allowed() {
        let dir = TempDir::new().expect("tmp");
        let ws = workspace();
        let tool = Write::new(ws);

        let path = dir.path().join("empty.txt");
        let reply = tool
            .execute(
                write_args(&path, ""),
                ctx("s-1", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).expect("read back"), "");
    }

    #[tokio::test]
    async fn the_write_tool_dispatches_through_the_registry_by_source() {
        let dir = TempDir::new().expect("tmp");
        let ws = workspace();
        let mut registry = Registry::new(32 * 1024);
        registry.register(Box::new(Write::new(ws)));

        let path = dir.path().join("f.txt");
        let request = write_args(&path, "hi");

        let present = registry
            .dispatch(
                "write",
                request.clone(),
                ctx("s-1", dir.path(), Mode::ReadWrite),
                &[ToolSource::Workspace],
            )
            .await;
        assert!(present.ok, "{}", present.content);

        fs::remove_file(&path).expect("cleanup");
        let absent = registry
            .dispatch(
                "write",
                request,
                ctx("s-1", dir.path(), Mode::ReadWrite),
                &[ToolSource::Builtin],
            )
            .await;
        assert!(!absent.ok);
        assert_eq!(
            absent.content,
            "ERROR: Tool write is not available in this session."
        );
    }
}
