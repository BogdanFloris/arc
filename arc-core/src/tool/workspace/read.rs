use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;

use super::{Access, Workspace};
use crate::provider::ToolDefinition;
use crate::tool::{Tool, ToolReply, ToolSource, TurnContext};

const DEFAULT_LIMIT: usize = 400;

pub struct Read {
    workspace: Arc<Workspace>,
}

impl Read {
    pub fn new(workspace: Arc<Workspace>) -> Self {
        Self { workspace }
    }
}

#[derive(Deserialize)]
struct ReadArgs {
    path: String,
    offset: Option<usize>,
    limit: Option<usize>,
}

impl Tool for Read {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "read".to_owned(),
            description: "Read a file's contents. path must be absolute. Large files page: \
                          offset is the 1-based first line to return, limit caps how many \
                          lines come back (default 400). A page that does not reach the end \
                          of the file ends with a marker naming the offset to continue from."
                .to_owned(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute path to the file."},
                    "offset": {"type": "integer", "description": "1-based first line to return."},
                    "limit": {"type": "integer", "description": "Max lines to return. Default 400."}
                },
                "required": ["path"]
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
            let args: ReadArgs = match serde_json::from_str(&arguments_json) {
                Ok(args) => args,
                Err(error) => {
                    return ToolReply::error(format!(
                        "ERROR: bad read arguments ({error}). Pass {{\"path\": \"/abs/path\"}}."
                    ));
                }
            };

            let resolved = match self.workspace.grants.resolve(&args.path, Access::Read) {
                Ok(path) => path,
                Err(reason) => return ToolReply::error(format!("ERROR: {reason}")),
            };

            if resolved.is_dir() {
                return ToolReply::error(format!(
                    "ERROR: {} is a directory, not a file.",
                    resolved.display()
                ));
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

            let window = match page(text, args.offset, args.limit) {
                Ok(window) => window,
                Err(reason) => return ToolReply::error(format!("ERROR: {reason}")),
            };

            self.workspace
                .record_read(&ctx.session_id, &resolved, &bytes);
            ToolReply::ok(window)
        })
    }
}

fn page(text: &str, offset: Option<usize>, limit: Option<usize>) -> Result<String, String> {
    let lines: Vec<&str> = text.lines().collect();
    let total = lines.len();
    if total == 0 {
        return Ok(String::new());
    }

    let first = offset.unwrap_or(1).max(1);
    if first > total {
        return Err(format!(
            "offset {first} is beyond the file's {total} lines."
        ));
    }
    let limit = limit.unwrap_or(DEFAULT_LIMIT).max(1);
    let last = (first + limit - 1).min(total);
    let body = lines[first - 1..last].join("\n");

    if first == 1 && last == total {
        return Ok(body);
    }
    Ok(format!(
        "{body}\n[lines {first}-{last} of {total}; continue with offset={next}]",
        next = last + 1
    ))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::Read;
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

    fn args(path: &std::path::Path) -> String {
        serde_json::json!({ "path": path }).to_string()
    }

    #[tokio::test]
    async fn a_happy_read_returns_the_file_verbatim() {
        let dir = TempDir::new().expect("tmp");
        fs::write(dir.path().join("f.txt"), "line one\nline two").expect("write");
        let ws = workspace(dir.path(), Mode::ReadOnly);
        let tool = Read::new(ws);

        let reply = tool
            .execute(args(&dir.path().join("f.txt")), ctx("s-1"))
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(reply.content, "line one\nline two");
    }

    #[tokio::test]
    async fn a_paginated_window_ends_with_the_continue_marker() {
        let dir = TempDir::new().expect("tmp");
        let lines: Vec<String> = (1..=10).map(|n| format!("line {n}")).collect();
        fs::write(dir.path().join("f.txt"), lines.join("\n")).expect("write");
        let ws = workspace(dir.path(), Mode::ReadOnly);
        let tool = Read::new(ws);

        let request = serde_json::json!({
            "path": dir.path().join("f.txt"),
            "offset": 1,
            "limit": 3
        })
        .to_string();
        let reply = tool.execute(request, ctx("s-1")).await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(
            reply.content,
            "line 1\nline 2\nline 3\n[lines 1-3 of 10; continue with offset=4]"
        );
    }

    #[tokio::test]
    async fn an_offset_past_the_end_of_file_is_a_named_error() {
        let dir = TempDir::new().expect("tmp");
        fs::write(dir.path().join("f.txt"), "only line").expect("write");
        let ws = workspace(dir.path(), Mode::ReadOnly);
        let tool = Read::new(ws);

        let request = serde_json::json!({
            "path": dir.path().join("f.txt"),
            "offset": 50
        })
        .to_string();
        let reply = tool.execute(request, ctx("s-1")).await;

        assert!(!reply.ok);
        assert!(reply.content.contains("beyond"), "{}", reply.content);
        assert!(reply.content.contains('1'), "{}", reply.content);
    }

    #[tokio::test]
    async fn reading_a_directory_is_a_named_error() {
        let dir = TempDir::new().expect("tmp");
        fs::create_dir_all(dir.path().join("sub")).expect("mkdir");
        let ws = workspace(dir.path(), Mode::ReadOnly);
        let tool = Read::new(ws);

        let reply = tool
            .execute(args(&dir.path().join("sub")), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("directory"), "{}", reply.content);
    }

    #[tokio::test]
    async fn non_utf8_bytes_are_a_named_error() {
        let dir = TempDir::new().expect("tmp");
        fs::write(dir.path().join("f.bin"), [0xff, 0xfe, 0x00, 0xff]).expect("write");
        let ws = workspace(dir.path(), Mode::ReadOnly);
        let tool = Read::new(ws);

        let reply = tool
            .execute(args(&dir.path().join("f.bin")), ctx("s-1"))
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("text"), "{}", reply.content);
    }

    #[tokio::test]
    async fn a_missing_file_is_a_named_error() {
        let dir = TempDir::new().expect("tmp");
        let ws = workspace(dir.path(), Mode::ReadOnly);
        let tool = Read::new(ws);

        let path = dir.path().join("nope.txt");
        let reply = tool.execute(args(&path), ctx("s-1")).await;

        assert!(!reply.ok);
        assert!(
            reply.content.contains(path.to_str().expect("utf8")),
            "{}",
            reply.content
        );
    }

    #[tokio::test]
    async fn a_successful_read_records_a_hash_for_the_session() {
        let dir = TempDir::new().expect("tmp");
        fs::write(dir.path().join("f.txt"), "hello").expect("write");
        let ws = workspace(dir.path(), Mode::ReadOnly);
        let tool = Read::new(Arc::clone(&ws));

        let path = dir.path().join("f.txt");
        let reply = tool.execute(args(&path), ctx("s-1")).await;
        assert!(reply.ok);

        let canonical = path.canonicalize().expect("canonicalize");
        assert!(ws.recorded_hash("s-1", &canonical).is_some());
    }

    #[tokio::test]
    async fn two_sessions_record_independent_hash_entries() {
        let dir = TempDir::new().expect("tmp");
        fs::write(dir.path().join("f.txt"), "hello").expect("write");
        let ws = workspace(dir.path(), Mode::ReadOnly);
        let tool = Read::new(Arc::clone(&ws));

        let path = dir.path().join("f.txt");
        tool.execute(args(&path), ctx("s-1")).await;
        tool.execute(args(&path), ctx("s-2")).await;

        let canonical = path.canonicalize().expect("canonicalize");
        assert!(ws.recorded_hash("s-1", &canonical).is_some());
        assert!(ws.recorded_hash("s-2", &canonical).is_some());
    }

    #[tokio::test]
    async fn the_read_tool_dispatches_through_the_registry_by_source() {
        let dir = TempDir::new().expect("tmp");
        fs::write(dir.path().join("f.txt"), "hello").expect("write");
        let ws = workspace(dir.path(), Mode::ReadOnly);
        let mut registry = Registry::new(32 * 1024);
        registry.register(Box::new(Read::new(ws)));

        let request = args(&dir.path().join("f.txt"));

        let present = registry
            .dispatch(
                "read",
                request.clone(),
                ctx("s-1"),
                &[ToolSource::Workspace],
            )
            .await;
        assert!(present.ok, "{}", present.content);

        let absent = registry
            .dispatch("read", request, ctx("s-1"), &[ToolSource::Builtin])
            .await;
        assert!(!absent.ok);
        assert_eq!(
            absent.content,
            "ERROR: Tool read is not available in this session."
        );
    }
}
