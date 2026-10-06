use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;

use super::{Workspace, ensure_fresh, resolve_path, to_crlf, to_lf, uses_crlf};
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
    #[serde(alias = "file_path")]
    path: String,
    #[serde(default, deserialize_with = "replacement_shapes")]
    replacements: Option<Vec<Replacement>>,
    #[serde(alias = "old_string")]
    old: Option<String>,
    #[serde(alias = "new_string")]
    new: Option<String>,
}

#[derive(Deserialize)]
struct Replacement {
    #[serde(alias = "old_string")]
    old: String,
    #[serde(alias = "new_string")]
    new: String,
}

fn replacement_shapes<'de, D>(deserializer: D) -> Result<Option<Vec<Replacement>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    fn shapes(value: serde_json::Value) -> Result<Vec<Replacement>, serde_json::Error> {
        let value = match value {
            serde_json::Value::String(text) => serde_json::from_str(&text)?,
            other => other,
        };
        match value {
            serde_json::Value::Array(items) => {
                items.into_iter().map(serde_json::from_value).collect()
            }
            object @ serde_json::Value::Object(_) => {
                serde_json::from_value(object).map(|one| vec![one])
            }
            other => Err(serde::de::Error::custom(format!(
                "replacements must be an array or an object, not {other}"
            ))),
        }
    }

    Option::<serde_json::Value>::deserialize(deserializer)?
        .map(|value| shapes(value).map_err(serde::de::Error::custom))
        .transpose()
}

impl Tool for Edit {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "edit".to_owned(),
            description: "Replace several non-overlapping spans in one file. path (or \
                          file_path) must be absolute. Each replacement's old (or old_string) \
                          text must appear exactly once in the original file; include context \
                          to make it unique. replacements accepts an array of {old, new} \
                          objects, a single object, or a JSON string holding either; the \
                          legacy top-level old and new (or old_string and new_string) also \
                          work. All replacements \
                          are validated before writing. A file whose newlines are all CRLF \
                          keeps them. Without an exact match, whole lines are compared \
                          ignoring trailing whitespace; a match that ignores indentation too \
                          is reported with the file's exact lines and not written. Requires \
                          having read the file using the `read` tool in this session, with no \
                          changes since. Reading through Bash does not count."
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

            let crlf = uses_crlf(text);
            let hay = if crlf {
                to_lf(text)
            } else {
                Cow::Borrowed(text)
            };
            let mut spans = Vec::with_capacity(replacements.len());
            for replacement in &replacements {
                let (old, new) = if crlf {
                    (
                        to_lf(&replacement.old).into_owned(),
                        to_lf(&replacement.new).into_owned(),
                    )
                } else {
                    (replacement.old.clone(), replacement.new.clone())
                };
                if old.is_empty() {
                    return ToolReply::error("ERROR: old must not be empty.".to_owned());
                }
                if old == new {
                    return ToolReply::error("ERROR: old and new must be different.".to_owned());
                }
                let mut matches = hay.match_indices(&old);
                let Some((start, _)) = matches.next() else {
                    match locate_loosely(&hay, &old) {
                        Loose::WholeLine(start, end) => {
                            spans.push((start, end, new));
                            continue;
                        }
                        Loose::Hint(first, last, text) => {
                            return ToolReply::error(format!(
                                "ERROR: old text was not found in {}; ignoring leading and \
                                 trailing whitespace, lines {first}-{last} match. Retry with \
                                 this exact text as old: {text}",
                                resolved.display()
                            ));
                        }
                        Loose::None => {
                            return ToolReply::error(format!(
                                "ERROR: old text was not found in {}.",
                                resolved.display()
                            ));
                        }
                    }
                };
                if matches.next().is_some() {
                    let occurrences = 2 + matches.count();
                    return ToolReply::error(format!(
                        "ERROR: old text appears {occurrences} times in {}; include more \
                         surrounding context to make the match unique.",
                        resolved.display()
                    ));
                }
                spans.push((start, start + old.len(), new));
            }
            spans.sort_unstable_by_key(|span| span.0);
            if spans.windows(2).any(|pair| pair[0].1 > pair[1].0) {
                return ToolReply::error(
                    "ERROR: replacements overlap in the original file.".to_owned(),
                );
            }
            let mut updated = String::with_capacity(hay.len());
            let mut end = 0;
            for (start, next, replacement) in spans {
                updated.push_str(&hay[end..start]);
                updated.push_str(&replacement);
                end = next;
            }
            updated.push_str(&hay[end..]);
            let updated_bytes = if crlf {
                to_crlf(&updated).into_owned().into_bytes()
            } else {
                updated.into_bytes()
            };
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

enum Loose {
    WholeLine(usize, usize),
    Hint(usize, usize, String),
    None,
}

fn locate_loosely(hay: &str, old: &str) -> Loose {
    let trailing = line_windows(hay, old, &|line, want| line.trim_end() == want.trim_end());
    if let [span] = trailing[..] {
        return Loose::WholeLine(span.0, span.1);
    }
    let loose = line_windows(hay, old, &|line, want| line.trim() == want.trim());
    if let [span] = loose[..] {
        let quoted = serde_json::to_string(&hay[span.0..span.1]).unwrap_or_default();
        let first = hay[..span.0].matches('\n').count() + 1;
        let last = first + old.split('\n').count() - usize::from(old.ends_with('\n')) - 1;
        return Loose::Hint(first, last, quoted);
    }
    Loose::None
}

fn line_windows(hay: &str, old: &str, equal: &dyn Fn(&str, &str) -> bool) -> Vec<(usize, usize)> {
    let mut lines: Vec<(usize, &str)> = Vec::new();
    let mut offset = 0;
    for line in hay.split('\n') {
        lines.push((offset, line));
        offset += line.len() + 1;
    }
    if hay.ends_with('\n') {
        lines.pop();
    }
    let mut pattern: Vec<&str> = old.split('\n').collect();
    if pattern.last() == Some(&"") {
        pattern.pop();
    }
    if pattern.is_empty() || pattern.len() > lines.len() {
        return Vec::new();
    }
    let mut spans = Vec::new();
    for start in 0..=lines.len() - pattern.len() {
        let matches = pattern
            .iter()
            .enumerate()
            .all(|(k, want)| equal(lines[start + k].1, want));
        if !matches {
            continue;
        }
        let last = &lines[start + pattern.len() - 1];
        let mut end = last.0 + last.1.len();
        if old.ends_with('\n') && hay[end..].starts_with('\n') {
            end += 1;
        }
        spans.push((lines[start].0, end));
    }
    spans
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

    async fn read_first(root: &std::path::Path, path: &std::path::Path) -> Arc<Workspace> {
        let ws = workspace();
        Read::new(Arc::clone(&ws))
            .execute(read_args(path), ctx("s", root, Mode::ReadWrite))
            .await;
        ws
    }

    #[tokio::test]
    async fn a_stringified_replacement_array_is_accepted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "alpha beta").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let request = serde_json::json!({
            "path": path,
            "replacements": r#"[{"old": "alpha", "new": "A"}, {"old": "beta", "new": "B"}]"#,
        })
        .to_string();
        let reply = Edit::new(ws)
            .execute(request, ctx("s", dir.path(), Mode::ReadWrite))
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A B");
    }

    #[tokio::test]
    async fn a_stringified_replacement_object_is_accepted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "alpha beta").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let request = serde_json::json!({
            "path": path,
            "replacements": r#"{"old": "alpha", "new": "A"}"#,
        })
        .to_string();
        let reply = Edit::new(ws)
            .execute(request, ctx("s", dir.path(), Mode::ReadWrite))
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A beta");
    }

    #[tokio::test]
    async fn a_bare_replacement_object_is_accepted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "alpha beta").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let request = serde_json::json!({
            "path": path,
            "replacements": {"old": "alpha", "new": "A"},
        })
        .to_string();
        let reply = Edit::new(ws)
            .execute(request, ctx("s", dir.path(), Mode::ReadWrite))
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A beta");
    }

    #[tokio::test]
    async fn replacement_item_aliases_are_accepted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "alpha beta").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let request = serde_json::json!({
            "path": path,
            "replacements": [{"old_string": "alpha", "new_string": "A"}],
        })
        .to_string();
        let reply = Edit::new(ws)
            .execute(request, ctx("s", dir.path(), Mode::ReadWrite))
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A beta");
    }

    #[tokio::test]
    async fn file_path_and_the_legacy_string_aliases_are_accepted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "alpha beta").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let request = serde_json::json!({
            "file_path": path,
            "old_string": "alpha",
            "new_string": "A",
        })
        .to_string();
        let reply = Edit::new(ws)
            .execute(request, ctx("s", dir.path(), Mode::ReadWrite))
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A beta");
    }

    #[tokio::test]
    async fn a_replacements_string_that_is_not_json_is_refused() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "alpha beta").unwrap();
        let request = serde_json::json!({"path": path, "replacements": "not json"}).to_string();

        let reply = Edit::new(workspace())
            .execute(request, ctx("s", dir.path(), Mode::ReadWrite))
            .await;

        assert!(!reply.ok);
        assert!(
            reply.content.contains("bad edit arguments"),
            "{}",
            reply.content
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "alpha beta");
    }

    #[tokio::test]
    async fn editing_a_crlf_file_keeps_its_line_endings() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "a\r\nb\r\nc\r\n").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let reply = Edit::new(ws)
            .execute(
                edit_args(&path, "a\nb", "A\nB"),
                ctx("s", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A\r\nB\r\nc\r\n");
    }

    #[tokio::test]
    async fn crlf_in_old_and_new_is_accepted_for_a_crlf_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "a\r\nb\r\nc\r\n").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let reply = Edit::new(ws)
            .execute(
                edit_args(&path, "a\r\nb", "A\r\nB"),
                ctx("s", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A\r\nB\r\nc\r\n");
    }

    #[tokio::test]
    async fn a_second_edit_after_a_crlf_write_needs_no_new_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "a\r\nb\r\nc\r\n").unwrap();
        let ws = read_first(dir.path(), &path).await;
        let tool = Edit::new(ws);

        let first = tool
            .execute(
                edit_args(&path, "a\nb", "A\nB"),
                ctx("s", dir.path(), Mode::ReadWrite),
            )
            .await;
        assert!(first.ok, "{}", first.content);

        let second = tool
            .execute(
                edit_args(&path, "c", "C"),
                ctx("s", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(second.ok, "{}", second.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A\r\nB\r\nC\r\n");
    }

    #[tokio::test]
    async fn mixed_line_endings_are_matched_as_before() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "a\nb\r\nc\r\n").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let reply = Edit::new(ws)
            .execute(
                edit_args(&path, "a\nb", "A\nB"),
                ctx("s", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A\nB\r\nc\r\n");
    }

    #[tokio::test]
    async fn a_trailing_space_difference_applies_automatically() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "a \nb\nc\n").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let reply = Edit::new(ws)
            .execute(
                edit_args(&path, "a\nb", "A\nB"),
                ctx("s", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A\nB\nc\n");
    }

    #[tokio::test]
    async fn the_span_includes_the_newline_when_old_ends_with_one() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "a \nb\nc\n").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let reply = Edit::new(ws)
            .execute(
                edit_args(&path, "a\nb\n", "X\n"),
                ctx("s", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "X\nc\n");
    }

    #[tokio::test]
    async fn tabs_versus_spaces_returns_the_exact_text_and_writes_nothing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "a\n\tb\nc\n").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let reply = Edit::new(ws)
            .execute(
                edit_args(&path, "a\n  b\n", "A\nB\n"),
                ctx("s", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("lines 1-2"), "{}", reply.content);
        assert!(reply.content.contains("a\\n\\tb"), "{}", reply.content);
        assert!(reply.changed_paths.is_empty());
        assert_eq!(fs::read_to_string(&path).unwrap(), "a\n\tb\nc\n");
    }

    #[tokio::test]
    async fn two_whitespace_only_candidates_are_not_found() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "x\ny\nx\ny\n").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let reply = Edit::new(ws)
            .execute(
                edit_args(&path, "x \ny ", "y"),
                ctx("s", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(!reply.ok);
        assert!(reply.content.contains("not found"), "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "x\ny\nx\ny\n");
    }

    #[tokio::test]
    async fn a_fallback_span_that_overlaps_another_replacement_still_fails() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "a \nb\n").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let reply = Edit::new(ws)
            .execute(
                batch(&path, &[("a\nb", "X"), ("b", "Y")]),
                ctx("s", dir.path(), Mode::ReadWrite),
            )
            .await;

        assert!(
            !reply.ok && reply.content.contains("overlap"),
            "{}",
            reply.content
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "a \nb\n");
    }

    #[tokio::test]
    async fn a_null_replacements_leaves_the_legacy_shape_working() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f.txt");
        fs::write(&path, "alpha beta").unwrap();
        let ws = read_first(dir.path(), &path).await;

        let request = serde_json::json!({
            "path": path,
            "replacements": null,
            "old": "alpha",
            "new": "A",
        })
        .to_string();
        let reply = Edit::new(ws)
            .execute(request, ctx("s", dir.path(), Mode::ReadWrite))
            .await;

        assert!(reply.ok, "{}", reply.content);
        assert_eq!(fs::read_to_string(&path).unwrap(), "A beta");
    }
}
