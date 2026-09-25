pub mod bash;
pub mod edit;
pub mod patch;
pub mod read;
pub mod write;

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::tool::Tool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone)]
pub struct Grant {
    pub root: PathBuf,
    pub mode: Mode,
}

impl Grant {
    pub fn new(root: impl Into<PathBuf>, mode: Mode) -> Self {
        Self {
            root: root.into(),
            mode,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Read,
    Write,
}

#[derive(Debug)]
pub struct Grants {
    roots: Vec<(PathBuf, Mode)>,
}

impl Grants {
    pub fn new(grants: Vec<Grant>) -> io::Result<Self> {
        let roots = grants
            .into_iter()
            .map(|grant| std::fs::canonicalize(&grant.root).map(|root| (root, grant.mode)))
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self { roots })
    }

    pub fn from_recorded(roots: Vec<(PathBuf, Mode)>) -> Grants {
        Self { roots }
    }

    pub fn canonical_roots(&self) -> &[(PathBuf, Mode)] {
        &self.roots
    }

    pub fn resolve(&self, path: &str, _access: Access) -> Result<PathBuf, String> {
        resolve_path(path)
    }

    pub fn project_root(&self) -> Option<&Path> {
        self.roots.first().map(|(root, _)| root.as_path())
    }
}

pub(crate) fn resolve_path(path: &str) -> Result<PathBuf, String> {
    let requested = Path::new(path);
    if !requested.is_absolute() {
        return Err(format!(
            "\"{path}\" is not an absolute path. Use an absolute path."
        ));
    }
    if path.ends_with("/.") || path.ends_with("/..") || requested.file_name().is_none() {
        return Err(format!("{path} does not name a file."));
    }
    let mut ancestor = requested;
    let mut remainder = Vec::new();
    loop {
        match std::fs::canonicalize(ancestor) {
            Ok(mut resolved) => {
                for name in remainder.iter().rev() {
                    resolved.push(name);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let name = ancestor
                    .file_name()
                    .ok_or_else(|| format!("could not resolve {path} ({error})."))?;
                if name == ".." || name == "." {
                    return Err(format!("could not resolve {path} ({error})."));
                }
                remainder.push(name.to_owned());
                ancestor = ancestor
                    .parent()
                    .ok_or_else(|| format!("could not resolve {path} ({error})."))?;
            }
            Err(error) => return Err(format!("could not resolve {path} ({error}).")),
        }
    }
}

pub struct Workspace {
    reads: Mutex<HashMap<(String, PathBuf), u64>>,
}

impl Default for Workspace {
    fn default() -> Self {
        Self::new()
    }
}

impl Workspace {
    pub fn new() -> Self {
        Self {
            reads: Mutex::new(HashMap::new()),
        }
    }

    fn record_read(&self, session_id: &str, path: &Path, bytes: &[u8]) {
        self.reads
            .lock()
            .expect("reads lock poisoned")
            .insert((session_id.to_owned(), path.to_path_buf()), hash_of(bytes));
    }

    pub(crate) fn recorded_hash(&self, session_id: &str, path: &Path) -> Option<u64> {
        self.reads
            .lock()
            .expect("reads lock poisoned")
            .get(&(session_id.to_owned(), path.to_path_buf()))
            .copied()
    }
}

pub(crate) fn hash_of(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// A tool that modifies an existing file must be working from a fresh read.
pub(crate) fn ensure_fresh(
    workspace: &Workspace,
    session_id: &str,
    path: &Path,
    current_bytes: &[u8],
) -> Result<(), String> {
    match workspace.recorded_hash(session_id, path) {
        None => Err(format!(
            "{} has not been read in this session. Read it using the `read` tool before \
             modifying it. Reading through Bash does not count.",
            path.display()
        )),
        Some(hash) if hash != hash_of(current_bytes) => Err(format!(
            "{} has changed since it was last read in this session. Read it again using \
             the `read` tool before modifying it. Reading through Bash does not count.",
            path.display()
        )),
        Some(_) => Ok(()),
    }
}

pub fn tools(workspace: Arc<Workspace>) -> Vec<Box<dyn Tool>> {
    vec![
        Box::new(bash::Bash::new()),
        Box::new(edit::Edit::new(Arc::clone(&workspace))),
        Box::new(read::Read::new(Arc::clone(&workspace))),
        Box::new(write::Write::new(Arc::clone(&workspace))),
        Box::new(patch::ApplyPatch::new(workspace)),
    ]
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::symlink;

    use tempfile::TempDir;

    use super::{Workspace, resolve_path};
    use crate::tool::ToolSource;

    #[test]
    fn freshness_errors_name_the_read_tool() {
        let workspace = Workspace::new();
        let path = std::path::Path::new("/workspace/file");
        let unread = super::ensure_fresh(&workspace, "s1", path, b"old").unwrap_err();
        workspace.record_read("s1", path, b"old");
        let stale = super::ensure_fresh(&workspace, "s1", path, b"new").unwrap_err();
        for error in [unread, stale] {
            assert!(error.contains("the `read` tool"), "{error}");
            assert!(
                error.contains("Reading through Bash does not count."),
                "{error}"
            );
        }
    }

    #[test]
    fn the_workspace_tools_have_distinct_editing_sources() {
        let tools = super::tools(std::sync::Arc::new(Workspace::new()));

        let names: Vec<String> = tools.iter().map(|tool| tool.definition().name).collect();
        assert_eq!(names, ["bash", "edit", "read", "write", "apply_patch"]);
        assert_eq!(
            tools.iter().map(|tool| tool.source()).collect::<Vec<_>>(),
            [
                ToolSource::Workspace,
                ToolSource::Replacement,
                ToolSource::Workspace,
                ToolSource::Replacement,
                ToolSource::Patch,
            ]
        );
    }

    #[test]
    fn paths_are_absolute_and_resolve_symlinks_outside_any_root() {
        let dir = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let file = outside.path().join("file.txt");
        fs::write(&file, "hello").unwrap();
        symlink(&file, dir.path().join("link")).unwrap();
        symlink(outside.path(), dir.path().join("linked_dir")).unwrap();
        assert_eq!(resolve_path(file.to_str().unwrap()).unwrap(), file);
        assert_eq!(
            resolve_path(dir.path().join("link").to_str().unwrap()).unwrap(),
            file
        );
        assert_eq!(
            resolve_path(dir.path().join("linked_dir/new.txt").to_str().unwrap()).unwrap(),
            outside.path().join("new.txt")
        );
        assert!(resolve_path("relative").unwrap_err().contains("absolute"));
        assert!(resolve_path("").unwrap_err().contains("absolute"));
    }

    #[test]
    fn missing_ancestors_resolve_but_missing_dotdot_is_rejected() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("new/child.txt");
        assert_eq!(resolve_path(nested.to_str().unwrap()).unwrap(), nested);
        assert!(resolve_path(dir.path().join("new/../child.txt").to_str().unwrap()).is_err());
    }
}
