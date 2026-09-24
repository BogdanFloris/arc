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

/// Every path-taking tool goes through `resolve`; a tool that skips it is a bug.
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

    /// Roots recorded in the log are already canonical; a root that has since
    /// drifted still fails closed because `resolve` canonicalizes the request.
    pub fn from_recorded(roots: Vec<(PathBuf, Mode)>) -> Grants {
        Self { roots }
    }

    pub fn canonical_roots(&self) -> &[(PathBuf, Mode)] {
        &self.roots
    }

    pub fn resolve(&self, path: &str, access: Access) -> Result<PathBuf, String> {
        if path.is_empty() {
            return Err("path is empty. Use an absolute path.".to_owned());
        }
        let requested = Path::new(path);
        if !requested.is_absolute() {
            return Err(format!(
                "\"{path}\" is not an absolute path. Use an absolute path."
            ));
        }

        let canonical = if requested.exists() {
            std::fs::canonicalize(requested)
                .map_err(|error| format!("could not resolve {path} ({error})."))?
        } else {
            let parent = requested
                .parent()
                .ok_or_else(|| format!("{path} has no parent directory."))?;
            let canonical_parent = std::fs::canonicalize(parent).map_err(|_| {
                format!("the parent directory {} does not exist.", parent.display())
            })?;
            let file_name = requested.file_name().ok_or_else(|| {
                format!("{path} does not name a file; \".\" and \"..\" are not allowed here.")
            })?;
            canonical_parent.join(file_name)
        };

        let mode = std::fs::canonicalize("/tmp")
            .ok()
            .filter(|tmp| canonical.starts_with(tmp))
            .map(|_| Mode::ReadWrite)
            .or_else(|| {
                self.roots
                    .iter()
                    .find(|(root, _)| canonical.starts_with(root))
                    .map(|(_, mode)| *mode)
            })
            .ok_or_else(|| "that path is outside the session's granted roots.".to_owned())?;

        if access == Access::Write && mode == Mode::ReadOnly {
            return Err(format!(
                "{} is read-only in this session.",
                canonical.display()
            ));
        }

        Ok(canonical)
    }

    /// The bash tool's working directory: the project root, whatever its
    /// mode. `bash` ignores grant modes (DESIGN §4.3), so an analyze job's
    /// read-only root is still where it runs, not an error.
    pub fn project_root(&self) -> Option<&Path> {
        self.roots.first().map(|(root, _)| root.as_path())
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

    use super::{Access, Grant, Grants, Mode, Workspace};
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

    fn proj(dir: &TempDir) -> std::path::PathBuf {
        let root = dir.path().join("proj");
        fs::create_dir_all(&root).expect("mkdir proj");
        root
    }

    fn non_tmp_dir() -> TempDir {
        TempDir::new_in(env!("CARGO_MANIFEST_DIR")).expect("temp dir outside /tmp")
    }

    fn grants(root: &std::path::Path, mode: Mode) -> Grants {
        Grants::new(vec![Grant::new(root, mode)]).expect("canonicalize grant")
    }

    #[test]
    fn a_dotdot_escape_is_refused() {
        let dir = non_tmp_dir();
        let root = proj(&dir);
        fs::write(dir.path().join("outside.txt"), b"secret").expect("write");
        let grants = grants(&root, Mode::ReadOnly);

        let path = root.join("..").join("outside.txt");
        let err = grants
            .resolve(path.to_str().expect("utf8"), Access::Read)
            .unwrap_err();
        assert!(err.contains("outside"), "{err}");
    }

    #[test]
    fn an_absolute_path_under_no_grant_is_refused() {
        let dir = TempDir::new().expect("tmp");
        let root = proj(&dir);
        let elsewhere = non_tmp_dir();
        fs::write(elsewhere.path().join("f.txt"), b"x").expect("write");
        let grants = grants(&root, Mode::ReadOnly);

        let target = elsewhere.path().join("f.txt");
        let err = grants
            .resolve(target.to_str().expect("utf8"), Access::Read)
            .unwrap_err();
        assert!(err.contains("outside"), "{err}");
    }

    #[test]
    fn a_symlink_inside_the_root_pointing_outside_is_refused() {
        let dir = non_tmp_dir();
        let root = proj(&dir);
        let outside = dir.path().join("secret.txt");
        fs::write(&outside, b"secret").expect("write");
        symlink(&outside, root.join("link.txt")).expect("symlink");
        let grants = grants(&root, Mode::ReadOnly);

        let target = root.join("link.txt");
        let err = grants
            .resolve(target.to_str().expect("utf8"), Access::Read)
            .unwrap_err();
        assert!(err.contains("outside"), "{err}");
    }

    #[test]
    fn a_path_through_a_symlinked_directory_leading_outside_is_refused() {
        let dir = non_tmp_dir();
        let root = proj(&dir);
        let outside_dir = dir.path().join("outside_dir");
        fs::create_dir_all(&outside_dir).expect("mkdir");
        fs::write(outside_dir.join("f.txt"), b"secret").expect("write");
        symlink(&outside_dir, root.join("linked_dir")).expect("symlink");
        let grants = grants(&root, Mode::ReadOnly);

        let target = root.join("linked_dir").join("f.txt");
        let err = grants
            .resolve(target.to_str().expect("utf8"), Access::Read)
            .unwrap_err();
        assert!(err.contains("outside"), "{err}");
    }

    #[test]
    fn a_sibling_directory_with_a_matching_prefix_is_refused() {
        let dir = non_tmp_dir();
        let root = proj(&dir);
        let evil = dir.path().join("proj-evil");
        fs::create_dir_all(&evil).expect("mkdir");
        fs::write(evil.join("f.txt"), b"x").expect("write");
        let grants = grants(&root, Mode::ReadOnly);

        let target = evil.join("f.txt");
        let err = grants
            .resolve(target.to_str().expect("utf8"), Access::Read)
            .unwrap_err();
        assert!(err.contains("outside"), "{err}");
    }

    #[test]
    fn a_relative_or_empty_path_is_refused_with_the_absolute_path_message() {
        let dir = TempDir::new().expect("tmp");
        let root = proj(&dir);
        let grants = grants(&root, Mode::ReadOnly);

        let err = grants.resolve("src/main.rs", Access::Read).unwrap_err();
        assert!(err.contains("absolute"), "{err}");

        let err = grants.resolve("", Access::Read).unwrap_err();
        assert!(err.contains("absolute"), "{err}");
    }

    #[test]
    fn a_write_against_a_read_only_grant_is_refused_but_read_is_allowed() {
        let dir = non_tmp_dir();
        let root = proj(&dir);
        fs::write(root.join("f.txt"), b"x").expect("write");
        let grants = grants(&root, Mode::ReadOnly);
        let target = root.join("f.txt");

        let err = grants
            .resolve(target.to_str().expect("utf8"), Access::Write)
            .unwrap_err();
        assert!(err.contains("read-only"), "{err}");

        grants
            .resolve(target.to_str().expect("utf8"), Access::Read)
            .expect("read is still allowed");
    }

    #[test]
    fn a_final_component_symlink_pointing_outside_is_refused_for_write() {
        let dir = non_tmp_dir();
        let root = proj(&dir);
        let outside = dir.path().join("secret.txt");
        fs::write(&outside, b"secret").expect("write");
        symlink(&outside, root.join("link.txt")).expect("symlink");
        let grants = grants(&root, Mode::ReadWrite);

        let target = root.join("link.txt");
        let err = grants
            .resolve(target.to_str().expect("utf8"), Access::Write)
            .unwrap_err();
        assert!(err.contains("outside"), "{err}");
    }

    #[test]
    fn recorded_read_only_grants_allow_tmp_without_changing_the_project() {
        let project = non_tmp_dir();
        let scratch = TempDir::new().expect("tmp");
        let grants = Grants::from_recorded(vec![(
            project.path().canonicalize().expect("canon"),
            Mode::ReadOnly,
        )]);

        let scratch_file = scratch.path().join("new.txt");
        assert_eq!(
            grants
                .resolve(scratch_file.to_str().unwrap(), Access::Write)
                .expect("scratch is writable"),
            scratch_file
        );
        let project_file = project.path().join("new.txt");
        assert!(
            grants
                .resolve(project_file.to_str().unwrap(), Access::Write)
                .unwrap_err()
                .contains("read-only")
        );
    }

    #[test]
    fn tmp_symlinks_outside_tmp_do_not_gain_write_access() {
        let project = non_tmp_dir();
        let scratch = TempDir::new().expect("tmp");
        let elsewhere = non_tmp_dir();
        let file = elsewhere.path().join("file.txt");
        fs::write(&file, "untouched").expect("write");
        symlink(&file, scratch.path().join("link.txt")).expect("symlink");
        symlink(elsewhere.path(), scratch.path().join("dir")).expect("symlink");
        let grants = grants(project.path(), Mode::ReadWrite);

        for path in [
            scratch.path().join("link.txt"),
            scratch.path().join("dir/new.txt"),
        ] {
            assert!(
                grants
                    .resolve(path.to_str().unwrap(), Access::Write)
                    .unwrap_err()
                    .contains("outside"),
                "{}",
                path.display()
            );
        }
    }
}
