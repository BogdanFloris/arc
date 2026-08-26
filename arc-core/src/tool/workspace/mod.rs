pub mod read;

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

        let Some((_, mode)) = self
            .roots
            .iter()
            .find(|(root, _)| canonical.starts_with(root))
        else {
            return Err("that path is outside the session's granted roots.".to_owned());
        };

        if access == Access::Write && *mode == Mode::ReadOnly {
            return Err(format!(
                "{} is read-only in this session.",
                canonical.display()
            ));
        }

        Ok(canonical)
    }
}

pub struct Workspace {
    grants: Grants,
    reads: Mutex<HashMap<(String, PathBuf), u64>>,
}

impl Workspace {
    pub fn new(grants: Grants) -> Self {
        Self {
            grants,
            reads: Mutex::new(HashMap::new()),
        }
    }

    fn record_read(&self, session_id: &str, path: &Path, bytes: &[u8]) {
        let mut hasher = DefaultHasher::new();
        bytes.hash(&mut hasher);
        self.reads
            .lock()
            .expect("reads lock poisoned")
            .insert((session_id.to_owned(), path.to_path_buf()), hasher.finish());
    }

    #[cfg(test)]
    pub(crate) fn recorded_hash(&self, session_id: &str, path: &Path) -> Option<u64> {
        self.reads
            .lock()
            .expect("reads lock poisoned")
            .get(&(session_id.to_owned(), path.to_path_buf()))
            .copied()
    }
}

/// The workspace source: `read` for now; `write`/`edit` follow in a later task.
pub fn tools(workspace: Arc<Workspace>) -> Vec<Box<dyn Tool>> {
    vec![Box::new(read::Read::new(workspace))]
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::symlink;

    use tempfile::TempDir;

    use super::{Access, Grant, Grants, Mode};

    fn proj(dir: &TempDir) -> std::path::PathBuf {
        let root = dir.path().join("proj");
        fs::create_dir_all(&root).expect("mkdir proj");
        root
    }

    fn grants(root: &std::path::Path, mode: Mode) -> Grants {
        Grants::new(vec![Grant::new(root, mode)]).expect("canonicalize grant")
    }

    #[test]
    fn a_dotdot_escape_is_refused() {
        let dir = TempDir::new().expect("tmp");
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
        let elsewhere = TempDir::new().expect("tmp2");
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
        let dir = TempDir::new().expect("tmp");
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
        let dir = TempDir::new().expect("tmp");
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
        let dir = TempDir::new().expect("tmp");
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
    fn a_dotdot_that_stays_inside_the_root_is_allowed() {
        let dir = TempDir::new().expect("tmp");
        let root = proj(&dir);
        fs::create_dir_all(root.join("sub")).expect("mkdir sub");
        fs::write(root.join("ok.txt"), b"x").expect("write");
        let grants = grants(&root, Mode::ReadOnly);

        let path = root.join("sub").join("..").join("ok.txt");
        let resolved = grants
            .resolve(path.to_str().expect("utf8"), Access::Read)
            .expect("stays inside");
        assert_eq!(resolved, root.canonicalize().expect("canon").join("ok.txt"));
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
        let dir = TempDir::new().expect("tmp");
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
    fn resolving_a_missing_file_for_read_does_not_panic_and_succeeds() {
        let dir = TempDir::new().expect("tmp");
        let root = proj(&dir);
        let grants = grants(&root, Mode::ReadOnly);

        let target = root.join("missing.txt");
        let resolved = grants
            .resolve(target.to_str().expect("utf8"), Access::Read)
            .expect("the gate does not require existence; the read tool does");
        assert_eq!(
            resolved,
            root.canonicalize().expect("canon").join("missing.txt")
        );
    }

    #[test]
    fn a_nonexistent_file_for_write_resolves_when_its_parent_exists() {
        let dir = TempDir::new().expect("tmp");
        let root = proj(&dir);
        let grants = grants(&root, Mode::ReadWrite);

        let target = root.join("new.txt");
        let resolved = grants
            .resolve(target.to_str().expect("utf8"), Access::Write)
            .expect("parent exists");
        assert_eq!(
            resolved,
            root.canonicalize().expect("canon").join("new.txt")
        );
    }

    #[test]
    fn a_nonexistent_parent_directory_is_refused_and_named() {
        let dir = TempDir::new().expect("tmp");
        let root = proj(&dir);
        let grants = grants(&root, Mode::ReadWrite);

        let target = root.join("missing_dir").join("new.txt");
        let err = grants
            .resolve(target.to_str().expect("utf8"), Access::Write)
            .unwrap_err();
        assert!(err.contains("parent"), "{err}");
        assert!(err.contains("missing_dir"), "{err}");
    }

    #[test]
    fn a_final_component_symlink_pointing_outside_is_refused_for_write() {
        let dir = TempDir::new().expect("tmp");
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
}
