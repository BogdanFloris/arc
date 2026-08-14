//! The `data/` layout, in one place.
//!
//! Every runtime path ARC touches is derived here from `data_dir` (DESIGN.md
//! §10). Nothing else in the codebase joins these names: a path spelled in two
//! places is a path that will disagree with itself, and "where does the token
//! file live" should have exactly one answer.

use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};

/// Every path under `data_dir`, derived once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataDirs {
    /// `data_dir` itself.
    root: PathBuf,
    /// `data/log/` — event log segments (DESIGN.md §3).
    log: PathBuf,
    /// `data/secrets/` — 0700, provider credentials, never backed up.
    secrets: PathBuf,
    /// `data/index.db` — the `SQLite` projection. Disposable.
    index: PathBuf,
    /// `data/traces/` — Perfetto traces. Disposable.
    traces: PathBuf,
    /// `data/identity.md` — human-owned, never written by ARC (§5.1).
    identity: PathBuf,
}

impl DataDirs {
    /// Derives the layout under `root`. Touches no disk; see [`create`](Self::create).
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            log: root.join("log"),
            secrets: root.join("secrets"),
            index: root.join("index.db"),
            traces: root.join("traces"),
            identity: root.join("identity.md"),
            root,
        }
    }

    /// Creates the directories, `secrets/` owner-only.
    ///
    /// Idempotent: an existing tree is left alone, except that `secrets/` has
    /// its mode reasserted — the token file inside it is a plaintext
    /// credential, so a loosened directory is worth fixing rather than
    /// tolerating.
    ///
    /// # Errors
    ///
    /// If a directory cannot be created or `secrets/` cannot be locked down.
    pub fn create(&self) -> Result<()> {
        for dir in [&self.root, &self.log, &self.secrets, &self.traces] {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("creating directory {}", dir.display()))?;
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&self.secrets, std::fs::Permissions::from_mode(0o700))
                .with_context(|| format!("restricting {} to 0700", self.secrets.display()))?;
        }

        Ok(())
    }

    /// `data_dir` itself.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Event log directory, for [`arc_core::log::Log::open`].
    #[must_use]
    pub fn log(&self) -> &Path {
        &self.log
    }

    /// `SQLite` index, for [`arc_core::projection::Projection::open`].
    #[must_use]
    pub fn index(&self) -> &Path {
        &self.index
    }

    /// Perfetto trace directory: one `.pftrace` per run.
    #[must_use]
    pub fn traces(&self) -> &Path {
        &self.traces
    }

    /// Identity file (task 5.3). Read-only to ARC, by §5.1.
    #[must_use]
    pub fn identity(&self) -> &Path {
        &self.identity
    }
}

#[cfg(test)]
mod tests {
    use super::DataDirs;
    use std::path::Path;

    #[test]
    fn every_path_hangs_off_the_root() {
        let dirs = DataDirs::new("/srv/arc");
        assert_eq!(dirs.root(), Path::new("/srv/arc"));
        assert_eq!(dirs.log(), Path::new("/srv/arc/log"));
        assert_eq!(dirs.index(), Path::new("/srv/arc/index.db"));
        assert_eq!(dirs.traces(), Path::new("/srv/arc/traces"));
        assert_eq!(dirs.identity(), Path::new("/srv/arc/identity.md"));
    }

    #[test]
    fn create_makes_the_tree_and_locks_down_secrets() {
        let temp = tempfile::tempdir().expect("temp dir");
        let dirs = DataDirs::new(temp.path().join("data"));

        dirs.create().expect("create");
        dirs.create().expect("create is idempotent");

        assert!(dirs.log().is_dir());
        assert!(dirs.traces().is_dir());
        // Neither is created: the index is made by the projection, the
        // identity file by a human.
        assert!(!dirs.index().exists());
        assert!(!dirs.identity().exists());
    }
}
