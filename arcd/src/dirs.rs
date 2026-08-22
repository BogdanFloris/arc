use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataDirs {
    root: PathBuf,
    log: PathBuf,
    secrets: PathBuf,
    index: PathBuf,
    traces: PathBuf,
    identity: PathBuf,
}

impl DataDirs {
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

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn log(&self) -> &Path {
        &self.log
    }

    #[must_use]
    pub fn index(&self) -> &Path {
        &self.index
    }

    #[must_use]
    pub fn traces(&self) -> &Path {
        &self.traces
    }

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
        assert!(!dirs.index().exists());
        assert!(!dirs.identity().exists());
    }
}
