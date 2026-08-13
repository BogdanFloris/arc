//! Loading `data/identity.md`

use std::io::ErrorKind;
use std::path::Path;

use anyhow::{Context as _, Result};

/// Reads the identity file, distinguishing "absent" from "broken".
///
/// - No file → `Ok(None)`. Running without an identity is a supported state;
///   the caller announces it.
/// - Whitespace-only → `Ok(None)`. An accidentally emptied file must not send
///   an empty system prompt as though it meant something.
/// - Present but unreadable (permissions, invalid UTF-8) → `Err`. A broken
///   identity file should stop the daemon, not silently produce an ARC with
///   amnesia.
pub fn load(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) if text.trim().is_empty() => Ok(None),
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("reading identity file {}", path.display()))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::load;

    #[test]
    fn loads_the_file_as_written() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("identity.md");
        fs::write(&path, "# ARC\n\nYou are ARC.\n").expect("write");

        let identity = load(&path).expect("load");

        assert_eq!(identity.as_deref(), Some("# ARC\n\nYou are ARC.\n"));
    }

    #[test]
    fn an_absent_file_is_none() {
        let dir = TempDir::new().expect("temp dir");

        assert_eq!(load(&dir.path().join("identity.md")).expect("load"), None);
    }

    #[test]
    fn a_whitespace_only_file_is_none() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("identity.md");
        fs::write(&path, "\n  \n\t\n").expect("write");

        assert_eq!(load(&path).expect("load"), None);
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_file_is_an_error_not_amnesia() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("identity.md");
        fs::write(&path, "# ARC\n").expect("write");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).expect("chmod");

        let err = load(&path).expect_err("unreadable must not be silent");
        assert!(err.to_string().contains("identity.md"), "got: {err:#}");
    }
}
