use std::io::ErrorKind;
use std::path::Path;

use anyhow::{Context as _, Result};

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
    use super::load;
    use std::fs;
    use tempfile::TempDir;

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
