use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Secrets {
    dir: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("secret name `{0}` must be a bare file name")]
    Name(String),

    #[error("no secret `{name}` in {}", dir.display())]
    Missing { name: String, dir: PathBuf },

    #[error("secret `{name}` is empty")]
    Empty { name: String },

    #[error("secret `{name}` is mode {mode:04o}; only its owner may read it (chmod 600)")]
    Permissions { name: String, mode: u32 },

    #[error("reading secret `{name}`: {source}")]
    Io {
        name: String,
        #[source]
        source: std::io::Error,
    },
}

impl Secrets {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }

    pub fn read(&self, name: &str) -> Result<String, Error> {
        if name.is_empty() || Path::new(name).components().count() != 1 {
            return Err(Error::Name(name.to_owned()));
        }
        let path = self.dir.join(name);

        let metadata = std::fs::metadata(&path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                Error::Missing {
                    name: name.to_owned(),
                    dir: self.dir.clone(),
                }
            } else {
                Error::Io {
                    name: name.to_owned(),
                    source,
                }
            }
        })?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = metadata.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                return Err(Error::Permissions {
                    name: name.to_owned(),
                    mode,
                });
            }
        }

        let text = std::fs::read_to_string(&path).map_err(|source| Error::Io {
            name: name.to_owned(),
            source,
        })?;
        let key = text.trim().to_owned();
        if key.is_empty() {
            return Err(Error::Empty {
                name: name.to_owned(),
            });
        }
        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, Secrets};
    use std::os::unix::fs::PermissionsExt as _;

    fn write(dir: &std::path::Path, name: &str, body: &str, mode: u32) {
        let path = dir.join(name);
        std::fs::write(&path, body).expect("write secret");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("chmod");
    }

    #[test]
    fn a_key_comes_back_without_its_trailing_newline() {
        let dir = tempfile::tempdir().expect("temp dir");
        write(dir.path(), "gemini", "sk-abc123\n", 0o600);

        let secrets = Secrets::new(dir.path());
        assert_eq!(secrets.read("gemini").expect("reads"), "sk-abc123");
    }

    #[test]
    fn a_missing_secret_names_the_directory_to_put_it_in() {
        let dir = tempfile::tempdir().expect("temp dir");

        let err = Secrets::new(dir.path())
            .read("gemini")
            .expect_err("nothing is there");

        assert!(matches!(err, Error::Missing { .. }), "{err}");
        let msg = err.to_string();
        assert!(
            msg.contains("gemini") && msg.contains(dir.path().to_str().unwrap()),
            "{msg}"
        );
    }

    #[test]
    fn a_world_readable_secret_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        write(dir.path(), "gemini", "sk-abc123", 0o644);

        let err = Secrets::new(dir.path())
            .read("gemini")
            .expect_err("0644 is not a secret");

        assert!(
            matches!(err, Error::Permissions { mode: 0o644, .. }),
            "{err}"
        );
        assert!(
            !err.to_string().contains("sk-abc123"),
            "the error must not leak the key"
        );
    }

    #[test]
    fn an_empty_secret_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        write(dir.path(), "gemini", "   \n", 0o600);

        let err = Secrets::new(dir.path())
            .read("gemini")
            .expect_err("whitespace is not a key");

        assert!(matches!(err, Error::Empty { .. }), "{err}");
    }

    #[test]
    fn a_name_that_walks_out_of_the_directory_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        write(dir.path(), "gemini", "sk-abc123", 0o600);

        for name in ["../gemini", "sub/gemini", "/etc/passwd", ""] {
            let err = Secrets::new(dir.path())
                .read(name)
                .expect_err("a secret name is a bare file name");
            assert!(matches!(err, Error::Name(_)), "{name}: {err}");
        }
    }
}
