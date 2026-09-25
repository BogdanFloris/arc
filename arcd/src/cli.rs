use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Run,
    Rebuild,
    Login,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub command: Command,
    pub config: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parsed {
    Run(Cli),
    Help,
}

pub const DEFAULT_CONFIG: &str = "data/arc.toml";

/// Installed config first, checkout fallback second: `~/.config/arc/arc.toml`
/// when it exists, else `data/arc.toml` for in-repo development.
pub fn default_config() -> PathBuf {
    if let Some(installed) = installed_config(std::env::var_os("XDG_CONFIG_HOME"), home()) {
        if installed.exists() {
            return installed;
        }
    }
    PathBuf::from(DEFAULT_CONFIG)
}

fn home() -> Option<std::ffi::OsString> {
    std::env::var_os("HOME")
}

fn installed_config(
    xdg: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
) -> Option<PathBuf> {
    let base = xdg
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| home.map(|home| PathBuf::from(home).join(".config")))?;
    Some(base.join("arc").join("arc.toml"))
}

pub const USAGE: &str = "\
usage: arcd run [--config <path>]
       arcd rebuild [--config <path>]
       arcd login codex [--config <path>]

commands:
  run             start the daemon (default)
  rebuild         replay the log into a fresh index and diff it against the
                  live one, read-only
  login codex     sign in to the ChatGPT plan with a device code and save the
                  credential under data/secrets/; restart arcd afterwards

options:
  --config <path>       config file (default: ~/.config/arc/arc.toml if present,
                        else data/arc.toml; a missing file means defaults)
  -h, --help            print this message";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Name {
    Run,
    Rebuild,
    Login,
}

pub fn parse<I, S>(args: I) -> Result<Parsed, String>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    let mut args = args.into_iter().map(Into::into).skip(1);
    let mut command = None;
    let mut config = None;

    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("-h" | "--help") => return Ok(Parsed::Help),
            Some("--config") => {
                let path = args
                    .next()
                    .ok_or_else(|| "--config needs a path".to_owned())?;
                if config.replace(PathBuf::from(path)).is_some() {
                    return Err("--config given twice".to_owned());
                }
            }
            Some("run") if command.is_none() => command = Some(Name::Run),
            Some("rebuild") if command.is_none() => command = Some(Name::Rebuild),
            Some("login") if command.is_none() => {
                match args.next().as_deref().and_then(|it| it.to_str()) {
                    Some("codex") => command = Some(Name::Login),
                    Some(other) => {
                        return Err(format!(
                            "login: unknown provider `{other}`; only codex has a login"
                        ));
                    }
                    None => return Err("login needs a provider: arcd login codex".to_owned()),
                }
            }
            _ => {
                let shown = arg.to_string_lossy().into_owned();
                return Err(if command.is_some() {
                    format!("unexpected argument: {shown}")
                } else {
                    format!("unknown command or option: {shown}")
                });
            }
        }
    }

    let command = match command.unwrap_or(Name::Run) {
        Name::Run => Command::Run,
        Name::Rebuild => Command::Rebuild,
        Name::Login => Command::Login,
    };

    Ok(Parsed::Run(Cli {
        command,
        config: config.unwrap_or_else(default_config),
    }))
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command, Parsed, parse};
    use std::path::PathBuf;

    fn ok(args: &[&str]) -> Cli {
        match parse(args.iter().copied()).expect("parses") {
            Parsed::Run(cli) => cli,
            Parsed::Help => panic!("expected a command, got help"),
        }
    }

    #[test]
    fn installed_config_prefers_xdg_and_falls_back_to_home() {
        use super::installed_config;
        assert_eq!(
            installed_config(Some("/xdg".into()), Some("/home/u".into())),
            Some(PathBuf::from("/xdg/arc/arc.toml"))
        );
        assert_eq!(
            installed_config(None, Some("/home/u".into())),
            Some(PathBuf::from("/home/u/.config/arc/arc.toml"))
        );
        assert_eq!(
            installed_config(Some("".into()), Some("/home/u".into())),
            Some(PathBuf::from("/home/u/.config/arc/arc.toml")),
            "an empty XDG_CONFIG_HOME is unset per the spec"
        );
        assert_eq!(installed_config(None, None), None);
    }

    #[test]
    fn login_names_its_provider_and_nothing_else() {
        assert_eq!(ok(&["arcd", "login", "codex"]).command, Command::Login);
        assert_eq!(
            ok(&["arcd", "login", "codex", "--config", "/etc/arc.toml"]).config,
            PathBuf::from("/etc/arc.toml")
        );
        for args in [
            vec!["arcd", "login"],
            vec!["arcd", "login", "gemini"],
            vec!["arcd", "login", "codex", "extra"],
            vec!["arcd", "login", "codex", "--prompt", "v1"],
        ] {
            assert!(parse(args.clone()).is_err(), "{args:?} should not parse");
        }
    }
}
