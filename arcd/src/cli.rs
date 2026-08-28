use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Run,
    Rebuild,
    MemoryReplay {
        prompt: String,
        against: Option<String>,
        sessions: Vec<String>,
    },
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
       arcd memory-replay --prompt <version> [--against <version>]
                          [--session <id>]... [--config <path>]

commands:
  run             start the daemon (default)
  rebuild         replay the log into a fresh index and diff it against the
                  live one, read-only
  memory-replay   re-run a consolidation prompt version over the log and
                  report the resulting memory state, read-only

options:
  --config <path>       config file (default: ~/.config/arc/arc.toml if present,
                        else data/arc.toml; a missing file means defaults)
  --prompt <version>    prompt version to replay (memory-replay only)
  --against <version>   second version to run and diff against (memory-replay only)
  --session <id>        limit the replay to this session; repeatable (memory-replay only)
  -h, --help            print this message";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Name {
    Run,
    Rebuild,
    MemoryReplay,
}

pub fn parse<I, S>(args: I) -> Result<Parsed, String>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    fn value(args: &mut dyn Iterator<Item = OsString>, flag: &str) -> Result<String, String> {
        args.next()
            .map(|value| value.to_string_lossy().into_owned())
            .ok_or_else(|| format!("{flag} needs a value"))
    }

    let mut args = args.into_iter().map(Into::into).skip(1);
    let mut command = None;
    let mut config = None;
    let mut prompt: Option<String> = None;
    let mut against: Option<String> = None;
    let mut sessions = Vec::new();

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
            Some("--prompt") => {
                if prompt.replace(value(&mut args, "--prompt")?).is_some() {
                    return Err("--prompt given twice".to_owned());
                }
            }
            Some("--against") => {
                if against.replace(value(&mut args, "--against")?).is_some() {
                    return Err("--against given twice".to_owned());
                }
            }
            Some("--session") => sessions.push(value(&mut args, "--session")?),
            Some("run") if command.is_none() => command = Some(Name::Run),
            Some("rebuild") if command.is_none() => command = Some(Name::Rebuild),
            Some("memory-replay") if command.is_none() => command = Some(Name::MemoryReplay),
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

    let name = command.unwrap_or(Name::Run);
    if !matches!(name, Name::MemoryReplay)
        && (prompt.is_some() || against.is_some() || !sessions.is_empty())
    {
        return Err("--prompt, --against, and --session are for memory-replay".to_owned());
    }
    let command = match name {
        Name::Run => Command::Run,
        Name::Rebuild => Command::Rebuild,
        Name::MemoryReplay => Command::MemoryReplay {
            prompt: prompt.ok_or_else(|| "memory-replay needs --prompt <version>".to_owned())?,
            against,
            sessions,
        },
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
    fn no_arguments_runs_the_daemon_with_the_default_config() {
        let cli = ok(&["arcd"]);
        assert_eq!(cli.command, Command::Run);
        assert_eq!(cli.config, super::default_config());
    }

    #[test]
    fn subcommands_and_the_config_flag_parse_in_either_order() {
        assert_eq!(
            ok(&["arcd", "--config", "/etc/arc.toml", "run"]).command,
            Command::Run
        );
    }

    #[test]
    fn help_is_its_own_outcome() {
        assert_eq!(parse(["arcd", "--help"]), Ok(Parsed::Help));
        assert_eq!(parse(["arcd", "run", "-h"]), Ok(Parsed::Help));
    }

    #[test]
    fn anything_else_is_a_usage_error() {
        for args in [
            vec!["arcd", "serve"],
            vec!["arcd", "--verbose"],
            vec!["arcd", "run", "extra"],
            vec!["arcd", "--config"],
        ] {
            assert!(parse(args.clone()).is_err(), "{args:?} should not parse");
        }
    }

    #[test]
    fn memory_replay_parses_with_its_flags_in_any_order() {
        let cli = ok(&[
            "arcd",
            "--session",
            "s-1",
            "memory-replay",
            "--prompt",
            "v1",
            "--against",
            "v2",
            "--session",
            "s-2",
            "--config",
            "/etc/arc.toml",
        ]);
        assert_eq!(
            cli.command,
            Command::MemoryReplay {
                prompt: "v1".to_owned(),
                against: Some("v2".to_owned()),
                sessions: vec!["s-1".to_owned(), "s-2".to_owned()],
            }
        );
        assert_eq!(cli.config, PathBuf::from("/etc/arc.toml"));
    }

    #[test]
    fn rebuild_parses() {
        let cli = ok(&["arcd", "rebuild"]);
        assert_eq!(cli.command, Command::Rebuild);
        assert_eq!(cli.config, super::default_config());

        assert_eq!(
            ok(&["arcd", "--config", "/etc/arc.toml", "rebuild"]).config,
            PathBuf::from("/etc/arc.toml")
        );
    }

    #[test]
    fn rebuild_flag_misuse_is_a_usage_error() {
        for args in [
            vec!["arcd", "rebuild", "--prompt", "v1"],
            vec!["arcd", "rebuild", "extra"],
        ] {
            assert!(parse(args.clone()).is_err(), "{args:?} should not parse");
        }
    }

    #[test]
    fn memory_replay_defaults_to_all_sessions_and_no_diff() {
        assert_eq!(
            ok(&["arcd", "memory-replay", "--prompt", "v1"]).command,
            Command::MemoryReplay {
                prompt: "v1".to_owned(),
                against: None,
                sessions: Vec::new(),
            }
        );
    }

    #[test]
    fn memory_replay_flag_misuse_is_a_usage_error() {
        for args in [
            vec!["arcd", "memory-replay"],
            vec!["arcd", "memory-replay", "--prompt"],
            vec!["arcd", "memory-replay", "--prompt", "v1", "--prompt", "v2"],
            vec!["arcd", "memory-replay", "--prompt", "v1", "--against"],
            vec!["arcd", "run", "--prompt", "v1"],
            vec!["arcd", "--session", "s-1"],
        ] {
            assert!(parse(args.clone()).is_err(), "{args:?} should not parse");
        }
    }
}
