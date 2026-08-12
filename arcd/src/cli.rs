//! Command line parsing, by hand.
//!
//! The surface is two subcommands and one flag, and it is meant to stay that
//! size: a daemon is configured by its config file, not by flags. Parsing it
//! by hand keeps the dependency out and the error messages ours.

use std::ffi::OsString;
use std::path::PathBuf;

/// What the user asked `arcd` to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Run the daemon. The default when no subcommand is given.
    Run,
    /// Run the OAuth flow and write the token file.
    Login,
}

/// A parsed command line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    /// Subcommand to dispatch.
    pub command: Command,
    /// Config file to read. Missing on disk is not an error (see [`crate::config`]).
    pub config: PathBuf,
}

/// What [`parse`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parsed {
    /// Dispatch this.
    Run(Cli),
    /// `--help`: print [`USAGE`] to stdout and exit 0.
    Help,
}

/// Config file used when `--config` is absent.
pub const DEFAULT_CONFIG: &str = "arc.toml";

/// Printed on `--help`, and on stderr for anything unparseable.
pub const USAGE: &str = "\
usage: arcd [run|login] [--config <path>]

commands:
  run     start the daemon (default)
  login   sign in to the provider and write the token file

options:
  --config <path>   config file (default: arc.toml; a missing file means defaults)
  -h, --help        print this message";

/// Parses `args`, argv[0] included.
///
/// # Errors
///
/// A message for stderr whenever the arguments are not a command line this
/// understands: an unknown subcommand, an unknown flag, a second subcommand, a
/// `--config` with nothing after it. The caller pairs it with [`USAGE`] and
/// exits 2.
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
            Some("run") if command.is_none() => command = Some(Command::Run),
            Some("login") if command.is_none() => command = Some(Command::Login),
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

    Ok(Parsed::Run(Cli {
        command: command.unwrap_or(Command::Run),
        config: config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG)),
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
    fn no_arguments_runs_the_daemon_with_the_default_config() {
        let cli = ok(&["arcd"]);
        assert_eq!(cli.command, Command::Run);
        assert_eq!(cli.config, PathBuf::from("arc.toml"));
    }

    #[test]
    fn subcommands_and_the_config_flag_parse_in_either_order() {
        assert_eq!(ok(&["arcd", "login"]).command, Command::Login);
        assert_eq!(
            ok(&["arcd", "login", "--config", "/etc/arc.toml"]).config,
            PathBuf::from("/etc/arc.toml")
        );
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
            vec!["arcd", "run", "login"],
            vec!["arcd", "run", "extra"],
            vec!["arcd", "--config"],
        ] {
            assert!(parse(args.clone()).is_err(), "{args:?} should not parse");
        }
    }
}
