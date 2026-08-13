//! `arcd` — the ARC daemon.
//!
//! A thin composition layer over `arc-core` (DESIGN.md §2): this binary owns
//! the command line, the config file, the `data/` layout, and the process
//! lifecycle. It owns no rules about logs, projections, or providers.
//!
//! ```text
//! arcd [run|login] [--config <path>]
//! ```
//!
//! Exit codes: 0 fine, 1 something failed, 2 the command line did not parse.

mod cli;
mod config;
mod daemon;
mod dirs;
mod identity;
mod login;
mod server;
mod telemetry;

use std::process::ExitCode;

use anyhow::Result;

use crate::cli::{Cli, Command, Parsed};
use crate::config::Config;
use crate::daemon::Daemon;
use crate::dirs::DataDirs;

/// Usage error.
const EXIT_USAGE: u8 = 2;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match cli::parse(std::env::args_os()) {
        Ok(Parsed::Run(cli)) => cli,
        Ok(Parsed::Help) => {
            println!("{}", cli::USAGE);
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("arcd: {message}\n\n{}", cli::USAGE);
            return ExitCode::from(EXIT_USAGE);
        }
    };

    match dispatch(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        // `{:?}` on an anyhow error is the whole chain: what failed, and under
        // what it failed. Startup errors are the ones worth reading in full.
        Err(err) => {
            eprintln!("arcd: {err:?}");
            ExitCode::FAILURE
        }
    }
}

/// Loads the config, starts tracing, and runs the requested command.
async fn dispatch(cli: Cli) -> Result<()> {
    let config = Config::load(&cli.config)?;

    // After the config (nothing before it is worth tracing) and before
    // anything touches disk (everything after it is).
    telemetry::init()?;

    let dirs = DataDirs::new(&config.data_dir);
    match cli.command {
        Command::Run => Daemon::start(config, dirs)?.serve().await,
        Command::Login => login::run(&dirs).await,
    }
}
