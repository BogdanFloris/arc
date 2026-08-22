//! `arcd` — the ARC daemon.
//!
//! A thin composition layer over `arc-core` (DESIGN.md §2): this binary owns
//! the command line, the config file, the `data/` layout, and the process
//! lifecycle. It owns no rules about logs, projections, or providers.
//!
//! ```text
//! arcd run [--config <path>]
//! arcd memory-replay --prompt <version> [--against <version>] [--session <id>]...
//! ```
//!
//! Exit codes: 0 fine, 1 something failed, 2 the command line did not parse.

mod cli;
mod config;
mod daemon;
mod dirs;
mod identity;
mod llama;
mod replay;
mod server;
mod telemetry;

use std::process::ExitCode;

use anyhow::Result;

use crate::cli::{Cli, Command, Parsed};
use crate::config::Config;
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

    let dirs = DataDirs::new(&config.data_dir);

    // After the config (nothing before it is worth tracing) and before
    // anything else touches disk (everything after it is). The trace file is
    // the one exception it makes to that order — it is what the rest gets
    // recorded into.
    telemetry::init(dirs.traces())?;

    match cli.command {
        Command::Run => daemon::run(config, dirs).await,
        Command::MemoryReplay {
            prompt,
            against,
            sessions,
        } => replay::run(config, dirs, &prompt, against.as_deref(), &sessions).await,
    }
}
