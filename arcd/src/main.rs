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
        Err(err) => {
            eprintln!("arcd: {err:?}");
            ExitCode::FAILURE
        }
    }
}

async fn dispatch(cli: Cli) -> Result<()> {
    let config = Config::load(&cli.config)?;

    let dirs = DataDirs::new(&config.data_dir);

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
