#![allow(
    clippy::cast_precision_loss,
    clippy::implicit_hasher,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::needless_raw_string_hashes,
    clippy::struct_excessive_bools,
    clippy::too_many_lines
)]

mod app;
mod markdown;
mod net;
mod syntax;
mod theme;
mod ui;

use std::io::Write as _;
use std::time::Duration;

use anyhow::Result;
use arc_core::herdr::{AgentState, Reporter};
use base64::Engine as _;
use crossterm::cursor::SetCursorStyle;
use crossterm::event::{
    DisableBracketedPaste, EnableBracketedPaste, Event, EventStream, KeyEventKind,
};
use futures::StreamExt as _;
use tokio::sync::mpsc;

use crate::app::{App, Command, Mode, Status};

const DEFAULT_URL: &str = "ws://127.0.0.1:8787";

const USAGE: &str = "\
usage: arc [--addr ws://host:port]

options:
  --addr <url>   daemon to connect to (default: ws://127.0.0.1:8787)
  -h, --help     print this message

keys:
  insert mode (the default)  type; enter sends; esc leaves
  normal mode                h l 0 $ w b  i I a A  x D dd
                             j k ctrl-d ctrl-u G gg   scroll
  any mode                   pageup / pagedown        scroll
                             s or ctrl-p  sessions    ctrl-n  new session
                             :q           quit";

#[tokio::main]
async fn main() -> Result<()> {
    let Some(url) = url_from_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let terminal = ratatui::init();
    let _ = crossterm::execute!(std::io::stdout(), EnableBracketedPaste);
    let mut herdr = Reporter::from_env();
    let result = run(terminal, url, &mut herdr).await;
    let _ = crossterm::execute!(std::io::stdout(), DisableBracketedPaste);
    ratatui::restore();
    let _ = crossterm::execute!(std::io::stdout(), SetCursorStyle::DefaultUserShape);
    herdr.shutdown().await;
    result
}

fn url_from_args() -> Result<Option<String>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => Ok(Some(DEFAULT_URL.to_owned())),
        [flag] if flag == "-h" || flag == "--help" => Ok(None),
        [flag, url] if flag == "--addr" => Ok(Some(url.clone())),
        _ => anyhow::bail!("{USAGE}"),
    }
}

// a remote daemon's directory means nothing here, so only a loopback host
// gets a door guessed from where the client was started
fn is_local(url: &str) -> bool {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let host = match rest.strip_prefix('[') {
        Some(bracketed) => bracketed.split(']').next().unwrap_or(""),
        None => rest.split(['/', ':']).next().unwrap_or(""),
    };
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

fn launch_dir(url: &str) -> Option<std::path::PathBuf> {
    if !is_local(url) {
        return None;
    }
    std::fs::canonicalize(std::env::current_dir().ok()?).ok()
}

async fn run(
    mut terminal: ratatui::DefaultTerminal,
    url: String,
    herdr: &mut Reporter,
) -> Result<()> {
    let mut app = App::new();
    app.set_launch_dir(launch_dir(&url));

    let (commands, command_rx) = mpsc::unbounded_channel();
    let (control_commands, control_command_rx) = mpsc::unbounded_channel();
    let (event_tx, mut events) = mpsc::unbounded_channel();
    tokio::spawn(net::run(url.clone(), command_rx, event_tx.clone()));
    tokio::spawn(net::run_control(url, control_command_rx, event_tx));

    let _ = commands.send(Command::List);

    let mut keys = EventStream::new();
    let mut cursor = Mode::Insert;
    set_cursor_style(cursor);

    // a ticking clock display is a legitimate timer; it only runs while a
    // running job is on the strip or a turn streams, never data polling
    let mut clock = tokio::time::interval(Duration::from_secs(1));

    while !app.quit {
        terminal.draw(|frame| ui::draw(frame, &mut app))?;

        let command = tokio::select! {
            key = keys.next() => match key {
                Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => app.on_key(key),
                Some(Ok(Event::Paste(text))) => app.on_paste(&text),
                Some(Ok(_)) => None,
                Some(Err(error)) => return Err(error.into()),
                None => break,
            },
            event = events.recv() => match event {
                Some(event) => app.on_net(event),
                None => anyhow::bail!("the connection task died"),
            },
            _ = clock.tick(), if app.has_running_job() || app.status == Status::Streaming => None,
        };

        match command {
            Some(Command::Yank(text)) => yank(&text),
            Some(command @ (Command::CancelTurn { .. } | Command::SendLive { .. })) => {
                control_commands.send(command).expect("control task alive");
            }
            Some(command) => commands.send(command).expect("connection task alive"),
            None => {}
        }
        if app.mode != cursor {
            cursor = app.mode;
            set_cursor_style(cursor);
        }

        herdr.state(agent_state(app.status));
        herdr.metadata(session_title(&app), app.running_job_count());
    }
    Ok(())
}

// arc never says done: herdr derives it from working→idle and clears it on
// pane focus, which is also what makes its finished-turn notification fire
fn agent_state(status: Status) -> AgentState {
    match status {
        Status::Streaming => AgentState::Working,
        Status::Disconnected => AgentState::Blocked,
        Status::Idle => AgentState::Idle,
    }
}

fn session_title(app: &App) -> &str {
    app.session_id
        .as_deref()
        .and_then(|id| app.sessions.iter().find(|session| session.id == id))
        .map_or("", |session| session.title.as_str())
}

// OSC 52 to the same stdout ratatui draws through, not a separate handle
fn yank(text: &str) {
    let encoded = base64::engine::general_purpose::STANDARD.encode(text);
    let mut out = std::io::stdout();
    let _ = crossterm::execute!(
        out,
        crossterm::style::Print(format!("\x1b]52;c;{encoded}\x07"))
    );
    let _ = out.flush();
}

fn set_cursor_style(mode: Mode) {
    let style = match mode {
        Mode::Insert | Mode::Cmd => SetCursorStyle::SteadyBar,
        Mode::Normal | Mode::Visual => SetCursorStyle::SteadyBlock,
    };
    let mut out = std::io::stdout();
    let _ = crossterm::execute!(out, style);
    let _ = out.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_hosts_are_local() {
        assert!(is_local("ws://127.0.0.1:8787"));
        assert!(is_local("ws://localhost:8787"));
        assert!(is_local("ws://[::1]:8787"));
        assert!(is_local(DEFAULT_URL), "the default is a loopback address");
    }

    #[test]
    fn a_remote_host_is_not_local() {
        assert!(!is_local("ws://100.64.0.1:8787"));
        assert!(!is_local("ws://arc.tailnet-1234.ts.net:8787"));
    }

    // a remote address never guesses a door: its directory means nothing
    // to the daemon, whatever the client's own cwd happens to be
    #[test]
    fn a_remote_address_yields_no_launch_dir() {
        assert_eq!(launch_dir("ws://100.64.0.1:8787"), None);
    }

    #[test]
    fn a_local_address_yields_the_canonical_cwd() {
        let expected = std::fs::canonicalize(std::env::current_dir().expect("cwd")).ok();
        assert_eq!(launch_dir(DEFAULT_URL), expected);
    }
}
