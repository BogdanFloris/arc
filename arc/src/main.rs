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
use crossterm::event::{Event, EventStream, KeyEventKind};
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
    let mut herdr = Reporter::from_env();
    let result = run(terminal, url, &mut herdr).await;
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

async fn run(
    mut terminal: ratatui::DefaultTerminal,
    url: String,
    herdr: &mut Reporter,
) -> Result<()> {
    let (commands, command_rx) = mpsc::unbounded_channel();
    let (control_commands, control_command_rx) = mpsc::unbounded_channel();
    let (event_tx, mut events) = mpsc::unbounded_channel();
    tokio::spawn(net::run(url.clone(), command_rx, event_tx.clone()));
    tokio::spawn(net::run_control(url, control_command_rx, event_tx));

    let mut app = App::new();
    let _ = commands.send(Command::List);

    let mut keys = EventStream::new();
    let mut cursor = Mode::Insert;
    set_cursor_style(cursor);

    let mut agent_state = AgentState::Idle;
    let mut cancelling = false;

    // a ticking clock display is a legitimate timer; it only runs while a
    // running job is on the strip or a turn streams, never data polling
    let mut clock = tokio::time::interval(Duration::from_secs(1));

    while !app.quit {
        terminal.draw(|frame| ui::draw(frame, &mut app))?;

        let mut key_pressed = false;
        let command = tokio::select! {
            key = keys.next() => match key {
                Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                    key_pressed = true;
                    app.on_key(key)
                }
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
            Some(command @ Command::CancelTurn { .. }) => {
                cancelling = true;
                control_commands.send(command).expect("control task alive");
            }
            Some(command) => commands.send(command).expect("connection task alive"),
            None => {}
        }
        if app.mode != cursor {
            cursor = app.mode;
            set_cursor_style(cursor);
        }

        agent_state = next_agent_state(agent_state, app.status, key_pressed, cancelling);
        if app.status != Status::Streaming {
            cancelling = false;
        }
        herdr.state(agent_state);
        herdr.metadata(session_title(&app), app.running_job_count());
    }
    Ok(())
}

/// What the herdr sidebar should say. Done means a turn finished while the
/// user was away; the next keypress collapses it to idle. A cancelled turn
/// never reads as done — the user ended it, nothing awaits review.
fn next_agent_state(
    previous: AgentState,
    status: Status,
    key_pressed: bool,
    cancelling: bool,
) -> AgentState {
    match status {
        Status::Streaming => AgentState::Working,
        Status::Disconnected => AgentState::Blocked,
        Status::Idle => match previous {
            AgentState::Working if !cancelling => AgentState::Done,
            AgentState::Done if !key_pressed => AgentState::Done,
            _ => AgentState::Idle,
        },
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
    fn a_finished_turn_reads_done_until_the_next_keypress() {
        let state = next_agent_state(AgentState::Working, Status::Idle, false, false);
        assert_eq!(state, AgentState::Done);
        let state = next_agent_state(state, Status::Idle, false, false);
        assert_eq!(state, AgentState::Done);
        let state = next_agent_state(state, Status::Idle, true, false);
        assert_eq!(state, AgentState::Idle);
    }

    #[test]
    fn a_cancelled_turn_lands_idle_not_done() {
        let state = next_agent_state(AgentState::Working, Status::Idle, false, true);
        assert_eq!(state, AgentState::Idle);
    }

    #[test]
    fn streaming_is_working_and_disconnect_is_blocked() {
        let state = next_agent_state(AgentState::Idle, Status::Streaming, true, false);
        assert_eq!(state, AgentState::Working);
        let state = next_agent_state(state, Status::Disconnected, false, false);
        assert_eq!(state, AgentState::Blocked);
        let state = next_agent_state(state, Status::Idle, false, false);
        assert_eq!(state, AgentState::Idle);
    }
}
