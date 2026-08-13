//! `arc` — the TUI client (DESIGN.md §7).
//!
//! Rendering and keys only: the wire lives in `arc_core::client`, driven by
//! the connection task in [`net`], and every state transition is [`app`]'s.
//! The loop below just moves events between the three and draws.

mod app;
mod net;
mod theme;
mod ui;

use std::io::Write as _;

use anyhow::Result;
use crossterm::cursor::SetCursorStyle;
use crossterm::event::{Event, EventStream, KeyEventKind};
use futures::StreamExt as _;
use tokio::sync::mpsc;

use crate::app::{App, Command, Mode};

/// Where the daemon listens unless `--addr` says otherwise (arcd's default
/// bind).
const DEFAULT_URL: &str = "ws://127.0.0.1:8787";

#[tokio::main]
async fn main() -> Result<()> {
    let url = url_from_args()?;
    let terminal = ratatui::init();
    let result = run(terminal, url).await;
    ratatui::restore();
    let _ = crossterm::execute!(std::io::stdout(), SetCursorStyle::DefaultUserShape);
    result
}

/// `arc [--addr ws://host:port]` — anything else is refused, not guessed at.
fn url_from_args() -> Result<String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => Ok(DEFAULT_URL.to_owned()),
        [flag, url] if flag == "--addr" => Ok(url.clone()),
        _ => anyhow::bail!("usage: arc [--addr ws://host:port]"),
    }
}

async fn run(mut terminal: ratatui::DefaultTerminal, url: String) -> Result<()> {
    let (commands, command_rx) = mpsc::unbounded_channel();
    let (event_tx, mut events) = mpsc::unbounded_channel();
    tokio::spawn(net::run(url, command_rx, event_tx));

    let mut app = App::new();
    // Populate the picker and prove the daemon is there, in one request.
    let _ = commands.send(Command::List);

    let mut keys = EventStream::new();
    let mut cursor = Mode::Insert;
    set_cursor_style(cursor);

    while !app.quit {
        terminal.draw(|frame| ui::draw(frame, &mut app))?;

        let command = tokio::select! {
            key = keys.next() => match key {
                Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => app.on_key(key),
                // Resizes and the rest redraw on the next pass.
                Some(Ok(_)) => None,
                Some(Err(error)) => return Err(error.into()),
                None => break,
            },
            event = events.recv() => match event {
                Some(event) => app.on_net(event),
                // The connection task is gone; nothing more will ever arrive.
                None => anyhow::bail!("the connection task died"),
            },
        };

        if let Some(command) = command {
            // The task only stops when the command channel closes, and we
            // hold the sender.
            commands.send(command).expect("connection task alive");
        }
        if app.mode != cursor {
            cursor = app.mode;
            set_cursor_style(cursor);
        }
    }
    Ok(())
}

/// A bar while typing, a block in normal mode — the shape vim trained.
fn set_cursor_style(mode: Mode) {
    let style = match mode {
        Mode::Insert | Mode::Cmd => SetCursorStyle::SteadyBar,
        Mode::Normal => SetCursorStyle::SteadyBlock,
    };
    let mut out = std::io::stdout();
    let _ = crossterm::execute!(out, style);
    let _ = out.flush();
}
