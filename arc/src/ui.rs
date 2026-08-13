//! Drawing: state in, one frame out. No borders, no box-drawing — structure
//! is whitespace, `--` rules, and color from the terminal's own palette.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::app::{App, Block, Mode, Status};
use crate::theme;

/// Left and right breathing room, in cells.
const MARGIN: u16 = 2;

/// The wordmark, shown on an empty transcript and nowhere else.
const WORDMARK: [&str; 4] = [
    r"  __ _ _ __ ___ ",
    r" / _` | '__/ __|",
    r"| (_| | | | (__ ",
    r" \__,_|_|  \___|",
];

const TAGLINE: &str = "autonomous robotic core";

pub fn draw(frame: &mut Frame, app: &mut App) {
    let [transcript, rule, input] = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    draw_transcript(frame, inset(transcript), app);
    draw_rule(frame, rule, app);
    draw_input(frame, inset(input), app);
    if let Some(selected) = app.picker {
        draw_picker(frame, frame.area(), app, selected);
    }
}

/// An area pulled in by [`MARGIN`] on both sides.
fn inset(area: Rect) -> Rect {
    Rect {
        x: area.x + MARGIN,
        width: area.width.saturating_sub(2 * MARGIN),
        ..area
    }
}

fn draw_transcript(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.transcript.is_empty() {
        draw_wordmark(frame, area);
        return;
    }

    let lines = transcript_lines(app, area.width as usize);
    // Clamp the scroll to the top of the transcript, in the state and not
    // just the view, so scrolling back down responds on the first key.
    let max_back = lines.len().saturating_sub(area.height as usize);
    app.scroll_back = app.scroll_back.min(max_back);

    let end = lines.len() - app.scroll_back;
    let start = end.saturating_sub(area.height as usize);
    let visible: Vec<Line> = lines[start..end].to_vec();
    frame.render_widget(Paragraph::new(visible), area);
}

/// The whole transcript as wrapped, styled lines.
fn transcript_lines(app: &App, width: usize) -> Vec<Line<'static>> {
    let mut out = Vec::new();
    let last = app.transcript.len().saturating_sub(1);
    for (i, block) in app.transcript.iter().enumerate() {
        if !out.is_empty() {
            out.push(Line::default());
        }
        match block {
            Block::You(text) => {
                out.push(Line::styled("you", theme::DIM));
                push_wrapped(&mut out, text, width, theme::PLAIN);
            }
            Block::Arc { text, partial } => {
                out.push(Line::styled("arc", theme::ACCENT));
                // The trailing underscore is the streaming indicator: it
                // rides the text, so there is nothing else to animate.
                let streaming = i == last && app.status == Status::Streaming;
                let text = if streaming {
                    format!("{text}_")
                } else {
                    text.clone()
                };
                push_wrapped(&mut out, &text, width, theme::PLAIN);
                if *partial {
                    out.push(Line::styled("-- cut --", theme::CUT));
                }
            }
            Block::Fault { code, msg } => {
                out.push(Line::styled(format!("! {code}"), theme::ERROR));
                push_wrapped(&mut out, msg, width, theme::DIM);
            }
            Block::Note(text) => {
                out.push(Line::styled(format!("-- {text} --"), theme::CUT));
            }
        }
    }
    out
}

/// Wraps `text` to `width`, one styled [`Line`] per wrapped line.
fn push_wrapped(out: &mut Vec<Line<'static>>, text: &str, width: usize, style: Style) {
    for paragraph in text.split('\n') {
        if paragraph.is_empty() {
            out.push(Line::default());
            continue;
        }
        for line in textwrap::wrap(paragraph, width.max(1)) {
            out.push(Line::styled(line.into_owned(), style));
        }
    }
}

fn draw_wordmark(frame: &mut Frame, area: Rect) {
    let mut lines: Vec<Line> = WORDMARK
        .iter()
        .map(|row| Line::styled(*row, theme::ACCENT))
        .collect();
    lines.push(Line::default());
    lines.push(Line::styled(TAGLINE, theme::DIM));

    let count = u16::try_from(lines.len()).unwrap_or(u16::MAX);
    let top = area.height.saturating_sub(count) / 2;
    let centered = Rect {
        y: area.y + top,
        height: area.height.saturating_sub(top),
        ..area
    };
    frame.render_widget(Paragraph::new(lines), centered);
}

/// The status rule: dashes across the width, the mode at the left edge,
/// state words at the right.
fn draw_rule(frame: &mut Frame, area: Rect, app: &App) {
    let mode = match app.mode {
        Mode::Insert => "-- insert ",
        Mode::Normal | Mode::Cmd => "",
    };
    let mut words: Vec<Span> = Vec::new();
    match app.status {
        Status::Streaming => words.push(Span::styled(" streaming", theme::DIM)),
        Status::Disconnected => words.push(Span::styled(" disconnected", theme::ERROR)),
        Status::Idle => {
            if let Some(code) = &app.last_error {
                words.push(Span::styled(format!(" {code}"), theme::ERROR));
            }
        }
    }
    if !app.queued.is_empty() {
        words.push(Span::styled(
            format!(" +{} queued", app.queued.len()),
            theme::DIM,
        ));
    }
    if !words.is_empty() {
        words.push(Span::styled(" --", theme::DIM));
    }

    let used: usize = mode.len() + words.iter().map(Span::width).sum::<usize>();
    let dashes = "-".repeat((area.width as usize).saturating_sub(used));
    let mut spans = vec![
        Span::styled(mode, theme::DIM),
        Span::styled(dashes, theme::DIM),
    ];
    spans.extend(words);
    frame.render_widget(Line::from(spans), area);
}

fn draw_input(frame: &mut Frame, area: Rect, app: &App) {
    if app.mode == Mode::Cmd {
        let line = Line::from(vec![
            Span::styled(":", theme::PLAIN),
            Span::styled(app.cmd.clone(), theme::PLAIN),
        ]);
        frame.render_widget(line, area);
        let ahead = u16::try_from(app.cmd.chars().count()).unwrap_or(u16::MAX);
        frame.set_cursor_position((area.x.saturating_add(1 + ahead), area.y));
        return;
    }

    let style = if app.picker.is_some() {
        theme::DIM
    } else {
        theme::PLAIN
    };
    let line = Line::from(vec![
        Span::styled("> ", theme::ACCENT),
        Span::styled(app.input.clone(), style),
    ]);
    frame.render_widget(line, area);

    if app.picker.is_none() {
        let ahead = u16::try_from(app.input[..app.cursor].chars().count()).unwrap_or(u16::MAX);
        frame.set_cursor_position((area.x.saturating_add(2 + ahead), area.y));
    }
}

/// The session picker: a cleared rectangle over the transcript, no border.
fn draw_picker(frame: &mut Frame, full: Rect, app: &App, selected: usize) {
    let rows = app.sessions.len() + 1;
    let width = 44.min(full.width.saturating_sub(4));
    let height = u16::try_from(rows + 3)
        .unwrap_or(u16::MAX)
        .min(full.height.saturating_sub(2));
    let area = Rect {
        x: (full.width - width) / 2,
        y: (full.height - height) / 2,
        width,
        height,
    };
    frame.render_widget(Clear, area);

    let mut lines = vec![Line::styled(" sessions", theme::DIM), Line::default()];
    let visible = (height as usize).saturating_sub(3);
    let start = selected.saturating_sub(visible.saturating_sub(1));
    for row in start..rows.min(start + visible) {
        let label = match app.picker_session(row) {
            None => "new session".to_owned(),
            Some(session) => session_label(session),
        };
        let (prefix, style) = if row == selected {
            (" > ", theme::ACCENT)
        } else {
            ("   ", theme::DIM)
        };
        lines.push(Line::styled(format!("{prefix}{label}"), style));
    }
    frame.render_widget(Paragraph::new(lines), area);
}

/// `2026-08-13 10:58  a1b2c3d4` — when it started, and enough id to grep.
fn session_label(session: &arc_proto::v1::SessionInfo) -> String {
    let started = session
        .started_at
        .as_ref()
        .and_then(|ts| {
            chrono::DateTime::from_timestamp(ts.seconds, u32::try_from(ts.nanos).unwrap_or(0))
        })
        .map_or_else(
            || "????-??-?? ??:??".to_owned(),
            |utc| {
                utc.with_timezone(&chrono::Local)
                    .format("%Y-%m-%d %H:%M")
                    .to_string()
            },
        );
    let id: String = session.id.chars().take(8).collect();
    format!("{started}  {id}")
}
