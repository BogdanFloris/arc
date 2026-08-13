//! Drawing: state in, one frame out. No borders, no box-drawing — structure
//! is whitespace, `--` rules, and color from the terminal's own palette.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::app::{App, Block, Mode, Status};
use crate::{markdown, theme};

/// Left and right breathing room, in cells.
const MARGIN: u16 = 2;

/// How far a wrapped line sits in from the first line of its paragraph.
/// Matches the markdown renderer's own continuation indent.
const CONTINUATION: &str = "  ";

/// A blank row between the last message and the status rule, so a bottom-
/// anchored transcript does not crowd the input line.
const GAP: u16 = 1;

/// The wordmark: always at the top of the pane, transcript or no transcript.
const WORDMARK: [&str; WORDMARK_ROWS as usize] = [
    r"  __ _ _ __ ___ ",
    r" / _` | '__/ __|",
    r"| (_| | | | (__ ",
    r" \__,_|_|  \___|",
];

const TAGLINE: &str = "autonomous robotic core";

/// Rows the masthead takes: the wordmark plus one blank under it.
const MASTHEAD: u16 = WORDMARK_ROWS + 1;

/// [`WORDMARK`]'s row count, as a `u16` for layout without a cast.
const WORDMARK_ROWS: u16 = 4;

/// Below this many rows the masthead is dropped — on a short pane the
/// transcript needs the space more than the branding does.
const MASTHEAD_FLOOR: u16 = 12;

pub fn draw(frame: &mut Frame, app: &mut App) {
    let [transcript, _gap, rule, input] = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(GAP),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    let body = if transcript.height >= MASTHEAD_FLOOR {
        let [masthead, rest] =
            Layout::vertical([Constraint::Length(MASTHEAD), Constraint::Fill(1)]).areas(transcript);
        draw_masthead(frame, inset(masthead), app);
        rest
    } else {
        transcript
    };

    draw_transcript(frame, inset(body), app);
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
    let height = area.height as usize;
    // The bar lives in the right margin, so the text keeps its full width and
    // nothing reflows when a reply grows long enough to need one.
    let lines = transcript_lines(app, area.width as usize);
    // Clamp the scroll to the top of the transcript, in the state and not
    // just the view, so scrolling back down responds on the first key.
    let max_back = lines.len().saturating_sub(height);
    app.scroll_back = app.scroll_back.min(max_back);

    let end = lines.len() - app.scroll_back;
    let start = end.saturating_sub(height);
    // Anchored to the bottom: a short exchange sits just above the input
    // line, where the eye already is, instead of stranded at the top under a
    // screen of blank rows.
    let mut visible: Vec<Line> = vec![Line::default(); height.saturating_sub(end - start)];
    visible.extend_from_slice(&lines[start..end]);
    frame.render_widget(Paragraph::new(visible), area);
    draw_scrollbar(frame, area, lines.len(), height, app.scroll_back);
}

/// A thumb in the right margin showing how much of the transcript is on
/// screen and where.
///
/// Drawn only when there is something off screen — a bar that is always full
/// height is furniture, not information. No track and no arrows: a run of
/// `|` says both things at once, and the 6.1 look has no room for a rail
/// down the side of the pane.
fn draw_scrollbar(frame: &mut Frame, area: Rect, total: usize, height: usize, scroll_back: usize) {
    if total <= height || height == 0 {
        return;
    }

    // At least one cell, so a very long transcript still shows a thumb.
    let thumb = ((height * height) / total).max(1);
    let travel = height - thumb;
    // `scroll_back` counts up from the bottom; the thumb counts down from the
    // top, so the fraction is inverted.
    let from_top = total - height - scroll_back.min(total - height);
    let top = (from_top * travel) / (total - height);

    // The first column of the right margin: past the text, never over it, so
    // the wrap width is the same whether a bar is showing or not.
    let x = area.x + area.width;
    for row in 0..thumb {
        let y = area.y + u16::try_from(top + row).unwrap_or(u16::MAX);
        if y < area.y + area.height {
            frame.render_widget(Line::styled("|", theme::DIM), Rect::new(x, y, 1, 1));
        }
    }
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
                // What the user typed is shown as typed: markdown in the
                // input is text they wrote, not a document to re-render.
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
                out.extend(markdown::render(&text, width, theme::PLAIN));
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
///
/// Continuation lines sit in two columns, so a paragraph that ran on is never
/// mistaken for the start of a new one — the speaker labels are the only
/// thing at column zero.
fn push_wrapped(out: &mut Vec<Line<'static>>, text: &str, width: usize, style: Style) {
    let options = textwrap::Options::new(width.max(2)).subsequent_indent(CONTINUATION);
    for paragraph in text.split('\n') {
        if paragraph.is_empty() {
            out.push(Line::default());
            continue;
        }
        for line in textwrap::wrap(paragraph, options.clone()) {
            out.push(Line::styled(line.into_owned(), style));
        }
    }
}

/// The wordmark at the top of the pane, with the tagline beside it while the
/// transcript is still empty.
///
/// It stays put once messages arrive: the transcript scrolls underneath it,
/// never over it.
fn draw_masthead(frame: &mut Frame, area: Rect, app: &App) {
    let tagline = app.transcript.is_empty();
    let lines: Vec<Line> = WORDMARK
        .iter()
        .enumerate()
        .map(|(row, art)| {
            // On the wordmark's last row, so the two read as one mark.
            if tagline && row + 1 == WORDMARK.len() {
                Line::from(vec![
                    Span::styled(*art, theme::ACCENT),
                    Span::styled(format!("  {TAGLINE}"), theme::DIM),
                ])
            } else {
                Line::styled(*art, theme::ACCENT)
            }
        })
        .collect();
    frame.render_widget(Paragraph::new(lines), area);
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
    let width = 64.min(full.width.saturating_sub(4));
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

    // One clock read for the whole list, so two rows a second apart never
    // disagree about what "now" was.
    let now = chrono::Utc::now();
    let mut lines = vec![Line::styled(" sessions", theme::DIM), Line::default()];
    let visible = (height as usize).saturating_sub(3);
    let start = selected.saturating_sub(visible.saturating_sub(1));
    // Room for the row prefix, the time, and the two spaces between.
    let room = (width as usize).saturating_sub(TIME_WIDTH + 5);
    for row in start..rows.min(start + visible) {
        let (prefix, style) = if row == selected {
            (" > ", theme::ACCENT)
        } else {
            ("   ", theme::DIM)
        };
        let spans = match app.picker_session(row) {
            None => vec![Span::styled(format!("{prefix}new session"), style)],
            // The label is padded to a fixed column so the times form a clean
            // right-hand edge instead of a ragged one.
            Some(session) => vec![
                Span::styled(format!("{prefix}{:<room$}", label(session, room)), style),
                Span::styled(
                    format!("  {:>TIME_WIDTH$}", last_active(session, now)),
                    theme::DIM,
                ),
            ],
        };
        lines.push(Line::from(spans));
    }
    frame.render_widget(Paragraph::new(lines), area);
}

/// Columns [`last_active`] renders into: `just now`, `3h ago`, `08-13`.
const TIME_WIDTH: usize = 8;

/// What a session is called in the picker: its opening line, elided to fit.
///
/// Falls back to a slice of the id — a session can exist with nothing said in
/// it (the daemon names it before the first message lands), and an unlabelled
/// row is still one a person can pick.
fn label(session: &arc_proto::v1::SessionInfo, room: usize) -> String {
    let text = if session.title.is_empty() {
        &session.preview
    } else {
        &session.title
    };
    // Newlines would break the row; take the first line and let the ellipsis
    // stand for the rest.
    let first = text.lines().next().unwrap_or_default().trim();
    if first.is_empty() {
        return session.id.chars().take(8).collect();
    }
    if first.chars().count() <= room {
        return first.to_owned();
    }
    let cut: String = first.chars().take(room.saturating_sub(1)).collect();
    format!("{}…", cut.trim_end())
}

/// How long ago the session was last spoken in: `just now`, `12m ago`,
/// `3h ago`, `2d ago`, and a date once "ago" stops meaning anything.
///
/// Relative, because a picker answers "what was I just doing" — and the
/// answer to that is never a wall-clock time you have to subtract from now.
fn last_active(session: &arc_proto::v1::SessionInfo, now: chrono::DateTime<chrono::Utc>) -> String {
    let Some(at) = session
        .last_at
        .as_ref()
        .or(session.started_at.as_ref())
        .and_then(|ts| {
            chrono::DateTime::from_timestamp(ts.seconds, u32::try_from(ts.nanos).unwrap_or(0))
        })
    else {
        return String::new();
    };

    let seconds = (now - at).num_seconds();
    match seconds {
        // A clock that disagrees with the daemon's should not print "-4m ago".
        ..60 => "just now".to_owned(),
        60..3_600 => format!("{}m ago", seconds / 60),
        3_600..86_400 => format!("{}h ago", seconds / 3_600),
        86_400..604_800 => format!("{}d ago", seconds / 86_400),
        // Past a week "ago" stops being a unit anyone reads; give the date.
        _ => at.with_timezone(&chrono::Local).format("%m-%d").to_string(),
    }
}
