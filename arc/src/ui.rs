use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::app::{App, Block, Mode, Status};
use crate::{markdown, theme};

const MARGIN: u16 = 2;

const CONTINUATION: &str = "  ";

const GAP: u16 = 1;

const WORDMARK: [&str; WORDMARK_ROWS as usize] = [
    r"  __ _ _ __ ___ ",
    r" / _` | '__/ __|",
    r"| (_| | | | (__ ",
    r" \__,_|_|  \___|",
];

const TAGLINE: &str = "autonomous robotic core";

const MASTHEAD: u16 = WORDMARK_ROWS + 1;

const WORDMARK_ROWS: u16 = 4;

const MASTHEAD_FLOOR: u16 = 12;

pub fn draw(frame: &mut Frame, app: &mut App) {
    let input_height = input_height(app, frame.area());
    let [transcript, _gap, rule, input] = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(GAP),
        Constraint::Length(1),
        Constraint::Length(input_height),
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
    if let Some(review) = &app.review {
        draw_review(frame, frame.area(), review);
    }
}

fn inset(area: Rect) -> Rect {
    Rect {
        x: area.x + MARGIN,
        width: area.width.saturating_sub(2 * MARGIN),
        ..area
    }
}

fn draw_transcript(frame: &mut Frame, area: Rect, app: &mut App) {
    let height = area.height as usize;
    let lines = transcript_lines(app, area.width as usize);
    let max_back = lines.len().saturating_sub(height);
    app.scroll_back = app.scroll_back.min(max_back);

    let end = lines.len() - app.scroll_back;
    let start = end.saturating_sub(height);
    // pad the top so a short transcript still sits on the bottom
    let mut visible: Vec<Line> = vec![Line::default(); height.saturating_sub(end - start)];
    visible.extend_from_slice(&lines[start..end]);
    frame.render_widget(Paragraph::new(visible), area);
    draw_scrollbar(frame, area, lines.len(), height, app.scroll_back);
}

fn draw_scrollbar(frame: &mut Frame, area: Rect, total: usize, height: usize, scroll_back: usize) {
    if total <= height || height == 0 {
        return;
    }

    let thumb = ((height * height) / total).max(1);
    let travel = height - thumb;
    let from_top = total - height - scroll_back.min(total - height);
    let top = (from_top * travel) / (total - height);

    let x = area.x + area.width;
    for row in 0..thumb {
        let y = area.y + u16::try_from(top + row).unwrap_or(u16::MAX);
        if y < area.y + area.height {
            frame.render_widget(Line::styled("|", theme::DIM), Rect::new(x, y, 1, 1));
        }
    }
}

fn transcript_lines(app: &App, width: usize) -> Vec<Line<'static>> {
    let mut out = Vec::new();
    let last = app.transcript.len().saturating_sub(1);
    let mut previous: Option<&Block> = None;
    for (i, block) in app.transcript.iter().enumerate() {
        let grouped = activity(block) && previous.is_some_and(activity);
        if !(out.is_empty() || grouped) {
            out.push(Line::default());
        }
        previous = Some(block);
        match block {
            Block::You(text) => {
                out.push(Line::styled("you", theme::DIM));
                push_wrapped(&mut out, text, width, theme::PLAIN);
            }
            Block::Arc { text, partial } => {
                out.push(Line::styled("arc", theme::ACCENT));
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
            Block::Thought {
                text,
                seconds,
                done,
                open,
            } => {
                let fold = if *open { '-' } else { '+' };
                let clock = if *done {
                    format!("{fold} thought for {seconds}s")
                } else {
                    format!("{fold} thinking {seconds}s")
                };
                out.push(Line::styled(clock, theme::DIM));
                if *open {
                    push_wrapped(&mut out, text, width, theme::DIM);
                }
            }
            Block::Tool { name, outcome, .. } => {
                let state = outcome.unwrap_or("...");
                out.push(Line::styled(format!("{name} {state}"), theme::DIM));
            }
        }
    }
    out
}

fn activity(block: &Block) -> bool {
    matches!(block, Block::Thought { .. } | Block::Tool { .. })
}

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

fn draw_masthead(frame: &mut Frame, area: Rect, app: &App) {
    let tagline = app.transcript.is_empty();
    let lines: Vec<Line> = WORDMARK
        .iter()
        .enumerate()
        .map(|(row, art)| {
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

const INPUT_ROWS_CAP: u16 = 8;

// measured before the layout: the input row has to grow with its wrapped text
fn input_height(app: &App, frame: Rect) -> u16 {
    let width = frame.width.saturating_sub(2 * MARGIN).max(1) as usize;
    let (prefix, text) = if app.mode == Mode::Cmd {
        (1, &app.cmd)
    } else {
        (2, &app.input)
    };
    let chars = prefix + text.chars().count();
    let rows = u16::try_from(chars / width + 1).unwrap_or(u16::MAX);
    rows.min(INPUT_ROWS_CAP).min(frame.height / 3).max(1)
}

fn draw_input(frame: &mut Frame, area: Rect, app: &App) {
    let (prefix, prefix_style, text, style, cursor) = if app.mode == Mode::Cmd {
        (":", theme::PLAIN, &app.cmd, theme::PLAIN, app.cmd.len())
    } else {
        let style = if app.picker.is_some() || app.review.is_some() {
            theme::DIM
        } else {
            theme::PLAIN
        };
        ("> ", theme::ACCENT, &app.input, style, app.cursor)
    };

    let width = (area.width.max(1)) as usize;
    let chars: Vec<char> = prefix.chars().chain(text.chars()).collect();
    let rows: Vec<String> = chars
        .chunks(width)
        .map(|chunk| chunk.iter().collect())
        .collect();
    let rows = if rows.is_empty() {
        vec![String::new()]
    } else {
        rows
    };

    let ahead = prefix.chars().count() + text[..cursor].chars().count();
    let cursor_row = ahead / width;
    let visible = area.height.max(1) as usize;
    let start = (cursor_row + 1).saturating_sub(visible);

    let lines: Vec<Line> = rows
        .iter()
        .enumerate()
        .skip(start)
        .take(visible)
        .map(|(index, row)| {
            if index == 0 {
                let body: String = row.chars().skip(prefix.chars().count()).collect();
                Line::from(vec![
                    Span::styled(prefix, prefix_style),
                    Span::styled(body, style),
                ])
            } else {
                Line::from(Span::styled(row.clone(), style))
            }
        })
        .collect();
    frame.render_widget(Paragraph::new(lines), area);

    if app.mode == Mode::Cmd || (app.picker.is_none() && app.review.is_none()) {
        let col = u16::try_from(ahead % width).unwrap_or(u16::MAX);
        let row = u16::try_from(cursor_row - start).unwrap_or(u16::MAX);
        frame.set_cursor_position((area.x.saturating_add(col), area.y.saturating_add(row)));
    }
}

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

    let now = chrono::Utc::now();
    let mut lines = vec![Line::styled(" sessions", theme::DIM), Line::default()];
    let visible = (height as usize).saturating_sub(3);
    let start = selected.saturating_sub(visible.saturating_sub(1));
    let room = (width as usize).saturating_sub(TIME_WIDTH + 5);
    for row in start..rows.min(start + visible) {
        let (prefix, style) = if row == selected {
            (" > ", theme::ACCENT)
        } else {
            ("   ", theme::DIM)
        };
        let spans = match app.picker_session(row) {
            None => vec![Span::styled(format!("{prefix}new session"), style)],
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

fn draw_review(frame: &mut Frame, full: Rect, review: &crate::app::Review) {
    let rows = review.items.len().max(1);
    let width = 72.min(full.width.saturating_sub(4));
    let detail = review
        .items
        .get(review.selected)
        .map_or(0, |entry| detail_height(entry, width));
    let height = u16::try_from(rows + 3 + detail)
        .unwrap_or(u16::MAX)
        .min(full.height.saturating_sub(2));
    let area = Rect {
        x: (full.width - width) / 2,
        y: (full.height - height) / 2,
        width,
        height,
    };
    frame.render_widget(Clear, area);

    let mut lines = vec![Line::styled(" review", theme::DIM), Line::default()];
    if review.items.is_empty() {
        let word = if review.loaded {
            "nothing to review"
        } else {
            "loading"
        };
        lines.push(Line::styled(format!("   {word}"), theme::DIM));
        frame.render_widget(Paragraph::new(lines), area);
        return;
    }

    let visible = (height as usize).saturating_sub(3 + detail);
    let start = review.selected.saturating_sub(visible.saturating_sub(1));
    let room = (width as usize).saturating_sub(ID_WIDTH + 5);
    let end = review.items.len().min(start + visible);
    for (row, entry) in review.items.iter().enumerate().take(end).skip(start) {
        let selected = row == review.selected;
        let (prefix, style) = if selected {
            (" > ", theme::ACCENT)
        } else {
            ("   ", theme::DIM)
        };
        let tag = if entry.superseded {
            " [superseded]"
        } else {
            ""
        };
        let label = elide(
            &format!(
                "{}/{}: {} — {}{tag}",
                arc_core::memory::kind_name(entry.kind),
                entry.namespace,
                entry.title,
                entry.summary
            ),
            room,
        );
        let mut spans = vec![
            Span::styled(format!("{prefix}{label:<room$}"), style),
            Span::styled(
                format!("  {:>ID_WIDTH$}", elide(&entry.id, ID_WIDTH)),
                theme::DIM,
            ),
        ];
        if selected && review.pending_delete {
            spans.push(Span::styled(" d deletes", theme::ERROR));
        }
        lines.push(Line::from(spans));
    }
    if let Some(entry) = review.items.get(review.selected) {
        lines.push(Line::default());
        for line in wrapped(&entry.summary, width as usize) {
            lines.push(Line::styled(format!("   {line}"), theme::PLAIN));
        }
        for line in wrapped(&entry.body, width as usize) {
            lines.push(Line::styled(format!("   {line}"), theme::DIM));
        }
    }
    frame.render_widget(Paragraph::new(lines), area);
}

// the list must stay stable while the detail below grows, so wrapping
// is measured here rather than left to the widget
fn wrapped(text: &str, width: usize) -> Vec<String> {
    let room = width.saturating_sub(4).max(20);
    let mut lines = Vec::new();
    for raw in text.lines() {
        let mut line = String::new();
        for word in raw.split_whitespace() {
            if !line.is_empty() && line.len() + 1 + word.len() > room {
                lines.push(std::mem::take(&mut line));
            }
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(word);
        }
        lines.push(line);
    }
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines
}

fn detail_height(entry: &crate::app::ReviewEntry, width: u16) -> usize {
    1 + wrapped(&entry.summary, width as usize).len() + wrapped(&entry.body, width as usize).len()
}

const ID_WIDTH: usize = 8;

fn elide(text: &str, room: usize) -> String {
    if text.chars().count() <= room {
        return text.to_owned();
    }
    let cut: String = text.chars().take(room.saturating_sub(1)).collect();
    format!("{}…", cut.trim_end())
}

const TIME_WIDTH: usize = 8;

fn label(session: &arc_proto::v1::SessionInfo, room: usize) -> String {
    let text = if session.title.is_empty() {
        &session.preview
    } else {
        &session.title
    };
    let first = text.lines().next().unwrap_or_default().trim();
    if first.is_empty() {
        return session.id.chars().take(8).collect();
    }
    elide(first, room)
}

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
        ..60 => "just now".to_owned(),
        60..3_600 => format!("{}m ago", seconds / 60),
        3_600..86_400 => format!("{}h ago", seconds / 3_600),
        86_400..604_800 => format!("{}d ago", seconds / 86_400),
        _ => at.with_timezone(&chrono::Local).format("%m-%d").to_string(),
    }
}
