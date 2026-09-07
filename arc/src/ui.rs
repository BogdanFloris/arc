use std::ops::Range;

use arc_proto::v1::{JobInfo, SessionRole, job_info};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::symbols::border;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block as Panel, Borders, Clear, Paragraph};

use crate::app::{App, Block, Mode, Overlay, Status, format_tokens};
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

    let masthead_height = if app.transcript.is_empty() && transcript.height >= MASTHEAD_FLOOR {
        MASTHEAD
    } else {
        3
    };
    let [masthead, body] =
        Layout::vertical([Constraint::Length(masthead_height), Constraint::Fill(1)])
            .areas(transcript);
    if masthead_height == MASTHEAD {
        draw_masthead(frame, inset(masthead), app);
    } else {
        draw_session_heading(frame, inset(masthead), app);
    }

    draw_transcript(frame, inset(body), app);
    draw_rule(frame, rule, app);
    draw_input(frame, inset(input), app);
    match &app.overlay {
        Overlay::Picker(picker) => draw_picker(frame, frame.area(), app, picker),
        Overlay::Review(review) => draw_review(frame, frame.area(), review),
        Overlay::Jobs(jobs) => draw_jobs(frame, frame.area(), jobs),
        Overlay::Projects(projects) => draw_projects(frame, frame.area(), projects),
        Overlay::Models(models) => draw_models(frame, frame.area(), models),
        Overlay::Help { .. } => draw_help(frame, app, frame.area()),
        Overlay::Inspection(_) => draw_inspection(frame, app, frame.area()),
        Overlay::None => {}
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
    let (lines, bounds) = transcript_layout(app, area.width as usize);
    let max_back = lines.len().saturating_sub(height);
    app.scroll_back = app.scroll_back.min(max_back);
    if std::mem::take(&mut app.restore_anchor) {
        if let Some((block, offset)) = app.viewport_anchor {
            if let Some(&(from, to)) = bounds.get(block) {
                let start = from + offset.min(to.saturating_sub(from + 1));
                app.scroll_back = lines.len().saturating_sub(start + height).min(max_back);
            }
        }
    }

    if let Some(boundary) = app.visual_boundary().or_else(|| app.search_block()) {
        bring_into_view(app, &bounds, boundary, lines.len(), height, max_back);
    }

    let end = lines.len() - app.scroll_back;
    let start = end.saturating_sub(height);
    app.visible_blocks = bounds
        .iter()
        .enumerate()
        .filter(|(_, (from, to))| *to > start && *from < end)
        .map(|(index, _)| index)
        .collect();
    app.viewport_anchor = app
        .visible_blocks
        .first()
        .map(|&index| (index, start.saturating_sub(bounds[index].0)));
    let selected = highlight_ranges(app, &bounds);
    // pad the top so a short transcript still sits on the bottom
    let mut visible: Vec<Line> = vec![Line::default(); height.saturating_sub(end - start)];
    visible.extend((start..end).map(|i| {
        if selected.iter().any(|range| range.contains(&i)) {
            lines[i]
                .clone()
                .patch_style(Style::new().add_modifier(Modifier::REVERSED))
        } else {
            lines[i].clone()
        }
    }));
    frame.render_widget(Paragraph::new(visible), area);
    draw_scrollbar(frame, area, lines.len(), height, app.scroll_back);
}

// keeps the boundary block's first line on screen
fn bring_into_view(
    app: &mut App,
    bounds: &[(usize, usize)],
    boundary: usize,
    total: usize,
    height: usize,
    max_back: usize,
) {
    let Some(&(block_start, _)) = bounds.get(boundary) else {
        return;
    };
    let end = total.saturating_sub(app.scroll_back);
    let start = end.saturating_sub(height);
    if block_start < start {
        app.scroll_back = total.saturating_sub(block_start + height).min(max_back);
    } else if block_start >= end {
        app.scroll_back = total.saturating_sub(block_start + 1).min(max_back);
    }
}

// line ranges rendered reversed: the visual selection, plus the current search match
fn highlight_ranges(app: &App, bounds: &[(usize, usize)]) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    if let Some((lo, hi)) = app.visual_range() {
        if let (Some(&(from, _)), Some(&(_, to))) = (bounds.get(lo), bounds.get(hi)) {
            ranges.push(from..to);
        }
    }
    if let Some(block) = app.search_block() {
        if let Some(&(from, to)) = bounds.get(block) {
            ranges.push(from..to);
        }
    }
    ranges
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

// rendered lines, plus each block's (start, end) line range
fn transcript_layout(app: &App, width: usize) -> (Vec<Line<'static>>, Vec<(usize, usize)>) {
    let mut out = Vec::new();
    let mut bounds = Vec::with_capacity(app.transcript.len());
    let last = app.transcript.len().saturating_sub(1);
    let mut previous: Option<&Block> = None;
    for (i, block) in app.transcript.iter().map(|entry| &entry.block).enumerate() {
        let grouped = activity(block) && previous.is_some_and(activity);
        if !(out.is_empty() || grouped) {
            out.push(Line::default());
        }
        previous = Some(block);
        let block_start = out.len();
        match block {
            Block::You(text) => {
                out.push(Line::styled("you", theme::DIM));
                push_wrapped(&mut out, text, width, theme::PLAIN);
            }
            Block::System(text) => {
                out.push(Line::styled("system", theme::DIM));
                push_wrapped(&mut out, text, width, theme::DIM);
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
            Block::Handback {
                subject,
                body,
                open,
            } => {
                out.push(Line::styled(
                    elide(&format!("± {subject}"), width),
                    theme::DIM,
                ));
                if *open {
                    push_wrapped(&mut out, body, width, theme::DIM);
                }
            }
            Block::Tool {
                name,
                args,
                outcome,
                content,
                open,
                ..
            } => {
                let state = outcome.unwrap_or("running");
                let fold = if *open { "−" } else { "+" };
                let args = crate::app::tool_summary(args).replace(['\n', '\r'], " ");
                let suffix = format!(" · {state}");
                let header = format!("{fold} {name} {args}");
                out.push(Line::from(vec![
                    Span::styled(
                        elide(
                            header.trim_end(),
                            width.saturating_sub(suffix.chars().count()),
                        ),
                        theme::DIM,
                    ),
                    Span::styled(
                        suffix,
                        if *outcome == Some("error") {
                            theme::ERROR
                        } else {
                            theme::DIM
                        },
                    ),
                ]));
                if *open && !content.is_empty() {
                    push_capped(&mut out, content, width, theme::DIM);
                }
            }
            Block::Sources(sources) => {
                for (title, uri) in sources {
                    let line = if title == uri {
                        format!("- {uri}")
                    } else {
                        format!("- {title} ({uri})")
                    };
                    out.push(Line::styled(elide(&line, width), theme::DIM));
                }
            }
            Block::Cost {
                input_tokens,
                output_tokens,
                seconds,
            } => {
                let text = format!(
                    "{} in · {} out · {seconds:.1}s",
                    format_tokens(u64::from(*input_tokens)),
                    format_tokens(u64::from(*output_tokens)),
                );
                out.push(Line::styled(text, theme::DIM));
            }
            Block::StepCapped => {
                out.push(Line::styled(
                    "stopped at the step cap — say continue to keep going",
                    theme::DIM,
                ));
            }
        }
        bounds.push((block_start, out.len()));
    }
    (out, bounds)
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

// the tool already capped the text at its own output limit; this caps the
// screen space an open result can take, independent of that
const TOOL_CONTENT_CAP: usize = 40;

fn push_capped(out: &mut Vec<Line<'static>>, text: &str, width: usize, style: Style) {
    let mut wrapped = Vec::new();
    push_wrapped(&mut wrapped, text, width, style);
    if wrapped.len() > TOOL_CONTENT_CAP {
        let cut = wrapped.len() - TOOL_CONTENT_CAP;
        wrapped.truncate(TOOL_CONTENT_CAP);
        out.extend(wrapped);
        out.push(Line::styled(
            format!("… {cut} more lines · o full output"),
            theme::DIM,
        ));
    } else {
        out.extend(wrapped);
    }
}

fn draw_session_heading(frame: &mut Frame, area: Rect, app: &App) {
    let session = app
        .session_id
        .as_ref()
        .and_then(|id| app.sessions.iter().find(|s| &s.id == id));
    let title = session
        .filter(|s| !s.title.is_empty())
        .map(|s| s.title.as_str())
        .or_else(|| {
            app.transcript.iter().find_map(|entry| match &entry.block {
                Block::You(text) => text.lines().next(),
                _ => None,
            })
        })
        .unwrap_or("New conversation");
    let model = match session {
        Some(session) if session.model.is_empty() => "model not recorded".to_owned(),
        Some(session) => format!("model: {}", session.model),
        None => "loading model…".to_owned(),
    };
    let room = usize::from(area.width);
    let door = elide(
        &app.open_door_label().unwrap_or_else(|| "chat".to_owned()),
        room / 3,
    );
    let title = elide(title, room.saturating_sub(door.chars().count() + 3));
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled(door, theme::ACCENT),
                Span::styled(" · ", theme::DIM),
                Span::styled(title, theme::STRONG),
            ]),
            Line::styled(elide(&model, room), theme::DIM),
            Line::styled("─".repeat(room), theme::DIM),
        ]),
        area,
    );
}

fn draw_inspection(frame: &mut Frame, app: &mut App, full: Rect) {
    let Overlay::Inspection(inspection) = &mut app.overlay else {
        return;
    };
    let Some(block) = app
        .transcript
        .get(inspection.block)
        .map(|entry| &entry.block)
    else {
        return;
    };
    let (title, content) = match block {
        Block::Tool {
            name,
            args,
            outcome,
            content,
            ..
        } => {
            let arguments = match serde_json::from_str::<serde_json::Value>(args) {
                Ok(serde_json::Value::Object(fields)) => fields
                    .iter()
                    .map(|(key, value)| {
                        format!(
                            "{key}: {}",
                            value
                                .as_str()
                                .map_or_else(|| value.to_string(), str::to_owned)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
                _ => args.clone(),
            };
            (
                format!("{name} · {}", outcome.unwrap_or("running")),
                format!("Arguments\n{arguments}\n\nOutput\n{content}"),
            )
        }
        Block::Thought { text, .. } => ("thought".to_owned(), text.clone()),
        Block::Handback { subject, body, .. } => (subject.clone(), body.clone()),
        _ => return,
    };
    let area = popup(
        frame,
        full,
        full.width.saturating_sub(4),
        full.height.saturating_sub(4),
        &title,
    );
    let [body, footer] = Layout::vertical([Constraint::Fill(1), Constraint::Length(1)]).areas(area);
    let mut lines = Vec::new();
    push_wrapped(&mut lines, &content, usize::from(body.width), theme::PLAIN);
    let total = lines.len();
    inspection.scroll = inspection
        .scroll
        .min(total.saturating_sub(usize::from(body.height)));
    let start = inspection.scroll;
    frame.render_widget(
        Paragraph::new(
            lines
                .into_iter()
                .skip(start)
                .take(usize::from(body.height))
                .collect::<Vec<_>>(),
        ),
        body,
    );
    frame.render_widget(
        Line::styled(
            elide(
                &format!("j/k scroll · g/G top/end · q close · {}/{total}", start + 1),
                usize::from(footer.width),
            ),
            theme::DIM,
        ),
        footer,
    );
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
    if area.height > WORDMARK_ROWS {
        frame.render_widget(
            Line::styled("Enter send · Esc normal · Ctrl-P sessions", theme::DIM),
            Rect::new(area.x, area.y + WORDMARK_ROWS, area.width, 1),
        );
    }
}

fn draw_rule(frame: &mut Frame, area: Rect, app: &App) {
    if let Some(job) = app.strip_job() {
        draw_strip(frame, area, app, job);
        return;
    }
    let mode_word = match app.mode {
        Mode::Insert => "insert",
        Mode::Visual => "visual",
        Mode::Normal | Mode::Cmd => "",
    };
    let mut left: Vec<String> = Vec::new();
    if !mode_word.is_empty() {
        left.push(mode_word.to_owned());
    }
    left.push(app.open_door_label().unwrap_or_else(|| "chat".to_owned()));
    if app.review_pending > 0 {
        left.push(format!("review {}", app.review_pending));
    }
    let mode = if left.is_empty() {
        String::new()
    } else {
        format!("-- {} ", left.join(" "))
    };
    let mut words: Vec<Span> = Vec::new();
    match app.status {
        Status::Streaming => {
            let seconds = app.turn_elapsed_seconds().unwrap_or(0);
            let tokens = format_tokens(app.streamed_tokens_estimate());
            let stop = if matches!(app.overlay, Overlay::Help { .. }) {
                ""
            } else {
                " · Esc Esc stop"
            };
            words.push(Span::styled(
                format!(" streaming {seconds}s · ~{tokens} tok{stop}"),
                theme::DIM,
            ));
        }
        Status::Disconnected => words.push(Span::styled(" disconnected", theme::DIM)),
        Status::Idle => {
            if let Some(code) = &app.last_error {
                words.push(Span::styled(format!(" {code}"), theme::ERROR));
            } else {
                let hint = match app.mode {
                    Mode::Normal => " Tab chat/code · o inspect · ? help",
                    Mode::Visual => " j/k select · Enter inspect · f fork · Esc back",
                    Mode::Insert => " Esc normal · Ctrl-P sessions",
                    Mode::Cmd => " :chat · :code · :help",
                };
                words.push(Span::styled(hint, theme::DIM));
            }
        }
    }
    if let Some(note) = &app.yank_note {
        words.push(Span::styled(format!(" {note}"), theme::DIM));
    }
    if !words.is_empty() {
        words.push(Span::styled(" --", theme::DIM));
    }

    let used: usize = mode.chars().count() + words.iter().map(Span::width).sum::<usize>();
    let dashes = "-".repeat((area.width as usize).saturating_sub(used));
    let mut spans = vec![
        Span::styled(mode, theme::DIM),
        Span::styled(dashes, theme::DIM),
    ];
    spans.extend(words);
    frame.render_widget(Line::from(spans), area);
}

// running jobs only: a finished job's evidence is its handback in the
// conversation, not a line that lingers here
fn draw_strip(frame: &mut Frame, area: Rect, app: &App, job: &JobInfo) {
    frame.render_widget(Line::styled(strip_label(app, job), theme::DIM), area);
}

fn strip_label(app: &App, job: &JobInfo) -> String {
    let count = app.running_job_count();
    let noun = if count == 1 { "job" } else { "jobs" };
    let activity = if job.last_call.is_empty() {
        format!("step {}", job.tool_steps)
    } else {
        job.last_call.clone()
    };
    format!(
        " {count} {noun} · {} {} · {} tok · {}s · {} - {}s ago",
        job_subject(job),
        job_state_word(job.state),
        format_tokens(job.spent_tokens),
        app.strip_elapsed_seconds(job),
        activity,
        app.strip_idle_seconds(job),
    )
}

const INPUT_ROWS_CAP: u16 = 8;

// walks the char stream once, tracking both the wrapped rows and where the
// cursor lands, so an embedded newline and a width wrap agree on the row
fn wrap_input(chars: &[char], cursor_index: usize, width: usize) -> (Vec<String>, usize, usize) {
    let width = width.max(1);
    let mut rows = vec![String::new()];
    let mut col = 0usize;
    let mut cursor = None;
    for (i, &c) in chars.iter().enumerate() {
        if c != '\n' && col == width {
            rows.push(String::new());
            col = 0;
        }
        if i == cursor_index {
            cursor = Some((rows.len() - 1, col));
        }
        if c == '\n' {
            rows.push(String::new());
            col = 0;
        } else {
            rows.last_mut().expect("at least one row").push(c);
            col += 1;
        }
    }
    // a cursor exactly at a full row's end wraps to the row after, even
    // though that row has no characters in it yet
    let (cursor_row, cursor_col) = match cursor {
        Some(pos) => pos,
        None if col == width => (rows.len(), 0),
        None => (rows.len() - 1, col),
    };
    (rows, cursor_row, cursor_col)
}

// measured before the layout: the input row has to grow with its wrapped text
fn input_height(app: &App, frame: Rect) -> u16 {
    let width = frame.width.saturating_sub(2 * MARGIN).max(1) as usize;
    let filtering = app.picker().is_some_and(|picker| picker.filtering);
    let (prefix, text): (&str, &str) = if app.mode == Mode::Cmd {
        (":", &app.cmd)
    } else if filtering || app.searching {
        ("/", &app.input)
    } else {
        ("> ", &app.input)
    };
    let chars: Vec<char> = prefix.chars().chain(text.chars()).collect();
    let count = chars.len();
    let (rows, cursor_row, _) = wrap_input(&chars, count, width);
    let needed = rows.len().max(cursor_row + 1);
    let rows = u16::try_from(needed).unwrap_or(u16::MAX);
    rows.min(INPUT_ROWS_CAP).min(frame.height / 3).max(1)
}

fn draw_input(frame: &mut Frame, area: Rect, app: &App) {
    let filtering = app.picker().is_some_and(|picker| picker.filtering);
    let (prefix, prefix_style, text, style, cursor) = if app.mode == Mode::Cmd {
        (":", theme::PLAIN, &app.cmd, theme::PLAIN, app.cmd.len())
    } else if filtering || app.searching {
        ("/", theme::ACCENT, &app.input, theme::PLAIN, app.cursor)
    } else {
        let style = if app.overlay == Overlay::None {
            theme::PLAIN
        } else {
            theme::DIM
        };
        ("> ", theme::ACCENT, &app.input, style, app.cursor)
    };

    let width = (area.width.max(1)) as usize;
    let chars: Vec<char> = prefix.chars().chain(text.chars()).collect();
    let cursor_index = prefix.chars().count() + text[..cursor].chars().count();
    let (rows, cursor_row, cursor_col) = wrap_input(&chars, cursor_index, width);

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

    if app.mode == Mode::Cmd || filtering || app.searching || app.overlay == Overlay::None {
        let col = u16::try_from(cursor_col).unwrap_or(u16::MAX);
        let row = u16::try_from(cursor_row.saturating_sub(start)).unwrap_or(u16::MAX);
        frame.set_cursor_position((area.x.saturating_add(col), area.y.saturating_add(row)));
    }
}

const POPUP_BORDER: border::Set = border::Set {
    top_left: "+",
    top_right: "+",
    bottom_left: "+",
    bottom_right: "+",
    vertical_left: "|",
    vertical_right: "|",
    horizontal_top: "-",
    horizontal_bottom: "-",
};

// every popup shares one frame: ASCII border, dim, name in the top rule
fn popup(frame: &mut Frame, full: Rect, want_width: u16, content: u16, title: &str) -> Rect {
    let width = want_width.min(full.width.saturating_sub(4));
    let height = content
        .saturating_add(2)
        .min(full.height.saturating_sub(2))
        .max(3);
    let area = Rect {
        x: (full.width.saturating_sub(width)) / 2,
        y: (full.height.saturating_sub(height)) / 2,
        width,
        height,
    };
    frame.render_widget(Clear, area);
    let block = Panel::new()
        .borders(Borders::ALL)
        .border_set(POPUP_BORDER)
        .border_style(theme::ACCENT)
        .title(Span::styled(format!(" {title} "), theme::ACCENT));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    inner
}

enum PickerRow {
    Flat(Option<String>),
    Tree(Vec<bool>),
}

fn draw_picker(frame: &mut Frame, full: Rect, app: &App, picker: &crate::app::Picker) {
    let rows: Vec<(&arc_proto::v1::SessionInfo, PickerRow)> = if picker.tree {
        app.picker_tree_rows()
            .into_iter()
            .map(|(session, flags)| (session, PickerRow::Tree(flags)))
            .collect()
    } else {
        app.picker_flat_rows()
            .into_iter()
            .map(|(session, parent)| (session, PickerRow::Flat(parent)))
            .collect()
    };
    let height = rows.len() + 1;
    let scope = if picker.show_all {
        "all projects + jobs"
    } else {
        app.open_project().unwrap_or("conversations")
    };
    let view = if picker.tree { "tree" } else { "recent" };
    let abandoned = if picker.show_abandoned {
        " + abandoned"
    } else {
        ""
    };
    let title = format!("sessions · {scope} · {view}{abandoned}");
    let inner = popup(
        frame,
        full,
        full.width.saturating_sub(8).clamp(64, 120),
        u16::try_from(height + 2).unwrap_or(u16::MAX),
        &title,
    );
    let [area, footer] =
        Layout::vertical([Constraint::Fill(1), Constraint::Length(2)]).areas(inner);
    let selected_title = app
        .picker_session(picker.selected)
        .map_or("New session", |session| {
            if session.title.is_empty() {
                session.preview.lines().next().unwrap_or("")
            } else {
                session.title.as_str()
            }
        });
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(
                elide(selected_title, usize::from(footer.width)),
                theme::PLAIN,
            ),
            Line::styled(
                "/ filter · Tab view · a all · x abandoned · Enter open",
                theme::DIM,
            ),
        ]),
        footer,
    );
    let width = area.width;

    let now = chrono::Utc::now();
    let visible = area.height as usize;
    let start = picker.selected.saturating_sub(visible.saturating_sub(1));
    for row in start..height.min(start + visible) {
        let prefix = if row == picker.selected { " > " } else { "   " };
        let spans = match row.checked_sub(1).and_then(|i| rows.get(i)) {
            None => {
                let style = theme::PLAIN;
                vec![Span::styled(format!("{prefix}new session"), style)]
            }
            Some((session, row_view)) => {
                let job = crate::app::is_job_session(session);
                let style = theme::PLAIN;
                let (connectors, lineage) = match row_view {
                    PickerRow::Flat(parent) => (
                        String::new(),
                        parent
                            .as_ref()
                            .map(|p| format!("  \\ of {p}"))
                            .unwrap_or_default(),
                    ),
                    PickerRow::Tree(flags) => (tree_prefix(flags), String::new()),
                };
                // connector cells are bullet-wide, so `│` lands under the parent
                let active = app.session_id.as_deref() == Some(session.id.as_str());
                let bullet = if active { "● " } else { "○ " };
                let tag = disposition_tag(session)
                    .map(|c| format!("  [{c}]"))
                    .unwrap_or_default();
                let room = (width as usize).saturating_sub(
                    3 + connectors.chars().count()
                        + bullet.chars().count()
                        + lineage.chars().count()
                        + TIME_WIDTH
                        + 2
                        + TAG_WIDTH,
                );
                let metadata = if job {
                    let role =
                        SessionRole::try_from(session.role).unwrap_or(SessionRole::Unspecified);
                    format!(
                        " {}/{}",
                        arc_core::provider::role_label(role),
                        session.project
                    )
                } else {
                    String::new()
                };
                let metadata = elide(&metadata, room / 2);
                let title_room = room.saturating_sub(metadata.chars().count());
                let mut spans = vec![
                    Span::styled(prefix.to_owned(), style),
                    Span::styled(connectors, theme::DIM),
                    Span::styled(bullet, if active { theme::ACCENT } else { theme::DIM }),
                    Span::styled(
                        format!("{:<title_room$}", label(session, title_room)),
                        style,
                    ),
                ];
                spans.push(Span::styled(metadata, theme::DIM));
                if !lineage.is_empty() {
                    spans.push(Span::styled(lineage, theme::DIM));
                }
                spans.push(Span::styled(
                    format!("  {:>TIME_WIDTH$}", last_active(session, now)),
                    theme::DIM,
                ));
                spans.push(Span::styled(format!("{tag:<TAG_WIDTH$}"), theme::DIM));
                spans
            }
        };
        let style = if row == picker.selected {
            Style::new().bg(ratatui::style::Color::Indexed(236))
        } else {
            theme::PLAIN
        };
        let row_area = Rect::new(
            area.x,
            area.y + u16::try_from(row - start).unwrap_or(u16::MAX),
            width,
            1,
        );
        frame.render_widget(Paragraph::new(Line::from(spans)).style(style), row_area);
    }
}

fn tree_prefix(flags: &[bool]) -> String {
    let mut prefix = String::new();
    for (level, continues) in flags.iter().enumerate() {
        if level + 1 == flags.len() {
            prefix.push_str(if *continues { "├─ " } else { "└─ " });
        } else {
            prefix.push_str(if *continues { "│  " } else { "   " });
        }
    }
    prefix
}

// lineage is an annotation, not a hierarchy: position stays pure recency.
// A root has no disposition to show; a branch's is unmarked/real/abandoned
fn disposition_tag(session: &arc_proto::v1::SessionInfo) -> Option<char> {
    use arc_proto::v1::branch_marked::Disposition;
    if session.parent_session.is_empty() {
        return None;
    }
    Some(match Disposition::try_from(session.disposition) {
        Ok(Disposition::Real) => '+',
        Ok(Disposition::Abandoned) => 'x',
        Ok(Disposition::Unspecified) | Err(_) => '?',
    })
}

fn draw_review(frame: &mut Frame, full: Rect, review: &crate::app::Review) {
    let rows = review.items.len().max(1);
    let inner_width = 72.min(full.width.saturating_sub(4)).saturating_sub(2);
    // sized to the deepest entry so the pane never resizes as the selection moves
    let detail = review
        .items
        .iter()
        .map(|entry| detail_height(entry, inner_width))
        .max()
        .unwrap_or(0)
        .min(REVIEW_DETAIL_CAP);
    let area = popup(
        frame,
        full,
        72,
        u16::try_from(rows + detail + 2).unwrap_or(u16::MAX),
        "review",
    );
    let width = area.width;

    let mut lines = Vec::new();
    if review.items.is_empty() {
        let word = if review.loaded {
            "nothing to review"
        } else {
            "loading"
        };
        lines.push(Line::styled(format!("   {word}"), theme::DIM));
        lines.push(review_rule(width));
        lines.push(review_footer(review));
        frame.render_widget(Paragraph::new(lines), area);
        return;
    }

    let visible = (area.height as usize).saturating_sub(detail + 2);
    let start = review.selected.saturating_sub(visible.saturating_sub(1));
    let room = (width as usize).saturating_sub(ID_WIDTH + 5);
    let end = review.items.len().min(start + visible);
    for (row, entry) in review.items.iter().enumerate().take(end).skip(start) {
        let selected = row == review.selected;
        let (prefix, style) = if selected {
            (" > ", theme::ACCENT)
        } else {
            ("   ", theme::PLAIN)
        };
        let label = elide(&review_label(entry), room);
        lines.push(Line::from(vec![
            Span::styled(format!("{prefix}{label:<room$}"), style),
            Span::styled(
                format!("  {:>ID_WIDTH$}", tail(&entry.id, ID_WIDTH)),
                theme::DIM,
            ),
        ]));
    }
    if let Some(entry) = review.items.get(review.selected) {
        let mut room_left = detail;
        lines.push(Line::default());
        room_left = room_left.saturating_sub(1);
        for line in wrapped(&entry.summary, width as usize) {
            if room_left == 0 {
                break;
            }
            lines.push(Line::styled(format!("   {line}"), theme::PLAIN));
            room_left -= 1;
        }
        for (id, title) in &entry.supersedes {
            if room_left == 0 {
                break;
            }
            lines.push(Line::from(vec![
                Span::styled(format!("   replaces {title}"), theme::ACCENT),
                Span::styled(format!("  {}", tail(id, ID_WIDTH)), theme::DIM),
            ]));
            room_left -= 1;
        }
        for line in wrapped(&entry.body, width as usize) {
            if room_left == 0 {
                break;
            }
            lines.push(Line::styled(format!("   {line}"), theme::DIM));
            room_left -= 1;
        }
        for _ in 0..room_left {
            lines.push(Line::default());
        }
    }
    lines.push(review_rule(width));
    lines.push(review_footer(review));
    frame.render_widget(Paragraph::new(lines), area);
}

fn review_rule(width: u16) -> Line<'static> {
    Line::styled("─".repeat(width as usize), theme::DIM)
}

fn review_footer(review: &crate::app::Review) -> Line<'static> {
    if review.pending_delete {
        Line::styled("   dd deletes the selected record", theme::ERROR)
    } else {
        Line::styled(
            "   a accept · dd delete · f fix · r refresh · q close",
            theme::DIM,
        )
    }
}

fn draw_jobs(frame: &mut Frame, full: Rect, jobs: &crate::app::Jobs) {
    let footer = usize::from(jobs.confirmation.is_some());
    let rows = jobs.items.len().max(1);
    let area = popup(
        frame,
        full,
        72,
        u16::try_from(rows + footer).unwrap_or(u16::MAX),
        "jobs",
    );
    let width = area.width;

    let mut lines = Vec::new();
    if jobs.items.is_empty() {
        let word = if jobs.loaded {
            "no jobs this daemon"
        } else {
            "loading"
        };
        lines.push(Line::styled(format!("   {word}"), theme::DIM));
        if let Some(confirmation) = &jobs.confirmation {
            lines.push(Line::styled(format!("   {confirmation}"), theme::DIM));
        }
        frame.render_widget(Paragraph::new(lines), area);
        return;
    }

    let visible = (area.height as usize).saturating_sub(footer);
    let start = jobs.selected.saturating_sub(visible.saturating_sub(1));
    let room = (width as usize).saturating_sub(ID_WIDTH + 5);
    let end = jobs.items.len().min(start + visible);
    for (row, job) in jobs.items.iter().enumerate().take(end).skip(start) {
        let selected = row == jobs.selected;
        let running = job.state == job_info::State::Running as i32;
        let (prefix, style) = match (selected, running) {
            (true, _) => (" > ", theme::ACCENT),
            (false, true) => ("   ", theme::PLAIN),
            (false, false) => ("   ", theme::DIM),
        };
        let label = elide(&job_label(job), room);
        let spans = vec![
            Span::styled(format!("{prefix}{label:<room$}"), style),
            Span::styled(
                format!("  {:>ID_WIDTH$}", tail(&job.session_id, ID_WIDTH)),
                theme::DIM,
            ),
        ];
        lines.push(Line::from(spans));
    }
    if let Some(confirmation) = &jobs.confirmation {
        lines.push(Line::styled(format!("   {confirmation}"), theme::DIM));
    }
    frame.render_widget(Paragraph::new(lines), area);
}

fn menu_area(
    frame: &mut Frame,
    full: Rect,
    width: u16,
    rows: usize,
    title: &str,
    hint: &str,
) -> Rect {
    let inner = popup(
        frame,
        full,
        width,
        u16::try_from(rows + 1).unwrap_or(u16::MAX),
        title,
    );
    let [body, footer] =
        Layout::vertical([Constraint::Fill(1), Constraint::Length(1)]).areas(inner);
    frame.render_widget(
        Line::styled(elide(hint, usize::from(footer.width)), theme::DIM),
        footer,
    );
    body
}

fn draw_projects(frame: &mut Frame, full: Rect, projects: &crate::app::Projects) {
    let rows = projects.items.len().max(1);
    let area = menu_area(
        frame,
        full,
        72,
        rows,
        "code",
        "j/k select · Enter open code session · q close",
    );

    let mut lines = Vec::new();
    if projects.items.is_empty() {
        let word = if projects.loaded {
            "no projects configured"
        } else {
            "loading"
        };
        lines.push(Line::styled(format!("   {word}"), theme::DIM));
        frame.render_widget(Paragraph::new(lines), area);
        return;
    }

    let name_width = projects
        .items
        .iter()
        .map(|p| p.name.chars().count())
        .max()
        .unwrap_or(0);
    let visible = area.height as usize;
    let start = projects.selected.saturating_sub(visible.saturating_sub(1));
    let room = (area.width as usize).saturating_sub(name_width + 5);
    let end = projects.items.len().min(start + visible);
    for (row, project) in projects.items.iter().enumerate().take(end).skip(start) {
        let selected = row == projects.selected;
        let (prefix, style) = if selected {
            (" > ", theme::ACCENT)
        } else {
            ("   ", theme::PLAIN)
        };
        lines.push(Line::from(vec![
            Span::styled(format!("{prefix}{:<name_width$}", project.name), style),
            Span::styled(
                format!("  {}", elide(&project.description, room)),
                theme::DIM,
            ),
        ]));
    }
    frame.render_widget(Paragraph::new(lines), area);
}

fn draw_models(frame: &mut Frame, full: Rect, models: &crate::app::Models) {
    let rows = models.items.len().max(1);
    let area = menu_area(
        frame,
        full,
        78,
        rows,
        "model",
        "Enter sets default for new sessions and forks · q close",
    );

    let mut lines = Vec::new();
    if models.items.is_empty() {
        let word = if models.loaded {
            "no model choices configured"
        } else {
            "loading"
        };
        lines.push(Line::styled(format!("   {word}"), theme::DIM));
        frame.render_widget(Paragraph::new(lines), area);
        return;
    }

    let name_width = models
        .items
        .iter()
        .map(|c| c.name.chars().count())
        .max()
        .unwrap_or(0);
    let visible = area.height as usize;
    let start = models.selected.saturating_sub(visible.saturating_sub(1));
    let room = (area.width as usize).saturating_sub(name_width + 16);
    let end = models.items.len().min(start + visible);
    for (row, choice) in models.items.iter().enumerate().take(end).skip(start) {
        let pointed = row == models.selected;
        let (prefix, style) = if pointed {
            (" > ", theme::ACCENT)
        } else {
            ("   ", theme::PLAIN)
        };
        let role = arc_core::provider::role_label(
            arc_proto::v1::SessionRole::try_from(choice.role)
                .unwrap_or(arc_proto::v1::SessionRole::Unspecified),
        );
        let mark = if choice.selected { "*" } else { " " };
        let detail = format!("{} {} {}", choice.provider, choice.model, choice.thinking);
        lines.push(Line::from(vec![
            Span::styled(format!("{prefix}{role:<9} "), theme::DIM),
            Span::styled(format!("{mark}{:<name_width$}", choice.name), style),
            Span::styled(format!("  {}", elide(&detail, room)), theme::DIM),
        ]));
    }
    frame.render_widget(Paragraph::new(lines), area);
}

// grouped and built from the table below so a changed key and its
// documentation land in the same diff
const HELP: &[(&str, &[&str])] = &[
    (
        "coding essentials",
        &[
            "enter             send; typing during work steers the next step",
            "esc esc           stop a turn (from insert mode)",
            "tab               switch chat/code in normal mode",
            ":chat :code       concierge / project picker",
            "ctrl-p            find a session; / filters, enter opens the match",
            "ctrl-o            fold the pointed or visible tool/thought",
            "v then j/k        point at messages, tools, or thoughts",
            "o or visual enter full arguments and output; j/k scroll, G end",
            "O                 expand/collapse all, retaining your place",
            ":compact          compact the current idle session",
            "M                 defaults for new sessions; open sessions stay pinned",
        ],
    ),
    (
        "normal mode",
        &[
            "i I a A           insert (before, line start, after, line end)",
            "h l 0 $           left, right, line start, line end",
            "w b               word forward / back",
            "x D dd            delete char / to end / whole line",
            "j k               scroll transcript",
            "ctrl-u ctrl-d     page up / down",
            "G gg              scroll to bottom / top",
            "/ n N             search (n older, N newer)",
            "ctrl-n ctrl-p     step matches while the / prompt is open",
            "s ctrl-p          open the session picker",
            "y                 yank the last reply (V for a range)",
            "Y                 yank the whole conversation",
            "V                 visual mode: select a block range",
            "v                 point visual: walk one block (:fork acts there)",
            "R                 rewind: walk your messages; enter reforks before it, refills the input",
            "ctrl-t            back to the previous session",
            "ctrl-n            new session",
            "ctrl-o            fold one tool / thought / handback",
            "? J Q             help / jobs / review queue popups",
            "C                 pick a project; enter opens it like :code",
            "M                 pick a model per role; * marks the current one, the pick outlives restarts",
            "ctrl-c            quit",
            ":                 command mode",
        ],
    ),
    (
        "visual mode",
        &[
            "j k               move the selection boundary (v: the point)",
            "gg G              selection to the first / last block",
            "y                 yank the selection",
            "f                 fork at the pointed message and open it",
            ":fork             branch at the pointed message and open it",
            "esc               back to normal mode",
        ],
    ),
    (
        "insert mode",
        &[
            "esc               back to normal mode",
            "enter             send",
            "ctrl-j            insert a newline",
        ],
    ),
    (
        "command mode",
        &[
            ":q :q! :qa :quit  quit",
            ":review           open the review pane",
            ":jobs             open the jobs pane",
            ":model            open the model picker",
            ":code <project>   open a bound executor session, no dispatch",
            "                  without a project, open the project picker",
            ":fork             branch at the visual selection",
            ":help             this popup (j k scroll it)",
        ],
    ),
    (
        "picker keys",
        &[
            "j k               move selection",
            "tab               toggle flat recency / tree view",
            "/                 filter by title/preview",
            "space a           toggle showing dispatched jobs",
            "x                 toggle showing abandoned branches",
            "m                 mark the selected branch real",
            "X                 mark the selected branch abandoned",
            "enter             open the selected session",
            "q esc             close (esc also clears an active filter)",
        ],
    ),
    (
        "review keys",
        &[
            "j k               move selection",
            "a                 accept the selected record",
            "dd                delete the selected record",
            "f                 prefill a fix instruction and close",
            "r                 refresh the list",
            "q esc             close",
        ],
    ),
    (
        "jobs keys",
        &[
            "j k               move selection",
            "r                 refresh the list",
            "enter             open the selected job's session",
            "x                 cancel the selected job (if running)",
            "d                 drop its queued steers (if any)",
            "q esc             close",
        ],
    ),
];

fn draw_help(frame: &mut Frame, app: &mut App, full: Rect) {
    let total = u16::try_from(help_line_count()).unwrap_or(u16::MAX);
    let area = popup(frame, full, 60, total, "help");
    let width = area.width.saturating_sub(2).max(8) as usize;

    // wrap first, then window: long entries fold instead of clipping
    let mut lines = Vec::new();
    for (i, (group, keys)) in HELP.iter().enumerate() {
        if i > 0 {
            lines.push(Line::default());
        }
        lines.push(Line::styled(format!(" {group}"), theme::DIM));
        for key in *keys {
            for folded in textwrap::wrap(key, width.saturating_sub(3).max(8)) {
                lines.push(Line::styled(format!("   {folded}"), theme::DIM));
            }
        }
    }

    let visible = area.height as usize;
    // write the clamp back, like the transcript does with scroll_back —
    // otherwise every extra j is debt that k has to repay
    let Overlay::Help { scroll } = &mut app.overlay else {
        return;
    };
    *scroll = (*scroll).min(lines.len().saturating_sub(visible));
    let top = *scroll;
    let more_below = lines.len() > top + visible;
    let mut lines: Vec<Line> = lines.into_iter().skip(top).take(visible).collect();
    if more_below {
        if let Some(last) = lines.last_mut() {
            *last = Line::styled("   ... j to scroll", theme::CUT);
        }
    }
    frame.render_widget(Paragraph::new(lines), area);
}

fn help_line_count() -> usize {
    HELP.iter()
        .map(|(_, keys)| keys.len() + 2)
        .sum::<usize>()
        .saturating_sub(1)
}

fn job_subject(job: &JobInfo) -> String {
    if job.title.is_empty() {
        let role = arc_core::provider::role_label(
            SessionRole::try_from(job.role).unwrap_or(SessionRole::Unspecified),
        );
        format!("{role}/{}", job.project)
    } else {
        job.title.clone()
    }
}

fn job_label(job: &JobInfo) -> String {
    let state = job_state_word(job.state);
    let subject = job_subject(job);
    let budget = if job.budget_tokens == 0 {
        "-".to_owned()
    } else {
        format_tokens(job.budget_tokens)
    };
    let mut label = format!(
        "{state} {subject} {}/{budget} tok {}s",
        format_tokens(job.spent_tokens),
        job.elapsed_seconds
    );
    if job.queued_steers > 0 {
        use std::fmt::Write as _;
        let _ = write!(label, " · {} queued", job.queued_steers);
    }
    label
}

fn job_state_word(state: i32) -> &'static str {
    match job_info::State::try_from(state) {
        Ok(job_info::State::Running) => "running",
        Ok(job_info::State::Finished) => "done",
        Ok(job_info::State::Failed) => "failed",
        Ok(job_info::State::OverBudget) => "over",
        Ok(job_info::State::Unspecified) | Err(_) => "unknown",
    }
}

fn tail(id: &str, width: usize) -> &str {
    &id[id.len().saturating_sub(width)..]
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
    1 + wrapped(&entry.summary, width as usize).len()
        + entry.supersedes.len()
        + wrapped(&entry.body, width as usize).len()
}

const REVIEW_DETAIL_CAP: usize = 9;

fn review_label(entry: &crate::app::ReviewEntry) -> String {
    format!(
        "{}/{}: {} — {}",
        arc_core::memory::kind_name(entry.kind),
        entry.namespace,
        entry.title,
        entry.summary
    )
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

// reserved on every row so the time column stays put
const TAG_WIDTH: usize = 5;

fn label(session: &arc_proto::v1::SessionInfo, room: usize) -> String {
    let text = if session.title.is_empty() {
        &session.preview
    } else {
        &session.title
    };
    let first = text.lines().next().unwrap_or_default().trim();
    if first.is_empty() {
        let fallback = if session.project.is_empty() {
            "(empty)".to_owned()
        } else {
            format!("(empty) · {}", session.project)
        };
        return elide(&fallback, room);
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

    let seconds = (now - at).num_seconds().max(0);
    match seconds {
        ..60 => "now".to_owned(),
        60..3_600 => format!("{}m", seconds / 60),
        3_600..86_400 => format!("{}h", seconds / 3_600),
        86_400..604_800 => format!("{}d", seconds / 86_400),
        _ => format!("{}w", seconds / 604_800),
    }
}

#[cfg(test)]
mod tests {
    use crate::app::{Entry, Overlay};
    use crate::theme;
    use arc_proto::v1::{JobInfo, ModelChoice, SessionInfo, SessionRole, job_info};
    use crossterm::event::{KeyCode, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::style::Modifier;

    use super::{disposition_tag, draw, job_label, label, last_active, strip_label, wrap_input};
    use crate::app::{App, Block, Mode, Models, Search, Status};

    #[test]
    fn the_model_picker_lists_each_roles_choices_and_marks_the_current_one() {
        let mut app = App::new();
        let choice = |role: SessionRole, name: &str, model: &str, selected: bool| ModelChoice {
            role: role as i32,
            name: name.to_owned(),
            provider: "codex".to_owned(),
            model: model.to_owned(),
            thinking: "medium".to_owned(),
            selected,
        };
        app.overlay = Overlay::Models(Models {
            items: vec![
                choice(SessionRole::Concierge, "astra", "gpt-6-astra", true),
                choice(SessionRole::Executor, "sol", "gpt-5.6-sol", true),
                choice(SessionRole::Executor, "glm-flash", "glm-5.3-flash", false),
            ],
            selected: 2,
            loaded: true,
        });

        let text = plain_text(&rendered(&mut app));

        assert!(
            text.contains(" model "),
            "the popup is titled model:\n{text}"
        );
        assert!(
            text.contains("   concierge *astra      codex gpt-6-astra medium"),
            "{text}"
        );
        assert!(
            text.contains("   executor  *sol        codex gpt-5.6-sol medium"),
            "{text}"
        );
        assert!(
            text.contains(" > executor   glm-flash  codex glm-5.3-flash medium"),
            "the pointed row carries the cursor, not the mark:\n{text}"
        );
    }

    fn rendered(app: &mut App) -> ratatui::buffer::Buffer {
        rendered_at(app, 100, 30)
    }

    fn rendered_at(app: &mut App, width: u16, height: u16) -> ratatui::buffer::Buffer {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal.draw(|frame| draw(frame, app)).expect("draw");
        terminal.backend().buffer().clone()
    }

    #[test]
    fn working_header_uses_the_door_and_recorded_model_at_both_widths() {
        use crate::app::NetEvent;

        let mut app = conversation();
        let mut info = session(
            "code",
            "Repair adjacent long session titles in the navigation picker",
            "",
        );
        info.role = SessionRole::Executor as i32;
        info.source = arc_proto::v1::Source::User as i32;
        info.project = "arc".to_owned();
        info.model = "pinned-model".to_owned();
        app.on_net(NetEvent::Sessions(vec![info.clone()]));
        app.session_id = Some(info.id.clone());
        for width in [120, 40] {
            let buffer = rendered_at(&mut app, width, 12);
            let text = plain_text(&buffer);
            assert!(text.contains("code/arc · Repair"), "{text}");
            assert!(text.contains("model: pinned-model"), "{text}");
            assert!(buffer[(13, 0)].modifier.contains(Modifier::BOLD));
            assert_eq!(buffer[(2, 0)].fg, theme::ACCENT.fg.unwrap());
            assert_eq!(buffer[(2, 1)].fg, theme::DIM.fg.unwrap());
            assert!(
                text.lines()
                    .nth(2)
                    .unwrap()
                    .contains(&"─".repeat(usize::from(width - 4)))
            );
            println!("HEADER {width}\n{text}");
        }
        info.model.clear();
        app.on_net(NetEvent::Sessions(vec![info]));
        assert!(plain_text(&rendered(&mut app)).contains("model not recorded"));
        app.on_net(NetEvent::Sessions(vec![]));
        assert!(plain_text(&rendered(&mut app)).contains("loading model…"));
    }

    #[test]
    fn adjacent_long_picker_titles_have_readable_styles_and_full_row_selection() {
        use crate::app::NetEvent;
        use ratatui::style::Color;

        let mut app = App::new();
        let mut one = session(
            "one",
            "Repair parser boundary handling for adjacent long session titles",
            "",
        );
        one.source = arc_proto::v1::Source::User as i32;
        let mut two = session(
            "two",
            "Repair picker selection styling for adjacent long session titles",
            "",
        );
        two.source = arc_proto::v1::Source::User as i32;
        let at = Some(prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        });
        one.last_at = at;
        two.last_at = at;
        app.on_net(NetEvent::Sessions(vec![one, two]));
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('s')));
        if let Overlay::Picker(picker) = &mut app.overlay {
            picker.selected = 1;
        }
        for width in [140, 40] {
            let buffer = rendered_at(&mut app, width, 16);
            let text = plain_text(&buffer);
            let rows: Vec<_> = text
                .lines()
                .enumerate()
                .filter(|(_, line)| line.contains("○ Repair"))
                .collect();
            assert_eq!(rows.len(), 2, "{text}");
            assert_eq!(rows[1].0, rows[0].0 + 1);
            let popup_width = width.saturating_sub(8).clamp(64, 120).min(width - 4);
            let left = (width - popup_width) / 2 + 1;
            for (y, line) in rows {
                let y = u16::try_from(y).expect("row");
                let selected = line.contains('>');
                assert!(line.contains("now"), "{line}");
                let title_x = left + 5;
                assert_eq!(buffer[(title_x, y)].fg, Color::Reset);
                assert_eq!(
                    buffer[(left + popup_width - 9, y)].fg,
                    theme::DIM.fg.unwrap()
                );
                for x in left..left + popup_width - 2 {
                    assert_eq!(
                        buffer[(x, y)].bg,
                        if selected {
                            Color::Indexed(236)
                        } else {
                            Color::Reset
                        }
                    );
                }
                if width == 140 {
                    assert!(line.contains("adjacent long session titles"), "{line}");
                } else {
                    assert!(line.contains('…'), "{line}");
                }
            }
            println!("PICKER {width}\n{text}");
        }
    }

    #[test]
    fn streaming_stop_guidance_appears_once_in_the_frame() {
        let mut app = conversation();
        app.status = Status::Streaming;
        let text = plain_text(&rendered(&mut app));
        assert_eq!(text.matches("Esc Esc stop").count(), 1, "{text}");
        println!("STREAMING\n{text}");
        app.overlay = Overlay::Help { scroll: 0 };
        let text = plain_text(&rendered(&mut app));
        assert!(!text.contains("Esc Esc stop"), "{text}");
        assert_eq!(text.matches("stop a turn").count(), 1, "{text}");
        println!("STREAMING HELP\n{text}");
    }

    #[test]
    fn the_session_header_and_full_tool_view_work_at_two_widths() {
        use crate::app::{Block, NetEvent};

        let mut app = App::new();
        let mut info = session(
            "s-code",
            "Session picker keyboard navigation",
            "Fix navigation",
        );
        info.model = "pinned-model".to_owned();
        app.on_net(NetEvent::Sessions(vec![info]));
        app.session_id = Some("s-code".to_owned());
        app.set_blocks(vec![
            Block::You("Fix session picker navigation".to_owned()),
            Block::Tool {
                call_id: "t1".to_owned(),
                name: "bash".to_owned(),
                args: r#"{"command":"just test","workdir":"/workspace/arc"}"#.to_owned(),
                outcome: Some("error"),
                content: (1..=80)
                    .map(|n| format!("output line {n}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
                open: false,
            },
        ]);
        app.on_key(key(KeyCode::Esc));
        let text = plain_text(&rendered(&mut app));
        assert!(text.contains("Session picker keyboard navigation"));
        assert!(text.contains("pinned-model"));
        assert!(text.contains("error"));
        println!("SESSION FRAME\n{text}");
        app.on_key(key(KeyCode::Char('o')));
        let text = plain_text(&rendered(&mut app));
        assert!(text.contains("/workspace/arc"));
        app.on_key(key(KeyCode::Char('G')));
        let text = plain_text(&rendered(&mut app));
        assert!(text.contains("output line 80"));
        app.on_key(key(KeyCode::Char('G')));
        let narrow = plain_text(&rendered_at(&mut app, 40, 12));
        assert!(narrow.contains("output line 80"));
        app.on_key(key(KeyCode::Char('q')));
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn folding_one_tool_preserves_the_top_visible_block() {
        use crate::app::Block;

        let mut app = App::new();
        app.transcript = (0..40)
            .map(|n| Block::Tool {
                call_id: n.to_string(),
                name: "read".to_owned(),
                args: format!("file-{n}"),
                outcome: Some("ok"),
                content: "detail\n".repeat(12),
                open: false,
            })
            .map(Entry::from)
            .collect();
        app.scroll_back = 20;
        rendered(&mut app);
        let anchor = app.viewport_anchor;
        app.on_key(crossterm::event::KeyEvent::new(
            KeyCode::Char('o'),
            KeyModifiers::CONTROL,
        ));
        rendered(&mut app);
        assert_eq!(app.viewport_anchor, anchor);
        assert_eq!(
            app.transcript
                .iter()
                .filter(|entry| matches!(&entry.block, Block::Tool { open: true, .. }))
                .count(),
            1
        );
    }

    fn reversed(text: &str, buffer: &ratatui::buffer::Buffer) -> Vec<String> {
        let mut rows = Vec::new();
        for y in 0..buffer.area.height {
            let mut row = String::new();
            let mut run = String::new();
            for x in 0..buffer.area.width {
                let cell = buffer.cell((x, y)).expect("cell");
                if cell.modifier.contains(ratatui::style::Modifier::REVERSED) {
                    run.push_str(cell.symbol());
                } else if !run.is_empty() {
                    row.push_str(run.trim());
                    row.push(' ');
                    run.clear();
                }
            }
            if !run.is_empty() {
                row.push_str(run.trim());
            }
            if !row.is_empty() {
                rows.push(row.trim().to_owned());
            }
        }
        rows.into_iter().filter(|row| row.contains(text)).collect()
    }

    fn plain_text(buffer: &ratatui::buffer::Buffer) -> String {
        let mut out = String::new();
        for y in 0..buffer.area.height {
            for x in 0..buffer.area.width {
                out.push_str(buffer.cell((x, y)).expect("cell").symbol());
            }
            out.push('\n');
        }
        out
    }

    fn search(app: &mut App, query: &str) {
        app.on_key(key(KeyCode::Char('/')));
        typed(app, query);
        app.on_key(key(KeyCode::Enter));
    }

    fn key(code: KeyCode) -> crossterm::event::KeyEvent {
        crossterm::event::KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn typed(app: &mut App, text: &str) {
        for c in text.chars() {
            app.on_key(key(crossterm::event::KeyCode::Char(c)));
        }
    }

    fn conversation() -> App {
        let mut app = App::new();
        app.on_key(key(crossterm::event::KeyCode::Esc));
        for text in [
            "old message",
            "needle here",
            "newer message",
            "needle again",
        ] {
            app.push_block(Block::You(text.to_owned()));
        }
        app
    }

    #[test]
    fn a_confirmed_search_scrolls_to_and_highlights_exactly_one_block() {
        let mut app = conversation();
        for i in 0..60 {
            app.push_block(Block::You(format!("filler {i}")));
        }
        app.scroll_back = 0; // parked at the bottom, the match sits far above
        search(&mut app, "needle here");
        assert_eq!(app.search_block(), Some(1));

        let buffer = rendered(&mut app);
        assert!(app.scroll_back > 0, "the view followed the match");
        assert_eq!(
            reversed("needle", &buffer),
            vec!["needle here"],
            "exactly the match renders reversed"
        );
    }

    fn tool_block(open: bool, content: &str) -> Block {
        Block::Tool {
            call_id: "t1".to_owned(),
            name: "bash".to_owned(),
            args: "cargo test".to_owned(),
            outcome: Some("ok"),
            content: content.to_owned(),
            open,
        }
    }

    fn ctrl(c: char) -> crossterm::event::KeyEvent {
        crossterm::event::KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    #[test]
    fn a_collapsed_and_an_open_tool_block_render_side_by_side() {
        let mut app = App::new();
        app.on_key(key(KeyCode::Esc));
        app.push_block(Block::You("run the tests".to_owned()));
        app.push_block(tool_block(false, "cargo test\n... 42 passed"));
        app.push_block(Block::You("what failed earlier?".to_owned()));
        app.push_block(tool_block(
            true,
            "bash -lc 'cargo test tool_result'\nrunning 3 tests\ntest a ... ok",
        ));

        let text = plain_text(&rendered(&mut app));
        assert!(text.contains("bash cargo test · ok"), "both headers render");
        assert!(
            !text.contains("42 passed"),
            "the collapsed block's content stays hidden"
        );
        assert!(
            text.contains("running 3 tests") && text.contains("test a ... ok"),
            "the open block's content renders wrapped beneath its header"
        );
    }

    #[test]
    fn a_collapsed_tool_block_renders_as_one_line() {
        let mut app = App::new();
        app.on_key(key(KeyCode::Esc));
        app.push_block(Block::You("run the tests".to_owned()));
        app.push_block(tool_block(false, "running 42 tests\nall green\n"));

        let buffer = rendered(&mut app);
        let text = plain_text(&buffer);
        assert!(text.contains("bash cargo test · ok"), "the header renders");
        assert!(
            !text.contains("running 42 tests") && !text.contains("all green"),
            "collapsed content stays off screen"
        );
    }

    #[test]
    fn ctrl_o_opens_the_collapsed_tool_block_and_its_content_appears() {
        let mut app = App::new();
        app.on_key(key(KeyCode::Esc));
        app.push_block(Block::You("run the tests".to_owned()));
        app.push_block(tool_block(false, "running 42 tests\nall green"));

        app.on_key(ctrl('o'));

        let text = plain_text(&rendered(&mut app));
        assert!(
            text.contains("running 42 tests") && text.contains("all green"),
            "ctrl-o opened it, the same gesture that opens a thought"
        );
    }

    #[test]
    fn a_long_open_tool_result_is_capped_at_forty_lines_with_a_marker() {
        let mut app = App::new();
        app.on_key(key(KeyCode::Esc));
        let content = (1..=45)
            .map(|n| format!("line {n}"))
            .collect::<Vec<_>>()
            .join("\n");
        app.push_block(tool_block(true, &content));

        let text = plain_text(&rendered(&mut app));
        assert!(text.contains("line 40"), "the cap keeps the first 40");
        assert!(!text.contains("line 41"), "the rest is cut");
        assert!(
            text.contains("5 more lines"),
            "the marker names how many were cut, got: {text}"
        );
    }

    #[test]
    fn the_rule_line_shows_elapsed_seconds_and_streamed_size_while_streaming() {
        let mut app = conversation();
        app.status = Status::Streaming;

        let buffer = rendered(&mut app);
        assert!(
            plain_text(&buffer).contains("streaming 0s"),
            "the counter renders on the rule line"
        );
    }

    #[test]
    fn the_rule_line_hides_the_counter_when_idle() {
        let mut app = conversation();

        let buffer = rendered(&mut app);
        assert!(!plain_text(&buffer).contains("streaming"));
    }

    #[test]
    fn the_rule_line_shows_the_review_queue_when_it_holds_records() {
        let mut app = conversation();
        app.review_pending = 2;

        let buffer = rendered(&mut app);
        assert!(plain_text(&buffer).contains("review 2"));
    }

    #[test]
    fn the_review_detail_names_the_record_a_supersede_replaced() {
        let mut app = App::new();
        app.overlay = Overlay::Review(crate::app::Review {
            items: vec![crate::app::ReviewEntry {
                id: "mr-new".to_owned(),
                kind: 4,
                namespace: "global".to_owned(),
                title: "address".to_owned(),
                summary: "lives at Y".to_owned(),
                body: "moved in spring".to_owned(),
                supersedes: vec![("mr-old".to_owned(), "old address".to_owned())],
            }],
            selected: 0,
            loaded: true,
            pending_delete: false,
        });

        let text = plain_text(&rendered(&mut app));
        assert!(text.contains("replaces old address"), "{text}");
        assert!(!text.contains("[superseded]"));
    }

    #[test]
    fn the_review_picker_shows_an_action_footer_and_arms_delete_there() {
        let mut app = App::new();
        app.overlay = Overlay::Review(crate::app::Review {
            items: vec![crate::app::ReviewEntry {
                id: "mr-new".to_owned(),
                kind: 4,
                namespace: "global".to_owned(),
                title: "address".to_owned(),
                summary: "lives at Y".to_owned(),
                body: "moved in spring".to_owned(),
                supersedes: Vec::new(),
            }],
            selected: 0,
            loaded: true,
            pending_delete: false,
        });

        let text = plain_text(&rendered(&mut app));
        assert!(
            text.contains("a accept · dd delete · f fix · r refresh · q close"),
            "the footer teaches the keys, got: {text:?}"
        );
        assert!(
            !text.contains("d deletes"),
            "unarmed shows no delete warning"
        );

        app.review_mut().expect("open").pending_delete = true;
        let text = plain_text(&rendered(&mut app));
        assert!(
            text.contains("dd deletes the selected record"),
            "the armed state replaces the hint in the footer, got: {text:?}"
        );
    }

    #[test]
    fn the_review_detail_is_capped_so_the_pane_never_grows_with_the_body() {
        let mut app = App::new();
        let body = (0..60)
            .map(|i| format!("body line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        app.overlay = Overlay::Review(crate::app::Review {
            items: vec![crate::app::ReviewEntry {
                id: "mr-new".to_owned(),
                kind: 4,
                namespace: "global".to_owned(),
                title: "address".to_owned(),
                summary: "lives at Y".to_owned(),
                body,
                supersedes: Vec::new(),
            }],
            selected: 0,
            loaded: true,
            pending_delete: false,
        });

        let text = plain_text(&rendered(&mut app));
        assert!(text.contains("body line 0"), "{text}");
        for i in 30..60 {
            assert!(
                !text.contains(&format!("body line {i}")),
                "the detail caps at a screen-filling body: {text:?}"
            );
        }
    }

    fn review_entry(id: &str, body: &str) -> crate::app::ReviewEntry {
        crate::app::ReviewEntry {
            id: id.to_owned(),
            kind: 4,
            namespace: "global".to_owned(),
            title: format!("title {id}"),
            summary: format!("summary {id}"),
            body: body.to_owned(),
            supersedes: Vec::new(),
        }
    }

    fn footer_row(text: &str) -> usize {
        text.lines()
            .position(|line| line.contains("a accept · dd delete"))
            .expect("the footer renders")
    }

    #[test]
    fn the_review_pane_does_not_resize_as_the_selection_moves() {
        let mut app = App::new();
        let deep = (0..40)
            .map(|i| format!("body line {i}"))
            .collect::<Vec<_>>()
            .join(" ");
        app.overlay = Overlay::Review(crate::app::Review {
            items: vec![
                review_entry("mr-short", "tiny body"),
                review_entry("mr-deep", &deep),
            ],
            selected: 0,
            loaded: true,
            pending_delete: false,
        });

        let short = footer_row(&plain_text(&rendered(&mut app)));
        app.review_mut().expect("open").selected = 1;
        let deep = footer_row(&plain_text(&rendered(&mut app)));

        assert_eq!(
            short, deep,
            "the pane is sized to the deepest entry, not the selected one"
        );
    }

    #[test]
    fn the_review_footer_is_separated_from_the_detail_by_a_rule() {
        let mut app = App::new();
        app.overlay = Overlay::Review(crate::app::Review {
            items: vec![review_entry("mr-1", "body text")],
            selected: 0,
            loaded: true,
            pending_delete: false,
        });

        let text = plain_text(&rendered(&mut app));
        let lines: Vec<&str> = text.lines().collect();
        let footer = footer_row(&text);
        let divider = lines[footer - 1];
        assert!(
            divider.contains('─') && !divider.contains("accept"),
            "a dim rule sits directly above the footer, got: {divider:?}"
        );
        let detail_above = lines[footer - 2];
        assert!(
            detail_above.contains("body text") || detail_above.trim().is_empty(),
            "the rule divides detail from controls, got: {detail_above:?}"
        );
    }

    #[test]
    fn the_empty_review_pane_still_shows_the_close_footer() {
        let mut app = App::new();
        app.overlay = Overlay::Review(crate::app::Review {
            items: Vec::new(),
            selected: 0,
            loaded: true,
            pending_delete: false,
        });

        let text = plain_text(&rendered(&mut app));
        assert!(text.contains("nothing to review"), "{text}");
        assert!(
            text.contains("q close"),
            "the empty pane teaches its way out: {text:?}"
        );
    }

    #[test]
    fn the_rule_line_hides_the_review_segment_at_zero() {
        let mut app = conversation();

        let buffer = rendered(&mut app);
        assert!(!plain_text(&buffer).contains("review"));
    }

    #[test]
    fn a_visual_selection_and_a_search_never_highlight_together() {
        let mut app = conversation();
        app.on_key(key(KeyCode::Char('V')));
        assert_eq!(app.mode, Mode::Visual, "V selects the last block");

        let buffer = rendered(&mut app);
        assert_eq!(reversed("needle again", &buffer).len(), 1);
        assert!(reversed("needle here", &buffer).is_empty());

        app.mode = Mode::Normal;
        app.search = Some(Search {
            query: "needle here".to_owned(),
            matches: vec![1],
            current: 0,
        });

        let buffer = rendered(&mut app);
        assert_eq!(reversed("needle here", &buffer).len(), 1);
        assert!(reversed("needle again", &buffer).is_empty());
    }

    fn session(id: &str, title: &str, preview: &str) -> SessionInfo {
        SessionInfo {
            provider: String::new(),
            model: String::new(),
            id: id.to_owned(),
            title: title.to_owned(),
            started_at: None,
            preview: preview.to_owned(),
            last_at: None,
            role: 0,
            project: String::new(),
            dispatched_by: String::new(),
            source: 0,
            parent_session: String::new(),
            disposition: 0,
        }
    }

    fn active_ago(now: chrono::DateTime<chrono::Utc>, seconds_ago: i64) -> SessionInfo {
        let at = now - chrono::Duration::seconds(seconds_ago);
        SessionInfo {
            provider: String::new(),
            model: String::new(),
            id: "s".to_owned(),
            title: String::new(),
            preview: String::new(),
            started_at: None,
            last_at: Some(prost_types::Timestamp {
                seconds: at.timestamp(),
                nanos: 0,
            }),
            role: 0,
            project: String::new(),
            dispatched_by: String::new(),
            source: 0,
            parent_session: String::new(),
            disposition: 0,
        }
    }

    #[test]
    fn relative_time_formats_by_band() {
        let now = chrono::Utc::now();
        assert_eq!(last_active(&active_ago(now, 59), now), "now");
        assert_eq!(last_active(&active_ago(now, 61 * 60), now), "1h");
        assert_eq!(last_active(&active_ago(now, 25 * 3_600), now), "1d");
        assert_eq!(last_active(&active_ago(now, 8 * 86_400), now), "1w");
    }

    #[test]
    fn wrap_input_breaks_at_the_width() {
        let chars: Vec<char> = "abcdef".chars().collect();
        let (rows, cursor_row, cursor_col) = wrap_input(&chars, chars.len(), 3);
        assert_eq!(rows, vec!["abc".to_owned(), "def".to_owned()]);
        assert_eq!(
            (cursor_row, cursor_col),
            (2, 0),
            "a cursor filling a row exactly wraps to a fresh row after it"
        );
    }

    #[test]
    fn wrap_input_starts_a_new_row_on_an_embedded_newline() {
        let chars: Vec<char> = "ab\ncd".chars().collect();
        let (rows, ..) = wrap_input(&chars, chars.len(), 10);
        assert_eq!(rows, vec!["ab".to_owned(), "cd".to_owned()]);
    }

    #[test]
    fn a_trailing_newline_adds_an_empty_row_for_the_cursor() {
        let chars: Vec<char> = "> ab\n".chars().collect();
        let (rows, cursor_row, cursor_col) = wrap_input(&chars, chars.len(), 10);
        assert_eq!(rows, vec!["> ab".to_owned(), String::new()]);
        assert_eq!((cursor_row, cursor_col), (1, 0));
    }

    #[test]
    fn a_picker_row_prefers_the_title_over_the_preview() {
        let session = session("s-01", "Palette bikeshed", "what color for the accent?");
        assert_eq!(label(&session, 40), "Palette bikeshed");
    }

    #[test]
    fn a_picker_row_falls_back_to_the_preview_without_a_title() {
        let session = session("s-01", "", "what color for the accent?");
        assert_eq!(label(&session, 40), "what color for the accent?");
    }

    #[test]
    fn a_session_with_no_title_or_preview_falls_back_to_a_dim_empty_marker() {
        let session = session("s-01", "", "");
        assert_eq!(label(&session, 40), "(empty)");
    }

    #[test]
    fn an_empty_session_with_a_project_names_it_alongside_the_marker() {
        let mut session = session("s-01", "", "");
        session.project = "scratch".to_owned();
        assert_eq!(label(&session, 40), "(empty) · scratch");
    }

    #[test]
    fn disposition_tag_is_none_for_a_root_and_a_char_per_disposition_for_a_branch() {
        use arc_proto::v1::branch_marked::Disposition;

        let root = session("s-01", "root", "");
        assert_eq!(disposition_tag(&root), None, "a root has nothing to mark");

        let mut branch = session("s-02", "branch", "");
        branch.parent_session = "s-01".to_owned();
        assert_eq!(disposition_tag(&branch), Some('?'), "unmarked is scratch");

        branch.disposition = Disposition::Real as i32;
        assert_eq!(disposition_tag(&branch), Some('+'));

        branch.disposition = Disposition::Abandoned as i32;
        assert_eq!(disposition_tag(&branch), Some('x'));
    }

    #[test]
    fn the_picker_indents_a_branch_under_its_parent_and_shows_its_tag() {
        use crate::app::{App, NetEvent};
        use arc_proto::v1::branch_marked::Disposition;

        let mut app = App::new();
        let mut root = session("s-root", "root conversation", "hi");
        root.source = arc_proto::v1::Source::User as i32;
        let mut fork = session("s-fork", "fixing the parser", "hi");
        fork.source = arc_proto::v1::Source::User as i32;
        fork.parent_session = "s-root".to_owned();
        fork.disposition = Disposition::Real as i32;
        app.on_net(NetEvent::Sessions(vec![root, fork]));
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('s')));

        let text = plain_text(&rendered(&mut app));
        let branch_line = text
            .lines()
            .find(|line| line.contains("fixing the parser"))
            .expect("the branch row renders");
        assert!(
            branch_line.contains("○ fixing the parser"),
            "every node gets a bullet, got: {branch_line:?}"
        );
        assert!(
            branch_line.contains("\\ of s-root"),
            "lineage renders as an annotation, got: {branch_line:?}"
        );
        assert!(
            branch_line.contains("[+]"),
            "the real disposition sits at the end of the row, got: {branch_line:?}"
        );
        let root_line = text
            .lines()
            .find(|line| line.contains("root conversation"))
            .expect("the root row renders");
        assert!(
            !root_line.contains('\\'),
            "a root carries no lineage annotation, got: {root_line:?}"
        );
        assert!(
            !root_line.contains('['),
            "a root carries no disposition tag, got: {root_line:?}"
        );
    }

    #[test]
    fn the_picker_tree_mode_renders_connectors_instead_of_lineage() {
        use crate::app::{App, NetEvent};

        let mut app = App::new();
        let mut root = session("s-root", "root conversation", "hi");
        root.source = arc_proto::v1::Source::User as i32;
        let mut fork = session("s-fork", "fixing the parser", "hi");
        fork.source = arc_proto::v1::Source::User as i32;
        fork.parent_session = "s-root".to_owned();
        app.on_net(NetEvent::Sessions(vec![root, fork]));
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('s')));
        app.on_key(key(KeyCode::Tab));

        let text = plain_text(&rendered(&mut app));
        let branch_line = text
            .lines()
            .find(|line| line.contains("fixing the parser"))
            .expect("the branch row renders");
        assert!(
            branch_line.contains("└─ ○ fixing the parser"),
            "the elbow hands off straight to the child bullet, got: {branch_line:?}"
        );
        assert!(
            !branch_line.contains("\\ of"),
            "a tree has no lineage annotation, got: {branch_line:?}"
        );
        let root_line = text
            .lines()
            .find(|line| line.contains("root conversation"))
            .expect("the root row renders");
        assert!(
            !root_line.contains("└─") && !root_line.contains("├─") && !root_line.contains('│'),
            "a root carries no connectors, got: {root_line:?}"
        );
    }

    #[test]
    fn the_picker_marks_the_active_session_in_both_modes() {
        use crate::app::{App, NetEvent};

        let mut app = App::new();
        let mut one = session("s-one", "the active one", "hi");
        one.source = arc_proto::v1::Source::User as i32;
        let mut two = session("s-two", "the other", "hi");
        two.source = arc_proto::v1::Source::User as i32;
        app.on_net(NetEvent::Sessions(vec![one, two]));
        app.session_id = Some("s-two".to_owned());
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('s')));

        let text = plain_text(&rendered(&mut app));
        let active = text
            .lines()
            .find(|line| line.contains("the other"))
            .expect("the active session renders");
        assert!(active.contains("● the other"), "got: {active:?}");
        let inactive = text
            .lines()
            .find(|line| line.contains("the active one"))
            .expect("the other session renders");
        assert!(
            inactive.contains("○ the active one"),
            "an inactive session still shows its hollow bullet, got: {inactive:?}"
        );
        assert!(!inactive.contains('●'), "got: {inactive:?}");

        app.on_key(key(KeyCode::Tab));
        let text = plain_text(&rendered(&mut app));
        assert!(
            text.lines()
                .find(|line| line.contains("the other"))
                .expect("tree mode still lists it")
                .contains('●'),
            "the marker survives the view switch"
        );
    }

    #[test]
    fn the_picker_tree_connectors_align_under_the_parent_bullets() {
        use crate::app::{App, NetEvent};
        use arc_proto::v1::branch_marked::Disposition;

        let mut app = App::new();
        let mut root = session("s-root", "root", "hi");
        root.source = arc_proto::v1::Source::User as i32;
        let mut first = session("s-first", "first branch", "hi");
        first.source = arc_proto::v1::Source::User as i32;
        first.parent_session = "s-root".to_owned();
        first.disposition = Disposition::Real as i32;
        let mut deep = session("s-deep", "grandchild", "hi");
        deep.source = arc_proto::v1::Source::User as i32;
        deep.parent_session = "s-first".to_owned();
        let mut last = session("s-last", "last branch", "hi");
        last.source = arc_proto::v1::Source::User as i32;
        last.parent_session = "s-root".to_owned();
        app.on_net(NetEvent::Sessions(vec![root, first, deep, last]));
        app.session_id = Some("s-first".to_owned());
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('s')));
        app.on_key(key(KeyCode::Tab));

        let text = plain_text(&rendered(&mut app));
        let column = |line: &str, needle: char| {
            line.chars()
                .position(|c| c == needle)
                .expect("the glyph renders on the row")
        };
        let root_line = text.lines().find(|l| l.contains(" root")).unwrap();
        let first_line = text.lines().find(|l| l.contains("first branch")).unwrap();
        let deep_line = text.lines().find(|l| l.contains("grandchild")).unwrap();
        let last_line = text.lines().find(|l| l.contains("last branch")).unwrap();
        let root_bullet = column(root_line, '○');

        assert_eq!(
            column(first_line, '├'),
            root_bullet,
            "the first child's elbow starts under the root bullet: {first_line:?}"
        );
        assert_eq!(column(first_line, '●'), root_bullet + 3, "active child");
        assert_eq!(
            column(deep_line, '│'),
            root_bullet,
            "the continuation bar holds the root's column: {deep_line:?}"
        );
        assert_eq!(
            column(deep_line, '○'),
            root_bullet + 6,
            "each depth steps the bullet one connector cell: {deep_line:?}"
        );
        assert_eq!(
            column(last_line, '└'),
            root_bullet,
            "the last child's elbow starts under the root bullet: {last_line:?}"
        );
        assert_eq!(column(last_line, '○'), root_bullet + 3, "last child");
        assert!(
            first_line.contains("[+]"),
            "the marked branch's tag reads at the end: {first_line:?}"
        );
    }

    #[test]
    fn the_time_column_ignores_whether_a_row_has_a_disposition_tag() {
        use crate::app::{App, NetEvent};
        use arc_proto::v1::branch_marked::Disposition;

        let mut app = App::new();
        let mut root = session("s-root", "conversation", "hi");
        root.source = arc_proto::v1::Source::User as i32;
        root.last_at = Some(prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        });
        let mut fork = session("s-fork", "branch", "hi");
        fork.source = arc_proto::v1::Source::User as i32;
        fork.parent_session = "s-root".to_owned();
        fork.disposition = Disposition::Unspecified as i32;
        fork.last_at = Some(prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp(),
            nanos: 0,
        });
        app.on_net(NetEvent::Sessions(vec![root, fork]));
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('s')));

        let text = plain_text(&rendered(&mut app));
        let root_line = text
            .lines()
            .find(|l| l.contains("conversation") && l.contains("now"))
            .unwrap();
        let fork_line = text
            .lines()
            .find(|l| l.contains("branch") && l.contains("now"))
            .unwrap();
        assert!(
            root_line.contains(" now") && fork_line.contains(" now"),
            "both rows show the same time band for the alignment check"
        );
        assert_eq!(
            fork_line.find("now"),
            root_line.find("now"),
            "a pending tag reserves the same column the untagged root gets: {root_line:?} / {fork_line:?}"
        );
        assert!(
            fork_line.contains("now  [?]"),
            "the tag still reads at the fixed column: {fork_line:?}"
        );
    }

    #[test]
    fn the_strip_label_shows_the_step_count_and_idle_seconds() {
        use crate::app::{App, NetEvent};

        let mut app = App::new();
        let mut job = job(SessionRole::Executor, "arc", "Fix the flaky test");
        job.tool_steps = 12;
        job.idle_seconds = 6;
        app.on_net(NetEvent::JobChanged(job.clone()));

        assert_eq!(
            strip_label(&app, &job),
            " 1 job · Fix the flaky test running · 12 tok · 5s · step 12 - 6s ago"
        );
    }

    #[test]
    fn a_strip_with_a_last_call_shows_it_instead_of_the_step_count() {
        use crate::app::{App, NetEvent};

        let mut app = App::new();
        let mut job = job(SessionRole::Executor, "arc", "");
        job.tool_steps = 12;
        job.idle_seconds = 6;
        job.last_call = "bash cargo test".to_owned();
        app.on_net(NetEvent::JobChanged(job.clone()));

        assert_eq!(
            strip_label(&app, &job),
            " 1 job · executor/arc running · 12 tok · 5s · bash cargo test - 6s ago"
        );
    }

    #[test]
    fn a_strip_step_of_zero_reads_as_thinking() {
        use crate::app::{App, NetEvent};

        let mut app = App::new();
        let mut job = job(SessionRole::Executor, "arc", "");
        job.tool_steps = 0;
        job.idle_seconds = 3;
        app.on_net(NetEvent::JobChanged(job.clone()));

        assert_eq!(
            strip_label(&app, &job),
            " 1 job · executor/arc running · 12 tok · 5s · step 0 - 3s ago"
        );
    }

    fn job(role: SessionRole, project: &str, title: &str) -> JobInfo {
        JobInfo {
            session_id: "s-01".to_owned(),
            role: role as i32,
            project: project.to_owned(),
            state: job_info::State::Running as i32,
            spent_tokens: 12,
            budget_tokens: 0,
            elapsed_seconds: 5,
            budget_seconds: 0,
            title: title.to_owned(),
            tool_steps: 0,
            idle_seconds: 0,
            parent_session: String::new(),
            queued_steers: 0,
            last_call: String::new(),
        }
    }

    #[test]
    fn a_jobs_row_shows_the_role_and_project_without_a_title() {
        let job = job(SessionRole::Executor, "arc", "");
        assert_eq!(job_label(&job), "running executor/arc 12/- tok 5s");
    }

    #[test]
    fn a_jobs_row_shows_the_title_in_place_of_role_and_project() {
        let job = job(SessionRole::Executor, "arc", "Fix the failing test");
        assert_eq!(job_label(&job), "running Fix the failing test 12/- tok 5s");
    }

    #[test]
    fn a_jobs_row_compacts_large_token_counts_like_the_strip() {
        let mut with_budget = job(SessionRole::Executor, "arc", "");
        with_budget.spent_tokens = 441_266;
        with_budget.budget_tokens = 500_000;
        assert_eq!(
            job_label(&with_budget),
            "running executor/arc 441.3k/500.0k tok 5s"
        );
    }

    #[test]
    fn a_jobs_row_appends_the_queued_count_when_nonzero() {
        let mut with_queue = job(SessionRole::Executor, "arc", "");
        with_queue.queued_steers = 2;
        assert_eq!(
            job_label(&with_queue),
            "running executor/arc 12/- tok 5s · 2 queued"
        );
    }
}
