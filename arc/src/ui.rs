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

    let masthead_height = if app.session_id.is_none()
        && app.transcript.is_empty()
        && transcript.height >= MASTHEAD_FLOOR
    {
        MASTHEAD
    } else {
        4
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
        Overlay::Models(models) => draw_models(frame, frame.area(), models),
        Overlay::Thinking(thinking) => draw_thinking(frame, frame.area(), thinking),
        Overlay::Help { .. } => draw_help(frame, app, frame.area()),
        Overlay::SessionStatus => draw_status(frame, frame.area(), app),
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
        if app.details_held_focus != Some(boundary) {
            bring_into_view(app, &bounds, boundary, lines.len(), height, max_back);
        }
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
                let suffix = format!(" · {state}");
                let header = if *open {
                    format!("{fold} {name}")
                } else {
                    let summary = crate::app::tool_summary(args).replace(['\n', '\r'], " ");
                    format!("{fold} {name} {summary}")
                };
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
                if *open {
                    push_tool_input(&mut out, name, args, width);
                    let completion = outcome.map(|state| tool_completion(name, state, content));
                    let output = completion
                        .as_ref()
                        .map_or(content.as_str(), |(_, text)| *text);
                    if outcome.is_some() || !output.is_empty() {
                        out.push(Line::styled("Output", theme::DIM));
                        push_literal(&mut out, output, width);
                    }
                    if let Some((status, _)) = completion {
                        out.push(Line::styled(format!("Status: {status}"), theme::DIM));
                    }
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

fn push_tool_input(out: &mut Vec<Line<'static>>, name: &str, args: &str, width: usize) {
    match serde_json::from_str::<serde_json::Value>(args) {
        Ok(serde_json::Value::Object(mut fields)) => {
            if name == "bash" {
                if let Some(serde_json::Value::String(command)) = fields.get("command") {
                    out.push(Line::styled("Command", theme::DIM));
                    push_literal(out, command, width);
                    fields.remove("command");
                }
            }
            if !fields.is_empty() {
                out.push(Line::styled("Input", theme::DIM));
                for (key, value) in fields {
                    push_literal(out, &format!("{key}:"), width);
                    push_input_value(out, &value, width);
                }
            }
        }
        Ok(value) => {
            out.push(Line::styled("Input", theme::DIM));
            push_input_value(out, &value, width);
        }
        Err(_) => {
            out.push(Line::styled("Input", theme::DIM));
            push_literal(out, args, width);
        }
    }
}

fn push_input_value(out: &mut Vec<Line<'static>>, value: &serde_json::Value, width: usize) {
    if let Some(text) = value.as_str() {
        push_literal(out, text, width);
    } else {
        push_literal(
            out,
            &serde_json::to_string_pretty(value).unwrap_or_default(),
            width,
        );
    }
}

// Bash errors prefix retained output with their exit status.
fn tool_completion<'a>(name: &str, outcome: &str, content: &'a str) -> (String, &'a str) {
    if name == "bash" {
        if outcome == "ok" {
            return ("exit 0".to_owned(), content);
        }
        if outcome == "error" {
            let (first, rest) = content.split_once('\n').unwrap_or((content, ""));
            if first
                .strip_prefix("exit ")
                .is_some_and(|code| code == "signal" || code.parse::<i32>().is_ok())
            {
                return (first.to_owned(), rest);
            }
        }
    }
    (outcome.to_owned(), content)
}

fn push_literal(out: &mut Vec<Line<'static>>, text: &str, width: usize) {
    let width = width.max(2);
    for source in text.split('\n') {
        let mut row = String::new();
        let mut column = 0;
        let mut source_column = 0;
        for c in source.chars() {
            let (c, count) = if c == '\t' {
                (' ', 8 - source_column % 8)
            } else {
                (c, 1)
            };
            let cell_width = textwrap::core::display_width(c.encode_utf8(&mut [0; 4]));
            source_column += cell_width * count;
            for _ in 0..count {
                if column + cell_width > width {
                    out.push(Line::styled(std::mem::take(&mut row), theme::PLAIN));
                    column = 0;
                }
                row.push(c);
                column += cell_width;
            }
        }
        out.push(Line::styled(row, theme::PLAIN));
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
        .unwrap_or("New session");
    let model = match session {
        Some(session) if session.model.is_empty() => "model not recorded".to_owned(),
        Some(session) => format!("model: {}", session.model),
        None => "loading model…".to_owned(),
    };
    let room = usize::from(area.width);
    let project = elide(app.current_project().unwrap_or("session"), room / 3);
    let title = elide(title, room.saturating_sub(project.chars().count() + 3));
    let mut heading = vec![Span::styled(project, theme::ACCENT)];
    if !app.herdr_enabled {
        heading.push(Span::styled(" · ", theme::DIM));
        heading.push(Span::styled(title, theme::STRONG));
    }
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(heading),
            Line::styled(elide(&model, room), theme::DIM),
            status_line(app, room),
            Line::styled("─".repeat(room), theme::DIM),
        ]),
        area,
    );
}

fn current_status(app: &App) -> Option<&arc_proto::v1::SessionStatus> {
    app.session_id
        .as_ref()
        .and_then(|id| app.session_status.get(id))
}

fn allowance_label(window: &arc_proto::v1::AllowanceWindow) -> String {
    match window.window_seconds {
        Some(604_800) => "week".to_owned(),
        Some(seconds) if seconds >= 3600 && seconds % 3600 == 0 => format!("{}h", seconds / 3600),
        Some(seconds) => format!("{}m", seconds / 60),
        None => "window".to_owned(),
    }
}

fn allowance_stale(status: &arc_proto::v1::SessionStatus) -> bool {
    status.allowance_stale
        || chrono::Utc::now()
            .timestamp()
            .saturating_sub(status.allowance_observed_at)
            > 120
}

fn status_line(app: &App, room: usize) -> Line<'static> {
    let status = current_status(app);
    let context = status.and_then(|s| s.context.as_ref());
    let text = match context {
        Some(context) => format!(
            "ctx {}/{}",
            status_tokens(context.input_tokens),
            context
                .context_window
                .map_or_else(|| "?".to_owned(), status_tokens)
        ),
        None => "ctx unmeasured".to_owned(),
    };
    let near_limit = context.is_some_and(|c| {
        c.context_window
            .is_some_and(|w| w > 0 && u64::from(c.input_tokens) * 10 >= u64::from(w) * 9)
    });
    let mut spans = vec![Span::styled(
        text,
        if near_limit { theme::ERROR } else { theme::DIM },
    )];
    if let Some(thinking) = status
        .map(|s| s.effective_thinking.as_str())
        .filter(|thinking| !thinking.is_empty())
    {
        spans.push(Span::styled(format!(" · thinking {thinking}"), theme::DIM));
    }
    let codex = status.is_some_and(|s| s.codex)
        || app.session_id.as_ref().is_some_and(|id| {
            app.sessions
                .iter()
                .any(|s| &s.id == id && s.provider == "codex")
        });
    if codex {
        match status.filter(|s| !s.allowance.is_empty()) {
            Some(status) => {
                let stale = allowance_stale(status);
                if stale {
                    spans.push(Span::styled(" · stale", theme::DIM));
                }
                for window in &status.allowance {
                    spans.push(Span::styled(
                        format!(
                            " · {} {}% left",
                            allowance_label(window),
                            window.remaining_percent
                        ),
                        if !stale && window.remaining_percent <= 10 {
                            theme::ERROR
                        } else {
                            theme::DIM
                        },
                    ));
                }
            }
            None => spans.push(Span::styled(" · Codex usage unknown", theme::DIM)),
        }
    }
    let mut remaining = room;
    for span in &mut spans {
        if remaining == 0 {
            span.content = "".into();
        } else if span.width() > remaining {
            span.content = elide(&span.content, remaining).into();
        }
        remaining = remaining.saturating_sub(span.width());
    }
    Line::from(spans)
}

fn draw_status(frame: &mut Frame, full: Rect, app: &App) {
    let mut lines = vec![status_line(app, usize::MAX), Line::default()];
    if let Some(status) = current_status(app) {
        match &status.context {
            Some(context) => {
                let age = chrono::Utc::now()
                    .timestamp()
                    .saturating_sub(status.context_observed_at)
                    .max(0);
                lines.push(Line::from(format!(
                    "Context: last reported prompt, measured {age}s ago"
                )));
                lines.push(Line::from(format!(
                    "Compaction threshold: {}",
                    context
                        .compact_at
                        .map_or_else(|| "not configured".to_owned(), status_tokens)
                )));
            }
            None => lines.push(Line::from(
                "Context: no measurement since creation or compaction",
            )),
        }
        if status.codex {
            lines.push(Line::default());
            if status.allowance_observed_at > 0 {
                let age = chrono::Utc::now()
                    .timestamp()
                    .saturating_sub(status.allowance_observed_at)
                    .max(0);
                lines.push(Line::from(format!(
                    "Codex account allowance: observed {age}s ago"
                )));
            } else {
                lines.push(Line::from("Codex account allowance: unavailable"));
            }
            for window in &status.allowance {
                let reset = window
                    .resets_at
                    .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
                    .map_or_else(
                        || "unknown".to_owned(),
                        |ts| {
                            ts.with_timezone(&chrono::Local)
                                .format("%a %d %b %H:%M %Z")
                                .to_string()
                        },
                    );
                lines.push(Line::from(format!(
                    "{}: {}% left · resets {reset}",
                    allowance_label(window),
                    window.remaining_percent
                )));
            }
            if allowance_stale(status) && !status.allowance.is_empty() {
                lines.push(Line::from(
                    "Stale: the last allowance refresh failed or is overdue",
                ));
            }
        }
    } else {
        lines.push(Line::from("Waiting for session status"));
    }
    lines.push(Line::default());
    lines.push(Line::styled(
        "Context is not turn spend. Allowance is shared across sessions.",
        theme::DIM,
    ));
    lines.push(Line::styled("Esc close", theme::DIM));
    let area = popup(
        frame,
        full,
        76,
        u16::try_from(lines.len()).unwrap_or(u16::MAX),
        "status",
    );
    frame.render_widget(
        Paragraph::new(lines).wrap(ratatui::widgets::Wrap { trim: false }),
        area,
    );
}

fn status_tokens(tokens: u32) -> String {
    format_tokens(u64::from(tokens))
        .replace(".0k", "k")
        .replace(".0M", "M")
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
    left.push(app.current_project().unwrap_or("session").to_owned());
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
            let stop = match app.stop_escape_count() {
                Some(1) => " · Esc · stop".to_owned(),
                Some(count) => format!(" · Esc ×{count} · stop"),
                None => String::new(),
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
                    Mode::Normal => " Ctrl-P sessions · ? help",
                    Mode::Visual => " j/k select · f fork · Esc back",
                    Mode::Insert => " Esc normal · Ctrl-P sessions",
                    Mode::Cmd => " :help",
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
    let (cursor_row, cursor_col) = match cursor {
        Some(pos) => pos,
        None if col == width => (rows.len(), 0),
        None => (rows.len() - 1, col),
    };
    (rows, cursor_row, cursor_col)
}

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
    let needed = rows
        .len()
        .max(cursor_row + 1)
        .saturating_add(app.pending_attachments().len());
    let rows = u16::try_from(needed).unwrap_or(u16::MAX);
    rows.min(INPUT_ROWS_CAP).min(frame.height / 3).max(1)
}

fn draw_input(frame: &mut Frame, area: Rect, app: &App) {
    let attachment_rows = u16::try_from(app.pending_attachments().len())
        .unwrap_or(u16::MAX)
        .min(area.height.saturating_sub(1));
    for (row, attachment) in (0..attachment_rows).zip(app.pending_attachments()) {
        frame.render_widget(
            Line::styled(
                elide(
                    &format!("[image: {}]", attachment.name),
                    area.width as usize,
                ),
                theme::DIM,
            ),
            Rect::new(area.x, area.y + row, area.width, 1),
        );
    }
    let area = Rect {
        y: area.y + attachment_rows,
        height: area.height.saturating_sub(attachment_rows),
        ..area
    };
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
        "all sessions"
    } else {
        app.current_project().unwrap_or("all sessions")
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
                "/ filter · t tree · a all · x abandoned · Enter open",
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

fn draw_thinking(frame: &mut Frame, full: Rect, thinking: &crate::app::ThinkingPicker) {
    let mut lines = Vec::new();
    if !thinking.current.is_empty() {
        lines.push(Line::styled(
            format!("Current: {}", thinking.current),
            theme::DIM,
        ));
    }
    if !thinking.loaded {
        lines.push(Line::styled("Loading thinking levels…", theme::DIM));
    } else if thinking.levels.is_empty() {
        lines.push(Line::styled(
            "No supported thinking levels for this session",
            theme::DIM,
        ));
    } else {
        for (index, level) in thinking.levels.iter().enumerate() {
            let selected = index == thinking.selected;
            let current = level == &thinking.current;
            lines.push(Line::styled(
                format!(
                    " {}{} {level}",
                    if current { "*" } else { " " },
                    if selected { ">" } else { " " }
                ),
                if selected {
                    theme::ACCENT
                } else {
                    theme::PLAIN
                },
            ));
        }
    }
    lines.push(Line::styled(
        "Enter change · Esc close · applies next user turn",
        theme::DIM,
    ));
    let area = popup(
        frame,
        full,
        64,
        u16::try_from(lines.len()).unwrap_or(u16::MAX),
        "thinking",
    );
    frame.render_widget(Paragraph::new(lines), area);
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
        if review.all { "memory" } else { "review" },
    );
    let width = area.width;

    let mut lines = Vec::new();
    if review.items.is_empty() {
        let word = if review.loaded {
            if review.all {
                "no memories"
            } else {
                "nothing to review"
            }
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
    } else if review.all {
        Line::styled("   dd delete · r refresh · q close", theme::DIM)
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

fn draw_models(frame: &mut Frame, full: Rect, models: &crate::app::Models) {
    let rows = models.items.len().max(1);
    let title = if models.default {
        "role defaults".to_owned()
    } else {
        "session model".to_owned()
    };
    let hint = if models.default {
        "Enter changes role default · * default · q close".to_owned()
    } else {
        let pinned = models
            .recorded_model
            .as_deref()
            .filter(|model| !model.is_empty())
            .unwrap_or("not recorded");
        format!("Current: {pinned} · Enter new/fork · * role default · q close")
    };
    let area = menu_area(frame, full, 78, rows, &title, &hint);

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
            "Esc · stop        normal mode; Esc ×2 · stop in insert",
            "                  close overlays/search first; pending d/g adds one Esc",
            "ctrl-p            find a session; / filters, enter opens the match",
            "ctrl-o            toggle session details (off by default)",
            "v then j/k        point at messages, tools, or thoughts",
            ":compact          compact the current idle session",
            ":attach <path>    attach a local image to the next message",
            ":attach clear     discard pending images",
            "M                 pick a model for a new session or fork",
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
            "ctrl-o            toggle all tools / thoughts",
            "? J Q             help / jobs / review queue popups",
            "M                 pick a model for a new session or fork; * marks the role default",
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
            ":memory           browse all active memories",
            ":jobs             open the jobs pane",
            ":model            open the session model picker",
            ":model-default    change a role's default model",
            ":status           context measurement and Codex allowance",
            ":attach <path>    attach PNG, JPEG, or WebP to the next message",
            ":attach clear     discard pending images",
            ":fork             branch at the visual selection",
            ":help             this popup (j k scroll it)",
        ],
    ),
    (
        "picker keys",
        &[
            "j k               move selection",
            "t                 toggle flat recency / tree view",
            "/                 filter by title/preview",
            "space a           toggle all projects / current project",
            "x                 toggle showing abandoned branches",
            "m                 mark the selected branch real",
            "X                 mark the selected branch abandoned",
            "enter             open the selected session",
            "q esc             close (esc also clears an active filter)",
        ],
    ),
    (
        "memory keys",
        &[
            "j k               move selection",
            "dd                delete the selected record",
            "r                 refresh the list",
            "q esc             close",
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

    use super::{draw, job_label};
    use crate::app::{App, Block, Mode, Models, Search};

    #[test]
    fn the_default_picker_lists_each_roles_choices_and_marks_the_defaults() {
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
                choice(SessionRole::Chat, "astra", "gpt-6-astra", true),
                choice(SessionRole::Executor, "sol", "gpt-5.6-sol", true),
                choice(SessionRole::Executor, "glm-flash", "glm-5.3-flash", false),
            ],
            selected: 2,
            loaded: true,
            default: true,
            role: SessionRole::Chat,
            recorded_model: None,
        });

        let text = plain_text(&rendered(&mut app));
        println!("{text}");

        assert!(
            text.contains(" role defaults "),
            "the popup is titled role defaults:\n{text}"
        );
        assert!(
            text.contains("   assistant *astra      codex gpt-6-astra medium"),
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

    #[test]
    fn thinking_picker_renders_supported_levels_and_turn_hint() {
        use crate::app::{Overlay, ThinkingPicker};

        let mut app = App::new();
        app.overlay = Overlay::Thinking(ThinkingPicker {
            session_id: "session-1".to_owned(),
            preset: None,
            levels: vec!["low".to_owned(), "medium".to_owned(), "high".to_owned()],
            selected: 1,
            current: "low".to_owned(),
            loaded: true,
        });
        let text = plain_text(&rendered(&mut app));
        println!("{text}");
        assert!(text.contains("+ thinking "), "{text}");
        assert!(text.contains("Current: low"), "{text}");
        assert!(text.contains("  > medium"), "{text}");
        assert!(text.contains(" *  low"), "{text}");
        assert!(
            text.contains("Enter change · Esc close · applies next user turn"),
            "{text}"
        );
    }

    #[test]
    fn session_model_picker_shows_the_pin_without_marking_it_as_default() {
        let mut app = App::new();
        app.overlay = Overlay::Models(Models {
            items: vec![
                ModelChoice {
                    role: SessionRole::Chat as i32,
                    name: "fast".to_owned(),
                    provider: "codex".to_owned(),
                    model: "model-fast".to_owned(),
                    thinking: "medium".to_owned(),
                    selected: true,
                },
                ModelChoice {
                    role: SessionRole::Chat as i32,
                    name: "deep".to_owned(),
                    provider: "codex".to_owned(),
                    model: "model-deep".to_owned(),
                    thinking: "high".to_owned(),
                    selected: false,
                },
            ],
            selected: 1,
            loaded: true,
            default: false,
            role: SessionRole::Chat,
            recorded_model: Some("model-deep".to_owned()),
        });
        let text = plain_text(&rendered(&mut app));
        println!("SESSION MODEL PICKER\n{text}");
        assert!(text.contains("session model"), "{text}");
        assert!(text.contains("Current: model-deep"), "{text}");
        assert!(text.contains("*fast"), "{text}");
        assert!(text.contains(" deep"), "{text}");
        assert!(!text.contains("*deep"), "{text}");
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
    fn working_header_uses_the_project_and_recorded_model_at_both_widths() {
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
            assert!(text.contains("arc · Repair"), "{text}");
            assert!(text.contains("model: pinned-model"), "{text}");
            assert!(buffer[(13, 0)].modifier.contains(Modifier::BOLD));
            assert_eq!(buffer[(2, 0)].fg, theme::ACCENT.fg.unwrap());
            assert_eq!(buffer[(2, 1)].fg, theme::DIM.fg.unwrap());
            assert!(
                text.lines()
                    .nth(3)
                    .unwrap()
                    .contains(&"─".repeat(usize::from(width - 4)))
            );
            println!("HEADER {width}\n{text}");
            app.herdr_enabled = true;
            let text = plain_text(&rendered_at(&mut app, width, 12));
            assert_eq!(text.lines().next().unwrap().trim(), "arc");
            assert!(text.contains("model: pinned-model"), "{text}");
            assert!(!text.contains("Repair"), "{text}");
            println!("HERDR HEADER {width}\n{text}");
            app.herdr_enabled = false;
        }
        info.model.clear();
        app.on_net(NetEvent::Sessions(vec![info]));
        assert!(plain_text(&rendered(&mut app)).contains("model not recorded"));
        app.on_net(NetEvent::Sessions(vec![]));
        assert!(plain_text(&rendered(&mut app)).contains("loading model…"));
    }

    #[test]
    fn session_status_renders_below_the_model_and_details_show_resets() {
        let mut app = conversation();
        let mut info = session("status", "Codex context and usage status", "");
        info.role = SessionRole::Executor as i32;
        info.project = "arc".to_owned();
        info.model = "gpt-5.5".to_owned();
        info.provider = "codex".to_owned();
        info.source = arc_proto::v1::Source::User as i32;
        app.on_net(crate::app::NetEvent::Sessions(vec![info]));
        app.session_id = Some("status".to_owned());
        app.session_status.insert(
            "status".to_owned(),
            arc_proto::v1::SessionStatus {
                session_id: "status".to_owned(),
                codex: true,
                context: Some(arc_proto::v1::ContextMeasured {
                    session_id: "status".to_owned(),
                    input_tokens: 84_000,
                    context_window: Some(272_000),
                    compact_at: Some(217_600),
                }),
                context_observed_at: chrono::Utc::now().timestamp(),
                allowance_observed_at: chrono::Utc::now().timestamp(),
                allowance: vec![
                    arc_proto::v1::AllowanceWindow {
                        remaining_percent: 72,
                        resets_at: Some(1_900_000_000),
                        window_seconds: Some(18_000),
                    },
                    arc_proto::v1::AllowanceWindow {
                        remaining_percent: 41,
                        resets_at: None,
                        window_seconds: Some(604_800),
                    },
                ],
                ..Default::default()
            },
        );
        let buffer = rendered_at(&mut app, 100, 16);
        let text = plain_text(&buffer);
        assert!(
            text.lines().nth(1).unwrap().contains("model: gpt-5.5"),
            "{text}"
        );
        assert!(
            text.lines()
                .nth(2)
                .unwrap()
                .contains("ctx 84k/272k · 5h 72% left · week 41% left"),
            "{text}"
        );
        assert_eq!(buffer[(2, 2)].fg, theme::DIM.fg.unwrap());
        println!("SESSION STATUS\n{text}");
        app.mode = Mode::Cmd;
        app.cmd = "status".to_owned();
        app.on_key(key(KeyCode::Enter));
        assert_eq!(app.overlay, Overlay::SessionStatus);
        let text = plain_text(&rendered_at(&mut app, 100, 24));
        assert!(text.contains("last reported prompt"), "{text}");
        assert!(text.contains("Compaction threshold: 217.6k"), "{text}");
        assert!(text.contains("week: 41% left · resets unknown"), "{text}");
        println!("STATUS DETAILS\n{text}");
        app.on_key(key(KeyCode::Esc));
        assert_eq!(app.overlay, Overlay::None);
        let status = app.session_status.get_mut("status").unwrap();
        status.context.as_mut().unwrap().input_tokens = 260_000;
        status.allowance[0].remaining_percent = 5;
        let buffer = rendered_at(&mut app, 100, 16);
        assert_eq!(buffer[(2, 2)].fg, theme::ERROR.fg.unwrap());
        app.session_status
            .get_mut("status")
            .unwrap()
            .allowance_stale = true;
        assert!(plain_text(&rendered_at(&mut app, 100, 16)).contains("· stale"));
        let text = plain_text(&rendered_at(&mut app, 40, 12));
        assert!(text.contains("ctx 260k/272k"), "{text}");
        assert_eq!(
            rendered_at(&mut app, 40, 12)[(2, 2)].fg,
            theme::ERROR.fg.unwrap()
        );
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
        app.on_key(key(KeyCode::Char('a')));
        if let Overlay::Picker(picker) = &mut app.overlay {
            picker.selected = 1;
        }
        for width in [140, 40] {
            let buffer = rendered_at(&mut app, width, 16);
            let text = plain_text(&buffer);
            if width == 140 {
                assert!(text.contains("sessions · all sessions · recent"), "{text}");
            }
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
        app.on_key(ctrl('o'));
        let text = plain_text(&rendered(&mut app));
        assert!(text.contains("output line 80"));
        assert_eq!(app.scroll_back, 0);
        app.on_key(key(KeyCode::Char('g')));
        app.on_key(key(KeyCode::Char('g')));
        let text = plain_text(&rendered(&mut app));
        assert!(text.contains("output line 1"));
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
    fn toggling_details_preserves_the_top_visible_block() {
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
        app.on_key(ctrl('o'));
        rendered(&mut app);
        assert_eq!(app.viewport_anchor, anchor);
        app.on_key(ctrl('o'));
        assert_eq!(
            app.transcript
                .iter()
                .filter(|entry| matches!(&entry.block, Block::Tool { open: true, .. }))
                .count(),
            40
        );
    }

    #[test]
    fn collapsing_visible_tool_details_anchors_to_its_summary() {
        let mut app = App::new();
        for n in 0..40 {
            app.push_block(Block::Tool {
                call_id: n.to_string(),
                name: "read".to_owned(),
                args: format!("file-{n}"),
                outcome: Some("ok"),
                content: "detail\n".repeat(12),
                open: false,
            });
        }
        app.on_key(ctrl('o'));
        let mut found = None;
        for back in 200..240 {
            app.scroll_back = back;
            rendered_at(&mut app, 76, 16);
            if let Some((block, offset)) = app.viewport_anchor {
                if offset > 3 {
                    found = Some(block);
                    break;
                }
            }
        }
        let block = found.expect("viewport starts inside tool output");
        app.on_key(ctrl('o'));
        rendered_at(&mut app, 76, 16);
        assert_eq!(app.viewport_anchor, Some((block, 0)));
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

    #[test]
    fn a_pending_picture_renders_as_a_text_placeholder() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("layout.png");
        std::fs::write(&path, b"\x89PNG\r\n\x1a\nbody").unwrap();
        let mut app = App::new();
        app.on_key(key(KeyCode::Esc));
        typed(&mut app, &format!(":attach {}", path.display()));
        app.on_key(key(KeyCode::Enter));

        let text = plain_text(&rendered(&mut app));
        println!("{text}");

        assert!(text.contains("[image: layout.png]"), "{text}");
        assert!(!text.contains(&path.display().to_string()), "{text}");
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
    fn multiline_bash_command_output_and_exit_status_render_separately() {
        use crate::app::NetEvent;
        use arc_proto::v1::ToolOutcome;

        let mut app = App::new();
        let command = "python3 - <<'PY'\nitems = [1, 2]\n\nfor item in items:\n    print(item)\nPY";
        app.on_key(ctrl('o'));
        app.on_net(NetEvent::Accepted {
            session_id: "s1".to_owned(),
        });
        app.on_net(NetEvent::ToolStarted {
            call_id: "t1".to_owned(),
            name: "bash".to_owned(),
            arguments_json: serde_json::json!({"command": command}).to_string(),
        });
        let running = plain_text(&rendered_at(&mut app, 76, 24));
        for line in command.split('\n').filter(|line| !line.is_empty()) {
            assert!(
                running
                    .lines()
                    .any(|row| row.trim_end() == format!("  {line}")),
                "{running}"
            );
        }
        assert!(running.contains("− bash · running"), "{running}");
        assert!(!running.contains("Status:"), "{running}");
        assert!(!running.contains("\\n"), "{running}");
        app.on_net(NetEvent::ToolEnded {
            call_id: "t1".to_owned(),
            outcome: ToolOutcome::Error as i32,
            content: "exit 3\nfirst line\n    indented output\n--- stderr ---\nproblem".to_owned(),
        });
        let text = plain_text(&rendered_at(&mut app, 76, 24));
        assert!(text.contains("      print(item)"), "{text}");
        assert!(text.contains("      indented output"), "{text}");
        assert!(text.contains("--- stderr ---"), "{text}");
        assert!(text.find("Command").unwrap() < text.find("Output").unwrap());
        assert!(text.find("problem").unwrap() < text.find("Status: exit 3").unwrap());
        assert_eq!(text.matches("exit 3").count(), 1, "{text}");
        println!("MULTILINE BASH FRAME\n{text}");
        app.on_key(ctrl('o'));
        let compact = plain_text(&rendered_at(&mut app, 76, 12));
        assert!(compact.contains("+ bash python3"), "{compact}");
        assert!(!compact.contains("Output"), "{compact}");
        assert!(!compact.contains("print(item)"), "{compact}");
        println!("COMPACT BASH FRAME\n{compact}");
    }

    #[test]
    fn tool_inputs_decode_strings_and_keep_other_json_readable() {
        let mut app = App::new();
        app.on_key(ctrl('o'));
        app.push_block(Block::Tool {
            call_id: "t1".to_owned(), name: "write".to_owned(),
            args: serde_json::json!({"path":"example.txt", "content":"first\n    second", "options":{"flag":true}}).to_string(),
            outcome: None, content: String::new(), open: false,
        });
        let text = plain_text(&rendered_at(&mut app, 76, 24));
        assert!(text.contains("      second"), "{text}");
        assert!(text.contains("example.txt"), "{text}");
        assert!(text.contains("\"flag\": true"), "{text}");
        assert!(!text.contains("\\n"), "{text}");
        println!("GENERAL TOOL INPUT FRAME\n{text}");
        for args in [
            "{partial JSON",
            "raw\n    input",
            r#""decoded\n    string""#,
        ] {
            app.set_blocks(vec![Block::Tool {
                call_id: "t1".to_owned(),
                name: "other".to_owned(),
                args: args.to_owned(),
                outcome: None,
                content: String::new(),
                open: true,
            }]);
            let text = plain_text(&rendered_at(&mut app, 76, 16));
            assert!(text.contains("Input"), "{text}");
            assert!(!text.contains("\\n"), "{text}");
        }
    }

    #[test]
    fn literal_tool_text_keeps_spaces_blank_lines_and_long_unicode_lines() {
        let input = "  abc  def\n\n    界界界界界界界界界界界界界界界界界界界界\n\tend";
        let mut app = App::new();
        app.on_key(ctrl('o'));
        app.push_block(Block::Tool {
            call_id: "t1".to_owned(),
            name: "bash".to_owned(),
            args: serde_json::json!({"command":input}).to_string(),
            outcome: None,
            content: String::new(),
            open: false,
        });
        let text = plain_text(&rendered_at(&mut app, 40, 16));
        assert!(text.contains("    abc  def"), "{text}");
        assert_eq!(text.matches('界').count(), 20, "{text}");
        assert!(text.contains("          end"), "{text}");
        let mut lines = Vec::new();
        super::push_literal(&mut lines, input, 36);
        assert_eq!(lines[0].to_string(), "  abc  def");
        assert_eq!(lines[1].to_string(), "");
        assert_eq!(
            lines[2].to_string() + &lines[3].to_string(),
            input.lines().nth(2).unwrap()
        );
        println!("NARROW LITERAL INPUT FRAME\n{text}");
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
            all: false,
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
    fn the_memory_browser_shows_active_record_detail_and_delete_confirmation() {
        let mut app = App::new();
        app.overlay = Overlay::Review(crate::app::Review {
            items: vec![review_entry("mr-older", "the full remembered detail")],
            selected: 0,
            loaded: true,
            pending_delete: false,
            all: true,
        });
        let text = plain_text(&rendered(&mut app));
        println!("MEMORY BROWSER\n{text}");
        assert!(text.contains("memory"), "{text}");
        assert!(text.contains("the full remembered detail"), "{text}");
        assert!(text.contains("dd delete · r refresh · q close"), "{text}");
        assert!(!text.contains("a accept"), "{text}");

        app.review_mut().expect("open").pending_delete = true;
        let armed = plain_text(&rendered(&mut app));
        assert!(armed.contains("dd deletes the selected record"), "{armed}");
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
            all: false,
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
            all: false,
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

    #[test]
    fn picker_frame_names_the_project_scope_and_all_toggle() {
        use crate::app::NetEvent;

        let mut app = App::new();
        let mut bound = session("bound", "fix parser", "hello");
        bound.project = "arc".to_owned();
        bound.role = SessionRole::Chat as i32;
        bound.source = arc_proto::v1::Source::User as i32;
        let mut other = session("unbound", "question", "hello");
        other.source = arc_proto::v1::Source::User as i32;
        app.on_net(NetEvent::Sessions(vec![bound, other]));
        app.session_id = Some("bound".to_owned());
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('s')));
        let scoped = plain_text(&rendered_at(&mut app, 100, 16));
        println!("SCOPED PICKER\n{scoped}");
        assert!(scoped.contains("sessions · arc · recent"), "{scoped}");
        assert!(scoped.contains("fix parser"), "{scoped}");
        assert!(!scoped.contains("question"), "{scoped}");
        app.on_key(key(KeyCode::Char('a')));
        let all = plain_text(&rendered_at(&mut app, 100, 16));
        assert!(all.contains("sessions · all sessions · recent"), "{all}");
        assert!(all.contains("question"), "{all}");
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
        app.on_key(key(KeyCode::Char('t')));

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
        app.on_key(key(KeyCode::Char('t')));

        let text = plain_text(&rendered(&mut app));
        let column = |line: &str, needle: char| {
            line.chars()
                .position(|c| c == needle)
                .expect("the glyph renders on the row")
        };
        let root_line = text.lines().find(|l| l.contains(" root")).unwrap();
        let first_line = text.lines().find(|l| l.contains("● first branch")).unwrap();
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
    fn a_jobs_row_shows_the_title_in_place_of_role_and_project() {
        let job = job(SessionRole::Executor, "arc", "Fix the failing test");
        assert_eq!(job_label(&job), "running Fix the failing test 12/- tok 5s");
    }
}
