use arc_proto::v1::{JobInfo, SessionRole, job_info};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::symbols::border;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block as Panel, Borders, Clear, Paragraph};

use crate::app::{App, Block, Mode, Status, format_tokens};
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
    if let Some(picker) = &app.picker {
        draw_picker(frame, frame.area(), app, picker);
    }
    if let Some(review) = &app.review {
        draw_review(frame, frame.area(), review);
    }
    if let Some(jobs) = &app.jobs {
        draw_jobs(frame, frame.area(), jobs);
    }
    if app.help {
        draw_help(frame, frame.area());
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

    if let Some(boundary) = app.visual_boundary() {
        bring_into_view(app, &bounds, boundary, lines.len(), height, max_back);
    }

    let end = lines.len() - app.scroll_back;
    let start = end.saturating_sub(height);
    let selected = app.visual_range().and_then(|(lo, hi)| {
        let from = bounds.get(lo)?.0;
        let to = bounds.get(hi)?.1;
        Some(from..to)
    });
    // pad the top so a short transcript still sits on the bottom
    let mut visible: Vec<Line> = vec![Line::default(); height.saturating_sub(end - start)];
    visible.extend((start..end).map(|i| {
        if selected.as_ref().is_some_and(|r| r.contains(&i)) {
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
    for (i, block) in app.transcript.iter().enumerate() {
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
                ..
            } => {
                let state = outcome.unwrap_or("...");
                let text = if args.is_empty() {
                    format!("{name} · {state}")
                } else {
                    format!("{name} {args} · {state}")
                };
                out.push(Line::styled(elide(&text, width), theme::DIM));
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
    if let Some(job) = app.strip_job() {
        draw_strip(frame, area, app, job);
        return;
    }
    let mode_word = match app.mode {
        Mode::Insert => "insert",
        Mode::Visual => "visual",
        Mode::Normal | Mode::Cmd => "",
    };
    let door = app.open_door_label();
    let mode = match (mode_word.is_empty(), door) {
        (true, None) => String::new(),
        (true, Some(door)) => format!("-- {door} "),
        (false, None) => format!("-- {mode_word} "),
        (false, Some(door)) => format!("-- {mode_word} {door} "),
    };
    let mut words: Vec<Span> = Vec::new();
    match app.status {
        Status::Streaming => words.push(Span::styled(" streaming", theme::DIM)),
        Status::Disconnected => words.push(Span::styled(" disconnected", theme::DIM)),
        Status::Idle => {
            if let Some(code) = &app.last_error {
                words.push(Span::styled(format!(" {code}"), theme::ERROR));
            }
        }
    }
    if let Some(note) = &app.yank_note {
        words.push(Span::styled(format!(" {note}"), theme::DIM));
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
    let steering = app
        .jobs
        .as_ref()
        .is_some_and(|jobs| jobs.steering.is_some());
    let filtering = app.picker.as_ref().is_some_and(|picker| picker.filtering);
    let (prefix, text): (&str, &str) = if app.mode == Mode::Cmd {
        (":", &app.cmd)
    } else if steering {
        ("s> ", &app.input)
    } else if filtering {
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
    let steering = app
        .jobs
        .as_ref()
        .is_some_and(|jobs| jobs.steering.is_some());
    let filtering = app.picker.as_ref().is_some_and(|picker| picker.filtering);
    let (prefix, prefix_style, text, style, cursor) = if app.mode == Mode::Cmd {
        (":", theme::PLAIN, &app.cmd, theme::PLAIN, app.cmd.len())
    } else if steering {
        ("s> ", theme::ACCENT, &app.input, theme::PLAIN, app.cursor)
    } else if filtering {
        ("/", theme::ACCENT, &app.input, theme::PLAIN, app.cursor)
    } else {
        let style =
            if app.picker.is_some() || app.review.is_some() || app.jobs.is_some() || app.help {
                theme::DIM
            } else {
                theme::PLAIN
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

    if app.mode == Mode::Cmd
        || steering
        || filtering
        || (app.picker.is_none() && app.review.is_none() && app.jobs.is_none() && !app.help)
    {
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

fn draw_picker(frame: &mut Frame, full: Rect, app: &App, picker: &crate::app::Picker) {
    let sessions = app.picker_rows();
    let rows = sessions.len() + 1;
    let area = popup(
        frame,
        full,
        64,
        u16::try_from(rows).unwrap_or(u16::MAX),
        "sessions",
    );
    let width = area.width;

    let now = chrono::Utc::now();
    let mut lines = Vec::new();
    let visible = area.height as usize;
    let start = picker.selected.saturating_sub(visible.saturating_sub(1));
    let room = (width as usize).saturating_sub(TIME_WIDTH + 5);
    for row in start..rows.min(start + visible) {
        let prefix = if row == picker.selected { " > " } else { "   " };
        let spans = match row.checked_sub(1).and_then(|i| sessions.get(i)) {
            None => {
                let style = if row == picker.selected {
                    theme::ACCENT
                } else {
                    theme::DIM
                };
                vec![Span::styled(format!("{prefix}new session"), style)]
            }
            Some(session) => {
                let job = crate::app::is_job_session(session);
                let style = if job {
                    theme::DIM
                } else if row == picker.selected {
                    theme::ACCENT
                } else {
                    theme::DIM
                };
                vec![
                    Span::styled(
                        format!("{prefix}{:<room$}", picker_label(session, room, job)),
                        style,
                    ),
                    Span::styled(
                        format!("  {:>TIME_WIDTH$}", last_active(session, now)),
                        theme::DIM,
                    ),
                ]
            }
        };
        lines.push(Line::from(spans));
    }
    frame.render_widget(Paragraph::new(lines), area);
}

// a job's role/project tag rides after its title
fn picker_label(session: &arc_proto::v1::SessionInfo, room: usize, job: bool) -> String {
    if !job {
        return label(session, room);
    }
    let role = SessionRole::try_from(session.role).unwrap_or(SessionRole::Unspecified);
    let tag = format!(
        " {}/{}",
        arc_core::provider::role_label(role),
        session.project
    );
    let base_room = room.saturating_sub(tag.chars().count());
    format!("{}{tag}", label(session, base_room))
}

fn draw_review(frame: &mut Frame, full: Rect, review: &crate::app::Review) {
    let rows = review.items.len().max(1);
    let inner_width = 72.min(full.width.saturating_sub(4)).saturating_sub(2);
    let detail = review
        .items
        .get(review.selected)
        .map_or(0, |entry| detail_height(entry, inner_width));
    let area = popup(
        frame,
        full,
        72,
        u16::try_from(rows + detail).unwrap_or(u16::MAX),
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
        frame.render_widget(Paragraph::new(lines), area);
        return;
    }

    let visible = (area.height as usize).saturating_sub(detail);
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

// grouped and built from the table below so a changed key and its
// documentation land in the same diff
const HELP: &[(&str, &[&str])] = &[
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
            "s ctrl-p          open the session picker",
            "y                 yank the last reply (V for a range)",
            "Y                 yank the whole conversation",
            "V                 visual mode: select a block range",
            "ctrl-t            back to the previous session",
            "ctrl-n            new session",
            "ctrl-o            toggle thought traces / handback summaries",
            "ctrl-c            quit",
            ":                 command mode",
        ],
    ),
    (
        "visual mode",
        &[
            "j k               move the selection boundary",
            "gg G              selection to the first / last block",
            "y                 yank the selection",
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
            ":code <project>   open a bound executor session, no dispatch",
            ":help             this popup",
        ],
    ),
    (
        "picker keys",
        &[
            "j k               move selection",
            "/                 filter by title/preview",
            "space a           toggle showing dispatched jobs",
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
            "q esc             close",
        ],
    ),
    (
        "jobs keys",
        &[
            "j k               move selection",
            "r                 refresh the list",
            "enter             open the selected job's session",
            "s                 steer the selected job",
            "x                 cancel the selected job (if running)",
            "d                 drop its queued steers (if any)",
            "q esc             close",
        ],
    ),
];

fn draw_help(frame: &mut Frame, full: Rect) {
    let mut lines = Vec::new();
    for (i, (group, keys)) in HELP.iter().enumerate() {
        if i > 0 {
            lines.push(Line::default());
        }
        lines.push(Line::styled(format!(" {group}"), theme::DIM));
        for key in *keys {
            lines.push(Line::styled(format!("   {key}"), theme::DIM));
        }
    }

    let total = u16::try_from(lines.len()).unwrap_or(u16::MAX);
    let area = popup(frame, full, 60, total, "help");
    if (area.height as usize) < lines.len() {
        lines.truncate((area.height as usize).saturating_sub(1));
        lines.push(Line::styled("   ...", theme::DIM));
    }
    frame.render_widget(Paragraph::new(lines), area);
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
    use arc_proto::v1::{JobInfo, SessionInfo, SessionRole, job_info};

    use super::{job_label, label, last_active, picker_label, strip_label, wrap_input};

    fn session(id: &str, title: &str, preview: &str) -> SessionInfo {
        SessionInfo {
            id: id.to_owned(),
            title: title.to_owned(),
            started_at: None,
            preview: preview.to_owned(),
            last_at: None,
            role: 0,
            project: String::new(),
            dispatched_by: String::new(),
            source: 0,
        }
    }

    fn active_ago(now: chrono::DateTime<chrono::Utc>, seconds_ago: i64) -> SessionInfo {
        let at = now - chrono::Duration::seconds(seconds_ago);
        SessionInfo {
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
    fn a_job_row_in_the_picker_keeps_its_title_and_gains_a_role_project_tag() {
        let mut job = session("s-01", "Fix the flaky test", "");
        job.role = SessionRole::Executor as i32;
        job.project = "arc".to_owned();

        assert_eq!(
            picker_label(&job, 40, true),
            "Fix the flaky test executor/arc"
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
