use std::collections::VecDeque;
use std::time::Instant;

use arc_proto::v1::{
    HistoryEntry, HistoryMessage, JobInfo, Role, SessionInfo, Source, ToolOutcome, history_entry,
    job_info,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

pub const PAGE: usize = 10;

const REVIEW_WINDOW_MICROS: i64 = 7 * 24 * 3_600 * 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    List,
    History {
        session_id: String,
    },
    Send {
        session_id: Option<String>,
        content: String,
    },
    ReviewList {
        since_micros: i64,
    },
    ReviewAccept {
        record_id: String,
    },
    ReviewDelete {
        record_id: String,
    },
    ListJobs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetEvent {
    Sessions(Vec<SessionInfo>),
    History {
        session_id: String,
        entries: Vec<HistoryEntry>,
    },
    Accepted {
        session_id: String,
    },
    Delta(String),
    Reasoning(String),
    ToolStarted {
        call_id: String,
        name: String,
        arguments_json: String,
    },
    ToolEnded {
        call_id: String,
        outcome: i32,
    },
    End {
        partial: bool,
        input_tokens: u32,
        output_tokens: u32,
    },
    Failed {
        code: String,
        msg: String,
    },
    Disconnected {
        reason: String,
    },
    ReviewItems(Vec<ReviewEntry>),
    JobItems(Vec<JobInfo>),
    SessionAppended {
        session_id: String,
    },
    JobChanged(JobInfo),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewEntry {
    pub id: String,
    pub kind: i32,
    pub namespace: String,
    pub title: String,
    pub summary: String,
    pub body: String,
    pub superseded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Review {
    pub items: Vec<ReviewEntry>,
    pub selected: usize,
    pub loaded: bool,
    pub pending_delete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Picker {
    pub selected: usize,
    /// True while the input line is owned by the filter query.
    pub filtering: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Jobs {
    pub items: Vec<JobInfo>,
    pub selected: usize,
    pub loaded: bool,
    /// The job being steered, if the input line is owned by the steer prompt.
    pub steering: Option<String>,
    pub confirmation: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Block {
    You(String),
    System(String),
    Arc {
        text: String,
        partial: bool,
    },
    Fault {
        code: String,
        msg: String,
    },
    Note(String),
    Thought {
        text: String,
        seconds: u64,
        done: bool,
        open: bool,
    },
    Tool {
        call_id: String,
        name: String,
        args: String,
        outcome: Option<&'static str>,
    },
    Cost {
        input_tokens: u32,
        output_tokens: u32,
        seconds: f32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Idle,
    Streaming,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Normal,
    Insert,
    Cmd,
}

pub struct App {
    pub transcript: Vec<Block>,
    pub input: String,
    pub cursor: usize,
    pub mode: Mode,
    pub cmd: String,
    pending: Option<char>,
    pub session_id: Option<String>,
    pub sessions: Vec<SessionInfo>,
    pub picker: Option<Picker>,
    pub review: Option<Review>,
    pub jobs: Option<Jobs>,
    pub help: bool,
    pub status: Status,
    pub last_error: Option<String>,
    pub queued: VecDeque<String>,
    thinking_since: Option<Instant>,
    turn_started: Option<Instant>,
    pub scroll_back: usize,
    pub quit: bool,
    /// Every `job_changed` push, oldest to newest touched; the strip reads only the running ones.
    pub ambient: Vec<JobInfo>,
    strip_since: Instant,
    refetch_in_flight: bool,
    steer_stash: Option<String>,
    picker_filter_stash: Option<String>,
    /// True between a steer's `Send` and its `Accepted`/`End`, so those don't touch the open conversation.
    steer_turn_pending: bool,
}

impl App {
    pub fn new() -> Self {
        Self {
            transcript: Vec::new(),
            input: String::new(),
            cursor: 0,
            mode: Mode::Insert,
            cmd: String::new(),
            pending: None,
            session_id: None,
            sessions: Vec::new(),
            picker: None,
            review: None,
            jobs: None,
            help: false,
            status: Status::Idle,
            last_error: None,
            queued: VecDeque::new(),
            thinking_since: None,
            turn_started: None,
            scroll_back: 0,
            quit: false,
            ambient: Vec::new(),
            strip_since: Instant::now(),
            refetch_in_flight: false,
            steer_stash: None,
            picker_filter_stash: None,
            steer_turn_pending: false,
        }
    }

    pub fn on_scroll(&mut self, up: bool, lines: usize) {
        if let Some(review) = self.review.as_mut() {
            review.pending_delete = false;
            review.selected = if up {
                review.selected.saturating_sub(1)
            } else {
                (review.selected + 1).min(review.items.len().saturating_sub(1))
            };
            return;
        }
        if let Some(jobs) = self.jobs.as_mut() {
            jobs.selected = if up {
                jobs.selected.saturating_sub(1)
            } else {
                (jobs.selected + 1).min(jobs.items.len().saturating_sub(1))
            };
            return;
        }
        if self.picker.is_some() {
            self.move_picker_selection(up);
            return;
        }
        self.scroll_back = if up {
            self.scroll_back.saturating_add(lines)
        } else {
            self.scroll_back.saturating_sub(lines)
        };
    }

    pub fn on_key(&mut self, key: KeyEvent) -> Option<Command> {
        match key.code {
            KeyCode::PageUp => {
                self.on_scroll(true, PAGE);
                return None;
            }
            KeyCode::PageDown => {
                self.on_scroll(false, PAGE);
                return None;
            }
            _ => {}
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            return self.on_control(key.code);
        }
        if self.help {
            return self.on_help_key(key.code);
        }
        if self.review.is_some() {
            return self.on_review_key(key.code);
        }
        if self.jobs.is_some() {
            return self.on_jobs_key(key.code);
        }
        if self.picker.is_some() {
            return self.on_picker_key(key.code);
        }
        match self.mode {
            Mode::Insert => self.on_insert(key.code),
            Mode::Normal => self.on_normal(key.code),
            Mode::Cmd => self.on_cmd(key.code),
        }
    }

    fn on_control(&mut self, code: KeyCode) -> Option<Command> {
        match code {
            KeyCode::Char('c') => self.quit = true,
            KeyCode::Char('u') => self.on_scroll(true, PAGE),
            KeyCode::Char('d') => self.on_scroll(false, PAGE),
            KeyCode::Char('o') => self.toggle_thought(),
            KeyCode::Char('p') => self.open_picker(),
            KeyCode::Char('n') if self.status != Status::Streaming => {
                return self.start_session(None);
            }
            KeyCode::Char('j') if self.mode == Mode::Insert && self.picker.is_none() => {
                self.insert_newline();
            }
            _ => {}
        }
        None
    }

    fn insert_newline(&mut self) {
        self.input.insert(self.cursor, '\n');
        self.cursor += '\n'.len_utf8();
    }

    fn on_insert(&mut self, code: KeyCode) -> Option<Command> {
        match code {
            KeyCode::Esc => {
                self.mode = Mode::Normal;
                self.cursor_left();
                self.clamp_normal();
            }
            KeyCode::Enter => return self.submit(),
            _ => self.edit_input(code),
        }
        None
    }

    fn edit_input(&mut self, code: KeyCode) {
        match code {
            KeyCode::Char(c) => {
                self.input.insert(self.cursor, c);
                self.cursor += c.len_utf8();
            }
            KeyCode::Backspace => {
                if let Some((at, c)) = self.char_before_cursor() {
                    self.input.remove(at);
                    self.cursor -= c.len_utf8();
                }
            }
            KeyCode::Delete => {
                if self.cursor < self.input.len() {
                    self.input.remove(self.cursor);
                }
            }
            KeyCode::Left => self.cursor_left(),
            KeyCode::Right => self.cursor_right(self.input.len()),
            KeyCode::Home => self.cursor = 0,
            KeyCode::End => self.cursor = self.input.len(),
            _ => {}
        }
    }

    fn on_normal(&mut self, code: KeyCode) -> Option<Command> {
        if let Some(pending) = self.pending.take() {
            match (pending, code) {
                ('d', KeyCode::Char('d')) => {
                    self.input.clear();
                    self.cursor = 0;
                }
                ('g', KeyCode::Char('g')) => self.scroll_back = usize::MAX / 2,
                _ => {}
            }
            return None;
        }
        match code {
            KeyCode::Enter => return self.submit(),
            KeyCode::Char('i') => self.mode = Mode::Insert,
            KeyCode::Char('I') => {
                self.cursor = 0;
                self.mode = Mode::Insert;
            }
            KeyCode::Char('a') => {
                self.cursor_right(self.input.len());
                self.mode = Mode::Insert;
            }
            KeyCode::Char('A') => {
                self.cursor = self.input.len();
                self.mode = Mode::Insert;
            }
            KeyCode::Char('h') | KeyCode::Left => self.cursor_left(),
            KeyCode::Char('l') | KeyCode::Right => {
                self.cursor_right(self.last_char_start());
            }
            KeyCode::Char('0') | KeyCode::Home => self.cursor = 0,
            KeyCode::Char('$') | KeyCode::End => self.cursor = self.last_char_start(),
            KeyCode::Char('w') => self.cursor = self.next_word_start(),
            KeyCode::Char('b') => self.cursor = self.prev_word_start(),
            KeyCode::Char('x') => {
                if self.cursor < self.input.len() {
                    self.input.remove(self.cursor);
                    self.clamp_normal();
                }
            }
            KeyCode::Char('D') => self.input.truncate(self.cursor),
            KeyCode::Char(c @ ('d' | 'g')) => self.pending = Some(c),
            KeyCode::Char('j') => self.scroll_back = self.scroll_back.saturating_sub(1),
            KeyCode::Char('k') => self.scroll_back = self.scroll_back.saturating_add(1),
            KeyCode::Char('G') => self.scroll_back = 0,
            KeyCode::Char('s') => self.open_picker(),
            KeyCode::Char(':') => {
                self.cmd.clear();
                self.mode = Mode::Cmd;
            }
            _ => {}
        }
        None
    }

    fn on_cmd(&mut self, code: KeyCode) -> Option<Command> {
        match code {
            KeyCode::Esc => self.mode = Mode::Normal,
            KeyCode::Char(c) => self.cmd.push(c),
            KeyCode::Backspace => {
                if self.cmd.pop().is_none() {
                    self.mode = Mode::Normal;
                }
            }
            KeyCode::Enter => {
                self.mode = Mode::Normal;
                match self.cmd.as_str() {
                    "q" | "q!" | "qa" | "quit" => self.quit = true,
                    "review" => return Some(self.open_review()),
                    "jobs" => return Some(self.open_jobs()),
                    "help" => self.help = true,
                    _ => self.last_error = Some("E492".to_owned()),
                }
            }
            _ => {}
        }
        None
    }

    fn open_review(&mut self) -> Command {
        self.review = Some(Review {
            items: Vec::new(),
            selected: 0,
            loaded: false,
            pending_delete: false,
        });
        Command::ReviewList {
            since_micros: chrono::Utc::now().timestamp_micros() - REVIEW_WINDOW_MICROS,
        }
    }

    fn on_review_key(&mut self, code: KeyCode) -> Option<Command> {
        let review = self.review.as_mut().expect("review is open");
        let last = review.items.len().saturating_sub(1);
        match code {
            KeyCode::Esc | KeyCode::Char('q') => self.review = None,
            KeyCode::Up | KeyCode::Char('k') => {
                review.selected = review.selected.saturating_sub(1);
                review.pending_delete = false;
            }
            KeyCode::Down | KeyCode::Char('j') => {
                review.selected = (review.selected + 1).min(last);
                review.pending_delete = false;
            }
            KeyCode::Char('a') => {
                review.pending_delete = false;
                return Self::take_verdict(review)
                    .map(|record_id| Command::ReviewAccept { record_id });
            }
            KeyCode::Char('d') if review.pending_delete => {
                review.pending_delete = false;
                return Self::take_verdict(review)
                    .map(|record_id| Command::ReviewDelete { record_id });
            }
            KeyCode::Char('d') => {
                review.pending_delete = !review.items.is_empty();
            }
            KeyCode::Char('f') => {
                if let Some(entry) = review.items.get(review.selected) {
                    self.input = format!("fix memory {}: {} — ", entry.id, entry.title);
                    self.cursor = self.input.len();
                    self.mode = Mode::Insert;
                    self.review = None;
                }
            }
            _ => review.pending_delete = false,
        }
        None
    }

    fn take_verdict(review: &mut Review) -> Option<String> {
        if review.items.is_empty() {
            return None;
        }
        let entry = review.items.remove(review.selected);
        review.selected = review.selected.min(review.items.len().saturating_sub(1));
        Some(entry.id)
    }

    fn on_help_key(&mut self, code: KeyCode) -> Option<Command> {
        if matches!(code, KeyCode::Esc | KeyCode::Char('q')) {
            self.help = false;
        }
        None
    }

    fn open_jobs(&mut self) -> Command {
        self.jobs = Some(Jobs {
            items: Vec::new(),
            selected: 0,
            loaded: false,
            steering: None,
            confirmation: None,
        });
        Command::ListJobs
    }

    fn on_jobs_key(&mut self, code: KeyCode) -> Option<Command> {
        if self.jobs.as_ref().expect("jobs is open").steering.is_some() {
            return self.on_steer_key(code);
        }
        let jobs = self.jobs.as_mut().expect("jobs is open");
        jobs.confirmation = None;
        let last = jobs.items.len().saturating_sub(1);
        match code {
            KeyCode::Esc | KeyCode::Char('q') => self.jobs = None,
            KeyCode::Up | KeyCode::Char('k') => jobs.selected = jobs.selected.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => jobs.selected = (jobs.selected + 1).min(last),
            KeyCode::Char('r') => return Some(Command::ListJobs),
            KeyCode::Enter => {
                let session_id = self.selected_job();
                self.jobs = None;
                return self.start_session(session_id);
            }
            KeyCode::Char('s') => {
                if let Some(session_id) = self.selected_job() {
                    self.start_steer(session_id);
                }
            }
            _ => {}
        }
        None
    }

    fn selected_job(&self) -> Option<String> {
        let jobs = self.jobs.as_ref()?;
        jobs.items
            .get(jobs.selected)
            .map(|job| job.session_id.clone())
    }

    fn start_steer(&mut self, session_id: String) {
        self.steer_stash = Some(std::mem::take(&mut self.input));
        self.cursor = 0;
        self.jobs.as_mut().expect("jobs is open").steering = Some(session_id);
    }

    fn on_steer_key(&mut self, code: KeyCode) -> Option<Command> {
        match code {
            KeyCode::Esc => self.cancel_steer(),
            KeyCode::Enter => return self.submit_steer(),
            _ => self.edit_input(code),
        }
        None
    }

    fn cancel_steer(&mut self) {
        self.jobs.as_mut().expect("jobs is open").steering = None;
        self.input = self.steer_stash.take().unwrap_or_default();
        self.cursor = self.input.len();
    }

    fn submit_steer(&mut self) -> Option<Command> {
        let content = self.input.trim().to_owned();
        if content.is_empty() {
            return None;
        }
        let jobs = self.jobs.as_mut().expect("jobs is open");
        let session_id = jobs.steering.take()?;
        jobs.confirmation = Some(format!("steered {}", short_id(&session_id)));
        self.input = self.steer_stash.take().unwrap_or_default();
        self.cursor = self.input.len();
        self.steer_turn_pending = true;
        Some(Command::Send {
            session_id: Some(session_id),
            content,
        })
    }

    fn on_picker_key(&mut self, code: KeyCode) -> Option<Command> {
        if self.picker.as_ref().expect("picker is open").filtering {
            return self.on_picker_filter_key(code);
        }
        let selected = self.picker.as_ref().expect("picker is open").selected;
        match code {
            KeyCode::Esc => self.picker = None,
            KeyCode::Up | KeyCode::Char('k') => self.move_picker_selection(true),
            KeyCode::Down | KeyCode::Char('j') => self.move_picker_selection(false),
            KeyCode::Char('/') => self.start_picker_filter(),
            KeyCode::Enter => {
                let chosen = self.picker_session(selected).map(|s| s.id.clone());
                self.picker = None;
                return self.start_session(chosen);
            }
            _ => {}
        }
        None
    }

    /// The filter query lives in `self.input`, same mechanism as the steer prompt.
    fn on_picker_filter_key(&mut self, code: KeyCode) -> Option<Command> {
        match code {
            KeyCode::Esc => self.cancel_picker_filter(),
            KeyCode::Enter => return self.open_filtered_session(),
            KeyCode::Up => self.move_picker_selection(true),
            KeyCode::Down => self.move_picker_selection(false),
            _ => {
                self.edit_input(code);
                self.clamp_picker_selection();
            }
        }
        None
    }

    fn start_picker_filter(&mut self) {
        self.picker_filter_stash = Some(std::mem::take(&mut self.input));
        self.cursor = 0;
        self.picker.as_mut().expect("picker is open").filtering = true;
    }

    fn cancel_picker_filter(&mut self) {
        self.picker.as_mut().expect("picker is open").filtering = false;
        self.input = self.picker_filter_stash.take().unwrap_or_default();
        self.cursor = self.input.len();
        self.clamp_picker_selection();
    }

    fn open_filtered_session(&mut self) -> Option<Command> {
        let selected = self.picker.as_ref().expect("picker is open").selected;
        let chosen = self.picker_session(selected).map(|s| s.id.clone());
        self.picker = None;
        self.input = self.picker_filter_stash.take().unwrap_or_default();
        self.cursor = self.input.len();
        self.start_session(chosen)
    }

    fn move_picker_selection(&mut self, up: bool) {
        let selected = self.picker.as_ref().expect("picker is open").selected;
        let last = self.picker_rows().len();
        self.picker.as_mut().expect("picker is open").selected = if up {
            selected.saturating_sub(1)
        } else {
            (selected + 1).min(last)
        };
    }

    fn clamp_picker_selection(&mut self) {
        let last = self.picker_rows().len();
        let picker = self.picker.as_mut().expect("picker is open");
        picker.selected = picker.selected.min(last);
    }

    fn open_picker(&mut self) {
        if self.status != Status::Streaming && self.review.is_none() && self.jobs.is_none() {
            self.picker = Some(Picker {
                selected: 0,
                filtering: false,
            });
        }
    }

    pub fn picker_session(&self, row: usize) -> Option<&SessionInfo> {
        row.checked_sub(1)
            .and_then(|i| self.picker_rows().get(i).copied())
    }

    /// The session list in recency order, narrowed by the active filter query if any.
    pub fn picker_rows(&self) -> Vec<&SessionInfo> {
        let order = self.by_recency();
        let filtering = self.picker.as_ref().is_some_and(|picker| picker.filtering);
        if !filtering || self.input.is_empty() {
            return order;
        }
        let needle = self.input.to_lowercase();
        order
            .into_iter()
            .filter(|session| {
                session.title.to_lowercase().contains(&needle)
                    || session.preview.to_lowercase().contains(&needle)
            })
            .collect()
    }

    /// The most recently touched running job, if any; the strip's subject.
    pub fn strip_job(&self) -> Option<&JobInfo> {
        self.ambient.iter().rev().find(|job| is_running(job))
    }

    pub fn running_job_count(&self) -> usize {
        self.ambient.iter().filter(|job| is_running(job)).count()
    }

    pub fn has_running_job(&self) -> bool {
        self.running_job_count() > 0
    }

    /// The job's elapsed seconds, ticked forward locally since the last push.
    pub fn strip_elapsed_seconds(&self, job: &JobInfo) -> u64 {
        u64::from(job.elapsed_seconds) + self.strip_since.elapsed().as_secs()
    }

    pub fn by_recency(&self) -> Vec<&SessionInfo> {
        let mut order: Vec<&SessionInfo> = self.sessions.iter().collect();
        order.sort_by(|a, b| activity(b).cmp(&activity(a)).then_with(|| a.id.cmp(&b.id)));
        order
    }

    fn start_session(&mut self, session_id: Option<String>) -> Option<Command> {
        self.session_id.clone_from(&session_id);
        self.transcript.clear();
        self.scroll_back = 0;
        self.last_error = None;
        self.refetch_in_flight = false;
        let session_id = session_id?;
        self.transcript.push(Block::Note("loading".to_owned()));
        Some(Command::History { session_id })
    }

    fn submit(&mut self) -> Option<Command> {
        let content = self.input.trim().to_owned();
        if content.is_empty() {
            return None;
        }
        self.input.clear();
        self.cursor = 0;
        self.scroll_back = 0;
        self.transcript.push(Block::You(content.clone()));
        if self.status == Status::Streaming {
            self.queued.push_back(content);
            return None;
        }
        Some(self.send(content))
    }

    fn send(&mut self, content: String) -> Command {
        self.status = Status::Streaming;
        self.last_error = None;
        self.turn_started = Some(Instant::now());
        Command::Send {
            session_id: self.session_id.clone(),
            content,
        }
    }

    pub fn on_net(&mut self, event: NetEvent) -> Option<Command> {
        match event {
            NetEvent::Sessions(sessions) => {
                self.sessions = sessions;
                None
            }
            NetEvent::History {
                session_id,
                entries,
            } => {
                if self.session_id.as_deref() == Some(session_id.as_str()) {
                    self.transcript = history_blocks(entries);
                    self.scroll_back = 0;
                    self.refetch_in_flight = false;
                }
                None
            }
            NetEvent::Accepted { session_id } => {
                if self.steer_turn_pending {
                    return None;
                }
                let created = self.session_id.is_none();
                self.session_id = Some(session_id);
                self.transcript.push(Block::Arc {
                    text: String::new(),
                    partial: false,
                });
                created.then_some(Command::List)
            }
            NetEvent::Delta(text) => {
                self.finalize_thinking();
                if let Some(Block::Arc { text: reply, .. }) = self.transcript.last_mut() {
                    reply.push_str(&text);
                } else {
                    self.transcript.push(Block::Arc {
                        text,
                        partial: false,
                    });
                }
                None
            }
            NetEvent::Reasoning(text) => {
                if let Some(Block::Thought {
                    text: thinking,
                    seconds,
                    done: false,
                    ..
                }) = self.transcript.last_mut()
                {
                    thinking.push_str(&text);
                    *seconds = Self::thought_seconds(self.thinking_since);
                } else {
                    self.pop_empty_reply();
                    self.thinking_since = Some(Instant::now());
                    self.transcript.push(Block::Thought {
                        text,
                        seconds: 1,
                        done: false,
                        open: false,
                    });
                }
                None
            }
            NetEvent::ToolStarted {
                call_id,
                name,
                arguments_json,
            } => {
                self.finalize_thinking();
                self.pop_empty_reply();
                self.transcript.push(Block::Tool {
                    call_id,
                    name,
                    args: tool_summary(&arguments_json),
                    outcome: None,
                });
                None
            }
            NetEvent::ToolEnded { call_id, outcome } => {
                let ended = self.transcript.iter_mut().rev().find(
                    |block| matches!(block, Block::Tool { call_id: id, .. } if *id == call_id),
                );
                if let Some(Block::Tool { outcome: o, .. }) = ended {
                    *o = Some(outcome_label(outcome));
                }
                None
            }
            NetEvent::End {
                partial,
                input_tokens,
                output_tokens,
            } => {
                if self.steer_turn_pending {
                    self.steer_turn_pending = false;
                    return None;
                }
                self.finalize_thinking();
                if let Some(Block::Arc { partial: p, .. }) = self.transcript.last_mut() {
                    *p = partial;
                }
                let elapsed = self.turn_started.take().map(|since| since.elapsed());
                if let Some(elapsed) = elapsed {
                    if input_tokens != 0 || output_tokens != 0 {
                        self.transcript.push(Block::Cost {
                            input_tokens,
                            output_tokens,
                            seconds: elapsed.as_secs_f32(),
                        });
                    }
                }
                self.turn_over(Status::Idle)
            }
            NetEvent::Failed { code, msg } => {
                self.steer_turn_pending = false;
                self.finalize_thinking();
                self.pop_empty_reply();
                self.turn_started = None;
                self.last_error = Some(code.clone());
                self.transcript.push(Block::Fault { code, msg });
                self.turn_over(Status::Idle)
            }
            NetEvent::ReviewItems(items) => {
                if let Some(review) = self.review.as_mut() {
                    review.items = items;
                    review.selected = 0;
                    review.loaded = true;
                    review.pending_delete = false;
                }
                None
            }
            NetEvent::JobItems(items) => {
                if let Some(jobs) = self.jobs.as_mut() {
                    jobs.items = items;
                    jobs.selected = 0;
                    jobs.loaded = true;
                }
                None
            }
            NetEvent::SessionAppended { session_id } => {
                let open = self.session_id.as_deref() == Some(session_id.as_str());
                if open && self.status != Status::Streaming && !self.refetch_in_flight {
                    self.refetch_in_flight = true;
                    return Some(Command::History { session_id });
                }
                None
            }
            NetEvent::JobChanged(job) => {
                self.ambient
                    .retain(|existing| existing.session_id != job.session_id);
                self.ambient.push(job.clone());
                self.strip_since = Instant::now();
                if let Some(jobs) = self.jobs.as_mut() {
                    if let Some(row) = jobs
                        .items
                        .iter_mut()
                        .find(|item| item.session_id == job.session_id)
                    {
                        *row = job;
                    }
                }
                None
            }
            NetEvent::Disconnected { reason } => {
                self.turn_started = None;
                self.last_error = Some("disconnected".to_owned());
                self.transcript.push(Block::Fault {
                    code: "disconnected".to_owned(),
                    msg: reason,
                });
                self.turn_over(Status::Disconnected)
            }
        }
    }

    fn turn_over(&mut self, status: Status) -> Option<Command> {
        self.status = status;
        let next = self.queued.pop_front()?;
        Some(self.send(next))
    }

    fn finalize_thinking(&mut self) {
        let since = self.thinking_since.take();
        if let Some(Block::Thought { seconds, done, .. }) = self.transcript.last_mut() {
            if !*done {
                *done = true;
                *seconds = Self::thought_seconds(since);
            }
        }
    }

    fn thought_seconds(since: Option<Instant>) -> u64 {
        since.map_or(0, |since| since.elapsed().as_secs()).max(1)
    }

    fn toggle_thought(&mut self) {
        let any_open = self
            .transcript
            .iter()
            .any(|block| matches!(block, Block::Thought { open: true, .. }));
        for block in &mut self.transcript {
            if let Block::Thought { open, .. } = block {
                *open = !any_open;
            }
        }
    }

    fn pop_empty_reply(&mut self) {
        if matches!(self.transcript.last(), Some(Block::Arc { text, .. }) if text.is_empty()) {
            self.transcript.pop();
        }
    }

    fn cursor_left(&mut self) {
        if let Some((at, _)) = self.char_before_cursor() {
            self.cursor = at;
        }
    }

    fn cursor_right(&mut self, limit: usize) {
        if let Some(c) = self.input[self.cursor..].chars().next() {
            self.cursor = (self.cursor + c.len_utf8()).min(limit);
        }
    }

    fn last_char_start(&self) -> usize {
        self.input
            .char_indices()
            .next_back()
            .map_or(0, |(at, _)| at)
    }

    fn clamp_normal(&mut self) {
        self.cursor = self.cursor.min(self.last_char_start());
    }

    fn next_word_start(&self) -> usize {
        let rest = &self.input[self.cursor..];
        let after_word = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let from_space = &rest[after_word..];
        let word = from_space
            .find(|c: char| !c.is_whitespace())
            .map(|at| self.cursor + after_word + at);
        word.unwrap_or_else(|| self.last_char_start())
    }

    fn prev_word_start(&self) -> usize {
        let before = &self.input[..self.cursor];
        let trimmed = before.trim_end();
        trimmed.rfind(char::is_whitespace).map_or(0, |at| at + 1)
    }

    fn char_before_cursor(&self) -> Option<(usize, char)> {
        self.input[..self.cursor].char_indices().next_back()
    }
}

fn is_running(job: &JobInfo) -> bool {
    job.state == job_info::State::Running as i32
}

fn short_id(id: &str) -> &str {
    &id[id.len().saturating_sub(8)..]
}

/// The first meaningful string value in the call's arguments: the bash
/// command, the read/write/edit path, and so on for anything shaped alike.
fn tool_summary(arguments_json: &str) -> String {
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str(arguments_json) else {
        return String::new();
    };
    map.values()
        .find_map(|value| match value {
            serde_json::Value::String(s) if !s.trim().is_empty() => Some(s.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

pub fn format_tokens(n: u32) -> String {
    if n < 1000 {
        n.to_string()
    } else {
        format!("{:.1}k", f64::from(n) / 1000.0)
    }
}

fn outcome_label(outcome: i32) -> &'static str {
    match ToolOutcome::try_from(outcome) {
        Ok(ToolOutcome::Ok) => "ok",
        Ok(ToolOutcome::Error) => "error",
        _ => "unknown",
    }
}

fn activity(session: &SessionInfo) -> Option<(i64, i32)> {
    session
        .last_at
        .or(session.started_at)
        .map(|ts| (ts.seconds, ts.nanos))
}

fn prose_block(message: HistoryMessage) -> Option<Block> {
    // handbacks ride the user role for the model; the display tells the truth
    if message.source == Source::System as i32 {
        return Some(Block::System(message.content));
    }
    match Role::try_from(message.role) {
        Ok(Role::User) => Some(Block::You(message.content)),
        Ok(Role::Assistant) if !message.content.is_empty() => Some(Block::Arc {
            text: message.content,
            partial: message.partial,
        }),
        _ => None,
    }
}

fn history_blocks(entries: Vec<HistoryEntry>) -> Vec<Block> {
    let mut blocks = Vec::new();
    for entry in entries {
        match entry.entry {
            Some(history_entry::Entry::Message(message)) => {
                blocks.extend(prose_block(message));
            }
            Some(history_entry::Entry::ToolCall(call)) => blocks.push(Block::Tool {
                call_id: call.call_id,
                name: call.name,
                args: tool_summary(&call.arguments_json),
                outcome: None,
            }),
            Some(history_entry::Entry::ToolResult(result)) => {
                let ended = blocks.iter_mut().rev().find(
                    |block| matches!(block, Block::Tool { call_id, .. } if *call_id == result.call_id),
                );
                if let Some(Block::Tool { outcome, .. }) = ended {
                    *outcome = Some(outcome_label(result.outcome));
                }
            }
            None => {}
        }
    }
    for block in &mut blocks {
        if let Block::Tool {
            outcome: outcome @ None,
            ..
        } = block
        {
            *outcome = Some("unknown");
        }
    }
    blocks
}

#[cfg(test)]
mod tests {
    use arc_proto::v1::{HistoryToolCall, HistoryToolResult};

    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    fn typed(app: &mut App, text: &str) {
        for c in text.chars() {
            assert_eq!(app.on_key(key(KeyCode::Char(c))), None);
        }
    }

    fn normal(app: &mut App, keys: &str) {
        app.on_key(key(KeyCode::Esc));
        for c in keys.chars() {
            app.on_key(key(KeyCode::Char(c)));
        }
    }

    fn session(id: &str) -> SessionInfo {
        SessionInfo {
            id: id.to_owned(),
            title: String::new(),
            started_at: None,
            preview: String::new(),
            last_at: None,
        }
    }

    fn session_with(id: &str, title: &str, preview: &str) -> SessionInfo {
        SessionInfo {
            id: id.to_owned(),
            title: title.to_owned(),
            started_at: None,
            preview: preview.to_owned(),
            last_at: None,
        }
    }

    fn picker_selected(app: &App) -> Option<usize> {
        app.picker.as_ref().map(|picker| picker.selected)
    }

    fn end(partial: bool) -> NetEvent {
        NetEvent::End {
            partial,
            input_tokens: 0,
            output_tokens: 0,
        }
    }

    fn started(call_id: &str, name: &str) -> NetEvent {
        NetEvent::ToolStarted {
            call_id: call_id.to_owned(),
            name: name.to_owned(),
            arguments_json: String::new(),
        }
    }

    #[test]
    fn enter_sends_and_shows_the_message() {
        let mut app = App::new();
        typed(&mut app, "hello");

        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(
            command,
            Some(Command::Send {
                session_id: None,
                content: "hello".to_owned()
            })
        );
        assert_eq!(app.transcript, [Block::You("hello".to_owned())]);
        assert_eq!(app.input, "");
        assert_eq!(app.status, Status::Streaming);
    }

    #[test]
    fn a_blank_input_does_not_send() {
        let mut app = App::new();
        typed(&mut app, "   ");
        assert_eq!(app.on_key(key(KeyCode::Enter)), None);
        assert_eq!(app.transcript, []);
    }

    #[test]
    fn a_turn_streams_into_one_reply_block() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));

        let refresh = app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Delta("hel".to_owned()));
        app.on_net(NetEvent::Delta("lo".to_owned()));
        let next = app.on_net(end(false));

        assert_eq!(
            refresh,
            Some(Command::List),
            "a new session refreshes the list"
        );
        assert_eq!(next, None);
        assert_eq!(app.session_id.as_deref(), Some("s-1"));
        assert_eq!(app.status, Status::Idle);
        assert_eq!(
            app.transcript,
            [
                Block::You("hi".to_owned()),
                Block::Arc {
                    text: "hello".to_owned(),
                    partial: false
                }
            ]
        );
    }

    #[test]
    fn an_existing_session_does_not_refresh_the_list() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));

        let refresh = app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        assert_eq!(refresh, None);
    }

    #[test]
    fn a_cut_reply_is_marked_partial() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Delta("half a th".to_owned()));
        app.on_net(end(true));

        assert!(matches!(
            app.transcript.last(),
            Some(Block::Arc { partial: true, .. })
        ));
    }

    #[test]
    fn reasoning_streams_into_a_closed_live_thought() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });

        app.on_net(NetEvent::Reasoning("let me ".to_owned()));
        app.on_net(NetEvent::Reasoning("think".to_owned()));
        assert_eq!(
            app.transcript,
            [
                Block::You("hi".to_owned()),
                Block::Thought {
                    text: "let me think".to_owned(),
                    seconds: 1,
                    done: false,
                    open: false,
                },
            ],
            "the trace accumulates folded, where the reply will appear"
        );
    }

    #[test]
    fn the_first_delta_finalizes_the_thought_and_keeps_the_words() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Reasoning("let me think".to_owned()));

        app.on_net(NetEvent::Delta("hello".to_owned()));
        assert_eq!(
            app.transcript,
            [
                Block::You("hi".to_owned()),
                Block::Thought {
                    text: "let me think".to_owned(),
                    seconds: 1,
                    done: true,
                    open: false,
                },
                Block::Arc {
                    text: "hello".to_owned(),
                    partial: false
                },
            ],
            "the words stay; only the streaming stops"
        );
    }

    #[test]
    fn reasoning_finalizes_on_end_when_no_text_ever_came() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Reasoning("hmm".to_owned()));
        app.on_net(end(true));

        assert_eq!(
            app.transcript,
            [
                Block::You("hi".to_owned()),
                Block::Thought {
                    text: "hmm".to_owned(),
                    seconds: 1,
                    done: true,
                    open: false,
                }
            ]
        );
        assert_eq!(app.status, Status::Idle);
    }

    #[test]
    fn ctrl_o_toggles_a_done_thought_from_either_mode() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Reasoning("hmm".to_owned()));
        app.on_net(NetEvent::Delta("hello".to_owned()));
        app.on_net(end(false));

        assert_eq!(app.on_key(ctrl('o')), None, "insert mode opens it");
        assert!(matches!(
            app.transcript[1],
            Block::Thought { open: true, .. }
        ));

        app.on_key(key(KeyCode::Esc));
        app.on_key(ctrl('o'));
        assert!(
            matches!(app.transcript[1], Block::Thought { open: false, .. }),
            "normal mode closes it again"
        );
    }

    #[test]
    fn ctrl_o_opens_a_live_thought_and_finalizing_keeps_it_open() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Reasoning("hmm".to_owned()));

        app.on_key(ctrl('o'));
        assert!(
            matches!(
                app.transcript.last(),
                Some(Block::Thought {
                    done: false,
                    open: true,
                    ..
                })
            ),
            "a live thought streams open"
        );

        app.on_net(NetEvent::Delta("hello".to_owned()));
        assert!(
            matches!(
                app.transcript[1],
                Block::Thought {
                    done: true,
                    open: true,
                    ..
                }
            ),
            "finalizing does not fold it back"
        );
    }

    #[test]
    fn ctrl_o_without_a_thought_is_a_no_op() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));

        assert_eq!(app.on_key(ctrl('o')), None);
        assert_eq!(app.transcript, [Block::You("hi".to_owned())]);
    }

    #[test]
    fn a_turn_that_fails_mid_thought_keeps_the_trace_openable() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Reasoning("hmm".to_owned()));
        app.on_net(NetEvent::Failed {
            code: "provider".to_owned(),
            msg: "upstream 500".to_owned(),
        });

        assert_eq!(
            app.transcript,
            [
                Block::You("hi".to_owned()),
                Block::Thought {
                    text: "hmm".to_owned(),
                    seconds: 1,
                    done: true,
                    open: false,
                },
                Block::Fault {
                    code: "provider".to_owned(),
                    msg: "upstream 500".to_owned()
                },
            ]
        );

        app.on_key(ctrl('o'));
        assert!(matches!(
            app.transcript[1],
            Block::Thought { open: true, .. }
        ));
    }

    #[test]
    fn ctrl_o_toggles_every_thought_at_once() {
        let mut app = App::new();
        typed(&mut app, "one");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Reasoning("first".to_owned()));
        app.on_net(NetEvent::Delta("a".to_owned()));
        app.on_net(end(false));

        typed(&mut app, "two");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Reasoning("second".to_owned()));
        app.on_net(NetEvent::Delta("b".to_owned()));
        app.on_net(end(false));

        app.on_key(ctrl('o'));
        for at in [1, 4] {
            assert!(
                matches!(&app.transcript[at], Block::Thought { open: true, .. }),
                "both traces open together"
            );
        }

        if let Block::Thought { open, .. } = &mut app.transcript[1] {
            *open = false;
        }
        app.on_key(ctrl('o'));
        for at in [1, 4] {
            assert!(
                matches!(&app.transcript[at], Block::Thought { open: false, .. }),
                "any open means the toggle closes all"
            );
        }
    }

    #[test]
    fn tool_lines_resolve_by_call_id_with_two_in_flight() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Reasoning("checking".to_owned()));
        app.on_net(started("a", "alpha"));
        app.on_net(started("b", "beta"));

        app.on_net(NetEvent::ToolEnded {
            call_id: "b".to_owned(),
            outcome: ToolOutcome::Ok as i32,
        });
        assert_eq!(
            app.transcript,
            [
                Block::You("hi".to_owned()),
                Block::Thought {
                    text: "checking".to_owned(),
                    seconds: 1,
                    done: true,
                    open: false,
                },
                Block::Tool {
                    call_id: "a".to_owned(),
                    name: "alpha".to_owned(),
                    args: String::new(),
                    outcome: None,
                },
                Block::Tool {
                    call_id: "b".to_owned(),
                    name: "beta".to_owned(),
                    args: String::new(),
                    outcome: Some("ok"),
                },
            ]
        );

        app.on_net(NetEvent::ToolEnded {
            call_id: "a".to_owned(),
            outcome: ToolOutcome::Error as i32,
        });
        assert!(matches!(
            &app.transcript[2],
            Block::Tool {
                outcome: Some("error"),
                ..
            }
        ));
    }

    #[test]
    fn a_live_tool_call_carries_its_bash_command_as_the_summary() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::ToolStarted {
            call_id: "t1".to_owned(),
            name: "bash".to_owned(),
            arguments_json: r#"{"command":"cargo test"}"#.to_owned(),
        });

        assert_eq!(
            app.transcript.last(),
            Some(&Block::Tool {
                call_id: "t1".to_owned(),
                name: "bash".to_owned(),
                args: "cargo test".to_owned(),
                outcome: None,
            })
        );
    }

    #[test]
    fn a_history_tool_call_carries_its_read_path_as_the_summary() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![HistoryEntry {
                entry: Some(history_entry::Entry::ToolCall(HistoryToolCall {
                    call_id: "t1".to_owned(),
                    name: "read".to_owned(),
                    arguments_json: r#"{"path":"src/main.rs"}"#.to_owned(),
                })),
            }],
        });

        assert_eq!(
            app.transcript,
            [Block::Tool {
                call_id: "t1".to_owned(),
                name: "read".to_owned(),
                args: "src/main.rs".to_owned(),
                outcome: Some("unknown"),
            }]
        );
    }

    #[test]
    fn unparseable_arguments_leave_the_summary_blank() {
        assert_eq!(tool_summary("not json"), "");
        assert_eq!(tool_summary(""), "");
        assert_eq!(
            tool_summary(r#"{"count":3}"#),
            "",
            "no string field to show"
        );
    }

    #[test]
    fn an_empty_assistant_message_produces_no_block() {
        let message = prose(Role::Assistant as i32, "", false);
        assert_eq!(prose_block(message), None, "a text-less tool step");

        let non_empty = prose(Role::Assistant as i32, "hello", false);
        assert!(prose_block(non_empty).is_some());
    }

    #[test]
    fn a_completed_turn_appends_a_cost_block_with_the_reported_usage() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Delta("hello".to_owned()));

        app.on_net(NetEvent::End {
            partial: false,
            input_tokens: 2345,
            output_tokens: 140,
        });

        assert!(matches!(
            app.transcript.last(),
            Some(Block::Cost {
                input_tokens: 2345,
                output_tokens: 140,
                ..
            })
        ));
    }

    #[test]
    fn a_zero_usage_end_appends_no_cost_block() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });

        app.on_net(end(false));

        assert!(
            !app.transcript
                .iter()
                .any(|block| matches!(block, Block::Cost { .. })),
            "a steer ack carries zeroed usage"
        );
    }

    #[test]
    fn a_failed_turn_appends_no_cost_block() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });

        app.on_net(NetEvent::Failed {
            code: "provider".to_owned(),
            msg: "upstream 500".to_owned(),
        });

        assert!(
            !app.transcript
                .iter()
                .any(|block| matches!(block, Block::Cost { .. }))
        );
    }

    #[test]
    fn format_tokens_stays_bare_under_a_thousand_and_gains_k_above_it() {
        assert_eq!(format_tokens(999), "999");
        assert_eq!(format_tokens(2345), "2.3k");
    }

    #[test]
    fn an_unrecognized_outcome_renders_as_unknown() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(started("t", "get_time"));
        app.on_net(NetEvent::ToolEnded {
            call_id: "t".to_owned(),
            outcome: 42,
        });

        assert_eq!(
            app.transcript,
            [
                Block::You("hi".to_owned()),
                Block::Tool {
                    call_id: "t".to_owned(),
                    name: "get_time".to_owned(),
                    args: String::new(),
                    outcome: Some("unknown"),
                },
            ]
        );
    }

    #[test]
    fn a_failed_turn_replaces_the_empty_reply_with_a_fault() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Failed {
            code: "provider".to_owned(),
            msg: "upstream 500".to_owned(),
        });

        assert_eq!(
            app.transcript,
            [
                Block::You("hi".to_owned()),
                Block::Fault {
                    code: "provider".to_owned(),
                    msg: "upstream 500".to_owned()
                }
            ]
        );
        assert_eq!(app.last_error.as_deref(), Some("provider"));
        assert_eq!(app.status, Status::Idle);
    }

    #[test]
    fn typing_while_streaming_queues_and_the_end_flushes() {
        let mut app = App::new();
        typed(&mut app, "one");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });

        typed(&mut app, "two");
        assert_eq!(app.on_key(key(KeyCode::Enter)), None, "queued, not sent");
        assert_eq!(app.queued.len(), 1);

        let next = app.on_net(end(false));
        assert_eq!(
            next,
            Some(Command::Send {
                session_id: Some("s-1".to_owned()),
                content: "two".to_owned()
            }),
            "the queued message goes to the session the first turn named"
        );
        assert_eq!(app.status, Status::Streaming);
        assert!(app.queued.is_empty());
    }

    #[test]
    fn disconnecting_faults_the_transcript_and_retries_the_queue() {
        let mut app = App::new();
        typed(&mut app, "one");
        app.on_key(key(KeyCode::Enter));
        typed(&mut app, "two");
        app.on_key(key(KeyCode::Enter));

        let retry = app.on_net(NetEvent::Disconnected {
            reason: "the daemon closed the connection".to_owned(),
        });

        assert!(matches!(
            app.transcript.last(),
            Some(Block::Fault { code, .. }) if code == "disconnected"
        ));
        assert_eq!(
            retry,
            Some(Command::Send {
                session_id: None,
                content: "two".to_owned()
            }),
            "the queued message drives the reconnect attempt"
        );
    }

    #[test]
    fn esc_enters_normal_mode_on_the_last_char() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Esc));
        assert_eq!(app.mode, Mode::Normal);
        assert_eq!(app.cursor, 1, "vim lands on the char, not past it");
        app.on_key(key(KeyCode::Char('i')));
        assert_eq!(app.mode, Mode::Insert);
    }

    #[test]
    fn normal_mode_edits_the_line_vim_style() {
        let mut app = App::new();
        typed(&mut app, "the quick fox");

        normal(&mut app, "0x");
        assert_eq!(app.input, "he quick fox");

        normal(&mut app, "0wD");
        assert_eq!(app.input, "he ", "D cuts from the word to the end");

        normal(&mut app, "dd");
        assert_eq!(app.input, "");
        assert_eq!(app.cursor, 0);
    }

    #[test]
    fn word_motions_move_between_words() {
        let mut app = App::new();
        typed(&mut app, "one two three");

        normal(&mut app, "0w");
        assert_eq!(app.cursor, 4);
        app.on_key(key(KeyCode::Char('w')));
        assert_eq!(app.cursor, 8);
        app.on_key(key(KeyCode::Char('b')));
        assert_eq!(app.cursor, 4);
        app.on_key(key(KeyCode::Char('$')));
        assert_eq!(app.cursor, 12, "$ sits on the last char");
        app.on_key(key(KeyCode::Char('a')));
        assert_eq!(app.mode, Mode::Insert);
        assert_eq!(app.cursor, 13, "a appends after the char");
    }

    #[test]
    fn normal_mode_scrolls_the_transcript() {
        let mut app = App::new();
        normal(&mut app, "kkk");
        assert_eq!(app.scroll_back, 3);
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.scroll_back, 2);
        app.on_key(key(KeyCode::Char('G')));
        assert_eq!(app.scroll_back, 0);
        app.on_key(key(KeyCode::Char('g')));
        app.on_key(key(KeyCode::Char('g')));
        assert!(app.scroll_back > 1000, "gg overshoots; drawing clamps");
    }

    #[test]
    fn the_page_keys_scroll_from_any_mode() {
        let mut app = App::new();

        app.on_scroll(true, PAGE);
        assert_eq!(app.scroll_back, PAGE);
        app.on_scroll(false, PAGE);
        assert_eq!(app.scroll_back, 0);
        app.on_scroll(false, PAGE);
        assert_eq!(app.scroll_back, 0, "scrolling down at the bottom stays put");

        typed(&mut app, "half a sentence");
        assert_eq!(app.on_key(key(KeyCode::PageUp)), None);
        assert_eq!(app.scroll_back, PAGE);
        assert_eq!(app.input, "half a sentence", "and do not type themselves");
        assert_eq!(app.mode, Mode::Insert);

        app.on_key(key(KeyCode::PageDown));
        assert_eq!(app.scroll_back, 0);
    }

    #[test]
    fn the_picker_orders_sessions_by_last_activity() {
        fn at(id: &str, started: i64, last: Option<i64>) -> SessionInfo {
            SessionInfo {
                id: id.to_owned(),
                title: String::new(),
                preview: String::new(),
                started_at: Some(prost_types::Timestamp {
                    seconds: started,
                    nanos: 0,
                }),
                last_at: last.map(|seconds| prost_types::Timestamp { seconds, nanos: 0 }),
            }
        }

        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![
            at("old-but-active", 100, Some(900)),
            at("newer-but-stale", 500, Some(600)),
            at("empty", 700, None),
        ]));

        let order: Vec<&str> = app.by_recency().iter().map(|s| s.id.as_str()).collect();
        assert_eq!(order, ["old-but-active", "empty", "newer-but-stale"]);
        assert_eq!(
            app.picker_session(1).map(|s| s.id.as_str()),
            Some("old-but-active"),
            "row 1 is the session you were last in"
        );
    }

    #[test]
    fn scrolling_moves_the_picker_when_it_is_open() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![session("old"), session("new")]));
        normal(&mut app, "s");

        app.on_scroll(false, PAGE);
        assert_eq!(
            picker_selected(&app),
            Some(1),
            "one row per gesture, not one page"
        );
        app.on_scroll(false, PAGE);
        assert_eq!(picker_selected(&app), Some(2));
        app.on_scroll(false, PAGE);
        assert_eq!(
            picker_selected(&app),
            Some(2),
            "and it stops at the last session"
        );

        app.on_scroll(true, PAGE);
        assert_eq!(picker_selected(&app), Some(1));
        assert_eq!(app.scroll_back, 0, "the transcript never moved");
    }

    #[test]
    fn colon_q_quits_and_unknown_commands_are_e492() {
        let mut app = App::new();
        normal(&mut app, ":wq");
        app.on_key(key(KeyCode::Enter));
        assert!(!app.quit);
        assert_eq!(app.last_error.as_deref(), Some("E492"));

        normal(&mut app, ":q");
        app.on_key(key(KeyCode::Enter));
        assert!(app.quit);
    }

    #[test]
    fn the_picker_lists_newest_first_and_switches_sessions() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![session("old"), session("new")]));

        normal(&mut app, "s");
        assert_eq!(picker_selected(&app), Some(0));
        assert_eq!(app.picker_session(1).map(|s| s.id.as_str()), Some("new"));
        assert_eq!(app.picker_session(2).map(|s| s.id.as_str()), Some("old"));

        app.on_key(key(KeyCode::Char('j')));
        let fetch = app.on_key(key(KeyCode::Enter));

        assert_eq!(app.picker, None);
        assert_eq!(app.session_id.as_deref(), Some("new"));
        assert_eq!(
            fetch,
            Some(Command::History {
                session_id: "new".to_owned()
            }),
            "opening a session asks for its transcript"
        );
        assert_eq!(
            app.transcript,
            [Block::Note("loading".to_owned())],
            "until the answer lands, the wait is visible"
        );
    }

    fn prose(role: i32, content: &str, partial: bool) -> HistoryMessage {
        HistoryMessage {
            role,
            content: content.to_owned(),
            partial,
            source: 0,
        }
    }

    fn prose_entry(role: i32, content: &str, partial: bool) -> HistoryEntry {
        HistoryEntry {
            entry: Some(history_entry::Entry::Message(prose(role, content, partial))),
        }
    }

    fn call_entry(call_id: &str, name: &str) -> HistoryEntry {
        HistoryEntry {
            entry: Some(history_entry::Entry::ToolCall(HistoryToolCall {
                call_id: call_id.to_owned(),
                name: name.to_owned(),
                arguments_json: String::new(),
            })),
        }
    }

    fn result_entry(call_id: &str, outcome: i32) -> HistoryEntry {
        HistoryEntry {
            entry: Some(history_entry::Entry::ToolResult(HistoryToolResult {
                call_id: call_id.to_owned(),
                outcome,
                truncated: false,
            })),
        }
    }

    #[test]
    fn history_replaces_the_loading_note_with_the_transcript() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![session("old")]));
        normal(&mut app, "s");
        app.on_key(key(KeyCode::Char('j')));
        app.on_key(key(KeyCode::Enter));

        app.on_net(NetEvent::History {
            session_id: "old".to_owned(),
            entries: vec![
                prose_entry(Role::User as i32, "what is a walking skeleton?", false),
                prose_entry(Role::Assistant as i32, "a thin end-to-end slice", true),
                prose_entry(Role::System as i32, "the identity file", false),
            ],
        });

        assert_eq!(
            app.transcript,
            [
                Block::You("what is a walking skeleton?".to_owned()),
                Block::Arc {
                    text: "a thin end-to-end slice".to_owned(),
                    partial: true
                }
            ],
            "user and model render, partial included; a system message has no speaker to be"
        );
    }

    #[test]
    fn a_reopened_tool_turn_matches_the_blocks_a_live_one_leaves() {
        let mut live = App::new();
        typed(&mut live, "hi");
        live.on_key(key(KeyCode::Enter));
        live.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        live.on_net(started("t1", "lookup"));
        live.on_net(NetEvent::ToolEnded {
            call_id: "t1".to_owned(),
            outcome: ToolOutcome::Ok as i32,
        });
        live.on_net(NetEvent::Delta("answer".to_owned()));
        live.on_net(end(false));

        let mut reopened = App::new();
        reopened.session_id = Some("s-1".to_owned());
        reopened.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![
                prose_entry(Role::User as i32, "hi", false),
                call_entry("t1", "lookup"),
                result_entry("t1", ToolOutcome::Ok as i32),
                prose_entry(Role::Assistant as i32, "answer", false),
            ],
        });

        assert_eq!(reopened.transcript, live.transcript);
    }

    #[test]
    fn unrecognized_and_missing_history_outcomes_read_unknown() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![
                call_entry("a", "alpha"),
                call_entry("b", "beta"),
                result_entry("b", 42),
            ],
        });

        assert_eq!(
            app.transcript,
            [
                Block::Tool {
                    call_id: "a".to_owned(),
                    name: "alpha".to_owned(),
                    args: String::new(),
                    outcome: Some("unknown"),
                },
                Block::Tool {
                    call_id: "b".to_owned(),
                    name: "beta".to_owned(),
                    args: String::new(),
                    outcome: Some("unknown"),
                },
            ]
        );
    }

    #[test]
    fn history_for_a_session_already_left_is_dropped() {
        let mut app = App::new();
        app.session_id = Some("second".to_owned());
        app.transcript = vec![Block::Note("loading".to_owned())];

        app.on_net(NetEvent::History {
            session_id: "first".to_owned(),
            entries: vec![prose_entry(Role::User as i32, "stale", false)],
        });

        assert_eq!(
            app.transcript,
            [Block::Note("loading".to_owned())],
            "the transcript we are actually waiting on is untouched"
        );
    }

    #[test]
    fn the_picker_row_zero_starts_a_new_session() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.transcript.push(Block::You("old".to_owned()));

        app.on_key(ctrl('p'));
        app.on_key(key(KeyCode::Enter));

        assert_eq!(app.session_id, None);
        assert_eq!(app.transcript, []);
    }

    #[test]
    fn the_picker_does_not_open_mid_stream() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_key(ctrl('p'));
        assert_eq!(app.picker, None);
        normal(&mut app, "s");
        assert_eq!(app.picker, None);
    }

    #[test]
    fn filter_narrows_the_picker_and_enter_opens_the_narrowed_selection() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![
            session_with("keep-a", "alpha topic", ""),
            session_with("skip", "beta topic", ""),
            session_with("keep-b", "", "another alpha mention"),
        ]));
        normal(&mut app, "s");
        assert_eq!(app.on_key(key(KeyCode::Char('/'))), None);
        typed(&mut app, "alpha");

        assert_eq!(
            app.picker_rows()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["keep-a", "keep-b"],
            "only sessions matching the query remain"
        );

        app.on_key(key(KeyCode::Down));
        app.on_key(key(KeyCode::Down));
        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(app.picker, None);
        assert_eq!(app.session_id.as_deref(), Some("keep-b"));
        assert_eq!(
            command,
            Some(Command::History {
                session_id: "keep-b".to_owned()
            }),
            "enter opens the row as numbered in the narrowed list"
        );
    }

    #[test]
    fn filter_esc_restores_the_full_list_and_the_draft() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![
            session_with("keep-a", "alpha topic", ""),
            session_with("skip", "beta topic", ""),
        ]));
        app.input = "draft reply".to_owned();
        app.cursor = app.input.len();

        app.on_key(ctrl('p'));
        app.on_key(key(KeyCode::Char('/')));
        typed(&mut app, "alpha");
        assert_eq!(app.picker_rows().len(), 1);

        assert_eq!(app.on_key(key(KeyCode::Esc)), None);

        assert!(
            !app.picker.as_ref().expect("picker still open").filtering,
            "esc exits filtering, not the picker"
        );
        assert_eq!(app.picker_rows().len(), 2, "the full list is back");
        assert_eq!(app.input, "draft reply", "the stashed draft comes back");
        assert_eq!(app.cursor, app.input.len());
    }

    #[test]
    fn ctrl_j_inserts_a_newline_in_insert_mode_and_submit_carries_it() {
        let mut app = App::new();
        typed(&mut app, "line one");
        assert_eq!(app.on_key(ctrl('j')), None);
        typed(&mut app, "line two");

        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(
            command,
            Some(Command::Send {
                session_id: None,
                content: "line one\nline two".to_owned()
            })
        );
    }

    #[test]
    fn ctrl_j_does_nothing_in_cmd_mode() {
        let mut app = App::new();
        normal(&mut app, ":review");
        assert_eq!(app.mode, Mode::Cmd);

        assert_eq!(app.on_key(ctrl('j')), None);

        assert_eq!(app.cmd, "review", "ctrl-j did not touch the command line");
    }

    #[test]
    fn ctrl_j_does_nothing_while_steering() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running)]);
        app.on_key(key(KeyCode::Char('s')));
        assert!(app.jobs.as_ref().expect("open").steering.is_some());

        assert_eq!(app.on_key(ctrl('j')), None);

        assert_eq!(app.input, "", "ctrl-j did not touch the steer input");
    }

    #[test]
    fn colon_help_opens_the_popup_and_q_closes_it() {
        let mut app = App::new();
        normal(&mut app, ":help");
        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(command, None);
        assert!(app.help);
        assert_eq!(app.mode, Mode::Normal);
        assert_eq!(app.last_error, None, ":help is a command, not E492");

        assert_eq!(app.on_key(key(KeyCode::Char('q'))), None);
        assert!(!app.help);
    }

    #[test]
    fn unknown_keys_in_the_help_popup_do_not_crash() {
        let mut app = App::new();
        normal(&mut app, ":help");
        app.on_key(key(KeyCode::Enter));

        assert_eq!(app.on_key(key(KeyCode::Char('z'))), None);
        assert_eq!(app.on_key(key(KeyCode::Enter)), None);
        assert!(app.help, "still open");

        assert_eq!(app.on_key(key(KeyCode::Esc)), None);
        assert!(!app.help, "esc closes it too");
    }

    #[test]
    fn ctrl_c_quits_from_any_mode() {
        let mut app = App::new();
        app.on_key(ctrl('c'));
        assert!(app.quit);
    }

    fn entry(id: &str, title: &str) -> ReviewEntry {
        ReviewEntry {
            id: id.to_owned(),
            kind: 4,
            namespace: "global".to_owned(),
            title: title.to_owned(),
            summary: "a summary".to_owned(),
            body: "the full body".to_owned(),
            superseded: false,
        }
    }

    fn reviewing(entries: Vec<ReviewEntry>) -> App {
        let mut app = App::new();
        normal(&mut app, ":review");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::ReviewItems(entries));
        app
    }

    #[test]
    fn colon_review_opens_the_pane_and_asks_for_the_last_week() {
        let mut app = App::new();
        normal(&mut app, ":review");
        let command = app.on_key(key(KeyCode::Enter));

        let Some(Command::ReviewList { since_micros }) = command else {
            panic!("expected ReviewList, got {command:?}");
        };
        let expected = chrono::Utc::now().timestamp_micros() - REVIEW_WINDOW_MICROS;
        assert!(
            (since_micros - expected).abs() < 60 * 1_000_000,
            "the window reaches a week back, got {since_micros}"
        );
        let review = app.review.as_ref().expect("the pane is open");
        assert!(!review.loaded, "nothing has been answered yet");
        assert_eq!(app.mode, Mode::Normal);
        assert_eq!(app.last_error, None, ":review is a command, not E492");
    }

    #[test]
    fn review_items_land_in_the_open_pane_and_nowhere_after_it_closed() {
        let mut app = reviewing(vec![entry("mr-1", "one")]);
        let review = app.review.as_ref().expect("open");
        assert!(review.loaded);
        assert_eq!(review.items, [entry("mr-1", "one")]);
        assert_eq!(review.selected, 0);

        app.on_key(key(KeyCode::Esc));
        assert_eq!(app.review, None);
        app.on_net(NetEvent::ReviewItems(vec![entry("mr-2", "two")]));
        assert_eq!(app.review, None);
    }

    #[test]
    fn j_and_k_move_the_selection_within_bounds() {
        let mut app = reviewing(vec![entry("mr-1", "one"), entry("mr-2", "two")]);

        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.review.as_ref().expect("open").selected, 1);
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(
            app.review.as_ref().expect("open").selected,
            1,
            "j stops at the last row"
        );
        app.on_key(key(KeyCode::Char('k')));
        assert_eq!(app.review.as_ref().expect("open").selected, 0);
        app.on_key(key(KeyCode::Char('k')));
        assert_eq!(app.review.as_ref().expect("open").selected, 0);
    }

    #[test]
    fn a_accepts_the_selected_record_and_it_leaves_the_list() {
        let mut app = reviewing(vec![entry("mr-1", "one"), entry("mr-2", "two")]);
        app.on_key(key(KeyCode::Char('j')));

        let command = app.on_key(key(KeyCode::Char('a')));

        assert_eq!(
            command,
            Some(Command::ReviewAccept {
                record_id: "mr-2".to_owned()
            })
        );
        let review = app.review.as_ref().expect("still open");
        assert_eq!(review.items, [entry("mr-1", "one")]);
        assert_eq!(review.selected, 0, "the selection is clamped to the list");
    }

    #[test]
    fn delete_takes_two_ds_and_anything_else_disarms() {
        let mut app = reviewing(vec![entry("mr-1", "one"), entry("mr-2", "two")]);

        assert_eq!(
            app.on_key(key(KeyCode::Char('d'))),
            None,
            "the first d arms"
        );
        assert!(app.review.as_ref().expect("open").pending_delete);

        app.on_key(key(KeyCode::Char('j')));
        assert!(!app.review.as_ref().expect("open").pending_delete);
        assert_eq!(app.on_key(key(KeyCode::Char('d'))), None);

        let command = app.on_key(key(KeyCode::Char('d')));
        assert_eq!(
            command,
            Some(Command::ReviewDelete {
                record_id: "mr-2".to_owned()
            })
        );
        let review = app.review.as_ref().expect("still open");
        assert_eq!(review.items, [entry("mr-1", "one")]);
        assert!(!review.pending_delete);
    }

    #[test]
    fn f_prefills_the_fix_instruction_and_closes_the_pane() {
        let mut app = reviewing(vec![entry("mr-1", "Old address")]);

        assert_eq!(
            app.on_key(key(KeyCode::Char('f'))),
            None,
            "fix sends nothing"
        );

        assert_eq!(app.review, None, "the pane closed");
        assert_eq!(app.input, "fix memory mr-1: Old address — ");
        assert_eq!(app.cursor, app.input.len(), "ready to finish the sentence");
        assert_eq!(app.mode, Mode::Insert);
    }

    #[test]
    fn q_closes_the_pane_and_verdict_keys_on_an_empty_pane_are_no_ops() {
        let mut app = reviewing(Vec::new());

        assert_eq!(app.on_key(key(KeyCode::Char('a'))), None);
        assert_eq!(app.on_key(key(KeyCode::Char('d'))), None);
        assert_eq!(
            app.on_key(key(KeyCode::Char('d'))),
            None,
            "nothing to delete"
        );
        assert_eq!(app.on_key(key(KeyCode::Char('f'))), None);
        assert!(app.review.is_some(), "an empty pane still shows its line");

        app.on_key(key(KeyCode::Char('q')));
        assert_eq!(app.review, None);
        assert!(!app.quit, "q closed the pane, not the app");
    }

    #[test]
    fn the_picker_does_not_open_under_the_review_pane() {
        let mut app = reviewing(vec![entry("mr-1", "one")]);
        app.on_key(ctrl('p'));
        assert_eq!(app.picker, None);
    }

    #[test]
    fn scrolling_moves_the_review_selection_when_the_pane_is_open() {
        let mut app = reviewing(vec![entry("mr-1", "one"), entry("mr-2", "two")]);

        app.on_scroll(false, PAGE);
        assert_eq!(
            app.review.as_ref().expect("open").selected,
            1,
            "one row per gesture, not one page"
        );
        app.on_scroll(true, PAGE);
        assert_eq!(app.review.as_ref().expect("open").selected, 0);
        assert_eq!(app.scroll_back, 0, "the transcript never moved");
    }

    fn job(session_id: &str, state: arc_proto::v1::job_info::State) -> JobInfo {
        JobInfo {
            session_id: session_id.to_owned(),
            role: arc_proto::v1::SessionRole::Executor as i32,
            project: "arc".to_owned(),
            state: state as i32,
            spent_tokens: 12,
            budget_tokens: 0,
            elapsed_seconds: 5,
            budget_seconds: 0,
            title: String::new(),
        }
    }

    fn jobsview(entries: Vec<JobInfo>) -> App {
        let mut app = App::new();
        normal(&mut app, ":jobs");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::JobItems(entries));
        app
    }

    #[test]
    fn colon_jobs_opens_the_pane_and_asks_for_the_list() {
        let mut app = App::new();
        normal(&mut app, ":jobs");
        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(command, Some(Command::ListJobs));
        let jobs = app.jobs.as_ref().expect("the pane is open");
        assert!(!jobs.loaded, "nothing has been answered yet");
        assert_eq!(app.mode, Mode::Normal);
        assert_eq!(app.last_error, None, ":jobs is a command, not E492");
    }

    #[test]
    fn job_items_land_in_the_open_pane_and_nowhere_after_it_closed() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-1", State::Running)]);
        let jobs = app.jobs.as_ref().expect("open");
        assert!(jobs.loaded);
        assert_eq!(jobs.items, [job("s-1", State::Running)]);
        assert_eq!(jobs.selected, 0);

        app.on_key(key(KeyCode::Esc));
        assert_eq!(app.jobs, None);
        app.on_net(NetEvent::JobItems(vec![job("s-2", State::Finished)]));
        assert_eq!(app.jobs, None);
    }

    #[test]
    fn j_and_k_move_the_job_selection_within_bounds() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![
            job("s-1", State::Running),
            job("s-2", State::Finished),
        ]);

        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.jobs.as_ref().expect("open").selected, 1);
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(
            app.jobs.as_ref().expect("open").selected,
            1,
            "j stops at the last row"
        );
        app.on_key(key(KeyCode::Char('k')));
        assert_eq!(app.jobs.as_ref().expect("open").selected, 0);
        app.on_key(key(KeyCode::Char('k')));
        assert_eq!(app.jobs.as_ref().expect("open").selected, 0);
    }

    #[test]
    fn r_asks_for_the_job_list_again_without_closing_the_pane() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-1", State::Running)]);

        let command = app.on_key(key(KeyCode::Char('r')));

        assert_eq!(command, Some(Command::ListJobs));
        assert!(app.jobs.is_some(), "the pane stays open while it refreshes");
    }

    #[test]
    fn enter_on_a_job_row_opens_its_child_session() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running), job("s-b", State::Running)]);
        app.on_key(key(KeyCode::Char('j')));

        let fetch = app.on_key(key(KeyCode::Enter));

        assert_eq!(app.jobs, None, "the popup closes");
        assert_eq!(app.session_id.as_deref(), Some("s-b"));
        assert_eq!(
            fetch,
            Some(Command::History {
                session_id: "s-b".to_owned()
            }),
            "the same open path a picker row takes"
        );
        assert_eq!(app.transcript, [Block::Note("loading".to_owned())]);
    }

    #[test]
    fn s_enters_the_steer_substate_and_typed_chars_land_in_the_input() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running)]);
        app.input = "draft reply".to_owned();
        app.cursor = app.input.len();

        assert_eq!(app.on_key(key(KeyCode::Char('s'))), None);
        assert_eq!(app.input, "", "the conversation draft is stashed away");
        assert_eq!(
            app.jobs.as_ref().expect("open").steering.as_deref(),
            Some("s-a")
        );

        typed(&mut app, "stop and check the tests");
        assert_eq!(app.input, "stop and check the tests");
        assert!(app.jobs.is_some(), "the popup stays open while steering");
    }

    #[test]
    fn esc_cancels_steering_and_restores_the_stashed_input() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running)]);
        app.input = "draft reply".to_owned();
        app.cursor = app.input.len();
        app.on_key(key(KeyCode::Char('s')));
        typed(&mut app, "abandoned");

        assert_eq!(app.on_key(key(KeyCode::Esc)), None);

        assert_eq!(app.input, "draft reply", "the stash comes back");
        assert_eq!(app.cursor, app.input.len());
        assert!(
            app.jobs.as_ref().expect("open").steering.is_none(),
            "back to browsing the list"
        );
    }

    #[test]
    fn submitting_a_steer_sends_to_the_jobs_session_not_the_open_one() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running), job("s-b", State::Running)]);
        app.session_id = Some("s-open".to_owned());
        app.on_key(key(KeyCode::Char('j')));
        app.on_key(key(KeyCode::Char('s')));
        typed(&mut app, "pause and summarize");

        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(
            command,
            Some(Command::Send {
                session_id: Some("s-b".to_owned()),
                content: "pause and summarize".to_owned()
            })
        );
        assert_eq!(
            app.session_id.as_deref(),
            Some("s-open"),
            "the open conversation never moved"
        );
        assert!(app.jobs.is_some(), "the popup stays open");
        assert!(app.jobs.as_ref().expect("open").steering.is_none());
    }

    #[test]
    fn the_confirmation_line_appears_after_submit_and_clears_on_the_next_key() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running)]);
        app.on_key(key(KeyCode::Char('s')));
        typed(&mut app, "go on");
        app.on_key(key(KeyCode::Enter));

        assert_eq!(
            app.jobs.as_ref().expect("open").confirmation.as_deref(),
            Some("steered s-a")
        );

        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.jobs.as_ref().expect("open").confirmation, None);
    }

    #[test]
    fn a_steer_accepted_and_end_do_not_touch_the_open_conversation() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running)]);
        app.session_id = Some("s-open".to_owned());
        app.transcript.push(Block::You("earlier".to_owned()));
        app.on_key(key(KeyCode::Char('s')));
        typed(&mut app, "go on");
        app.on_key(key(KeyCode::Enter));

        app.on_net(NetEvent::Accepted {
            session_id: "s-a".to_owned(),
        });
        let next = app.on_net(end(false));

        assert_eq!(next, None);
        assert_eq!(app.session_id.as_deref(), Some("s-open"));
        assert_eq!(app.transcript, [Block::You("earlier".to_owned())]);
        assert_eq!(app.status, Status::Idle);
    }

    #[test]
    fn q_closes_the_jobs_pane_and_an_empty_pane_still_shows_its_line() {
        let mut app = jobsview(Vec::new());
        assert!(app.jobs.is_some(), "an empty pane still shows its line");

        app.on_key(key(KeyCode::Char('q')));
        assert_eq!(app.jobs, None);
        assert!(!app.quit, "q closed the pane, not the app");
    }

    #[test]
    fn the_picker_does_not_open_under_the_jobs_pane() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-1", State::Running)]);
        app.on_key(ctrl('p'));
        assert_eq!(app.picker, None);
    }

    #[test]
    fn scrolling_moves_the_job_selection_when_the_pane_is_open() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![
            job("s-1", State::Running),
            job("s-2", State::Finished),
        ]);

        app.on_scroll(false, PAGE);
        assert_eq!(
            app.jobs.as_ref().expect("open").selected,
            1,
            "one row per gesture, not one page"
        );
        app.on_scroll(true, PAGE);
        assert_eq!(app.jobs.as_ref().expect("open").selected, 0);
        assert_eq!(app.scroll_back, 0, "the transcript never moved");
    }

    #[test]
    fn job_changed_populates_the_strip_and_a_second_push_replaces_not_appends() {
        use arc_proto::v1::job_info::State;

        let mut app = App::new();
        app.on_net(NetEvent::JobChanged(job("s-1", State::Running)));
        assert_eq!(app.ambient.len(), 1);
        assert_eq!(app.strip_job().map(|j| j.session_id.as_str()), Some("s-1"));

        let mut updated = job("s-1", State::Running);
        updated.spent_tokens = 99;
        app.on_net(NetEvent::JobChanged(updated));

        assert_eq!(
            app.ambient.len(),
            1,
            "the second push replaces, not appends"
        );
        assert_eq!(app.strip_job().map(|j| j.spent_tokens), Some(99));
    }

    #[test]
    fn the_strip_shows_the_most_recently_touched_running_job_and_ignores_terminal_ones() {
        use arc_proto::v1::job_info::State;

        let mut app = App::new();
        app.on_net(NetEvent::JobChanged(job("s-1", State::Running)));
        app.on_net(NetEvent::JobChanged(job("s-2", State::Finished)));
        assert_eq!(
            app.strip_job().map(|j| j.session_id.as_str()),
            Some("s-1"),
            "the finished job does not shadow the running one"
        );
        assert_eq!(app.running_job_count(), 1);

        app.on_net(NetEvent::JobChanged(job("s-3", State::Running)));
        assert_eq!(
            app.strip_job().map(|j| j.session_id.as_str()),
            Some("s-3"),
            "the newest running job leads"
        );
        assert_eq!(app.running_job_count(), 2);
    }

    #[test]
    fn no_running_jobs_means_no_strip_job() {
        use arc_proto::v1::job_info::State;

        let mut app = App::new();
        app.on_net(NetEvent::JobChanged(job("s-1", State::Finished)));
        assert_eq!(app.strip_job(), None);
        assert_eq!(app.running_job_count(), 0);
    }

    #[test]
    fn job_changed_patches_an_open_jobs_popup_row_in_place() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-1", State::Running), job("s-2", State::Running)]);
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.jobs.as_ref().expect("open").selected, 1);

        let mut updated = job("s-2", State::Finished);
        updated.spent_tokens = 42;
        app.on_net(NetEvent::JobChanged(updated.clone()));

        let jobs = app.jobs.as_ref().expect("open");
        assert_eq!(
            jobs.items,
            [job("s-1", State::Running), updated],
            "the row patched in place"
        );
        assert_eq!(jobs.selected, 1, "the selection stayed put");
    }

    #[test]
    fn job_changed_for_a_job_not_in_the_open_popup_only_updates_the_strip() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-1", State::Running)]);
        app.on_net(NetEvent::JobChanged(job("s-2", State::Running)));

        assert_eq!(
            app.jobs.as_ref().expect("open").items,
            [job("s-1", State::Running)],
            "the popup only shows what it fetched"
        );
        assert_eq!(app.ambient.len(), 1, "the strip data saw the push");
    }

    #[test]
    fn session_appended_for_the_open_session_refetches_once_per_burst() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());

        let first = app.on_net(NetEvent::SessionAppended {
            session_id: "s-1".to_owned(),
        });
        assert_eq!(
            first,
            Some(Command::History {
                session_id: "s-1".to_owned()
            })
        );

        let second = app.on_net(NetEvent::SessionAppended {
            session_id: "s-1".to_owned(),
        });
        assert_eq!(second, None, "a refetch is already in flight");

        let landed = app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![],
        });
        assert_eq!(landed, None);

        let third = app.on_net(NetEvent::SessionAppended {
            session_id: "s-1".to_owned(),
        });
        assert_eq!(
            third,
            Some(Command::History {
                session_id: "s-1".to_owned()
            }),
            "the history landed, so the next push refetches again"
        );
    }

    #[test]
    fn session_appended_for_another_session_is_a_no_op() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());

        let command = app.on_net(NetEvent::SessionAppended {
            session_id: "s-2".to_owned(),
        });
        assert_eq!(command, None);
        assert_eq!(app.transcript, [], "nothing else moved either");
    }

    #[test]
    fn session_appended_for_the_open_session_is_ignored_while_our_turn_streams() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.status = Status::Streaming;

        let command = app.on_net(NetEvent::SessionAppended {
            session_id: "s-1".to_owned(),
        });
        assert_eq!(
            command, None,
            "a push for our own in-flight turn is redundant"
        );
    }

    #[test]
    fn editing_moves_by_chars_not_bytes() {
        let mut app = App::new();
        typed(&mut app, "héllo");
        for _ in 0..3 {
            app.on_key(key(KeyCode::Left));
        }
        app.on_key(key(KeyCode::Backspace));
        assert_eq!(app.input, "hllo", "backspace removed the two-byte é");
    }

    #[test]
    fn a_system_sourced_message_becomes_a_system_block_not_yours() {
        let mut message = prose(Role::User as i32, "Job s-x finished.\nAll good.", false);
        message.source = Source::System as i32;

        let block = prose_block(message).expect("block");
        assert_eq!(
            block,
            Block::System("Job s-x finished.\nAll good.".to_owned())
        );
    }
}
