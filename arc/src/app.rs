use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use arc_core::projection::REVIEW_WINDOW_MICROS;
use arc_proto::v1::{
    HistoryEntry, HistoryMessage, JobInfo, ProjectInfo, Role, SessionInfo, SessionRole, Source,
    ToolOutcome, branch_marked, history_entry, job_info,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

pub const PAGE: usize = 10;

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
    ListProjects,
    CancelJob {
        session_id: String,
    },
    CancelTurn {
        session_id: String,
    },
    DropSteers {
        session_id: String,
    },
    CreateSession {
        role: SessionRole,
        project: String,
    },
    ForkSession {
        session_id: String,
        fork_point: u64,
    },
    MarkBranch {
        session_id: String,
        disposition: branch_marked::Disposition,
    },
    Yank(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetEvent {
    Sessions(Vec<SessionInfo>),
    History {
        session_id: String,
        entries: Vec<HistoryEntry>,
        parent_session: String,
        fork_point: u64,
        branches: Vec<(u64, String)>,
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
        step_capped: bool,
        grounding_json: String,
    },
    Failed {
        code: String,
        msg: String,
    },
    Disconnected {
        reason: String,
    },
    ReviewItems(Vec<ReviewEntry>),
    ReviewChanged(u32),
    JobItems(Vec<JobInfo>),
    ProjectItems(Vec<ProjectInfo>),
    SessionAppended {
        session_id: String,
    },
    JobReasoning {
        session_id: String,
        text: String,
    },
    JobChanged(JobInfo),
    SessionCreated {
        session_id: String,
    },
    SessionForked {
        session_id: String,
    },
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
    pub filtering: bool,
    pub show_all: bool,
    pub show_abandoned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projects {
    pub items: Vec<ProjectInfo>,
    pub selected: usize,
    pub loaded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Jobs {
    pub items: Vec<JobInfo>,
    pub selected: usize,
    pub loaded: bool,
    pub steering: Option<String>,
    pub confirmation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Search {
    pub query: String,
    pub matches: Vec<usize>,
    pub current: usize,
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
    Handback {
        subject: String,
        body: String,
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
    StepCapped,
    Sources(Vec<(String, String)>),
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
    Visual,
}

pub struct App {
    pub transcript: Vec<Block>,
    /// Parallel to `transcript`: the history seq a block came from, `None`
    /// for one built live — forking reads from history only.
    seqs: Vec<Option<u64>>,
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
    pub projects: Option<Projects>,
    pub help: bool,
    pub help_scroll: usize,
    pub search: Option<Search>,
    pub searching: bool,
    pub status: Status,
    pub last_error: Option<String>,
    pub yank_note: Option<String>,
    pub queued: VecDeque<String>,
    thinking_since: Option<Instant>,
    turn_started: Option<Instant>,
    streamed_chars: usize,
    pub scroll_back: usize,
    pub quit: bool,
    pub ambient: Vec<JobInfo>,
    strip_since: Instant,
    refetch_in_flight: bool,
    steer_stash: Option<String>,
    previous_session: Option<String>,
    picker_filter_stash: Option<String>,
    search_stash: Option<String>,
    steer_turn_pending: bool,
    visual_anchor: usize,
    visual_point: bool,
    visual_boundary: usize,
    visual_at_cmd: Option<usize>,
    visual_rewind: bool,
    pending_rewind_text: Option<String>,
    session_meta: HashMap<String, (SessionRole, String, Source)>,
    pending_code: Option<(SessionRole, String)>,
    pending_first: Option<String>,
    pub review_pending: u32,
}

impl App {
    pub fn new() -> Self {
        Self {
            transcript: Vec::new(),
            seqs: Vec::new(),
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
            projects: None,
            help: false,
            help_scroll: 0,
            search: None,
            searching: false,
            status: Status::Idle,
            last_error: None,
            yank_note: None,
            queued: VecDeque::new(),
            thinking_since: None,
            turn_started: None,
            streamed_chars: 0,
            scroll_back: 0,
            quit: false,
            ambient: Vec::new(),
            strip_since: Instant::now(),
            refetch_in_flight: false,
            steer_stash: None,
            previous_session: None,
            picker_filter_stash: None,
            search_stash: None,
            steer_turn_pending: false,
            visual_anchor: 0,
            visual_point: false,
            visual_boundary: 0,
            visual_at_cmd: None,
            visual_rewind: false,
            pending_rewind_text: None,
            session_meta: HashMap::new(),
            pending_code: None,
            pending_first: None,
            review_pending: 0,
        }
    }

    fn push_block(&mut self, block: Block) {
        self.transcript.push(block);
        self.seqs.push(None);
    }

    fn pop_block(&mut self) -> Option<Block> {
        self.seqs.pop();
        self.transcript.pop()
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
        self.yank_note = None;
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
        if self.projects.is_some() {
            return self.on_projects_key(key.code);
        }
        if self.picker.is_some() {
            return self.on_picker_key(key.code);
        }
        if self.searching {
            return self.on_search_key(key.code);
        }
        match self.mode {
            Mode::Insert => self.on_insert(key.code),
            Mode::Normal => self.on_normal(key.code),
            Mode::Cmd => self.on_cmd(key.code),
            Mode::Visual => self.on_visual(key.code),
        }
    }

    fn on_control(&mut self, code: KeyCode) -> Option<Command> {
        match code {
            KeyCode::Char('c') => self.quit = true,
            KeyCode::Char('u') => self.on_scroll(true, PAGE),
            KeyCode::Char('d') => self.on_scroll(false, PAGE),
            KeyCode::Char('o') => self.toggle_thought(),
            KeyCode::Char('t') if self.status != Status::Streaming => {
                return self.back_session();
            }
            KeyCode::Char('n') if self.searching => self.search_live_step(true),
            KeyCode::Char('p') if self.searching => self.search_live_step(false),
            KeyCode::Char('n') if self.picker.is_some() => self.move_picker_selection(false),
            KeyCode::Char('p') if self.picker.is_some() => self.move_picker_selection(true),
            KeyCode::Char('p') => return self.open_picker(),
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
            KeyCode::Esc if self.status == Status::Streaming => return self.cancel_turn(),
            KeyCode::Esc if self.search.is_some() => self.search = None,
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
            KeyCode::Char('/') => self.start_search(),
            KeyCode::Char('n') => self.search_next(true),
            KeyCode::Char('N') => self.search_next(false),
            KeyCode::Char('s') => return self.open_picker(),
            KeyCode::Char('?') => self.help = true,
            KeyCode::Char('J') => return Some(self.open_jobs()),
            KeyCode::Char('M') => return Some(self.open_review()),
            KeyCode::Char('C') => return Some(self.open_projects()),
            KeyCode::Char('y') if self.status != Status::Streaming => {
                return self.yank_last_reply();
            }
            KeyCode::Char('V') if self.status != Status::Streaming => self.enter_visual(false),
            KeyCode::Char('v') if self.status != Status::Streaming => self.enter_visual(true),
            KeyCode::Char('R') if self.status != Status::Streaming => self.enter_rewind(),
            KeyCode::Char('Y') if self.status != Status::Streaming => return self.yank_all(),
            KeyCode::Char(':') => {
                self.cmd.clear();
                self.mode = Mode::Cmd;
                self.visual_at_cmd = None;
            }
            _ => {}
        }
        None
    }

    fn yank_last_reply(&mut self) -> Option<Command> {
        let reply = self.transcript.iter().rev().find_map(|block| match block {
            Block::Arc { text, .. } => Some(text.clone()),
            _ => None,
        });
        if let Some(text) = reply {
            self.yank_note = Some("yanked".to_owned());
            Some(Command::Yank(text))
        } else {
            self.yank_note = Some("nothing to yank".to_owned());
            None
        }
    }

    fn enter_visual(&mut self, point: bool) {
        let Some(last) = self.transcript.len().checked_sub(1) else {
            return;
        };
        self.pending = None;
        self.visual_anchor = last;
        self.visual_boundary = last;
        self.visual_point = point;
        self.visual_rewind = false;
        self.mode = Mode::Visual;
    }

    fn enter_rewind(&mut self) {
        let Some(last_you) = self
            .transcript
            .iter()
            .rposition(|b| matches!(b, Block::You(_)))
        else {
            return;
        };
        self.pending = None;
        self.visual_anchor = last_you;
        self.visual_boundary = last_you;
        self.visual_point = true;
        self.visual_rewind = true;
        self.mode = Mode::Visual;
    }

    fn follow_point(&mut self) {
        if self.visual_point {
            self.visual_anchor = self.visual_boundary;
        }
    }

    fn is_visual_stop(&self, block: &Block) -> bool {
        if self.visual_rewind {
            matches!(block, Block::You(_))
        } else {
            matches!(block, Block::You(_) | Block::Arc { .. })
        }
    }

    fn step_point(&mut self, up: bool) {
        let len = self.transcript.len();
        let mut at = self.visual_boundary;
        loop {
            let next = if up {
                at.checked_sub(1)
            } else {
                at.checked_add(1)
            };
            let Some(next) = next else {
                return;
            };
            if next >= len {
                return;
            }
            at = next;
            if self.is_visual_stop(&self.transcript[at]) {
                self.visual_boundary = at;
                self.follow_point();
                return;
            }
        }
    }

    fn jump_point(&mut self, to_end: bool) {
        let found = if to_end {
            self.transcript.iter().rposition(|b| self.is_visual_stop(b))
        } else {
            self.transcript.iter().position(|b| self.is_visual_stop(b))
        };
        if let Some(at) = found {
            self.visual_boundary = at;
            self.follow_point();
        }
    }

    fn on_visual(&mut self, code: KeyCode) -> Option<Command> {
        if let Some(pending) = self.pending.take() {
            if pending == 'g' && code == KeyCode::Char('g') {
                if self.visual_point {
                    self.jump_point(false);
                } else {
                    self.visual_boundary = 0;
                }
            }
            return None;
        }
        match code {
            KeyCode::Esc => self.mode = Mode::Normal,
            KeyCode::Char('j') => {
                if self.visual_point {
                    self.step_point(false);
                } else {
                    self.visual_boundary = (self.visual_boundary + 1).min(self.visual_anchor);
                }
            }
            KeyCode::Char('k') => {
                if self.visual_point {
                    self.step_point(true);
                } else {
                    self.visual_boundary = self.visual_boundary.saturating_sub(1);
                }
            }
            KeyCode::Char('G') => {
                if self.visual_point {
                    self.jump_point(true);
                } else {
                    self.visual_boundary = self.visual_anchor;
                }
            }
            KeyCode::Char('g') => self.pending = Some('g'),
            KeyCode::Char('y') => return self.yank_visual(),
            KeyCode::Char('f') => return self.fork_selected_visual(),
            KeyCode::Enter if self.visual_rewind => return self.rewind_fork(),
            // `visual_boundary()` reads `mode`, which Enter flips to Normal
            // before the typed command runs — stash the selection now
            KeyCode::Char(':') => {
                self.visual_at_cmd = self.visual_boundary();
                self.cmd.clear();
                self.mode = Mode::Cmd;
            }
            _ => {}
        }
        None
    }

    pub fn visual_range(&self) -> Option<(usize, usize)> {
        if self.mode != Mode::Visual {
            return None;
        }
        let last = self.transcript.len().checked_sub(1)?;
        let anchor = self.visual_anchor.min(last);
        let boundary = self.visual_boundary.min(last);
        Some((anchor.min(boundary), anchor.max(boundary)))
    }

    pub fn visual_boundary(&self) -> Option<usize> {
        if self.mode != Mode::Visual || self.transcript.is_empty() {
            return None;
        }
        Some(self.visual_boundary.min(self.transcript.len() - 1))
    }

    fn yank_visual(&mut self) -> Option<Command> {
        let range = self.visual_range();
        self.mode = Mode::Normal;
        let (lo, hi) = range?;
        let text = format_yank(&self.transcript[lo..=hi]);
        self.finish_yank(text)
    }

    fn yank_all(&mut self) -> Option<Command> {
        let text = format_yank(&self.transcript);
        self.finish_yank(text)
    }

    fn finish_yank(&mut self, text: Option<String>) -> Option<Command> {
        if let Some(text) = text {
            self.yank_note = Some("yanked".to_owned());
            Some(Command::Yank(text))
        } else {
            self.yank_note = Some("nothing to yank".to_owned());
            None
        }
    }

    fn start_search(&mut self) {
        self.search_stash = Some(std::mem::take(&mut self.input));
        self.cursor = 0;
        self.searching = true;
    }

    fn on_search_key(&mut self, code: KeyCode) -> Option<Command> {
        match code {
            KeyCode::Esc => self.cancel_search(),
            KeyCode::Enter => return self.confirm_search(),
            _ => self.edit_input(code),
        }
        None
    }

    fn cancel_search(&mut self) {
        self.searching = false;
        self.input = self.search_stash.take().unwrap_or_default();
        self.cursor = self.input.len();
    }

    fn confirm_search(&mut self) -> Option<Command> {
        self.searching = false;
        let query = std::mem::take(&mut self.input);
        self.input = self.search_stash.take().unwrap_or_default();
        self.cursor = self.input.len();
        if let Some(search) = &self.search {
            if search.query == query {
                self.yank_note = Some(format!(
                    "match {}/{}",
                    search.current + 1,
                    search.matches.len()
                ));
                return None;
            }
        }
        let matches = self.search_matches(&query);
        if matches.is_empty() {
            self.search = None;
            self.yank_note = Some("no match".to_owned());
            return None;
        }
        self.yank_note = Some(format!("match 1/{}", matches.len()));
        self.search = Some(Search {
            query,
            matches,
            current: 0,
        });
        None
    }

    // the yank text is the searchable surface: chrome never matches
    fn search_live_step(&mut self, older: bool) {
        let query = self.input.clone();
        if query.is_empty() {
            return;
        }
        let stale = self.search.as_ref().is_none_or(|s| s.query != query);
        if stale {
            let matches = self.search_matches(&query);
            if matches.is_empty() {
                self.search = None;
                self.yank_note = Some("no match".to_owned());
                return;
            }
            self.yank_note = Some(format!("match 1/{}", matches.len()));
            self.search = Some(Search {
                query,
                matches,
                current: 0,
            });
            return;
        }
        self.search_next(older);
    }

    fn search_matches(&self, query: &str) -> Vec<usize> {
        let needle = query.to_lowercase();
        self.transcript
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, block)| {
                block_yank_text(block).is_some_and(|text| text.to_lowercase().contains(&needle))
            })
            .map(|(index, _)| index)
            .collect()
    }

    fn search_next(&mut self, older: bool) {
        let Some(search) = self.search.as_mut() else {
            return;
        };
        search.current = if older {
            (search.current + 1).min(search.matches.len() - 1)
        } else {
            search.current.saturating_sub(1)
        };
        self.yank_note = Some(format!(
            "match {}/{}",
            search.current + 1,
            search.matches.len()
        ));
    }

    pub fn search_block(&self) -> Option<usize> {
        let search = self.search.as_ref()?;
        let block = *search.matches.get(search.current)?;
        (block < self.transcript.len()).then_some(block)
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
                let cmd = std::mem::take(&mut self.cmd);
                match cmd.as_str() {
                    "q" | "q!" | "qa" | "quit" => self.quit = true,
                    "review" => return Some(self.open_review()),
                    "jobs" => return Some(self.open_jobs()),
                    "help" => self.help = true,
                    "fork" => return self.fork_selected(),
                    cmd => match cmd.strip_prefix("code ") {
                        Some(project) => return self.open_code(project.trim()),
                        None => self.last_error = Some("E492".to_owned()),
                    },
                }
            }
            _ => {}
        }
        None
    }

    fn open_code(&mut self, project: &str) -> Option<Command> {
        if project.is_empty() {
            self.last_error = Some("E492".to_owned());
            return None;
        }
        let command = self.start_session(None);
        self.pending_code = Some((SessionRole::Executor, project.to_owned()));
        command
    }

    fn fork_selected(&mut self) -> Option<Command> {
        let Some(index) = self.visual_at_cmd.take() else {
            self.last_error = Some("fork: select a message in visual mode first".to_owned());
            return None;
        };
        let Some(session_id) = self.session_id.clone() else {
            self.last_error = Some("fork: no open session".to_owned());
            return None;
        };
        let is_message = matches!(
            self.transcript.get(index),
            Some(Block::You(_) | Block::Arc { .. })
        );
        if let (true, Some(fork_point)) = (is_message, self.seqs.get(index).copied().flatten()) {
            Some(Command::ForkSession {
                session_id,
                fork_point,
            })
        } else {
            self.last_error =
                Some("fork: select a sent message, not a tool or live block".to_owned());
            None
        }
    }

    fn fork_selected_visual(&mut self) -> Option<Command> {
        self.visual_at_cmd = self.visual_boundary();
        self.mode = Mode::Normal;
        self.fork_selected()
    }

    fn rewind_fork(&mut self) -> Option<Command> {
        let index = self.visual_boundary;
        self.mode = Mode::Normal;
        let Some(Block::You(text)) = self.transcript.get(index) else {
            self.last_error = Some("rewind: no message selected".to_owned());
            return None;
        };
        let text = text.clone();
        let Some(session_id) = self.session_id.clone() else {
            self.last_error = Some("rewind: no open session".to_owned());
            return None;
        };
        let preceding = self.transcript[..index]
            .iter()
            .rposition(|block| matches!(block, Block::You(_) | Block::Arc { .. }));
        let Some(fork_point) = preceding.and_then(|at| self.seqs.get(at).copied().flatten()) else {
            self.last_error = Some("rewind: no earlier message to fork before".to_owned());
            return None;
        };
        self.pending_rewind_text = Some(text);
        Some(Command::ForkSession {
            session_id,
            fork_point,
        })
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
        match code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.help = false;
                self.help_scroll = 0;
            }
            KeyCode::Char('j') | KeyCode::Down => {
                self.help_scroll = self.help_scroll.saturating_add(1);
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.help_scroll = self.help_scroll.saturating_sub(1);
            }
            KeyCode::Char('G') => self.help_scroll = usize::MAX,
            KeyCode::Char('g') => self.help_scroll = 0,
            _ => {}
        }
        None
    }

    fn open_projects(&mut self) -> Command {
        self.projects = Some(Projects {
            items: Vec::new(),
            selected: 0,
            loaded: false,
        });
        Command::ListProjects
    }

    fn on_projects_key(&mut self, code: KeyCode) -> Option<Command> {
        let projects = self.projects.as_mut().expect("projects is open");
        let last = projects.items.len().saturating_sub(1);
        match code {
            KeyCode::Esc | KeyCode::Char('q') => self.projects = None,
            KeyCode::Up | KeyCode::Char('k') => {
                projects.selected = projects.selected.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                projects.selected = (projects.selected + 1).min(last);
            }
            KeyCode::Enter => {
                let chosen = projects
                    .items
                    .get(projects.selected)
                    .map(|p| p.name.clone());
                if let Some(name) = chosen {
                    self.projects = None;
                    return self.open_code(&name);
                }
            }
            _ => {}
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
            KeyCode::Char('x') => return self.cancel_selected_job(),
            KeyCode::Char('d') => return self.drop_selected_steers(),
            _ => {}
        }
        None
    }

    fn cancel_selected_job(&mut self) -> Option<Command> {
        let jobs = self.jobs.as_mut().expect("jobs is open");
        let job = jobs.items.get(jobs.selected)?;
        if !is_running(job) {
            jobs.confirmation = Some("not running".to_owned());
            return None;
        }
        let session_id = job.session_id.clone();
        jobs.confirmation = Some(format!("cancelled {}", short_id(&session_id)));
        Some(Command::CancelJob { session_id })
    }

    fn drop_selected_steers(&mut self) -> Option<Command> {
        let jobs = self.jobs.as_mut().expect("jobs is open");
        let job = jobs.items.get(jobs.selected)?;
        if !is_running(job) || job.queued_steers == 0 {
            return None;
        }
        let session_id = job.session_id.clone();
        jobs.confirmation = Some(format!("dropped {}", job.queued_steers));
        Some(Command::DropSteers { session_id })
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
            KeyCode::Char('a' | ' ') => self.toggle_picker_show_all(),
            KeyCode::Char('x') => self.toggle_picker_show_abandoned(),
            KeyCode::Char('m') => {
                return self.mark_selected_branch(selected, branch_marked::Disposition::Real);
            }
            KeyCode::Char('X') => {
                return self.mark_selected_branch(selected, branch_marked::Disposition::Abandoned);
            }
            KeyCode::Enter => {
                let chosen = self.picker_session(selected).map(|s| s.id.clone());
                self.picker = None;
                return self.start_session(chosen);
            }
            _ => {}
        }
        None
    }

    fn mark_selected_branch(
        &mut self,
        row: usize,
        disposition: branch_marked::Disposition,
    ) -> Option<Command> {
        let session = self.picker_session(row)?;
        if session.parent_session.is_empty() {
            self.last_error = Some("mark: only a branch has a disposition".to_owned());
            return None;
        }
        Some(Command::MarkBranch {
            session_id: session.id.clone(),
            disposition,
        })
    }

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

    fn toggle_picker_show_abandoned(&mut self) {
        if let Some(picker) = self.picker.as_mut() {
            picker.show_abandoned = !picker.show_abandoned;
            picker.selected = 0;
        }
    }

    fn toggle_picker_show_all(&mut self) {
        let picker = self.picker.as_mut().expect("picker is open");
        picker.show_all = !picker.show_all;
        picker.selected = 0;
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

    // refreshes on every open: a branch forked seconds ago must be in the tree
    fn open_picker(&mut self) -> Option<Command> {
        if self.status != Status::Streaming && self.review.is_none() && self.jobs.is_none() {
            self.picker = Some(Picker {
                selected: 0,
                filtering: false,
                show_all: false,
                show_abandoned: false,
            });
            return Some(Command::List);
        }
        None
    }

    pub fn picker_session(&self, row: usize) -> Option<&SessionInfo> {
        row.checked_sub(1)
            .and_then(|i| self.picker_rows().get(i).copied())
    }

    pub fn picker_rows(&self) -> Vec<&SessionInfo> {
        self.picker_tree().into_iter().map(|(s, _)| s).collect()
    }

    /// `picker_rows`, paired with the lineage annotation: the parent's id
    /// prefix for a branch, `None` for a root. Position is pure recency —
    /// the freshest work is the first row; lineage is information, not
    /// hierarchy (decided 2026-08-29, after nesting made the dead parent
    /// the click target).
    pub fn picker_tree(&self) -> Vec<(&SessionInfo, Option<String>)> {
        self.picker_candidates()
            .into_iter()
            .map(|session| {
                let parent = (!session.parent_session.is_empty())
                    .then(|| session.parent_session.chars().take(8).collect());
                (session, parent)
            })
            .collect()
    }

    fn picker_candidates(&self) -> Vec<&SessionInfo> {
        let show_all = self.picker.as_ref().is_some_and(|picker| picker.show_all);
        let show_abandoned = self
            .picker
            .as_ref()
            .is_some_and(|picker| picker.show_abandoned);
        let order: Vec<&SessionInfo> = self
            .by_recency()
            .into_iter()
            .filter(|session| {
                !session.title.is_empty()
                    || !session.preview.is_empty()
                    || session.last_at.is_some()
            })
            .filter(|session| show_all || !is_job_session(session))
            .filter(|session| show_abandoned || !is_abandoned(session))
            .collect();
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

    pub fn strip_job(&self) -> Option<&JobInfo> {
        let parent = self.session_id.as_deref()?;
        self.ambient
            .iter()
            .rev()
            .find(|job| is_running(job) && job.parent_session == parent)
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

    /// The job's idle seconds, ticked forward locally since the last push,
    /// the same clock the elapsed readout shares.
    pub fn strip_idle_seconds(&self, job: &JobInfo) -> u64 {
        u64::from(job.idle_seconds) + self.strip_since.elapsed().as_secs()
    }

    pub fn turn_elapsed_seconds(&self) -> Option<u64> {
        self.turn_started.map(|since| since.elapsed().as_secs())
    }

    pub fn streamed_tokens_estimate(&self) -> u64 {
        (self.streamed_chars / 4) as u64
    }

    pub fn by_recency(&self) -> Vec<&SessionInfo> {
        let mut order: Vec<&SessionInfo> = self.sessions.iter().collect();
        order.sort_by(|a, b| activity(b).cmp(&activity(a)).then_with(|| a.id.cmp(&b.id)));
        order
    }

    fn record_session_meta(&mut self, session_id: &str, role: i32, project: &str, source: i32) {
        let role = SessionRole::try_from(role).unwrap_or(SessionRole::Unspecified);
        let source = Source::try_from(source).unwrap_or(Source::Unspecified);
        self.session_meta
            .insert(session_id.to_owned(), (role, project.to_owned(), source));
    }

    pub fn open_door_label(&self) -> Option<String> {
        if self.session_id.is_none() {
            if let Some((SessionRole::Executor, project)) = &self.pending_code {
                return Some(format!("code/{project}"));
            }
        }
        let (role, project, source) = self
            .session_id
            .as_deref()
            .and_then(|id| self.session_meta.get(id))?;
        match (*source, *role) {
            (Source::Model, _) => Some(format!("job/{project}")),
            (Source::User, SessionRole::Executor) => Some(format!("code/{project}")),
            _ => None,
        }
    }

    fn back_session(&mut self) -> Option<Command> {
        let previous = self.previous_session.take()?;
        self.start_session(Some(previous))
    }

    fn start_session(&mut self, session_id: Option<String>) -> Option<Command> {
        self.pending_code = None;
        self.pending_first = None;
        if self.session_id != session_id {
            self.previous_session = self.session_id.clone();
        }
        self.session_id.clone_from(&session_id);
        self.transcript.clear();
        self.seqs.clear();
        self.scroll_back = 0;
        self.search = None;
        self.last_error = None;
        self.refetch_in_flight = false;
        let session_id = session_id?;
        self.push_block(Block::Note("loading".to_owned()));
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
        self.push_block(Block::You(content.clone()));
        if self.status == Status::Streaming {
            self.queued.push_back(content);
            return None;
        }
        if self.session_id.is_none() {
            if let Some((role, project)) = self.pending_code.clone() {
                self.status = Status::Streaming;
                self.pending_first = Some(content);
                return Some(Command::CreateSession { role, project });
            }
        }
        Some(self.send(content))
    }

    fn cancel_turn(&mut self) -> Option<Command> {
        let session_id = self.session_id.clone()?;
        Some(Command::CancelTurn { session_id })
    }

    fn send(&mut self, content: String) -> Command {
        self.status = Status::Streaming;
        self.last_error = None;
        self.turn_started = Some(Instant::now());
        self.streamed_chars = 0;
        Command::Send {
            session_id: self.session_id.clone(),
            content,
        }
    }

    pub fn on_net(&mut self, event: NetEvent) -> Option<Command> {
        // any live event proves the socket is back; only another disconnect says otherwise
        if self.status == Status::Disconnected && !matches!(event, NetEvent::Disconnected { .. }) {
            self.status = Status::Idle;
        }
        match event {
            NetEvent::Sessions(sessions) => {
                for session in &sessions {
                    self.record_session_meta(
                        &session.id,
                        session.role,
                        &session.project,
                        session.source,
                    );
                }
                self.sessions = sessions;
                None
            }
            NetEvent::History {
                session_id,
                entries,
                parent_session,
                fork_point,
                branches,
            } => {
                if self.session_id.as_deref() == Some(session_id.as_str()) {
                    let (rebuilt, rebuilt_seqs) =
                        history_blocks(entries, &parent_session, fork_point, &branches);
                    // append-only rebuilds keep the selection valid
                    let appended_only = rebuilt.len() >= self.transcript.len()
                        && rebuilt.starts_with(&self.transcript);
                    let kept_search =
                        appended_only
                            .then_some(self.search.as_ref())
                            .flatten()
                            .map(|search| {
                                (
                                    search.query.clone(),
                                    search.matches.get(search.current).copied(),
                                )
                            });
                    self.transcript = rebuilt;
                    self.seqs = rebuilt_seqs;
                    self.scroll_back = 0;
                    self.refetch_in_flight = false;
                    self.search = kept_search.and_then(|(query, selected_block)| {
                        let matches = self.search_matches(&query);
                        if matches.is_empty() {
                            return None;
                        }
                        let current = selected_block
                            .and_then(|block| matches.iter().position(|&m| m == block))
                            .unwrap_or(0);
                        Some(Search {
                            query,
                            matches,
                            current,
                        })
                    });
                    if self.mode == Mode::Visual && !appended_only {
                        self.mode = Mode::Normal;
                    }
                }
                None
            }
            NetEvent::Accepted { session_id } => {
                if self.steer_turn_pending {
                    return None;
                }
                let created = self.session_id.is_none();
                self.session_id = Some(session_id);
                self.push_block(Block::Arc {
                    text: String::new(),
                    partial: false,
                });
                created.then_some(Command::List)
            }
            NetEvent::Delta(text) => {
                self.finalize_thinking();
                self.streamed_chars += text.chars().count();
                if let Some(Block::Arc { text: reply, .. }) = self.transcript.last_mut() {
                    reply.push_str(&text);
                } else {
                    self.push_block(Block::Arc {
                        text,
                        partial: false,
                    });
                }
                None
            }
            NetEvent::Reasoning(text) => {
                self.streamed_chars += text.chars().count();
                self.stream_thought(text);
                None
            }
            // a watched job's thinking, pushed over the subscription; only
            // the session on screen renders it, and never over an own turn
            NetEvent::JobReasoning { session_id, text } => {
                let open = self.session_id.as_deref() == Some(session_id.as_str());
                if open && self.status != Status::Streaming {
                    self.stream_thought(text);
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
                self.push_block(Block::Tool {
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
                step_capped,
                grounding_json,
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
                        self.push_block(Block::Cost {
                            input_tokens,
                            output_tokens,
                            seconds: elapsed.as_secs_f32(),
                        });
                    }
                }
                if step_capped {
                    self.push_block(Block::StepCapped);
                }
                let sources = grounding_sources(&grounding_json);
                if !sources.is_empty() {
                    self.push_block(Block::Sources(sources));
                }
                self.turn_over(Status::Idle)
            }
            NetEvent::Failed { code, msg } => {
                self.steer_turn_pending = false;
                self.pending_first = None;
                self.pending_rewind_text = None;
                self.finalize_thinking();
                self.pop_empty_reply();
                self.turn_started = None;
                self.last_error = Some(code.clone());
                self.push_block(Block::Fault { code, msg });
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
            NetEvent::ReviewChanged(pending) => {
                self.review_pending = pending;
                None
            }
            NetEvent::ProjectItems(items) => {
                if let Some(projects) = self.projects.as_mut() {
                    projects.selected = 0;
                    projects.items = items;
                    projects.loaded = true;
                }
                None
            }
            NetEvent::JobItems(items) => {
                for job in &items {
                    self.record_session_meta(
                        &job.session_id,
                        job.role,
                        &job.project,
                        Source::Model as i32,
                    );
                }
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
                self.record_session_meta(
                    &job.session_id,
                    job.role,
                    &job.project,
                    Source::Model as i32,
                );
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
            NetEvent::SessionCreated { session_id } => {
                if let Some((role, project)) = self.pending_code.take() {
                    self.session_meta
                        .insert(session_id.clone(), (role, project, Source::User));
                }
                self.session_id = Some(session_id);
                let first = self.pending_first.take()?;
                Some(self.send(first))
            }
            NetEvent::SessionForked { session_id } => {
                let command = self.start_session(Some(session_id));
                if let Some(text) = self.pending_rewind_text.take() {
                    self.input = text;
                    self.cursor = self.input.len();
                    self.mode = Mode::Insert;
                }
                command
            }
            NetEvent::Disconnected { reason } => {
                self.turn_started = None;
                self.last_error = Some("disconnected".to_owned());
                self.push_block(Block::Fault {
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

    fn stream_thought(&mut self, text: String) {
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
            self.push_block(Block::Thought {
                text,
                seconds: 1,
                done: false,
                open: false,
            });
        }
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
        let any_open = self.transcript.iter().any(|block| {
            matches!(
                block,
                Block::Thought { open: true, .. } | Block::Handback { open: true, .. }
            )
        });
        for block in &mut self.transcript {
            match block {
                Block::Thought { open, .. } | Block::Handback { open, .. } => {
                    *open = !any_open;
                }
                _ => {}
            }
        }
    }

    fn pop_empty_reply(&mut self) {
        if matches!(self.transcript.last(), Some(Block::Arc { text, .. }) if text.is_empty()) {
            self.pop_block();
            // a shrunk transcript can dangle a selection
            if self.mode == Mode::Visual {
                self.mode = Mode::Normal;
            }
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

/// A dispatched child, kept behind the picker's show-all toggle; a root
/// conversation — including a `:code` session — always lists. Keyed on the
/// recorded creation source (row 9.5), correct for all history unlike
/// `dispatched_by` alone; UNSPECIFIED only exists in a stale index and
/// fails toward hiding it as noise.
pub fn is_job_session(session: &SessionInfo) -> bool {
    session.source != Source::User as i32
}

fn is_abandoned(session: &SessionInfo) -> bool {
    session.disposition == arc_proto::v1::branch_marked::Disposition::Abandoned as i32
}

fn short_id(id: &str) -> &str {
    &id[id.len().saturating_sub(8)..]
}

/// The first line of a handback reads `Job {id} finished.` or
/// `Job {id} stopped: {reason}.`; what follows is the child's summary.
fn handback_parts(content: &str) -> Option<(String, String)> {
    let mut lines = content.splitn(2, '\n');
    let head = lines.next()?.trim_end();
    let rest = lines.next()?;
    if !handback_subject(head) {
        return None;
    }
    Some((head.to_owned(), rest.to_owned()))
}

/// Matches `Job {uuid} finished.` or `Job {uuid} stopped: {reason}.` — the
/// exact shape `record_handback` writes. Anything looser would fold prose.
fn handback_subject(line: &str) -> bool {
    let Some(rest) = line.strip_prefix("Job ") else {
        return false;
    };
    if let Some(id) = rest.strip_suffix(" finished.") {
        return is_uuid_like(id);
    }
    match rest.strip_suffix('.') {
        None => false,
        Some(rest) => match rest.split_once(" stopped: ") {
            Some((id, reason)) => is_uuid_like(id) && !reason.is_empty(),
            None => false,
        },
    }
}

/// A session id: hex digits and dashes, 32–36 chars (`record_handback`
/// embeds a bare uuid). Tight enough that prose never matches.
fn is_uuid_like(text: &str) -> bool {
    (32..=36).contains(&text.len()) && text.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
}

fn format_yank(blocks: &[Block]) -> Option<String> {
    let parts: Vec<String> = blocks.iter().filter_map(block_yank_text).collect();
    (!parts.is_empty()).then(|| parts.join("\n\n"))
}

fn block_yank_text(block: &Block) -> Option<String> {
    match block {
        Block::You(text) => Some(format!("you: {text}")),
        Block::Arc { text, .. } => Some(format!("arc: {text}")),
        Block::System(text) => Some(format!("system: {text}")),
        Block::Thought { .. }
        | Block::Handback { .. }
        | Block::Tool { .. }
        | Block::Cost { .. }
        | Block::Note(_)
        | Block::StepCapped
        | Block::Sources(_)
        | Block::Fault { .. } => None,
    }
}

// the map is a BTreeMap, so "first value" is alphabetical-key order —
// `project` would beat `question`; known payload keys are tried first
const SUMMARY_KEYS: &[&str] = &["question", "command", "query", "brief", "path", "id"];

fn grounding_sources(grounding_json: &str) -> Vec<(String, String)> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(grounding_json) else {
        return Vec::new();
    };
    let Some(chunks) = value.get("groundingChunks").and_then(|c| c.as_array()) else {
        return Vec::new();
    };
    chunks
        .iter()
        .filter_map(|chunk| {
            let web = chunk.get("web")?;
            let uri = web.get("uri")?.as_str()?.to_owned();
            let title = web
                .get("title")
                .and_then(|t| t.as_str())
                .filter(|t| !t.trim().is_empty())
                .unwrap_or(&uri)
                .to_owned();
            Some((title, uri))
        })
        .collect()
}

fn tool_summary(arguments_json: &str) -> String {
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str(arguments_json) else {
        return String::new();
    };
    let string_of = |value: &serde_json::Value| match value {
        serde_json::Value::String(s) if !s.trim().is_empty() => Some(s.clone()),
        _ => None,
    };
    SUMMARY_KEYS
        .iter()
        .find_map(|key| map.get(*key).and_then(string_of))
        .or_else(|| map.values().find_map(string_of))
        .unwrap_or_default()
}

pub fn format_tokens(n: u64) -> String {
    if n < 1000 {
        n.to_string()
    } else {
        format!("{:.1}k", n as f64 / 1000.0)
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
        if let Some((subject, body)) = handback_parts(&message.content) {
            return Some(Block::Handback {
                subject,
                body,
                open: false,
            });
        }
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

// the parallel Vec carries each block's history seq, `None` for a block with
// no row of its own (a derived Sources/Cost line, or a ToolResult that only
// mutates its call's block); forking reads only the paired vector's `Some`s
fn history_blocks(
    entries: Vec<HistoryEntry>,
    parent_session: &str,
    fork_point: u64,
    branches: &[(u64, String)],
) -> (Vec<Block>, Vec<Option<u64>>) {
    let mut blocks = Vec::new();
    let mut seqs = Vec::new();
    for entry in entries {
        let seq = entry.seq;
        let was_len = blocks.len();
        match entry.entry {
            Some(history_entry::Entry::Message(message)) => {
                let input_tokens = message.input_tokens;
                let output_tokens = message.output_tokens;
                let elapsed_ms = message.elapsed_ms;
                let sources = grounding_sources(&message.grounding_json);
                if let Some(block) = prose_block(message) {
                    let is_arc = matches!(block, Block::Arc { .. });
                    blocks.push(block);
                    seqs.push(Some(seq));
                    if is_arc && !sources.is_empty() {
                        blocks.push(Block::Sources(sources));
                        seqs.push(None);
                    }
                    if is_arc && (input_tokens != 0 || output_tokens != 0) {
                        blocks.push(Block::Cost {
                            input_tokens,
                            output_tokens,
                            seconds: elapsed_ms as f32 / 1000.0,
                        });
                        seqs.push(None);
                    }
                }
            }
            Some(history_entry::Entry::ToolCall(call)) => {
                blocks.push(Block::Tool {
                    call_id: call.call_id,
                    name: call.name,
                    args: tool_summary(&call.arguments_json),
                    outcome: None,
                });
                seqs.push(Some(seq));
            }
            Some(history_entry::Entry::ToolResult(result)) => {
                let ended = blocks.iter_mut().rev().find(
                    |block| matches!(block, Block::Tool { call_id, .. } if *call_id == result.call_id),
                );
                if let Some(Block::Tool { outcome, .. }) = ended {
                    *outcome = Some(outcome_label(result.outcome));
                }
            }
            // provider-side, arrives resolved; styled like a finished tool line
            Some(history_entry::Entry::ServerCall(call)) => {
                blocks.push(Block::Tool {
                    call_id: String::new(),
                    name: call.name,
                    args: tool_summary(&call.arguments_json),
                    outcome: Some("web"),
                });
                seqs.push(Some(seq));
            }
            None => {}
        }
        // the door out: a fork leaving from this entry gets its signpost
        if blocks.len() > was_len {
            for (_, label) in branches.iter().filter(|(at, _)| *at == seq) {
                blocks.push(Block::Note(format!(
                    "a branch continues from here: {label}"
                )));
                seqs.push(None);
            }
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
    if !parent_session.is_empty() {
        if let Some(cut) = seqs
            .iter()
            .rposition(|seq| seq.is_some_and(|s| s <= fork_point))
        {
            let mut at = cut + 1;
            while at < blocks.len() && seqs[at].is_none() {
                at += 1;
            }
            let head = &parent_session[..parent_session.len().min(8)];
            blocks.insert(at, Block::Note(format!("branched from {head} here")));
            seqs.insert(at, None);
        }
    }
    (blocks, seqs)
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
            preview: "hi".to_owned(),
            last_at: None,
            role: 0,
            project: String::new(),
            dispatched_by: String::new(),
            source: Source::User as i32,
            parent_session: String::new(),
            disposition: 0,
        }
    }

    fn session_with(id: &str, title: &str, preview: &str) -> SessionInfo {
        SessionInfo {
            id: id.to_owned(),
            title: title.to_owned(),
            started_at: None,
            preview: preview.to_owned(),
            last_at: None,
            role: 0,
            project: String::new(),
            dispatched_by: String::new(),
            source: Source::User as i32,
            parent_session: String::new(),
            disposition: 0,
        }
    }

    fn job_session(id: &str, title: &str, role: SessionRole, project: &str) -> SessionInfo {
        SessionInfo {
            role: role as i32,
            project: project.to_owned(),
            dispatched_by: "s-parent".to_owned(),
            source: Source::Model as i32,
            ..session_with(id, title, "hi")
        }
    }

    fn code_session(id: &str, title: &str, project: &str) -> SessionInfo {
        SessionInfo {
            role: SessionRole::Executor as i32,
            project: project.to_owned(),
            dispatched_by: String::new(),
            source: Source::User as i32,
            ..session_with(id, title, "hi")
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
            step_capped: false,
            grounding_json: String::new(),
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
                seq: 0,
            }],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
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

    const GROUNDING: &str = r#"{"webSearchQueries":["arc daemon"],"groundingChunks":[
        {"web":{"uri":"https://example.org/a","title":"Example A"}},
        {"web":{"uri":"https://example.org/b"}},
        {"retrieval":{"uri":"ignored"}}]}"#;

    #[test]
    fn grounding_sources_pull_titled_and_untitled_web_chunks_and_skip_the_rest() {
        assert_eq!(
            grounding_sources(GROUNDING),
            [
                ("Example A".to_owned(), "https://example.org/a".to_owned()),
                (
                    "https://example.org/b".to_owned(),
                    "https://example.org/b".to_owned()
                ),
            ],
            "a missing title falls back to the uri; non-web chunks are skipped"
        );
        assert_eq!(grounding_sources(""), Vec::<(String, String)>::new());
        assert_eq!(
            grounding_sources("not json"),
            Vec::<(String, String)>::new()
        );
    }

    #[test]
    fn a_grounded_end_appends_a_sources_block_and_an_ungrounded_one_does_not() {
        let mut app = App::new();
        typed(&mut app, "what's new?");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Delta("the answer".to_owned()));
        app.on_net(NetEvent::End {
            partial: false,
            input_tokens: 0,
            output_tokens: 0,
            step_capped: false,
            grounding_json: GROUNDING.to_owned(),
        });
        assert!(
            matches!(
                app.transcript.last(),
                Some(Block::Sources(sources)) if sources.len() == 2
            ),
            "citations sit under the answer, got {:?}",
            app.transcript.last()
        );

        typed(&mut app, "and plainly?");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Delta("no web involved".to_owned()));
        app.on_net(end(false));
        assert!(
            !matches!(app.transcript.last(), Some(Block::Sources(_))),
            "an ungrounded turn adds nothing"
        );
    }

    #[test]
    fn a_grounded_history_message_renders_its_sources_after_the_reply() {
        let entries = vec![HistoryEntry {
            entry: Some(history_entry::Entry::Message(HistoryMessage {
                role: Role::Assistant as i32,
                content: "sourced answer".to_owned(),
                partial: false,
                source: 0,
                input_tokens: 0,
                output_tokens: 0,
                elapsed_ms: 0,
                grounding_json: GROUNDING.to_owned(),
            })),
            seq: 0,
        }];
        let (blocks, _seqs) = history_blocks(entries, "", 0, &[]);
        assert!(
            matches!(
                blocks.as_slice(),
                [Block::Arc { .. }, Block::Sources(sources)] if sources.len() == 2
            ),
            "got {blocks:?}"
        );
    }

    #[test]
    fn a_branched_session_gets_a_marker_after_the_last_inherited_row() {
        let entries = vec![
            prose_entry_at(1, Role::User as i32, "inherited question", false),
            prose_entry_at(2, Role::Assistant as i32, "inherited answer", false),
            prose_entry_at(3, Role::User as i32, "own question", false),
        ];
        let (blocks, seqs) = history_blocks(entries, "s-parent-uuid", 2, &[]);

        assert_eq!(
            blocks,
            [
                Block::You("inherited question".to_owned()),
                Block::Arc {
                    text: "inherited answer".to_owned(),
                    partial: false,
                },
                Block::Note("branched from s-parent here".to_owned()),
                Block::You("own question".to_owned()),
            ],
            "the marker lands right after the last inherited row"
        );
        assert_eq!(seqs, [Some(1), Some(2), None, Some(3)]);
    }

    #[test]
    fn a_parentless_session_gets_no_marker() {
        let entries = vec![prose_entry_at(1, Role::User as i32, "hi", false)];
        let (blocks, _seqs) = history_blocks(entries, "", 0, &[]);

        assert!(
            !blocks.iter().any(|b| matches!(b, Block::Note(_))),
            "no lineage, no marker"
        );
    }

    #[test]
    fn point_visual_lights_exactly_one_block_and_walks_both_ways() {
        let mut app = App::new();
        app.transcript = vec![
            Block::You("one".to_owned()),
            Block::Arc {
                text: "two".to_owned(),
                partial: false,
            },
            Block::You("three".to_owned()),
        ];
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('v')));
        assert_eq!(app.visual_range(), Some((2, 2)), "starts at the last block");
        app.on_key(key(KeyCode::Char('k')));
        assert_eq!(app.visual_range(), Some((1, 1)), "one block, moved up");
        app.on_key(key(KeyCode::Char('j')));
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(
            app.visual_range(),
            Some((2, 2)),
            "walks back down and clamps at the end"
        );
    }

    #[test]
    fn point_visual_skips_a_tool_block_between_two_messages() {
        let mut app = App::new();
        app.transcript = vec![
            Block::You("one".to_owned()),
            Block::Tool {
                call_id: "t1".to_owned(),
                name: "bash".to_owned(),
                args: String::new(),
                outcome: Some("ok"),
            },
            Block::Arc {
                text: "two".to_owned(),
                partial: false,
            },
        ];
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('v')));
        assert_eq!(
            app.visual_range(),
            Some((2, 2)),
            "starts on the last message"
        );

        app.on_key(key(KeyCode::Char('k')));
        assert_eq!(
            app.visual_range(),
            Some((0, 0)),
            "the tool block is skipped"
        );

        app.on_key(key(KeyCode::Char('k')));
        assert_eq!(
            app.visual_range(),
            Some((0, 0)),
            "no earlier message: stays put"
        );

        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(
            app.visual_range(),
            Some((2, 2)),
            "skips the tool block going down too"
        );

        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(
            app.visual_range(),
            Some((2, 2)),
            "no later message: stays put"
        );
    }

    #[test]
    fn range_visual_still_extends_upward_from_its_anchor() {
        let mut app = App::new();
        app.transcript = vec![
            Block::You("one".to_owned()),
            Block::Arc {
                text: "two".to_owned(),
                partial: false,
            },
            Block::You("three".to_owned()),
        ];
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('V')));
        app.on_key(key(KeyCode::Char('k')));
        assert_eq!(app.visual_range(), Some((1, 2)), "a range, not a point");
    }

    #[test]
    fn help_scrolling_moves_and_close_resets_it() {
        let mut app = App::new();
        app.on_key(key(KeyCode::Esc));
        app.help = true;
        app.on_key(key(KeyCode::Char('j')));
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.help_scroll, 2);
        app.on_key(key(KeyCode::Char('g')));
        assert_eq!(app.help_scroll, 0);
        app.on_key(key(KeyCode::Char('j')));
        app.on_key(key(KeyCode::Char('q')));
        assert!(!app.help);
        assert_eq!(app.help_scroll, 0, "closing forgets the scroll");
    }

    #[test]
    fn abandoned_branches_hide_from_the_picker_until_x_toggles_them_in() {
        use arc_proto::v1::branch_marked::Disposition;
        let mut app = App::new();
        app.on_key(key(KeyCode::Esc));
        let mut root = session_with("s-root", "the trunk", "hi");
        root.last_at = Some(prost_types::Timestamp::default());
        let mut dead = session_with("s-dead", "wrong turn", "hi");
        dead.parent_session = "s-root".to_owned();
        dead.disposition = Disposition::Abandoned as i32;
        dead.last_at = Some(prost_types::Timestamp::default());
        app.sessions = vec![root, dead];
        app.on_key(key(KeyCode::Char('s')));

        let listed: Vec<&str> = app
            .picker_tree()
            .iter()
            .map(|(s, _)| s.id.as_str())
            .collect();
        assert_eq!(
            listed,
            ["s-root"],
            "abandoned stays out of sight by default"
        );

        app.on_key(key(KeyCode::Char('x')));
        let listed: Vec<&str> = app
            .picker_tree()
            .iter()
            .map(|(s, _)| s.id.as_str())
            .collect();
        assert_eq!(
            listed,
            ["s-dead", "s-root"],
            "x brings the abandoned branch back, in plain recency order"
        );
    }

    #[test]
    fn a_fork_out_of_a_session_gets_a_forward_marker_after_its_entry() {
        let entries = vec![
            HistoryEntry {
                entry: Some(history_entry::Entry::Message(HistoryMessage {
                    role: Role::User as i32,
                    content: "keep this".to_owned(),
                    ..Default::default()
                })),
                seq: 4,
            },
            HistoryEntry {
                entry: Some(history_entry::Entry::Message(HistoryMessage {
                    role: Role::Assistant as i32,
                    content: "the dead path starts here".to_owned(),
                    ..Default::default()
                })),
                seq: 6,
            },
        ];
        let branches = vec![(4, "an alternate take".to_owned())];
        let (blocks, _seqs) = history_blocks(entries, "", 0, &branches);
        assert!(
            matches!(
                &blocks[..],
                [Block::You(_), Block::Note(note), Block::Arc { .. }]
                    if note == "a branch continues from here: an alternate take"
            ),
            "the signpost sits right after the fork point, got {blocks:?}"
        );
    }

    #[test]
    fn opening_the_picker_requests_a_fresh_session_list() {
        let mut app = App::new();
        app.on_key(key(KeyCode::Esc));
        assert_eq!(
            app.on_key(key(KeyCode::Char('s'))),
            Some(Command::List),
            "a branch forked seconds ago must appear without a restart"
        );
        assert!(app.picker.is_some());
    }

    #[test]
    fn the_summary_prefers_the_payload_key_over_alphabetical_order() {
        assert_eq!(
            tool_summary(r#"{"project":"arc","question":"is this the simplest?"}"#),
            "is this the simplest?",
            "consult_expert shows what was asked, not where"
        );
        assert_eq!(
            tool_summary(r#"{"namespace":"global","zz":"other"}"#),
            "global",
            "no known key falls back to the first string value, as before"
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
            step_capped: false,
            grounding_json: String::new(),
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
    fn a_step_capped_end_appends_a_dim_notice_after_the_reply() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Delta("in progress".to_owned()));

        app.on_net(NetEvent::End {
            partial: false,
            input_tokens: 0,
            output_tokens: 0,
            step_capped: true,
            grounding_json: String::new(),
        });

        assert_eq!(app.transcript.last(), Some(&Block::StepCapped));
    }

    #[test]
    fn a_normal_end_appends_no_step_capped_notice() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Delta("done".to_owned()));

        app.on_net(end(false));

        assert!(
            !app.transcript
                .iter()
                .any(|block| matches!(block, Block::StepCapped))
        );
    }

    #[test]
    fn turn_counters_are_idle_until_a_turn_starts_then_track_streamed_deltas() {
        let mut app = App::new();
        assert_eq!(app.turn_elapsed_seconds(), None);
        assert_eq!(app.streamed_tokens_estimate(), 0);

        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        assert_eq!(app.turn_elapsed_seconds(), Some(0));

        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Reasoning("thinking".to_owned())); // 8 chars
        app.on_net(NetEvent::Delta("hello world".to_owned())); // 11 chars
        assert_eq!(app.streamed_tokens_estimate(), (8 + 11) / 4);
    }

    #[test]
    fn watched_job_reasoning_streams_a_thought_block_for_the_open_session_only() {
        let mut app = App::new();
        app.session_id = Some("s-job".to_owned());

        app.on_net(NetEvent::JobReasoning {
            session_id: "s-other".to_owned(),
            text: "not mine".to_owned(),
        });
        assert!(
            app.transcript.is_empty(),
            "another session's thinking never lands on screen"
        );

        app.on_net(NetEvent::JobReasoning {
            session_id: "s-job".to_owned(),
            text: "weighing".to_owned(),
        });
        app.on_net(NetEvent::JobReasoning {
            session_id: "s-job".to_owned(),
            text: " options".to_owned(),
        });
        assert!(
            matches!(
                app.transcript.as_slice(),
                [Block::Thought { text, done: false, .. }] if text == "weighing options"
            ),
            "one open thought block accumulates the deltas, got {:?}",
            app.transcript
        );
    }

    #[test]
    fn watched_job_reasoning_is_dropped_while_an_own_turn_streams() {
        let mut app = App::new();
        app.session_id = Some("s-job".to_owned());
        app.status = Status::Streaming;

        app.on_net(NetEvent::JobReasoning {
            session_id: "s-job".to_owned(),
            text: "late".to_owned(),
        });
        assert!(
            app.transcript.is_empty(),
            "a watched push must not interleave with an own turn's stream"
        );
    }

    #[test]
    fn the_streamed_counter_resets_between_turns() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Delta("some streamed reply text".to_owned()));
        assert!(app.streamed_tokens_estimate() > 0);

        app.on_net(end(false));
        typed(&mut app, "again");
        app.on_key(key(KeyCode::Enter));

        assert_eq!(
            app.streamed_tokens_estimate(),
            0,
            "a fresh turn starts the counter over"
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
    fn disconnecting_sets_the_status_and_it_persists_with_no_queue() {
        let mut app = App::new();
        app.on_net(NetEvent::Disconnected {
            reason: "the daemon closed the connection".to_owned(),
        });
        assert_eq!(app.status, Status::Disconnected);
    }

    #[test]
    fn a_browsing_reply_clears_the_disconnected_status() {
        let mut app = App::new();
        app.on_net(NetEvent::Disconnected {
            reason: "the daemon closed the connection".to_owned(),
        });
        assert_eq!(app.status, Status::Disconnected);

        app.on_net(NetEvent::Sessions(Vec::new()));
        assert_eq!(
            app.status,
            Status::Idle,
            "a successful command proves the reconnect worked"
        );
    }

    #[test]
    fn another_disconnect_leaves_the_status_disconnected() {
        let mut app = App::new();
        app.on_net(NetEvent::Disconnected {
            reason: "first".to_owned(),
        });
        app.on_net(NetEvent::Disconnected {
            reason: "second".to_owned(),
        });
        assert_eq!(app.status, Status::Disconnected);
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
                role: 0,
                project: String::new(),
                dispatched_by: String::new(),
                source: Source::User as i32,
                parent_session: String::new(),
                disposition: 0,
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

    #[test]
    fn the_picker_keeps_recency_order_and_annotates_lineage() {
        let mut app = App::new();
        let mut fork = session_with("child-of-1", "the fork", "hi");
        fork.parent_session = "s-1-uuid-long".to_owned();
        app.on_net(NetEvent::Sessions(vec![
            fork,
            session_with("s-1-uuid-long", "root one", "hi"),
            session_with("s-2", "root two", "hi"),
        ]));

        let rows = app.picker_tree();
        let ids: Vec<&str> = rows.iter().map(|(s, _)| s.id.as_str()).collect();
        assert_eq!(
            ids,
            ["child-of-1", "s-1-uuid-long", "s-2"],
            "position is pure recency: the fresh branch leads, its stale parent follows"
        );
        assert_eq!(
            rows.into_iter().map(|(_, p)| p).collect::<Vec<_>>(),
            [Some("s-1-uuid".to_owned()), None, None],
            "lineage is an annotation — the parent's 8-char prefix — not a hierarchy"
        );
    }

    #[test]
    fn m_and_shift_x_mark_the_selected_branch_only() {
        let mut app = App::new();
        let mut fork = session_with("s-fork", "the fork", "hi");
        fork.parent_session = "s-root".to_owned();
        app.on_net(NetEvent::Sessions(vec![
            session_with("s-root", "root", "hi"),
            fork,
        ]));
        normal(&mut app, "s");
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(
            app.picker_session(1).map(|s| s.id.as_str()),
            Some("s-fork"),
            "the fresh branch is the first row under flat recency"
        );

        let command = app.on_key(key(KeyCode::Char('m')));

        assert_eq!(
            command,
            Some(Command::MarkBranch {
                session_id: "s-fork".to_owned(),
                disposition: branch_marked::Disposition::Real,
            })
        );

        let command = app.on_key(key(KeyCode::Char('X')));
        assert_eq!(
            command,
            Some(Command::MarkBranch {
                session_id: "s-fork".to_owned(),
                disposition: branch_marked::Disposition::Abandoned,
            })
        );
    }

    #[test]
    fn m_on_a_root_row_is_an_instructive_error_with_no_wire_call() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![session_with(
            "s-root", "root", "hi",
        )]));
        normal(&mut app, "s");
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.picker_session(1).map(|s| s.id.as_str()), Some("s-root"));

        let command = app.on_key(key(KeyCode::Char('X')));

        assert_eq!(command, None, "a root has nothing to mark");
        assert!(app.last_error.is_some());
    }

    fn prose(role: i32, content: &str, partial: bool) -> HistoryMessage {
        HistoryMessage {
            role,
            content: content.to_owned(),
            partial,
            source: 0,
            ..Default::default()
        }
    }

    fn prose_with_usage(
        role: i32,
        content: &str,
        input_tokens: u32,
        output_tokens: u32,
        elapsed_ms: u32,
    ) -> HistoryMessage {
        HistoryMessage {
            input_tokens,
            output_tokens,
            elapsed_ms,
            ..prose(role, content, false)
        }
    }

    fn prose_entry(role: i32, content: &str, partial: bool) -> HistoryEntry {
        HistoryEntry {
            entry: Some(history_entry::Entry::Message(prose(role, content, partial))),
            seq: 0,
        }
    }

    fn prose_entry_at(seq: u64, role: i32, content: &str, partial: bool) -> HistoryEntry {
        HistoryEntry {
            seq,
            ..prose_entry(role, content, partial)
        }
    }

    fn call_entry(call_id: &str, name: &str) -> HistoryEntry {
        HistoryEntry {
            entry: Some(history_entry::Entry::ToolCall(HistoryToolCall {
                call_id: call_id.to_owned(),
                name: name.to_owned(),
                arguments_json: String::new(),
            })),
            seq: 0,
        }
    }

    fn result_entry(call_id: &str, outcome: i32) -> HistoryEntry {
        HistoryEntry {
            entry: Some(history_entry::Entry::ToolResult(HistoryToolResult {
                call_id: call_id.to_owned(),
                outcome,
                truncated: false,
            })),
            seq: 0,
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
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
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
    fn history_renders_a_cost_block_after_an_assistant_row_carrying_usage() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![
                prose_entry(Role::User as i32, "hi", false),
                HistoryEntry {
                    entry: Some(history_entry::Entry::Message(prose_with_usage(
                        Role::Assistant as i32,
                        "hello there",
                        2345,
                        140,
                        1500,
                    ))),
                    seq: 0,
                },
            ],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });

        assert_eq!(
            app.transcript,
            [
                Block::You("hi".to_owned()),
                Block::Arc {
                    text: "hello there".to_owned(),
                    partial: false
                },
                Block::Cost {
                    input_tokens: 2345,
                    output_tokens: 140,
                    seconds: 1.5,
                },
            ]
        );
    }

    #[test]
    fn history_renders_no_cost_block_for_a_zero_usage_row() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![prose_entry(Role::Assistant as i32, "hello there", false)],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });

        assert!(
            !app.transcript
                .iter()
                .any(|block| matches!(block, Block::Cost { .. })),
            "zero usage renders no cost line"
        );
    }

    #[test]
    fn history_renders_no_cost_block_for_a_system_row_even_with_usage() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![HistoryEntry {
                entry: Some(history_entry::Entry::Message(HistoryMessage {
                    source: Source::System as i32,
                    ..prose_with_usage(Role::User as i32, "a handback note", 10, 20, 30)
                })),
                seq: 0,
            }],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });

        assert_eq!(
            app.transcript,
            [Block::System("a handback note".to_owned())],
            "a system row never grows a cost line, even carrying stray usage"
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
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
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
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
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
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
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
    fn the_picker_hides_job_sessions_until_a_reveals_them() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![
            session("conv"),
            job_session("job", "", SessionRole::Executor, "arc"),
        ]));
        normal(&mut app, "s");

        assert_eq!(
            app.picker_rows()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["conv"],
            "a dispatched job is not a conversation"
        );

        app.on_key(key(KeyCode::Char('a')));
        assert_eq!(app.picker_rows().len(), 2, "a reveals the job too");

        app.on_key(key(KeyCode::Char('a')));
        assert_eq!(app.picker_rows().len(), 1, "a toggles back off");
    }

    #[test]
    fn a_user_sourced_code_session_lists_it_as_a_conversation() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![
            code_session("code", "", "arc"),
            job_session("job", "", SessionRole::Executor, "arc"),
        ]));
        normal(&mut app, "s");

        assert_eq!(
            app.picker_rows()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["code"],
            "a user-opened executor session is a root conversation, not a job"
        );

        app.on_key(key(KeyCode::Char('a')));
        assert_eq!(
            app.picker_rows().len(),
            2,
            "a reveals the dispatched job alongside it"
        );
    }

    #[test]
    fn a_model_sourced_session_with_no_dispatched_by_still_hides_as_a_job() {
        // pre-6.34: dispatched_by was never recorded, but source always was
        let mut pre_634_job = job_session("job", "", SessionRole::Executor, "arc");
        pre_634_job.dispatched_by = String::new();
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![session("conv"), pre_634_job]));
        normal(&mut app, "s");

        assert_eq!(
            app.picker_rows()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["conv"],
            "the recorded source hides it even with no dispatched_by"
        );
    }

    #[test]
    fn an_unspecified_source_session_hides_as_a_job() {
        // a stale index predating the source column; a rebuild fixes it
        let mut unspecified = session("stale");
        unspecified.source = 0;
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![session("conv"), unspecified]));
        normal(&mut app, "s");

        assert_eq!(
            app.picker_rows()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["conv"],
            "unspecified source fails toward hiding noise, not showing it"
        );
    }

    #[test]
    fn space_toggles_show_all_same_as_a() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![
            session("conv"),
            job_session("job", "", SessionRole::Executor, "arc"),
        ]));
        normal(&mut app, "s");

        app.on_key(key(KeyCode::Char(' ')));
        assert_eq!(app.picker_rows().len(), 2, "space reveals the job too");

        app.on_key(key(KeyCode::Char(' ')));
        assert_eq!(app.picker_rows().len(), 1, "space toggles back off");
    }

    #[test]
    fn space_while_filtering_types_into_the_filter_instead_of_toggling() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![
            session_with("keep", "two words", ""),
            job_session("job", "two words job", SessionRole::Executor, "arc"),
        ]));
        normal(&mut app, "s");
        app.on_key(key(KeyCode::Char('/')));
        typed(&mut app, "two");
        assert_eq!(app.picker_rows().len(), 1, "the job stays hidden");

        app.on_key(key(KeyCode::Char(' ')));
        typed(&mut app, "words");

        assert_eq!(app.input, "two words", "space landed in the filter text");
        assert_eq!(
            app.picker_rows().len(),
            1,
            "show-all never toggled, the job is still hidden"
        );
    }

    #[test]
    fn the_filter_narrows_within_the_current_toggle_state() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![
            session_with("conv", "alpha talk", ""),
            job_session("job", "alpha job", SessionRole::Archivist, "arc"),
        ]));
        normal(&mut app, "s");
        app.on_key(key(KeyCode::Char('/')));
        typed(&mut app, "alpha");
        assert_eq!(
            app.picker_rows()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["conv"],
            "the job stays hidden while filtering"
        );

        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('a')));
        app.on_key(key(KeyCode::Char('/')));
        typed(&mut app, "alpha");
        assert_eq!(
            app.picker_rows().len(),
            2,
            "with jobs shown, the filter matches both"
        );
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
    fn a_review_changed_push_sets_the_pending_count() {
        let mut app = App::new();
        assert_eq!(app.review_pending, 0);

        app.on_net(NetEvent::ReviewChanged(2));
        assert_eq!(app.review_pending, 2);

        app.on_net(NetEvent::ReviewChanged(0));
        assert_eq!(app.review_pending, 0);
    }

    #[test]
    fn opening_the_review_pane_does_not_touch_the_pending_count() {
        let mut app = App::new();
        app.on_net(NetEvent::ReviewChanged(3));

        normal(&mut app, ":review");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::ReviewItems(vec![entry("mr-1", "one")]));

        assert_eq!(
            app.review_pending, 3,
            "the queue's size, not an unread badge that opening clears"
        );
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
            tool_steps: 0,
            idle_seconds: 0,
            parent_session: String::new(),
            queued_steers: 0,
            last_call: String::new(),
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
    fn question_mark_opens_help_from_normal_mode() {
        let mut app = normal_app();

        assert_eq!(app.on_key(key(KeyCode::Char('?'))), None);

        assert!(app.help);
    }

    #[test]
    fn shift_j_opens_jobs_from_normal_mode_same_as_colon_jobs() {
        let mut app = normal_app();

        let command = app.on_key(key(KeyCode::Char('J')));

        assert_eq!(command, Some(Command::ListJobs));
        assert!(app.jobs.is_some());
    }

    #[test]
    fn shift_m_opens_review_from_normal_mode_same_as_colon_review() {
        let mut app = normal_app();

        let command = app.on_key(key(KeyCode::Char('M')));

        assert!(matches!(command, Some(Command::ReviewList { .. })));
        assert!(app.review.is_some());
    }

    #[test]
    fn capital_c_picks_a_project_and_enter_walks_the_code_door() {
        let mut app = App::new();
        app.on_key(key(KeyCode::Esc));
        let command = app.on_key(key(KeyCode::Char('C')));
        assert_eq!(command, Some(Command::ListProjects));
        assert!(!app.projects.as_ref().expect("picker is open").loaded);

        app.on_net(NetEvent::ProjectItems(vec![
            ProjectInfo {
                name: "arc".to_owned(),
                description: "ARC's own repo".to_owned(),
            },
            ProjectInfo {
                name: "scratch".to_owned(),
                description: String::new(),
            },
        ]));
        app.on_key(key(KeyCode::Char('j')));
        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(command, None, "nothing durable for an unsent pick");
        assert_eq!(app.projects, None, "enter closes the picker");
        assert_eq!(app.open_door_label().as_deref(), Some("code/scratch"));

        app.on_key(key(KeyCode::Char('i')));
        typed(&mut app, "hello");
        let command = app.on_key(key(KeyCode::Enter));
        assert_eq!(
            command,
            Some(Command::CreateSession {
                role: SessionRole::Executor,
                project: "scratch".to_owned(),
            }),
            "from the pick on, this is exactly the :code flow"
        );
    }

    #[test]
    fn an_empty_or_unloaded_project_picker_swallows_enter_and_esc_closes() {
        let mut app = App::new();
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('C')));

        let command = app.on_key(key(KeyCode::Enter));
        assert_eq!(command, None, "enter on a loading list picks nothing");
        assert!(app.projects.is_some(), "the picker stays open");
        assert_eq!(app.open_door_label(), None);

        app.on_key(key(KeyCode::Esc));
        assert_eq!(app.projects, None);
    }

    #[test]
    fn colon_code_is_frontend_only_until_the_first_message() {
        let mut app = App::new();
        normal(&mut app, ":code arc");
        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(command, None, "nothing durable for an unsent :code");
        assert_eq!(app.open_door_label().as_deref(), Some("code/arc"));
        assert_eq!(app.last_error, None, ":code is a command, not E492");

        app.on_key(key(KeyCode::Char('i')));
        typed(&mut app, "hello");
        let command = app.on_key(key(KeyCode::Enter));
        assert_eq!(
            command,
            Some(Command::CreateSession {
                role: SessionRole::Executor,
                project: "arc".to_owned(),
            }),
            "the first message is what opens the session"
        );
    }

    #[test]
    fn the_code_flow_labels_the_pending_door_and_the_created_session() {
        let mut app = App::new();
        normal(&mut app, ":code scratch");
        app.on_key(key(KeyCode::Enter));

        assert_eq!(
            app.open_door_label().as_deref(),
            Some("code/scratch"),
            "the pending door is labelled before anything exists"
        );

        app.on_key(key(KeyCode::Char('i')));
        typed(&mut app, "hello");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::SessionCreated {
            session_id: "s-new".to_owned(),
        });
        assert_eq!(
            app.open_door_label().as_deref(),
            Some("code/scratch"),
            "labelled from the create flow, before any Sessions push lands"
        );
    }

    #[test]
    fn abandoning_a_pending_door_creates_nothing_and_drops_the_label() {
        let mut app = App::new();
        normal(&mut app, ":code arc");
        app.on_key(key(KeyCode::Enter));
        assert_eq!(app.open_door_label().as_deref(), Some("code/arc"));

        app.on_key(ctrl('n'));
        assert_eq!(app.open_door_label(), None, "navigation abandons the door");
    }

    #[test]
    fn opening_a_session_from_a_sessions_list_shows_its_door_label() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![
            code_session("s-code", "", "scratch"),
            job_session("s-job", "", SessionRole::Executor, "arc"),
            session("s-concierge"),
        ]));

        app.start_session(Some("s-code".to_owned()));
        assert_eq!(app.open_door_label().as_deref(), Some("code/scratch"));

        app.start_session(Some("s-job".to_owned()));
        assert_eq!(app.open_door_label().as_deref(), Some("job/arc"));

        app.start_session(Some("s-concierge".to_owned()));
        assert_eq!(
            app.open_door_label(),
            None,
            "a concierge conversation is the default door, unlabelled"
        );
    }

    #[test]
    fn colon_code_with_no_project_is_e492() {
        let mut app = App::new();
        normal(&mut app, ":code");
        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(command, None);
        assert_eq!(app.last_error.as_deref(), Some("E492"));
    }

    #[test]
    fn a_created_session_receives_the_stashed_first_message() {
        let mut app = App::new();
        normal(&mut app, ":code scratch");
        app.on_key(key(KeyCode::Enter));
        app.on_key(key(KeyCode::Char('i')));
        typed(&mut app, "run the tests");
        app.on_key(key(KeyCode::Enter));

        let command = app.on_net(NetEvent::SessionCreated {
            session_id: "s-code".to_owned(),
        });

        assert_eq!(app.session_id.as_deref(), Some("s-code"));
        assert_eq!(
            command,
            Some(Command::Send {
                session_id: Some("s-code".to_owned()),
                content: "run the tests".to_owned(),
            }),
            "the message that opened the door is the session's first turn"
        );
    }

    #[test]
    fn a_message_less_session_is_hidden_from_the_picker() {
        let mut app = App::new();
        let mut empty = session("s-empty");
        empty.title = String::new();
        empty.preview = String::new();
        app.on_net(NetEvent::Sessions(vec![empty, session("s-real")]));
        app.on_key(ctrl('p'));

        let ids: Vec<&str> = app.picker_rows().iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, ["s-real"], "an artifact with no messages never lists");
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
    fn the_jobs_popup_lists_every_job_regardless_of_which_session_is_open() {
        use arc_proto::v1::job_info::State;

        let mine = job_of("s-mine", "s-open", State::Running);
        let unrelated = job_of("s-other", "s-elsewhere", State::Running);
        let mut app = App::new();
        app.session_id = Some("s-open".to_owned());
        normal(&mut app, ":jobs");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::JobItems(vec![mine.clone(), unrelated.clone()]));

        assert_eq!(
            app.jobs.as_ref().expect("open").items,
            [mine, unrelated],
            "the popup is unscoped: only the ambient strip filters by parent_session"
        );
    }

    #[test]
    fn j_and_up_move_the_job_selection_within_bounds() {
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
        app.on_key(key(KeyCode::Up));
        assert_eq!(app.jobs.as_ref().expect("open").selected, 0);
        app.on_key(key(KeyCode::Up));
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
    fn x_on_a_running_row_sends_cancel_job_and_confirms_in_the_footer() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running)]);

        let command = app.on_key(key(KeyCode::Char('x')));

        assert_eq!(
            command,
            Some(Command::CancelJob {
                session_id: "s-a".to_owned()
            })
        );
        assert_eq!(
            app.jobs.as_ref().expect("open").confirmation.as_deref(),
            Some("cancelled s-a")
        );
        assert!(app.jobs.is_some(), "the popup stays open");
    }

    #[test]
    fn x_on_a_terminal_row_is_a_no_op_with_a_footer_note() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Finished)]);

        let command = app.on_key(key(KeyCode::Char('x')));

        assert_eq!(command, None, "nothing to cancel on a finished job");
        assert_eq!(
            app.jobs.as_ref().expect("open").confirmation.as_deref(),
            Some("not running")
        );
    }

    #[test]
    fn k_moves_the_job_selection_up_and_cancels_nothing() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running), job("s-b", State::Running)]);
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.jobs.as_ref().expect("open").selected, 1);

        let command = app.on_key(key(KeyCode::Char('k')));

        assert_eq!(command, None, "k navigates, it does not cancel");
        assert_eq!(app.jobs.as_ref().expect("open").selected, 0);
        assert_eq!(app.jobs.as_ref().expect("open").confirmation, None);
    }

    #[test]
    fn d_on_a_row_with_queued_steers_sends_drop_steers_and_confirms_the_count() {
        use arc_proto::v1::job_info::State;

        let mut queued = job("s-a", State::Running);
        queued.queued_steers = 2;
        let mut app = jobsview(vec![queued]);

        let command = app.on_key(key(KeyCode::Char('d')));

        assert_eq!(
            command,
            Some(Command::DropSteers {
                session_id: "s-a".to_owned()
            })
        );
        assert_eq!(
            app.jobs.as_ref().expect("open").confirmation.as_deref(),
            Some("dropped 2")
        );
    }

    #[test]
    fn d_on_a_row_with_no_queued_steers_is_a_footer_no_op() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running)]);

        let command = app.on_key(key(KeyCode::Char('d')));

        assert_eq!(command, None);
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

    fn job_of(
        session_id: &str,
        parent_session: &str,
        state: arc_proto::v1::job_info::State,
    ) -> JobInfo {
        let mut job = job(session_id, state);
        job.parent_session = parent_session.to_owned();
        job
    }

    #[test]
    fn job_changed_populates_the_strip_and_a_second_push_replaces_not_appends() {
        use arc_proto::v1::job_info::State;

        let mut app = App::new();
        app.session_id = Some("s-parent".to_owned());
        app.on_net(NetEvent::JobChanged(job_of(
            "s-1",
            "s-parent",
            State::Running,
        )));
        assert_eq!(app.ambient.len(), 1);
        assert_eq!(app.strip_job().map(|j| j.session_id.as_str()), Some("s-1"));

        let mut updated = job_of("s-1", "s-parent", State::Running);
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
        app.session_id = Some("s-parent".to_owned());
        app.on_net(NetEvent::JobChanged(job_of(
            "s-1",
            "s-parent",
            State::Running,
        )));
        app.on_net(NetEvent::JobChanged(job_of(
            "s-2",
            "s-parent",
            State::Finished,
        )));
        assert_eq!(
            app.strip_job().map(|j| j.session_id.as_str()),
            Some("s-1"),
            "the finished job does not shadow the running one"
        );
        assert_eq!(app.running_job_count(), 1);

        app.on_net(NetEvent::JobChanged(job_of(
            "s-3",
            "s-parent",
            State::Running,
        )));
        assert_eq!(
            app.strip_job().map(|j| j.session_id.as_str()),
            Some("s-3"),
            "the newest running job leads"
        );
        assert_eq!(app.running_job_count(), 2);
    }

    #[test]
    fn the_strip_picks_the_open_sessions_own_child_over_a_more_recent_unrelated_job() {
        use arc_proto::v1::job_info::State;

        let mut app = App::new();
        app.session_id = Some("s-parent".to_owned());
        app.on_net(NetEvent::JobChanged(job_of(
            "s-mine",
            "s-parent",
            State::Running,
        )));
        app.on_net(NetEvent::JobChanged(job_of(
            "s-other",
            "s-elsewhere",
            State::Running,
        )));

        assert_eq!(
            app.strip_job().map(|j| j.session_id.as_str()),
            Some("s-mine"),
            "a more recently touched job scoped to a different session never wins"
        );
    }

    #[test]
    fn no_open_session_means_no_strip_job() {
        use arc_proto::v1::job_info::State;

        let mut app = App::new();
        app.on_net(NetEvent::JobChanged(job_of(
            "s-1",
            "s-parent",
            State::Running,
        )));

        assert_eq!(app.session_id, None);
        assert_eq!(
            app.strip_job(),
            None,
            "nothing is open to scope the strip to"
        );
    }

    #[test]
    fn no_running_jobs_means_no_strip_job() {
        use arc_proto::v1::job_info::State;

        let mut app = App::new();
        app.session_id = Some("s-parent".to_owned());
        app.on_net(NetEvent::JobChanged(job_of(
            "s-1",
            "s-parent",
            State::Finished,
        )));
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
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
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

    const JOB_ID: &str = "c1a4a9e7-d2b8-4f60-91e3-b5a7c9d1e3f5";

    fn handback_message(content: &str) -> HistoryMessage {
        HistoryMessage {
            source: Source::System as i32,
            ..prose(Role::User as i32, content, false)
        }
    }

    fn handback_entry(content: &str) -> HistoryEntry {
        HistoryEntry {
            entry: Some(history_entry::Entry::Message(handback_message(content))),
            seq: 0,
        }
    }

    #[test]
    fn a_handback_history_row_lands_as_one_folded_block() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![handback_entry(&format!(
                "Job {JOB_ID} finished.\nfixed the flaky test"
            ))],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });

        assert_eq!(
            app.transcript,
            [Block::Handback {
                subject: format!("Job {JOB_ID} finished."),
                body: "fixed the flaky test".to_owned(),
                open: false,
            }],
            "a handback starts folded"
        );
    }

    #[test]
    fn a_stopped_handback_keeps_the_reason_in_its_subject() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![handback_entry(&format!(
                "Job {JOB_ID} stopped: token budget exhausted (500/400).\npartial work"
            ))],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });

        assert_eq!(
            app.transcript,
            [Block::Handback {
                subject: format!("Job {JOB_ID} stopped: token budget exhausted (500/400)."),
                body: "partial work".to_owned(),
                open: false,
            }],
        );
    }

    #[test]
    fn system_rows_without_the_handback_shape_stay_system_blocks() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![
                handback_entry("consolidation wrote three memories."),
                handback_entry("Job finished.\nno id in the header"),
                handback_entry(&format!("Job {JOB_ID} finished.")),
            ],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });

        assert_eq!(
            app.transcript,
            [
                Block::System("consolidation wrote three memories.".to_owned()),
                Block::System("Job finished.\nno id in the header".to_owned()),
                Block::System(format!("Job {JOB_ID} finished.")),
            ],
            "prose that merely mentions jobs must not fold away"
        );
    }

    #[test]
    fn ctrl_o_opens_thoughts_and_handbacks_together() {
        let mut app = App::new();
        typed(&mut app, "hi");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Reasoning("checking the failing case".to_owned()));
        app.on_net(NetEvent::Delta("done".to_owned()));
        app.transcript.push(
            prose_block(handback_message(&format!(
                "Job {JOB_ID} finished.\nall green"
            )))
            .expect("handback block"),
        );

        app.on_key(ctrl('o'));
        assert!(
            matches!(&app.transcript[1], Block::Thought { open: true, .. }),
            "ctrl-o opens the thought"
        );
        assert!(
            matches!(&app.transcript[3], Block::Handback { open: true, .. }),
            "and the handback with it"
        );

        app.on_key(ctrl('o'));
        assert!(matches!(
            &app.transcript[1],
            Block::Thought { open: false, .. }
        ));
        assert!(matches!(
            &app.transcript[3],
            Block::Handback { open: false, .. }
        ));
    }

    #[test]
    fn a_handback_is_yank_invisible_like_other_activity() {
        assert_eq!(
            block_yank_text(&Block::Handback {
                subject: format!("Job {JOB_ID} finished."),
                body: "all green".to_owned(),
                open: true,
            }),
            None,
            "yanking the conversation never scoops up job machinery"
        );
    }

    #[tokio::test]
    async fn ctrl_t_bounces_between_the_last_two_sessions() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![]));
        let first = app.start_session(Some("s-first".to_owned()));
        assert!(first.is_some());
        let second = app.start_session(Some("s-job".to_owned()));
        assert!(second.is_some());
        assert_eq!(app.session_id.as_deref(), Some("s-job"));

        let back = app.on_key(ctrl('t'));
        assert!(back.is_some(), "going back refetches history");
        assert_eq!(app.session_id.as_deref(), Some("s-first"));

        let forth = app.on_key(ctrl('t'));
        assert!(forth.is_some());
        assert_eq!(app.session_id.as_deref(), Some("s-job"));
    }

    #[tokio::test]
    async fn ctrl_t_with_no_history_does_nothing() {
        let mut app = App::new();
        assert_eq!(app.on_key(ctrl('t')), None);
        assert_eq!(app.session_id, None);
    }

    #[test]
    fn y_yanks_the_last_reply() {
        let mut app = App::new();
        app.transcript.push(Block::You("hi".to_owned()));
        app.transcript.push(Block::Arc {
            text: "hello there".to_owned(),
            partial: false,
        });
        app.on_key(key(KeyCode::Esc));

        let command = app.on_key(key(KeyCode::Char('y')));

        assert_eq!(command, Some(Command::Yank("hello there".to_owned())));
        assert_eq!(app.yank_note.as_deref(), Some("yanked"));
    }

    #[test]
    fn y_yanks_a_partial_reply_too() {
        let mut app = App::new();
        app.transcript.push(Block::Arc {
            text: "cut off mid".to_owned(),
            partial: true,
        });
        app.on_key(key(KeyCode::Esc));

        let command = app.on_key(key(KeyCode::Char('y')));

        assert_eq!(command, Some(Command::Yank("cut off mid".to_owned())));
    }

    #[test]
    fn y_with_no_reply_yet_is_a_no_op_with_a_footer_note() {
        let mut app = App::new();
        app.transcript.push(Block::You("hi".to_owned()));
        app.on_key(key(KeyCode::Esc));

        let command = app.on_key(key(KeyCode::Char('y')));

        assert_eq!(command, None);
        assert_eq!(app.yank_note.as_deref(), Some("nothing to yank"));
    }

    #[test]
    fn y_is_ignored_while_streaming() {
        let mut app = App::new();
        app.transcript.push(Block::Arc {
            text: "hello".to_owned(),
            partial: false,
        });
        app.on_key(key(KeyCode::Esc));
        app.status = Status::Streaming;

        let command = app.on_key(key(KeyCode::Char('y')));

        assert_eq!(command, None);
        assert_eq!(app.yank_note, None);
    }

    #[test]
    fn y_is_ignored_while_a_popup_is_open() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running)]);
        app.transcript.push(Block::Arc {
            text: "hello".to_owned(),
            partial: false,
        });

        let command = app.on_key(key(KeyCode::Char('y')));

        assert_eq!(command, None);
        assert_eq!(app.yank_note, None);
    }

    #[test]
    fn the_yank_note_clears_on_the_next_key() {
        let mut app = App::new();
        app.transcript.push(Block::Arc {
            text: "hello".to_owned(),
            partial: false,
        });
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('y')));
        assert_eq!(app.yank_note.as_deref(), Some("yanked"));

        app.on_key(key(KeyCode::Char('j')));

        assert_eq!(app.yank_note, None);
    }

    fn conversation() -> App {
        let mut app = App::new();
        app.transcript.push(Block::You("first question".to_owned()));
        app.transcript.push(Block::Arc {
            text: "first answer".to_owned(),
            partial: false,
        });
        app.transcript.push(Block::Tool {
            call_id: "t1".to_owned(),
            name: "bash".to_owned(),
            args: "ls".to_owned(),
            outcome: Some("ok"),
        });
        app.transcript.push(Block::Cost {
            input_tokens: 10,
            output_tokens: 20,
            seconds: 1.0,
        });
        app.transcript
            .push(Block::You("second question".to_owned()));
        app.transcript.push(Block::Arc {
            text: "second answer".to_owned(),
            partial: false,
        });
        app.on_key(key(KeyCode::Esc));
        app
    }

    #[test]
    fn v_then_y_yanks_only_the_last_block() {
        let mut app = conversation();

        app.on_key(key(KeyCode::Char('V')));
        assert_eq!(app.mode, Mode::Visual);

        let command = app.on_key(key(KeyCode::Char('y')));

        assert_eq!(
            command,
            Some(Command::Yank("arc: second answer".to_owned()))
        );
        assert_eq!(app.mode, Mode::Normal, "y exits visual mode");
        assert_eq!(app.yank_note.as_deref(), Some("yanked"));
    }

    #[test]
    fn v_k_y_yanks_the_last_two_blocks() {
        let mut app = conversation();

        app.on_key(key(KeyCode::Char('V')));
        app.on_key(key(KeyCode::Char('k')));
        let command = app.on_key(key(KeyCode::Char('y')));

        assert_eq!(
            command,
            Some(Command::Yank(
                "you: second question\n\narc: second answer".to_owned()
            ))
        );
    }

    #[test]
    fn v_gg_y_yanks_from_the_first_block() {
        let mut app = conversation();

        app.on_key(key(KeyCode::Char('V')));
        app.on_key(key(KeyCode::Char('g')));
        app.on_key(key(KeyCode::Char('g')));
        let command = app.on_key(key(KeyCode::Char('y')));

        assert_eq!(
            command,
            Some(Command::Yank(
                "you: first question\n\narc: first answer\n\nyou: second question\n\narc: second answer"
                    .to_owned()
            )),
            "tools and costs between them are skipped"
        );
    }

    #[test]
    fn v_gg_g_returns_the_boundary_to_the_last_block() {
        let mut app = conversation();

        app.on_key(key(KeyCode::Char('V')));
        app.on_key(key(KeyCode::Char('g')));
        app.on_key(key(KeyCode::Char('g')));
        app.on_key(key(KeyCode::Char('G')));
        let command = app.on_key(key(KeyCode::Char('y')));

        assert_eq!(
            command,
            Some(Command::Yank("arc: second answer".to_owned())),
            "G snaps back to the anchor"
        );
    }

    #[test]
    fn shift_y_yanks_the_whole_conversation_skipping_tools_and_costs() {
        let mut app = conversation();

        let command = app.on_key(key(KeyCode::Char('Y')));

        assert_eq!(
            command,
            Some(Command::Yank(
                "you: first question\n\narc: first answer\n\nyou: second question\n\narc: second answer"
                    .to_owned()
            ))
        );
        assert_eq!(app.mode, Mode::Normal, "Y never enters visual mode");
    }

    #[test]
    fn esc_leaves_visual_mode_without_yanking() {
        let mut app = conversation();
        app.on_key(key(KeyCode::Char('V')));

        let command = app.on_key(key(KeyCode::Esc));

        assert_eq!(command, None);
        assert_eq!(app.mode, Mode::Normal);
    }

    #[test]
    fn a_history_rebuild_while_in_visual_exits_it() {
        let mut app = conversation();
        app.session_id = Some("s-1".to_owned());
        app.on_key(key(KeyCode::Char('V')));
        assert_eq!(app.mode, Mode::Visual);

        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![prose_entry(Role::User as i32, "fresh", false)],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });

        assert_eq!(app.mode, Mode::Normal);
    }

    #[test]
    fn an_append_only_rebuild_keeps_visual_mode() {
        let mut app = conversation();
        app.session_id = Some("s-1".to_owned());
        let base = vec![
            prose_entry(Role::User as i32, "hi", false),
            prose_entry(Role::Assistant as i32, "hello", false),
        ];
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: base.clone(),
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });
        app.on_key(key(KeyCode::Char('V')));
        assert_eq!(app.mode, Mode::Visual);

        let mut grown = base;
        grown.push(prose_entry(Role::User as i32, "one more", false));
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: grown,
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });

        assert_eq!(
            app.mode,
            Mode::Visual,
            "an append-only rebuild keeps the selection"
        );
    }

    #[test]
    fn v_is_ignored_while_streaming() {
        let mut app = conversation();
        app.status = Status::Streaming;

        assert_eq!(app.on_key(key(KeyCode::Char('V'))), None);
        assert_eq!(app.mode, Mode::Normal);
    }

    #[test]
    fn v_is_ignored_while_a_popup_is_open() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running)]);
        app.transcript.push(Block::Arc {
            text: "hello".to_owned(),
            partial: false,
        });

        assert_eq!(app.on_key(key(KeyCode::Char('V'))), None);
        assert_ne!(app.mode, Mode::Visual);
    }

    // built through History, not raw pushes: only that path threads real seqs
    fn conversation_from_history() -> App {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![
                prose_entry_at(3, Role::User as i32, "question", false),
                prose_entry_at(4, Role::Assistant as i32, "answer", false),
                call_entry("t1", "bash"),
                result_entry("t1", ToolOutcome::Ok as i32),
            ],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });
        app.on_key(key(KeyCode::Esc));
        app
    }

    fn run_fork_command(app: &mut App) -> Option<Command> {
        app.on_key(key(KeyCode::Char(':')));
        for c in "fork".chars() {
            app.on_key(key(KeyCode::Char(c)));
        }
        app.on_key(key(KeyCode::Enter))
    }

    #[test]
    fn fork_on_a_selected_message_block_sends_the_command_with_its_seq() {
        let mut app = conversation_from_history();

        app.on_key(key(KeyCode::Char('V')));
        assert_eq!(app.mode, Mode::Visual);
        // boundary starts on the folded tool block (last); one 'k' lands on
        // the assistant's answer, seq 4
        app.on_key(key(KeyCode::Char('k')));
        let command = run_fork_command(&mut app);

        assert_eq!(
            command,
            Some(Command::ForkSession {
                session_id: "s-1".to_owned(),
                fork_point: 4,
            })
        );
        assert_eq!(app.mode, Mode::Normal, "the command consumes the selection");
    }

    #[test]
    fn fork_on_a_tool_block_is_an_instructive_error() {
        let mut app = conversation_from_history();

        app.on_key(key(KeyCode::Char('V')));
        // boundary starts on the last block: the tool result folded into its call
        let command = run_fork_command(&mut app);

        assert_eq!(command, None);
        assert!(
            app.last_error.is_some(),
            "the block has a seq but is not a message"
        );
    }

    #[test]
    fn fork_outside_visual_mode_is_an_instructive_error() {
        let mut app = conversation_from_history();

        let command = run_fork_command(&mut app);

        assert_eq!(command, None);
        assert!(app.last_error.is_some());
    }

    #[test]
    fn a_session_forked_reply_opens_it_like_the_picker_opens_a_session() {
        let mut app = conversation_from_history();

        let command = app.on_net(NetEvent::SessionForked {
            session_id: "s-2".to_owned(),
        });

        assert_eq!(app.session_id.as_deref(), Some("s-2"));
        assert_eq!(
            command,
            Some(Command::History {
                session_id: "s-2".to_owned(),
            })
        );
    }

    #[test]
    fn f_in_visual_mode_forks_exactly_like_colon_fork() {
        let mut app = conversation_from_history();

        app.on_key(key(KeyCode::Char('V')));
        // boundary starts on the folded tool block (last); one 'k' lands on
        // the assistant's answer, seq 4
        app.on_key(key(KeyCode::Char('k')));
        let command = app.on_key(key(KeyCode::Char('f')));

        assert_eq!(
            command,
            Some(Command::ForkSession {
                session_id: "s-1".to_owned(),
                fork_point: 4,
            })
        );
        assert_eq!(app.mode, Mode::Normal, "the key consumes the selection");
    }

    // built through History with two full turns, so a preceding message
    // block exists for rewind to fork before
    fn conversation_with_two_turns() -> App {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![
                prose_entry_at(1, Role::User as i32, "first", false),
                prose_entry_at(2, Role::Assistant as i32, "first reply", false),
                prose_entry_at(3, Role::User as i32, "second", false),
            ],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });
        app.on_key(key(KeyCode::Esc));
        app
    }

    #[test]
    fn r_positions_on_the_last_you_block_and_walks_only_you_blocks() {
        let mut app = conversation_with_two_turns();

        app.on_key(key(KeyCode::Char('R')));

        assert_eq!(app.mode, Mode::Visual);
        assert_eq!(
            app.visual_range(),
            Some((2, 2)),
            "starts on the last You block"
        );

        app.on_key(key(KeyCode::Char('k')));
        assert_eq!(
            app.visual_range(),
            Some((0, 0)),
            "walks past the Arc reply straight to the earlier You block"
        );

        app.on_key(key(KeyCode::Char('k')));
        assert_eq!(
            app.visual_range(),
            Some((0, 0)),
            "no earlier You block: stays put"
        );
    }

    #[test]
    fn rewind_enter_forks_before_the_chosen_message_and_prefills_it() {
        let mut app = conversation_with_two_turns();
        app.on_key(key(KeyCode::Char('R')));

        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(
            command,
            Some(Command::ForkSession {
                session_id: "s-1".to_owned(),
                fork_point: 2,
            }),
            "forks at the preceding reply's seq, excluding the chosen message"
        );
        assert_eq!(app.mode, Mode::Normal);

        let followup = app.on_net(NetEvent::SessionForked {
            session_id: "s-2".to_owned(),
        });

        assert_eq!(
            followup,
            Some(Command::History {
                session_id: "s-2".to_owned(),
            })
        );
        assert_eq!(app.input, "second", "the chosen message refills the input");
        assert_eq!(app.cursor, app.input.len());
        assert_eq!(app.mode, Mode::Insert);
    }

    #[test]
    fn rewind_on_the_first_message_is_an_instructive_error() {
        let mut app = conversation_from_history();

        app.on_key(key(KeyCode::Char('R')));
        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(command, None);
        assert!(app.last_error.is_some());
        assert_eq!(app.mode, Mode::Normal);
    }

    fn search(app: &mut App, query: &str) {
        app.on_key(key(KeyCode::Esc));
        assert_eq!(app.on_key(key(KeyCode::Char('/'))), None);
        typed(app, query);
        assert_eq!(app.on_key(key(KeyCode::Enter)), None);
    }

    fn normal_app() -> App {
        let mut app = App::new();
        app.on_key(key(KeyCode::Esc));
        app
    }

    #[test]
    fn slash_opens_the_search_prompt_and_esc_restores_the_draft() {
        let mut app = conversation();
        normal(&mut app, "i");
        typed(&mut app, "a draft");
        app.on_key(key(KeyCode::Esc));

        app.on_key(key(KeyCode::Char('/')));
        assert!(app.searching, "the prompt owns the input line");
        assert_eq!(app.input, "", "the draft is stashed");
        assert_eq!(app.cursor, 0, "the query starts empty");
        assert!(app.search.is_none(), "only a confirm starts a search");

        typed(&mut app, "needle");
        assert_eq!(app.input, "needle", "typing edits the query");

        app.on_key(key(KeyCode::Esc));
        assert!(!app.searching);
        assert_eq!(app.input, "a draft", "the draft is restored");
        assert_eq!(app.cursor, "a draft".len());
        assert!(app.search.is_none());
    }

    #[test]
    fn a_confirmed_search_starts_on_the_newest_match() {
        let mut app = conversation();
        search(&mut app, "question");

        assert!(!app.searching);
        let found = app.search.as_ref().expect("confirmed");
        assert_eq!(found.matches, vec![4, 0], "newest first");
        assert_eq!(found.current, 0);
        assert_eq!(app.search_block(), Some(4));
        assert_eq!(app.yank_note.as_deref(), Some("match 1/2"));
    }

    #[test]
    fn n_and_n_walk_older_and_newer_and_stop_at_the_ends() {
        let mut app = normal_app();
        for i in 0..3 {
            app.transcript.push(Block::You(format!("needle {i}")));
        }
        search(&mut app, "needle");
        assert_eq!(app.search_block(), Some(2), "1 is the newest");

        app.on_key(key(KeyCode::Char('n')));
        assert_eq!(app.search_block(), Some(1));
        assert_eq!(app.yank_note.as_deref(), Some("match 2/3"));
        app.on_key(key(KeyCode::Char('n')));
        assert_eq!(app.search_block(), Some(0), "the oldest block");
        assert_eq!(app.yank_note.as_deref(), Some("match 3/3"));
        app.on_key(key(KeyCode::Char('n')));
        assert_eq!(app.search_block(), Some(0), "the oldest stops the walk");

        app.on_key(key(KeyCode::Char('N')));
        assert_eq!(app.search_block(), Some(1));
        assert_eq!(app.yank_note.as_deref(), Some("match 2/3"));
        app.on_key(key(KeyCode::Char('N')));
        assert_eq!(app.search_block(), Some(2));
        assert_eq!(app.yank_note.as_deref(), Some("match 1/3"));
        app.on_key(key(KeyCode::Char('N')));
        assert_eq!(app.search_block(), Some(2), "the newest stops the walk");
    }

    #[test]
    fn n_and_n_do_nothing_without_a_confirmed_search() {
        let mut app = conversation();
        app.on_key(key(KeyCode::Char('n')));
        app.on_key(key(KeyCode::Char('N')));
        assert!(app.search.is_none());
        assert_eq!(app.yank_note, None);
    }

    #[test]
    fn chrome_never_matches() {
        let mut app = normal_app();
        app.transcript = vec![
            Block::Thought {
                text: "secret trace words".to_owned(),
                seconds: 3,
                done: true,
                open: true,
            },
            Block::Tool {
                call_id: "t1".to_owned(),
                name: "bash".to_owned(),
                args: "secret.sh".to_owned(),
                outcome: Some("ok"),
            },
            Block::Cost {
                input_tokens: 1,
                output_tokens: 2,
                seconds: 1.0,
            },
            Block::Note("secret note".to_owned()),
            Block::StepCapped,
            Block::Fault {
                code: "boom".to_owned(),
                msg: "secret failure".to_owned(),
            },
            Block::Handback {
                subject: "Job 1234 finished.".to_owned(),
                body: "secret handback".to_owned(),
                open: true,
            },
            Block::You("plain words".to_owned()),
        ];
        search(&mut app, "secret");

        assert!(app.search.is_none());
        assert_eq!(app.yank_note.as_deref(), Some("no match"));
    }

    #[test]
    fn a_no_match_search_leaves_the_view_alone() {
        let mut app = conversation();
        app.scroll_back = 3;
        let transcript = app.transcript.clone();

        search(&mut app, "absent everywhere");

        assert!(app.search.is_none());
        assert_eq!(app.scroll_back, 3, "the view stays put");
        assert_eq!(app.transcript, transcript);
        assert_eq!(app.yank_note.as_deref(), Some("no match"));
    }

    #[test]
    fn ctrl_n_steps_matches_live_while_the_prompt_is_open() {
        let mut app = conversation();
        normal(&mut app, "/");
        typed(&mut app, "question");

        app.on_key(ctrl('n'));
        assert!(app.searching, "the prompt stays open");
        assert_eq!(app.search_block(), Some(4), "first press lands newest");
        assert_eq!(app.yank_note.as_deref(), Some("match 1/2"));

        app.on_key(ctrl('n'));
        assert_eq!(app.search_block(), Some(0));
        assert_eq!(app.yank_note.as_deref(), Some("match 2/2"));
        app.on_key(ctrl('p'));
        assert_eq!(app.yank_note.as_deref(), Some("match 1/2"));

        let command = app.on_key(key(KeyCode::Enter));
        assert_eq!(command, None);
        assert!(!app.searching);
        assert_eq!(
            app.yank_note.as_deref(),
            Some("match 1/2"),
            "enter keeps the stepped position"
        );
    }

    #[test]
    fn an_edited_query_recomputes_on_the_next_live_step() {
        let mut app = conversation();
        normal(&mut app, "/");
        typed(&mut app, "question");
        app.on_key(ctrl('n'));
        app.on_key(ctrl('n'));
        assert_eq!(app.yank_note.as_deref(), Some("match 2/2"));

        typed(&mut app, "zzz");
        app.on_key(ctrl('n'));
        assert_eq!(app.yank_note.as_deref(), Some("no match"));
        assert_eq!(app.search_block(), None);
    }

    #[test]
    fn ctrl_n_and_p_navigate_the_picker_instead_of_leaving_it() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![session("a"), session("b")]));
        app.on_key(ctrl('p'));
        let before = app.picker.as_ref().expect("open").selected;
        app.on_key(ctrl('n'));
        assert!(app.picker.is_some(), "ctrl-n stays in the picker");
        assert_eq!(app.picker.as_ref().expect("open").selected, before + 1);
        app.on_key(ctrl('p'));
        assert!(app.picker.is_some(), "ctrl-p navigates, not reopens");
        assert_eq!(app.picker.as_ref().expect("open").selected, before);
    }

    #[test]
    fn esc_in_normal_mode_clears_the_search() {
        let mut app = conversation();
        search(&mut app, "question");
        assert!(app.search.is_some());

        app.on_key(key(KeyCode::Esc));
        assert!(app.search.is_none());
        assert_eq!(app.search_block(), None);
    }

    #[test]
    fn esc_while_streaming_emits_cancel_turn_for_the_open_session() {
        let mut app = App::new();
        app.mode = Mode::Normal;
        app.session_id = Some("s-live".to_owned());
        app.status = Status::Streaming;

        let command = app.on_key(key(KeyCode::Esc));

        assert_eq!(
            command,
            Some(Command::CancelTurn {
                session_id: "s-live".to_owned()
            })
        );
    }

    #[test]
    fn esc_while_streaming_with_no_named_session_yet_is_a_no_op() {
        let mut app = App::new();
        app.mode = Mode::Normal;
        app.status = Status::Streaming;

        assert_eq!(app.on_key(key(KeyCode::Esc)), None);
    }

    #[test]
    fn esc_while_idle_keeps_clearing_the_search_not_cancelling() {
        let mut app = conversation();
        app.session_id = Some("s-live".to_owned());
        search(&mut app, "question");
        assert!(app.search.is_some());
        assert_eq!(app.status, Status::Idle);

        let command = app.on_key(key(KeyCode::Esc));

        assert_eq!(command, None, "idle Esc never cancels a turn");
        assert!(app.search.is_none());
    }

    #[test]
    fn a_new_search_replaces_the_old_one() {
        let mut app = conversation();
        search(&mut app, "question");
        search(&mut app, "answer");

        let found = app.search.as_ref().expect("replaced");
        assert_eq!(found.matches, vec![5, 1]);
        assert_eq!(app.yank_note.as_deref(), Some("match 1/2"));
    }

    #[test]
    fn an_append_only_history_rebuild_keeps_the_search() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        let entries = vec![prose_entry(Role::User as i32, "find the needle", false)];
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: entries.clone(),
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });
        search(&mut app, "needle");
        assert!(app.search.is_some());
        let block = app.search_block();

        let mut appended = entries;
        appended.push(prose_entry(
            Role::Assistant as i32,
            "no needle in here",
            false,
        ));
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: appended,
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });
        assert!(
            app.search.is_some(),
            "the old transcript is a prefix of the new one"
        );
        assert_eq!(app.search_block(), block, "the same block stays selected");
    }

    #[test]
    fn a_changed_prefix_history_rebuild_drops_the_search() {
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());
        let entries = vec![prose_entry(Role::User as i32, "find the needle", false)];
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries,
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });
        search(&mut app, "needle");
        assert!(app.search.is_some());

        let rewritten = vec![prose_entry(
            Role::User as i32,
            "edited: find the needle",
            false,
        )];
        app.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: rewritten,
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });
        assert!(app.search.is_none(), "a changed prefix drops it");
    }

    #[test]
    fn opening_another_session_drops_the_search() {
        let mut app = conversation();
        search(&mut app, "question");

        app.on_key(key(KeyCode::Char('s')));
        app.on_key(key(KeyCode::Enter));
        assert!(app.search.is_none());
    }

    #[test]
    fn search_keys_keep_their_meaning_in_insert_mode() {
        let mut app = App::new();
        typed(&mut app, "/nN");

        assert_eq!(app.input, "/nN", "they type into the message");
        assert!(!app.searching);
        assert!(app.search.is_none());
    }

    #[test]
    fn slash_in_the_picker_still_starts_the_filter() {
        let mut app = conversation();
        search(&mut app, "question");
        app.on_key(key(KeyCode::Char('s')));

        app.on_key(key(KeyCode::Char('/')));
        assert!(
            app.picker.as_ref().expect("picker is open").filtering,
            "the picker filter, not the search prompt"
        );
        assert!(!app.searching);
        assert!(app.search.is_some(), "the confirmed search survives");
    }
}
