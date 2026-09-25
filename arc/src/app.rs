use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Instant;

use arc_core::projection::REVIEW_WINDOW_MICROS;
use arc_proto::v1::{
    HistoryEntry, HistoryMessage, ImageAttachment, JobInfo, ModelChoice, ProjectInfo, Role,
    SessionInfo, SessionRole, Source, ToolOutcome, branch_marked, history_entry, job_info,
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
        attachments: Vec<ImageAttachment>,
    },
    SendLive {
        session_id: String,
        content: String,
        attachments: Vec<ImageAttachment>,
    },
    ReviewList {
        since_micros: i64,
    },
    MemoryList,
    ReviewAccept {
        record_id: String,
    },
    ReviewDelete {
        record_id: String,
    },
    ListJobs,
    ListModels,
    SelectModel {
        role: SessionRole,
        choice: String,
    },
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
        choice: String,
        working_directory: String,
    },
    ForkSession {
        session_id: String,
        fork_point: u64,
        choice: String,
    },
    MarkBranch {
        session_id: String,
        disposition: branch_marked::Disposition,
    },
    CompactSession {
        session_id: String,
    },
    Yank(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum NetEvent {
    SessionStatus(arc_proto::v1::SessionStatus),
    StatusUnavailable(String),
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
    AttachmentsAccepted(Vec<ImageAttachment>),
    AttachmentsFailed(Vec<ImageAttachment>),
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
        content: String,
    },
    End {
        partial: bool,
        input_tokens: u32,
        output_tokens: u32,
        step_capped: bool,
        grounding_json: String,
        queued: bool,
    },
    Failed {
        code: String,
        msg: String,
    },
    Disconnected {
        reason: String,
    },
    ReviewItems(Vec<ReviewEntry>),
    MemoryItems(Vec<ReviewEntry>),
    ReviewChanged(u32),
    JobItems(Vec<JobInfo>),
    ProjectsSeeded(Vec<ProjectInfo>),
    ModelItems(Vec<ModelChoice>),
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
    Compacted {
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
    pub supersedes: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Review {
    pub items: Vec<ReviewEntry>,
    pub selected: usize,
    pub loaded: bool,
    pub pending_delete: bool,
    pub all: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Picker {
    pub selected: usize,
    pub filtering: bool,
    pub show_all: bool,
    pub show_abandoned: bool,
    pub tree: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Models {
    pub items: Vec<ModelChoice>,
    pub selected: usize,
    pub loaded: bool,
    pub default: bool,
    pub role: SessionRole,
    pub recorded_model: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Jobs {
    pub items: Vec<JobInfo>,
    pub selected: usize,
    pub loaded: bool,
    pub confirmation: Option<String>,
}

enum PendingModel {
    Create(SessionRole, String),
    Fork(SessionRole, String),
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
        content: String,
        open: bool,
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Overlay {
    #[default]
    None,
    Help {
        scroll: usize,
    },
    Picker(Picker),
    Review(Review),
    Jobs(Jobs),
    Models(Models),
    SessionStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub block: Block,
    pub seq: Option<u64>,
}

impl From<Block> for Entry {
    fn from(block: Block) -> Self {
        Self { block, seq: None }
    }
}

pub struct App {
    pub herdr_enabled: bool,
    pub transcript: Vec<Entry>,
    pub show_details: bool,
    session_details: HashMap<String, bool>,
    pub input: String,
    pub cursor: usize,
    pub mode: Mode,
    pub cmd: String,
    pending: Option<char>,
    pub session_id: Option<String>,
    pub sessions: Vec<SessionInfo>,
    pub session_status: HashMap<String, arc_proto::v1::SessionStatus>,
    pub picker_tree: bool,
    pub overlay: Overlay,
    pub viewport_anchor: Option<(usize, usize)>,
    pub restore_anchor: bool,
    pub details_held_focus: Option<usize>,
    pub visible_blocks: Vec<usize>,
    pending_project: Option<String>,
    pub search: Option<Search>,
    pub searching: bool,
    pub status: Status,
    pub last_error: Option<String>,
    pub yank_note: Option<String>,
    /// Count of turns this client is inside; `Status::Streaming` iff > 0.
    /// `Accepted` increments, `StreamEnd`/error decrements, `Disconnected`
    /// resets — two sockets can each hold one at once.
    live_streams: u32,
    /// A message typed while streaming but before the session id is known
    /// (the first message of a brand-new session): sent as `Send` once an
    /// `Accepted` names the session, the way `pending_first` already works.
    pending_live: Option<(String, Vec<ImageAttachment>)>,
    pending_attachments: Vec<ImageAttachment>,
    sending_attachments: VecDeque<Vec<ImageAttachment>>,
    thinking_since: Option<Instant>,
    turn_started: Option<Instant>,
    streamed_chars: usize,
    pub scroll_back: usize,
    pub quit: bool,
    pub ambient: Vec<JobInfo>,
    strip_since: Instant,
    refetch_in_flight: bool,
    previous_session: Option<String>,
    picker_filter_stash: Option<String>,
    search_stash: Option<String>,
    visual_anchor: usize,
    visual_point: bool,
    visual_boundary: usize,
    visual_at_cmd: Option<usize>,
    visual_rewind: bool,
    pending_rewind_text: Option<String>,
    session_meta: HashMap<String, (SessionRole, String, Source)>,
    pending_first: Option<(String, Vec<ImageAttachment>)>,
    pending_model: Option<PendingModel>,
    pub review_pending: u32,
    launch_dir: Option<PathBuf>,
}

impl App {
    #[cfg(test)]
    pub fn block_contents(&self) -> Vec<Block> {
        self.transcript
            .iter()
            .map(|entry| entry.block.clone())
            .collect()
    }

    #[cfg(test)]
    pub fn set_blocks(&mut self, blocks: Vec<Block>) {
        self.transcript = blocks.into_iter().map(Entry::from).collect();
    }

    pub fn picker(&self) -> Option<&Picker> {
        match &self.overlay {
            Overlay::Picker(value) => Some(value),
            _ => None,
        }
    }
    pub fn picker_mut(&mut self) -> Option<&mut Picker> {
        match &mut self.overlay {
            Overlay::Picker(value) => Some(value),
            _ => None,
        }
    }
    pub fn review(&self) -> Option<&Review> {
        match &self.overlay {
            Overlay::Review(value) => Some(value),
            _ => None,
        }
    }
    pub fn review_mut(&mut self) -> Option<&mut Review> {
        match &mut self.overlay {
            Overlay::Review(value) => Some(value),
            _ => None,
        }
    }
    pub fn jobs(&self) -> Option<&Jobs> {
        match &self.overlay {
            Overlay::Jobs(value) => Some(value),
            _ => None,
        }
    }
    pub fn jobs_mut(&mut self) -> Option<&mut Jobs> {
        match &mut self.overlay {
            Overlay::Jobs(value) => Some(value),
            _ => None,
        }
    }
    pub fn models_mut(&mut self) -> Option<&mut Models> {
        match &mut self.overlay {
            Overlay::Models(value) => Some(value),
            _ => None,
        }
    }

    pub fn pending_attachments(&self) -> &[ImageAttachment] {
        &self.pending_attachments
    }
    pub fn new() -> Self {
        Self {
            herdr_enabled: false,
            transcript: Vec::new(),
            show_details: false,
            session_details: HashMap::new(),
            input: String::new(),
            cursor: 0,
            mode: Mode::Insert,
            cmd: String::new(),
            pending: None,
            session_id: None,
            sessions: Vec::new(),
            session_status: HashMap::new(),
            picker_tree: false,
            overlay: Overlay::None,
            viewport_anchor: None,
            restore_anchor: false,
            details_held_focus: None,
            visible_blocks: Vec::new(),
            pending_project: None,
            search: None,
            searching: false,
            status: Status::Idle,
            last_error: None,
            yank_note: None,
            live_streams: 0,
            pending_live: None,
            pending_attachments: Vec::new(),
            sending_attachments: VecDeque::new(),
            thinking_since: None,
            turn_started: None,
            streamed_chars: 0,
            scroll_back: 0,
            quit: false,
            ambient: Vec::new(),
            strip_since: Instant::now(),
            refetch_in_flight: false,
            previous_session: None,
            picker_filter_stash: None,
            search_stash: None,
            visual_anchor: 0,
            visual_point: false,
            visual_boundary: 0,
            visual_at_cmd: None,
            visual_rewind: false,
            pending_rewind_text: None,
            session_meta: HashMap::new(),
            pending_first: None,
            pending_model: None,
            review_pending: 0,
            launch_dir: None,
        }
    }

    pub fn set_launch_dir(&mut self, dir: Option<PathBuf>) {
        self.launch_dir = dir;
    }

    pub(super) fn push_block(&mut self, mut block: Block) {
        set_details(&mut block, self.show_details);
        self.transcript.push(block.into());
    }

    fn pop_block(&mut self) -> Option<Block> {
        self.transcript.pop().map(|entry| entry.block)
    }

    pub fn on_scroll(&mut self, up: bool, lines: usize) {
        let move_row = |selected: &mut usize, len: usize| {
            *selected = if up {
                selected.saturating_sub(1)
            } else {
                (*selected + 1).min(len.saturating_sub(1))
            };
        };
        match &mut self.overlay {
            Overlay::Help { scroll } => {
                *scroll = if up {
                    scroll.saturating_sub(lines)
                } else {
                    scroll.saturating_add(lines)
                };
            }
            Overlay::Review(review) => {
                review.pending_delete = false;
                move_row(&mut review.selected, review.items.len());
            }
            Overlay::Jobs(jobs) => {
                jobs.confirmation = None;
                move_row(&mut jobs.selected, jobs.items.len());
            }
            Overlay::Models(models) => move_row(&mut models.selected, models.items.len()),
            Overlay::SessionStatus => {}
            Overlay::Picker(_) => self.move_picker_selection(up),
            Overlay::None => {
                self.scroll_back = if up {
                    self.scroll_back.saturating_add(lines)
                } else {
                    self.scroll_back.saturating_sub(lines)
                };
            }
        }
    }

    pub fn on_key(&mut self, key: KeyEvent) -> Option<Command> {
        self.details_held_focus = None;
        self.yank_note = None;
        match key.code {
            KeyCode::PageUp if self.picker().is_some() => {
                self.page_picker_selection(true);
                return None;
            }
            KeyCode::PageDown if self.picker().is_some() => {
                self.page_picker_selection(false);
                return None;
            }
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
        if self.overlay != Overlay::None && !self.picker().is_some_and(|picker| picker.filtering) {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => {
                    self.overlay = Overlay::None;
                    return None;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.on_scroll(true, 1);
                    return None;
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.on_scroll(false, 1);
                    return None;
                }
                _ => {}
            }
        }
        if let Overlay::Help { scroll } = &mut self.overlay {
            match key.code {
                KeyCode::Char('g') | KeyCode::Home => *scroll = 0,
                KeyCode::Char('G') | KeyCode::End => *scroll = usize::MAX,
                _ => {}
            }
            return None;
        }
        match self.overlay {
            Overlay::Review(_) => return self.on_review_key(key.code),
            Overlay::Jobs(_) => return self.on_jobs_key(key.code),
            Overlay::Models(_) => return self.on_models_key(key.code),
            Overlay::Picker(_) => return self.on_picker_key(key.code),
            Overlay::SessionStatus => return None,
            Overlay::None | Overlay::Help { .. } => {}
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

    pub fn on_paste(&mut self, text: &str) -> Option<Command> {
        self.yank_note = None;
        let text = text.replace("\r\n", "\n").replace('\r', "\n");
        if self.overlay != Overlay::None {
            return None;
        }
        let first_line = || text.lines().next().unwrap_or_default().to_owned();
        match self.mode {
            Mode::Cmd => self.cmd.push_str(&first_line()),
            _ if self.searching => self.insert_text(&first_line()),
            Mode::Insert | Mode::Normal => self.insert_text(&text),
            Mode::Visual => {}
        }
        None
    }

    fn insert_text(&mut self, text: &str) {
        self.input.insert_str(self.cursor, text);
        self.cursor += text.len();
    }

    fn on_control(&mut self, code: KeyCode) -> Option<Command> {
        if self.overlay != Overlay::None
            && !matches!(code, KeyCode::Char('c' | 'u' | 'd'))
            && !(self.picker().is_some() && matches!(code, KeyCode::Char('n' | 'p')))
        {
            return None;
        }
        match code {
            KeyCode::Char('c') => self.quit = true,
            KeyCode::Char('u') if self.picker().is_some() => self.page_picker_selection(true),
            KeyCode::Char('d') if self.picker().is_some() => self.page_picker_selection(false),
            KeyCode::Char('u') => self.on_scroll(true, PAGE),
            KeyCode::Char('d') => self.on_scroll(false, PAGE),
            KeyCode::Char('o') => self.toggle_open_blocks(),
            KeyCode::Char('t') if self.status != Status::Streaming => {
                return self.back_session();
            }
            KeyCode::Char('n') if self.searching => self.search_live_step(true),
            KeyCode::Char('p') if self.searching => self.search_live_step(false),
            KeyCode::Char('n') if self.picker().is_some() => self.move_picker_selection(false),
            KeyCode::Char('p') if self.picker().is_some() => self.move_picker_selection(true),
            KeyCode::Char('p') => return self.open_picker(),
            KeyCode::Char('n') if self.status != Status::Streaming => {
                return self.start_session(None);
            }
            KeyCode::Char('j') if self.mode == Mode::Insert && self.picker().is_none() => {
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
            KeyCode::Char('?') => self.overlay = Overlay::Help { scroll: 0 },
            KeyCode::Char('J') => return Some(self.open_jobs()),
            KeyCode::Char('Q') => return Some(self.open_review()),
            KeyCode::Char('M') => return self.open_models(false),
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
        let reply = self
            .transcript
            .iter()
            .rev()
            .find_map(|entry| match &entry.block {
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
        let last = if point {
            self.current_foldable().unwrap_or(last)
        } else {
            last
        };
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
            .rposition(|b| matches!(b.block, Block::You(_)))
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
            matches!(
                block,
                Block::You(_)
                    | Block::Arc { .. }
                    | Block::Tool { .. }
                    | Block::Thought { .. }
                    | Block::Handback { .. }
            )
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
            if self.is_visual_stop(&self.transcript[at].block) {
                self.visual_boundary = at;
                self.follow_point();
                return;
            }
        }
    }

    fn jump_point(&mut self, to_end: bool) {
        let found = if to_end {
            self.transcript
                .iter()
                .rposition(|entry| self.is_visual_stop(&entry.block))
        } else {
            self.transcript
                .iter()
                .position(|entry| self.is_visual_stop(&entry.block))
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
            .map(|entry| &entry.block)
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
                    "memory" => return Some(self.open_memory()),
                    "jobs" => return Some(self.open_jobs()),
                    "model" => return self.open_models(false),
                    "model-default" => return self.open_models(true),
                    "help" => self.overlay = Overlay::Help { scroll: 0 },
                    "status" => self.overlay = Overlay::SessionStatus,
                    "fork" => return self.fork_selected(),
                    "compact" => return self.compact_session(),
                    "attach clear" => self.clear_attachments(),
                    cmd => match cmd.strip_prefix("attach ") {
                        Some(path) => self.attach(path.trim()),
                        None => {
                            self.last_error = Some(format!("Unknown command :{cmd}; use :help"));
                        }
                    },
                }
            }
            _ => {}
        }
        None
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
            self.transcript.get(index).map(|entry| &entry.block),
            Some(Block::You(_) | Block::Arc { .. })
        );
        if let (true, Some(fork_point)) = (
            is_message,
            self.transcript.get(index).and_then(|entry| entry.seq),
        ) {
            Some(Command::ForkSession {
                session_id,
                fork_point,
                choice: String::new(),
            })
        } else {
            self.last_error =
                Some("fork: select a sent message, not a tool or live block".to_owned());
            None
        }
    }

    fn compact_session(&mut self) -> Option<Command> {
        let Some(session_id) = self.session_id.clone() else {
            self.last_error = Some("compact: no open session".to_owned());
            return None;
        };
        Some(Command::CompactSession { session_id })
    }

    fn attach(&mut self, path: &str) {
        if path.is_empty() {
            self.last_error = Some("attach: provide an image path".to_owned());
            return;
        }
        match arc_core::attachment::load(Path::new(path)) {
            Ok(attachment) => {
                self.pending_attachments.push(attachment);
                if let Err(error) = arc_core::attachment::validate(&mut self.pending_attachments) {
                    self.pending_attachments.pop();
                    self.last_error = Some(format!("attach: {error}"));
                    return;
                }
                self.last_error = None;
                self.yank_note = self
                    .pending_attachments
                    .last()
                    .map(|attachment| format!("attached {}", attachment.name));
            }
            Err(error) => self.last_error = Some(format!("attach: {error}")),
        }
    }

    fn clear_attachments(&mut self) {
        let count = self.pending_attachments.len();
        self.pending_attachments.clear();
        self.yank_note = Some(if count == 0 {
            "no pending images".to_owned()
        } else {
            format!("cleared {count} image attachment(s)")
        });
    }

    fn fork_selected_visual(&mut self) -> Option<Command> {
        self.visual_at_cmd = self.visual_boundary();
        self.mode = Mode::Normal;
        self.fork_selected()
    }

    fn rewind_fork(&mut self) -> Option<Command> {
        let index = self.visual_boundary;
        self.mode = Mode::Normal;
        let Some(Block::You(text)) = self.transcript.get(index).map(|entry| &entry.block) else {
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
            .rposition(|entry| matches!(entry.block, Block::You(_) | Block::Arc { .. }));
        let Some(fork_point) =
            preceding.and_then(|at| self.transcript.get(at).and_then(|entry| entry.seq))
        else {
            self.last_error = Some("rewind: no earlier message to fork before".to_owned());
            return None;
        };
        self.pending_rewind_text = Some(text);
        Some(Command::ForkSession {
            session_id,
            fork_point,
            choice: String::new(),
        })
    }

    fn open_review(&mut self) -> Command {
        self.overlay = Overlay::Review(Review {
            items: Vec::new(),
            selected: 0,
            loaded: false,
            pending_delete: false,
            all: false,
        });
        Command::ReviewList {
            since_micros: chrono::Utc::now().timestamp_micros() - REVIEW_WINDOW_MICROS,
        }
    }

    fn open_memory(&mut self) -> Command {
        self.overlay = Overlay::Review(Review {
            items: Vec::new(),
            selected: 0,
            loaded: false,
            pending_delete: false,
            all: true,
        });
        Command::MemoryList
    }

    fn on_review_key(&mut self, code: KeyCode) -> Option<Command> {
        if code == KeyCode::Char('r') {
            return Some(if self.review().expect("open").all {
                self.open_memory()
            } else {
                self.open_review()
            });
        }
        let review = self.review_mut().expect("review is open");
        match code {
            KeyCode::Char('a') if !review.all => {
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
            KeyCode::Char('f') if !review.all => {
                if let Some(entry) = review.items.get(review.selected) {
                    self.input = format!("fix memory {}: {} — ", entry.id, entry.title);
                    self.cursor = self.input.len();
                    self.mode = Mode::Insert;
                    self.overlay = Overlay::None;
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

    fn can_switch_session(&mut self) -> bool {
        if self.status == Status::Streaming {
            self.last_error = Some("Finish or stop the turn before switching sessions".to_owned());
            self.yank_note = self.last_error.clone();
            return false;
        }
        true
    }

    fn current_foldable(&self) -> Option<usize> {
        if let Some(at) = self.visual_boundary() {
            return self
                .transcript
                .get(at)
                .filter(|entry| foldable(&entry.block))
                .map(|_| at);
        }
        let mut visible: Vec<usize> = self
            .visible_blocks
            .iter()
            .copied()
            .filter(|&at| {
                self.transcript
                    .get(at)
                    .is_some_and(|entry| foldable(&entry.block))
            })
            .collect();
        if self.scroll_back == 0 {
            visible.reverse();
        }
        visible.first().copied().or_else(|| {
            self.transcript
                .iter()
                .rposition(|entry| foldable(&entry.block))
        })
    }

    fn open_models(&mut self, default: bool) -> Option<Command> {
        if !default && !self.can_switch_session() {
            return None;
        }
        if !default && self.pending_model.is_some() {
            self.last_error = Some("Wait for the model switch to finish".to_owned());
            return None;
        }
        if !default
            && self
                .session_id
                .as_deref()
                .is_some_and(|id| !self.session_meta.contains_key(id))
        {
            self.last_error = Some("Session metadata unavailable; retry after refresh".to_owned());
            return None;
        }
        let role = if default {
            SessionRole::Chat
        } else {
            self.session_id
                .as_deref()
                .and_then(|id| self.session_meta.get(id))
                .map_or(SessionRole::Chat, |(role, _, source)| {
                    if *source == Source::User {
                        SessionRole::Chat
                    } else {
                        *role
                    }
                })
        };
        if !default && role == SessionRole::Unspecified {
            self.last_error = Some("Session role unknown; cannot choose a model".to_owned());
            return None;
        }
        let recorded_model = self
            .session_id
            .as_ref()
            .and_then(|id| self.sessions.iter().find(|session| &session.id == id))
            .map(|session| session.model.clone());
        self.overlay = Overlay::Models(Models {
            items: Vec::new(),
            selected: 0,
            loaded: false,
            default,
            role,
            recorded_model,
        });
        Some(Command::ListModels)
    }

    fn on_models_key(&mut self, code: KeyCode) -> Option<Command> {
        if code != KeyCode::Enter {
            return None;
        }
        let models = self.models_mut()?;
        let choice = models.items.get(models.selected)?;
        let role = SessionRole::try_from(choice.role).unwrap_or(SessionRole::Unspecified);
        let name = choice.name.clone();
        let default = models.default;
        self.overlay = Overlay::None;
        if default {
            return Some(Command::SelectModel { role, choice: name });
        }
        let (role, project) = if let Some(session_id) = self.session_id.clone() {
            let Some((role, project, source)) = self.session_meta.get(&session_id).cloned() else {
                self.last_error =
                    Some("Session metadata unavailable; retry after refresh".to_owned());
                return None;
            };
            let fork_point = self.transcript.iter().rev().find_map(|entry| {
                matches!(entry.block, Block::You(_) | Block::Arc { .. })
                    .then_some(entry.seq)
                    .flatten()
            });
            if let Some(fork_point) = fork_point {
                self.pending_model = Some(PendingModel::Fork(role, project));
                return Some(Command::ForkSession {
                    session_id,
                    fork_point,
                    choice: name,
                });
            }
            if source == Source::Model {
                self.last_error =
                    Some("Job session has no message to fork; choose another session".to_owned());
                return None;
            }
            if self.transcript.iter().any(|entry| {
                entry.seq.is_some()
                    || matches!(&entry.block, Block::Note(text) if text == "loading")
            }) {
                self.last_error =
                    Some("No durable message to fork; retry after history loads".to_owned());
                return None;
            }
            (
                if source == Source::User {
                    SessionRole::Chat
                } else {
                    role
                },
                project,
            )
        } else {
            (
                SessionRole::Chat,
                self.pending_project.clone().unwrap_or_default(),
            )
        };
        self.pending_model = Some(PendingModel::Create(role, project.clone()));
        Some(Command::CreateSession {
            role,
            project,
            choice: name,
            working_directory: self
                .launch_dir
                .as_ref()
                .map_or_else(String::new, |dir| dir.to_string_lossy().into_owned()),
        })
    }

    fn open_jobs(&mut self) -> Command {
        self.overlay = Overlay::Jobs(Jobs {
            items: Vec::new(),
            selected: 0,
            loaded: false,
            confirmation: None,
        });
        Command::ListJobs
    }

    fn on_jobs_key(&mut self, code: KeyCode) -> Option<Command> {
        let jobs = self.jobs_mut().expect("jobs is open");
        jobs.confirmation = None;
        match code {
            KeyCode::Char('r') => return Some(Command::ListJobs),
            KeyCode::Enter => {
                let session_id = self.selected_job();
                self.overlay = Overlay::None;
                return self.start_session(session_id);
            }
            KeyCode::Char('x') => return self.cancel_selected_job(),
            KeyCode::Char('d') => return self.drop_selected_steers(),
            _ => {}
        }
        None
    }

    fn cancel_selected_job(&mut self) -> Option<Command> {
        let jobs = self.jobs_mut().expect("jobs is open");
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
        let jobs = self.jobs_mut().expect("jobs is open");
        let job = jobs.items.get(jobs.selected)?;
        if !is_running(job) || job.queued_steers == 0 {
            return None;
        }
        let session_id = job.session_id.clone();
        jobs.confirmation = Some(format!("dropped {}", job.queued_steers));
        Some(Command::DropSteers { session_id })
    }

    fn selected_job(&self) -> Option<String> {
        let jobs = self.jobs()?;
        jobs.items
            .get(jobs.selected)
            .map(|job| job.session_id.clone())
    }

    fn on_picker_key(&mut self, code: KeyCode) -> Option<Command> {
        if self.picker().expect("picker is open").filtering {
            return self.on_picker_filter_key(code);
        }
        let selected = self.picker().expect("picker is open").selected;
        match code {
            KeyCode::Tab => self.toggle_picker_tree(),
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
                self.overlay = Overlay::None;
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
            KeyCode::Tab => self.toggle_picker_tree(),
            KeyCode::Up => self.move_picker_selection(true),
            KeyCode::Down => self.move_picker_selection(false),
            _ => {
                self.edit_input(code);
                let selected =
                    usize::from(!self.input.is_empty() && !self.picker_rows().is_empty());
                self.picker_mut().expect("picker").selected = selected;
                self.clamp_picker_selection();
            }
        }
        None
    }

    fn toggle_picker_show_abandoned(&mut self) {
        if let Some(picker) = self.picker_mut() {
            picker.show_abandoned = !picker.show_abandoned;
            picker.selected = 0;
        }
    }

    fn toggle_picker_tree(&mut self) {
        let selected_id = self
            .picker()
            .and_then(|picker| picker.selected.checked_sub(1))
            .and_then(|i| self.picker_rows().get(i).map(|s| s.id.clone()));
        let picker = self.picker_mut().expect("picker is open");
        picker.tree = !picker.tree;
        self.picker_tree = picker.tree;
        let position =
            selected_id.and_then(|id| self.picker_rows().iter().position(|s| s.id == id));
        self.picker_mut().expect("picker is open").selected = match position {
            Some(row) => row + 1,
            None => 0,
        };
    }

    fn toggle_picker_show_all(&mut self) {
        let picker = self.picker_mut().expect("picker is open");
        picker.show_all = !picker.show_all;
        picker.selected = 0;
    }

    fn start_picker_filter(&mut self) {
        self.picker_filter_stash = Some(std::mem::take(&mut self.input));
        self.cursor = 0;
        self.picker_mut().expect("picker is open").filtering = true;
    }

    fn cancel_picker_filter(&mut self) {
        self.picker_mut().expect("picker is open").filtering = false;
        self.input = self.picker_filter_stash.take().unwrap_or_default();
        self.cursor = self.input.len();
        self.clamp_picker_selection();
    }

    fn open_filtered_session(&mut self) -> Option<Command> {
        let selected = self.picker().expect("picker is open").selected;
        let chosen = self.picker_session(selected).map(|s| s.id.clone());
        self.overlay = Overlay::None;
        self.input = self.picker_filter_stash.take().unwrap_or_default();
        self.cursor = self.input.len();
        self.start_session(chosen)
    }

    fn move_picker_selection(&mut self, up: bool) {
        let selected = self.picker().expect("picker is open").selected;
        let last = self.picker_rows().len();
        self.picker_mut().expect("picker is open").selected = if up {
            selected.saturating_sub(1)
        } else {
            (selected + 1).min(last)
        };
    }

    fn page_picker_selection(&mut self, up: bool) {
        let selected = self.picker().expect("picker is open").selected;
        let last = self.picker_rows().len();
        let step = PAGE.min(last.max(1));
        self.picker_mut().expect("picker is open").selected = if up {
            selected.saturating_sub(step)
        } else {
            (selected + step).min(last)
        };
    }

    fn clamp_picker_selection(&mut self) {
        let last = self.picker_rows().len();
        let picker = self.picker_mut().expect("picker is open");
        picker.selected = picker.selected.min(last);
    }

    // refreshes on every open: a branch forked seconds ago must be in the tree
    fn open_picker(&mut self) -> Option<Command> {
        if self.status != Status::Streaming && self.review().is_none() && self.jobs().is_none() {
            self.overlay = Overlay::Picker(Picker {
                selected: 0,
                filtering: false,
                show_all: false,
                show_abandoned: false,
                tree: self.picker_tree,
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
        if self.picker().is_some_and(|picker| picker.tree) {
            self.picker_tree_rows()
                .into_iter()
                .map(|(session, _)| session)
                .collect()
        } else {
            self.picker_candidates()
        }
    }

    /// `picker_candidates`, paired with the lineage annotation: the parent's
    /// id prefix for a branch, `None` for a root. Position is pure recency —
    /// the freshest work is the first row; lineage is information, not
    /// hierarchy (decided 2026-08-29, after nesting made the dead parent
    /// the click target).
    pub fn picker_flat_rows(&self) -> Vec<(&SessionInfo, Option<String>)> {
        self.picker_candidates()
            .into_iter()
            .map(|session| {
                let parent = (!session.parent_session.is_empty())
                    .then(|| session.parent_session.chars().take(8).collect());
                (session, parent)
            })
            .collect()
    }

    pub fn picker_tree_rows(&self) -> Vec<(&SessionInfo, Vec<bool>)> {
        fn walk<'a>(
            session: &'a SessionInfo,
            flags: &[bool],
            children: &HashMap<&'a str, Vec<&'a SessionInfo>>,
            rows: &mut Vec<(&'a SessionInfo, Vec<bool>)>,
            visited: &mut HashSet<&'a str>,
        ) {
            if !visited.insert(session.id.as_str()) {
                return;
            }
            rows.push((session, flags.to_owned()));
            if let Some(kids) = children.get(session.id.as_str()) {
                let last = kids.len() - 1;
                for (i, kid) in kids.iter().enumerate() {
                    let mut kid_flags = flags.to_owned();
                    kid_flags.push(i < last);
                    walk(kid, &kid_flags, children, rows, visited);
                }
            }
        }
        let candidates = self.picker_candidates();
        let ids: HashSet<&str> = candidates.iter().map(|s| s.id.as_str()).collect();
        let mut roots = Vec::new();
        let mut children: HashMap<&str, Vec<&SessionInfo>> = HashMap::new();
        for session in &candidates {
            // a filtered-out parent leaves its branch a root
            if session.parent_session.is_empty() || !ids.contains(session.parent_session.as_str()) {
                roots.push(session);
            } else {
                children
                    .entry(session.parent_session.as_str())
                    .or_default()
                    .push(session);
            }
        }
        let mut rows = Vec::new();
        let mut visited = HashSet::new();
        for root in roots {
            walk(root, &[], &children, &mut rows, &mut visited);
        }
        // a parent cycle strands sessions the walk never reaches
        for session in &candidates {
            if !visited.contains(session.id.as_str()) {
                rows.push((session, Vec::new()));
            }
        }
        rows
    }

    fn picker_candidates(&self) -> Vec<&SessionInfo> {
        let show_all = self.picker().is_some_and(|picker| picker.show_all);
        let show_abandoned = self.picker().is_some_and(|picker| picker.show_abandoned);
        let open_project = self.current_project();
        let order: Vec<&SessionInfo> = self
            .by_recency()
            .into_iter()
            .filter(|session| {
                !session.title.is_empty()
                    || !session.preview.is_empty()
                    || session.last_at.is_some()
            })
            .filter(|session| !is_job_session(session))
            .filter(|session| show_abandoned || !is_abandoned(session))
            .filter(|session| match open_project {
                Some(project) if !show_all => session.project == project,
                _ => true,
            })
            .collect();
        let filtering = self.picker().is_some_and(|picker| picker.filtering);
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

    pub fn current_project(&self) -> Option<&str> {
        match self.session_id.as_deref() {
            Some(id) => self
                .session_meta
                .get(id)
                .and_then(|(_, project, _)| (!project.is_empty()).then_some(project.as_str())),
            None => self.pending_project.as_deref(),
        }
    }

    fn back_session(&mut self) -> Option<Command> {
        let previous = self.previous_session.take()?;
        self.start_session(Some(previous))
    }

    fn start_session(&mut self, session_id: Option<String>) -> Option<Command> {
        if session_id.as_deref().is_some_and(|id| {
            self.sessions.iter().any(|session| {
                session.id == id && session.role == arc_core::provider::LEGACY_DIRECT_ROLE
            })
        }) {
            self.start_session(None);
            self.last_error = Some("historical_session".to_owned());
            self.push_block(Block::Fault {
                code: "historical_session".to_owned(),
                msg: "This session used the retired code role. Start a new assistant session."
                    .to_owned(),
            });
            return None;
        }
        self.pending_project = match session_id.as_deref() {
            Some(_) => None,
            None => self.current_project().map(str::to_owned),
        };
        self.pending_first = None;
        self.pending_attachments.clear();
        if self.session_id != session_id {
            self.previous_session = self.session_id.clone();
        }
        if let Some(current) = &self.session_id {
            self.session_details
                .insert(current.clone(), self.show_details);
        }
        self.show_details = session_id
            .as_ref()
            .and_then(|id| self.session_details.get(id))
            .copied()
            .unwrap_or(false);
        self.session_id.clone_from(&session_id);
        self.transcript.clear();
        self.viewport_anchor = None;
        self.restore_anchor = false;
        self.details_held_focus = None;
        self.visible_blocks.clear();
        self.overlay = Overlay::None;
        self.scroll_back = 0;
        self.search = None;
        self.last_error = None;
        self.refetch_in_flight = false;
        let session_id = session_id?;
        self.push_block(Block::Note("loading".to_owned()));
        Some(Command::History { session_id })
    }

    fn submit(&mut self) -> Option<Command> {
        if self.pending_model.is_some() {
            self.last_error = Some("Wait for the model switch to finish".to_owned());
            return None;
        }
        let content = self.input.trim().to_owned();
        if content.is_empty() && self.pending_attachments.is_empty() {
            return None;
        }
        let attachments = std::mem::take(&mut self.pending_attachments);
        let shown = arc_core::attachment::display_text(&content, &attachments);
        self.input.clear();
        self.cursor = 0;
        self.scroll_back = 0;
        self.push_block(Block::You(shown));
        if self.status == Status::Streaming {
            if let Some(session_id) = self.session_id.clone() {
                if !attachments.is_empty() {
                    self.sending_attachments.push_back(attachments.clone());
                }
                return Some(Command::SendLive {
                    session_id,
                    content,
                    attachments,
                });
            }
            // the first message of a brand-new session: no session id to
            // steer into yet, so hold it for the accept that names one
            self.pending_live = Some((content, attachments));
            return None;
        }
        if self.session_id.is_none()
            && (self.pending_project.is_some() || self.launch_dir.is_some())
        {
            self.status = Status::Streaming;
            self.pending_first = Some((content, attachments));
            return Some(Command::CreateSession {
                role: SessionRole::Chat,
                project: self.pending_project.clone().unwrap_or_default(),
                choice: String::new(),
                working_directory: self
                    .launch_dir
                    .as_ref()
                    .map_or_else(String::new, |dir| dir.to_string_lossy().into_owned()),
            });
        }
        Some(self.send_with_attachments(content, attachments))
    }

    pub fn stop_escape_count(&self) -> Option<u8> {
        if self.status != Status::Streaming
            || self.session_id.is_none()
            || self.overlay != Overlay::None
            || self.searching
        {
            return None;
        }
        let count = match self.mode {
            Mode::Insert => 2,
            Mode::Normal => 1,
            Mode::Cmd | Mode::Visual => return None,
        };
        Some(count + u8::from(self.pending.is_some()))
    }

    fn cancel_turn(&mut self) -> Option<Command> {
        let session_id = self.session_id.clone()?;
        Some(Command::CancelTurn { session_id })
    }

    fn send_with_attachments(
        &mut self,
        content: String,
        attachments: Vec<ImageAttachment>,
    ) -> Command {
        self.status = Status::Streaming;
        self.last_error = None;
        self.turn_started = Some(Instant::now());
        self.streamed_chars = 0;
        if !attachments.is_empty() {
            self.sending_attachments.push_back(attachments.clone());
        }
        Command::Send {
            session_id: self.session_id.clone(),
            content,
            attachments,
        }
    }

    pub fn on_net(&mut self, event: NetEvent) -> Option<Command> {
        // any live event proves the socket is back; only another disconnect says otherwise
        if self.status == Status::Disconnected
            && !matches!(
                event,
                NetEvent::Disconnected { .. }
                    | NetEvent::SessionStatus(_)
                    | NetEvent::StatusUnavailable(_)
            )
        {
            self.status = Status::Idle;
        }
        match event {
            NetEvent::SessionStatus(status) => {
                self.session_status
                    .insert(status.session_id.clone(), status);
                None
            }
            NetEvent::StatusUnavailable(id) => {
                if let Some(status) = self.session_status.get_mut(&id) {
                    status.allowance_stale = true;
                }
                None
            }
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
                if self.picker().is_some() {
                    self.clamp_picker_selection();
                }
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
                    let mut rebuilt =
                        history_blocks(entries, &parent_session, fork_point, &branches);
                    for entry in &mut rebuilt {
                        set_details(&mut entry.block, self.show_details);
                    }
                    // append-only rebuilds keep the selection valid
                    let appended_only = rebuilt.len() >= self.transcript.len()
                        && rebuilt
                            .iter()
                            .zip(&self.transcript)
                            .all(|(a, b)| a.block == b.block);
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
                // the first live stream gets a fresh reply block; a second
                // one accepted into an already-streaming session is just a
                // queuing handshake, its answer is the first stream's
                let first_stream = self.live_streams == 0;
                self.live_streams += 1;
                self.status = Status::Streaming;
                self.session_id = Some(session_id);
                if first_stream {
                    self.push_block(Block::Arc {
                        text: String::new(),
                        partial: false,
                    });
                }
                if let Some((content, attachments)) = self.pending_live.take() {
                    return Some(self.send_with_attachments(content, attachments));
                }
                None
            }
            NetEvent::AttachmentsAccepted(attachments) => {
                self.remove_sending_attachments(&attachments);
                None
            }
            NetEvent::AttachmentsFailed(attachments) => {
                if let Some(mut attachments) = self.remove_sending_attachments(&attachments) {
                    self.pending_attachments.append(&mut attachments);
                }
                None
            }
            NetEvent::Delta(text) => {
                self.finalize_thinking();
                self.streamed_chars += text.chars().count();
                if let Some(Block::Arc { text: reply, .. }) =
                    self.transcript.last_mut().map(|entry| &mut entry.block)
                {
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
                    args: arguments_json,
                    outcome: None,
                    content: String::new(),
                    open: false,
                });
                None
            }
            NetEvent::ToolEnded {
                call_id,
                outcome,
                content,
            } => {
                complete_tool(&mut self.transcript, &call_id, outcome, content);
                None
            }
            NetEvent::End {
                partial,
                input_tokens,
                output_tokens,
                step_capped,
                grounding_json,
                queued,
            } => {
                self.live_streams = self.live_streams.saturating_sub(1);
                if queued {
                    // a queuing handshake, not a finished turn: the real
                    // reply is still streaming on whichever request holds it
                    self.turn_over();
                    return None;
                }
                self.finalize_thinking();
                if let Some(Block::Arc { partial: p, .. }) =
                    self.transcript.last_mut().map(|entry| &mut entry.block)
                {
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
                self.turn_over();
                None
            }
            NetEvent::Failed { code, msg } => {
                self.pending_model = None;
                self.live_streams = self.live_streams.saturating_sub(1);
                if let Some((content, mut attachments)) = self.pending_first.take() {
                    if self.input.is_empty() {
                        self.input = content;
                        self.cursor = self.input.len();
                    }
                    self.pending_attachments.append(&mut attachments);
                }
                self.pending_rewind_text = None;
                self.finalize_thinking();
                self.pop_empty_reply();
                self.turn_started = None;
                self.last_error = Some(code.clone());
                self.push_block(Block::Fault { code, msg });
                self.turn_over();
                None
            }
            NetEvent::ReviewItems(items) => {
                if let Some(review) = self.review_mut() {
                    if !review.all {
                        review.items = items;
                        review.selected = 0;
                        review.loaded = true;
                        review.pending_delete = false;
                    }
                }
                None
            }
            NetEvent::MemoryItems(items) => {
                if let Some(review) = self.review_mut() {
                    if review.all {
                        review.items = items;
                        review.selected = 0;
                        review.loaded = true;
                        review.pending_delete = false;
                    }
                }
                None
            }
            NetEvent::ReviewChanged(pending) => {
                self.review_pending = pending;
                None
            }
            NetEvent::ModelItems(items) => {
                if let Some(models) = self.models_mut() {
                    let items: Vec<_> = items
                        .into_iter()
                        .filter(|c| models.default || c.role == models.role as i32)
                        .collect();
                    models.selected = if models.default {
                        items
                            .iter()
                            .position(|c| c.selected && c.role == SessionRole::Chat as i32)
                            .or_else(|| {
                                items.iter().position(|c| {
                                    c.selected && c.role == SessionRole::Executor as i32
                                })
                            })
                            .or_else(|| items.iter().position(|c| c.selected))
                    } else {
                        items.iter().position(|c| c.selected)
                    }
                    .unwrap_or(0);
                    models.items = items;
                    models.loaded = true;
                }
                None
            }
            NetEvent::ProjectsSeeded(items) => {
                if self.session_id.is_none()
                    && self.pending_project.is_none()
                    && self.status == Status::Idle
                {
                    let roots = canonical_roots(&items);
                    self.pending_project = self
                        .launch_dir
                        .as_deref()
                        .and_then(|dir| longest_matching_root(dir, &roots))
                        .map(str::to_owned);
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
                if let Some(jobs) = self.jobs_mut() {
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
                if let Some(jobs) = self.jobs_mut() {
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
                if let Some(PendingModel::Create(role, project)) = self
                    .pending_model
                    .take_if(|model| matches!(model, PendingModel::Create(..)))
                {
                    self.session_meta
                        .insert(session_id.clone(), (role, project, Source::User));
                    let attachments = std::mem::take(&mut self.pending_attachments);
                    let command = self.start_session(Some(session_id));
                    self.pending_attachments = attachments;
                    return command;
                }
                if let Some(project) = self.pending_project.take() {
                    self.session_meta.insert(
                        session_id.clone(),
                        (SessionRole::Chat, project, Source::User),
                    );
                }
                self.session_id = Some(session_id);
                let (content, attachments) = self.pending_first.take()?;
                Some(self.send_with_attachments(content, attachments))
            }
            NetEvent::SessionForked { session_id } => {
                if let Some(PendingModel::Fork(role, project)) = self
                    .pending_model
                    .take_if(|model| matches!(model, PendingModel::Fork(..)))
                {
                    self.session_meta
                        .insert(session_id.clone(), (role, project, Source::User));
                }
                let attachments = std::mem::take(&mut self.pending_attachments);
                let command = self.start_session(Some(session_id));
                self.pending_attachments = attachments;
                if let Some(text) = self.pending_rewind_text.take() {
                    self.input = text;
                    self.cursor = self.input.len();
                    self.mode = Mode::Insert;
                }
                command
            }
            NetEvent::Compacted { session_id } => {
                self.yank_note = Some("compacted".to_owned());
                Some(Command::History { session_id })
            }
            NetEvent::Disconnected { reason } => {
                self.pending_model = None;
                for mut attachments in self.sending_attachments.drain(..) {
                    self.pending_attachments.append(&mut attachments);
                }
                self.turn_started = None;
                self.live_streams = 0;
                self.last_error = Some("disconnected".to_owned());
                self.push_block(Block::Fault {
                    code: "disconnected".to_owned(),
                    msg: reason,
                });
                self.status = Status::Disconnected;
                None
            }
        }
    }

    fn remove_sending_attachments(
        &mut self,
        attachments: &[ImageAttachment],
    ) -> Option<Vec<ImageAttachment>> {
        let position = self
            .sending_attachments
            .iter()
            .position(|sending| sending == attachments)?;
        self.sending_attachments.remove(position)
    }

    fn turn_over(&mut self) {
        self.status = if self.live_streams > 0 {
            Status::Streaming
        } else {
            Status::Idle
        };
    }

    fn stream_thought(&mut self, text: String) {
        if let Some(Block::Thought {
            text: thinking,
            seconds,
            done: false,
            ..
        }) = self.transcript.last_mut().map(|entry| &mut entry.block)
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
        if let Some(Block::Thought { seconds, done, .. }) =
            self.transcript.last_mut().map(|entry| &mut entry.block)
        {
            if !*done {
                *done = true;
                *seconds = Self::thought_seconds(since);
            }
        }
    }

    fn thought_seconds(since: Option<Instant>) -> u64 {
        since.map_or(0, |since| since.elapsed().as_secs()).max(1)
    }

    fn toggle_open_blocks(&mut self) {
        self.restore_anchor = self.scroll_back > 0;
        if self.show_details {
            if let Some((block, offset)) = &mut self.viewport_anchor {
                if self.transcript.get(*block).is_some_and(|entry| {
                    matches!(entry.block, Block::Tool { .. } | Block::Thought { .. })
                }) {
                    *offset = 0;
                }
            }
        }
        self.details_held_focus = self.visual_boundary().or_else(|| self.search_block());
        self.show_details = !self.show_details;
        for entry in &mut self.transcript {
            set_details(&mut entry.block, self.show_details);
        }
    }

    fn pop_empty_reply(&mut self) {
        if matches!(self.transcript.last().map(|entry| &entry.block), Some(Block::Arc { text, .. }) if text.is_empty())
        {
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

pub fn is_job_session(session: &SessionInfo) -> bool {
    session.source != Source::User as i32
}

fn is_abandoned(session: &SessionInfo) -> bool {
    session.disposition == arc_proto::v1::branch_marked::Disposition::Abandoned as i32
}

/// The seeded projects whose root exists locally and canonicalizes, paired
/// with that canonical root.
fn canonical_roots(projects: &[ProjectInfo]) -> Vec<(&str, PathBuf)> {
    projects
        .iter()
        .filter(|project| !project.root.is_empty())
        .filter_map(|project| {
            std::fs::canonicalize(&project.root)
                .ok()
                .map(|root| (project.name.as_str(), root))
        })
        .collect()
}

/// Longest-prefix match by path component, over already-canonical roots —
/// a root `/a/b` matches a launch dir under it but not a sibling `/a/b2`.
fn longest_matching_root<'a>(launch_dir: &Path, roots: &[(&'a str, PathBuf)]) -> Option<&'a str> {
    roots
        .iter()
        .filter(|(_, root)| launch_dir.starts_with(root))
        .max_by_key(|(_, root)| root.components().count())
        .map(|(name, _)| *name)
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

fn format_yank(blocks: &[Entry]) -> Option<String> {
    let parts: Vec<String> = blocks
        .iter()
        .filter_map(|entry| block_yank_text(&entry.block))
        .collect();
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

const SUMMARY_KEYS: &[&str] = &["command", "query", "brief", "path", "id"];

// two provider shapes, verbatim: Gemini's groundingChunks[].web and the
// Responses API's url_citation annotations
fn grounding_sources(grounding_json: &str) -> Vec<(String, String)> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(grounding_json) else {
        return Vec::new();
    };
    let titled = |source: &serde_json::Value, uri_key: &str| {
        let uri = source.get(uri_key)?.as_str()?.to_owned();
        let title = source
            .get("title")
            .and_then(|t| t.as_str())
            .filter(|t| !t.trim().is_empty())
            .unwrap_or(&uri)
            .to_owned();
        Some((title, uri))
    };
    if let Some(chunks) = value.get("groundingChunks").and_then(|c| c.as_array()) {
        return chunks
            .iter()
            .filter_map(|chunk| titled(chunk.get("web")?, "uri"))
            .collect();
    }
    if let Some(annotations) = value.get("annotations").and_then(|a| a.as_array()) {
        return annotations
            .iter()
            .filter(|a| a.get("type").and_then(|t| t.as_str()) == Some("url_citation"))
            .filter_map(|a| titled(a, "url"))
            .collect();
    }
    Vec::new()
}

fn set_details(block: &mut Block, details: bool) {
    match block {
        Block::Thought { open, .. } | Block::Tool { open, .. } => {
            *open = details;
        }
        _ => {}
    }
}

fn foldable(block: &Block) -> bool {
    matches!(
        block,
        Block::Tool { .. } | Block::Thought { .. } | Block::Handback { .. }
    )
}

pub fn tool_summary(arguments_json: &str) -> String {
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str(arguments_json) else {
        return arguments_json.to_owned();
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

fn complete_tool(entries: &mut [Entry], call_id: &str, outcome: i32, content: String) {
    let ended = entries
        .iter_mut()
        .rev()
        .map(|entry| &mut entry.block)
        .find(|block| matches!(block, Block::Tool { call_id: id, .. } if id == call_id));
    if let Some(Block::Tool {
        outcome: current,
        content: result,
        ..
    }) = ended
    {
        *current = Some(outcome_label(outcome));
        *result = content;
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

fn history_blocks(
    entries: Vec<HistoryEntry>,
    parent_session: &str,
    fork_point: u64,
    branches: &[(u64, String)],
) -> Vec<Entry> {
    let mut blocks = Vec::new();
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
                    blocks.push(Entry {
                        block,
                        seq: Some(seq),
                    });
                    if is_arc && !sources.is_empty() {
                        blocks.push(Entry::from(Block::Sources(sources)));
                    }
                    if is_arc && (input_tokens != 0 || output_tokens != 0) {
                        blocks.push(Entry::from(Block::Cost {
                            input_tokens,
                            output_tokens,
                            seconds: elapsed_ms as f32 / 1000.0,
                        }));
                    }
                }
            }
            Some(history_entry::Entry::ToolCall(call)) => {
                blocks.push(Entry {
                    block: Block::Tool {
                        call_id: call.call_id,
                        name: call.name,
                        args: call.arguments_json,
                        outcome: Some("unknown"),
                        content: String::new(),
                        open: false,
                    },
                    seq: Some(seq),
                });
            }
            Some(history_entry::Entry::ToolResult(result)) => {
                complete_tool(&mut blocks, &result.call_id, result.outcome, result.content);
            }
            // provider-side, arrives resolved; styled like a finished tool line
            Some(history_entry::Entry::ServerCall(call)) => {
                blocks.push(Entry {
                    block: Block::Tool {
                        call_id: String::new(),
                        name: call.name,
                        args: call.arguments_json,
                        outcome: Some("web"),
                        content: call.response_json,
                        open: false,
                    },
                    seq: Some(seq),
                });
            }
            None => {}
        }
        // the door out: a fork leaving from this entry gets its signpost
        if blocks.len() > was_len {
            for (_, label) in branches.iter().filter(|(at, _)| *at == seq) {
                blocks.push(Entry::from(Block::Note(format!(
                    "a branch continues from here: {label}"
                ))));
            }
        }
    }
    if !parent_session.is_empty() {
        if let Some(cut) = blocks
            .iter()
            .rposition(|entry| entry.seq.is_some_and(|s| s <= fork_point))
        {
            let mut at = cut + 1;
            while at < blocks.len() && blocks[at].seq.is_none() {
                at += 1;
            }
            let head = &parent_session[..parent_session.len().min(8)];
            blocks.insert(at, Block::Note(format!("branched from {head} here")).into());
        }
    }
    blocks
}

#[cfg(test)]
mod tests {
    use crate::app::Overlay;
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
            provider: String::new(),
            model: String::new(),
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
            source: Source::User as i32,
            parent_session: String::new(),
            disposition: 0,
        }
    }

    fn job_session(id: &str, title: &str, role: SessionRole, project: &str) -> SessionInfo {
        SessionInfo {
            provider: String::new(),
            model: String::new(),
            role: role as i32,
            project: project.to_owned(),
            dispatched_by: "s-parent".to_owned(),
            source: Source::Model as i32,
            ..session_with(id, title, "hi")
        }
    }

    fn code_session(id: &str, title: &str, project: &str) -> SessionInfo {
        SessionInfo {
            provider: String::new(),
            model: String::new(),
            role: SessionRole::Executor as i32,
            project: project.to_owned(),
            dispatched_by: String::new(),
            source: Source::User as i32,
            ..session_with(id, title, "hi")
        }
    }

    fn picker_selected(app: &App) -> Option<usize> {
        app.picker().map(|picker| picker.selected)
    }

    fn end(partial: bool) -> NetEvent {
        NetEvent::End {
            partial,
            input_tokens: 0,
            output_tokens: 0,
            step_capped: false,
            grounding_json: String::new(),
            queued: false,
        }
    }

    fn queued_end() -> NetEvent {
        NetEvent::End {
            partial: false,
            input_tokens: 0,
            output_tokens: 0,
            step_capped: false,
            grounding_json: String::new(),
            queued: true,
        }
    }

    fn started(call_id: &str, name: &str) -> NetEvent {
        NetEvent::ToolStarted {
            call_id: call_id.to_owned(),
            name: name.to_owned(),
            arguments_json: String::new(),
        }
    }

    fn ended(call_id: &str, outcome: i32, content: &str) -> NetEvent {
        NetEvent::ToolEnded {
            call_id: call_id.to_owned(),
            outcome,
            content: content.to_owned(),
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
                content: "hello".to_owned(),
                attachments: Vec::new(),
            })
        );
        assert_eq!(app.block_contents(), [Block::You("hello".to_owned())]);
        assert_eq!(app.input, "");
        assert_eq!(app.status, Status::Streaming);
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

        assert_eq!(refresh, None);
        assert_eq!(next, None);
        assert_eq!(app.session_id.as_deref(), Some("s-1"));
        assert_eq!(app.status, Status::Idle);
        assert_eq!(
            app.block_contents(),
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
            app.transcript.last().map(|entry| &entry.block),
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
            app.block_contents(),
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
            app.block_contents(),
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
            app.transcript[1].block,
            Block::Thought { open: true, .. }
        ));
    }

    #[test]
    fn details_follow_new_blocks_history_and_session_switches() {
        let mut app = App::new();
        app.on_key(ctrl('o'));
        app.on_net(NetEvent::Accepted {
            session_id: "s1".to_owned(),
        });
        for id in ["t1", "t2"] {
            app.on_net(NetEvent::Reasoning("checking".to_owned()));
            app.on_net(started(id, "bash"));
            app.on_net(ended(id, ToolOutcome::Ok as i32, "done"));
        }
        app.push_block(Block::Handback {
            subject: "job".to_owned(),
            body: "done".to_owned(),
            open: false,
        });
        for entry in app.transcript.iter().filter(|entry| foldable(&entry.block)) {
            let mut expected = entry.block.clone();
            set_details(&mut expected, true);
            assert_eq!(entry.block, expected);
        }
        app.on_net(end(false));
        app.start_session(Some("s2".to_owned()));
        assert!(!app.show_details);
        app.start_session(Some("s1".to_owned()));
        assert!(app.show_details);
        app.on_net(NetEvent::History {
            session_id: "s1".to_owned(),
            entries: vec![handback_entry(&format!(
                "Job {JOB_ID} finished.\nall green"
            ))],
            parent_session: String::new(),
            fork_point: 0,
            branches: vec![],
        });
        assert!(matches!(
            app.transcript[0].block,
            Block::Handback { open: false, .. }
        ));
        app.mode = Mode::Visual;
        app.on_key(ctrl('o'));
        assert!(!app.show_details);
        assert!(matches!(
            app.transcript[0].block,
            Block::Handback { open: false, .. }
        ));
        app.on_net(NetEvent::Reasoning("next".to_owned()));
        assert!(matches!(
            app.transcript.last().unwrap().block,
            Block::Thought { open: false, .. }
        ));
        for mode in [Mode::Normal, Mode::Visual] {
            app.mode = mode;
            for c in ['o', 'O'] {
                app.on_key(key(KeyCode::Char(c)));
                assert!(!app.show_details);
                assert_eq!(app.overlay, Overlay::None);
            }
        }
        app.start_session(None);
        assert!(!app.show_details);
        assert!(!App::new().show_details);
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

        app.on_net(ended("b", ToolOutcome::Ok as i32, "found it"));
        assert_eq!(
            app.block_contents(),
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
                    content: String::new(),
                    open: false,
                },
                Block::Tool {
                    call_id: "b".to_owned(),
                    name: "beta".to_owned(),
                    args: String::new(),
                    outcome: Some("ok"),
                    content: "found it".to_owned(),
                    open: false,
                },
            ]
        );

        app.on_net(ended("a", ToolOutcome::Error as i32, "boom"));
        assert!(matches!(
            &app.transcript[2].block,
            Block::Tool {
                outcome: Some("error"),
                content,
                ..
            } if content == "boom"
        ));
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
            queued: false,
        });
        assert!(
            matches!(
                app.transcript.last().map(|entry| &entry.block),
                Some(Block::Sources(sources)) if sources.len() == 2
            ),
            "citations sit under the answer, got {:?}",
            app.transcript.last().map(|entry| &entry.block)
        );

        typed(&mut app, "and plainly?");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Delta("no web involved".to_owned()));
        app.on_net(end(false));
        assert!(
            !matches!(
                app.transcript.last().map(|entry| &entry.block),
                Some(Block::Sources(_))
            ),
            "an ungrounded turn adds nothing"
        );
    }

    #[test]
    fn a_branched_session_gets_a_marker_after_the_last_inherited_row() {
        let entries = vec![
            prose_entry_at(1, Role::User as i32, "inherited question", false),
            prose_entry_at(2, Role::Assistant as i32, "inherited answer", false),
            prose_entry_at(3, Role::User as i32, "own question", false),
        ];
        let entries = history_blocks(entries, "s-parent-uuid", 2, &[]);
        let seqs: Vec<_> = entries.iter().map(|entry| entry.seq).collect();
        let blocks: Vec<_> = entries.into_iter().map(|entry| entry.block).collect();

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
            .picker_flat_rows()
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
            .picker_flat_rows()
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
            queued: false,
        });

        assert!(matches!(
            app.transcript.last().map(|entry| &entry.block),
            Some(Block::Cost {
                input_tokens: 2345,
                output_tokens: 140,
                ..
            })
        ));
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
            queued: false,
        });

        assert_eq!(
            app.transcript.last().map(|entry| &entry.block),
            Some(&Block::StepCapped)
        );
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
                app.block_contents().as_slice(),
                [Block::Thought { text, done: false, .. }] if text == "weighing options"
            ),
            "one open thought block accumulates the deltas, got {:?}",
            app.transcript
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
            app.block_contents(),
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
    fn typing_while_streaming_sends_live_and_pushes_the_you_block() {
        let mut app = App::new();
        typed(&mut app, "one");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });

        typed(&mut app, "two");
        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(
            command,
            Some(Command::SendLive {
                session_id: "s-1".to_owned(),
                content: "two".to_owned(),
                attachments: Vec::new(),
            }),
            "a message typed mid-turn goes to the live turn, not a local queue"
        );
        assert_eq!(
            app.block_contents(),
            [
                Block::You("one".to_owned()),
                Block::Arc {
                    text: String::new(),
                    partial: false
                },
                Block::You("two".to_owned()),
            ],
            "the You block lands immediately, not after the turn ends"
        );
        assert_eq!(app.status, Status::Streaming);
    }

    #[test]
    fn a_message_typed_before_the_first_accept_sends_once_the_session_is_named() {
        let mut app = App::new();
        typed(&mut app, "one");
        app.on_key(key(KeyCode::Enter));

        // the first message is in flight; no session id exists yet to steer into
        typed(&mut app, "two");
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            None,
            "held until a session id is known, like pending_first"
        );

        let flushed = app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });

        assert_eq!(
            flushed,
            Some(Command::Send {
                session_id: Some("s-1".to_owned()),
                content: "two".to_owned(),
                attachments: Vec::new(),
            }),
            "sent as soon as the accept names the session"
        );
        assert_eq!(app.status, Status::Streaming);
    }

    #[test]
    fn disconnecting_faults_the_transcript_with_no_retry() {
        let mut app = App::new();
        typed(&mut app, "one");
        app.on_key(key(KeyCode::Enter));

        let retry = app.on_net(NetEvent::Disconnected {
            reason: "the daemon closed the connection".to_owned(),
        });

        assert!(matches!(
            app.transcript.last().map(|entry| &entry.block),
            Some(Block::Fault { code, .. }) if code == "disconnected"
        ));
        assert_eq!(retry, None, "there is no local queue left to retry");
        assert_eq!(app.status, Status::Disconnected);
    }

    #[test]
    fn attach_queues_image_bytes_for_the_next_message_and_clear_discards_them() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("screen.png");
        std::fs::write(&path, b"\x89PNG\r\n\x1a\nbody").unwrap();
        let mut app = App::new();

        normal(&mut app, &format!(":attach {}", path.display()));
        app.on_key(key(KeyCode::Enter));
        assert_eq!(app.pending_attachments.len(), 1);
        assert_eq!(app.pending_attachments[0].name, "screen.png");

        app.mode = Mode::Insert;
        typed(&mut app, "what is this?");
        let command = app.on_key(key(KeyCode::Enter));
        let Some(Command::Send {
            content,
            attachments,
            ..
        }) = command
        else {
            panic!("the next message carries the picture");
        };
        assert_eq!(content, "what is this?");
        assert_eq!(attachments[0].data, b"\x89PNG\r\n\x1a\nbody");
        assert_eq!(
            app.block_contents(),
            [Block::You("[image: screen.png]\nwhat is this?".to_owned())]
        );
        assert!(app.pending_attachments.is_empty());

        app.on_net(NetEvent::AttachmentsFailed(attachments));
        app.on_net(NetEvent::Failed {
            code: "unsupported_attachment".to_owned(),
            msg: "not supported".to_owned(),
        });
        assert_eq!(app.pending_attachments.len(), 1);
        normal(&mut app, ":attach clear");
        app.on_key(key(KeyCode::Enter));
        assert!(app.pending_attachments.is_empty());
    }

    #[test]
    fn each_send_restores_only_its_own_attachments_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.png");
        let second = dir.path().join("second.png");
        std::fs::write(&first, b"\x89PNG\r\n\x1a\nfirst").unwrap();
        std::fs::write(&second, b"\x89PNG\r\n\x1a\nsecond").unwrap();
        let mut app = App::new();
        app.session_id = Some("s-1".to_owned());

        normal(&mut app, &format!(":attach {}", first.display()));
        app.on_key(key(KeyCode::Enter));
        app.mode = Mode::Insert;
        typed(&mut app, "first");
        assert!(matches!(
            app.on_key(key(KeyCode::Enter)),
            Some(Command::Send { attachments, .. }) if !attachments.is_empty()
        ));

        normal(&mut app, &format!(":attach {}", second.display()));
        app.on_key(key(KeyCode::Enter));
        app.mode = Mode::Insert;
        typed(&mut app, "second");
        assert!(matches!(
            app.on_key(key(KeyCode::Enter)),
            Some(Command::SendLive { attachments, .. }) if !attachments.is_empty()
        ));
        let first_sent = app.sending_attachments[0].clone();
        let second_sent = app.sending_attachments[1].clone();

        app.on_net(NetEvent::AttachmentsFailed(second_sent));
        app.on_net(NetEvent::Failed {
            code: "bad_attachment".to_owned(),
            msg: "rejected".to_owned(),
        });
        app.on_net(NetEvent::AttachmentsAccepted(first_sent));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });

        assert_eq!(
            app.pending_attachments
                .iter()
                .map(|attachment| attachment.name.as_str())
                .collect::<Vec<_>>(),
            ["second.png"]
        );
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

        assert_eq!(app.picker(), None);
        assert_eq!(app.session_id.as_deref(), Some("new"));
        assert_eq!(
            fetch,
            Some(Command::History {
                session_id: "new".to_owned()
            }),
            "opening a session asks for its transcript"
        );
        assert_eq!(
            app.block_contents(),
            [Block::Note("loading".to_owned())],
            "until the answer lands, the wait is visible"
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
    fn tree_mode_lists_roots_then_branches_depth_first_with_connector_geometry() {
        let mut app = App::new();
        let mut a_branch = session_with("a-2", "branch", "hi");
        a_branch.parent_session = "a-root".to_owned();
        let mut deep = session_with("a-3", "deep", "hi");
        deep.parent_session = "a-2".to_owned();
        let mut b_branch = session_with("b-2", "branch b", "hi");
        b_branch.parent_session = "b-root".to_owned();
        app.on_net(NetEvent::Sessions(vec![
            a_branch,
            session_with("a-root", "root a", "hi"),
            deep,
            b_branch,
            session_with("b-root", "root b", "hi"),
        ]));
        normal(&mut app, "s");
        app.on_key(key(KeyCode::Tab));

        let rows = app.picker_tree_rows();
        let ids: Vec<&str> = rows.iter().map(|(s, _)| s.id.as_str()).collect();
        assert_eq!(
            ids,
            ["a-root", "a-2", "a-3", "b-root", "b-2"],
            "tree view nests the fresh branch under its root instead of leading with it"
        );
        assert_eq!(
            rows.iter().map(|(_, f)| f.clone()).collect::<Vec<_>>(),
            [
                Vec::new(),
                vec![false],
                vec![false, false],
                Vec::new(),
                vec![false],
            ],
            "flags: a-2 ends its root's line, a-3 is a-2's only child, b-2 ends its root's line"
        );
        assert_eq!(
            app.picker_rows()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ids,
            "navigation follows the tree order"
        );
    }

    #[test]
    fn tree_mode_filters_with_the_same_gates_and_orphans_a_filtered_parent() {
        use arc_proto::v1::branch_marked::Disposition;
        let mut app = App::new();
        let mut ordinary = session_with("s-ordinary", "conversation", "hi");
        ordinary.parent_session = "s-job".to_owned(); // job parent hidden by default
        let mut dead = session_with("s-dead", "wrong turn", "hi");
        dead.parent_session = "s-root".to_owned();
        dead.disposition = Disposition::Abandoned as i32;
        let mut job = session_with("s-job", "the dispatch", "hi");
        job.source = Source::Model as i32;
        app.on_net(NetEvent::Sessions(vec![
            ordinary,
            dead,
            job,
            session_with("s-root", "root", "hi"),
        ]));
        normal(&mut app, "s");
        app.on_key(key(KeyCode::Tab));

        let rows = app.picker_tree_rows();
        let ids: Vec<&str> = rows.iter().map(|(s, _)| s.id.as_str()).collect();
        assert_eq!(
            ids,
            ["s-ordinary", "s-root"],
            "abandoned branches and job sessions are hidden exactly as in flat mode"
        );
        let orphan = rows.iter().find(|(s, _)| s.id == "s-ordinary").unwrap();
        assert_eq!(
            orphan.1,
            Vec::<bool>::new(),
            "a branch whose parent is filtered out renders as its own root"
        );

        app.on_key(key(KeyCode::Char('x')));
        let ids: Vec<&str> = app
            .picker_tree_rows()
            .iter()
            .map(|(s, _)| s.id.as_str())
            .collect();
        assert_eq!(
            ids,
            ["s-ordinary", "s-root", "s-dead"],
            "x nests the abandoned branch under its root in tree view"
        );
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
                content: String::new(),
            })),
            seq: 0,
        }
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
        live.on_net(ended("t1", ToolOutcome::Ok as i32, "found it"));
        live.on_net(NetEvent::Delta("answer".to_owned()));
        live.on_net(end(false));

        let mut reopened = App::new();
        reopened.session_id = Some("s-1".to_owned());
        reopened.on_net(NetEvent::History {
            session_id: "s-1".to_owned(),
            entries: vec![
                prose_entry(Role::User as i32, "hi", false),
                call_entry("t1", "lookup"),
                HistoryEntry {
                    entry: Some(history_entry::Entry::ToolResult(HistoryToolResult {
                        call_id: "t1".to_owned(),
                        outcome: ToolOutcome::Ok as i32,
                        truncated: false,
                        content: "found it".to_owned(),
                    })),
                    seq: 0,
                },
                prose_entry(Role::Assistant as i32, "answer", false),
            ],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });

        assert_eq!(reopened.block_contents(), live.block_contents());
        assert!(
            matches!(
                &live.transcript[1].block,
                Block::Tool { content, .. } if content == "found it"
            ),
            "the live path filled the tool result's content"
        );
    }

    #[test]
    fn history_for_a_session_already_left_is_dropped() {
        let mut app = App::new();
        app.session_id = Some("second".to_owned());
        app.set_blocks(vec![Block::Note("loading".to_owned())]);

        app.on_net(NetEvent::History {
            session_id: "first".to_owned(),
            entries: vec![prose_entry(Role::User as i32, "stale", false)],
            parent_session: String::new(),
            fork_point: 0,
            branches: Vec::new(),
        });

        assert_eq!(
            app.block_contents(),
            [Block::Note("loading".to_owned())],
            "the transcript we are actually waiting on is untouched"
        );
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

        assert_eq!(app.picker(), None);
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
            !app.picker().expect("picker still open").filtering,
            "esc exits filtering, not the picker"
        );
        assert_eq!(app.picker_rows().len(), 2, "the full list is back");
        assert_eq!(app.input, "draft reply", "the stashed draft comes back");
        assert_eq!(app.cursor, app.input.len());
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
                content: "line one\nline two".to_owned(),
                attachments: Vec::new(),
            })
        );
    }

    fn entry(id: &str, title: &str) -> ReviewEntry {
        ReviewEntry {
            id: id.to_owned(),
            kind: 4,
            namespace: "global".to_owned(),
            title: title.to_owned(),
            summary: "a summary".to_owned(),
            body: "the full body".to_owned(),
            supersedes: Vec::new(),
        }
    }

    fn reviewing(entries: Vec<ReviewEntry>) -> App {
        let mut app = App::new();
        normal(&mut app, ":review");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::ReviewItems(entries));
        app
    }

    fn browsing_memory(entries: Vec<ReviewEntry>) -> App {
        let mut app = App::new();
        normal(&mut app, ":memory");
        assert_eq!(app.on_key(key(KeyCode::Enter)), Some(Command::MemoryList));
        app.on_net(NetEvent::MemoryItems(entries));
        app
    }

    #[test]
    fn memory_browse_deletes_with_dd_without_changing_review_behavior() {
        let mut app = browsing_memory(vec![
            entry("mr-1", "one"),
            entry("mr-2", "two"),
            entry("mr-3", "three"),
        ]);
        app.on_net(NetEvent::ReviewItems(vec![entry("mr-other", "other")]));
        assert_eq!(app.review().expect("memory").items.len(), 3);
        assert_eq!(app.on_key(key(KeyCode::Char('a'))), None);
        assert_eq!(app.on_key(key(KeyCode::Char('f'))), None);
        assert_eq!(app.input, "");

        app.on_key(key(KeyCode::Char('j')));
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.on_key(key(KeyCode::Char('d'))), None);
        assert_eq!(
            app.on_key(key(KeyCode::Char('d'))),
            Some(Command::ReviewDelete {
                record_id: "mr-3".to_owned()
            })
        );
        assert_eq!(app.review().expect("memory").selected, 1);
        assert_eq!(app.review().expect("memory").items.len(), 2);
        assert_eq!(
            app.on_key(key(KeyCode::Char('r'))),
            Some(Command::MemoryList)
        );
        assert!(app.review().expect("memory").all);

        let mut review = reviewing(vec![entry("mr-1", "one")]);
        review.on_net(NetEvent::MemoryItems(vec![entry("mr-other", "other")]));
        assert_eq!(
            review.review().expect("review").items,
            [entry("mr-1", "one")]
        );
        assert!(matches!(
            review.on_key(key(KeyCode::Char('r'))),
            Some(Command::ReviewList { .. })
        ));
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
        let review = app.review().expect("still open");
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
        assert!(app.review().expect("open").pending_delete);

        app.on_key(key(KeyCode::Char('j')));
        assert!(!app.review().expect("open").pending_delete);
        assert_eq!(app.on_key(key(KeyCode::Char('d'))), None);

        let command = app.on_key(key(KeyCode::Char('d')));
        assert_eq!(
            command,
            Some(Command::ReviewDelete {
                record_id: "mr-2".to_owned()
            })
        );
        let review = app.review().expect("still open");
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

        assert_eq!(app.review(), None, "the pane closed");
        assert_eq!(app.input, "fix memory mr-1: Old address — ");
        assert_eq!(app.cursor, app.input.len(), "ready to finish the sentence");
        assert_eq!(app.mode, Mode::Insert);
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

    fn choice(role: SessionRole, name: &str, selected: bool) -> ModelChoice {
        ModelChoice {
            role: role as i32,
            name: name.to_owned(),
            provider: "codex".to_owned(),
            model: format!("model-{name}"),
            thinking: "medium".to_owned(),
            selected,
        }
    }

    #[test]
    fn shift_m_opens_the_contextual_model_picker() {
        let mut app = normal_app();

        let command = app.on_key(key(KeyCode::Char('M')));
        assert_eq!(command, Some(Command::ListModels));
        assert!(!app.models_mut().expect("picker is open").loaded);

        app.on_net(NetEvent::ModelItems(vec![
            choice(SessionRole::Chat, "astra", true),
            choice(SessionRole::Executor, "sol", true),
            choice(SessionRole::Chat, "glm-flash", false),
        ]));
        assert_eq!(
            app.models_mut().unwrap().selected,
            0,
            "the cursor lands on the chat role default"
        );

        app.on_key(key(KeyCode::Char('j')));
        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(
            command,
            Some(Command::CreateSession {
                role: SessionRole::Chat,
                project: String::new(),
                choice: "glm-flash".to_owned(),
                working_directory: String::new(),
            })
        );
        assert_eq!(app.models_mut(), None, "enter closes the picker");

        app.on_key(key(KeyCode::Char('M')));
        app.on_key(key(KeyCode::Esc));
        assert_eq!(app.models_mut(), None);
    }

    #[test]
    fn model_default_picker_selects_chat_independently_from_executor() {
        let mut app = normal_app();
        app.on_key(key(KeyCode::Char(':')));
        typed(&mut app, "model-default");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::ModelItems(vec![
            choice(SessionRole::Executor, "sol", true),
            choice(SessionRole::Chat, "sol", true),
            choice(SessionRole::Chat, "astra", false),
        ]));
        assert_eq!(app.models_mut().unwrap().selected, 1);
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Command::SelectModel {
                role: SessionRole::Chat,
                choice: "astra".to_owned(),
            })
        );
    }

    #[test]
    fn choosing_for_a_nonempty_session_forks_at_its_latest_message() {
        let mut app = normal_app();
        app.session_id = Some("old".to_owned());
        app.session_meta.insert(
            "old".to_owned(),
            (SessionRole::Executor, "arc".to_owned(), Source::Model),
        );
        app.transcript = vec![
            Entry {
                block: Block::You("hello".to_owned()),
                seq: Some(3),
            },
            Entry {
                block: Block::Tool {
                    call_id: "x".to_owned(),
                    name: "read".to_owned(),
                    args: String::new(),
                    outcome: Some("ok"),
                    content: String::new(),
                    open: false,
                },
                seq: Some(5),
            },
            Entry {
                block: Block::Arc {
                    text: "reply".to_owned(),
                    partial: false,
                },
                seq: Some(7),
            },
            Entry {
                block: Block::Cost {
                    input_tokens: 1,
                    output_tokens: 1,
                    seconds: 0.1,
                },
                seq: None,
            },
        ];
        app.input = "still typing".to_owned();
        app.cursor = app.input.len();
        app.pending_attachments.push(ImageAttachment {
            name: "draft.png".to_owned(),
            ..Default::default()
        });
        assert_eq!(
            app.on_key(key(KeyCode::Char('M'))),
            Some(Command::ListModels)
        );
        app.on_net(NetEvent::ModelItems(vec![
            choice(SessionRole::Chat, "other", true),
            choice(SessionRole::Executor, "fast", false),
        ]));
        assert_eq!(app.models_mut().unwrap().items.len(), 1);
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Command::ForkSession {
                session_id: "old".to_owned(),
                fork_point: 7,
                choice: "fast".to_owned(),
            })
        );
        assert_eq!(
            app.on_net(NetEvent::SessionForked {
                session_id: "branch".to_owned()
            }),
            Some(Command::History {
                session_id: "branch".to_owned()
            })
        );
        assert_eq!(app.input, "still typing");
        assert_eq!(app.pending_attachments().len(), 1);
        assert_eq!(app.session_meta["branch"].0, SessionRole::Executor);
    }

    #[test]
    fn choosing_for_an_empty_session_creates_a_bound_session() {
        let mut app = normal_app();
        app.session_id = Some("empty".to_owned());
        app.session_meta.insert(
            "empty".to_owned(),
            (SessionRole::Chat, "arc".to_owned(), Source::User),
        );
        app.on_key(key(KeyCode::Char('M')));
        app.on_net(NetEvent::ModelItems(vec![choice(
            SessionRole::Chat,
            "deep",
            false,
        )]));
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Command::CreateSession {
                role: SessionRole::Chat,
                project: "arc".to_owned(),
                choice: "deep".to_owned(),
                working_directory: String::new(),
            })
        );
        assert_eq!(app.session_id.as_deref(), Some("empty"));
    }

    #[test]
    fn model_switch_refuses_streaming_and_empty_job_sessions() {
        let mut app = normal_app();
        app.status = Status::Streaming;
        assert_eq!(app.on_key(key(KeyCode::Char('M'))), None);
        assert!(app.models_mut().is_none());
        app.status = Status::Idle;
        app.session_id = Some("job".to_owned());
        app.session_meta.insert(
            "job".to_owned(),
            (SessionRole::Executor, "arc".to_owned(), Source::Model),
        );
        assert_eq!(
            app.on_key(key(KeyCode::Char('M'))),
            Some(Command::ListModels)
        );
        app.on_net(NetEvent::ModelItems(vec![choice(
            SessionRole::Executor,
            "fast",
            false,
        )]));
        assert_eq!(app.on_key(key(KeyCode::Enter)), None);
        assert!(
            app.last_error
                .as_ref()
                .unwrap()
                .contains("no message to fork")
        );
    }

    #[test]
    fn nested_roots_prefer_the_longer_and_reject_a_sibling() {
        let roots = vec![
            ("outer", PathBuf::from("/a")),
            ("inner", PathBuf::from("/a/b")),
        ];
        assert_eq!(
            longest_matching_root(Path::new("/a/b/c"), &roots),
            Some("inner"),
            "the deeper root wins over its ancestor"
        );
        assert_eq!(
            longest_matching_root(Path::new("/a/b2"), &[("inner", PathBuf::from("/a/b"))]),
            None,
            "a sibling that merely shares a prefix string is not a component match"
        );
    }

    #[test]
    fn launch_project_and_picker_new_session_share_scope() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = std::fs::canonicalize(root.path()).expect("canonicalize");
        let seed = vec![ProjectInfo {
            name: "arc".to_owned(),
            root: dir.display().to_string(),
            ..Default::default()
        }];
        let mut app = App::new();
        app.set_launch_dir(Some(dir.clone()));
        app.on_net(NetEvent::ProjectsSeeded(seed.clone()));
        assert_eq!(app.current_project(), Some("arc"));
        app.on_net(NetEvent::Sessions(vec![
            code_session("arc-old", "in arc", "arc"),
            code_session("other", "elsewhere", "other"),
            session_with("unbound", "question", "hi"),
        ]));
        app.on_key(ctrl('p'));
        assert_eq!(
            app.picker_rows()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["arc-old"]
        );
        app.on_key(key(KeyCode::Enter));
        assert_eq!(app.current_project(), Some("arc"));
        typed(&mut app, "hello");
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Command::CreateSession {
                role: SessionRole::Chat,
                project: "arc".to_owned(),
                choice: String::new(),
                working_directory: dir.display().to_string(),
            })
        );
        assert_eq!(
            app.on_net(NetEvent::SessionCreated {
                session_id: "new".to_owned()
            }),
            Some(Command::Send {
                session_id: Some("new".to_owned()),
                content: "hello".to_owned(),
                attachments: Vec::new(),
            })
        );
        assert_eq!(app.current_project(), Some("arc"));

        let mut remote = App::new();
        remote.on_net(NetEvent::ProjectsSeeded(seed));
        assert_eq!(remote.current_project(), None);
        typed(&mut remote, "hello");
        assert!(matches!(
            remote.on_key(key(KeyCode::Enter)),
            Some(Command::Send {
                session_id: None,
                ..
            })
        ));
    }

    #[test]
    fn project_seed_after_typing_still_binds_the_unsent_session() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = std::fs::canonicalize(root.path()).expect("canonicalize");
        let mut app = App::new();
        app.set_launch_dir(Some(dir.clone()));
        typed(&mut app, "already typing");
        app.on_net(NetEvent::ProjectsSeeded(vec![ProjectInfo {
            name: "arc".to_owned(),
            root: dir.display().to_string(),
            ..Default::default()
        }]));
        assert_eq!(app.current_project(), Some("arc"));
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Command::CreateSession {
                role: SessionRole::Chat,
                project: "arc".to_owned(),
                choice: String::new(),
                working_directory: dir.display().to_string(),
            })
        );
    }

    #[test]
    fn picker_switches_scope_to_selected_project_and_new_inherits_it() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![
            code_session("old", "legacy", "arc"),
            SessionInfo {
                role: SessionRole::Chat as i32,
                ..code_session("other", "other project", "scratch")
            },
            session_with("free", "unbound", "hello"),
        ]));
        app.start_session(Some("old".to_owned()));
        app.on_key(ctrl('p'));
        assert_eq!(app.picker_rows().len(), 1);
        app.on_key(key(KeyCode::Char('a')));
        assert_eq!(app.picker_rows().len(), 3);
        let row = app
            .picker_rows()
            .iter()
            .position(|s| s.id == "other")
            .unwrap()
            + 1;
        app.picker_mut().unwrap().selected = row;
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Command::History {
                session_id: "other".to_owned()
            })
        );
        assert_eq!(app.current_project(), Some("scratch"));
        app.on_key(ctrl('p'));
        assert_eq!(
            app.picker_rows()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            ["other"]
        );
        app.on_key(key(KeyCode::Enter));
        assert_eq!(app.session_id, None);
        assert_eq!(app.current_project(), Some("scratch"));
    }

    #[test]
    fn opening_a_historical_code_session_shows_a_chat_error() {
        let mut app = normal_app();
        let mut old = code_session("legacy", "ongoing", "arc");
        old.role = arc_core::provider::LEGACY_DIRECT_ROLE;
        app.on_net(NetEvent::Sessions(vec![old]));
        normal(&mut app, "s");
        let row = app
            .picker_rows()
            .iter()
            .position(|s| s.id == "legacy")
            .unwrap()
            + 1;
        app.picker_mut().unwrap().selected = row;
        assert_eq!(app.on_key(key(KeyCode::Enter)), None);
        assert_eq!(app.session_id, None);
        assert!(matches!(
            app.transcript.last().map(|entry| &entry.block),
            Some(Block::Fault { code, msg })
                if code == "historical_session" && msg.contains("Start a new assistant session")
        ));
    }

    #[test]
    fn tab_never_switches_sessions_or_drafts() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![code_session(
            "old", "legacy", "arc",
        )]));
        app.start_session(Some("old".to_owned()));
        app.input = "draft".to_owned();
        app.mode = Mode::Normal;
        assert_eq!(app.on_key(key(KeyCode::Tab)), None);
        assert_eq!(app.session_id.as_deref(), Some("old"));
        assert_eq!(app.input, "draft");
    }

    #[test]
    fn enter_on_a_job_row_opens_its_child_session() {
        use arc_proto::v1::job_info::State;

        let mut app = jobsview(vec![job("s-a", State::Running), job("s-b", State::Running)]);
        app.on_key(key(KeyCode::Char('j')));

        let fetch = app.on_key(key(KeyCode::Enter));

        assert_eq!(app.jobs(), None, "the popup closes");
        assert_eq!(app.session_id.as_deref(), Some("s-b"));
        assert_eq!(
            fetch,
            Some(Command::History {
                session_id: "s-b".to_owned()
            }),
            "the same open path a picker row takes"
        );
        assert_eq!(app.block_contents(), [Block::Note("loading".to_owned())]);
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
            app.jobs().expect("open").confirmation.as_deref(),
            Some("cancelled s-a")
        );
        assert!(app.jobs().is_some(), "the popup stays open");
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
            app.jobs().expect("open").confirmation.as_deref(),
            Some("dropped 2")
        );
    }

    #[test]
    fn a_queued_stream_end_decrements_without_a_cost_block_or_touching_the_reply() {
        let mut app = App::new();
        typed(&mut app, "one");
        app.on_key(key(KeyCode::Enter));
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        app.on_net(NetEvent::Delta("still going".to_owned()));

        typed(&mut app, "two");
        let command = app.on_key(key(KeyCode::Enter));
        assert_eq!(
            command,
            Some(Command::SendLive {
                session_id: "s-1".to_owned(),
                content: "two".to_owned(),
                attachments: Vec::new(),
            })
        );

        // the control channel's own accept-then-queued-end handshake for "two"
        app.on_net(NetEvent::Accepted {
            session_id: "s-1".to_owned(),
        });
        let next = app.on_net(queued_end());

        assert_eq!(next, None);
        assert_eq!(
            app.status,
            Status::Streaming,
            "the main turn for \"one\" is still live"
        );
        assert_eq!(
            app.block_contents(),
            [
                Block::You("one".to_owned()),
                Block::Arc {
                    text: "still going".to_owned(),
                    partial: false
                },
                Block::You("two".to_owned()),
            ],
            "no Cost block, and the reply-in-progress is untouched"
        );
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
            app.block_contents(),
            [Block::Handback {
                subject: format!("Job {JOB_ID} finished."),
                body: "fixed the flaky test".to_owned(),
                open: false,
            }],
            "a handback starts folded"
        );
    }

    fn conversation() -> App {
        let mut app = App::new();
        app.push_block(Block::You("first question".to_owned()));
        app.push_block(Block::Arc {
            text: "first answer".to_owned(),
            partial: false,
        });
        app.push_block(Block::Tool {
            call_id: "t1".to_owned(),
            name: "bash".to_owned(),
            args: "ls".to_owned(),
            outcome: Some("ok"),
            content: "total 0".to_owned(),
            open: false,
        });
        app.push_block(Block::Cost {
            input_tokens: 10,
            output_tokens: 20,
            seconds: 1.0,
        });
        app.push_block(Block::You("second question".to_owned()));
        app.push_block(Block::Arc {
            text: "second answer".to_owned(),
            partial: false,
        });
        app.on_key(key(KeyCode::Esc));
        app
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
                choice: String::new(),
            })
        );
        assert_eq!(app.mode, Mode::Normal, "the command consumes the selection");
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
                choice: String::new(),
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
    fn n_and_n_walk_older_and_newer_and_stop_at_the_ends() {
        let mut app = normal_app();
        for i in 0..3 {
            app.push_block(Block::You(format!("needle {i}")));
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
    fn colon_compact_emits_compact_session_for_the_open_session() {
        let mut app = App::new();
        app.session_id = Some("s-open".to_owned());
        normal(&mut app, ":compact");
        let command = app.on_key(key(KeyCode::Enter));

        assert_eq!(
            command,
            Some(Command::CompactSession {
                session_id: "s-open".to_owned()
            })
        );
        assert_eq!(app.last_error, None, ":compact is a command, not E492");
    }

    #[test]
    fn a_compacted_ack_notes_it_and_refetches_history() {
        let mut app = App::new();
        app.session_id = Some("s-open".to_owned());

        let command = app.on_net(NetEvent::Compacted {
            session_id: "s-open".to_owned(),
        });

        assert_eq!(app.yank_note.as_deref(), Some("compacted"));
        assert_eq!(
            command,
            Some(Command::History {
                session_id: "s-open".to_owned()
            })
        );
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
}
