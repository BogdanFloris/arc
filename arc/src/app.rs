//! The TUI's state and how keys and daemon events change it.
//!
//! Everything here is synchronous and side-effect-free: handlers mutate the
//! state and return the [`Command`] to hand the connection task, if any. That
//! split is what makes the transitions testable without a terminal or a
//! daemon.
//!
//! Keys are modal, vim-style: insert mode types into the input line, normal
//! mode moves and edits it (`h l 0 $ w b`, `i I a A`, `x D dd`) and scrolls
//! the transcript (`j k`, `ctrl-d/u`, `G`, `gg`), `:` takes commands (`:q`).
//! The app starts in insert — a chat's first act is typing.

use std::collections::VecDeque;

use arc_proto::v1::{Role, SessionInfo};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// Lines a half-page scroll moves: `ctrl-d`, `ctrl-u`, and the page keys.
pub const PAGE: usize = 10;

/// What the connection task is asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Ask the daemon for its sessions.
    List,
    /// Ask the daemon for one session's past messages.
    History { session_id: String },
    /// Send one user message; `None` starts a new session.
    Send {
        session_id: Option<String>,
        content: String,
    },
}

/// What the connection task reports back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetEvent {
    /// The daemon answered `ListSessions`.
    Sessions(Vec<SessionInfo>),
    /// The daemon answered `FetchHistory` for `session_id`.
    History {
        session_id: String,
        messages: Vec<(i32, String)>,
    },
    /// The daemon accepted the message and named the session.
    Accepted { session_id: String },
    /// A piece of the model's reply.
    Delta(String),
    /// The turn finished.
    End { partial: bool },
    /// The turn (or a list request) failed; the connection survives.
    Failed { code: String, msg: String },
    /// The connection is gone; the next command will try to reconnect.
    Disconnected { reason: String },
}

/// One block of the transcript.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Block {
    /// The user's words, shown the moment they are sent or queued.
    You(String),
    /// A model reply, streamed into place.
    Arc { text: String, partial: bool },
    /// A fault, inline where it happened: error code + explanation.
    Fault { code: String, msg: String },
    /// A dim aside from the client itself (e.g. "history not shown").
    Note(String),
}

/// What the status rule says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Idle,
    Streaming,
    Disconnected,
}

/// The vim mode the input line is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Normal,
    Insert,
    /// A `:` command is being typed.
    Cmd,
}

/// The whole TUI state.
pub struct App {
    pub transcript: Vec<Block>,
    pub input: String,
    /// Byte offset of the cursor in `input`, always on a char boundary.
    pub cursor: usize,
    pub mode: Mode,
    /// The `:` line being typed, without the colon.
    pub cmd: String,
    /// A pending operator/prefix key in normal mode (`d` of `dd`, `g` of `gg`).
    pending: Option<char>,
    /// The session new messages go to; `None` means the next send starts one.
    pub session_id: Option<String>,
    /// The daemon's sessions, oldest first, as `ListSessions` returns them.
    pub sessions: Vec<SessionInfo>,
    /// Selected picker row when the picker is open. Row 0 is "new session";
    /// row `i + 1` is `sessions` newest-first.
    pub picker: Option<usize>,
    pub status: Status,
    /// The last error code, shown on the rule until the next send.
    pub last_error: Option<String>,
    /// Messages typed while a turn was streaming, sent in order after it.
    pub queued: VecDeque<String>,
    /// How many wrapped lines the view is scrolled up from the bottom. May
    /// overshoot; drawing clamps it to the transcript's real height.
    pub scroll_back: usize,
    pub quit: bool,
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
            status: Status::Idle,
            last_error: None,
            queued: VecDeque::new(),
            scroll_back: 0,
            quit: false,
        }
    }

    /// Scrolls the transcript by `lines`, or moves the picker if it is open.
    ///
    /// Every scroll gesture lands here — `ctrl-d/u` and the page keys. When
    /// the picker is open it takes the gesture instead: scrolling the
    /// transcript behind a modal reads as the wrong thing moving.
    ///
    /// There is deliberately no mouse wheel. Reporting it means asking the
    /// terminal for mouse motion, and a terminal that reports motion stops
    /// doing its own text selection — which broke selecting text out of the
    /// pane far worse than the wheel was worth.
    pub fn on_scroll(&mut self, up: bool, lines: usize) {
        if let Some(selected) = self.picker {
            let last = self.sessions.len();
            self.picker = Some(if up {
                selected.saturating_sub(1)
            } else {
                (selected + 1).min(last)
            });
            return;
        }
        self.scroll_back = if up {
            self.scroll_back.saturating_add(lines)
        } else {
            self.scroll_back.saturating_sub(lines)
        };
    }

    /// Handles one key press.
    pub fn on_key(&mut self, key: KeyEvent) -> Option<Command> {
        // Page keys are not text, so they scroll from any mode — including
        // mid-sentence in insert.
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
        if self.picker.is_some() {
            return self.on_picker_key(key.code);
        }
        match self.mode {
            Mode::Insert => self.on_insert(key.code),
            Mode::Normal => self.on_normal(key.code),
            Mode::Cmd => {
                self.on_cmd(key.code);
                None
            }
        }
    }

    /// Control chords, any mode.
    fn on_control(&mut self, code: KeyCode) -> Option<Command> {
        match code {
            KeyCode::Char('c') => self.quit = true,
            // Half-page scrolls; drawing clamps the overshoot.
            KeyCode::Char('u') => self.on_scroll(true, PAGE),
            KeyCode::Char('d') => self.on_scroll(false, PAGE),
            KeyCode::Char('p') => self.open_picker(),
            KeyCode::Char('n') if self.status != Status::Streaming => {
                return self.start_session(None);
            }
            _ => {}
        }
        None
    }

    /// Insert mode: type, or leave.
    fn on_insert(&mut self, code: KeyCode) -> Option<Command> {
        match code {
            KeyCode::Esc => {
                self.mode = Mode::Normal;
                // Vim lands on the char just typed, not after it.
                self.cursor_left();
                self.clamp_normal();
            }
            KeyCode::Enter => return self.submit(),
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
        None
    }

    /// Normal mode: motions, edits, scrolls.
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

    /// The `:` line.
    fn on_cmd(&mut self, code: KeyCode) {
        match code {
            KeyCode::Esc => self.mode = Mode::Normal,
            KeyCode::Char(c) => self.cmd.push(c),
            KeyCode::Backspace => {
                if self.cmd.pop().is_none() {
                    // Backspace past the colon leaves the line, like vim.
                    self.mode = Mode::Normal;
                }
            }
            KeyCode::Enter => {
                match self.cmd.as_str() {
                    "q" | "q!" | "qa" | "quit" => self.quit = true,
                    // Vim's "not an editor command", verbatim.
                    _ => self.last_error = Some("E492".to_owned()),
                }
                self.mode = Mode::Normal;
            }
            _ => {}
        }
    }

    /// Keys while the picker is open.
    fn on_picker_key(&mut self, code: KeyCode) -> Option<Command> {
        let selected = self.picker.expect("picker is open");
        let last = self.sessions.len(); // rows: 0 = new, 1..=len = sessions
        match code {
            KeyCode::Esc => self.picker = None,
            KeyCode::Up | KeyCode::Char('k') => {
                self.picker = Some(selected.saturating_sub(1));
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.picker = Some((selected + 1).min(last));
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

    /// Opens the picker — not mid-stream: the in-flight turn would keep
    /// rendering into whatever session the view switched to.
    fn open_picker(&mut self) {
        if self.status != Status::Streaming {
            self.picker = Some(0);
        }
    }

    /// The session behind picker row `row`, `None` for the "new session" row.
    ///
    /// Rows are ordered by [`Self::by_recency`].
    pub fn picker_session(&self, row: usize) -> Option<&SessionInfo> {
        let order = self.by_recency();
        row.checked_sub(1).and_then(|i| order.get(i).copied())
    }

    /// Sessions as the picker shows them: last spoken in first.
    ///
    /// The daemon answers in a stable order — oldest first, by when each was
    /// created — because that is the order a log replays in. What you want to
    /// reopen is what you were last doing, which is a different question, so
    /// the client asks it here. Sessions with nothing said in them fall back
    /// to when they started, and ties break on id so the list never shuffles
    /// between draws.
    pub fn by_recency(&self) -> Vec<&SessionInfo> {
        let mut order: Vec<&SessionInfo> = self.sessions.iter().collect();
        order.sort_by(|a, b| activity(b).cmp(&activity(a)).then_with(|| a.id.cmp(&b.id)));
        order
    }

    /// Switches the view to `session_id` (`None` = a fresh one), asking the
    /// daemon for its history if it has any.
    fn start_session(&mut self, session_id: Option<String>) -> Option<Command> {
        self.session_id.clone_from(&session_id);
        self.transcript.clear();
        self.scroll_back = 0;
        self.last_error = None;
        let session_id = session_id?;
        // Say the transcript is on its way rather than showing an old session
        // as empty; the answer replaces this note wholesale.
        self.transcript.push(Block::Note("loading".to_owned()));
        Some(Command::History { session_id })
    }

    /// Enter on the input line, either mode.
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

    /// Starts a turn for `content` against the current session.
    fn send(&mut self, content: String) -> Command {
        self.status = Status::Streaming;
        self.last_error = None;
        Command::Send {
            session_id: self.session_id.clone(),
            content,
        }
    }

    /// Handles one event from the connection task.
    pub fn on_net(&mut self, event: NetEvent) -> Option<Command> {
        match event {
            NetEvent::Sessions(sessions) => {
                self.sessions = sessions;
                None
            }
            NetEvent::History {
                session_id,
                messages,
            } => {
                // Switching sessions twice in a row leaves an answer for the
                // one we left in flight; it must not land in this transcript.
                if self.session_id.as_deref() == Some(session_id.as_str()) {
                    self.transcript = messages.into_iter().filter_map(history_block).collect();
                    self.scroll_back = 0;
                }
                None
            }
            NetEvent::Accepted { session_id } => {
                // A turn that named a session the picker has never seen means
                // the list is stale; refresh it once the turn is done (the
                // daemon serializes, so the list waits its turn anyway).
                let created = self.session_id.is_none();
                self.session_id = Some(session_id);
                self.transcript.push(Block::Arc {
                    text: String::new(),
                    partial: false,
                });
                created.then_some(Command::List)
            }
            NetEvent::Delta(text) => {
                if let Some(Block::Arc { text: reply, .. }) = self.transcript.last_mut() {
                    reply.push_str(&text);
                } else {
                    // A delta with no block to land in should not happen, but
                    // dropping model text on the floor is worse than healing.
                    self.transcript.push(Block::Arc {
                        text,
                        partial: false,
                    });
                }
                None
            }
            NetEvent::End { partial } => {
                if let Some(Block::Arc { partial: p, .. }) = self.transcript.last_mut() {
                    *p = partial;
                }
                self.turn_over(Status::Idle)
            }
            NetEvent::Failed { code, msg } => {
                // A turn that failed before any delta leaves an empty reply
                // block; the fault replaces it rather than trailing it.
                if matches!(self.transcript.last(), Some(Block::Arc { text, .. }) if text.is_empty())
                {
                    self.transcript.pop();
                }
                self.last_error = Some(code.clone());
                self.transcript.push(Block::Fault { code, msg });
                self.turn_over(Status::Idle)
            }
            NetEvent::Disconnected { reason } => {
                self.last_error = Some("disconnected".to_owned());
                self.transcript.push(Block::Fault {
                    code: "disconnected".to_owned(),
                    msg: reason,
                });
                // Queued messages stay queued: sending the next one is what
                // makes the connection task try to reconnect.
                self.turn_over(Status::Disconnected)
            }
        }
    }

    /// Closes the current turn and starts the next queued one, if any.
    fn turn_over(&mut self, status: Status) -> Option<Command> {
        self.status = status;
        let next = self.queued.pop_front()?;
        Some(self.send(next))
    }

    fn cursor_left(&mut self) {
        if let Some((at, _)) = self.char_before_cursor() {
            self.cursor = at;
        }
    }

    /// Moves right, but never past `limit`.
    fn cursor_right(&mut self, limit: usize) {
        if let Some(c) = self.input[self.cursor..].chars().next() {
            self.cursor = (self.cursor + c.len_utf8()).min(limit);
        }
    }

    /// Where normal mode's `$` lands: on the last char, not past it.
    fn last_char_start(&self) -> usize {
        self.input
            .char_indices()
            .next_back()
            .map_or(0, |(at, _)| at)
    }

    /// Keeps a normal-mode cursor on a char, vim-style.
    fn clamp_normal(&mut self) {
        self.cursor = self.cursor.min(self.last_char_start());
    }

    /// `w`: the start of the next whitespace-delimited word.
    fn next_word_start(&self) -> usize {
        let rest = &self.input[self.cursor..];
        let after_word = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let from_space = &rest[after_word..];
        let word = from_space
            .find(|c: char| !c.is_whitespace())
            .map(|at| self.cursor + after_word + at);
        word.unwrap_or_else(|| self.last_char_start())
    }

    /// `b`: the start of the previous word.
    fn prev_word_start(&self) -> usize {
        let before = &self.input[..self.cursor];
        let trimmed = before.trim_end();
        trimmed.rfind(char::is_whitespace).map_or(0, |at| at + 1)
    }

    /// The char ending at the cursor, as `(start_offset, char)`.
    fn char_before_cursor(&self) -> Option<(usize, char)> {
        self.input[..self.cursor].char_indices().next_back()
    }
}

/// When a session was last touched, for ordering: its last message, or when
/// it started if nothing has been said in it.
fn activity(session: &SessionInfo) -> Option<(i64, i32)> {
    session
        .last_at
        .or(session.started_at)
        .map(|ts| (ts.seconds, ts.nanos))
}

/// One past message as a transcript block.
///
/// Roles this build has no rendering for — a system message, or a value from
/// a newer schema — are dropped rather than guessed at: the daemon never puts
/// them in a session's history today, and inventing a speaker label for one
/// would be worse than its absence.
fn history_block((role, content): (i32, String)) -> Option<Block> {
    match Role::try_from(role) {
        Ok(Role::User) => Some(Block::You(content)),
        Ok(Role::Assistant) => Some(Block::Arc {
            text: content,
            // The projection carries no `partial` column, so a cut reply
            // reopens without its `-- cut --` marker. See `wire.proto`.
            partial: false,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
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

    /// Feed normal-mode keys (the app starts in insert).
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
        let next = app.on_net(NetEvent::End { partial: false });

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
        app.on_net(NetEvent::End { partial: true });

        assert!(matches!(
            app.transcript.last(),
            Some(Block::Arc { partial: true, .. })
        ));
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

        let next = app.on_net(NetEvent::End { partial: false });
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

        // Insert mode: the page keys are not text, so they still scroll.
        typed(&mut app, "half a sentence");
        assert_eq!(app.on_key(key(KeyCode::PageUp)), None);
        assert_eq!(app.scroll_back, PAGE);
        assert_eq!(app.input, "half a sentence", "and do not type themselves");
        assert_eq!(app.mode, Mode::Insert);

        app.on_key(key(KeyCode::PageDown));
        assert_eq!(app.scroll_back, 0);
    }

    /// A modal takes the gesture: scrolling what is behind it would move the
    /// wrong thing.
    /// What you reopen is what you were last doing, which is not what you
    /// started most recently.
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
            // Opened first, but spoken in most recently.
            at("old-but-active", 100, Some(900)),
            at("newer-but-stale", 500, Some(600)),
            // Never spoken in: falls back to when it started.
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
        assert_eq!(app.picker, Some(1), "one row per gesture, not one page");
        app.on_scroll(false, PAGE);
        assert_eq!(app.picker, Some(2));
        app.on_scroll(false, PAGE);
        assert_eq!(app.picker, Some(2), "and it stops at the last session");

        app.on_scroll(true, PAGE);
        assert_eq!(app.picker, Some(1));
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
        assert_eq!(app.picker, Some(0));
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
    fn history_replaces_the_loading_note_with_the_transcript() {
        let mut app = App::new();
        app.on_net(NetEvent::Sessions(vec![session("old")]));
        normal(&mut app, "s");
        app.on_key(key(KeyCode::Char('j')));
        app.on_key(key(KeyCode::Enter));

        app.on_net(NetEvent::History {
            session_id: "old".to_owned(),
            messages: vec![
                (Role::User as i32, "what is a walking skeleton?".to_owned()),
                (Role::Assistant as i32, "a thin end-to-end slice".to_owned()),
                (Role::System as i32, "the identity file".to_owned()),
            ],
        });

        assert_eq!(
            app.transcript,
            [
                Block::You("what is a walking skeleton?".to_owned()),
                Block::Arc {
                    text: "a thin end-to-end slice".to_owned(),
                    partial: false
                }
            ],
            "user and model render; a system message has no speaker to be"
        );
    }

    /// Switching twice leaves the first answer in flight behind the second.
    #[test]
    fn history_for_a_session_already_left_is_dropped() {
        let mut app = App::new();
        app.session_id = Some("second".to_owned());
        app.transcript = vec![Block::Note("loading".to_owned())];

        app.on_net(NetEvent::History {
            session_id: "first".to_owned(),
            messages: vec![(Role::User as i32, "stale".to_owned())],
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
    fn ctrl_c_quits_from_any_mode() {
        let mut app = App::new();
        app.on_key(ctrl('c'));
        assert!(app.quit);
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
}
