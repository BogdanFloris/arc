//! Semantic colors over the terminal's own palette.
//!
//! Never RGB: the terminal theme (gruvbox, here) supplies the actual values,
//! so `arc` matches whatever variant the terminal runs. The one exception is
//! orange — the accent — which the ANSI 16 does not carry: indexed 208 is
//! xterm-256's orange (`#ff8700`), close enough to gruvbox's `#fe8019` that
//! unthemed terminals look right, and gruvbox terminal setups that define
//! indexed colors map it exactly.

use ratatui::style::{Color, Modifier, Style};

/// The single accent: gruvbox orange. Speaker label `arc`, prompt, selection.
pub const ACCENT: Style = Style::new().fg(Color::Indexed(208));

/// Secondary everything: speaker label `you`, rules, notes, timestamps.
pub const DIM: Style = Style::new().fg(Color::DarkGray);

/// Faults, and nothing else.
pub const ERROR: Style = Style::new().fg(Color::Red);

/// Message text: the terminal's default foreground.
pub const PLAIN: Style = Style::new();

/// The `-- cut --` marker on a partial reply.
pub const CUT: Style = Style::new()
    .fg(Color::DarkGray)
    .add_modifier(Modifier::ITALIC);

/// A markdown heading. The accent again, with weight — headings are structure,
/// and structure in this UI is orange.
pub const HEADING: Style = Style::new()
    .fg(Color::Indexed(208))
    .add_modifier(Modifier::BOLD);

/// Code, inline and fenced. Cyan is the ANSI slot gruvbox paints aqua, which
/// is what its editor themes already use for literals.
pub const CODE: Style = Style::new().fg(Color::Cyan);

/// Blockquoted text and its gutter.
pub const QUOTE: Style = Style::new()
    .fg(Color::DarkGray)
    .add_modifier(Modifier::ITALIC);

/// `**bold**` — weight only, so the terminal's foreground still decides color.
pub const STRONG: Style = Style::new().add_modifier(Modifier::BOLD);

/// `*italic*`.
pub const EMPHASIS: Style = Style::new().add_modifier(Modifier::ITALIC);

/// List bullets and ordinals, so the text outranks its marker.
pub const MARKER: Style = Style::new().fg(Color::DarkGray);

// Syntax highlighting inside a code block. The roles and their slots are
// gruvbox's own editor layout — red keywords, green strings, purple numbers,
// aqua types, yellow calls, grey comments — expressed as ANSI palette indices,
// so the block reads as familiar code in the user's own theme. Plain code
// keeps `CODE`, which is what an unhighlighted block was already using.

/// `//` and `#` comments.
pub const SYN_COMMENT: Style = Style::new().fg(Color::DarkGray);

/// String and char literals.
pub const SYN_STRING: Style = Style::new().fg(Color::Green);

/// Numeric literals, and the word-shaped ones (`true`, `None`).
pub const SYN_NUMBER: Style = Style::new().fg(Color::Magenta);

/// Language keywords, and shell flags.
pub const SYN_KEYWORD: Style = Style::new().fg(Color::Red);

/// Type-shaped names, and shell variables.
pub const SYN_TYPE: Style = Style::new().fg(Color::Cyan);

/// A name with a `(` after it.
pub const SYN_CALL: Style = Style::new().fg(Color::Yellow);
