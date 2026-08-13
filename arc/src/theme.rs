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
