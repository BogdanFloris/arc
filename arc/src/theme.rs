use ratatui::style::{Color, Modifier, Style};

pub const ACCENT: Style = Style::new().fg(Color::Indexed(208));

pub const DIM: Style = Style::new().fg(Color::DarkGray);

pub const ERROR: Style = Style::new().fg(Color::Red);

pub const PLAIN: Style = Style::new();

pub const CUT: Style = Style::new()
    .fg(Color::DarkGray)
    .add_modifier(Modifier::ITALIC);

pub const HEADING: Style = Style::new()
    .fg(Color::Indexed(208))
    .add_modifier(Modifier::BOLD);

pub const CODE: Style = Style::new().fg(Color::Cyan);

pub const QUOTE: Style = Style::new()
    .fg(Color::DarkGray)
    .add_modifier(Modifier::ITALIC);

pub const STRONG: Style = Style::new().add_modifier(Modifier::BOLD);

pub const EMPHASIS: Style = Style::new().add_modifier(Modifier::ITALIC);

pub const MARKER: Style = Style::new().fg(Color::DarkGray);

pub const LINK: Style = Style::new().add_modifier(Modifier::UNDERLINED);

pub const SYN_COMMENT: Style = Style::new().fg(Color::DarkGray);

pub const SYN_STRING: Style = Style::new().fg(Color::Green);

pub const SYN_NUMBER: Style = Style::new().fg(Color::Magenta);

pub const SYN_KEYWORD: Style = Style::new().fg(Color::Red);

pub const SYN_TYPE: Style = Style::new().fg(Color::Cyan);

pub const SYN_CALL: Style = Style::new().fg(Color::Yellow);
