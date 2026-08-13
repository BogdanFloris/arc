//! Syntax highlighting for fenced code blocks.
//!
//! A heuristic lexer, not a parser: it finds comments, strings, numbers,
//! keywords and type-shaped names, and leaves everything else alone. It will
//! not tell a local from a field, and it does not try to. Reading a snippet in
//! a chat transcript needs the shape of the code, which colour carries; it
//! does not need a symbol table.
//!
//! Colours are the terminal's own (theme.rs), laid out the way gruvbox's
//! editor themes lay them out — red keywords, green strings, purple numbers,
//! aqua types, grey comments — so the block reads as familiar code in the
//! user's own palette and stays correct in anyone else's.
//!
//! # State across lines
//!
//! A block comment or a Python docstring runs past its line, so the caller
//! threads a [`Carry`] from one line to the next through a block. Nothing
//! carries *across* fences: a new code block starts clean, which is also what
//! makes a half-streamed block render sanely.

use ratatui::style::Style;

use crate::theme;

/// A language's lexical rules. `Plain` highlights nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    Rust,
    /// C-family syntax: Go, C, C++, Java, `JavaScript`, `TypeScript`.
    CFamily,
    Python,
    Shell,
    /// `TOML`, `INI`, and close enough for `YAML`: `#` comments, `key = value`.
    Config,
    Json,
    Plain,
}

/// The language a fence's info string names.
///
/// Unknown tags are [`Language::Plain`] rather than a guess: uncoloured code
/// reads fine, miscoloured code reads as a bug in the terminal.
pub fn language(info: &str) -> Language {
    // ```rust,ignore and ```python title=x — the tag is the first word.
    let tag = info
        .trim()
        .split(|c: char| c == ',' || c.is_whitespace())
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();

    match tag.as_str() {
        "rust" | "rs" => Language::Rust,
        "go" | "golang" | "c" | "h" | "cpp" | "c++" | "cc" | "java" | "js" | "javascript"
        | "jsx" | "ts" | "typescript" | "tsx" | "kotlin" | "swift" | "zig" => Language::CFamily,
        "python" | "py" => Language::Python,
        "sh" | "bash" | "zsh" | "shell" | "console" | "terminal" => Language::Shell,
        "toml" | "ini" | "cfg" | "conf" | "yaml" | "yml" => Language::Config,
        "json" | "jsonc" => Language::Json,
        _ => Language::Plain,
    }
}

/// Guesses the language of an untagged block from the lines inside it.
///
/// Small local models drop the info string constantly — a bare ```` ``` ````
/// with Python under it is the common case, not the exception — and leaving
/// every one of those flat was worse than guessing.
///
/// Only markers that belong to one language count, and a tie is
/// [`Language::Plain`]: a wrong guess miscolours code, which reads as a bug in
/// the terminal, while no guess just reads as plain code. A tag that *is*
/// present is always believed, even when it is one this build does not know.
pub fn sniff(lines: &[&str]) -> Language {
    let candidates = [
        (Language::Python, PYTHON_MARKS),
        (Language::Rust, RUST_MARKS),
        (Language::CFamily, C_FAMILY_MARKS),
        (Language::Shell, SHELL_MARKS),
    ];

    let mut best = (Language::Plain, 0_usize);
    let mut runner_up = 0_usize;
    for (language, marks) in candidates {
        let score = lines
            .iter()
            .filter(|line| {
                let line = line.trim();
                marks.iter().any(|mark| match mark.strip_prefix('^') {
                    Some(prefix) => line.starts_with(prefix),
                    None => line.contains(mark),
                })
            })
            .count();
        if score > best.1 {
            runner_up = best.1;
            best = (language, score);
        } else if score > runner_up {
            runner_up = score;
        }
    }

    // Structured data last: `{` and `key = value` are weak on their own, so
    // they only win when nothing with real syntax scored.
    if best.1 == 0 {
        return structured(lines);
    }
    if best.1 > runner_up {
        best.0
    } else {
        Language::Plain
    }
}

/// JSON and config files, told apart by their opening shape.
fn structured(lines: &[&str]) -> Language {
    let first = lines.iter().map(|line| line.trim()).find(|l| !l.is_empty());
    match first {
        Some(line) if line.starts_with('{') || line.starts_with('[') => {
            if lines.iter().any(|l| l.contains("\":")) {
                Language::Json
            } else {
                // `[section]` with no quoted keys is TOML, not JSON.
                Language::Config
            }
        }
        // `key = value` on its own line: TOML, INI, and near enough YAML.
        Some(_) if lines.iter().any(|l| is_assignment(l)) => Language::Config,
        _ => Language::Plain,
    }
}

/// A bare `key = value` line, with nothing that would make it code.
fn is_assignment(line: &str) -> bool {
    let Some((key, _)) = line.split_once('=') else {
        return false;
    };
    let key = key.trim();
    !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// A `^` prefix means "at the start of the trimmed line"; otherwise anywhere.
const PYTHON_MARKS: &[&str] = &[
    "^def ",
    "^class ",
    "^import ",
    "^from ",
    "^elif ",
    "^print(",
    "^if __name__",
    "^@",
    "self.",
    "None",
    "True",
    "False",
    "^async def ",
];

const RUST_MARKS: &[&str] = &[
    "^fn ",
    "^pub ",
    "^impl ",
    "^use ",
    "^struct ",
    "^enum ",
    "^let ",
    "&self",
    "->",
    "::",
    "Vec<",
    "Option<",
    "Result<",
    "^#[",
    ".unwrap()",
    "&mut ",
];

const C_FAMILY_MARKS: &[&str] = &[
    "^func ",
    "^package ",
    "^#include",
    "^public ",
    "^private ",
    "^function ",
    "console.log",
    "^var ",
    "^const ",
    "=> {",
    "^import {",
    "nil",
    "^type ",
];

const SHELL_MARKS: &[&str] = &[
    "^$ ",
    "^#!/",
    "^sudo ",
    "^cd ",
    "^ls ",
    "^git ",
    "^cargo ",
    "^just ",
    "^npm ",
    "^echo ",
    "^export ",
    "^mkdir ",
    "^curl ",
    "^nix ",
    "^systemctl ",
    "^rm ",
    "^cat ",
    "^grep ",
];

/// What a `/*` or `"""` on this line turned out to be.
enum Multiline {
    /// The line does not start one here.
    No,
    /// Opened and closed on this line; lexing carries on after it.
    Closed,
    /// Still open when the line ran out.
    Open(Carry),
}

/// What a previous line left open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Carry {
    #[default]
    None,
    /// Inside `/* ... */`.
    BlockComment,
    /// Inside a Python `"""` or `'''` string; the bool is true for `"""`.
    Docstring(bool),
}

/// Splits one line of code into styled segments.
///
/// Returns the segments and what the line leaves open for the next one.
pub fn highlight(line: &str, language: Language, carry: Carry) -> (Vec<(String, Style)>, Carry) {
    if language == Language::Plain {
        return (vec![(line.to_owned(), theme::CODE)], Carry::None);
    }
    let mut lexer = Lexer {
        rest: line,
        out: Vec::new(),
        pending: String::new(),
        language,
    };
    let carry = lexer.run(carry);
    (lexer.finish(), carry)
}

struct Lexer<'a> {
    rest: &'a str,
    out: Vec<(String, Style)>,
    /// Unclaimed text, held back so a run of punctuation and plain identifiers
    /// becomes one span instead of a dozen.
    pending: String,
    language: Language,
}

impl Lexer<'_> {
    /// Lexes the whole line, returning what it leaves open.
    fn run(&mut self, carry: Carry) -> Carry {
        // Finish whatever the previous line started before lexing normally.
        match carry {
            Carry::BlockComment => {
                if !self.resume_block_comment() {
                    return Carry::BlockComment;
                }
            }
            Carry::Docstring(double) => {
                if !self.resume_docstring(double) {
                    return Carry::Docstring(double);
                }
            }
            Carry::None => {}
        }

        while !self.rest.is_empty() {
            if self.whitespace() || self.comment() {
                continue;
            }
            // Before `string`: Python's `"""` opens a docstring, but the
            // string arm would see it as an empty `""` and take two of the
            // three quotes.
            match self.multiline() {
                Multiline::No => {}
                Multiline::Closed => continue,
                Multiline::Open(carry) => return carry,
            }
            if self.string() || self.number() || self.word() || self.shell_special() {
                continue;
            }
            // Punctuation and anything unclaimed: one char, held as plain.
            let c = self.rest.chars().next().expect("rest is not empty");
            self.pending.push(c);
            self.rest = &self.rest[c.len_utf8()..];
        }
        Carry::None
    }

    /// Emits the last pending run.
    fn finish(mut self) -> Vec<(String, Style)> {
        self.flush();
        self.out
    }

    /// Emits the pending run in the terminal's own foreground.
    ///
    /// Not `CODE`: once a block is highlighted, the uncoloured tokens are the
    /// background against which the coloured ones read. Painting them too
    /// would leave nothing plain to contrast with — and `CODE` is aqua, which
    /// is what types already use.
    fn flush(&mut self) {
        if !self.pending.is_empty() {
            let text = std::mem::take(&mut self.pending);
            self.out.push((text, theme::PLAIN));
        }
    }

    /// Emits `len` bytes of `rest` as one styled span.
    fn emit(&mut self, len: usize, style: Style) {
        self.flush();
        self.out.push((self.rest[..len].to_owned(), style));
        self.rest = &self.rest[len..];
    }

    /// Whitespace joins the pending run; it never breaks a span.
    fn whitespace(&mut self) -> bool {
        let len = self
            .rest
            .find(|c: char| !c.is_whitespace())
            .unwrap_or(self.rest.len());
        if len == 0 {
            return false;
        }
        self.pending.push_str(&self.rest[..len]);
        self.rest = &self.rest[len..];
        true
    }

    /// A line comment runs to the end of the line; a block comment may not.
    fn comment(&mut self) -> bool {
        for marker in self.line_comments() {
            if self.rest.starts_with(marker) {
                let len = self.rest.len();
                self.emit(len, theme::SYN_COMMENT);
                return true;
            }
        }
        false
    }

    fn line_comments(&self) -> &'static [&'static str] {
        match self.language {
            // JSON has no comments; JSONC's are C-style and harmless here.
            Language::Rust | Language::CFamily | Language::Json => &["//"],
            Language::Python | Language::Shell | Language::Config => &["#"],
            Language::Plain => &[],
        }
    }

    /// `/*` and Python's `"""`: delimiters a line *may* not close, but very
    /// often does.
    ///
    /// A one-line `"""Adds two numbers."""` is the ordinary way to comment
    /// Python, and treating its opening marker as unclosed would paint the
    /// rest of the block as one long string.
    fn multiline(&mut self) -> Multiline {
        let block = matches!(
            self.language,
            Language::Rust | Language::CFamily | Language::Json
        ) && self.rest.starts_with("/*");

        let (marker, closing, carry, style) = if block {
            ("/*", "*/", Carry::BlockComment, theme::SYN_COMMENT)
        } else if self.language == Language::Python && self.rest.starts_with(r#"""""#) {
            let m = r#"""""#;
            (m, m, Carry::Docstring(true), theme::SYN_STRING)
        } else if self.language == Language::Python && self.rest.starts_with("'''") {
            ("'''", "'''", Carry::Docstring(false), theme::SYN_STRING)
        } else {
            return Multiline::No;
        };

        // Search past the opening marker, so `"""` does not close itself.
        if let Some(at) = self.rest[marker.len()..].find(closing) {
            self.emit(marker.len() + at + closing.len(), style);
            return Multiline::Closed;
        }
        let len = self.rest.len();
        self.emit(len, style);
        Multiline::Open(carry)
    }

    /// Consumes a block comment continued from an earlier line. Returns
    /// whether it closed on this one.
    fn resume_block_comment(&mut self) -> bool {
        if let Some(at) = self.rest.find("*/") {
            self.emit(at + 2, theme::SYN_COMMENT);
            return true;
        }
        let len = self.rest.len();
        self.emit(len, theme::SYN_COMMENT);
        false
    }

    /// The same, for a Python docstring.
    fn resume_docstring(&mut self, double: bool) -> bool {
        let marker = if double { r#"""""# } else { "'''" };
        if let Some(at) = self.rest.find(marker) {
            self.emit(at + marker.len(), theme::SYN_STRING);
            return true;
        }
        let len = self.rest.len();
        self.emit(len, theme::SYN_STRING);
        false
    }

    /// A quoted string, including Rust's raw strings.
    ///
    /// An unterminated string is styled to the end of the line rather than
    /// abandoned: a half-streamed line ends mid-string constantly, and the
    /// colour is still the truth about what is being typed.
    fn string(&mut self) -> bool {
        if self.language == Language::Rust && self.raw_string() {
            return true;
        }
        let quotes: &[char] = match self.language {
            // Rust's `'` is a lifetime far more often than a char literal.
            Language::Rust | Language::Json => &['"'],
            Language::CFamily | Language::Python | Language::Config => &['"', '\''],
            // Shell backticks are command substitution, but colouring them as
            // a string reads better than leaving them bare.
            Language::Shell => &['"', '\'', '`'],
            Language::Plain => &[],
        };
        let Some(quote) = self.rest.chars().next().filter(|c| quotes.contains(c)) else {
            return false;
        };
        // A Rust `'` is a lifetime unless it closes within a char's length.
        if self.language == Language::Rust && quote == '\'' {
            return false;
        }
        let len = self.quoted(quote);
        self.emit(len, theme::SYN_STRING);
        true
    }

    /// The byte length of the string starting at `rest`, quote included.
    fn quoted(&self, quote: char) -> usize {
        let body = &self.rest[quote.len_utf8()..];
        let mut escaped = false;
        for (at, c) in body.char_indices() {
            if escaped {
                escaped = false;
            } else if c == '\\' && self.language != Language::Config {
                escaped = true;
            } else if c == quote {
                return quote.len_utf8() + at + c.len_utf8();
            }
        }
        self.rest.len()
    }

    /// `r"..."`, `r#"..."#` — Rust's raw strings, which this codebase uses.
    fn raw_string(&mut self) -> bool {
        let Some(after_r) = self
            .rest
            .strip_prefix("br")
            .or_else(|| self.rest.strip_prefix('r'))
        else {
            return false;
        };
        let hashes = after_r.chars().take_while(|c| *c == '#').count();
        let Some(body) = after_r[hashes..].strip_prefix('"') else {
            return false;
        };
        let closing = format!("\"{}", "#".repeat(hashes));
        let end = body.find(&closing).map_or(self.rest.len(), |at| {
            self.rest.len() - body.len() + at + closing.len()
        });
        self.emit(end, theme::SYN_STRING);
        true
    }

    /// A numeric literal: decimal, hex, binary, float, with `_` separators and
    /// a trailing type suffix.
    fn number(&mut self) -> bool {
        let mut chars = self.rest.chars();
        if !chars.next().is_some_and(|c| c.is_ascii_digit()) {
            return false;
        }
        // Not a number if it is the tail of an identifier — the word arm runs
        // first for those, so reaching here means the previous char was not
        // one. Guard on the pending run's last char to be sure.
        if self
            .pending
            .chars()
            .next_back()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
        {
            return false;
        }
        let len = self
            .rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.'))
            .unwrap_or(self.rest.len());
        self.emit(len, theme::SYN_NUMBER);
        true
    }

    /// An identifier, classified as a keyword, a literal, a type or plain.
    fn word(&mut self) -> bool {
        let first = self
            .rest
            .chars()
            .next()
            .filter(|c| c.is_alphabetic() || *c == '_');
        if first.is_none() {
            return false;
        }
        let len = self
            .rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(self.rest.len());
        let word = &self.rest[..len];

        let style = if self.keywords().contains(&word) {
            theme::SYN_KEYWORD
        } else if self.literals().contains(&word) {
            theme::SYN_NUMBER
        } else if self.is_type(word) {
            theme::SYN_TYPE
        } else if self.rest[len..].starts_with('(') {
            theme::SYN_CALL
        } else {
            // Plain identifiers join the pending run.
            self.pending.push_str(word);
            self.rest = &self.rest[len..];
            return true;
        };
        self.emit(len, style);
        true
    }

    /// A name that looks like a type: `CamelCase`, or a known primitive.
    ///
    /// Config and shell have no type namespace, so nothing qualifies there —
    /// an env var like `PATH` is not a type.
    fn is_type(&self, word: &str) -> bool {
        match self.language {
            Language::Rust => {
                RUST_PRIMITIVES.contains(&word) || word.starts_with(char::is_uppercase)
            }
            Language::CFamily | Language::Python => word.starts_with(char::is_uppercase),
            _ => false,
        }
    }

    /// Shell's own shapes: `$VAR`, `${VAR}`, and `-x` / `--long` flags.
    fn shell_special(&mut self) -> bool {
        if self.language != Language::Shell {
            return false;
        }
        if self.rest.starts_with('$') {
            let body = &self.rest[1..];
            let len = if body.starts_with('{') {
                body.find('}').map_or(self.rest.len(), |at| at + 2)
            } else {
                1 + body
                    .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .unwrap_or(body.len())
            };
            self.emit(len, theme::SYN_TYPE);
            return true;
        }
        // A flag, not a subtraction: `-` at the start of a word.
        let boundary = self
            .pending
            .chars()
            .next_back()
            .is_none_or(char::is_whitespace);
        if boundary && self.rest.starts_with('-') {
            let len = self
                .rest
                .find(char::is_whitespace)
                .unwrap_or(self.rest.len());
            if len > 1 {
                self.emit(len, theme::SYN_KEYWORD);
                return true;
            }
        }
        false
    }

    fn keywords(&self) -> &'static [&'static str] {
        match self.language {
            Language::Rust => RUST,
            Language::CFamily => C_FAMILY,
            Language::Python => PYTHON,
            Language::Shell => SHELL,
            Language::Config | Language::Json | Language::Plain => &[],
        }
    }

    /// Words that are values, not operations — coloured like the numbers they
    /// keep company with.
    fn literals(&self) -> &'static [&'static str] {
        match self.language {
            Language::Rust | Language::CFamily => &["true", "false", "null", "nil", "None"],
            Language::Python => &["True", "False", "None"],
            Language::Config | Language::Json => &["true", "false", "null"],
            Language::Shell | Language::Plain => &[],
        }
    }
}

const RUST: &[&str] = &[
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern",
    "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
    "return", "self", "Self", "static", "struct", "super", "trait", "type", "unsafe", "use",
    "where", "while", "yield",
];

const RUST_PRIMITIVES: &[&str] = &[
    "bool", "char", "f32", "f64", "i8", "i16", "i32", "i64", "i128", "isize", "str", "u8", "u16",
    "u32", "u64", "u128", "usize",
];

const C_FAMILY: &[&str] = &[
    "break",
    "case",
    "catch",
    "chan",
    "class",
    "const",
    "continue",
    "default",
    "defer",
    "delete",
    "do",
    "else",
    "enum",
    "export",
    "extends",
    "extern",
    "final",
    "finally",
    "float",
    "for",
    "func",
    "function",
    "go",
    "goto",
    "if",
    "implements",
    "import",
    "in",
    "instanceof",
    "interface",
    "let",
    "map",
    "new",
    "package",
    "private",
    "protected",
    "public",
    "range",
    "return",
    "select",
    "static",
    "struct",
    "switch",
    "this",
    "throw",
    "try",
    "type",
    "typeof",
    "var",
    "void",
    "while",
];

const PYTHON: &[&str] = &[
    "and", "as", "assert", "async", "await", "break", "class", "continue", "def", "del", "elif",
    "else", "except", "finally", "for", "from", "global", "if", "import", "in", "is", "lambda",
    "nonlocal", "not", "or", "pass", "raise", "return", "try", "while", "with", "yield",
];

const SHELL: &[&str] = &[
    "case", "do", "done", "elif", "else", "esac", "export", "fi", "for", "function", "if", "in",
    "local", "read", "readonly", "return", "then", "unset", "until", "while",
];

#[cfg(test)]
mod tests {
    use super::*;

    /// The line as `(text, role)` pairs, with roles named for legibility.
    fn lex(line: &str, language: Language) -> Vec<(String, &'static str)> {
        let (spans, _) = highlight(line, language, Carry::None);
        spans
            .into_iter()
            .map(|(text, style)| (text, role(style)))
            .collect()
    }

    fn role(style: Style) -> &'static str {
        match style {
            s if s == theme::SYN_COMMENT => "comment",
            s if s == theme::SYN_STRING => "string",
            s if s == theme::SYN_NUMBER => "number",
            s if s == theme::SYN_KEYWORD => "keyword",
            s if s == theme::SYN_TYPE => "type",
            s if s == theme::SYN_CALL => "call",
            _ => "plain",
        }
    }

    /// Every span concatenated must be the input, byte for byte. A highlighter
    /// that drops or duplicates a character is worse than none.
    fn assert_lossless(line: &str, language: Language) {
        let (spans, _) = highlight(line, language, Carry::None);
        let rebuilt: String = spans.iter().map(|(text, _)| text.as_str()).collect();
        assert_eq!(rebuilt, line, "highlighting must preserve the text");
    }

    #[test]
    fn rust_keywords_types_and_literals() {
        assert_eq!(
            lex("pub fn seq(&self) -> u64 {", Language::Rust),
            [
                ("pub".to_owned(), "keyword"),
                (" ".to_owned(), "plain"),
                ("fn".to_owned(), "keyword"),
                (" ".to_owned(), "plain"),
                ("seq".to_owned(), "call"),
                ("(&".to_owned(), "plain"),
                ("self".to_owned(), "keyword"),
                (") -> ".to_owned(), "plain"),
                ("u64".to_owned(), "type"),
                (" {".to_owned(), "plain"),
            ]
        );
    }

    #[test]
    fn strings_numbers_and_comments() {
        assert_eq!(
            lex("let n = 42; // count", Language::Rust),
            [
                ("let".to_owned(), "keyword"),
                (" n = ".to_owned(), "plain"),
                ("42".to_owned(), "number"),
                ("; ".to_owned(), "plain"),
                ("// count".to_owned(), "comment"),
            ]
        );
        assert_eq!(
            lex(r#"m("a\"b", 0xff)"#, Language::Rust),
            [
                ("m".to_owned(), "call"),
                ("(".to_owned(), "plain"),
                (r#""a\"b""#.to_owned(), "string"),
                (", ".to_owned(), "plain"),
                ("0xff".to_owned(), "number"),
                (")".to_owned(), "plain"),
            ],
            "an escaped quote does not end the string"
        );
    }

    #[test]
    fn rust_raw_strings_and_lifetimes() {
        assert_eq!(
            lex(r##"let s = r#"a "quoted" b"#;"##, Language::Rust),
            [
                ("let".to_owned(), "keyword"),
                (" s = ".to_owned(), "plain"),
                (r##"r#"a "quoted" b"#"##.to_owned(), "string"),
                (";".to_owned(), "plain"),
            ]
        );
        assert_eq!(
            lex("fn f<'a>(x: &'a str)", Language::Rust),
            [
                ("fn".to_owned(), "keyword"),
                (" f<'a>(x: &'a ".to_owned(), "plain"),
                ("str".to_owned(), "type"),
                (")".to_owned(), "plain"),
            ],
            "a lifetime is not an unterminated char literal"
        );
    }

    /// The streaming case: a code line arrives a few characters at a time.
    #[test]
    fn an_unterminated_string_colours_to_the_end_of_the_line() {
        assert_eq!(
            lex(r#"let s = "half a th"#, Language::Rust),
            [
                ("let".to_owned(), "keyword"),
                (" s = ".to_owned(), "plain"),
                (r#""half a th"#.to_owned(), "string"),
            ]
        );
    }

    #[test]
    fn block_comments_carry_across_lines() {
        let (_, carry) = highlight("code /* opened", Language::Rust, Carry::None);
        assert_eq!(carry, Carry::BlockComment);

        let (middle, carry) = highlight("still inside", Language::Rust, carry);
        assert_eq!(role(middle[0].1), "comment");
        assert_eq!(carry, Carry::BlockComment);

        let (closing, carry) = highlight("done */ let x", Language::Rust, carry);
        let closing: Vec<(String, &str)> = closing
            .into_iter()
            .map(|(text, style)| (text, role(style)))
            .collect();
        assert_eq!(
            closing,
            [
                ("done */".to_owned(), "comment"),
                (" ".to_owned(), "plain"),
                ("let".to_owned(), "keyword"),
                (" x".to_owned(), "plain"),
            ],
            "the comment ends where it closes and lexing resumes after it"
        );
        assert_eq!(carry, Carry::None);
    }

    #[test]
    fn python_docstrings_carry_across_lines() {
        let (_, carry) = highlight("def f():", Language::Python, Carry::None);
        assert_eq!(carry, Carry::None);

        let (_, carry) = highlight(r#"    """what it does"#, Language::Python, carry);
        assert_eq!(carry, Carry::Docstring(true));

        let (closing, carry) = highlight(r#"    """"#, Language::Python, carry);
        assert_eq!(role(closing[0].1), "string");
        assert_eq!(carry, Carry::None);
    }

    /// The ordinary way to comment Python. Treating the opening `"""` as
    /// unclosed painted the whole rest of the block as one string.
    #[test]
    fn a_docstring_that_closes_on_its_own_line_carries_nothing() {
        let (spans, carry) = highlight(
            r#"    """Adds two numbers.""""#,
            Language::Python,
            Carry::None,
        );

        let named: Vec<(String, &str)> = spans
            .into_iter()
            .map(|(text, style)| (text, role(style)))
            .collect();
        assert_eq!(
            named,
            [
                ("    ".to_owned(), "plain"),
                (r#""""Adds two numbers.""""#.to_owned(), "string"),
            ]
        );
        assert_eq!(carry, Carry::None, "the next line lexes normally");

        // And the line after it is untouched.
        assert_eq!(
            lex("return a + b", Language::Python)[0],
            ("return".to_owned(), "keyword")
        );
    }

    /// The same trap in the C family.
    #[test]
    fn a_block_comment_that_closes_on_its_own_line_carries_nothing() {
        let (spans, carry) = highlight("/* note */ let x = 1;", Language::Rust, Carry::None);

        assert_eq!(spans[0].0, "/* note */");
        assert_eq!(role(spans[0].1), "comment");
        assert_eq!(role(spans[2].1), "keyword", "lexing resumes after it");
        assert_eq!(carry, Carry::None);
    }

    #[test]
    fn python_reads_its_own_keywords() {
        assert_eq!(
            lex("def f(x): return None", Language::Python),
            [
                ("def".to_owned(), "keyword"),
                (" ".to_owned(), "plain"),
                ("f".to_owned(), "call"),
                ("(x): ".to_owned(), "plain"),
                ("return".to_owned(), "keyword"),
                (" ".to_owned(), "plain"),
                ("None".to_owned(), "number"),
            ]
        );
    }

    #[test]
    fn shell_variables_and_flags() {
        assert_eq!(
            lex("ls -la $HOME # list", Language::Shell),
            [
                ("ls ".to_owned(), "plain"),
                ("-la".to_owned(), "keyword"),
                (" ".to_owned(), "plain"),
                ("$HOME".to_owned(), "type"),
                (" ".to_owned(), "plain"),
                ("# list".to_owned(), "comment"),
            ]
        );
        assert_eq!(
            lex("echo ${VAR}", Language::Shell),
            [("echo ".to_owned(), "plain"), ("${VAR}".to_owned(), "type"),]
        );
    }

    #[test]
    fn config_files_get_comments_strings_and_numbers() {
        assert_eq!(
            lex("port = 8080 # the sidecar", Language::Config),
            [
                ("port = ".to_owned(), "plain"),
                ("8080".to_owned(), "number"),
                (" ".to_owned(), "plain"),
                ("# the sidecar".to_owned(), "comment"),
            ]
        );
        assert_eq!(
            lex(r#"server = "llama-server""#, Language::Config),
            [
                ("server = ".to_owned(), "plain"),
                (r#""llama-server""#.to_owned(), "string"),
            ]
        );
    }

    /// Asserted on the style directly: an unhighlighted block keeps `CODE`,
    /// which shares aqua with `SYN_TYPE` and so cannot be told apart by
    /// [`role`]. The two never meet on a line — a block is either highlighted
    /// or it is not.
    #[test]
    fn an_unknown_language_is_left_alone() {
        let (spans, carry) = highlight("fn main() { let x = 1; }", Language::Plain, Carry::None);

        assert_eq!(spans.len(), 1, "one span, unlexed");
        assert_eq!(spans[0].0, "fn main() { let x = 1; }");
        assert_eq!(spans[0].1, theme::CODE);
        assert_eq!(carry, Carry::None, "an unlexed block opens nothing");
    }

    /// Verbatim from a reply that rendered flat: the model opened a bare
    /// fence, so nothing was highlighted and the whole block came out one
    /// colour.
    #[test]
    fn an_untagged_python_block_is_recognised() {
        let block = [
            "# Demonstrating a simple Python function with a comment, string, and number",
            "def example():",
            "    # Perform an operation",
            "    message = \"Task completed\"",
            "    number = 3.14",
            "    return number",
        ];
        assert_eq!(sniff(&block), Language::Python);
    }

    #[test]
    fn sniffing_recognises_the_languages_it_knows() {
        assert_eq!(
            sniff(&["fn main() {", "    let x: Vec<u8> = vec![];", "}"]),
            Language::Rust
        );
        assert_eq!(
            sniff(&["func main() {", "\tpackage main", "}"]),
            Language::CFamily
        );
        assert_eq!(
            sniff(&["cd /home/bogdan/arc", "just test", "git status"]),
            Language::Shell
        );
        assert_eq!(
            sniff(&["{", "  \"port\": 8080", "}"]),
            Language::Json,
            "quoted keys mean JSON"
        );
        assert_eq!(
            sniff(&["[llama]", "port = 8080"]),
            Language::Config,
            "a section header with bare keys is TOML"
        );
    }

    /// The bar for guessing: no signal, or an even split, means no colour.
    #[test]
    fn sniffing_declines_when_it_cannot_tell() {
        assert_eq!(sniff(&["hello world", "second line"]), Language::Plain);
        assert_eq!(sniff(&[]), Language::Plain);
        assert_eq!(sniff(&[""]), Language::Plain);
        assert_eq!(
            sniff(&["some prose about a def and a fn"]),
            Language::Plain,
            "one hit each is a tie, and a tie declines"
        );
    }

    #[test]
    fn info_strings_map_to_languages() {
        assert_eq!(language("rust"), Language::Rust);
        assert_eq!(
            language("rust,ignore"),
            Language::Rust,
            "attributes are ignored"
        );
        assert_eq!(
            language("  BASH  "),
            Language::Shell,
            "case and space are ignored"
        );
        assert_eq!(language("ts"), Language::CFamily);
        assert_eq!(language("toml"), Language::Config);
        assert_eq!(
            language(""),
            Language::Plain,
            "a bare fence highlights nothing"
        );
        assert_eq!(
            language("brainfuck"),
            Language::Plain,
            "unknown is not a guess"
        );
    }

    #[test]
    fn identifiers_with_digits_are_not_numbers() {
        assert_eq!(
            lex("let sha256 = 1", Language::Rust),
            [
                ("let".to_owned(), "keyword"),
                (" sha256 = ".to_owned(), "plain"),
                ("1".to_owned(), "number"),
            ]
        );
    }

    #[test]
    fn nothing_is_ever_dropped() {
        for (line, language) in [
            (r#"let s = "a\"b"; // x"#, Language::Rust),
            (r##"r#"raw"#"##, Language::Rust),
            ("fn f<'a>(x: &'a str) -> u8 { 0 }", Language::Rust),
            ("  spaced   out  ", Language::Rust),
            ("def f(): pass  # c", Language::Python),
            ("ls -la $HOME/x", Language::Shell),
            ("k = 'v' # c", Language::Config),
            ("émoji = \"héllo 🦀\"", Language::Rust),
            ("", Language::Rust),
        ] {
            assert_lossless(line, language);
        }
    }
}
