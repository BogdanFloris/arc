//! Markdown for the transcript: text in, wrapped styled lines out.
//!
//! Hand-rolled and line-oriented, for two reasons. It has to render *partial*
//! input — a reply is drawn on every delta, so half the document is a half-
//! written sentence with an unclosed `**` in it — and a line-oriented pass
//! degrades into plain text there instead of reflowing the screen when the
//! closing marker finally arrives. And the ASCII-minimal look (6.1) wants
//! headings and quotes carried by color and weight, not by drawn boxes, so
//! most of what a full parser gives us would go unused.
//!
//! Supported: ATX headings, fenced code, blockquotes, horizontal rules,
//! bullet and ordered lists, and inline `**bold**`, `*italic*`, `` `code` ``.
//!
//! Underscores are deliberately *not* emphasis. `_` shows up inside
//! identifiers far more often than it opens an italic in a chat about this
//! codebase, and `snake_case_names` rendering as italics is a worse failure
//! than `_this_` staying literal.

use ratatui::style::Style;
use ratatui::text::{Line, Span};
use textwrap::core::display_width;

use crate::{syntax, theme};

/// How far continuation lines sit in from the first line of a block. Two
/// columns: enough to read as "still the same paragraph", not so much that a
/// wrapped reply looks nested.
const CONTINUATION: &str = "  ";

/// Code block contents, in from the surrounding text.
const CODE_INDENT: &str = "    ";

/// Renders `text` as markdown, wrapped to `width` columns.
///
/// `base` styles anything the markup does not claim — the caller's voice for
/// this block (plain for a message, dim for an error's detail).
pub fn render(text: &str, width: usize, base: Style) -> Vec<Line<'static>> {
    let width = width.max(8);
    let mut out = Vec::new();
    // `Some` while inside a fence: the language its info string named, and
    // whatever the last line left open. Both reset at every fence, so a
    // half-streamed block cannot leak its state into the next one.
    let mut fenced: Option<(syntax::Language, syntax::Carry)> = None;

    let lines: Vec<&str> = text.split('\n').collect();
    for (at, raw) in lines.iter().enumerate() {
        if let Some(info) = fence_info(raw) {
            fenced = match fenced {
                Some(_) => None,
                // An untagged fence gets its language sniffed from the block
                // it opens, which means looking ahead to the closing fence.
                None if info.trim().is_empty() => {
                    Some((syntax::sniff(block_after(&lines, at)), syntax::Carry::None))
                }
                None => Some((syntax::language(info), syntax::Carry::None)),
            };
            continue;
        }
        if let Some((language, carry)) = fenced {
            fenced = Some((language, push_code(&mut out, raw, width, language, carry)));
            continue;
        }
        let line = raw.trim_end();
        if line.trim().is_empty() {
            out.push(Line::default());
        } else if let Some(rule) = horizontal_rule(line, width) {
            out.push(rule);
        } else if let Some(rest) = heading(line) {
            // Level shows as weight and color, not as indentation: a reply's
            // top heading may be `#` or `###` depending on nothing, and
            // indenting by the absolute level would stagger equals.
            wrap_into(
                &mut out,
                inline(rest, theme::HEADING),
                width,
                Indent::none(),
            );
        } else if let Some(rest) = blockquote(line) {
            let gutter = Indent {
                initial: " | ",
                hanging: " | ",
                style: theme::QUOTE,
            };
            wrap_into(&mut out, inline(rest, theme::QUOTE), width, gutter);
        } else if let Some((marker, rest)) = list_item(line) {
            // Hanging indent: continuation lines align with the text, not the
            // bullet, so the marker column stays a clean vertical edge.
            let hanging = " ".repeat(display_width(&marker));
            let mut spans = vec![(marker, theme::MARKER)];
            spans.extend(inline(rest, base));
            wrap_into(
                &mut out,
                spans,
                width,
                Indent {
                    initial: "",
                    hanging: &hanging,
                    style: base,
                },
            );
        } else {
            wrap_into(
                &mut out,
                inline(line, base),
                width,
                Indent {
                    initial: "",
                    hanging: CONTINUATION,
                    style: base,
                },
            );
        }
    }
    out
}

/// The lines of the block opened at `at`, up to its closing fence.
///
/// An unclosed block runs to the end of the text — which is every block, half
/// the time, since a reply is drawn while it is still streaming. Sniffing what
/// has arrived so far is the point: the language is settled from the first few
/// lines and does not flicker as the rest lands.
fn block_after<'a>(lines: &'a [&'a str], at: usize) -> &'a [&'a str] {
    let body = &lines[at + 1..];
    let end = body
        .iter()
        .position(|line| fence_info(line).is_some())
        .unwrap_or(body.len());
    &body[..end]
}

/// A ```` ``` ```` or `~~~` fence, and the info string after it.
fn fence_info(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    trimmed
        .strip_prefix("```")
        .or_else(|| trimmed.strip_prefix("~~~"))
}

/// A fenced line, highlighted and indented. Returns what it leaves open for
/// the next line of the block.
///
/// Hard-wrapped at the display width rather than word-wrapped: code means what
/// its columns say, and dropping the overflow off the right edge would hide it
/// entirely. The wrap splits styled spans, so a long string keeps its colour
/// across the break.
fn push_code(
    out: &mut Vec<Line<'static>>,
    raw: &str,
    width: usize,
    language: syntax::Language,
    carry: syntax::Carry,
) -> syntax::Carry {
    let room = width.saturating_sub(CODE_INDENT.len()).max(1);
    let (spans, carry) = syntax::highlight(raw, language, carry);

    let mut row: Vec<Span<'static>> = vec![Span::raw(CODE_INDENT)];
    let mut col = 0;
    for (text, style) in spans {
        let mut rest = text.as_str();
        while !rest.is_empty() {
            let take = rest
                .char_indices()
                .nth(room - col)
                .map_or(rest.len(), |(at, _)| at);
            let (head, tail) = rest.split_at(take);
            col += head.chars().count();
            row.push(Span::styled(head.to_owned(), style));
            if tail.is_empty() {
                break;
            }
            out.push(Line::from(std::mem::take(&mut row)));
            row.push(Span::raw(CODE_INDENT));
            col = 0;
            rest = tail;
        }
    }
    out.push(Line::from(row));
    carry
}

/// `---`, `***` or `___` alone on a line, drawn as a dim rule.
fn horizontal_rule(line: &str, width: usize) -> Option<Line<'static>> {
    let trimmed = line.trim();
    let marker = trimmed.chars().next()?;
    let is_rule = matches!(marker, '-' | '*' | '_')
        && trimmed.len() >= 3
        && trimmed.chars().all(|c| c == marker);
    is_rule.then(|| Line::styled("-".repeat(width), theme::DIM))
}

/// `## Heading` — the text after the hashes.
fn heading(line: &str) -> Option<&str> {
    let hashes = line.chars().take_while(|c| *c == '#').count();
    if hashes == 0 || hashes > 6 {
        return None;
    }
    let rest = line[hashes..].strip_prefix(' ')?;
    Some(rest.trim_start())
}

/// `> quoted` — the text after the marker.
fn blockquote(line: &str) -> Option<&str> {
    let rest = line.trim_start().strip_prefix('>')?;
    Some(rest.strip_prefix(' ').unwrap_or(rest))
}

/// `- item` or `3. item` — the marker to draw, and the text after it.
///
/// The marker keeps the source's leading whitespace, so nested lists stay
/// nested; bullets are normalised to `-` so a document that mixes `*` and `+`
/// still renders as one list.
fn list_item(line: &str) -> Option<(String, &str)> {
    let indent = line.len() - line.trim_start().len();
    let body = &line[indent..];
    let pad = &line[..indent];

    if let Some(rest) = body
        .strip_prefix("- ")
        .or_else(|| body.strip_prefix("* "))
        .or_else(|| body.strip_prefix("+ "))
    {
        return Some((format!("{pad}- "), rest.trim_start()));
    }

    let digits = body.chars().take_while(char::is_ascii_digit).count();
    if digits > 0 {
        let rest = body[digits..].strip_prefix(". ")?;
        let number = &body[..digits];
        return Some((format!("{pad}{number}. "), rest.trim_start()));
    }
    None
}

/// Splits one line into styled segments on its inline markup.
///
/// Unclosed markup is left as literal text — the half-typed `**` at the end of
/// a streaming reply must not turn the rest of the message bold.
fn inline(line: &str, base: Style) -> Vec<(String, Style)> {
    let mut out: Vec<(String, Style)> = Vec::new();
    let mut plain = String::new();
    let mut rest = line;

    while !rest.is_empty() {
        let taken = if let Some(escaped) = escape(rest) {
            plain.push(escaped);
            2
        } else if let Some((body, len)) = delimited(rest, "`") {
            flush(&mut out, &mut plain, base);
            out.push((body.to_owned(), theme::CODE));
            len
        } else if let Some((body, len)) = delimited(rest, "**") {
            flush(&mut out, &mut plain, base);
            out.push((body.to_owned(), base.patch(theme::STRONG)));
            len
        } else if let Some((body, len)) = delimited(rest, "*") {
            flush(&mut out, &mut plain, base);
            out.push((body.to_owned(), base.patch(theme::EMPHASIS)));
            len
        } else {
            let c = rest.chars().next().expect("rest is not empty");
            plain.push(c);
            c.len_utf8()
        };
        rest = &rest[taken..];
    }
    flush(&mut out, &mut plain, base);
    out
}

/// A backslash-escaped markup character, as the literal it stands for.
fn escape(rest: &str) -> Option<char> {
    let after = rest.strip_prefix('\\')?;
    let c = after.chars().next()?;
    matches!(c, '*' | '`' | '_' | '\\' | '#' | '>' | '-').then_some(c)
}

/// The text between a matched pair of `mark`s, and the bytes the whole span
/// took. `None` when the pair does not close on this line, or closes on
/// nothing (`****` is not empty bold, it is four asterisks).
fn delimited<'a>(rest: &'a str, mark: &str) -> Option<(&'a str, usize)> {
    let after = rest.strip_prefix(mark)?;
    // `*` must not swallow the `**` case; the caller tries the longer mark
    // first, so anything still starting with the mark here is a run, not a
    // delimiter.
    if after.starts_with(mark) {
        return None;
    }
    let end = after.find(mark)?;
    (end > 0).then(|| (&after[..end], mark.len() * 2 + end))
}

/// Moves the pending plain run into `out`.
fn flush(out: &mut Vec<(String, Style)>, plain: &mut String, base: Style) {
    if !plain.is_empty() {
        out.push((std::mem::take(plain), base));
    }
}

/// What sits at the left of a wrapped block: the first line's prefix, every
/// later line's prefix, and the style both are drawn in.
///
/// The prefixes are spans in their own right rather than leading spaces in the
/// text, because [`split_words`] eats whitespace — and because a blockquote's
/// `|` gutter has to carry a color.
#[derive(Clone, Copy)]
struct Indent<'a> {
    initial: &'a str,
    hanging: &'a str,
    style: Style,
}

impl Indent<'_> {
    /// Flush left, both lines.
    fn none() -> Self {
        Self {
            initial: "",
            hanging: "",
            style: Style::new(),
        }
    }
}

/// Greedily wraps styled segments to `width`, appending the lines to `out`.
///
/// Wrapping happens across segments, not within them: `**two words**` breaks
/// between its words and both halves stay bold.
fn wrap_into(
    out: &mut Vec<Line<'static>>,
    segments: Vec<(String, Style)>,
    width: usize,
    indent: Indent<'_>,
) {
    let words = split_words(segments);
    if words.is_empty() {
        out.push(Line::default());
        return;
    }

    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut col = 0;
    let mut first_on_line = true;
    let mut prefix = indent.initial;

    for word in words {
        let word_width: usize = word.iter().map(|(text, _)| display_width(text)).sum();
        if first_on_line {
            if !prefix.is_empty() {
                spans.push(Span::styled(prefix.to_owned(), indent.style));
                col += display_width(prefix);
            }
        } else if col + 1 + word_width > width {
            out.push(Line::from(std::mem::take(&mut spans)));
            prefix = indent.hanging;
            col = display_width(prefix);
            if !prefix.is_empty() {
                spans.push(Span::styled(prefix.to_owned(), indent.style));
            }
        } else {
            spans.push(Span::raw(" "));
            col += 1;
        }
        for (text, style) in word {
            col += display_width(&text);
            spans.push(Span::styled(text, style));
        }
        first_on_line = false;
    }
    if !spans.is_empty() {
        out.push(Line::from(spans));
    }
}

/// Groups segments into whitespace-free words, each a run of styled pieces.
///
/// A word can span segments — `**bo**ld` is one word in two styles — so the
/// grouping is by whitespace in the concatenated text, not by segment.
fn split_words(segments: Vec<(String, Style)>) -> Vec<Vec<(String, Style)>> {
    let mut words = Vec::new();
    let mut current: Vec<(String, Style)> = Vec::new();

    for (text, style) in segments {
        let mut rest = text.as_str();
        while !rest.is_empty() {
            let Some(at) = rest.find(char::is_whitespace) else {
                current.push((rest.to_owned(), style));
                break;
            };
            if at > 0 {
                current.push((rest[..at].to_owned(), style));
            }
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            let skipped = rest[at..]
                .find(|c: char| !c.is_whitespace())
                .map_or(rest.len(), |i| at + i);
            rest = &rest[skipped..];
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rendered text, one string per line — what the eye sees, ignoring
    /// style.
    fn text(text: &str, width: usize) -> Vec<String> {
        render(text, width, theme::PLAIN)
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// The styles applied across one rendered line, span by span.
    fn styles(text: &str, width: usize) -> Vec<(String, Style)> {
        render(text, width, theme::PLAIN)
            .into_iter()
            .flat_map(|line| line.spans)
            .map(|span| (span.content.into_owned(), span.style))
            .collect()
    }

    #[test]
    fn a_paragraph_wraps_with_a_hanging_indent() {
        assert_eq!(
            text("one two three four five", 12),
            ["one two", "  three four", "  five"],
            "continuation lines sit in past the first"
        );
    }

    #[test]
    fn inline_markup_becomes_style_not_punctuation() {
        assert_eq!(
            styles("a **b** `c` *d*", 40),
            [
                ("a".to_owned(), theme::PLAIN),
                (" ".to_owned(), Style::default()),
                ("b".to_owned(), theme::PLAIN.patch(theme::STRONG)),
                (" ".to_owned(), Style::default()),
                ("c".to_owned(), theme::CODE),
                (" ".to_owned(), Style::default()),
                ("d".to_owned(), theme::PLAIN.patch(theme::EMPHASIS)),
            ]
        );
    }

    /// The streaming case: every delta redraws, so most frames end mid-markup.
    #[test]
    fn unclosed_markup_stays_literal() {
        assert_eq!(text("half a **thou", 40), ["half a **thou"]);
        assert_eq!(text("and `code", 40), ["and `code"]);
        assert_eq!(
            styles("**bold** then **half", 40)
                .into_iter()
                .filter(|(_, style)| *style == theme::PLAIN.patch(theme::STRONG))
                .count(),
            1,
            "only the closed pair is bold"
        );
    }

    #[test]
    fn underscores_are_left_alone() {
        assert_eq!(
            text("call snake_case_name here", 40),
            ["call snake_case_name here"],
            "identifiers are commoner than underscore emphasis"
        );
    }

    #[test]
    fn escaped_markup_renders_literally() {
        assert_eq!(text(r"a \*not italic\* b", 40), ["a *not italic* b"]);
    }

    #[test]
    fn headings_lose_their_hashes_and_gain_the_accent() {
        assert_eq!(text("## Walking skeleton", 40), ["Walking skeleton"]);
        assert_eq!(
            styles("## Walking skeleton", 40)[0].1,
            theme::HEADING,
            "the heading style, not the base"
        );
        assert_eq!(
            text("### deep", 40),
            ["deep"],
            "every level sits flush left; style carries the difference"
        );
        assert_eq!(
            text("#nospace", 40),
            ["#nospace"],
            "a bare # is not a heading"
        );
    }

    #[test]
    fn lists_normalise_their_markers_and_hang() {
        assert_eq!(
            text("- one\n* two\n+ three", 40),
            ["- one", "- two", "- three"]
        );
        assert_eq!(
            text("1. first item here\n2. second", 14),
            ["1. first item", "   here", "2. second"],
            "wrapped text aligns under the text, not the ordinal"
        );
    }

    #[test]
    fn code_blocks_keep_their_columns() {
        assert_eq!(
            text("before\n```rust\nfn main() {}\n```\nafter", 40),
            ["before", "    fn main() {}", "after"],
            "the fences themselves are not drawn"
        );
        assert_eq!(
            styles("```\nlet x = 1;\n```", 40)[1].1,
            theme::SYN_KEYWORD,
            "an untagged block is sniffed, so `let` still reads as Rust"
        );
    }

    #[test]
    fn a_long_code_line_is_cut_into_columns_not_reflowed() {
        assert_eq!(
            text("```\nabcdefghijkl\n```", 12),
            ["    abcdefgh", "    ijkl"],
            "hard wrap at the width; nothing is dropped"
        );
    }

    /// An untagged fence is the common case from a small local model, so the
    /// language has to come from the block itself.
    #[test]
    fn an_untagged_block_is_highlighted_by_what_is_in_it() {
        let reply = "Here it is:\n\n```\ndef example():\n    return 1\n```";
        let lines = render(reply, 40, theme::PLAIN);
        let code: Vec<&Line> = lines
            .iter()
            .filter(|l| l.to_string().contains("def"))
            .collect();

        assert_eq!(code.len(), 1);
        assert_eq!(
            code[0].spans[1].style,
            theme::SYN_KEYWORD,
            "`def` is a keyword, not flat text"
        );
    }

    /// The tag wins even when the content disagrees: the author said what it
    /// is, and second-guessing them is how a highlighter earns distrust.
    #[test]
    fn a_tagged_block_is_not_sniffed() {
        let lines = render("```text\ndef example():\n```", 40, theme::PLAIN);
        let code = lines
            .iter()
            .find(|l| l.to_string().contains("def"))
            .expect("the code line");

        assert_eq!(
            code.spans[1].style,
            theme::CODE,
            "an explicit unknown tag stays plain"
        );
    }

    #[test]
    fn markup_inside_a_code_block_is_not_markup() {
        assert_eq!(text("```\nlet a = **b;\n```", 40), ["    let a = **b;"]);
    }

    #[test]
    fn blockquotes_get_a_gutter_on_every_line() {
        assert_eq!(
            text("> one two three four", 14),
            [" | one two", " | three four"],
            "the gutter repeats down the wrapped block"
        );
    }

    #[test]
    fn a_horizontal_rule_spans_the_width() {
        assert_eq!(text("---", 8), ["--------"]);
        assert_eq!(text("--", 8), ["--"], "two dashes is text; three is a rule");
    }

    #[test]
    fn blank_lines_survive_as_paragraph_breaks() {
        assert_eq!(text("one\n\ntwo", 40), ["one", "", "two"]);
    }

    #[test]
    fn a_word_split_across_styles_stays_one_word() {
        assert_eq!(
            text("xxxxx **bo**ld", 8),
            ["xxxxx", "  bold"],
            "the styled and plain halves wrap together"
        );
    }

    #[test]
    fn an_empty_message_renders_nothing_that_panics() {
        assert_eq!(text("", 40), [""]);
        assert_eq!(text("", 0), [""], "a zero width is clamped, not divided by");
    }
}
