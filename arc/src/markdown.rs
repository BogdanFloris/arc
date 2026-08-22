use ratatui::style::Style;
use ratatui::text::{Line, Span};
use textwrap::core::display_width;

use crate::{syntax, theme};

const CONTINUATION: &str = "  ";

const CODE_INDENT: &str = "    ";

pub fn render(text: &str, width: usize, base: Style) -> Vec<Line<'static>> {
    let width = width.max(8);
    let mut out = Vec::new();
    let mut fenced: Option<(syntax::Language, syntax::Carry)> = None;

    let lines: Vec<&str> = text.split('\n').collect();
    for (at, raw) in lines.iter().enumerate() {
        if let Some(info) = fence_info(raw) {
            fenced = match fenced {
                Some(_) => None,
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

fn block_after<'a>(lines: &'a [&'a str], at: usize) -> &'a [&'a str] {
    let body = &lines[at + 1..];
    let end = body
        .iter()
        .position(|line| fence_info(line).is_some())
        .unwrap_or(body.len());
    &body[..end]
}

fn fence_info(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    trimmed
        .strip_prefix("```")
        .or_else(|| trimmed.strip_prefix("~~~"))
}

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

fn horizontal_rule(line: &str, width: usize) -> Option<Line<'static>> {
    let trimmed = line.trim();
    let marker = trimmed.chars().next()?;
    let is_rule = matches!(marker, '-' | '*' | '_')
        && trimmed.len() >= 3
        && trimmed.chars().all(|c| c == marker);
    is_rule.then(|| Line::styled("-".repeat(width), theme::DIM))
}

fn heading(line: &str) -> Option<&str> {
    let hashes = line.chars().take_while(|c| *c == '#').count();
    if hashes == 0 || hashes > 6 {
        return None;
    }
    let rest = line[hashes..].strip_prefix(' ')?;
    Some(rest.trim_start())
}

fn blockquote(line: &str) -> Option<&str> {
    let rest = line.trim_start().strip_prefix('>')?;
    Some(rest.strip_prefix(' ').unwrap_or(rest))
}

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

fn escape(rest: &str) -> Option<char> {
    let after = rest.strip_prefix('\\')?;
    let c = after.chars().next()?;
    matches!(c, '*' | '`' | '_' | '\\' | '#' | '>' | '-').then_some(c)
}

fn delimited<'a>(rest: &'a str, mark: &str) -> Option<(&'a str, usize)> {
    let after = rest.strip_prefix(mark)?;
    if after.starts_with(mark) {
        return None;
    }
    let end = after.find(mark)?;
    (end > 0).then(|| (&after[..end], mark.len() * 2 + end))
}

fn flush(out: &mut Vec<(String, Style)>, plain: &mut String, base: Style) {
    if !plain.is_empty() {
        out.push((std::mem::take(plain), base));
    }
}

#[derive(Clone, Copy)]
struct Indent<'a> {
    initial: &'a str,
    hanging: &'a str,
    style: Style,
}

impl Indent<'_> {
    fn none() -> Self {
        Self {
            initial: "",
            hanging: "",
            style: Style::new(),
        }
    }
}

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
    use expect_test::expect;

    fn lines(src: &str, width: usize) -> String {
        text(src, width).join("\n")
    }

    fn text(text: &str, width: usize) -> Vec<String> {
        render(text, width, theme::PLAIN)
            .iter()
            .map(ToString::to_string)
            .collect()
    }

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

    #[test]
    fn unclosed_markup_stays_literal() {
        expect!["half a **thou"].assert_eq(&lines("half a **thou", 40));
        expect!["and `code"].assert_eq(&lines("and `code", 40));
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
        expect!["a *not italic* b"].assert_eq(&lines(r"a \*not italic\* b", 40));
    }

    #[test]
    fn headings_lose_their_hashes_and_gain_the_accent() {
        expect!["Walking skeleton"].assert_eq(&lines("## Walking skeleton", 40));
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
        expect![[r#"
            - one
            - two
            - three"#]]
        .assert_eq(&lines("- one\n* two\n+ three", 40));
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
        expect!["    let a = **b;"].assert_eq(&lines("```\nlet a = **b;\n```", 40));
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
        expect!["--------"].assert_eq(&lines("---", 8));
        assert_eq!(text("--", 8), ["--"], "two dashes is text; three is a rule");
    }

    #[test]
    fn blank_lines_survive_as_paragraph_breaks() {
        expect![[r#"
            one

            two"#]]
        .assert_eq(&lines("one\n\ntwo", 40));
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
        expect![[""]].assert_eq(&lines("", 40));
        assert_eq!(text("", 0), [""], "a zero width is clamped, not divided by");
    }
}
