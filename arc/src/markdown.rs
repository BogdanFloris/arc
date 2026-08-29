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
    let mut at = 0;
    while at < lines.len() {
        let raw = lines[at];
        if let Some(info) = fence_info(raw) {
            fenced = match fenced {
                Some(_) => None,
                None if info.trim().is_empty() => {
                    Some((syntax::sniff(block_after(&lines, at)), syntax::Carry::None))
                }
                None => Some((syntax::language(info), syntax::Carry::None)),
            };
            at += 1;
            continue;
        }
        if let Some((language, carry)) = fenced {
            fenced = Some((language, push_code(&mut out, raw, width, language, carry)));
            at += 1;
            continue;
        }
        if let Some(span) = table_span(&lines, at) {
            push_table(&mut out, &lines[at..at + span], width, base);
            at += span;
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
        at += 1;
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

const CELL_GAP: &str = "  ";

const CELL_FLOOR: usize = 3;

/// How many consecutive lines starting at `at` form a pipe table: a header
/// row, a separator row, then any run of rows. `None` if this is not one.
fn table_span(lines: &[&str], at: usize) -> Option<usize> {
    row_cells(lines[at])?;
    if !lines
        .get(at + 1)
        .copied()
        .and_then(row_cells)
        .is_some_and(|cells| {
            cells.iter().all(|cell| {
                let dashes = cell.trim_start_matches(':').trim_end_matches(':');
                !dashes.is_empty() && dashes.chars().all(|c| c == '-')
            })
        })
    {
        return None;
    }
    let rows = lines[at..]
        .iter()
        .take_while(|line| row_cells(line).is_some())
        .count();
    Some(rows)
}

fn row_cells(line: &str) -> Option<Vec<String>> {
    let trimmed = line.trim();
    let body = trimmed.strip_prefix('|')?;
    let body = body.strip_suffix('|').unwrap_or(body);
    Some(body.split('|').map(|cell| cell.trim().to_owned()).collect())
}

/// The pipes themselves are not drawn: columns align on two-space gutters,
/// the header holds bold, and a dash rule per column stands in for the
/// separator row. Cells wrap within their column when the natural widths
/// don't fit; the widest column gives ground first.
fn push_table(out: &mut Vec<Line<'static>>, lines: &[&str], width: usize, base: Style) {
    let rows: Vec<Vec<String>> = lines
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != 1)
        .filter_map(|(_, line)| row_cells(line))
        .collect();
    let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
    if columns == 0 {
        return;
    }

    let styled: Vec<Vec<Vec<(String, Style)>>> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let style = if i == 0 {
                base.patch(theme::STRONG)
            } else {
                base
            };
            (0..columns)
                .map(|col| inline(row.get(col).map_or("", String::as_str), style))
                .collect()
        })
        .collect();

    let mut widths: Vec<usize> = (0..columns)
        .map(|col| {
            styled
                .iter()
                .map(|row| segments_width(&row[col]))
                .max()
                .unwrap_or(0)
        })
        .collect();
    let gaps = CELL_GAP.len() * (columns - 1);
    while widths.iter().sum::<usize>() + gaps > width {
        let widest = (0..columns).max_by_key(|&col| widths[col]).unwrap_or(0);
        if widths[widest] <= CELL_FLOOR {
            break;
        }
        widths[widest] -= 1;
    }

    for (i, row) in styled.iter().enumerate() {
        push_row(out, row, &widths);
        if i == 0 {
            let rule = widths
                .iter()
                .map(|w| "-".repeat(*w))
                .collect::<Vec<_>>()
                .join(CELL_GAP);
            out.push(Line::styled(rule, theme::MARKER));
        }
    }
}

fn push_row(out: &mut Vec<Line<'static>>, row: &[Vec<(String, Style)>], widths: &[usize]) {
    let cells: Vec<Vec<Vec<(String, Style)>>> = row
        .iter()
        .zip(widths)
        .map(|(cell, w)| wrap_cell(cell, *w))
        .collect();
    let height = cells.iter().map(Vec::len).max().unwrap_or(0).max(1);

    for line in 0..height {
        let mut spans: Vec<Span<'static>> = Vec::new();
        for (col, cell) in cells.iter().enumerate() {
            let filled = match cell.get(line) {
                Some(segments) => {
                    let used = segments_width(segments);
                    spans.extend(
                        segments
                            .iter()
                            .map(|(text, style)| Span::styled(text.clone(), *style)),
                    );
                    used
                }
                None => 0,
            };
            if col + 1 < cells.len() {
                let pad = widths[col].saturating_sub(filled) + CELL_GAP.len();
                spans.push(Span::raw(" ".repeat(pad)));
            }
        }
        out.push(Line::from(spans));
    }
}

/// Greedy word wrap of one cell into lines at most `width` wide; a single
/// word wider than the column overflows rather than being cut.
fn wrap_cell(segments: &[(String, Style)], width: usize) -> Vec<Vec<(String, Style)>> {
    let mut lines: Vec<Vec<(String, Style)>> = Vec::new();
    let mut current: Vec<(String, Style)> = Vec::new();
    let mut col = 0;

    for word in split_words(segments.to_vec()) {
        let word_width: usize = word.iter().map(|(text, _)| display_width(text)).sum();
        if !current.is_empty() && col + 1 + word_width > width {
            lines.push(std::mem::take(&mut current));
            col = 0;
        } else if !current.is_empty() {
            current.push((" ".to_owned(), Style::new()));
            col += 1;
        }
        col += word_width;
        current.extend(word);
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}

fn segments_width(segments: &[(String, Style)]) -> usize {
    segments.iter().map(|(text, _)| display_width(text)).sum()
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
        } else if let Some((text, url, len)) = link(rest) {
            flush(&mut out, &mut plain, base);
            out.push((text.to_owned(), base.patch(theme::LINK)));
            out.push((format!(" ({url})"), theme::DIM));
            len
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

/// `[text](url)`: the text underlined, the url dimmed beside it. Nested
/// brackets and urls with spaces stay literal.
fn link(rest: &str) -> Option<(&str, &str, usize)> {
    let after = rest.strip_prefix('[')?;
    let close = after.find(']')?;
    let text = &after[..close];
    let tail = after[close + 1..].strip_prefix('(')?;
    let end = tail.find(')')?;
    let url = &tail[..end];
    (!text.is_empty() && !text.contains('[') && !url.is_empty() && !url.contains(' ')).then_some((
        text,
        url,
        close + end + 4,
    ))
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
    fn a_table_aligns_columns_under_a_bold_header_with_a_rule() {
        expect![[r#"
            Task    Status
            ------  ------
            fork    todo
            rewind  done"#]]
        .assert_eq(&lines(
            "| Task | Status |\n|---|---|\n| fork | todo |\n| rewind | done |",
            40,
        ));
        assert_eq!(
            styles("| Task | Status |\n|---|---|\n| fork | todo |", 40)[0].1,
            theme::PLAIN.patch(theme::STRONG),
            "the header row is bold"
        );
    }

    #[test]
    fn a_wide_table_wraps_cells_within_their_columns() {
        expect![[r#"
            Concept     ARC
            ----------  -----------------
            provenance  record level, not
                        derivation level
            intervals   implicit"#]]
        .assert_eq(&lines(
            "| Concept | ARC |\n|---|---|\n| provenance | record level, not derivation level |\n| intervals | implicit |",
            29,
        ));
    }

    #[test]
    fn table_cells_keep_their_inline_markup() {
        assert!(
            styles("| a | b |\n|---|---|\n| `code` | plain |", 40)
                .iter()
                .any(|(text, style)| text == "code" && *style == theme::CODE),
            "markup inside a cell still styles"
        );
    }

    #[test]
    fn a_pipe_line_without_a_separator_row_stays_text() {
        expect!["| just | text |"].assert_eq(&lines("| just | text |", 40));
        expect![[r#"
            a | b
            more prose"#]]
        .assert_eq(&lines("a | b\nmore prose", 40));
    }

    #[test]
    fn alignment_colons_in_the_separator_are_accepted() {
        expect![[r#"
            a  b
            -  -
            1  2"#]]
        .assert_eq(&lines("| a | b |\n|:--|--:|\n| 1 | 2 |", 40));
    }

    #[test]
    fn a_link_renders_its_text_underlined_with_the_url_dimmed_beside_it() {
        expect!["the post (https://pwning.systems/x) says"]
            .assert_eq(&lines("the [post](https://pwning.systems/x) says", 60));
        let styled = styles("[post](https://a.b)", 60);
        assert_eq!(
            styled[0].1,
            theme::PLAIN.patch(theme::LINK),
            "text underlined"
        );
        assert_eq!(
            styled.last().expect("the url span").1,
            theme::DIM,
            "url dimmed"
        );
    }

    #[test]
    fn a_bracketed_phrase_without_a_url_stays_literal() {
        expect!["see [chapter 4] for details"].assert_eq(&lines("see [chapter 4] for details", 60));
        expect!["[text] (spaced apart)"].assert_eq(&lines("[text] (spaced apart)", 60));
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
