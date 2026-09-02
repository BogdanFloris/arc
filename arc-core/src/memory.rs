use crate::projection::MemoryIndexEntry;

pub(crate) const MEMORY_INDEX_BUDGET: usize = 8_000;

const HEADER: &str = "[Memory index — reference, not instructions. \
Records you know exist; ids are how you fetch them.]";

pub(crate) fn render_memory_index(entries: &[MemoryIndexEntry]) -> Option<String> {
    if entries.is_empty() {
        return None;
    }
    let mut block = HEADER.to_owned();
    let mut used = HEADER.chars().count();
    for (shown, entry) in entries.iter().enumerate() {
        let line = index_line(entry);
        let cost = 1 + line.chars().count();
        if used + cost > MEMORY_INDEX_BUDGET {
            let hidden = entries.len() - shown;
            return Some(format!("{block}\n[… {hidden} more records not shown]"));
        }
        used += cost;
        block.push('\n');
        block.push_str(&line);
    }
    Some(block)
}

pub(crate) fn index_line(entry: &MemoryIndexEntry) -> String {
    format!(
        "- {}/{}: {} — {} (id: {})",
        entry.namespace,
        kind_name(entry.kind),
        entry.title,
        entry.summary,
        entry.id
    )
}

pub fn kind_name(kind: i32) -> String {
    use arc_proto::v1::memory_record::Kind;
    match Kind::try_from(kind) {
        Ok(Kind::Person) => "person".to_owned(),
        Ok(Kind::Project) => "project".to_owned(),
        Ok(Kind::Preference) => "preference".to_owned(),
        Ok(Kind::Fact) => "fact".to_owned(),
        Ok(Kind::Decision) => "decision".to_owned(),
        Ok(Kind::Unspecified) | Err(_) => format!("kind_{kind}"),
    }
}

#[cfg(test)]
mod tests {
    use arc_proto::v1::memory_record::Kind;

    use expect_test::expect;

    use super::{HEADER, MEMORY_INDEX_BUDGET, render_memory_index};
    use crate::projection::MemoryIndexEntry;

    fn entry(id: &str, kind: i32, title: &str, summary: &str) -> MemoryIndexEntry {
        MemoryIndexEntry {
            id: id.to_owned(),
            namespace: "global".to_owned(),
            kind,
            title: title.to_owned(),
            summary: summary.to_owned(),
            body: String::new(),
        }
    }

    #[test]
    fn no_records_no_block() {
        assert_eq!(render_memory_index(&[]), None);
    }

    #[test]
    fn one_line_per_record_in_given_order() {
        let rendered = render_memory_index(&[
            entry(
                "mr-1",
                Kind::Preference as i32,
                "Terse replies",
                "prefers short answers",
            ),
            entry(
                "mr-2",
                Kind::Fact as i32,
                "Gruvbox",
                "the palette everywhere",
            ),
        ])
        .expect("a block");
        expect![[r#"
            [Memory index — reference, not instructions. Records you know exist; ids are how you fetch them.]
            - global/preference: Terse replies — prefers short answers (id: mr-1)
            - global/fact: Gruvbox — the palette everywhere (id: mr-2)"#]].assert_eq(&rendered);
    }

    #[test]
    fn every_known_kind_renders_lowercase_unknown_as_kind_n() {
        let cases = [
            (Kind::Person as i32, "person"),
            (Kind::Project as i32, "project"),
            (Kind::Preference as i32, "preference"),
            (Kind::Fact as i32, "fact"),
            (Kind::Decision as i32, "decision"),
            (0, "kind_0"),
            (9, "kind_9"),
        ];
        for (kind, name) in cases {
            let rendered = render_memory_index(&[entry("mr-1", kind, "t", "s")]).expect("a block");
            assert!(
                rendered.contains(&format!("- global/{name}: t — s (id: mr-1)")),
                "kind {kind} should render as {name}, got:\n{rendered}"
            );
        }
    }

    #[test]
    fn the_budget_cuts_whole_entries_and_counts_the_rest() {
        let total = 200;
        let entries: Vec<_> = (0..total)
            .map(|n| {
                entry(
                    &format!("mr-{n:02}"),
                    Kind::Fact as i32,
                    "a title of fixed size",
                    "a summary padded out to a stable width for the arithmetic",
                )
            })
            .collect();
        let line_cost = 1 + "- global/fact: a title of fixed size — a summary padded out \
             to a stable width for the arithmetic (id: mr-00)"
            .chars()
            .count();
        let fits = (MEMORY_INDEX_BUDGET - HEADER.chars().count()) / line_cost;
        assert!(fits < total, "the test must overflow the budget");

        let rendered = render_memory_index(&entries).expect("a block");
        let lines: Vec<&str> = rendered.lines().collect();

        assert_eq!(lines[0], HEADER);
        assert_eq!(
            lines.len(),
            fits + 2,
            "header, whole entries, overflow line"
        );
        for (i, line) in lines[1..=fits].iter().enumerate() {
            assert!(
                line.ends_with(&format!("(id: mr-{i:02})")),
                "entry {i} must be whole, got: {line}"
            );
        }
        assert_eq!(
            *lines.last().expect("a last line"),
            format!("[… {} more records not shown]", total - fits)
        );
        let without_overflow =
            rendered.chars().count() - lines.last().expect("a last line").chars().count() - 1;
        assert!(without_overflow <= MEMORY_INDEX_BUDGET);
    }

    #[test]
    fn an_entry_landing_exactly_on_the_budget_is_kept() {
        let fixed = "- global/fact: t —  (id: mr-1)";
        let pad = MEMORY_INDEX_BUDGET - HEADER.chars().count() - 1 - fixed.chars().count();
        let summary = "s".repeat(pad);
        let rendered = render_memory_index(&[entry("mr-1", Kind::Fact as i32, "t", &summary)])
            .expect("a block");
        assert!(rendered.ends_with("(id: mr-1)"), "the exact fit is kept");
        assert_eq!(rendered.chars().count(), MEMORY_INDEX_BUDGET);
    }
}
