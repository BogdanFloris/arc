use std::time::Duration;

use anyhow::{Context as _, Result, anyhow};
use arc_core::consolidation::extract::KNOWN_VERSIONS;
use arc_core::consolidation::replay::{self, ReplayDiff, ReplayRecord, ReplayReport};
use arc_core::secrets::Secrets;
use tracing::info;

use crate::config::Config;
use crate::dirs::DataDirs;
use crate::identity;
use crate::llama::Sidecar;
use crate::roles::Roles;

pub async fn run(
    config: Config,
    dirs: DataDirs,
    prompt: &str,
    against: Option<&str>,
    sessions: &[String],
) -> Result<()> {
    let mut versions = vec![resolve(prompt)?];
    if let Some(against) = against {
        versions.push(resolve(against)?);
    }
    let timeout = Duration::from_secs(config.consolidation.timeout_seconds);
    let identity = identity::load(dirs.identity()).context("loading the identity file")?;

    // replaying extraction is archivist work, so it runs on the archivist's model
    let endpoint = format!("http://127.0.0.1:{}", config.llama.port);
    let roles = Roles::resolve(&config, &endpoint, &Secrets::new(dirs.secrets()), None)?;
    let archivist = roles.archivist();
    // asking the endpoint, not the provider: only the sidecar needs spawning
    let sidecar = match archivist.provider.endpoint() == endpoint {
        true if probe(&endpoint).await => {
            info!(%endpoint, "using the already-running llama endpoint");
            None
        }
        true => Some(Sidecar::start(&config.llama, &archivist.model).await?),
        false => None,
    };

    let outcome = replay::run(
        &archivist.provider,
        &archivist.model,
        timeout,
        dirs.log(),
        &versions,
        sessions,
        identity.as_deref(),
    )
    .await;
    if let Some(sidecar) = sidecar {
        sidecar.stop().await;
    }
    let reports = outcome.context("memory replay")?;

    let mut lines = Vec::new();
    for report in &reports {
        lines.extend(report_lines(report));
    }
    if let [a, b] = reports.as_slice() {
        lines.extend(diff_lines(a, b, &replay::diff(a, b)));
    }
    println!("{}", lines.join("\n"));
    Ok(())
}

fn resolve(version: &str) -> Result<(&'static str, &'static str)> {
    KNOWN_VERSIONS
        .iter()
        .find(|(known, _)| *known == version)
        .copied()
        .ok_or_else(|| {
            let known: Vec<&str> = KNOWN_VERSIONS.iter().map(|(known, _)| *known).collect();
            anyhow!(
                "unknown prompt version {version:?} — known versions: {}",
                known.join(", ")
            )
        })
}

async fn probe(endpoint: &str) -> bool {
    let Ok(http) = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
    else {
        return false;
    };
    match http.get(format!("{endpoint}/health")).send().await {
        Ok(response) => response.status().is_success(),
        Err(_) => false,
    }
}

fn report_lines(report: &ReplayReport) -> Vec<String> {
    let mut lines = vec![
        format!("== prompt {} ==", report.version),
        format!("sessions processed: {}", report.sessions.len()),
    ];
    for session in &report.sessions {
        if session.operations.is_empty() {
            continue;
        }
        lines.push(format!("{}:", session.session_id));
        for op in &session.operations {
            lines.push(format!(
                "  {} {} {:?} \u{2014} {}",
                op.op, op.kind, op.title, op.summary
            ));
        }
    }
    lines.push(format!("final state: {} records", report.final_state.len()));
    for record in &report.final_state {
        lines.push(format!("  {}", record_line(record)));
    }
    lines.push(String::new());
    lines
}

fn diff_lines(a: &ReplayReport, b: &ReplayReport, diff: &ReplayDiff) -> Vec<String> {
    let mut lines = vec![
        format!("== diff {} -> {} ==", a.version, b.version),
        "keyed on content (kind + title, case-insensitive); minted ids differ between runs by construction".to_owned(),
        format!(
            "records: {} {}, {} {}",
            a.version,
            a.final_state.len(),
            b.version,
            b.final_state.len()
        ),
    ];
    section(
        &mut lines,
        &format!("only in {}", a.version),
        &diff.only_in_a,
    );
    section(
        &mut lines,
        &format!("only in {}", b.version),
        &diff.only_in_b,
    );
    if diff.changed.is_empty() {
        lines.push("changed summaries: none".to_owned());
    } else {
        lines.push("changed summaries:".to_owned());
        for change in &diff.changed {
            lines.push(format!("  {} {:?}:", change.kind, change.title));
            lines.push(format!("    {}: {}", a.version, change.summary_a));
            lines.push(format!("    {}: {}", b.version, change.summary_b));
        }
    }
    lines
}

fn section(lines: &mut Vec<String>, label: &str, records: &[ReplayRecord]) {
    if records.is_empty() {
        lines.push(format!("{label}: none"));
        return;
    }
    lines.push(format!("{label}:"));
    for record in records {
        lines.push(format!("  {}", record_line(record)));
    }
}

fn record_line(record: &ReplayRecord) -> String {
    format!(
        "{}/{}: {} \u{2014} {}",
        record.kind, record.namespace, record.title, record.summary
    )
}

#[cfg(test)]
mod tests {
    use arc_core::consolidation::extract::PROMPT_V1;
    use arc_core::consolidation::replay::{
        ReplayOperation, ReplayRecord, ReplayReport, SessionReplay, diff,
    };

    use expect_test::expect;

    use super::{diff_lines, report_lines, resolve};

    #[test]
    fn v1_resolves_to_its_pinned_prompt() {
        assert_eq!(resolve("v1").expect("known"), ("v1", PROMPT_V1));
    }

    #[test]
    fn an_unknown_version_errors_listing_the_known_ones() {
        let err = resolve("v9").expect_err("v9 is not a version");
        let text = err.to_string();
        assert!(text.contains("\"v9\""), "{text}");
        assert!(text.contains("known versions: v1"), "{text}");
    }

    fn record(kind: &str, title: &str, summary: &str) -> ReplayRecord {
        ReplayRecord {
            kind: kind.to_owned(),
            namespace: "global".to_owned(),
            title: title.to_owned(),
            summary: summary.to_owned(),
        }
    }

    fn reports() -> (ReplayReport, ReplayReport) {
        let a = ReplayReport {
            version: "v1".to_owned(),
            sessions: vec![
                SessionReplay {
                    session_id: "s-1".to_owned(),
                    operations: vec![ReplayOperation {
                        op: "write",
                        kind: "fact".to_owned(),
                        title: "User name".to_owned(),
                        summary: "named Bogdan".to_owned(),
                    }],
                },
                SessionReplay {
                    session_id: "s-2".to_owned(),
                    operations: Vec::new(),
                },
            ],
            final_state: vec![record("fact", "User name", "named Bogdan")],
        };
        let b = ReplayReport {
            version: "v2".to_owned(),
            sessions: Vec::new(),
            final_state: vec![
                record("fact", "User name", "goes by Bogdan"),
                record("preference", "Storytelling", "likes big stories"),
            ],
        };
        (a, b)
    }

    #[test]
    fn a_report_renders_counts_operations_and_final_state() {
        let (a, _) = reports();
        expect![[r#"
            == prompt v1 ==
            sessions processed: 2
            s-1:
              write fact "User name" — named Bogdan
            final state: 1 records
              fact/global: User name — named Bogdan
        "#]]
        .assert_eq(&report_lines(&a).join("\n"));
    }

    #[test]
    fn a_diff_renders_counts_exclusives_and_changed_summaries() {
        let (a, b) = reports();
        let rendered = diff_lines(&a, &b, &diff(&a, &b)).join("\n");
        expect![[r#"
            == diff v1 -> v2 ==
            keyed on content (kind + title, case-insensitive); minted ids differ between runs by construction
            records: v1 1, v2 2
            only in v1: none
            only in v2:
              preference/global: Storytelling — likes big stories
            changed summaries:
              fact "User name":
                v1: named Bogdan
                v2: goes by Bogdan"#]].assert_eq(&rendered);
    }
}
