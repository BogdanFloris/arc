use anyhow::{Context as _, Result, anyhow};
use arc_core::projection;

use crate::dirs::DataDirs;

pub fn run(dirs: &DataDirs) -> Result<()> {
    let report = projection::rebuild(dirs.log(), dirs.index())
        .with_context(|| format!("rebuilding against {}", dirs.index().display()))?;

    println!(
        "schema version: live {}, replayed {}",
        version_text(report.schema_version_live),
        version_text(report.schema_version_replayed),
    );
    for table in &report.tables {
        match &table.divergence {
            None => println!("{}: {} rows match", table.table, table.rows_live),
            Some(divergence) => {
                println!("{} diverges at {}:", table.table, divergence.key);
                println!("  live:     {}", row_text(divergence.live.as_deref()));
                println!("  replayed: {}", row_text(divergence.replayed.as_deref()));
            }
        }
    }

    if report.is_clean() {
        println!("rebuild proof: the log reproduces live state");
        Ok(())
    } else {
        Err(anyhow!(
            "rebuild found a divergence between the log and the live index"
        ))
    }
}

fn version_text(version: Option<u32>) -> String {
    version.map_or_else(|| "none".to_owned(), |v| v.to_string())
}

fn row_text(row: Option<&[String]>) -> String {
    match row {
        Some(row) => format!("[{}]", row.join(", ")),
        None => "missing".to_owned(),
    }
}
