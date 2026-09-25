use std::path::Path;

use tracing::warn;

use crate::identity;

pub(super) fn project_context(
    name: &str,
    description: &str,
    root: &Path,
    identity_text: Option<&str>,
) -> String {
    let mut parts = Vec::new();
    if let Some(identity) = identity_text {
        parts.push(identity.trim_end().to_owned());
    }
    parts.push(format!(
        "Project: {name}\nDescription: {description}\nRoot: {}",
        root.display()
    ));
    add_agents(&mut parts, root);
    parts.join("\n\n")
}

pub(super) fn directory_context(root: &Path, identity_text: Option<&str>) -> String {
    let mut parts = Vec::new();
    if let Some(identity) = identity_text {
        parts.push(identity.trim_end().to_owned());
    }
    parts.push(format!("Working directory: {}", root.display()));
    add_agents(&mut parts, root);
    parts.join("\n\n")
}

fn add_agents(parts: &mut Vec<String>, root: &Path) {
    match identity::load(&root.join("AGENTS.md")) {
        Ok(Some(agents)) => parts.push(agents),
        Ok(None) => {}
        Err(error) => warn!(root = %root.display(), %error, "could not read AGENTS.md"),
    }
}

#[cfg(test)]
mod tests {
    use super::project_context;

    #[test]
    fn the_project_and_agents_file_add_context_without_a_coding_preamble() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "Use jj.\n").unwrap();
        let prompt = project_context("arc", "A Rust harness", dir.path(), Some("You are ARC."));
        assert!(prompt.starts_with("You are ARC.\n\nProject: arc\nDescription: A Rust harness"));
        assert!(prompt.trim_end().ends_with("\n\nUse jj."));
        assert!(!prompt.contains("coding agent"));
    }
}
