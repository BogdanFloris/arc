pub mod continue_job;
pub mod dispatch;
pub mod memory;
pub mod sessions;
pub mod time;

use std::sync::Arc;

use continue_job::ContinueJob;
use dispatch::Dispatch;
use memory::{MemoryRead, MemorySearch, MemorySupersede, MemoryWrite};
use sessions::{SessionRead, SessionsSearch};
use time::GetTime;

use crate::archive::Archive;
use crate::tool::Tool;

/// The builtin source: memory, the archive, the clock, dispatch, and
/// `continue_job`. `projects` names what a job may bind to; `scratch`, if
/// configured, is where `dispatch` sends a job with no natural project.
pub fn tools(
    archive: Arc<Archive>,
    projects: Vec<String>,
    scratch: Option<String>,
) -> Vec<Box<dyn Tool>> {
    vec![
        Box::new(ContinueJob),
        Box::new(Dispatch::new(projects, scratch)),
        Box::new(GetTime),
        Box::new(MemoryRead::new(Arc::clone(&archive))),
        Box::new(MemorySearch::new(Arc::clone(&archive))),
        Box::new(MemorySupersede::new(Arc::clone(&archive))),
        Box::new(MemoryWrite),
        Box::new(SessionRead::new(Arc::clone(&archive))),
        Box::new(SessionsSearch::new(archive)),
    ]
}

#[cfg(test)]
mod tests {
    use crate::testkit::archive_at;
    use crate::tool::ToolSource;
    use tempfile::TempDir;

    #[test]
    fn the_builtin_source_is_the_nine_tools_the_daemon_had() {
        let dir = TempDir::new().expect("temp dir");
        let tools = super::tools(archive_at(&dir), vec!["arc".to_owned()], None);

        let names: Vec<String> = tools.iter().map(|tool| tool.definition().name).collect();
        assert_eq!(
            names,
            [
                "continue_job",
                "dispatch",
                "get_time",
                "memory_read",
                "memory_search",
                "memory_supersede",
                "memory_write",
                "session_read",
                "sessions_search",
            ]
        );
        assert!(
            tools
                .iter()
                .all(|tool| tool.source() == ToolSource::Builtin)
        );
    }
}
