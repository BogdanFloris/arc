pub mod memory;
pub mod sessions;
pub mod time;

use std::sync::Arc;

use memory::{MemoryRead, MemorySearch, MemorySupersede, MemoryWrite};
use sessions::{SessionRead, SessionsSearch};
use time::GetTime;

use crate::archive::Archive;
use crate::tool::Tool;

/// The builtin source: memory, the archive, and the clock.
pub fn tools(archive: Arc<Archive>) -> Vec<Box<dyn Tool>> {
    vec![
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
    fn the_builtin_source_is_the_seven_tools_the_daemon_had() {
        let dir = TempDir::new().expect("temp dir");
        let tools = super::tools(archive_at(&dir));

        let names: Vec<String> = tools.iter().map(|tool| tool.definition().name).collect();
        assert_eq!(
            names,
            [
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
