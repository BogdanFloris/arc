mod layer;
mod writer;

use std::{
    io,
    path::{Path, PathBuf},
};

pub use layer::PerfettoLayer;

pub fn perfetto(dir: &Path, process_name: &str) -> io::Result<(PerfettoLayer, PathBuf)> {
    PerfettoLayer::create(dir, process_name)
}
