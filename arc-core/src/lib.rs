#![allow(
    clippy::cast_precision_loss,
    clippy::implicit_hasher,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::needless_raw_string_hashes,
    clippy::too_many_lines
)]

pub mod archive;
pub mod client;
pub mod consolidation;
pub mod log;
pub mod memory;
pub mod orphan;
pub mod projection;
pub mod provider;
pub mod session;
pub mod store;
#[cfg(any(test, feature = "testkit"))]
pub mod testkit;
pub mod tool;
pub mod trace;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
