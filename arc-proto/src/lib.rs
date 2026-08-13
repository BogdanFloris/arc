// Generated code is exempt from pedantic — we don't control its style.
#[allow(clippy::pedantic)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/arc.v1.rs"));
}

/// Perfetto's trace schema, or the subset of it ARC writes. Third-party
/// field numbers — see `proto/perfetto.proto` for what that changes.
#[allow(clippy::pedantic)]
pub mod perfetto {
    include!(concat!(env!("OUT_DIR"), "/perfetto.protos.rs"));
}
