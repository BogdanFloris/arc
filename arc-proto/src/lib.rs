#[allow(clippy::pedantic)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/arc.v1.rs"));
}

#[allow(clippy::pedantic)]
pub mod perfetto {
    include!(concat!(env!("OUT_DIR"), "/perfetto.protos.rs"));
}
