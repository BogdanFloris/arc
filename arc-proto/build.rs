use std::{env, fs, path::PathBuf};

fn main() -> std::io::Result<()> {
    let protos = [
        "proto/events.proto",
        "proto/memory.proto",
        "proto/wire.proto",
        "proto/perfetto.proto",
    ];

    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    prost_build::compile_protos(&protos, &["proto"])?;

    let generated = PathBuf::from(env::var("OUT_DIR").unwrap()).join("arc.v1.rs");
    if !generated.exists() {
        fs::write(&generated, "")?;
    }

    Ok(())
}
