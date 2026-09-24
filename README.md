# ARC (Autonomous Robotic Core)

A personal assistant built around an always-on Rust daemon, a TUI, branching conversations, durable memory, and replaceable providers. Development happens directly in a project-bound session; independent work runs as jobs.

![The arc TUI](docs/arc-tui.png)

One person's daily driver, built in the open. **Current phase: 3.7, the direct door.** No releases, stability promises, or multi-user plan.

- [Design](docs/DESIGN.md) — architectural authority and crate layout.
- [Tasks](docs/TASKS.md) — live work and acceptance checks.
- [Providers](docs/providers.md) — configuration and dated measurements.
- [Testing](docs/testing.md) — focused runs, logs, rendered frames.

## Build and run

The Nix development shell supplies Rust, `protoc`, and build tools. Local inference additionally needs `llama-server` and a GGUF; hosted-only configuration starts no sidecar.

```sh
nix develop
just build
# Configure data/arc.toml before starting:
target/debug/arcd run --config data/arc.toml
target/debug/arc  # another terminal
```

Without `--config`, arcd prefers `~/.config/arc/arc.toml`, then `data/arc.toml`. Missing configuration uses local defaults. For local inference, set `llama.model_file` to your GGUF; `just model` downloads the default under `~/.local/state/arc/models/`, not the checkout.

Runtime state follows `data_dir`: checkout default `data/`, installed convention `~/.local/state/arc/`. `just install` builds release binaries, installs the user service, and restarts it. `just install-service` only installs the unit.

Checks: `just test`, `just fmt`, `just lint`.

## Traces

The daemon reports its Perfetto trace path at startup, under the data directory's `traces/`. Open it in Perfetto or query it from the development shell:

```sh
trace_processor_shell path/to/arc.pftrace
> select name, dur/1e6 as ms from slice order by dur desc limit 10;
```

Spans cover model calls, tools, memory, and jobs. `RUST_LOG` filters trace and stderr output. Traces are disposable and excluded from backups.

## License

MIT.
