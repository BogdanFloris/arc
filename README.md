# ARC (Autonomous Robotic Core)

A personal AI assistant built around an always-on Rust daemon (`arcd`). Thin clients connect over a local socket: a TUI today and voice later. ARC has durable event-sourced memory, a stable identity, branching sessions, and swappable providers. It starts with a local llama.cpp model.

![The arc TUI](docs/arc-tui.png)

This is one person's daily driver, built in the open. There are no releases, stability promises, or multi-user plan. [docs/DESIGN.md](docs/DESIGN.md) is the architectural authority. [docs/TASKS.md](docs/TASKS.md) is the current work list.

Priorities, in order: **durability** (one append-only log; everything else rebuildable), **observability** (Perfetto traces cover every LLM call, tool call, and memory write), **speed** (no GC; protobuf on disk and on the wire), and **independence** (one provider trait over plain HTTP and SSE; no vendor SDKs).

**Status:** Phases 1 (walking skeleton) and 2 (memory and tool calling) are complete and used daily. Phase 3 makes ARC the harness used to write its own code. [DESIGN.md](docs/DESIGN.md) describes every phase.

## Running it

Install a Rust toolchain, `protoc`, and llama.cpp's `llama-server` on your `PATH`. The Nix flake (`nix develop`) provides everything except the sidecar.

```sh
just model          # download the default GGUF (Qwen3-8B Q4_K_M, ~5GB) to data/models/
just build
cargo run -p arcd   # daemon: owns the log, supervises the sidecar, binds 127.0.0.1:8787
cargo run -p arc    # TUI, in another terminal
```

Configuration is in `data/arc.toml`; every field is optional. See `arcd/src/config.rs` for field documentation. Runtime state lives under `data/`: the log, SQLite projection, `identity.md`, and traces. `just install-service` installs a systemd user unit.

## Layout

| Crate | What it is |
|---|---|
| `arc-proto` | protobuf schemas + prost types. The only place serialized formats are defined |
| `arc-core` | all logic: event log, projections, providers, tracing. Testable without a daemon |
| `arcd` | the daemon. Owns the log, serves the WebSocket |
| `arc` | the TUI client |
| `arc-voice` | voice client, Phase 4 placeholder |

`just build` / `test` / `fmt` / `lint`.

## Traces

Each `arcd` run writes a Perfetto trace to `data/traces/arc-<unix-seconds>.pftrace` and reports its path in the first log line. Drag it into <https://ui.perfetto.dev>. The browser parses the file locally; nothing is uploaded. The trace shows one row per session, each turn's span tree (`session.send_message` → `openai.complete`), and input/output token counters.

The development shell also provides `trace_processor_shell`:

```sh
nix develop --command trace_processor_shell data/traces/arc-*.pftrace
> select name, dur/1e6 as ms from slice order by dur desc limit 10;
> select name, value from stats where severity = 'error' and value > 0;   -- should be empty
```

Packets are flushed as they are written, so you can open a trace while its daemon is still running. `RUST_LOG` filters both the trace and stderr log. Traces are disposable and excluded from backups.

## License

MIT.
