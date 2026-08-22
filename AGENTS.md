# AGENTS.md — ARC (Autonomous Robotic Core)

Personal AI assistant harness: always-on Rust daemon, thin clients, event-sourced memory, multi-provider LLM support. **`docs/DESIGN.md` is the architectural authority.** If a task conflicts with it, stop and flag the conflict — amend DESIGN.md first, then implement. Do not silently diverge.

## You

Write and talk in plain English, only as much information as the point needs. Lead with the claim, then the evidence. Short sentences over hedged compound ones; no filler ("importantly", "notably", "it is worth noting"); no restating context the reader already has; one statement of each correction, not a narrated history. Reference standard: the plain-language principles of ISO 24495-1 (readers find what they need, understand it on first read, can use it)

## Current phase

Phase 2 — memory (see `docs/DESIGN.md` §11 for the phase contract, `docs/TASKS.md` for the live task list). Do not implement features from later phases, even if convenient. When a task tempts you toward Phase 3+ scope, build the seam, not the feature.

## Workspace

- `arc-proto` — protobuf schemas + prost-generated types. The ONLY place serialized formats are defined. `.proto` files in `arc-proto/proto/`, package `arc.v1`.
- `arc-core` — all logic: event log, projections, providers, memory tools, tracing. Testable without a running daemon.
- `arcd` — daemon binary. Thin composition over arc-core. Owns the log, serves the WebSocket.
- `arc` — TUI client.
- `arc-voice` — voice client (Phase 4; placeholder until then).

New logic goes in `arc-core` unless it is genuinely binary-specific wiring.

## Commands

- `just build` / `just test` / `just fmt` / `just lint` — use these, not raw cargo, so flags stay consistent.
- Run `just fmt` and `just lint` before declaring any task done. Warnings are not acceptable in new code.

## Invariants — never violate

1. **Append-only.** Durable state changes ONLY by appending an `Event` to the log. Never edit or rewrite log bytes. Hand-edits and migrations are events too.
2. **Everything else is a projection.** SQLite index, memory state, session trees must be deterministic replays of the log. Any code that writes projection state outside replay is a bug.
3. **Additive schemas.** Never renumber, remove, or repurpose a proto field. Old events must always decode. Reserve numbers when deprecating.
4. **No vendor SDKs.** Providers are plain HTTP + SSE via reqwest behind the `Provider` trait. Auth is a swappable layer; API keys only for now.
5. **Secrets never touch the log**, backups, traces, or test fixtures.
6. **Memory is tools, not injection.** Nothing enters model context automatically except the identity file and the distilled-record index.
7. **Identity file is human-owned.** Code may propose edits in session output; it never writes `data/identity.md`.

## Conventions

- Rust 2021+, workspace-level deps in root `Cargo.toml`; crates opt in to what they use.
- Errors: `thiserror` for library errors in arc-core, `anyhow` at binary edges.
- Async: tokio throughout; no other runtimes.
- Instrumentation: every LLM call, tool call, memory operation, and consolidation pass gets `tracing` spans (these become Perfetto traces). If you add a subsystem, instrument it in the same change, not later.
- Comments are rare and load-bearing. Default to none; the code says what it does. Write one only when the code cannot:
  - an external rule you can't see from here (an SSE framing quirk, what SQLite's `content=` tables do, fsync ordering);
  - a constant whose consequence lives in another file (bumping this rebuilds the index; 20 digits is what makes names sort);
  - a line that looks wrong and isn't (dropping this sender *is* the kill signal; this empty loop reaps tasks).

  One line above the code, under ten words, plain English. Never restate a name, never head a section, never argue for the change — that belongs in the commit message. Tests, `thiserror` messages, and generated code document themselves.
- Tests live with the code; projection logic must have replay tests (log in → state out, deterministic).
- Runtime state under `data/` (gitignored). Never write outside it at runtime.

## Commit style

Small, single-purpose commits: `<crate>: <imperative summary>` (e.g. `arc-core: add event log segment writer`). Schema changes are their own commit, separate from code that uses them.

## When unsure

Prefer: smaller diff, seam over feature, DESIGN.md over cleverness, asking over assuming. Open questions in `docs/DESIGN.md` §12 are deliberately unresolved — do not resolve them in code.
