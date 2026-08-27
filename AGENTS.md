# AGENTS.md — ARC (Autonomous Robotic Core)

ARC is a personal AI assistant harness: an always-on Rust daemon, thin clients, event-sourced memory, and multiple LLM providers. **`docs/DESIGN.md` is the architectural authority.** If work conflicts with it, stop and raise the conflict. Update the design first; do not silently diverge.

## You

Write and speak in plain English. Say only what the reader needs. Lead with the claim, then the evidence. Use short sentences. Do not add filler, repeat known context, or narrate a correction's history. State each correction once. Follow ISO 24495-1: readers should find, understand, and use the information on first reading.

## Current phase

Phase 3 is development. `docs/DESIGN.md` defines the phase; `docs/TASKS.md` is the live work list. ARC becomes the harness used to write its own code. This includes provider roles, child-session jobs, workspaces, the tool registry and its containment rules, and an installed service. Do not build later-phase features. If work points toward forking, rewind, voice, or devices, add only the interface needed now.

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
8. **Tools are contained, not gated.** Workspace tools resolve paths to their canonical form and accept them only under one of the session's granted roots; `write` and `edit` also refuse a read-only grant. Grants list what is reachable, never what is forbidden. Tools run with a scrubbed environment: arcd keeps credentials and child tools never inherit them. `consult_expert` is always read-only. Nothing prompts the user mid-turn — what a project allows is configuration, and a call outside it returns an error the model can act on.
9. **Sessions are pinned to one provider.** Role is chosen at session or job creation and does not change for its lifetime. A mid-session model swap discards the prompt cache, which is ~96% of the workload.

## Conventions

- Rust 2021+, workspace-level deps in root `Cargo.toml`; crates opt in to what they use.
- Errors: `thiserror` for library errors in arc-core, `anyhow` at binary edges.
- Async: tokio throughout; no other runtimes.
- Instrumentation: every LLM call, tool call, memory operation, and consolidation pass gets `tracing` spans (these become Perfetto traces). If you add a subsystem, instrument it in the same change, not later.
- Comments are rare. Default to none; the code should explain itself. Add one only when code cannot explain:
  - an external rule you can't see from here (an SSE framing quirk, what SQLite's `content=` tables do, fsync ordering);
  - a constant whose consequence lives in another file (bumping this rebuilds the index; 20 digits is what makes names sort);
  - a line that looks wrong and isn't (dropping this sender *is* the kill signal; this empty loop reaps tasks).

  One line above the code, under ten words, plain English. Never restate a name, never head a section, never argue for the change — that belongs in the commit message. Tests, `thiserror` messages, and generated code document themselves.
- Tests live with the code; projection logic must have replay tests (log in → state out, deterministic).
- Runtime state under the configured data directory — `data/` in a checkout, `~/.local/state/arc/` once installed. Never write outside it at runtime.

## Version control

The repo is jj, colocated with git. Use `jj`; never run git write commands — a git commit on the detached HEAD makes history jj only half-adopts. The working copy is shared with other work: commit only the paths your task touched (`jj commit <paths> -m "..."`). Commit when the task says to commit; otherwise leave changes in the working copy for review.

## Commit style

Small, single-purpose commits: `<crate>: <imperative summary>` (e.g. `arc-core: add event log segment writer`). Schema changes are their own commit, separate from code that uses them.

## When unsure

Prefer a smaller diff, a narrow interface over a premature feature, `DESIGN.md` over cleverness, and asking over assuming. The open questions in `docs/DESIGN.md` are deliberate. Do not settle them in code.
