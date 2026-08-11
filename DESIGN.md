# ARC — Autonomous Robotic Core

## Design Document

**Status:** v1, governs initial implementation. Amend this file before diverging from it.

---

## 1. What ARC is

ARC is a personal AI assistant built as an always-on Rust daemon (`arcd`) with thin clients (TUI, voice daemon, mobile app) connected over a WebSocket. It has durable memory, a stable identity, pi-style branching sessions, and multi-provider LLM support with no vendor lock-in. Long term, it is the control plane for everything the user wants to interact with: the underlying os, cloud tools, robotics workbench: physical devices are exposed to ARC as MCP servers, starting with small actuators and eventually a robotic arm.

Design priorities, in order:

1. **Durability.** Memory and history survive machine changes, provider changes, and schema evolution. One source of truth, everything else rebuildable.
2. **Observability.** Every LLM call, tool call, and memory operation is traceable. ARC emits Perfetto traces of its own behavior.
3. **Speed.** Systems-level engineering: no GC, small idle footprint, protobuf on disk and on the wire.
4. **Independence.** Providers, auth methods, and models are swappable behind one interface. Local models are a planned path, not an afterthought.

Non-goals for v1: multi-user support, cloud hosting, plugin sandboxing, and any robotics code. The architecture leaves room for these; the code does not attempt them.

## 2. Repository layout

Cargo workspace with five crates:

- `arc-proto` — all protobuf schemas (`events.proto`, `memory.proto`, `wire.proto`, package `arc.v1`) and generated types via prost. The single schema authority: the event log on disk and the WebSocket wire protocol use the same generated types. No other crate defines serialized formats.
- `arc-core` — shared library: event log reader/writer, projections (SQLite index, memory state), provider abstraction, memory tools, tracing integration. All logic lives here so the daemon stays a thin composition layer and logic is testable without a running daemon.
- `arcd` — the daemon binary. Owns the event log, runs projections, serves the WebSocket, holds provider credentials, runs the consolidation pipeline.
- `arc` — the TUI client.
- `arc-voice` — the wake-word voice client.

A future `arc-mobile` app consumes the same wire protocol; it is a separate (non-Rust or Rust-core + native shell) project and only depends on `arc-proto` schemas.

## 3. The event log

The event log is the single source of truth for everything durable except the identity file. It is an append-only sequence of length-prefixed protobuf messages on disk under `data/log/`, segmented into files by size for backup friendliness.

```proto
message Event {
  uint64 seq = 1;            // monotonic, gapless
  google.protobuf.Timestamp ts = 2;
  Source source = 3;         // MODEL, USER, SYSTEM
  oneof payload {
    SessionEvent session = 10;    // message appended, session created, branch forked, tool call + result
    MemoryEvent memory = 11;      // record created / updated / superseded / deleted
    IdentityEvent identity = 12;  // reserved; see §5.1
  }
}
```

Rules that make the model work:

1. **Nothing mutates durable state except by appending an event.** Model-initiated memory writes, user hand-edits, and system migrations all append; they never edit prior bytes.
2. **All state is a projection.** The SQLite index, the current distilled-memory state, and session trees are all deterministic replays of the log. Any projection can be deleted and rebuilt at any time.
3. **Schema evolution is additive.** Fields are never renumbered or repurposed; old events must always decode.

Consequences: backup is "back up the log segments" (rustic handles append-only files efficiently), transfer to a new machine is "copy log + identity file, replay," and there is no live-database backup problem because SQLite is disposable.

## 4. Sessions

Sessions are pi-style: a tree, not a list. Forking a session at any message creates a child session with a `parent_session` and `fork_point`; the tree structure is just parent pointers in the projection. Clients render the tree; the daemon only stores it.

Branch semantics interact with memory (see §5.4): only the main line and branches explicitly marked *real* feed the consolidation pipeline. Abandoned experimental branches remain searchable in the archive but never write distilled memory.

## 5. Memory

"Feels alive" decomposes into three subsystems with different storage, different update rules, and different retrieval paths. They are deliberately not unified.

### 5.1 Identity

One small human-owned file: `data/identity.md`. Who ARC is, how it talks, stable facts about its user. Loaded into context unconditionally, every session. Budget: a few KB — small enough that loading it is never a decision.

Identity is exempt from event-sourcing. It is a plain file, versioned in git, backed up by rustic alongside the log. ARC may propose edits (as ordinary session output); accepted edits are applied by the user editing the file. `IdentityEvent` is reserved in the schema in case this policy changes, but v1 does not emit it.

### 5.2 Distilled tier

A flat set of structured records ARC writes deliberately. Flat, not hierarchical: namespaces and links provide organization; a folder tree does not.

```proto
message MemoryRecord {
  string id = 1;
  Kind kind = 2;              // PERSON, PROJECT, PREFERENCE, FACT, DECISION
  string namespace = 3;       // "global" or a project id (per-project memory)
  string title = 4;
  string summary = 5;         // one line; appears in the always-loaded index
  string body = 6;            // markdown, freeform
  repeated string links = 7;  // related record ids
  Provenance provenance = 8;  // session ids + timestamps where learned
  Status status = 9;          // ACTIVE, SUPERSEDED — never hard-delete
}
```

Current record state is a projection of `MemoryEvent`s. The always-loaded context contains an index of ACTIVE records — `namespace + kind + title + summary` only — so the model always knows what exists without loading bodies. Superseding rather than deleting preserves history ("you used to live at X") and keeps replay honest; a user-requested purge is a `DELETED` event, and the projection then excludes the record entirely.

Provenance is load-bearing: every fact can answer "where did you learn that" by pointing into the archive.

### 5.3 Archive tier

The raw session tree, fully indexed, derived from the log into SQLite (`data/index.db`, rusqlite, bundled):

- `sessions(id, parent_session, fork_point, project, title, started_at)`
- `messages(session_id, seq, role, content, ts)` + FTS5 index over `content`
- later: `chunks(session_id, span, embedding)` via sqlite-vec — only added if FTS proves insufficient in practice, and addable at any time by re-projecting the log

The distilled tier answers "what do you know about X"; the archive answers "what did we say about X in March." Both are needed; neither substitutes for the other.

### 5.4 Write pipeline (consolidation)
 
Storage is the easy half; deciding what gets remembered is the hard half. v1 keeps it embarrassingly simple:
 
1. **Explicit:** "remember this" → the model calls `memory_write` immediately.
2. **End-of-session extraction:** when a session (main line or *real*-marked branch) goes idle, a cheap model pass extracts durable facts, merges them with existing records, and resolves contradictions by superseding. Runs async on the daemon.
Every consolidation decision emits Perfetto spans, so tuning is done against traces of real behavior, not guesses. Expected failure modes to watch for: hoarding noise (too eager) and remembering nothing useful (too strict). Rules get complexity only after real usage shows where these bite.
 
**Tuning loop.** Because sessions live in the append-only log, consolidation is re-runnable — this is the primary tuning mechanism:
 
- The consolidation prompt is versioned. `arcd memory-replay` re-runs a given prompt version over historical sessions and diffs the resulting memory states against another version. This is the regression suite for memory: every prompt change is evaluated against real history before it goes live.
- A weekly review in the TUI shows records created and superseded that week; each is accepted, fixed, or deleted. Reviews are the ground truth.
- Corrections feed back as few-shot examples in the consolidation prompt — fixed records are exactly the examples that encode the user's standards.
- Three metrics tracked in traces: records created per session (hoarding detector), supersede rate (contradiction handling), and retrieval hit rate — how often a `memory_search` result is actually used in a response. The last is the honest measure of whether memory earns its tokens.
- Once enough reviews accumulate, a hand-labeled sample of sessions ("what should have been remembered") yields precision/recall on real usage.

### 5.5 Retrieval

Memory access is tools, not silent RAG injection. The model calls:

- `memory_read(id)` — fetch a record body
- `memory_search(query, namespace?)` — search distilled records
- `memory_write(record)` / `memory_supersede(id, record)` — emit MemoryEvents
- `sessions_search(query, project?)` — FTS over the archive, returns snippets + session ids
- `session_read(id, range)` — pull actual past context

One pattern throughout: search cheap, read targeted. Lookups are visible in traces and debuggable like any other tool call. Nothing is injected into context automatically except the identity file and the distilled-record index.

## 6. Providers

One trait in `arc-core`:

```rust
trait Provider {
    fn complete(&self, req: CompletionRequest) -> impl Stream<Item = CompletionDelta>;
    // model listing, token counting, capability flags
}
```

v1 ships Anthropic (Claude) and Google (Gemini) implementations over plain HTTP + SSE (`reqwest` + rustls); no vendor SDKs. Consumer-subscription OAuth is implemented only if and when the providers' terms of service permit third-party harness use — the auth abstraction exists precisely so this is a config change, not a redesign. Should be preferred for API tokens for cost reasons. Local models (an OpenAI-compatible endpoint pointed at llama.cpp/vLLM) are a planned third implementation.

Provider choice is per-session with a global default. Tool-calling and system-prompt differences are normalized in `arc-core`, not leaked to clients. They should be hot-swappable even by a voice request in the future and even by the agent if for example it decides to use a cheap model to do something.

## 7. Wire protocol and clients

Protobuf over WebSocket (`wire.proto`), served by `arcd` on localhost. Remote access (mobile) is Tailscale reaching the same socket; ARC does not implement its own tunnel, TLS termination, or auth beyond a local token in v1.

The protocol is client-agnostic: subscribe to a session, send a message, fork, receive streamed deltas and tool-call events, query the session tree. Clients hold no durable state.

- `arc` (TUI): first client, exercises everything — session tree navigation, streaming, memory tool visibility. Should connect over UDS for fast communication if local, websocket if not
- `arc-voice`: wake word via openWakeWord or Porcupine, local ASR via whisper.cpp/faster-whisper, TTS local, or gemini voice api, decide later. It is a thin client: audio in, text over the socket, audio out. No model logic.
- Mobile: same protocol over Tailscale. Last client, after the protocol has stabilized under two others.

## 8. Observability: Perfetto

ARC emits Perfetto protobuf traces of its own operation: a track per session (and per branch), spans for LLM calls (with token counts as counters), tool calls, memory reads/writes, consolidation passes, and log replays. Traces are written to `data/traces/` and open directly in the Perfetto UI.

This is not decoration. It is the debugging surface for the two genuinely hard subsystems — consolidation quality and retrieval behavior — and it makes latency and token spend measurable from day one. Implementation: `tracing` subscriber in `arc-core` that renders to Perfetto's `TracePacket` protos.

## 9. Robotics (future)

Physical devices integrate as MCP servers, never as bespoke daemon code: an ESP32 pan-tilt rig, later an SO-101-class arm, eventually larger. Two constraints are fixed now:

1. The model plans and issues high-level actions only; firmware enforces joint limits, speeds, and e-stop. The LLM never commands motors directly.
2. Device MCP servers are separate processes with their own lifecycle; `arcd` treats them like any other tool source.

No robotics code exists in this repo until Phase 5.

## 10. Security and backup

- Runtime state lives under `data/` (log, index, identity, traces).
- Backup: rustic, encrypted at the repository level, covering `data/log/`, `data/identity.md`, and snapshots. `data/index.db` and `data/traces/` are excluded — both are rebuildable/disposable.
- Provider credentials live in the OS keychain or an encrypted secrets file, never in the log and never in backups.
- The WebSocket binds localhost only; remote access is Tailscale's problem, by design.

## 11. Phases

Each phase ends in something used daily. No phase begins until the previous one is a daily driver, because real usage is the input to the next phase's design decisions (especially memory).

**Phase 0 — Scaffold.** *Done.* Workspace, empty crates, empty schemas, build/test/fmt/lint targets.

**Phase 1 — Walking skeleton.** `arcd` with one provider (Gemini via Auth because it's cheaper to test with), linear sessions only, event log with `SessionEvent`, SQLite projection, the TUI with streaming, identity file loaded into context, Perfetto spans on LLM calls. Memory is *only* the identity file. Exit criterion: ARC replaces a chat app for daily use.

**Phase 2 — Memory.** `MemoryEvent`, distilled records + always-loaded index, the five memory/archive tools, FTS5 over messages, explicit `memory_write` plus end-of-session consolidation, `arcd memory-replay` with a versioned consolidation prompt, the weekly review flow in the TUI, Perfetto spans on all memory operations. Exit criterion: "what do you know about X" and "what did we say about X" both work on real history.

**Phase 3 — Tree + second provider.** Session forking with branch semantics from §4, tree navigation in the TUI, Gemini provider, per-session provider choice, replay/rebuild command (`arcd rebuild`) proven against the real log. Exit criterion: branching used naturally; a full index rebuild from the log matches live state.

**Phase 4 — Voice + remote.** `arc-voice` (wake word, local ASR/TTS), Tailscale-reached daemon from the phone (mobile client can start as the TUI over SSH; the real app comes when the protocol has been stable for a while), rustic backup automated and restore tested — a restore drill is the exit criterion, not a backup existing.

**Phase 5 — Hands.** First device MCP server (ESP32 pan-tilt), device-tool safety conventions, then the arm when the workbench reaches it. sqlite-vec embeddings land here or earlier only if Phase 2–4 usage shows FTS falling short.

## 12. Open questions

Deferred deliberately; decide when the phase forces them:

- Consolidation triggering: idle-timeout vs explicit session close vs continuous. (Phase 2, from traces.)
- Whether identity edits ever move into the event log. (Revisit if hand-editing becomes a bottleneck.)
- Embeddings model choice for sqlite-vec, local vs API. (Phase 4/5.)
- Multi-machine story beyond backup/restore (log sync). (Post-v1.)
