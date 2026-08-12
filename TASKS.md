# TASKS — Phase 1 walking skeleton

Working agreement: Bogdan assigns each task (`bogdan` or `claude`). The implementer's work is reviewed by the other. Statuses: `todo` → `in progress` → `in review` → `done`.

Tasks are ordered by dependency; anything at the same number can go in parallel.

Loose ends (fold into the next touch of the relevant file, no own task): `log::Error::Io`'s field doc says "segment" but the variant also carries directory paths (from `sync_parent_dir`, `discover_segments`).

## 1. Schemas (`arc-proto`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 1.1 | `events.proto`: `Event` envelope (seq, ts, source, oneof payload) + `SessionEvent` (session created, message appended; fork fields reserved) | bogdan | done |
| 1.2 | `wire.proto`: minimal protocol — send message (empty session_id creates a session), streamed deltas, list sessions, error frame | bogdan | done |
| 1.3 | prost generation in `build.rs` + a round-trip encode/decode smoke test | claude | done |

## 2. Event log (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 2.1 | Segment writer: length-prefix + CRC32 framing, protobuf append, fsync policy, monotonic gapless seq. Refuse to write an `Event` with `payload: None` | claude | done |
| 2.2 | Segment reader: iterate events across segment files, detect/stop at torn tail. Truncation detection comes from the length prefix, corruption from the CRC — never from decode failure (empty/partial bytes decode "successfully" in proto3); `payload: None` on a full-length record is a hard error | bogdan | done |
| 2.3 | Segment rollover by size + segment file naming. Add a shared `MAX_RECORD_LEN` sanity cap to `log::format` (writer enforces, reader rejects) | claude | done |

## 3. SQLite projection (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 3.1 | Schema: `sessions` + `messages` tables, projection struct over rusqlite | claude | done |
| 3.2 | Replay: log in → state out, resumable from last projected seq; deterministic replay test | bogdan | done |

## 4. Provider (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 4.1 | `Provider` trait + `CompletionRequest` / `CompletionDelta` types | claude | done |
| 4.2 | Google OAuth: loopback flow with the community-documented public client, token cache in `data/secrets/` (0600), refresh | claude | done |
| 4.3 | Antigravity provider: `loadCodeAssist`/`onboardUser` onboarding, request building + required headers against `cloudcode-pa.googleapis.com` | claude | done |
| 4.4 | SSE stream parsing → `CompletionDelta` stream, with parser unit tests against captured fixtures (no secrets in fixtures) | claude | done |

## 5. Daemon (`arcd`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 5.1 | Skeleton: config, `data/` layout, tracing subscriber init | — | todo |
| 5.2 | Session engine: create session / append user message → drive provider → append model message, all via log events | — | todo |
| 5.3 | Identity file: load `data/identity.md` into system context (read-only) | — | todo |
| 5.4 | WebSocket server on localhost speaking `wire.proto`, streaming deltas to the client | — | todo |

## 6. TUI (`arc`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 6.1 | Connect + session list + create session + send message | — | todo |
| 6.2 | Streaming render of model deltas | — | todo |

## 7. Observability

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 7.1 | Perfetto `TracePacket` output from `tracing` spans, written to `data/traces/` | — | todo |
| 7.2 | Spans + token counters on LLM calls (lands with 4.x/5.2, verified in Perfetto UI) | — | todo |
