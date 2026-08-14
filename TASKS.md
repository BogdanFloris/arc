# TASKS — Phase 2 memory

The live list. Phase 1's frozen record is `TASKS-phase1.md`; DESIGN.md §5 is the spec, §11 the phase contract. Exit criterion: "what do you know about X" and "what did we say about X" both work on real history.

Working agreement, unchanged: Bogdan assigns each task (`bogdan` or `claude`). The implementer's work is reviewed by the other. Statuses: `todo` → `in progress` → `in review` → `done`.

Tasks are ordered by dependency; anything at the same number can go in parallel. The phase runs in two halves: the **agentic substrate** (1–4 — Phase 1 has no tool support anywhere in the stack) and **memory itself** (5–8). Sections 5 and 6 each ship value on their own; neither waits for 7.

Decisions banked 2026-08-14, before task-cutting:

- **Consolidation trigger: idle timeout.** A session untouched for a configurable window consolidates. Provisional by design — DESIGN.md §12 keeps the question open for tuning from traces; this is the v1 placeholder that lets the code exist.
- **Consolidation model: the same local model.** The sidecar serves exactly one; "a cheap model pass" means the model we have until routing exists.
- **Tool activity is visible in the TUI.** Additive wire frames, rendered in the transcript. Watching memory work is how it gets tuned; invisible tools cannot be reviewed.
- **Routing is later, deliberately.** The ambition — match a Claude Code 100-style tier with our own router picking the best model per request across local + OpenRouter — is banked in DESIGN.md §12, gated on usage data Phase 2 will generate. Nothing in this phase may assume more than one provider.

## 1. Spike (before schemas harden)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 1.1 | Tool-calling spike against `llama-server`: tools on (`--jinja`), one toy tool, capture SSE fixtures of Qwen3-8B tool calls. Output is fixtures plus a written verdict — the delta dialect for 3.x's parser, and whether 8B/Q4 calls tools reliably enough to build on. No production code | — | todo |

If the verdict is "shaky", the fallback is a bigger model as a config line, decided before section 3 starts — not a redesign.

## 2. Schemas (`arc-proto`)

Each schema change is its own commit, separate from code that uses it (invariant 3: additive only).

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 2.1 | Sketch the tool-call event vocabulary (the question banked in DESIGN.md §12): likely new kinds inside `SessionEvent` — a new top-level payload arm is a replay hazard per §3 rule 3, a new kind inside an arm is skipped safely. Includes the resume contract for a durable call with no durable result (prior art: DeepSeek Harness's `TOOL_OUTCOME_UNKNOWN`). Lands as a DESIGN.md amendment first, proto after | — | todo |
| 2.2 | `events.proto`: tool call / tool result events per the 2.1 sketch | — | todo |
| 2.3 | `events.proto`: `MemoryEvent` + `MemoryRecord` (§5.2) — created / updated / superseded / deleted | — | todo |
| 2.4 | `wire.proto`: tool-activity frames so a client can render what a turn is doing. Additive oneof numbers only; the Phase 3 wire-friction list in `TASKS-phase1.md` stays banked, this does not open it | — | todo |

## 3. Provider tool calling (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 3.1 | `CompletionRequest` grows tool definitions, `CompletionDelta` grows a tool-call variant — the seam `provider/mod.rs` reserved | — | todo |
| 3.2 | OpenAI-compat: request building with tools + SSE parsing of tool-call deltas, fixture tests from 1.1's captures | — | todo |

## 4. Engine tool loop (`arc-core` / `arcd`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 4.1 | Tool registry seam in `arc-core`: a trait, dispatch, result → event. Memory tools plug in at 5.2/6.3; the toy tool from 1.1 proves the seam | — | todo |
| 4.2 | Engine loop: completion → tool-call event → execute → tool-result event → continue until final text. Iteration cap; a span per call (instrument in the same change) | — | todo |
| 4.3 | Resume: on startup/replay, a durable call with no durable result surfaces per the 2.1 contract instead of being silently dropped | — | todo |
| 4.4 | Wire + TUI: daemon emits 2.4's frames during the loop; `arc` renders tool activity in the transcript (dim, inline — same voice as status words) | — | todo |

## 5. Archive tier (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 5.1 | Projection: messages shape extended for structured turns per the 2.1 sketch, FTS5 index over `content`, replay tests. Fold in the reserved `partial` column (`HistoryMessage` field 3) while the schema is open | — | todo |
| 5.2 | `sessions_search` (FTS, snippets + session ids) + `session_read` (targeted range) as tools over the projection | — | todo |

## 6. Distilled tier (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 6.1 | Memory projection: record state as a deterministic replay of `MemoryEvent`s; supersede keeps history, `DELETED` excludes entirely (§5.2) | — | todo |
| 6.2 | Always-loaded index of ACTIVE records (namespace + kind + title + summary) into system context beside the identity file — the one sanctioned injection (invariant 6) | — | todo |
| 6.3 | `memory_read` / `memory_search` / `memory_write` / `memory_supersede` tools; writes emit events, never touch projection state directly (invariant 2) | — | todo |

## 7. Consolidation (`arc-core` / `arcd`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 7.1 | Idle-timeout trigger in `arcd` (configurable window) → async consolidation pass on the daemon; spans on every decision | — | todo |
| 7.2 | Extraction pass: versioned consolidation prompt, extract durable facts, merge with existing records, resolve contradictions by superseding (§5.4) | — | todo |
| 7.3 | `arcd memory-replay`: run a prompt version over historical sessions, diff resulting memory state against another version — the regression suite for prompt changes | — | todo |

## 8. Review + metrics

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 8.1 | Weekly review flow in the TUI: records created/superseded that week; accept / fix / delete, each an ordinary `MemoryEvent` | — | todo |
| 8.2 | The three §5.4 counters in traces: records per session, supersede rate, retrieval hit rate — `counter.*` fields, so the 8.x trace layer needs no new code | — | todo |

## Carried from Phase 1 (fold into the next touch, no own task)

- `log::Error::Io`'s field doc says "segment" but the variant also carries directory paths.
- No sidecar restart policy (unexpected exit logged loudly; decide when it hurts).
- No retention on `data/traces/` (kilobytes per run; revisit if that changes).
- Wire-protocol friction list → Phase 3, banked in `TASKS-phase1.md`.

## Later / gated on traces

- Batched log appends with flush checkpoint — only with trace evidence that fsync-per-append is a real cost (DESIGN.md §3, durability policy).
- Provider routing over local + OpenRouter (DESIGN.md §12) — after Phase 2 usage shows what a router would need to know.
