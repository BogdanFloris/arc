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
| 1.1 | Tool-calling spike against `llama-server`: tools on (`--jinja`), one toy tool, capture SSE fixtures of Qwen3-8B tool calls. Output is fixtures plus a written verdict — the delta dialect for 3.x's parser, and whether 8B/Q4 calls tools reliably enough to build on. No production code | claude | in review |

If the verdict is "shaky", the fallback is a bigger model as a config line, decided before section 3 starts — not a redesign.

### 1.1 verdict (run 2026-08-14 on erebor, `llama-server` b10273-a6aa6f5, Qwen3-8B-Q4_K_M, Vulkan1, port 8090)

**Build on it.** 40 scored runs, 40 clean: no wrong tool, no spurious call, no malformed argument JSON, no missed call. The 8B is not the risk in this phase.

| Scenario | Result | Wrong tool | Malformed JSON |
|---|---|---|---|
| A — prompt needs `memory_search` | 10/10 called it, every `query` a sensible string, 10/10 also passed `namespace: "projects"` | 0 | 0 |
| B — prompt needs no tool (a rhyme) | 10/10 answered plainly, `finish_reason: stop` | 0 spurious calls | — |
| C — round trip, `role:"tool"` result fed back | 10/10 grounded final answers naming SQLite/FTS5 from the record, no re-call of the tool | 0 | — |
| D — both tools offered, prompt needs `get_time` | 10/10 picked `get_time` | 0 | 0 |

Across all 71 captures (the 40 scored, 20 `/no_think` comparisons, 11 probes) there were 46 tool calls and zero structural defects: every call's arguments parsed as a JSON object, every stream ended `data: [DONE]`.

**The dialect, as bytes.** Standard OpenAI `chat.completion.chunk` framing — `data:` payloads, LF-framed, blank line between frames, no CRLF anywhere, `[DONE]` sentinel last, the usage frame (with llama.cpp's own `timings` sibling) before it. Everything the Phase 1 parser already knows still holds. Four things it does not:

1. **`delta.tool_calls[]`.** The opening chunk of a call carries `index`, `id`, `type: "function"`, `function.name` **and** the first `function.arguments` fragment. Every later chunk carries `index` and `function.arguments` only — never a repeated id, type or name (0 of 494 continuation chunks repeated one). Arguments stream token-by-token, so JSON is only valid once concatenated; a call with no arguments arrives as `"{"` then `"}"`, never as a single `"{}"` and never absent (14 of 46 calls).
2. **`finish_reason: "tool_calls"`** instead of `"stop"`, on a chunk whose `delta` is `{}`, followed by the usage frame. `id`s are server-generated, 32 chars of alphanumeric, stable within a stream — the model never picks them.
3. **Parallel calls in one turn.** A prompt needing both tools produced two calls, `index` 0 and 1, in 8 of 8 runs — index 1's opener follows index 0's last argument fragment with no separator frame. Indexes were always dense from 0. A parser keyed on "the tool call" rather than on `index` will silently merge them.
4. **`delta.reasoning_content`.** Qwen3's thinking is split into its own delta field, not wrapped in `<think>` tags inside `content`. It dominates the stream: 6493 reasoning deltas against 1255 content deltas across the captures. No single delta ever carried both `content` and `reasoning_content`, and no stream ever mixed assistant text with a tool call — text and calls were mutually exclusive in all 71.

**Two surprises.** `--jinja` is not load-bearing on this build: with the flag off — exactly how `arcd` runs the sidecar today — tool calls and `reasoning_content` came out identically. Which leads to the second: **the current parser is already dropping the model's thinking on the floor.** `arc-core`'s OpenAI parser reads `content` and ignores unknown fields, so a live turn today streams hundreds of reasoning deltas that never reach the TUI. Replaying `arcd`'s exact payload against the same build produced 774 frames of reasoning for "Say hello to arc in five words"; every trivial prompt tried thought for 126–541 tokens. `openai_stream.sse` (7 completion tokens, no reasoning) is therefore not a representative capture — whatever suppressed thinking when it was taken, the default does not. That is a silent-dead-air bug in Phase 1's output, not a Phase 2 one, and it is 3.2's to decide: drop, stream as a distinct delta kind, or store.

**What it costs.** ~110 tok/s throughout, unchanged by tools. Two tool schemas cost ~320 prompt tokens per request (334 with tools vs 9–13 without) — the always-loaded index in 6.2 pays on top of that, on an 8k context. With thinking on, a tool call costs 106–149 completion tokens (~1.1 s) before the call is even visible, a grounded answer after a tool result 168–227 (~1.7 s), and the two-call turn 298–359 (~3.0 s). So a one-tool turn is ~3 s of model time before the user sees prose, and each extra loop iteration adds ~1–2 s. `/no_think` in the system prompt cut that 3–4× (A: 34 tokens, 0.33 s; C: 53 tokens) **with identical accuracy** — 10/10 on A and B, same tools, same arguments. It is the obvious dial if the loop feels slow, and it is a per-request choice, so consolidation (7.2) can think while an interactive turn does not.

**What this constrains.**

- *2.1:* one assistant turn can hold N calls, so the event vocabulary needs a stable per-call identity within a turn — the server's `id` plus `index`. Reasoning is a third kind of assistant output beside text and calls; decide whether it is durable before the schema hardens, because "we'll add it later" means old events that silently lost it.
- *3.2:* accumulate arguments by `index`, not by arrival order; treat only the opening chunk as authoritative for id/type/name; do not assume `finish_reason: "stop"`; do not assume `content` and `tool_calls` are alternatives to *decide* between per stream — they are per delta. The four fixtures are raw bytes off the wire, same convention as `openai_stream.sse` (no header comment; provenance lives in the parser's module docs): `openai_tool_call_stream.sse` (one `memory_search` call, `/no_think`, the minimal case), `openai_parallel_tool_calls_stream.sse` (two calls, indexes 0 and 1, one with empty arguments), `openai_tool_result_stream.sse` (round trip: `role:"tool"` in, grounded answer out, `finish_reason: stop`), `openai_reasoning_stream.sse` (thinking on: `reasoning_content` deltas then text — the contrast with `openai_stream.sse`).

**What the numbers do not cover**, deliberately, inside the timebox: one model at its defaults (temp 0.8 / top-p 0.95 / top-k 40), two tools, unambiguous prompts, single-round loops, short contexts. Nothing here says the 8B holds up with eight tools, a full memory index in the prompt, a 7k-token context, or a prompt where the right move is genuinely unclear — those are what section 4's loop will actually stress, and the honest read is that reliability was never tested near its edge. If it frays there, the fallback stands: a bigger model is a config line.

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
