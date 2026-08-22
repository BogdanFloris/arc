# TASKS — Phase 3, development

The live list. Frozen records: `TASKS-phase1.md` (walking skeleton, closed 2026-08-13), `TASKS-phase2.md` (memory, closed 2026-08-22). DESIGN.md §11 is the phase contract; §4, §6, and §7 are the spec.

**The phase in one line:** ARC becomes the harness its own code is written in, and runs as an installed service. Exit criterion is a week of real development done through ARC rather than through another harness, plus a full rebuild matching live state.

Working agreement, unchanged: Bogdan assigns each task (`bogdan` or `claude`). The implementer's work is reviewed by the other. Statuses: `todo` → `in progress` → `in review` → `done`. Assignees are unset below; assignment is Bogdan's.

Tasks are ordered by dependency; anything at the same number can go in parallel. The phase runs in three arcs: the **substrate** (1–4 — roles, registry, tools), **jobs** (5–6 — the thing the substrate is for), and **production** (7 — the part that makes it a daily driver rather than a demo).

**This phase is larger than Phase 2.** If it drags past the point where the substrate is usable, sections 1–5 alone satisfy the development half of the exit criterion, and 6–7 can become Phase 3.1. Prefer that over letting the phase sprawl.

## Decisions banked 2026-08-22, before task-cutting

- **Four roles, static config.** face / hands / counsel / local (DESIGN.md §6.1). No runtime difficulty classifier. Which model fills each slot is `providers.md`, not this file, and is expected to change under us.
- **The concrete stack to build against:** face = Gemini 3.7 Flash on a direct key; hands = DeepSeek V4 Pro via OpenCode Go's OpenAI-compatible endpoint; counsel = Opus via `claude -p` on Pro, read-only, for both modes; local = the existing Qwen3-8B sidecar. Budget target under $50/month against $100 today.
- **Roles are ladders, not single models.** Opus for counsel until budget pressure, then Sonnet; Gemini for face until the network or the allowance is gone, then local. The ladder doubles as the allow-list and every descent is visible in the client.
- **Dispatch is a tool call with a late result.** No new event vocabulary for asynchrony: `ToolCallIssued` when the job starts, `ToolResultRecorded` carrying the summary when it finishes, and §3.1's orphan contract for the crash case. This is why 5.2 is small.
- **Jobs run in a separate worker process.** `bash` does not run in the always-on daemon's address space.
- **counsel never writes**, and is an argv template rather than a code path. It has two modes — `plan` before a job, `review` after each change — and is called from *inside* the job, not from the face.
- **The coding job's loop is fixed and lives in the worker:** plan → implement → review → fix → review, bounded. "Loop until no comments" does not terminate on its own, so only *blocking* comments trigger another round and the bound is configured. A job out of rounds reports done-with-unresolved and names them; it never claims success it did not reach.
- **Sessions pin to one provider for their lifetime.** Reverses the v1 "hot-swappable mid-session" position; cache economics decide it.
- **Approval is one shape** for `bash` and for a servo (§4.3), so Phase 5 inherits it rather than growing a second one.
- **The bet this phase tests:** that most of ~19M output tokens a month are mechanical and a cheap model does them acceptably *given a strict harness*. If it fails, the harness is the first suspect — strict `edit`, a working test loop, rewind — not the model.

## 1. Spikes (before schemas harden)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 1.1 | Provider reachability spike: OpenCode Go and Gemini against the existing `provider/openai` parser. Go is an OpenAI-compatible base URL, a key, and a model id — confirm the SSE dialect matches Phase 1's fixtures. Gemini is the open question: whether its OpenAI-compat layer is faithful enough, or whether it needs its own `Provider`. Output is fixtures plus a written verdict | — | todo |
| 1.2 | `consult_expert` spike, both modes: invoke `claude -p` in a project directory with read-only tools, capture the output shape, and establish how read-only is *enforced* rather than requested. Measure a real `plan` question and a real `review` of a diff end to end — latency, whether the reviewer emits a usable severity classification on request, and **whether any usage signal is exposed** (headers, exit code, stderr) that a threshold degrade could key on. Output is a verdict and two argv templates | — | todo |
| 1.3 | Face dispatch reliability: does Flash-class judgment pick the right role, project, and brief? Twenty scripted asks against the dispatch schema, scored for wrong role, wrong project, and malformed brief. Cheap to run, and it decides the §12 question of whether dispatch escalates | — | todo |

If 1.3 comes back shaky, the fallback is dispatch on a stronger model as a config line — decided before section 5 starts, not as a redesign.

## 2. Schemas (`arc-proto`)

Each schema change is its own commit, separate from code that uses it (invariant 3: additive only).

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 2.1 | `events.proto`: `ToolApprovalRecorded` — a new kind inside `SessionEvent`, keyed by `call_id`, carrying verdict, scope (once / session / project), and denial reason. `Event.source = USER`, like §5.4's review verdicts | — | todo |
| 2.2 | `events.proto`: session creation carries `role`, `project`, and budget, so a replayed job reproduces what it actually ran with. Uses the reserved `ToolCallIssued` model field from §3.1 rather than adding a second place to look | — | todo |
| 2.3 | `events.proto`: structured tool-result content, filling §3.1's third reserve — an image result needs its own field | — | todo |
| 2.4 | `wire.proto`: images in messages, modality on subscribe, and server-initiated output. Folds in the wire-friction list banked in `TASKS-phase1.md`, since this is the evolution it was waiting for | — | todo |
| 2.5 | `wire.proto`: job frames — status, steering, and the approval prompt/response round trip | — | todo |
| 2.6 | `arc.toml`: `[roles.*]`, `[experts.*]`, `[projects.*]`. Roles resolve to provider + model + allow-list; experts are argv + cwd + timeout, one block per mode; the coding job carries a review-round bound and a severity gate | — | todo |

## 3. Roles and providers (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 3.1 | Role resolution: config to provider + model, a role label on every `CompletionRequest`, and that label on the span (§8). This is the hermes chokepoint and everything else in the phase measures through it | — | todo |
| 3.2 | Session pinning: role chosen at session or job creation, immutable for its lifetime | — | todo |
| 3.3 | Keyed providers: keys from `data/secrets/` (0700), Go via `provider/openai`, Gemini per 1.1's verdict | — | todo |
| 3.4 | Role ladders (§6.1): an ordered list of rungs per role, each with a degrade condition. Doubles as the allow-list, so a lineup change fails closed rather than silently routing somewhere unvetted. Rungs recover when the window resets | — | todo |
| 3.5 | Ladder descent: credit exhaustion (402 at a plan cap) is not a retryable rate limit (429). Exhaustion falls through to spillover if enabled, then to the next rung. `counsel` degrades Opus → Sonnet, threshold-triggered per 1.2's verdict or reactive if no usage signal exists; `face` degrades to `local`. The client says which rung is live and why | — | todo |
| 3.6 | Prefix stability for the face: identity file and record index render first and byte-identically, everything volatile after. A regression test that asserts two consecutive turns produce an identical prefix — this is ~96% of the workload and it fails silently | — | todo |
| 3.7 | **Sidecar restart policy** (carried from Phase 1). More pressing now: `local` is the offline fallback for face, not only a background worker | — | todo |
| 3.8 | **Write `data/identity.md` for the face** — ARC's register per §5.1's four rules, plus the stable facts the always-loaded prompt should carry. Not code, and by invariant 7 not something an agent may write; this one is Bogdan's. Until it exists the face runs on whatever voice the model defaults to | bogdan | todo |

## 4. Tool registry (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 4.1 | Registry with sources (builtin / workspace / expert / mcp), session-scoped declaration, and the five memory tools migrated onto it unchanged. Uses §3.1's reserved tool-source field | — | todo |
| 4.2 | Approval gate: propose, block, record the verdict (2.1), and project the allow-list. Denial returns `ToolOutcome::ERROR` with the reason so the loop adapts | — | todo |
| 4.3 | Workspace tools, read-only half: `read`, `glob`, `grep`. Path confinement to the project root — canonical resolution, symlinks and `..` rejected — tested adversarially | — | todo |
| 4.4 | Workspace tools, writing half: `write`, and `edit` with exact single-occurrence matching and a staleness check that refuses if the file changed since last read. DESIGN.md calls this the highest-leverage rule in §4.3; treat its tests as such | — | todo |
| 4.5 | `bash` with a scrubbed environment (§10) and approval by default. Resolves §3.1's banked redaction question | — | todo |
| 4.6 | `consult_expert` from 1.2's templates: command, cwd, timeout, read-only enforced by invocation, `plan` and `review` modes. Callable from a worker as well as from the face | — | todo |
| 4.7 | **Token budget re-measure** (carried, re-scoped). The Phase 2 note assumed a 16k local face; with a hosted face the ceiling changes and the question becomes which sources load into which session. Measure before 5.2 wires anything else always-on | — | todo |

## 5. Jobs

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 5.1 | Workspace binding: `sessions.project` plus a root on disk, and the rule that unbound sessions get no workspace tools | — | todo |
| 5.2 | The dispatch tool: fork a child session with its own role, tool sources, and budget; return the call late with the summary. Small by construction — the asynchrony is §3.1's, not new | — | todo |
| 5.3 | The worker process: owns the loop, talks to the same `Store`, dies without taking `arcd` with it. Supervision and restart policy, sharing whatever 3.7 lands | — | todo |
| 5.3b | The coding job's review loop: plan → implement → review → fix → review, counsel on both ends, bounded rounds, severity gate on what re-triggers implement. Termination is honest — done-with-unresolved is a reportable outcome. Every step an ordinary event in the child session, so the cycle is replayable | — | todo |
| 5.4 | Steering: messages to a running child, queued and processed in order | — | todo |
| 5.5 | Budget enforcement per job, in tokens and in wall-clock, appended at dispatch and checked by the worker | — | todo |
| 5.6 | Handback: a summary plus the child's session id into the parent, with the full transcript staying in the archive | — | todo |

## 6. Clients

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 6.1 | TUI: job strip — live jobs, status, budget consumed. The conversation stays usable while one runs | — | todo |
| 6.2 | TUI: approval prompts with the three verdicts, and a view of the current project allow-list | — | todo |
| 6.3 | Notification: server-initiated output reaching the client, with a badge and a bell. Built general rather than wired only to job completion — this is the path ARC speaks first on (§7) | — | todo |
| 6.4 | TUI: images in the transcript, and the modality hint sent on subscribe | — | todo |
| 6.5 | **Session titling pass** (carried). Now cheap: titles are `local`-role work and the job strip needs something better than a session id to display | — | todo |

## 7. Production

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 7.1 | `arcd rebuild` proven against the real log: drop the index, replay, diff against live state. The phase contract names this explicitly | — | todo |
| 7.2 | Installed for real: systemd user unit enabled, release binary on a stable path, `data/` surviving a rebuild of the machine. Phase 1 left the unit installed but not enabled | — | todo |
| 7.3 | Cost accounting from traces: spend per role and per completed task, enough to rewrite `providers.md` §3 from measurements instead of published rates | — | todo |
| 7.4 | Phase 1 leftovers: `log::Error::Io` field doc, and a retention policy for `data/traces/` — which stops being theoretical once jobs emit spans for twenty-minute runs | — | todo |

## Exit criteria

Checked live, not by test suite:

- [ ] A week of real development on ARC done through ARC, not through another harness.
- [ ] `arcd rebuild` reproduces live state from the log.
- [ ] A job runs to completion while the conversation stays responsive, and reports back.
- [ ] A job completes a full plan → implement → review → fix cycle, and a job that exhausts its rounds says so instead of claiming success.
- [ ] An approval prompt reaches the user, a denial reaches the model, and the allow-list survives a restart.
- [ ] Voice-less notification works: a job finishing while the user is elsewhere is noticed on return.
- [ ] Face degrades to `local` with the network off, and says so. Counsel's Opus → Sonnet rung is exercised at least once, deliberately, and recovers.
- [ ] Measured monthly spend under $50, with the split visible by role in traces.

## Not in this phase

- **Forking, rewind, tree navigation** — Phase 3.5, immediately after. Expect to want it during this phase; that is the argument for it being next, not for pulling it in.
- **Voice** — Phase 4. Nothing here may assume an audio client, but 2.4 and 6.3 must not make one harder.
- **Devices** — Phase 5. §4.3's `mcp` source is named and unimplemented, deliberately.
- **Compaction as an event** (DESIGN.md §12) — only when a real job hits a context limit. It is a schema change and should be paid for by evidence.
- **Prompt v2** (carried) — still gated on extraction quality warranting it, with its two pinned regression cases and `memory-replay --against` as the gate.
- **Batched log appends** — still only with trace evidence that fsync-per-append costs something. Jobs will generate that evidence or settle it.
