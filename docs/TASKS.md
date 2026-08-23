# TASKS — Phase 3, development

This is the live list. `TASKS-phase1.md` and `TASKS-phase2.md` are historical records. `DESIGN.md` defines the phase and the technical rules.

**Phase goal:** ARC becomes the harness used to write its own code and runs as an installed service. Success requires a week of real development through ARC and a full rebuild matching live state.

Bogdan assigns each task (`bogdan` or `claude`); the other reviews it. Statuses are `todo` → `in progress` → `in review` → `done`. Assignees below are intentionally unset.

Tasks are dependency-ordered; tasks at the same number may run in parallel. The phase has three parts: **substrate** (1–4: roles, registry, tools), **jobs** (5–6), and **production** (7: daily-driver operation).

**This phase is larger than Phase 2.** Once the substrate is usable, sections 1–5 satisfy the development half of the exit criterion. Move sections 6–7 to Phase 3.1 rather than expanding this phase.

## Decisions made before planning, 2026-08-22

- **Four configured roles:** face, hands, counsel, and local. There is no runtime difficulty classifier. `providers.md` records the current models.
- **The concrete stack to build against:** face = Gemini 3.7 Flash on a direct key; hands = DeepSeek V4 Pro via OpenCode Go's OpenAI-compatible endpoint; counsel = Opus via `claude -p` on Pro, read-only, for both modes; local = the existing Qwen3-8B sidecar. Budget target under $50/month against $100 today.
- **Roles use ladders, not single models.** Counsel uses Opus, then Sonnet under budget pressure. Face uses Gemini, then local when the network or allowance fails. The ladder is the allow-list, and the client shows every descent.
- **Dispatch is a tool call with a delayed result.** `ToolCallIssued` starts the job. `ToolResultRecorded` carries the final summary. The existing unfinished-call recovery rule handles crashes.
- **Jobs run in a separate worker process.** `bash` does not run in the always-on daemon's address space.
- **counsel never writes.** It is an argv template, not a code path. Its `plan` and `review` modes run inside the job, not from face.
- **The worker has a bounded coding loop:** plan → implement → review → fix → review. Only *blocking* comments start another round. A job that runs out of rounds reports `done-with-unresolved` and names the remaining issues; it does not claim success.
- **Sessions pin one provider for life.** This replaces v1's hot-swapping position because of cache economics.
- **Approval works the same way** for `bash` and a servo. Phase 5 will reuse it instead of adding a second system.
- **This phase tests whether** a cheap model can handle most of about 19M monthly output tokens when the harness is strict. If it fails, investigate the harness first: strict `edit`, a working test loop, and rewind.

## 1. Spikes (before schemas harden)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 1.1 | Test OpenCode Go and Gemini with the existing `provider/openai` parser. Confirm that Go's SSE output matches the Phase 1 fixtures. Decide whether Gemini's OpenAI-compatible API is sufficient or needs its own `Provider`. Deliver fixtures and a written decision. | — | todo |
| 1.2 | Test both `consult_expert` modes. Run `claude -p` in a project with read-only tools and prove that read-only access is enforced. Measure a real plan request and a real diff review: latency, usable severity labels, and any usage signal in headers, exit status, or stderr. Deliver a decision and two argv templates. | — | todo |
| 1.3 | Test whether Flash chooses the right role, project, and brief. Score 20 scripted requests for wrong role, wrong project, and malformed brief. Use the result to decide whether dispatch needs a stronger model. | — | todo |

If 1.3 is unreliable, configure dispatch to use a stronger model before section 5 starts. Do not redesign the system.

## 2. Schemas (`arc-proto`)

Each schema change is its own commit, separate from code that uses it (invariant 3: additive only).

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 2.1 | Add `ToolApprovalRecorded` to `SessionEvent` in `events.proto`. It uses `call_id` and records the verdict, scope (once, session, or project), and denial reason. Set `Event.source` to `USER`. | — | todo |
| 2.2 | Record a session's role, project, and budget at creation so replay shows what a job ran with. Use the reserved model field on `ToolCallIssued`; do not add another copy. | — | todo |
| 2.3 | Add structured tool-result content to `events.proto`. Image results need their own field. | — | todo |
| 2.4 | Add message images, subscribe modality, and server-initiated output to `wire.proto`. Include the deferred wire-protocol fixes from `TASKS-phase1.md`. | — | todo |
| 2.5 | `wire.proto`: job frames — status, steering, and the approval prompt/response round trip | — | todo |
| 2.6 | `arc.toml`: `[roles.*]`, `[experts.*]`, `[projects.*]`. Roles resolve to provider + model + allow-list; experts are argv + cwd + timeout, one block per mode; the coding job carries a review-round bound and a severity gate | — | todo |

## 3. Roles and providers (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 3.1 | Resolve each configured role to a provider and model. Add the role label to every `CompletionRequest` and trace span. This is the single place where Phase 3 work is measured. | — | todo |
| 3.2 | Session pinning: role chosen at session or job creation, immutable for its lifetime | — | todo |
| 3.3 | Keyed providers: keys from `data/secrets/` (0700), Go via `provider/openai`, Gemini per 1.1's verdict | — | todo |
| 3.4 | Add an ordered fallback list to each role. Each entry has a degradation condition. The list is also the allow-list, so provider changes fail closed. Restore the normal entry when its window resets. | — | todo |
| 3.5 | Ladder descent: credit exhaustion (402 at a plan cap) is not a retryable rate limit (429). Exhaustion falls through to spillover if enabled, then to the next rung. `counsel` degrades Opus → Sonnet, threshold-triggered per 1.2's verdict or reactive if no usage signal exists; `face` degrades to `local`. The client says which rung is live and why | — | todo |
| 3.6 | Prefix stability for the face: identity file and record index render first and byte-identically, everything volatile after. A regression test that asserts two consecutive turns produce an identical prefix — this is ~96% of the workload and it fails silently | — | todo |
| 3.7 | **Sidecar restart policy** (carried from Phase 1). More pressing now: `local` is the offline fallback for face, not only a background worker | — | todo |
| 3.8 | **Write `data/identity.md` for the face** — ARC's register per §5.1's four rules, plus the stable facts the always-loaded prompt should carry. Not code, and by invariant 7 not something an agent may write; this one is Bogdan's. Until it exists the face runs on whatever voice the model defaults to | bogdan | todo |

## 4. Tool registry (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 4.1 | Add a registry with builtin, workspace, expert, and MCP sources. Declarations are session-scoped. Move the five memory tools without changing them. Use the reserved tool-source field. | — | todo |
| 4.2 | Approval gate: propose, block, record the verdict (2.1), and project the allow-list. Denial returns `ToolOutcome::ERROR` with the reason so the loop adapts | — | todo |
| 4.3 | Workspace tools, read-only half: `read`, `glob`, `grep`. Path confinement to the project root — canonical resolution, symlinks and `..` rejected — tested adversarially | — | todo |
| 4.4 | Add workspace `write` and `edit`. `edit` must match exactly one occurrence and reject a file changed since the last read. Test this rule thoroughly. | — | todo |
| 4.5 | Add approved-by-default `bash` with a scrubbed environment. This settles how tools that could read their own environment protect secrets. | — | todo |
| 4.6 | `consult_expert` from 1.2's templates: command, cwd, timeout, read-only enforced by invocation, `plan` and `review` modes. Callable from a worker as well as from the face | — | todo |
| 4.7 | **Token budget re-measure** (carried, re-scoped). The Phase 2 note assumed a 16k local face; with a hosted face the ceiling changes and the question becomes which sources load into which session. Measure before 5.2 wires anything else always-on | — | todo |

## 5. Jobs

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 5.1 | Workspace binding: `sessions.project` plus a root on disk, and the rule that unbound sessions get no workspace tools | — | todo |
| 5.2 | Add the dispatch tool. It creates a child session with its own role, tool sources, and budget, then returns the summary later. It uses the existing tool-call recovery model. | — | todo |
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
| 6.3 | Deliver server-initiated output to clients with a badge and bell. Support general notifications, not only completed jobs. | — | todo |
| 6.4 | TUI: images in the transcript, and the modality hint sent on subscribe | — | todo |
| 6.5 | **Session titling pass** (carried). Now cheap: titles are `local`-role work and the job strip needs something better than a session id to display | — | todo |

## 7. Production

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 7.1 | `arcd rebuild` proven against the real log: drop the index, replay, diff against live state. The phase contract names this explicitly | — | todo |
| 7.2 | Installed for real: systemd user unit enabled, release binary on a stable path, `data/` surviving a rebuild of the machine. Phase 1 left the unit installed but not enabled | — | todo |
| 7.3 | Account for spend per role and completed task from traces. Use the measurements to replace published-rate estimates in `providers.md`. | — | todo |
| 7.4 | Phase 1 leftovers: `log::Error::Io` field doc, and a retention policy for `data/traces/` — which stops being theoretical once jobs emit spans for twenty-minute runs | — | todo |

## Exit criteria

Verify these live, not only in the test suite:

- [ ] A week of real development on ARC done through ARC, not through another harness.
- [ ] `arcd rebuild` reproduces live state from the log.
- [ ] A job runs to completion while the conversation stays responsive, and reports back.
- [ ] A job completes a full plan → implement → review → fix cycle, and a job that exhausts its rounds says so instead of claiming success.
- [ ] An approval prompt reaches the user, a denial reaches the model, and the allow-list survives a restart.
- [ ] Voice-less notification works: a job finishing while the user is elsewhere is noticed on return.
- [ ] Face degrades to `local` with the network off, and says so. Counsel's Opus → Sonnet rung is exercised at least once, deliberately, and recovers.
- [ ] Measured monthly spend under $50, with the split visible by role in traces.

## Not in this phase

- **Forking, rewind, tree navigation:** Phase 3.5. Wanting them now makes them next, not part of this phase.
- **Voice:** Phase 4. Nothing here may assume an audio client, but 2.4 and 6.3 must preserve its path.
- **Devices:** Phase 5. §4.3 names but does not implement the `mcp` source.
- **Compaction as an event** (DESIGN.md §12): wait until a real job reaches a context limit. This schema change needs evidence.
- **Prompt v2:** wait until extraction quality warrants it. Its gate remains two pinned regression cases and `memory-replay --against`.
- **Batched log appends:** wait for traces showing that fsync per append costs enough. Jobs will provide that evidence or settle the question.
