# TASKS — Phase 3, development

This is the live list. `TASKS-phase1.md` and `TASKS-phase2.md` are historical records. `DESIGN.md` defines the phase and the technical rules.

**Phase goal:** ARC becomes the harness used to write its own code and runs as an installed service. Success requires a week of real development through ARC and a full rebuild matching live state.

Bogdan assigns each task (`bogdan` or `claude`); the other reviews it. Statuses are `todo` → `in progress` → `in review` → `done`. Assignees below are intentionally unset.

Tasks are dependency-ordered; tasks at the same number may run in parallel. The phase has three parts: **substrate** (1–4: roles, registry, tools), **jobs** (5–6), and **production** (7: daily-driver operation).

**This phase is larger than Phase 2.** Once the substrate is usable, sections 1–5 satisfy the development half of the exit criterion. Move sections 6–7 to Phase 3.1 rather than expanding this phase.

## Decisions made before planning, 2026-08-22

- **Three configured roles:** face, hands, and local. There is no runtime difficulty classifier. `providers.md` records the current models.
- **The concrete stack to build against:** face = Gemini 3.7 Flash on a direct key; hands = DeepSeek V4 Pro via OpenCode Go's OpenAI-compatible endpoint; local = the existing Qwen3-8B sidecar. Budget target under $50/month against $100 today.
- **Roles use one model each.** Provider failure reaches the client. Fallback ladders need real outage or spend evidence before they become stateful policy.
- **Dispatch is a tool call with a delayed result.** `ToolCallIssued` starts the job. `ToolResultRecorded` carries the final summary. The existing unfinished-call recovery rule handles crashes.
- **Jobs run as supervised daemon tasks.** This keeps the conversation responsive; it is not containment. Process separation waits for a sandbox design.
- **The coding loop is generic:** send messages, run requested tools, append results, and stop. Planning and review are prompt or configuration policy until use proves they need machinery.
- **Sessions pin one provider for life.** This replaces v1's hot-swapping position because of cache economics.
- **Nothing prompts for permission.** What a project allows is configuration; a call outside it returns an error the model acts on. A per-call prompt would block the twenty-minute jobs it exists to guard. Phase 5 designs an actuator's confirmation against a real actuator instead of inheriting this.
- **This phase tests whether** a cheap model can handle most of about 19M monthly output tokens when the harness is strict. If it fails, investigate the harness first: strict `edit`, a working test loop, and rewind.

## 1. Spikes (before schemas harden)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 1.1 | Test OpenCode Go and Gemini with the existing `provider/openai` parser. Confirm that Go's SSE output matches the Phase 1 fixtures. Decide whether Gemini's OpenAI-compatible API is sufficient or needs its own `Provider`. Deliver fixtures and a written decision. **Go passes unchanged; Gemini needs its own `Provider`, because a tool result cannot be fed back without echoing an opaque per-call `thought_signature` that has to survive replay.** | claude | done |
| 1.2 | Test both `consult_expert` modes. Run `claude -p` in a project with read-only tools and prove that read-only access is enforced. Measure a real plan request and a real diff review: latency, usable severity labels, and any usage signal in headers, exit status, or stderr. Deliver a decision and two argv templates. **Read-only is enforced on both `claude -p` and `codex exec`, and severity labels and usage come back structured. Keep these templates for a later expert-tool task; they are not part of the initial job path.** | claude | done |
| 1.3 | Test whether Flash chooses the right role, project, and brief. Score 20 scripted requests for wrong role, wrong project, and malformed brief. Use the result to decide whether dispatch needs a stronger model. **Flash is good enough — 0/20 wrong project and 0/20 bad briefs — provided every dispatch field is required with an explicit escape value, since optional fields get dropped.** | claude | done |

1.3 came back reliable, so dispatch stays on Flash. The findings that bind later tasks are folded into the rows below. Two remain unfiled:

- **Gemini caching is unverified.** No probe carried a prefix worth caching, so no cached tokens appeared in `usage`. Re-check with a real face prompt before trusting the 90% discount in `providers.md`. Explicit caching needs `extra_body.cached_content`, which the new `Provider` should carry from the start.
- **Gemini bills thinking it never streams.** There is no `reasoning_content`; one reply reported 70 completion tokens against 406 total. `Usage` under-reports output by about five times until the new `Provider` reads the right field.

## 2. Schemas (`arc-proto`)

Each schema change is its own commit, separate from code that uses it (invariant 3: additive only).

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 2.1 | Record a session's role, project, and budget at creation so replay shows what a job ran with. Use the reserved model field on `ToolCallIssued`; do not add another copy. | — | todo |
| 2.2 | `arc.toml`: `[roles.*]` and `[projects.*]`. A role resolves to one provider and model. A project resolves to its read-write root, any read-only grants, and its declared builtin/workspace sources. Do not add expert configuration, fallback policy, or workflow configuration. | — | todo |
| 2.3 | Add a generic opaque per-call provider blob to `ToolCallIssued`. Gemini rejects a tool result whose call did not carry back its `thought_signature`, about 620 bytes, and the transcript is rebuilt from the log, so it cannot be provider-local state. Name it for what it is — provider round-trip data — not for Gemini. Must land before 3.3 | — | todo |

## 3. Roles and providers (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 3.1 | Resolve each configured role to a provider and model. Add the role label to every `CompletionRequest` and trace span. This is the single place where Phase 3 work is measured. | — | todo |
| 3.2 | Session pinning: role chosen at session or job creation, immutable for its lifetime | — | todo |
| 3.3 | Keyed providers: keys from `data/secrets/` (0700). `OpenAiCompat` sends no `Authorization` header at all today. Go needs nothing else — `glm-5.3` decoded every parser case unchanged, model ids are bare (`deepseek-v4-flash`, not `opencode-go/…`), and `/v1/models` exists so 3.4 can validate the allow-list at startup. Gemini gets its own `Provider`: it omits `index` on tool-call deltas, never sends `reasoning_content`, and needs 2.6's blob echoed back or the next turn is a 400 | — | todo |
| 3.4 | Prefix stability for the face: identity file and record index render first and byte-identically, everything volatile after. A regression test asserts two consecutive turns produce an identical prefix. | — | todo |
| 3.5 | **Sidecar restart policy** (carried from Phase 1). It supports the local role's consolidation work. | — | todo |
| 3.6 | **Write `data/identity.md` for the face** — ARC's register per §5.1's four rules, plus the stable facts the always-loaded prompt should carry. Not code, and by invariant 7 not something an agent may write; this one is Bogdan's. Until it exists the face runs on whatever voice the model defaults to | bogdan | todo |

## 4. Tool registry (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 4.1 | Add a registry with builtin and workspace sources. Declarations are session-scoped. Move the five memory tools without changing them. Do not add expert or MCP source plumbing. | — | todo |
| 4.2 | Session tool sources: a session gets the builtin and workspace sources its project declares, resolved once at creation and fixed for its lifetime. A call to a tool the session does not hold returns `ToolOutcome::ERROR` with the reason so the loop adapts. No runtime prompt and no durable verdict — there is no decision left to record | — | todo |
| 4.3 | Workspace tools, read-only half: `read`, `glob`, `grep`. Confinement resolves every path to canonical form and accepts it only if it sits under one of the session's granted roots, with `..`, symlinks, and absolute paths outside them rejected — tested adversarially. A grant carries a mode; the read-only half only ever needs `read`. The check lives in `resolve()`, so `glob` and `grep` walk granted roots and nothing else — a walk that skips the check leaks file contents through match output. A rejected path returns `ToolOutcome::ERROR` with the reason so the loop adapts | — | todo |
| 4.4 | Add workspace `write` and `edit`. Both refuse a path whose grant is read-only, so a session can read notes it cannot change. `edit` must match exactly one occurrence and reject a file changed since the last read. Test this rule thoroughly. | — | todo |
| 4.5 | Add `bash` with a scrubbed environment. It runs as the user with nothing between it and the filesystem; the grants are a tool-level check, not containment. A sandbox is later work. | — | todo |
| 4.6 | **Token budget re-measure** (carried, re-scoped). The Phase 2 note assumed a 16k local face; with a hosted face the ceiling changes and the question becomes which sources load into which session. Measure before 5.2 wires anything else always-on | — | todo |

The two expert invocations from spike 1.2 are retained here for the deferred expert-tool task. Both were verified read-only against a workspace on 2026-08-23. Close stdin: both CLIs block on an open one. The prompt is a positional argument. claude has no `--cd`, so set the child's working directory; codex takes `-C`. codex runs commands through `zsh -lc`, a login shell that sources the user's profile, so set `shell_environment_policy.inherit` explicitly for invariant 8.

```
claude -p "<prompt>" --model opus --fallback-model sonnet
  --tools "Read,Glob,Grep" --strict-mcp-config --setting-sources ""
  --permission-mode manual --no-session-persistence
  --json-schema '<schema>' --output-format json < /dev/null
```

```
codex exec "<prompt>" -s read-only --json --ephemeral
  -C <project root> --output-schema <schema file> -o <result file> < /dev/null
```

`--strict-mcp-config --setting-sources ""` is load-bearing. Without them the child inherits the user's MCP servers — a plain `claude -p` came back holding Gmail, Calendar, and Drive tools, none of which are read-only or inside the workspace. codex cannot be closed the same way: `--ignore-user-config` leaves `web__run` and its bundled app tools in place, so its read-only covers the filesystem and not the tool surface. Prefer claude when a diff may contain secrets. `--bare` would cut claude's prompt prefix but reads auth only from `ANTHROPIC_API_KEY`, so it cannot run on the subscription.

On 4.3 and 5.1: the workspace is a set of granted roots, not one root. Settled in DESIGN.md under workspaces and confinement, and in invariant 8, so the tasks below implement a decision rather than making one.

The reason to grant rather than deny is the same as principle 6 in `providers.md`. A deny list fails open the first time an entry is forgotten. A grant list fails closed: `~/.local/state/arc/` is unreachable because nobody granted it, not because it was banned. Do not redact tool results either — redaction is unbounded pattern-matching and it lies to the model about what it read. DESIGN.md already settles the adjacent case for `bash` by scrubbing the environment, which is the same argument: protection comes from what the tool can reach, not from a regex afterwards.

The residual risk is worth stating rather than designing around. arcd runs as the user, so the process can read those keys whatever `resolve()` says. Confinement is a check in a tool, not an OS boundary, and the file-read path has no backstop equivalent to the scrubbed environment. Closing that needs privilege separation, which is not this phase.

## 5. Jobs

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 5.1 | Workspace binding: `sessions.project` plus a set of granted roots on disk, and the rule that unbound sessions get no workspace tools. The project root is granted read-write; anything else the session should reach — notes, dotfiles, a reference checkout — is a separate read-only grant. Grants are a list of what is reachable, never a list of what is forbidden, so a path nobody granted is unreachable by construction rather than by remembering to ban it. Grants are session-scoped and belong in the log, since replay has to rebuild what a tool call was allowed to see | — | todo |
| 5.2 | Add the dispatch tool. It creates a child session with its own role, tool sources, and budget, then returns the summary later. It uses the existing tool-call recovery model. Every field is required with an explicit escape value in its enum — 1.3 measured optional fields being silently dropped, which was most of the local model's errors. The face prompt must say that recall is answered directly and that `local` is for extraction. | — | todo |
| 5.3 | Supervised job task: owns the generic loop, talks to the same `Store`, and leaves the daemon responsive. It is not a sandbox and does not add process supervision or restart semantics. | — | todo |
| 5.4 | Steering: messages to a running child, queued and processed in order | — | todo |
| 5.5 | Budget enforcement per job, in tokens and wall-clock, recorded at dispatch and checked by the task | — | todo |
| 5.6 | Handback: a summary plus the child's session id into the parent, with the full transcript staying in the archive | — | todo |

## 6. TUI

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 6.1 | TUI: refreshable job list showing live jobs, status, and budget consumed. The conversation stays usable while one runs. | — | todo |
| 6.2 | **Session titling pass** (carried). Now cheap: titles are `local`-role work and the job list needs something better than a session id to display. | — | todo |

## 7. Production

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 7.1 | `arcd rebuild` proven against the real log: drop the index, replay, diff against live state. The phase contract names this explicitly | — | todo |
| 7.2 | Installed for real: systemd user unit enabled, release binary on a stable path, runtime state surviving a rebuild of the machine. Phase 1 left the unit installed but not enabled. Move runtime state out of the repository to XDG paths — `~/.local/state/arc/` for the log, index, and secrets, `~/.config/arc/arc.toml` for config — so that arcd's own keys stop living inside a project root the workspace tools can be granted. DESIGN.md and AGENTS.md already name the installed layout; this task is the code and the migration | — | todo |
| 7.3 | Account for token use and latency by role from existing spans. Use the measurements to replace published-rate estimates in `providers.md`. | — | todo |
| 7.4 | Phase 1 leftovers: `log::Error::Io` field doc, and a retention policy for `data/traces/`. | — | todo |

## Exit criteria

Verify these live, not only in the test suite:

- [ ] A week of real development on ARC done through ARC, not through another harness.
- [ ] `arcd rebuild` reproduces live state from the log.
- [ ] A job runs to completion while the conversation stays responsive, and reports back.
- [ ] A call to a tool the session does not hold reaches the model as an error it acts on, and the declared sources survive a restart.
- [ ] The TUI can refresh a running job's status and shows its final handback.
- [ ] Measured token use and latency are visible by role in traces.

## Not in this phase

- **A sandboxed worker.** The honest replacement for the approval gate we removed, and the change that would make `bash` genuinely contained. Phase 3.1, once jobs are real enough to say what the sandbox has to allow.
- **Provider fallback ladders.** Add them only after real outage or spend data justifies their state and recovery rules.
- **Expert consultation and MCP sources.** The spike and source interface guide later work; neither is in the initial job path.
- **Built-in to-dos.** A file in the workspace is the whole feature. Do not add a tool for it.

- **Forking, rewind, tree navigation:** Phase 3.5. Wanting them now makes them next, not part of this phase.
- **Voice:** Phase 4. Nothing here may assume an audio client.
- **Devices:** Phase 5. Add an MCP source with the first real device.
- **Compaction as an event** (DESIGN.md §12): wait until a real job reaches a context limit. This schema change needs evidence.
- **Prompt v2:** wait until extraction quality warrants it. Its gate remains two pinned regression cases and `memory-replay --against`.
- **Batched log appends:** wait for traces showing that fsync per append costs enough. Jobs will provide that evidence or settle the question.
