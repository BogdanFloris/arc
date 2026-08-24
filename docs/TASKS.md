# TASKS — Phase 3, development

This is the live list. `TASKS-phase1.md` and `TASKS-phase2.md` are historical records. `DESIGN.md` defines the phase and the technical rules.

**Phase goal:** ARC becomes the harness used to write its own code and runs as an installed service. Success requires a week of real development through ARC and a full rebuild matching live state.

Bogdan assigns each task (`bogdan` or `claude`); the other reviews it. Statuses are `todo` → `in progress` → `in review` → `done`. Assignees below are intentionally unset.

Tasks are dependency-ordered; tasks at the same number may run in parallel. The phase has three parts: **substrate** (1–4: roles, registry, tools), **jobs** (5–6), and **production** (7: daily-driver operation).

**This phase is larger than Phase 2.** Once the substrate is usable, sections 1–5 satisfy the development half of the exit criterion. Move sections 6–7 to Phase 3.1 rather than expanding this phase.

## Decisions made before planning, 2026-08-22

- **Three configured roles:** concierge, executor, and archivist. Each is named for the work it does, not for where its model runs. There is no runtime difficulty classifier. `providers.md` records the current models.
- **The concrete stack to build against:** concierge = Gemini 3.7 Flash on a direct key; executor = DeepSeek V4 Pro via OpenCode Go's OpenAI-compatible endpoint; archivist = the existing Qwen3-8B sidecar. Budget target under $50/month against $100 today.
- **Roles use one model each.** Provider failure reaches the client. Fallback ladders need real outage or spend evidence before they become stateful policy.
- **Dispatch is a tool call with a delayed result.** `ToolCallIssued` starts the job. `ToolResultRecorded` carries the final summary. The existing unfinished-call recovery rule handles crashes.
- **Jobs run as supervised daemon tasks.** This keeps the conversation responsive; it is not containment. Process separation waits for a sandbox design.
- **The coding loop is generic:** send messages, run requested tools, append results, and stop. Planning and review are prompt or configuration policy until use proves they need machinery.
- **Sessions pin one provider for life.** This replaces v1's hot-swapping position because of cache economics.
- **Nothing prompts for permission.** What a project allows is configuration; a call outside it returns an error the model acts on. A per-call prompt would block the twenty-minute jobs it exists to guard. Phase 5 designs an actuator's confirmation against a real actuator instead of inheriting this.
- **This phase tests whether** a cheap model can handle most of about 19M monthly output tokens when the harness is strict. If it fails, investigate the harness first: strict `edit`, a working test loop, and rewind.

## Decisions, 2026-08-23

Taken after a read of pi, opencode, Claude Code, codex, and mini-swe-agent. Across those five the only universal core is a mutation tool plus a shell; everything above it is output shaping.

- **Four workspace tools: `read`, `write`, `edit`, `bash`.** `glob` and `grep` are cut. They are one search program with different arguments, that program is already on the machine, and DESIGN.md's rule already prefers the program that exists over a new builtin. Search moves to `bash`.
- **`edit` is string-replace, not a patch format.** Codex is the only harness on patch and it is OpenAI-only. No spike; follow the field.
- **Web is a builtin source, not a CLI.** It is the one capability a session needs before it can run anything, which is the exception the CLI rule already names. It keeps the shell out of the concierge, which is the session most exposed to untrusted text, and it keeps the search credential inside arcd.
- **Dispatch names a configured project.** A model selects from projects a human wrote; it never composes roots and modes. A voice session has no working directory because it is unbound and has no filesystem.
- **An actuator gets its own dispatch.** Starting a print or activating a system generation is confirmed at a turn boundary, not by a mid-turn prompt. This is prompt and configuration, not machinery.
- **A home-wide grant waits for the sandboxed worker.** OS-level change goes through a project over the Nix configuration instead, with `nixos-rebuild build` unprivileged inside the job and `switch` as its own dispatch.

## 1. Spikes (before schemas harden)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 1.1 | Test OpenCode Go and Gemini with the existing `provider/openai` parser. Confirm that Go's SSE output matches the Phase 1 fixtures. Decide whether Gemini's OpenAI-compatible API is sufficient or needs its own `Provider`. Deliver fixtures and a written decision. **Go passes unchanged; Gemini needs its own `Provider`, because a tool result cannot be fed back without echoing an opaque per-call `thought_signature` that has to survive replay.** | claude | done |
| 1.2 | Test both `consult_expert` modes. Run `claude -p` in a project with read-only tools and prove that read-only access is enforced. Measure a real plan request and a real diff review: latency, usable severity labels, and any usage signal in headers, exit status, or stderr. Deliver a decision and two argv templates. **Read-only is enforced on both `claude -p` and `codex exec`, and severity labels and usage come back structured. Keep these templates for a later expert-tool task; they are not part of the initial job path.** | claude | done |
| 1.3 | Test whether Flash chooses the right role, project, and brief. Score 20 scripted requests for wrong role, wrong project, and malformed brief. Use the result to decide whether dispatch needs a stronger model. **Flash is good enough — 0/20 wrong project and 0/20 bad briefs — provided every dispatch field is required with an explicit escape value, since optional fields get dropped.** | claude | done |

1.3 came back reliable, so dispatch stays on Flash. The findings that bind later tasks are folded into the rows below. Two remain unfiled:

- **Gemini caching is unverified.** No probe carried a prefix worth caching, so no cached tokens appeared in `usage`. Re-check with a real concierge prompt before trusting the 90% discount in `providers.md`. Explicit caching needs `extra_body.cached_content`, which the new `Provider` should carry from the start.
- **Gemini bills thinking it never streams.** There is no `reasoning_content`; one reply reported 70 completion tokens against 406 total. `Usage` under-reports output by about five times until the new `Provider` reads the right field.

## 2. Schemas (`arc-proto`)

Each schema change is its own commit, separate from code that uses it (invariant 3: additive only).

These are schemas and nothing writes a non-default value yet. 3.1 fills in the role, 5.1 the project, 5.2 the budget, and 3.3 the round-trip blob; the projection's `sessions.project` column stays NULL until then. The top-level `provider` and `model` keys are still the live path, and 3.1 replaces them with role resolution.

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 2.1 | Record a session's role, project, and budget at creation so replay shows what a job ran with. Use the reserved model field on `ToolCallIssued`; do not add another copy. **`SessionCreated` fields 7, 8, 9: `SessionRole`, `project`, and an optional `Budget` of total tokens and wall-clock seconds. Absent budget means no budget.** | claude | done |
| 2.2 | `arc.toml`: `[roles.*]` and `[projects.*]`. A role resolves to one provider and model. A project resolves to its read-write root, any read-only grants, and its declared builtin/workspace sources. Do not add expert configuration, fallback policy, or workflow configuration. **The three role keys are fixed, so a typo is a load error rather than a silent extra role. Grants must be absolute — `~` is not expanded — and a read-only grant may not sit inside the read-write root, because a grant is a separate root and not a hole.** | claude | done |
| 2.3 | Add a generic opaque per-call provider blob to `ToolCallIssued`. Gemini rejects a tool result whose call did not carry back its `thought_signature`, about 620 bytes, and the transcript is rebuilt from the log, so it cannot be provider-local state. Name it for what it is — provider round-trip data — not for Gemini. Must land before 3.3 **Landed as `bytes provider_roundtrip = 9`.** | claude | done |

## 3. Roles and providers (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 3.1 | Resolve each configured role to a provider and model. Add the role label to every `CompletionRequest` and trace span. This is the single place where Phase 3 work is measured. **Roles resolve once at startup and roles sharing an endpoint share one client. An unconfigured role falls back to the sidecar, so an empty config still runs. A role configured for gemini is a startup error until 3.3, rather than silently taking the OpenAI-compatible path that spike 1.1 proved wrong. The daemon-wide `provider` key is gone; `data/arc.toml` names the three roles instead, and the running release binary predates it.** | claude | done |
| 3.2 | Session pinning: role chosen at session or job creation, immutable for its lifetime. **An engine refuses to continue a session whose recorded role is not its own, before anything is appended; the client gets `role_mismatch`. The sessions table gained a `role` column, so the index rebuilds on next start. Sessions logged before roles exist carry UNSPECIFIED and stay unpinned.** | bogdan | done |
| 3.3 | Keyed providers: keys from `data/secrets/` (0700). `OpenAiCompat` sends no `Authorization` header at all today. Go needs nothing else — `glm-5.3` decoded every parser case unchanged, model ids are bare (`deepseek-v4-flash`, not `opencode-go/…`), and `/v1/models` exists so 3.4 can validate the allow-list at startup. Gemini gets its own `Provider`: it omits `index` on tool-call deltas, never sends `reasoning_content`, and needs 2.3's blob echoed back or the next turn is a 400 **Confirmed against the live endpoint, and two prose findings were wrong. `index` is missing on the tool-call object, not the choice, and the signature sits at `tool_calls[].extra_content.google.thought_signature` as base64 text that goes back verbatim — decoding it would only add a way to corrupt it. A plain text turn carries a signature too, but only function calls need one echoed: without it the reply is HTTP 400 `Function call is missing a thought_signature in functionCall parts`, not a degraded answer. Usage under-reports output because thinking is billed and never streamed, so output is `total - prompt` (measured 77 total against 10 prompt and 2 completion). Fixtures are in `arc-core/tests/fixtures/gemini_*.sse` and the live round trip is an `#[ignore]`d test. Go's base URL in `arc.toml` is `https://opencode.ai/zen/go`, one segment short of the published `/v1`, because arcd appends the version; config rejects the longer form rather than 404ing at the first turn.** | claude | in review |
| 3.4 | Prefix stability for the concierge: identity file and record index render first and byte-identically, everything volatile after. A regression test asserts two consecutive turns produce an identical prefix. | — | todo |
| 3.5 | **Sidecar restart policy** (carried from Phase 1). It supports the archivist's consolidation work. | — | todo |
| 3.6 | **Write `data/identity.md` for the concierge** — ARC's register per §5.1's four rules, plus the stable facts the always-loaded prompt should carry. Not code, and by invariant 7 not something an agent may write; this one is Bogdan's. Until it exists the concierge runs on whatever voice the model defaults to | bogdan | todo |

Thinking is a per-role setting, not a global. `no_think` was a Qwen prompt hack sitting in front of every provider, and the sidecar's model alias was a top-level `model` key that meant nothing to a hosted role; both moved to where they apply, `[roles.*].thinking` and `[llama].model`. Each provider expresses the level its own way: Gemini sends `reasoning_effort`, the sidecar gets `/no_think` appended by the one code path that knows it is talking to the sidecar, and an `openai_compat` role rejects the setting at load because no endpoint there is measured to accept the field and an unknown field is a 400.

`minimal` is the level that stops thinking, and having it is a property of the model rather than of the API: 3.6, 3.5 and 3-flash-preview accept it, while 3.7 Flash rejects it identically on the native and OpenAI-compatible paths. The concierge therefore runs **3.6 Flash on `minimal`** — five runs of one chat turn gave 28–33 output tokens against 31–337 for 3.7 on `low`, and the live round-trip test ran in a fifth of the wall time. `low` is a cap the model may use rather than a level it obeys, so it is bimodal and unreadable from a single turn; an earlier three-run pass reported a clean ladder and did not survive more samples. Treat every thinking number as a distribution.

Nothing in config can catch `minimal` against a model that lacks it — it is a 400 at the first turn, not a load error. 3.4's `/v1/models` allow-list is the place to close that if it becomes a habit. `extra_body.google.thinking_config.thinking_level` reaches the same control and the two are mutually exclusive; `reasoning_effort` wins because it is flat and leaves the `extra_body.google` envelope free for the `cached_content` that caching still needs. Caching is still unverified: nothing has measured `cached_tokens` on a real concierge prefix.

## 4. Tool registry (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 4.1 | Add a registry with builtin, web, and workspace sources. Declarations are session-scoped. Move the five memory tools without changing them. Do not add expert or MCP source plumbing. | — | todo |
| 4.2 | Session tool sources: a session gets the builtin and workspace sources its project declares, resolved once at creation and fixed for its lifetime. A call to a tool the session does not hold returns `ToolOutcome::ERROR` with the reason so the loop adapts. No runtime prompt and no durable verdict — there is no decision left to record | — | todo |
| 4.3 | Workspace `read`, and the confinement it rests on. `resolve()` canonicalises every path and accepts it only under one of the session's granted roots, with `..`, symlinks, and absolute paths outside them rejected — tested adversarially. A grant carries a mode; `read` only ever needs the read one. The check lives in `resolve()` so every path-taking tool shares one gate. `read` caps and paginates, since it is what keeps a large file from spending the executor's context in one call. A rejected path returns `ToolOutcome::ERROR` with the reason so the loop adapts | — | todo |
| 4.4 | Add workspace `write` and `edit`. `edit` is string-replace. Both refuse a path whose grant is read-only, so a session can read notes it cannot change. `edit` must match exactly one occurrence and reject a file changed since the last read — that rule is why `read` stays a tool, since a file read through `cat` gives it nothing to compare against. Test this thoroughly. | — | todo |
| 4.5 | Add `bash` with a scrubbed environment. It is now the search and listing path, which makes two things load-bearing: a deterministic output cap with a timeout, and a `PATH` in the scrubbed environment that carries the search tool, git, and the toolchain. Scrubbed is not empty — Nix and cargo also need `HOME`, `USER`, and the `XDG_*` vars, or builds fail in ways that read as bugs. `bash` runs as the user with nothing between it and the filesystem; the grants are a tool-level check, not containment. A sandbox is later work. | — | todo |
| 4.6 | Web source: `web_search` and `web_fetch`. Read-only, no grants, available to unbound sessions. arcd caps the output of both — a fetched page is unbounded text heading into a small model — and holds the search credential, so no tool process ever sees it. `web_fetch` returns markdown. Treat everything either returns as untrusted text | — | todo |
| 4.7 | **Token budget re-measure** (carried, re-scoped). The Phase 2 note assumed a 16k local concierge; with a hosted one the ceiling changes and the question becomes which sources load into which session. Measure before 5.2 wires anything else always-on | — | todo |

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

On 4.5: `rg` is on erebor but not in `flake.nix`'s devshell. Routing search through `bash` makes that a dependency rather than a convenience, so it goes in the devshell in the same change as the `PATH` the tool hands its children.

On changing the machine: a project over `~/dotfiles` plus one sudoers entry for `nixos-rebuild switch` reaches the whole system without a home-wide grant, and every change is a reviewable diff with a generation to roll back to. Two rules make it hold. The rule that grants the power cannot live where the power reaches, so the sudoers entry and arcd's own unit and `arc.toml` stay outside any granted root — otherwise one rebuild widens the grant to `ALL`. And `switch` is a separate dispatch, because a mid-uptime switch leaves the running kernel on the old generation and the mismatch surfaces at the next reboot. `build` and `--dry-activate` need no privilege at all, so the job iterates freely and only activation needs a yes.

On 4.3 and 5.1: the workspace is a set of granted roots, not one root. Settled in DESIGN.md under workspaces and confinement, and in invariant 8, so the tasks below implement a decision rather than making one.

The reason to grant rather than deny is the same as principle 6 in `providers.md`. A deny list fails open the first time an entry is forgotten. A grant list fails closed: `~/.local/state/arc/` is unreachable because nobody granted it, not because it was banned. Do not redact tool results either — redaction is unbounded pattern-matching and it lies to the model about what it read. DESIGN.md already settles the adjacent case for `bash` by scrubbing the environment, which is the same argument: protection comes from what the tool can reach, not from a regex afterwards.

The residual risk is worth stating rather than designing around. arcd runs as the user, so the process can read those keys whatever `resolve()` says. Confinement is a check in a tool, not an OS boundary, and the file-read path has no backstop equivalent to the scrubbed environment. Closing that needs privilege separation, which is not this phase.

## 5. Jobs

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 5.1 | Workspace binding: `sessions.project` plus a set of granted roots on disk, and the rule that unbound sessions get no workspace tools. The project root is granted read-write; anything else the session should reach — notes, dotfiles, a reference checkout — is a separate read-only grant. Grants are a list of what is reachable, never a list of what is forbidden, so a path nobody granted is unreachable by construction rather than by remembering to ban it. Grants are session-scoped and belong in the log, since replay has to rebuild what a tool call was allowed to see | — | todo |
| 5.2 | Add the dispatch tool. It creates a child session with its own role, tool sources, and budget, then returns the summary later. The project is named from the configured set, never composed by the model; with none named it lands in a standing scratch project and the handback says where it went. It uses the existing tool-call recovery model. Every field is required with an explicit escape value in its enum — 1.3 measured optional fields being silently dropped, which was most of the local model's errors. The concierge prompt must say that recall is answered directly and that `archivist` is for extraction. | — | todo |
| 5.3 | Supervised job task: owns the generic loop, talks to the same `Store`, and leaves the daemon responsive. It is not a sandbox and does not add process supervision or restart semantics. | — | todo |
| 5.4 | Steering: messages to a running child, queued and processed in order | — | todo |
| 5.5 | Budget enforcement per job, in tokens and wall-clock, recorded at dispatch and checked by the task | — | todo |
| 5.6 | Handback: a summary plus the child's session id into the parent, with the full transcript staying in the archive | — | todo |

## 6. TUI

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 6.1 | TUI: refreshable job list showing live jobs, status, and budget consumed. The conversation stays usable while one runs. | — | todo |
| 6.2 | **Session titling pass** (carried). Now cheap: titles are `archivist` work and the job list needs something better than a session id to display. | — | todo |

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

- **A sandboxed worker.** The honest replacement for the approval gate we removed, and the change that would make `bash` genuinely contained. Phase 3.1, once jobs are real enough to say what the sandbox has to allow. A grant over the whole home directory waits for it: arcd's own keys live under `~/.local/state/arc/`, so a wide grant puts them back inside a project root, and an exclusion list cannot help when the shell never consulted one.
- **Provider fallback ladders.** Add them only after real outage or spend data justifies their state and recovery rules.
- **Expert consultation and MCP sources.** The spike and source interface guide later work; neither is in the initial job path. Web is not on this list any more: it is a source in 4.6.
- **Built-in to-dos.** A file in the workspace is the whole feature. Do not add a tool for it.

- **Forking, rewind, tree navigation:** Phase 3.5. Wanting them now makes them next, not part of this phase.
- **Voice:** Phase 4. Nothing here may assume an audio client.
- **Devices:** Phase 5. Add an MCP source with the first real device.
- **Compaction as an event** (DESIGN.md §12): wait until a real job reaches a context limit. This schema change needs evidence.
- **Prompt v2:** wait until extraction quality warrants it. Its gate remains two pinned regression cases and `memory-replay --against`.
- **Batched log appends:** wait for traces showing that fsync per append costs enough. Jobs will provide that evidence or settle the question.
