# ARC — Autonomous Robotic Core

This is the architectural authority. Amend it before changing a contract.
[Architecture diagram](diagrams/arc-architecture.excalidraw); [live tasks](TASKS.md).

## 1. What ARC is

ARC is a personal assistant: an always-on Rust daemon, thin clients, durable memory, and replaceable LLM providers.

- Inside a configured project, the user develops directly with a bound coding session.
- Elsewhere, an unbound conversation handles talk, recall, and dispatch.
- Either session can delegate independent or away-from-keyboard work to jobs.

Priorities: durability, observability, speed, provider independence. v1 excludes multi-user hosting, plugin sandboxing, and robotics code. Voice and devices are later phases.

## 2. Repository layout

| Crate | Owns |
| --- | --- |
| `arc-proto` | Serialized formats: `.proto` schemas and generated types in `arc.v1`. The trimmed upstream `perfetto.proto` keeps its package and field numbers and is never logged. |
| `arc-core` | Logic: log, projections, providers, sessions, tools, memory, tracing. Testable without a daemon. |
| `arcd` | Composition: log ownership, WebSocket, supervised turns, credentials, sidecar, background work. |
| `arc` | TUI client. |
| `arc-voice` | Phase 4 audio client. |

A future mobile client uses the same protocol and depends only on `arc-proto`.

## 3. The event log

The log is the source of truth for durable state except the human-owned identity file. `data/log/` holds length-prefixed protobuf events with CRC32 payload checks.

1. Durable changes append events; nothing rewrites old bytes.
2. SQLite, memory, and session trees are deterministic projections, rebuildable from the log.
3. Schemas are additive. Never renumber or repurpose fields. Older binaries can skip unknown kinds within a payload arm, but an unknown top-level `Event.payload` arm decodes as corruption. Replay needs a binary supporting the newest payload arm in the log.

**Segments and recovery**

- Names use the first sequence number, padded to 20 digits. A replacement for a segment that died on its first record takes a `_1`, `_2`, … suffix.
- Creating a successor seals the preceding segment; there are no seal markers.
- Recovery leaves torn tails untouched and starts a new segment at the recovered sequence number. Replay checks gapless sequence numbers from 0 across every boundary.
- An encoded event is capped at 16 MiB.
- v1 fsyncs each append. Sequence numbers are assigned internally; written but unsynced records keep their numbers. A failed writer is rebuilt, never retried. Batching waits for evidence; any future coalescing must flush before turn completion and preserve these rules.

Backup needs the segments and identity file, not SQLite.

### 3.1 Tool-call events

`ToolCallIssued` and `ToolResultRecorded` are separate `SessionEvent` kinds. `MessageAppended` holds displayed text, not calls. Schemas live in `arc-proto`, not duplicate definitions here.

**Identity and ordering**

- A user message creates a `turn_id`; all events in that turn carry it. There are no turn-start/end events. Legacy empty turn IDs mean one message per turn.
- Filter by session and turn before grouping steps. Assistant text and following calls, before any result, form a step.
- Each call has a dense step-local `index` starting at 0. Streaming parsers key parallel calls by index.
- `call_id` is unique across the whole session. Keep the provider's ID unless absent or already logged; mint a replacement in those cases and persist the ID actually used. Replay never regenerates it.
- Results name only `call_id`; tool name is joined from the call. Calls have source MODEL, results SYSTEM.
- Persist and fsync the entire call batch before executing any call. Results append in completion order; provider transcripts order them by call index.
- Reasoning is streamed, never durable. Tools-only steps append no empty assistant message.

**Crash recovery.** A durable call without a durable result has UNKNOWN outcome: it may have run. Never silently retry or drop it. At startup, before dispatching work, arcd closes each orphan with one SYSTEM `ToolResultRecorded{UNKNOWN}` explaining that the daemon restarted and the call may have run. Readers and rebuild append nothing. A live in-flight call is not an orphan.

Recovery does not restart model turns. The next user message resumes the conversation. Automatic retry of idempotent UNKNOWN calls remains open; tools have no idempotence declaration.

**Errors and limits**

- Bad arguments, tool failures, timeouts, missing tools, and denied access are ERROR results; the model sees them and can continue.
- Provider, framing, and log-write failures go only to the client. Failed appends abandon the turn.
- If the model sees it, it is durable; a user-only failure is a wire `Error`.
- Unknown outcome values read as UNKNOWN, never ERROR.
- Cut text uses `MessageAppended.partial`. Calls are logged only with complete arguments; results arrive whole. Cut tool loops use orphaned calls, not `partial`.
- The registry caps results before event construction (`max_tool_result_bytes`, initially 32 KiB), marks truncation, and stores exactly what the model saw. The event cap is a backstop.
- Credential-using tools return references, not values. Keep credentials out of results before logging; post-hoc regex redaction is not the security boundary.

The projection stores calls/results, turn IDs, outcomes, partial/truncated flags, and tool names. FTS indexes result content but excludes it from default archive search; arguments are not indexed. Revisit that default with real queries.

### 3.2 One writer, many readers

`store::Store` owns log and index and exposes the only append path. Reads use a separate read-only `projection::Reader` connection in WAL mode.

Do not hold the engine lock over a model call. Background work snapshots, releases the lock, runs the model, then rechecks eligibility before committing (§5.4).

## 4. Sessions, jobs, and tools

Sessions form a tree through `parent_session` and `fork_point`. A fork is a new branch; rewind forks at an earlier point. The projection stores parent pointers and clients render the tree. Mainline sessions and branches marked *real* feed memory extraction; abandoned branches remain archive-searchable.

A cut reply is logged with `partial = true`. Client errors are not archived as messages.

### 4.1 Jobs

A job is a child session with its own role, tools, and budget, not a separate transcript store or runner. It gets ordinary archive, replay, fork, and rewind semantics.

- Every session runs as a supervised task in arcd, with one live turn per session. Connections subscribe and may disconnect without stopping the turn.
- Dispatch durably creates the child and returns its ID immediately. The supervisor starts it after the tool result is durable, without waiting for the parent's turn to end. Continue/cancel take effect at the same boundary.
- User messages, parent steers, and child handbacks reach live turns at the next step boundary. Into an idle session, they start a turn.
- Jobs cannot dispatch, continue, or cancel other jobs. The tree below a user-opened session is one level deep.
- Jobs stay pinned to role and provider (§6.1).
- Budget enforcement remains dormant during daily-use calibration; dispatch does not ask the model for budgets. Restore enforcement only when spend data supports useful limits.

**Handbacks.** Completion appends a SYSTEM message to the parent with the child's summary and session ID, separate from dispatch's immediate result. The parent reads it in its current or a new turn. Pending handbacks may share a turn; fifty consecutive system-started turns without user input bound pathological loops. Crash recovery still never restarts turns (§3.1).

The parent receives a summary, not the transcript, and can inspect the child's archive. Physical actions may use a plan-only job followed by a user-requested action job; which actions need that split belongs in prompts/configuration, not mid-turn permission machinery.

**Footprints.** Successful `write`, `edit`, and `apply_patch` operations record canonical `changed_paths` independently of capped result text, including completed operations before a later patch failure. Handbacks read only the completed turn's projected paths. These prove operations, not exclusive authorship: Bash, external writers, and interrupted operations without durable results remain unattributed. Repository diffs and commits are separate, workspace-wide observations.

**Coding prompts.** Direct sessions do the work themselves and dispatch only independent or away work. Stop on whole-task handoff; continue independent work on partial handoff. Give self-contained briefs and verify handbacks. Assign disjoint files; format only after writers stop. Build a small end-to-end slice, run focused tests, wait for ready handbacks before checks spanning child files, then run the full suite after integration. These are versioned harness preambles, not identity or injected memory. Planning/review/retry workflows remain prompts or configuration until use proves they need machinery.

Children report formatting needed unless explicitly assigned integration with exclusive workspace access. Reread formatted files before editing; never bypass freshness checks.

### 4.2 Workspaces

A bound session records its configured project and grants durably. The project root is read-write; additional roots are read-only grants. `/tmp` is always read-write scratch for bound sessions, including analyze jobs; this is tool policy, not a stored grant.

Only a human edits project configuration. Models select configured projects, never author roots or modes. Grants list reachable roots, not forbidden paths. Unbound conversation and voice sessions have no workspace tools.

### 4.3 Tools, sources, and containment

Session-scoped sources determine both advertised and executable tools:

- **Builtin:** memory and archive (§5.5).
- **Jobs:** dispatch, continue, cancel; only in user-opened sessions.
- **Web:** provider-hosted search/grounding where supported, otherwise empty.
- **Workspace:** `read`, `bash`, and a pinned editing interface for bound sessions.

MCP/device sources wait for the phase that needs them.

Codex defaults to `apply_patch`; other providers to `edit` and `write`. Presets or inline roles may override with `editing = "patch"` or `"replacement"`. Record the choice at session creation. Legacy sessions use their pinned provider, or the serving provider if unpinned.

**File tools** canonicalize paths through the shared resolver and require a grant or `/tmp`; symlinks cannot escape that gate. Writes additionally require a writable grant. Existing-file edits require a fresh read in the same session.

`edit` accepts unique, non-overlapping replacements against the original file; the single-replacement legacy shape remains valid. `apply_patch` preflights all hunks before writing. Filesystem failures during sequential writes can leave a partial patch; results name completed paths.

**Bash is not sandboxed.** It starts at the project root with a scrubbed environment but runs as the user and can reach beyond grants. Grants are advisory in shell-bearing sessions. A whole-home grant waits for a sandboxed worker. Nothing prompts for permission mid-turn; out-of-grant file operations return tool errors.

Prefer workspace CLIs over new builtins. Search uses Bash; preserve output caps and a usable scrubbed `PATH`. `read` supplies pagination and the freshness anchor.

Web is provider-native so unbound sessions need no shell or search credentials. Switching providers can lose that capability. Clients must satisfy provider attribution requirements; an audio-only client must resolve that before using grounded answers.

### 4.4 Compaction

Compaction changes the provider transcript, never history.

- `SessionCompacted` stores covered sequence, summary, prompt version, and model. Replay substitutes the summary through that sequence and retains the rest verbatim, without another model call. Archive and TUI history remain complete.
- Trigger from the latest step's reported prompt tokens against a fraction of the configured window, never cumulative turn usage.
- The selected archivist writes the summary without changing the session pin. Trace model, usage, and response byte counts, not response text.
- Accept nonempty summaries within 32 KiB; headings are optional. Oversized drafts get one bounded shrink call containing the draft and limit, not history. Empty text or failed repair ends the turn visibly and appends no compaction. Never fall back to the session model.
- Summarize older user requirements. Append at most two newest original user messages covered by compaction, whole and ordered, when their contiguous tail fits 8 KiB. Apply this across ancestry and repeated compactions; never copy partial messages.
- Keep the latest two user exchanges outside compaction when possible. Long exchanges may compact through completed tool batches, keeping the latest two batches whole. Never split a call from its result.
- Recheck each step; repeated compaction requires an advancing cutoff.
- Forks before the event inherit the full prefix; forks after inherit the summary.
- `:compact` appends the same event manually.

Each compaction inherently pays one prompt-cache miss.

### 4.5 Picture attachments

Pictures are durable message content, not file references. `MessageAppended` stores media type, display name, and bytes. Projection, forks, and provider transcripts carry them until compaction covers the message; summaries carry no stale attachment reference.

`:attach <path>` stages a validated local PNG, JPEG, or WebP and displays `[image: name]`; `:attach clear` clears it. Failed sends preserve pending pictures. Per-message bytes stay below the event cap. The daemon revalidates before append and refuses unsupported pinned providers before writing the turn.

Provider support is explicit. Codex sends `input_image` data URLs alongside `input_text`, omitting optional `detail` to match its Responses Lite builder. Other providers reject pictures until implemented. Inline previews, clipboard input, generated pictures, and model-side image reading are outside this slice.

**Session status.** The header shows the latest completed step's context reading. `ContextMeasured` durably records input tokens, window, and threshold. Forks start unmeasured; compaction invalidates old readings. Codex allowance is transient telemetry shared by credential, with short caching and bounded waits; stale/unknown stays explicit. Status refreshes independently of streaming; allowance failures never fail turns. Reset times and observation age appear in `:status`. Never log credentials or raw account responses.

## 5. Memory

### 5.1 Identity

`data/identity.md` is small, human-owned, exempt from event-sourcing, and backed up beside the log. ARC may propose edits in output but never writes it; `IdentityEvent` remains reserved.

Load identity wherever the user is present: chat, direct code sessions, user follow-ups inside finished jobs. Dispatched jobs get no personality preamble. Presence, not role, is the boundary.

The voice is direct: answer first, no enthusiasm scaffolding, plain disagreement, warmth through attention rather than adjectives. Tool-coupled operational rules belong in harness preambles, not identity.

### 5.2 Distilled tier

Flat records use kind, namespace, title, one-line summary, Markdown body, links, and provenance. `arc-proto` owns their schema.

`MemoryEvent`s project current state. Context carries only ACTIVE records' namespace, kind, title, and summary; bodies require tool reads. Superseding preserves history. A user-requested DELETED event removes a record from the projection, not the append-only log.

Every fact must point to the sessions where it was learned.

### 5.3 Archive tier

SQLite (`data/index.db`, bundled rusqlite) projects the session tree, messages, and FTS5 content index. Distilled memory answers what ARC knows; the archive answers what was said.

Embeddings via sqlite-vec wait until FTS proves insufficient and can be added by replay.

### 5.4 Write pipeline (consolidation)

Explicit requests use memory tools immediately. After a session goes idle, an asynchronous archivist pass extracts durable facts, merges records, and supersedes contradictions.

**Eligibility and dedup**

- Mine user-opened sessions at either door; dispatched jobs are titled and marked consolidated, not mined for user facts. Recorded creation source governs; legacy unspecified sources retain the old role gate.
- Mainline sessions and branches marked *real* consolidate. Branches contribute only their own rows, not inherited history. Presence and branch gates both apply.
- Show identity as already-known context; elide recalled memory/archive tool results so injected facts are not learned again.
- Show a bounded selection of existing records relevant to the user's messages. One extraction call chooses write, supersede, or nothing; exact-normalized duplicates and unchanged supersedes are dropped in code. Ambiguous near matches go to review, not another model call.

**Commit.** `SessionConsolidated{session_id, through_seq, prompt_version}` records coverage even when nothing was extracted. Eligibility is a query over idle time and rows after the latest marker, not daemon memory.

Snapshot under the engine lock, run the model unlocked, then recheck idleness and unchanged input before appending records and marker together. New activity discards the pass. Extractor writes have source SYSTEM.

**Review.** Weekly TUI review checks created/superseded records; corrections supply examples for precision and recall. Trace created-per-session, supersede rate, and retrieval use to detect hoarding or missed memory.

Review verdicts have source USER: accept appends `MemoryRecordReviewed`, fix supersedes, delete appends deletion. `changed_at`/`reviewed_at` selects records changed in the window and not reviewed since. Fixing prefills a conversational instruction; the UI never mutates memory directly. See [Hermes notes](prior-art-hermes.md) for curation lessons.

### 5.5 Retrieval

Memory is tools, not silent RAG: `memory_read`, `memory_search`, `memory_write`, `memory_supersede`, `sessions_search`, and `session_read`. Nothing is automatically injected except identity and the record index.

Search cheaply, then read targeted context. Archive ranges cap at 8 KiB of JSON and 20 messages. Continuations carry sequence and UTF-8 byte offset; preserve the original end sequence. Long messages paginate losslessly; search previews and bookends stay clipped. Stop once answered. Trace retrieval like any tool call.

## 6. Providers

Reasoning providers implement `Provider` in `arc-core`. Normalize tool-calling and system-prompt differences there, not in clients.

### 6.1 Roles

| Role | Purpose |
| --- | --- |
| `chat` | Conversation, recall, dispatch; latency, voice, vision, judgment. |
| `code` | Interactive development; judgment and collaboration. |
| `executor` | Delegated work; cost per completed task. |
| `archivist` | Extraction, classification, titling, compaction; quality and bulk cost. |

Routing is static configuration, not a difficulty classifier. Requests and spans carry the role.

**Menus and pins**

- `[models]` presets and role `choices` define allowed models. First choice is default until `RoleModelSelected` records another; removed defaults fall back to the first configured choice.
- Defaults affect new sessions and forks, never open sessions. Sessions record role, preset, provider, and model. Missing presets/credentials or pin mismatches fail explicitly, without provider fallback.
- Forks keep role but take an explicitly requested preset or the current default, paying one cache miss.
- New direct development uses `code`; dispatch uses `executor`. Omitted `roles.code` inherits the executor menu, not its durable selection. Legacy interactive executor sessions keep their role.
- Legacy sessions without presets resolve by provider/model only when unambiguous. Sessions predating roles remain unpinned.

Cache reads dominate long sessions; model swaps would repay the prefix. Change models through a new session or fork. Add fallback policy only when outage/spend evidence requires it.

### 6.2 Transport and credentials

Local inference uses a supervised llama.cpp `llama-server` over OpenAI-compatible HTTP/SSE. Start it only when a configured role/choice uses local inference; omitted roles retain local defaults. Hosted-only configuration starts no sidecar. Idle sleep releases device memory. Other compatible servers use the same adapter by configuration.

Hosted providers use reqwest/rustls HTTP/SSE, never vendor SDKs. Auth is replaceable: API keys plus the single Codex OAuth exception in [provider principle 2](providers.md#1-principles). `arcd login codex` writes under `data/secrets/`; refresh updates credentials without logging them. Secret storage is mode 0700 and excluded from backups.

The log records the model that actually ran.

### 6.3 The concrete stack is dated

[providers.md](providers.md) holds dated configuration, measurements, and review triggers. Keep model prices and plan choices out of the architecture.

## 7. Wire protocol and clients

`wire.proto` defines protobuf over localhost WebSocket. Remote access uses Tailscale; v1 adds no tunnel, TLS termination, or auth beyond a local token. Clients hold no durable state.

Send with empty session ID to create a session. Clients stream text/tool events and query history, metadata, jobs, and status. A local launch under a configured root opens a pending code session using the canonical longest root prefix; other directories and remote launches open chat. Each door remains reachable from the other.

**TUI**

- Empty conversations have a masthead; work has a compact door/title/recorded-model header. Herdr sends the title as pane metadata and omits only that title from ARC's header.
- The session model menu selects presets for creation or forks under them. Role-default selection is separate and never relabels an open session.
- Normal-mode Tab switches remembered chat/code sessions with separate drafts. `:chat` opens chat; bare `:code` opens projects. Finish or stop streaming before switching.
- Ctrl-o toggles all thoughts/tools, including new blocks, in Insert/Normal/Visual modes. Job handbacks stay collapsed. No individual folding, inspector, or footer flag.
- At bottom, toggling follows bottom. Scrolled up, preserve the top visible block/offset; collapsing details anchors to their summary. Remember details per session until restart.
- Expanded tools separate readable inputs, retained output, and completion status. No second display truncation.

**Titles.** Background titling follows completed exchanges independently of memory's idle gate. Use bounded opening/recent conversation, excluding system handbacks, to name the concrete task. Retain saved titles; retry greetings after conversation advances. Deduplicate unchanged input, reject stale results, and notify clients on `SessionTitled`. Never change pins or memory eligibility.

`arc` exercises the protocol first; local UDS remains a desired transport. `arc-voice` owns audio devices and wake/mute controls, not reasoning or durable state. Backend adapters live in arc-core, credentials in arcd. Phase 4 defines audio/control schemas; the text wire is not assumed to support full-duplex audio unchanged. Mobile follows two stable clients.

### 7.1 Replaceable voice backends

Phase 4 intent only: prefer natural full-duplex speech without giving the voice backend authority over ARC.

- `arc-voice` ↔ arcd voice adapter ↔ cloud/local speech backend. ARC sessions own reasoning, memory, tools, and dispatch.
- Backends handle listening, speaking, timing, and interruptions. They may acknowledge/pass requests, not execute tools, write memory, or promise unaccepted actions.
- Interrupting playback is not cancelling work. Voice begins unbound; the runner routes corrections.
- Requests, answers, corrections, and spoken wording must be durable. Define generated/played/interrupted speech before schemas; transcripts alone prove no playback. Audio retention is separate and undecided.
- Reconnect restores ARC history. Replacing voice backends never replaces the pinned reasoning model.
- Waiting runs local wake detection only. Wake/button opens audio with a cue; active follow-ups need no wake word. Stop/button/timeout closes audio; mute disables capture. Closing audio leaves sessions/jobs running; handbacks never reopen the mic. Timeout and extended-conversation mode remain prototype choices.

GPT-Live-1 is the first cloud candidate, not a dependency or verified integration. Prototype: start a job, interrupt/correct it, close/reopen audio, compare job state, speech, and history. Measure delegation, playback accounting, reconnects, latency, billing, and local resource contention against a local full-duplex candidate.

Keep local ASR → ARC → local TTS as the simpler fallback if full-duplex cannot preserve contracts. Starting candidates are whisper.cpp/Silero VAD and Kokoro behind replaceable interfaces. Offline operation must visibly degrade to a local path without silently swapping an existing pin. This remains an exit requirement, not implemented policy.

Choose the speaking voice through use, separately from writing voice. Record backend, voice ID/version/settings; local assets live under `data/`.

## 8. Observability: Perfetto

Trace LLM calls, tools, memory operations, consolidation, and jobs. Role, job ID, latency, and token use diagnose live work. Richer cost attribution waits for trace evidence.

## 9. Robotics (future)

Phase 5 adds separate device MCP servers, first an ESP32 pan-tilt and later an arm. The LLM requests high-level actions; firmware owns limits, speed, and e-stop. No direct model motor control.

Design confirmation against the real actuator. UNKNOWN outcomes cannot be retried as failures (§3.1). No robotics code before Phase 5.

## 10. Security, backup, and running

- `arcd/arcd.service` is an always-on systemd user unit. SIGTERM stops cleanly; the control group kills the sidecar on any daemon exit.
- Runtime state stays in one data directory: checkout `data/`, installed `~/.local/state/arc/`. Installed config is `~/.config/arc/arc.toml`.
- Rustic backs up `log/` and `identity.md` with repository encryption. Exclude rebuildable index/traces and credentials.
- Credentials use OS keychain or protected secrets storage under the data directory. Never include them in log, traces, fixtures, or backups.
- Workspace tools inherit a scrubbed environment, not arcd's credentials. This does not sandbox Bash or prevent access to files the user can read (§4.3).
- WebSocket binds localhost; Tailscale supplies remote access.

## 11. Phases

Each phase must become a daily driver before the next starts.

| Phase | Scope and exit |
| --- | --- |
| 0 — Scaffold | Done: crates, schemas, build/test/fmt/lint. |
| 1 — Walking skeleton | Done 2026-08-13: local chat, log/projection, streaming TUI, identity, traces; daily simple questions. |
| 2 — Memory | Done 2026-08-22: records, archive search, consolidation, review; both recall paths work on real history. |
| 3 — Development | Jobs, workspaces, roles, installation. Exit: a week of development and rebuild matching live state. |
| 3.5 — Tree | Fork, rewind, navigation. Exit: branching used naturally. |
| 3.6 — Quiet week | Done 2026-09-03. Relay failures motivated the direct door. |
| **3.7 — Direct door** | **Current.** One runner, mid-turn messages, visible tools, event compaction, directory-selected door, presence-gated memory. Exit: a week in `:code`, chat for talk/away dispatch, real compaction without visible context loss. |
| 4 — Voice + remote | §7.1 prototype/local fallback, phone access, automated backup. Exit: voice correction/reconnect, restore drill, provider-pinned offline degradation. |
| 5 — Devices | First MCP actuator and safety policy, then arm. Room satellites are clients, not device tools. Embeddings only when FTS falls short. |

## 12. Open questions

Decide when evidence or the relevant phase requires it:

- Consolidation timing: keep configurable idle timeout until traces justify close-triggered or continuous extraction.
- Role-specific timeouts/concurrency, especially titling vs extraction; dispatch model quality.
- Compaction threshold and summary quality on real work.
- Voice playback accounting, retention, delegation, activation timeout, pinned offline fallback, resource use (§7.1).
- Logged identity edits only if human edits become a bottleneck.
- Embedding model if FTS proves insufficient.
- Multi-machine log sync after v1.
- Replay checkpoints when startup measurements justify them.
