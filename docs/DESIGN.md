# ARC — Autonomous Robotic Core

## Design Document

**Status:** v1, governs initial implementation. Amend this file before diverging from it.

---

## 1. What ARC is

ARC is a personal AI assistant. An always-on Rust daemon (`arcd`) serves thin TUI, voice, and mobile clients over WebSocket. ARC has durable memory, a stable identity, pi-style branching sessions, and swappable LLM providers. In time, it will control machine and cloud tools and support a robotics workbench through MCP device servers.

Conversation stays short and responsive. ARC sends longer work, such as writing a program or flashing a board, to jobs with their own models and tools. Coding is the first job type, not a special case.

Priorities, in order:

1. **Durability.** Memory and history survive machine, provider, and schema changes. One source of truth; everything else is rebuildable.
2. **Observability.** Every LLM call, tool call, and memory operation is traceable. ARC emits Perfetto traces of itself.
3. **Speed.** No GC, small idle footprint, protobuf on disk and on the wire.
4. **Independence.** Providers, auth, and models sit behind one interface. Local models are a planned path, not an afterthought.

v1 excludes multi-user use, cloud hosting, plugin sandboxing, and robotics code. The architecture leaves room for them; the code does not.

## 2. Repository layout

The workspace has five crates:

- `arc-proto` — every protobuf schema (`events.proto`, `memory.proto`, `wire.proto`, package `arc.v1`) and its prost types. It is the only place ARC defines serialized formats; log and wire use the same generated types. `perfetto.proto` is different: it is a trimmed copy of Perfetto's upstream schema in package `perfetto.protos`. Its upstream field numbers must remain unchanged, and it is never written to the log.
- `arc-core` — all logic: log, projections, provider abstraction, memory tools, tracing. Testable without a running daemon.
- `arcd` — the daemon. Owns the log, runs projections, serves the WebSocket, holds credentials, runs consolidation.
- `arc` — the TUI client.
- `arc-voice` — the wake-word voice client.

A future `arc-mobile` speaks the same protocol and depends only on `arc-proto`.

## 3. The event log

The log is the source of truth for all durable state except the identity file. It is an append-only sequence of length-prefixed protobuf records in `data/log/`. Each payload has a CRC32: the prefix detects truncation and the CRC detects corruption. Size-based segments keep backups cheap.

These segment rules preserve durability:

- **Naming.** A segment is named for the seq of its first event, padded to 20 digits (`00000000000000004711.log`). Name order is log order, and any seq is locatable without opening a file. If a segment dies on its first record, its replacement takes a `_1`, `_2`, … suffix. Ordering still holds. This is deliberate.
- **Sealing.** A segment is sealed when a later one exists. Creating the successor is the seal; there are no marker files.
- **Recovery seals; it never truncates.** A torn tail remains untouched and the next segment starts at the recovered sequence number. Replay tolerates torn bytes and verifies gapless sequence numbers across every boundary, beginning at 0. Neither a torn tail nor a missing segment can hide records.
- **Record cap.** One encoded event is at most 16 MiB. This is a product constraint, not framing trivia: no memory record or tool result may exceed it, and it lets the reader reject an absurd length prefix before allocating for it.

```proto
message Event {
  uint64 seq = 1;            // monotonic, gapless
  google.protobuf.Timestamp ts = 2;
  Source source = 3;         // MODEL, USER, SYSTEM
  oneof payload {
    SessionEvent session = 10;    // message, session, fork, tool call + result
    MemoryEvent memory = 11;      // record created / updated / superseded / deleted
    IdentityEvent identity = 12;  // reserved; see §5.1
  }
}
```

Three rules define the model:

1. **Only an append changes durable state.** Model writes, user hand-edits, and migrations all append. None edits prior bytes.
2. **Everything else is a projection.** The SQLite index, current memory state, and session trees are deterministic replays. Delete any of them and rebuild.
3. **Schema changes are additive.** Fields are never renumbered or repurposed; old events must always decode. Forward compatibility has one boundary: a new *kind* inside an existing payload arm is skipped safely on replay, but a new top-level `Event.payload` arm decodes as empty on an older binary and reads as corruption. So replaying a log needs a binary at least as new as its newest payload arm. That is fine while writer and reader ship together; revisit if they stop.

**Durability.** v1 fsyncs every append. Add batching only when traces justify it. Tool loops may make this worthwhile in Phases 2–3. The candidate design is a fixed coalescing window of about 200 ms, one fsync per batch, and an explicit flush before completing a turn. Batching must retain the writer contract: sequence numbers are assigned internally; a written but unsynced record keeps its number; and a failed writer is rebuilt, never retried.

**Consequences.** Backup copies the segments. Moving machines copies the log and identity file, then replays. SQLite needs no live-database backup because it is disposable.

### 3.1 Tool-call events

Tool use appends two kinds inside `SessionEvent`: `ToolCallIssued` and `ToolResultRecorded`.

Do not add them to `MessageAppended`. Its `content` is the text the user saw and the archive indexes. An optional calls field would give every reader another message form to handle. Do not add a top-level payload arm either: an older binary can skip a new kind in an existing arm, but it treats a new top-level arm as corruption.

One turn with reasoning, two parallel calls, their results, and final text:

```
user asks                    MessageAppended{USER, content, turn_id=T}
model reasons                — nothing durable
step 0 text, if any          MessageAppended{ASSISTANT, content, turn_id=T}
step 0 call index 0          ToolCallIssued{T, call_id=a, index=0, name, arguments_json}
step 0 call index 1          ToolCallIssued{T, call_id=b, index=1, name, arguments_json}
                             → both dispatch, b finishes first
                             ToolResultRecorded{T, call_id=b, OK, content}
                             ToolResultRecorded{T, call_id=a, OK, content}
step 1 final text            MessageAppended{ASSISTANT, content, partial=false, turn_id=T}
```

- Reasoning is streamed, never durable.
- A step that only calls tools appends no `MessageAppended`. The call events are the assistant's utterance for that step. Text and calls were mutually exclusive in all 71 captures, so this is the ordinary case.
- Results append in completion order, because the log records what happened when. The provider transcript sorts them by the index of the call each one closes.
- `Event.source` is `MODEL` on a call and `SYSTEM` on a result: the model asked, arcd ran it.

**Turns use an id, not events.** There are no `TurnStarted` or `TurnEnded` events. A user message creates a `turn_id`, which every event in the turn carries. This gives the same grouping without two extra fsyncs or another source of truth. Phase 1 events decode with an empty `turn_id`, meaning one message per turn, which is true for a log without tools.

Within a turn, grouping is by seq: an assistant text message plus the calls after it, with no result between, is one step. That works only because events are filtered by `turn_id` first. The raw log interleaves sessions and payload arms, so adjacency in it means nothing.

**Per-call identity.** A call uses the provider's `call_id`, recorded verbatim, and a dense `index` that starts at 0 for each step. Parallel calls are valid. A parser keyed on anything other than `index` can silently merge them.

`call_id` is the unique key within a session. arcd mints one if the provider sends none, and mints a replacement if an incoming id collides with any call the session already logged, open or closed — the projection's join and the rebuilt transcript both see the whole session, where a repeated string is an ambiguity the open-call set never catches. Either way the log records the id actually used and replay reads it rather than regenerating it, so the provider never sees a mismatch.

A result names its call by `call_id` and nothing else. No copied tool name: a second copy of a fact is a second thing that can be wrong, and the projection joins once. The dialect's `type: "function"` is not recorded, since it has one value and a second would arrive as a new field.

```proto
message ToolCallIssued {
  string session_id;
  string turn_id;
  string call_id;
  uint32 index;
  string name;
  string arguments_json;  // complete object, verbatim as sent to the tool
}

message ToolResultRecorded {
  string session_id;
  string turn_id;
  string call_id;
  ToolOutcome outcome;
  string content;         // what the model is shown, verbatim
  bool truncated;
}

enum ToolOutcome {        // UNSPECIFIED stays 0 and is never written
  TOOL_OUTCOME_UNSPECIFIED;
  TOOL_OUTCOME_OK;
  TOOL_OUTCOME_ERROR;
  TOOL_OUTCOME_UNKNOWN;
}
```

**Write order and resume.** ARC appends and fsyncs `ToolCallIssued` before running its tool. It makes a step's full batch durable before running any call. The log can therefore prove that no unrecorded call ran.

This creates one important case: a durable call with no durable result. Replay can conclude only that **the outcome is unknown**, not that it failed. The tool may not have started, may have completed before the process died, or may have lost its result in the crash. The log cannot distinguish these cases. ARC therefore never silently retries or drops the call. Replay tracks open `call_id`s and removes them when it sees a result; calls left over are orphaned.

Only arcd may act on an orphan, at startup, before it dispatches anything for that session. An in-flight call in a live daemon looks identical on disk to an abandoned one, so "orphaned" is a property of the log *plus* nobody running. That is why the repair is an appended event rather than something each reader invents: `arcd rebuild`, `memory-replay`, and the projection all read an orphan as unknown and append nothing.

At startup, arcd appends one `ToolResultRecorded{outcome: UNKNOWN}` for each orphan, with `Event.source = SYSTEM`. Its fixed message says that the daemon restarted before recording the result and that the call may have run. This closes the call durably, keeps future replays clean, and gives every `tool_call_id` in a rebuilt transcript a result. The result may appear hours after the call, so correlation uses `call_id`, not neighbouring log entries.

arcd does not re-drive the model. A restarted daemon resuming a turn the user walked away from is a surprise, and the cost of not doing it is that the user types "continue." The turn resumes on the next user message, which follows a tool result perfectly well.

**Tool errors are results.** Bad arguments, missing files, timeouts, unknown tools, and denials produce `ToolResultRecorded{outcome: ERROR}` containing the text shown to the model. The loop continues. ARC's own failures—an unavailable provider, malformed frame, or failed log write—go only to the client.

The boundary is one line: **if the model will see it, it is durable; if only the user sees it, it is a wire `Error`.** A tool error changes the conversation and the model must reason about it. A provider outage changes nothing and the fix is to ask again. If the log append itself fails, nothing durable happened, the turn is abandoned, and the client gets an `Error`. Readers treat an unrecognized `ToolOutcome` as UNKNOWN, never ERROR — unknown carries the safe behaviour.

**`partial` does not extend to calls.** It stays on `MessageAppended` and stays about text. A call is appended only once its arguments are complete, so a half-streamed call is never appended and there is nothing to mark. A result arrives whole. What marks a turn cut mid-loop is the orphaned call. Two mechanisms for two different failures: cut text loses only what the user did not see, while a cut loop may have left an effect in the world.

**Size.** The 16 MiB cap is a backstop, not the policy. Before building an event, the tool registry truncates results to configurable `max_tool_result_bytes`, initially 32 KiB. An 8k-token context is the real limit. Truncation sets `truncated` and adds a content marker. This loss is intentional: the log stores what the model saw, not a hidden full result. The registry enforces the limit because it knows how to truncate each tool's result. An event above 16 MiB is a registry bug that the log catches.

**Secrets.** Tools must keep secrets out of results before they reach the log. The log writer cannot know which text is secret, and regex redaction is unreliable. A tool that uses credentials returns a reference, never the value. The model cannot echo a credential because credentials never enter its context. This guarantee only holds for tools that cannot read their own environment; Phase 2 has none that can.

**Reserved, not built.** One number in the `SessionEvent.event` oneof, for a future reasoning event. Reasoning is streamed by decision, and reserving keeps "durable after all" an addition rather than a migration. A oneof number beats a field on `MessageAppended`, because a call-only step has no `MessageAppended` to hang reasoning on.

Three field-level reserves, cheap while the messages are new:

- `ToolCallIssued`, tool *source* — an MCP server id. §9's devices are MCP servers, and two servers can both offer `read`.
- `ToolCallIssued`, the model that issued the call. §6 makes provider choice per-completion, and only `SessionCreated` records what ran.
- `ToolResultRecorded`, structured content. `content` is a string; an image result needs its own field.

Not reserved: call duration. Latency is trace material (§8), and in the log it would make every replay assert timing it cannot reproduce.

**What the projection needs.** Messages stop being `(role, content)` rows:

- calls and results as their own rows, keyed by `call_id`
- `turn_id` on every row, so a reopened session can rebuild the display and a valid provider transcript
- tool name and outcome as columns, so "when did you last write a memory record" is a query
- `partial` and `truncated`, so a cut reply and a cut result differ from whole ones

FTS indexes tool-result content, tagged by row kind and excluded from `sessions_search`'s default — a 30 KB directory listing would otherwise outrank the sentence the user wrote. Arguments are not indexed: small JSON, and the call is already findable by name.

**Prior art (DeepSeek Harness).**

- Taken: the write-ahead checkpoint.
- Taken: closing an unanswered call with a synthetic result, so the resumed transcript stays valid. Theirs injects a risk-classified *error*; ours records UNKNOWN, the honest classification and the one that forbids silent retry.
- Rejected: `turn/start` and `turn/end` events. `turn_id` over a gapless seq groups just as well, without two fsyncs a turn.
- Rejected: persisting `assistant/chunk` delta runs. The log stores what the user saw, not how it arrived.
- Rejected: a catalog of string event types with an `ignorable` escape hatch. Oneof numbers plus rule 3 already give skip-safety, and strings would put a second schema authority outside `arc-proto`.

Open, left to the task that hits them:

- Whether an UNKNOWN call may ever be re-dispatched automatically for a tool that declares itself idempotent. The registry trait has no idempotence flag; if one lands, this contract gains a branch.
- The FTS default for tool-result rows, to be confirmed against real queries. The rows must be tagged either way, so the choice stays a query change rather than a re-projection.
- The redaction policy for a tool that can capture its own environment (a shell tool, a device tool that echoes config). None exists in Phase 2. The first one decides it, and it is tool-side either way.

### 3.2 One writer, many readers

`arc-core::store::Store` holds the log and the index together and exposes a single `append`. Nothing else writes. Invariants 1 and 2 stop being a discipline that call sites remember and become a property of the type that owns the state.

Reads do not go through it. The index runs in WAL mode, so a second connection reads committed state while the writer works. `arcd` holds one `projection::Reader` — read-only flags, its own lock — and serves `list_sessions`, `fetch_history`, and `memory_review_list` from it without touching the engine. Only a turn, a memory verdict, and the consolidation commit take the engine lock, and they hold it for an append rather than for a whole completion.

That is why the consolidation pass may re-lock instead of holding: it snapshots, runs the model unlocked, then commits and re-checks that the session has not grown (§5.4).

## 4. Sessions, jobs, and tools

Sessions are pi-style: a tree, not a list. Forking at any message creates a child with a `parent_session` and `fork_point`. The tree is just parent pointers in the projection. Clients render it; the daemon only stores it.

Branches interact with memory (§5.4): only the main line and branches marked *real* feed consolidation. Abandoned branches stay searchable in the archive but never write distilled memory.

A reply cut mid-stream is appended with `partial = true` — the log records what the user actually saw. Errors go to clients and are never archived as messages.

### 4.1 Jobs

**A job is a child session.** It is not a queue or a separate abstraction. The child has its own provider role, tools, and budget; the parent stores its identifier.

This gives a job the same archive, replay, fork, and rewind behaviour as any other session. It needs no separate transcript store.

The reason to separate them at all is size. A conversational turn is small; writing a driver is twenty minutes and a hundred thousand tokens. Run that in the conversation and the context the user talks to drowns in tool output.

Rules:

- **The conversation never blocks.** Dispatch returns a job id immediately and the user keeps talking.
- **Jobs accept messages while running.** Steering — "no, use GPIO 4" — is a message to the child, not a restart of the parent. Without it the only correction is a rewind, which throws the work away.
- **Handback is a summary, not a transcript.** The child's full history stays in the archive; the parent receives a short report and the child's session id. The same split as §5.2 against §5.3, for the same reason: the index stays small so the body can be large.
- **A job is pinned to one provider for its lifetime.** Prompt caches are model-scoped and prefix-matched, and cache reads dominate the cost of any long agentic session. A job that switches models pays for its whole context again. Role choice happens at dispatch, never mid-job.
- **A job has a budget**, declared at dispatch and enforced by arcd. A loop that cannot terminate must not be able to drain a month's allowance.

**A job runs in its own process**, not inside `arcd`. The daemon must not share an address space with `bash` or a runaway loop. The worker uses the same `Store` and appends the same events, but it can fail without taking down the conversation.

**Dispatch is a normal tool call with a delayed result.** The model calls dispatch, ARC appends `ToolCallIssued`, the worker starts, and the turn ends. When the job finishes, its summary becomes that call's `ToolResultRecorded`. The existing unfinished-call recovery rule also covers a crash: a durable dispatch without a result is unknown, not failed.

Coding is the first job kind, not a privileged one. A device job (§9) is the same shape with a different tool set.

Its internal shape is fixed and lives in the worker, not in a prompt: **plan → implement → review → fix → review**, with counsel (§6.2) supplying plan and review, `hands` supplying implement and fix, and the loop bounded. Every step is an ordinary event in the child session, so the whole cycle is replayable and traceable, and the parent sees one summary at the end. A job that runs out of rounds with blocking comments left says so rather than reporting success.

### 4.2 Workspaces

A session may be bound to a project: `sessions.project` (§5.3) plus a root directory on disk. The binding scopes the workspace tools in §4.3, and every path those tools resolve must stay inside the root.

Unbound sessions are ordinary conversation and get no workspace tools. That is also the token argument — schemas for tools a session cannot use are not loaded into it.

### 4.3 Tools, sources, and approval

One registry, many sources. A tool reaches the model identically whichever source it came from:

- **builtin** — memory and archive (§5.5)
- **workspace** — `read`, `write`, `edit`, `glob`, `grep`, `bash`; only in a bound session (§4.2)
- **expert** — `consult_expert` (§6)
- **mcp** — device servers (§9), later

Sources exist so that Phase 5 devices are a config entry rather than a subsystem. §3.1 already reserves the tool-source field on `ToolCallIssued` for exactly this case: two sources may both offer `read`.

**Sources are session-scoped.** A session declares which it gets. Seven always-loaded schemas already cost real context; loading every source into every session does not survive Phase 5.

**Approval has one shape.** Running `rm -rf` and rotating a servo are the same event: a proposed call, a gate, a durable decision, and an allow-list that is a projection rather than a hand-edited file. Verdicts carry `Event.source = USER`, like §5.4's review verdicts and for the same reason — this is ground truth the log must be able to answer.

Three verdicts: once, for this session, always for this project. A denial carries a reason, returned to the model as an ordinary `ToolOutcome::ERROR` result (§3.1) so the loop adapts instead of stalling.

**Confinement.** Every path resolves to canonical form and is rejected if it escapes the workspace root — `..`, symlinks, and absolute paths are the obvious cases. The check lives in the tool, not the caller.

**Edits are strict.** `edit` matches exactly one occurrence and refuses if the file changed since it was last read. A cheap model's most common failure is a plausible wrong edit; a strict tool turns that into a retryable error instead of silent damage. This is the highest-leverage rule in the section, because §6's economics depend on a cheap model doing the bulk of the work.

**The shell tool settles the open redaction question.** `bash` is the first tool that can read its own environment. arcd runs workspace tools with a scrubbed environment: no keys and no tokens. A result cannot contain credentials the process never received. Secret protection depends on what the tool can access, not on a regex applied afterwards.

## 5. Memory

"Feels alive" splits into three subsystems with different storage, update rules, and retrieval paths. They are deliberately not unified.

### 5.1 Identity

One small human-owned file: `data/identity.md`. Who ARC is, how it talks, stable facts about its user. Loaded into context every session, unconditionally. A few KB — small enough that loading it is never a decision.

Identity is exempt from event-sourcing. It is a plain file, versioned in git and backed up beside the log. ARC may propose edits as ordinary session output; the user applies them by editing the file. `IdentityEvent` is reserved in case this changes. v1 never emits it.

**Voice.** The identity file is where ARC's register is defined, and the target is direct without being cold. Four rules, as design intent for whoever writes the file:

- Lead with the answer. The first sentence is the conclusion, not the setup.
- No enthusiasm scaffolding — no "great question", no exclamation points, no recap of what was just said.
- Disagree in one sentence, then keep working. State the concern plainly; do not hedge and do not moralise.
- Warmth lives in brevity and attention, not adjectives. Remembering a detail from Tuesday reads as care; "happy to help" reads as a form letter.

This is close to what `AGENTS.md` already asks of contributors, and deliberately so — one house style, applied to the assistant and to the people working on it.

Identity loads into the **face** role only (§6). The roles that execute jobs have no voice and no personality preamble; paying for one on the bulk of the token spend is waste.

### 5.2 Distilled tier

A flat set of structured records ARC writes deliberately. Flat, not hierarchical: namespaces and links organize; a folder tree does not.

```proto
message MemoryRecord {
  string id = 1;
  Kind kind = 2;              // PERSON, PROJECT, PREFERENCE, FACT, DECISION
  string namespace = 3;       // "global" or a project id
  string title = 4;
  string summary = 5;         // one line; appears in the always-loaded index
  string body = 6;            // markdown, freeform
  repeated string links = 7;  // related record ids
  Provenance provenance = 8;  // session ids + timestamps where learned
  Status status = 9;          // ACTIVE, SUPERSEDED — never hard-delete
}
```

Current state is a projection of `MemoryEvent`s. Context always carries an index of ACTIVE records — `namespace + kind + title + summary` only — so the model knows what exists without loading bodies. Superseding rather than deleting keeps history ("you used to live at X") and keeps replay honest. A user-requested purge is a `DELETED` event, and the projection then drops the record entirely.

Provenance is required: every fact must answer “where did you learn that?” by pointing into the archive.

### 5.3 Archive tier

The raw session tree, fully indexed, projected into SQLite (`data/index.db`, rusqlite, bundled):

- `sessions(id, parent_session, fork_point, project, title, started_at)`
- `messages(session_id, seq, role, content, ts)` + an FTS5 index over `content`
- later: `chunks(session_id, span, embedding)` via sqlite-vec — added only if FTS proves insufficient, and addable any time by re-projecting

The distilled tier answers "what do you know about X." The archive answers "what did we say about X in March." Neither substitutes for the other.

### 5.4 Write pipeline (consolidation)

Storage is the easy half. Deciding what to remember is the hard half. v1 keeps it simple:

1. **Explicit.** "Remember this" → the model calls `memory_write` immediately.
2. **End-of-session extraction.** When a session goes idle, a cheap model pass extracts durable facts, merges them into existing records, and resolves contradictions by superseding. Runs async on the daemon.

Every decision emits Perfetto spans, so tuning happens against traces, not guesses. The failure modes to watch are hoarding noise and remembering nothing useful. Rules get complexity only once real usage shows which one bites.

**Tuning loop.** Sessions live in an append-only log, so consolidation is re-runnable. That is the primary tuning mechanism:

- The prompt is versioned. `arcd memory-replay` re-runs a version over historical sessions and diffs the resulting memory against another version. This is the regression suite: every prompt change is evaluated against real history first.
- A weekly TUI review shows records created and superseded that week. Each is accepted, fixed, or deleted. Reviews are the ground truth.
- Corrections become few-shot examples in the prompt. Fixed records are exactly the examples that encode the user's standards.
- Three metrics live in traces: records created per session (hoarding), supersede rate (contradiction handling), and retrieval hit rate — how often a `memory_search` result is actually used. The last is the honest measure of whether memory earns its tokens.
- Once reviews accumulate, a hand-labeled sample yields precision and recall on real usage.

**Review verdicts.** All three are ordinary events with `Event.source = USER`: *fix* is a `MemoryRecordSuperseded`, *delete* a `MemoryRecordDeleted`, *accept* a skip-safe `MemoryRecordReviewed { record_id }`. Accept is durable because reviews are the ground truth this section rests on — without it, "human-confirmed" is not a fact the log can answer and the sampling above has no labels.

The projection stamps `changed_at` and `reviewed_at`, so the review list is "changed in the window, not reviewed since its last change." An accepted record leaves the queue; a later change re-enters it.

Fixing happens through conversation, not a TUI editor: the review pane prefills the chat input with a supersede instruction the user edits and sends, and the model writes through the ordinary tools. The review UI never mutates memory.

**Coverage and atomicity.** What the pass has covered is durable state, so it lives in the log: `SessionConsolidated { session_id, through_seq, prompt_version }`, a skip-safe kind appended when the pass finishes a session — including when it extracted nothing, because "looked and found nothing" is a decision. `through_seq` is the last event the pass read. A session is due when it has been idle past the window and has events after its latest marker, so "what is due" is a query and never daemon memory.

The pass is atomic and shaped for a shared sidecar: read the session, release the engine, run the model with nobody blocked, then re-check under the lock that the session is still idle before appending records and marker together. New activity since the read discards the pass whole, and a fresh idle timeout re-runs it over the longer history. Model-written records use `Event.source = SYSTEM` — arcd initiated the write, not the user's turn.

Prior art: hermes-agent's curation policy — do-not-capture rules, nudge mechanics, search and background-call lessons — is distilled in `docs/prior-art-hermes.md`.

### 5.5 Retrieval

Memory access is tools, not silent RAG injection. The model calls:

- `memory_read(id)` — fetch a record body
- `memory_search(query, namespace?)` — search distilled records
- `memory_write(record)` / `memory_supersede(id, record)` — emit MemoryEvents
- `sessions_search(query, project?)` — FTS over the archive; returns snippets and session ids
- `session_read(id, range)` — pull actual past context

One pattern throughout: search cheap, read targeted. Lookups appear in traces and debug like any other tool call. Nothing enters context automatically except the identity file and the record index.

These five are the **builtin** source in §4.3's registry. Workspace, expert, and device tools reach the model through the same registry and the same events.

## 6. Providers

One trait in `arc-core`:

```rust
trait Provider {
    fn complete(&self, req: CompletionRequest) -> impl Stream<Item = CompletionDelta>;
    // model listing, token counting, capability flags
}
```

### 6.1 Roles

**A role is a name in config that resolves to a provider and a model.** Four in v1:

| Role | Job | Why it is its own role |
| --- | --- | --- |
| **face** | The conversation. Talk, recall, dispatch jobs (§4.1). Loads the identity file and the record index; §5.1 defines its register. | Small token volume, high sensitivity to voice and judgment, latency-critical once §7's voice client lands. Needs vision. |
| **hands** | Job execution. The overwhelming majority of tokens ARC will ever spend. | High volume, mechanical. Cost per *completed task* is the only figure that matters. |
| **counsel** | Bounded advice — plans, unsticking. Never writes. | Rare, expensive, worth a frontier model precisely because it is rare. |
| **local** | Consolidation, extraction, classification, and the offline fallback for face. | High volume, low stakes, latency-insensitive, free. |

This is how §12's routing question gets answered without a runtime difficulty classifier: the mapping is static config, and the role label rides every `CompletionRequest` onto its span (§8), so traces attribute spend by role from the first day.

**A role resolves to an ordered ladder, not a single model.** The first rung is what the role uses when nothing is wrong; each lower rung is what it degrades to under a named condition — an allowance crossing a threshold, an allowance exhausted, the network gone. `face` degrades to `local`. `counsel` degrades from Opus to Sonnet under budget pressure and no further, because advice from a weak model is worse than no advice.

Three properties make this safe rather than a silent quality drop:

- **The ladder is the allow-list.** A role can only ever run a model named on one of its rungs, so a provider adding a model — or changing which one an alias points at — cannot route work somewhere unvetted. It fails closed.
- **Degrading is visible.** The client says which rung is live and why. A user who cannot tell that answers got cheaper cannot judge them.
- **Rungs recover.** A threshold-triggered degrade lifts when the window resets. Degraded is a state, not a latch.

**Local is a role, not a lesser tier.** Its profile — bulk, structured, latency-insensitive — is exactly what a small model is good at and exactly what should never be paid for hosted. It is also the bottom rung of `face`'s ladder, which is what lets ARC keep talking with the network down.

**A session is pinned to its role's provider for its lifetime.** This amends the earlier v1 position that sessions do not own a provider. The trait stays per-completion, but prompt caches are model-scoped and prefix-matched, and cache reads dominate the cost of any long agentic session — a mid-session model swap pays for the whole context again. Hot-swapping a live session is therefore no longer a feature to reach for; changing role means a new session or a fork.

### 6.2 counsel is a tool, not a provider

`consult_expert` spawns a foreign agent — a CLI subprocess with **read-only** access to the workspace — asks one bounded question, and returns text. It is not a `Provider`, has no `CompletionDelta` stream, and never touches the calling session's prefix.

That last property is the point. Escalating costs one subprocess and one tool-result event; it does not invalidate the face's or the job's cache. And because the expert reads the repository itself rather than being handed a summary, its answer is better than a completion built from a lossy brief.

**Two modes, one mechanism.** `plan` is asked before work starts and returns an approach. `review` is asked after a change and returns comments. Both read the repository themselves, both are read-only, and both are the same subprocess with a different prompt.

**counsel is called from inside a job, not only from the face.** The coding job's loop is plan, implement, review, fix, review — all of it inside the child session (§4.1). The conversation sees one dispatch and one summary. Putting the review cycle in the face instead would pay for the whole transcript twice and would put "is this comment worth another round" on the model least suited to judging it.

Three invariants:

- **counsel never writes.** Read-only tools, always. An expert that edits is a second coding agent with a second cost profile, and the whole point of the split is that the expensive model does not do the work that is expensive by volume.
- **The expert is a command, not a code path.** An argv template, a working directory, and a timeout in config. Swapping one CLI for another is an edit, not a release.
- **Review loops are bounded and terminate honestly.** "Loop until no comments" does not terminate on its own — a reviewer asked for comments will find some, and the last few rounds trade real money for taste. So: a configured maximum number of rounds, and only comments the reviewer marks *blocking* trigger another implement round. Non-blocking comments ride along in the handback for a human to judge. If the bound is reached with blocking comments outstanding, the job reports done-with-unresolved and names them. It never reports success it did not reach.

### 6.3 Transport and credentials

The local provider is a llama.cpp `llama-server` sidecar supervised by `arcd`, spoken to as an OpenAI-compatible endpoint (`/v1/chat/completions`, HTTP + SSE, no auth). The same implementation covers vLLM or any OpenAI-compatible server by config. The sidecar releases device memory after an idle window (`--sleep-idle-seconds`), so an always-on daemon holds tens of MiB of VRAM between turns instead of several GiB, and pays about 1.5 s to wake. That is what makes a local default workable on a machine the user also games on.

It is also why the same code reaches most hosted options: an OpenAI-compatible endpoint is a base URL, a key, and a model id.

Hosted providers use plain HTTP and SSE (`reqwest` + rustls), never vendor SDKs. Authentication is replaceable; for now it uses API keys only. The Google OAuth path was removed after hidden rate limits made it unreliable and its terms became questionable. Keys live in `data/secrets/` (0700 and excluded from backups). Phase 3 uses that storage for face, hands, and counsel.

Tool-calling and system-prompt differences are normalized in `arc-core`, never leaked to clients. The log records which model actually ran.

### 6.4 The concrete stack is dated

`docs/providers.md` records each current model, its cost, its limits, and the conditions for changing it. Update that file as plans and prices change. Keep this section stable because it defines the architecture.

## 7. Wire protocol and clients

Protobuf over WebSocket (`wire.proto`), served by `arcd` on localhost. Remote access is Tailscale reaching the same socket. ARC does not implement its own tunnel, TLS termination, or auth beyond a local token in v1.

The protocol is client-agnostic: subscribe to a session, send a message, fork, receive streamed deltas and tool-call events, query the tree. Sessions are created implicitly — send with an empty session id and the daemon replies with the assigned one. Clients hold no durable state.

Three capabilities the protocol needs beyond that, all forced by §4.1 and §6:

- **Images in messages.** The face has vision. A turn may carry a camera frame or a screenshot, and `ToolResultRecorded` may need to return one — §3.1 already reserves structured tool-result content for this.
- **A modality hint on subscribe.** Without it a voice turn gets the answer the TUI would get: four hundred words of markdown, read aloud. The client declares its modality and the face adapts length. Same identity, shorter sentences.
- **Server-initiated output.** A job runs for twenty minutes while the user is elsewhere; "this needs approval" and "this is done" have to reach them. Clients subscribe for unsolicited output rather than only answering when addressed. This is also the path ARC speaks first on at all — a contradiction noticed during consolidation, a sidecar down for two days — so it is built general and voice is one subscriber.

Clients:

- `arc` (TUI): first client, exercises everything — tree navigation, streaming, tool visibility, job status, approval prompts. Should use UDS when local, WebSocket when not.
- `arc-voice`: a thin pipeline — wake word, local ASR, text over this socket, reply text, local TTS. No model logic. Stages sit behind traits like providers do: openWakeWord or Porcupine for the wake word, whisper.cpp for ASR with Silero VAD in front for endpointing, Kokoro for TTS streamed sentence-by-sentence so the first sentence speaks while the model writes the third. Cloud stage backends can slot in later without touching the architecture.
- Mobile: same protocol over Tailscale. Last, after the protocol has been stable under two other clients.

**Speech-to-speech APIs are rejected.** They would own the conversation loop, and under §4.3 the face is not a chat model — it holds the memory tools, `consult_expert`, job dispatch, and the approval gate. Handing the loop to a vendor means replumbing all of it through their tool protocol, and the log stops being where the conversation happens. That breaks invariants 1 and 2, not merely §7's client-agnosticism. Text on the wire is the only shape that keeps the log authoritative, and it keeps voice provider-independent besides.

**Voice degrades rather than failing.** With a hosted face, a dropped network breaks talking, not just coding. Falling back to the local role (§6.1) is a Phase 4 exit requirement, with the degraded state visible or audible.

ARC's speaking voice is designed separately from its writing voice (§5.1) and is not a stock persona. Phase 4 ships on a stock Kokoro voice named in config; the real voice is chosen later by living with three or four candidates for a day each rather than by demo impressiveness, then pinned under `data/` as clip, engine, version, **and stage config** — rate, pitch, and sentence-split thresholds shape perceived character as much as timbre, and they are the part that silently drifts across engine upgrades.

## 8. Observability: Perfetto

ARC emits Perfetto protobuf traces of its own operation: a track per session and branch, spans for LLM calls (with token counts as counters), tool calls, memory reads and writes, consolidation passes, and log replays. Traces land in `data/traces/` and open directly in the Perfetto UI.

This is not decoration. It is the debugging surface for the two genuinely hard subsystems — consolidation quality and retrieval behaviour — and it makes latency and token spend measurable from day one. Implementation: a `tracing` subscriber in `arc-core` that renders `TracePacket` protos.

Every LLM span carries its **role** (§6.1), and every job span carries its job id and budget consumption. Cost is metered by role and by completed task, not by request, because that is the figure §6's model choices are actually made against.

## 9. Robotics (future)

Devices integrate as MCP servers, never as bespoke daemon code: an ESP32 pan-tilt rig, later an SO-101-class arm. Two constraints are fixed now:

1. The model plans and issues high-level actions only. Firmware enforces joint limits, speeds, and e-stop. The LLM never commands motors directly.
2. Device MCP servers are separate processes with their own lifecycle. `arcd` treats them like any other tool source.

Both constraints sit on top of machinery Phase 3 already builds. A device is an `mcp` source in §4.3's registry, and a movement request goes through the same approval gate as `bash` — one shape, one event, one allow-list. §3.1's orphan contract already covers the hard case: a durable call with no durable result means the outcome is *unknown*, never failed, because an actuator that moved cannot be un-moved by a retry.

No robotics code lands before Phase 5.

## 10. Security, backup, and running

- **Always-on** means a systemd user unit (`arcd/arcd.service`): starts with the machine, restarts on failure, logs to the journal. `SIGTERM` is a clean stop, and the sidecar dies with it either way because systemd kills the whole control group. Nothing is left holding the GPU.
- Runtime state lives under `data/`: log, index, identity, traces.
- Backup is rustic, encrypted at the repository level, covering `data/log/` and `data/identity.md`. `data/index.db` and `data/traces/` are excluded — both are rebuildable.
- Credentials live in the OS keychain or an encrypted secrets file under `data/secrets/` (0700 and excluded from backups). They never enter the log or backups. Phase 3 uses credentials for face, hands, and counsel.
- **Workspace tools run with a scrubbed environment** (§4.3). `bash` is the first thing ARC runs that could read its own process environment, and the answer is that there is nothing there to read — no keys, no tokens. arcd holds credentials; the tools it spawns do not inherit them.
- The WebSocket binds localhost only. Remote access is Tailscale's problem, by design.

## 11. Phases

Each phase ends in something used daily. No phase starts until the previous one is a daily driver, because real usage is the input to the next phase's design — especially for memory.

**Phase 0 — Scaffold.** *Done.* Workspace, empty crates, empty schemas, build/test/fmt/lint targets.

**Phase 1 — Walking skeleton.** *Done 2026-08-13.* `arcd` with the local provider, linear sessions, the event log with `SessionEvent`, the SQLite projection, the TUI with streaming, the identity file in context, Perfetto spans on LLM calls. Memory is *only* the identity file. Exit criterion — ARC replaces a chat app for daily use — is met where it counts: ARC gets the simple questions, daily. The gaps left are not Phase 1's to close; tools arrive with memory and devices, and a better model is a config line.

**Phase 2 — Memory.** *Done 2026-08-22.* `MemoryEvent`, distilled records and the always-loaded index, the five memory and archive tools, FTS5 over messages, explicit `memory_write` plus end-of-session consolidation, `arcd memory-replay` with a versioned prompt, the weekly TUI review, Perfetto spans on every memory operation. Exit criterion: "what do you know about X" and "what did we say about X" both work on real history.

**Phase 3 — Development.** ARC becomes the way its own code gets written, and runs in production. The four roles of §6 with real credentials, the job abstraction (§4.1), workspaces (§4.2), the tool registry with workspace tools and the approval model (§4.3), the three protocol capabilities of §7, notification, and `arcd rebuild` proven against the real log. Installed as a systemd user unit with a data directory that survives a rebuild. Exit criterion: a week of real development done through ARC rather than through another harness, and a full rebuild matching live state.

**Phase 3.5 — Tree.** Session forking with §4's branch semantics, rewind, and tree navigation in the TUI. Split out of Phase 3 and kept immediately after it because rewind is a development feature: recovering from a bad edit path without re-prompting from scratch is what makes a cheap `hands` model affordable. Exit criterion: branching gets used naturally.

**Phase 4 — Voice + remote.** `arc-voice` per §7 — wake word, local ASR, text on the wire, local TTS — the daemon reached from a phone over Tailscale (the mobile client can start as the TUI over SSH), and rustic backup automated. Exit criteria: a restore drill rather than a backup existing, and voice degrading to the local role when the network is gone.

**Phase 5 — Devices.** The first device MCP server (ESP32 pan-tilt) as a source in §4.3's registry, device-tool safety conventions on the approval model already built, then the arm. A wake-word room satellite, if one appears, is a §7 *client* and not a device — same board, different integration path, and conflating them would put a special case in the device layer. sqlite-vec embeddings land here, or earlier only if Phase 2–4 usage shows FTS falling short.

## 12. Open questions

Deferred on purpose. Decide when the phase forces it.

- **Consolidation triggering:** idle timeout vs explicit session close vs continuous. The v1 placeholder is a configurable idle timeout, so the pass has something to hang on. Traces judge it. (Phase 2.)
- ~~**Model routing.**~~ **Decided.** Configuration assigns static roles; there is no runtime difficulty classifier, and every trace span records its role. Two questions remain: whether roles need task-specific labels (for example, consolidation and titling may need different timeouts and concurrency), and whether face can dispatch reliably enough or needs a stronger model just for dispatch. Phase 3 traces should answer both.
- **Compaction and the log.** A long job (§4.1) will exceed any context window, and summarising its own history is a durable decision that changes what the model sees. Replay must reproduce it, so it cannot be an in-memory convenience. Likely a new kind inside `SessionEvent`, recording what was compacted and the prompt version that did it — the same shape as `SessionConsolidated` in §5.4. Decide when the first job hits the limit, not before. (Phase 3.)
- **Voice stage placement.** With the face hosted, the GPU is free during a voice turn — the sidecar is asleep and consolidation only runs on idle sessions. The Phase 4 plan's "whisper on CPU so the GPU stays free" constraint may no longer apply, which would allow a larger ASR model or GPU-side TTS for faster first audio. Measure in Phase 4 rather than inheriting the assumption. (Phase 4.)
- **Identity edits in the log.** Revisit if hand-editing becomes a bottleneck.
- **Embeddings model for sqlite-vec,** local or API. (Phase 4/5.)
- **Multi-machine beyond backup/restore** (log sync). (Post-v1.)
- **Startup recovery** is a full replay today. A checkpoint bounds it when the log grows. (When startup time or traces say so.)
