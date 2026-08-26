# ARC — Autonomous Robotic Core

## Design Document

**Status:** v1, governs initial implementation. Amend this file before diverging from it.

---

## 1. What ARC is

ARC is a personal AI assistant. An always-on Rust daemon (`arcd`) serves a thin TUI client over WebSocket. ARC has durable memory, a stable identity, and swappable LLM providers. Voice, mobile, and device support are later work.

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

**A job runs as a supervised task in `arcd`.** It uses the same `Store` and appends the same events. This keeps the conversation responsive without adding process coordination. It is not a security boundary: workspace tools still run as the user. Move jobs to a separate worker only when a real sandbox design is ready.

**Dispatch is a normal tool call; the handback is a separate message.** The model calls dispatch, ARC appends `ToolCallIssued`, creates the child durably, and records an immediate `ToolResultRecorded` acknowledging the job and naming the child session. When the job finishes, its summary arrives in the parent as a `MessageAppended` with `Event.source = SYSTEM`, carrying the child's session id.

An earlier draft made the summary the dispatch call's own delayed `ToolResultRecorded`. That shape cannot be built honestly: while the job runs, the parent's transcript holds a call with no result, and providers reject such a history — so the transcript builder would have to show the model a synthetic "still running" line that was never logged, breaking the rule that what the model sees is durable. The immediate ack keeps every transcript valid at every moment, and the system-sourced summary message is durable, ordered, and visible to the model on the next turn exactly as logged. The unfinished-call recovery rule still covers a crash between `ToolCallIssued` and the ack: unknown, not failed.

**Handback is also where a physical action gets its yes.** A job that ends by reporting what it would do returns into the conversation as an ordinary result, and the action itself is a second dispatch the user asks for. Starting a print, or activating a system generation, is confirmed at a turn boundary the user is present for. Nothing prompts mid-turn and no new mechanism appears. Which calls deserve the split is prompt and configuration, like planning and review.

Coding is the first job kind, not a privileged one. Its loop is deliberately small: send messages, run requested tools, append results, and stop when the model stops. ARC adds strict `edit`, durable events, and a per-job budget. Planning, review, retry policy, and similar workflow choices belong in prompts or configuration until repeated use proves they need machinery.

### 4.2 Workspaces

A session may be bound to a project: `sessions.project` plus a set of granted roots on disk. The project's own root is granted read-write. Anything else the session should reach — notes, dotfiles, a reference checkout — is a separate read-only grant. The binding scopes the workspace tools, and every path those tools resolve must sit under one of the grants.

Grants list what is reachable. They are never a list of what is forbidden. A deny list fails open the first time an entry is forgotten; a grant list fails closed, so arcd's own state directory is unreachable because nobody granted it rather than because it was banned. This is the same argument as the model allow-list.

Grants are session-scoped and durable. Replay has to be able to say what a tool call was allowed to see.

**Projects are configuration and only a human writes them.** A session or a job names one; it never composes roots and modes of its own. A grant list fails closed because a person authored it, not because of its shape. A model that can write its own grants asks for whatever the task needs and gets it, which is a deny list with extra steps.

Unbound sessions are ordinary conversation and get no workspace tools. That is also the token argument — schemas for tools a session cannot use are not loaded into it. A voice session starts as one: it has no working directory because it has no filesystem to be wrong about. The directory question appears at dispatch, where the model names a configured project or asks which one.

### 4.3 Tools, sources, and containment

One registry has three sources in Phase 3. A tool reaches the model identically whichever source it came from:

- **builtin** — memory and archive (§5.5)
- **web** — read-only, no grants; provided by the model's own provider where it has one, and empty where it does not
- **workspace** — `read`, `write`, `edit`, `bash`; only in a bound session (§4.2)

Expert and MCP tools are deferred. Add a source only when that tool type is ready to ship; a future source is not a current registry requirement.

**Sources are session-scoped.** A session declares which it gets. Available tool schemas cost real context, so a session never receives tools it cannot use.

**Nothing prompts for permission.** What a project allows is configuration, read once when the session is created. A call outside it is refused and comes back as an ordinary `ToolOutcome::ERROR` with the reason, so the loop adapts instead of stalling. There is no runtime verdict and therefore nothing to record: a per-call prompt trains the user to say yes, and it would block the jobs that most need to run — the twenty-minute ones, started while the user is elsewhere.

Containment does the work instead: granted roots, a scrubbed environment, and a tool set the session declares rather than discovers.

**That containment is incomplete and it should be said plainly.** Every check here lives in a tool, not in the kernel. arcd runs as the user, and `bash` has nothing between it and the filesystem. The honest fix is a sandboxed worker, not a dialog. Until then the protection is the tool set, the granted roots, and the fact that this is a personal machine.

Grants are therefore advisory in any session that holds `bash`. They stop the model wandering out of its project, which is the common failure, and they record what a session was scoped to. They do not stop a determined one. A grant over the whole home directory waits for the sandbox for the same reason: arcd's own keys live under it, so a wide grant puts them back inside a project root, and no exclusion list helps when the shell never consulted one. Changing the machine itself does not need that grant — a project over the Nix configuration plus one privileged activation command reaches the whole system, and every change is a reviewable diff with a generation to roll back to.

**Prefer a CLI tool in the workspace over a new builtin.** Every builtin is paid for in context by every session that declares its source. A program in the project with a README is discovered through `bash`, costs nothing until used, and ships as a file rather than a release. Add a builtin only when the model needs it before it can run anything at all.

That rule sets the workspace list. `glob` and `grep` are one search program with different arguments, and that program is already on the machine, so they are not builtins: two schemas in every bound session buying what a shell call already does. `read` stays, because the staleness rule below needs an anchor and because it can cap and paginate where `cat` cannot. Routing search through `bash` makes two incidental things load-bearing — the shell tool caps its own output, and the scrubbed environment still carries a `PATH` with the search tool on it. Scrubbed is not empty.

The web source is that rule's exception rather than a break from it. A session with no shell cannot run a program, and the concierge is exactly that session: unbound, no filesystem, and the one place a spoken question becomes a web lookup. Giving it a shell to reach a CLI would put the widest exposure to untrusted text in front of the tool with the least between it and the machine.

**But the exception no longer needs tools of ours to satisfy it.** A provider that grounds its own answers — searching, reading, and citing server-side — meets the concierge's need without a search credential in arcd, without a cap on unbounded page text, and without two schemas in every unbound session. The web source therefore stays in the registry as a declaration and resolves to whatever the session's provider offers. Where the provider offers nothing, the source is empty, and a bound session reaches the web through `bash` like any other program. Tools of our own get written only when a role needs the web on a provider that cannot ground, and that has not happened.

The cost is a pin. A concierge whose web access comes from its provider is tied to that provider for a capability, not merely for price and latency, and changing it costs a feature rather than a line of configuration. That is the trade, taken knowingly. It also imports the provider's attribution terms into the clients: a grounded answer generally carries a display obligation, which the text client can meet and an audio-only client cannot, so the voice work has to answer that before it does anything else with the web.

**Confinement.** Every path resolves to canonical form and is accepted only if it sits under one of the session's grants — `..`, symlinks, and absolute paths outside them are the obvious cases. `write` and `edit` additionally refuse a path whose grant is read-only, so a session can read notes it cannot change. The check lives in `resolve()`, not the caller, so every tool that touches a path goes through the same gate.

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

Identity loads into the **concierge** role only (§6). The roles that execute jobs have no voice and no personality preamble; paying for one on the bulk of the token spend is waste.

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

These five are the **builtin** source in §4.3's registry. Web and workspace tools use the same registry and events. Future expert and device tools must do the same when they are introduced.

## 6. Providers

One trait in `arc-core`:

```rust
trait Provider {
    fn complete(&self, req: CompletionRequest) -> impl Stream<Item = CompletionDelta>;
    // model listing, token counting, capability flags
}
```

### 6.1 Roles

**A role is a name in config that resolves to a provider and a model.** Three are needed in Phase 3:

| Role | Job | Why it is its own role |
| --- | --- | --- |
| **concierge** | The conversation. Talk, recall, dispatch jobs (§4.1). Loads the identity file and the record index; §5.1 defines its register. | Small token volume, high sensitivity to voice and judgment, latency-critical once §7's voice client lands. Needs vision. |
| **executor** | Job execution. The overwhelming majority of tokens ARC will ever spend. | High volume, mechanical. Cost per *completed task* is the only figure that matters. |
| **archivist** | Consolidation, extraction, classification, and titling. | High volume, low stakes, latency-insensitive. A local model does it for nothing. |

This is how §12's routing question gets answered without a runtime difficulty classifier: the mapping is static config, and the role label rides every `CompletionRequest` onto its span (§8), so traces attribute spend by role from the first day.

**A role resolves to one configured model in Phase 3.** A provider failure is reported to the client. Add an explicit fallback policy only when real outages or spend data show that it is needed.

**The archivist is a role, not a lesser tier.** Its profile — bulk, structured, latency-insensitive — is exactly what a small model is good at and exactly what should never be paid for hosted. The role is named for the work, not for where the model runs, so moving it to a hosted model would not rename it.

**A session is pinned to its role's provider for its lifetime.** This amends the earlier v1 position that sessions do not own a provider. The trait stays per-completion, but prompt caches are model-scoped and prefix-matched, and cache reads dominate the cost of any long agentic session — a mid-session model swap pays for the whole context again. Hot-swapping a live session is therefore no longer a feature to reach for; changing role means a new session or a fork. The engine refuses to continue a session whose recorded role is not its own. Sessions written before roles existed carry `SESSION_ROLE_UNSPECIFIED` and stay unpinned.

### 6.2 Expert consultation is deferred

`consult_expert` is a useful future tool, but it does not belong in the initial job path. Add it when the basic executor job repeatedly needs a separate planning or review model. It must then be a read-only command-backed tool, not a provider or a hard-coded job workflow.

### 6.3 Transport and credentials

The local provider is a llama.cpp `llama-server` sidecar supervised by `arcd`, spoken to as an OpenAI-compatible endpoint (`/v1/chat/completions`, HTTP + SSE, no auth). The same implementation covers vLLM or any OpenAI-compatible server by config. The sidecar releases device memory after an idle window (`--sleep-idle-seconds`), so an always-on daemon holds tens of MiB of VRAM between turns instead of several GiB, and pays about 1.5 s to wake. That is what makes a local default workable on a machine the user also games on.

It is also why the same code reaches most hosted options: an OpenAI-compatible endpoint is a base URL, a key, and a model id.

Hosted providers use plain HTTP and SSE (`reqwest` + rustls), never vendor SDKs. Authentication is replaceable; for now it uses API keys only. The Google OAuth path was removed after hidden rate limits made it unreliable and its terms became questionable. Keys live in `data/secrets/` (0700 and excluded from backups). Phase 3 uses that storage for the concierge and the executor.

Tool-calling and system-prompt differences are normalized in `arc-core`, never leaked to clients. The log records which model actually ran.

### 6.4 The concrete stack is dated

`docs/providers.md` records each current model, its cost, its limits, and the conditions for changing it. Update that file as plans and prices change. Keep this section stable because it defines the architecture.

## 7. Wire protocol and clients

Protobuf over WebSocket (`wire.proto`), served by `arcd` on localhost. Remote access is Tailscale reaching the same socket. ARC does not implement its own tunnel, TLS termination, or auth beyond a local token in v1.

The protocol serves the TUI in Phase 3: send a message, receive streamed deltas and tool-call events, and query sessions and history. Sessions are created implicitly — send with an empty session id and the daemon replies with the assigned one. Clients hold no durable state. Job status can be queried or refreshed by the TUI. Images, modality hints, unsolicited notifications, and additional transports arrive with the clients that require them.

Clients:

- `arc` (TUI): first client, exercises everything — tree navigation, streaming, tool visibility, job status. Should use UDS when local, WebSocket when not.
- `arc-voice`: a thin pipeline — wake word, local ASR, text over this socket, reply text, local TTS. No model logic. Stages sit behind traits like providers do: openWakeWord or Porcupine for the wake word, whisper.cpp for ASR with Silero VAD in front for endpointing, Kokoro for TTS streamed sentence-by-sentence so the first sentence speaks while the model writes the third. Cloud stage backends can slot in later without touching the architecture.
- Mobile: same protocol over Tailscale. Last, after the protocol has been stable under two other clients.

**Speech-to-speech APIs are rejected.** They would own the conversation loop, while the concierge holds memory tools and job dispatch. Handing the loop to a vendor means replumbing those tools through its protocol, and the log stops being where the conversation happens. That breaks invariants 1 and 2, not merely §7's client-agnosticism. Text on the wire is the only shape that keeps the log authoritative and voice provider-independent.

**Voice degrades rather than failing.** With a hosted concierge, a dropped network breaks talking, not just coding. Falling back to the local provider (§6.1) is a Phase 4 exit requirement, with the degraded state visible or audible.

ARC's speaking voice is designed separately from its writing voice (§5.1) and is not a stock persona. Phase 4 ships on a stock Kokoro voice named in config; the real voice is chosen later by living with three or four candidates for a day each rather than by demo impressiveness, then pinned under `data/` as clip, engine, version, **and stage config** — rate, pitch, and sentence-split thresholds shape perceived character as much as timbre, and they are the part that silently drifts across engine upgrades.

## 8. Observability: Perfetto

ARC records `tracing` spans for LLM calls, tool calls, memory operations, and jobs. Existing Perfetto output remains the debugging surface, but Phase 3 adds only the fields needed to diagnose live work: role, job id, latency, and token use. Cost attribution and richer trace structure wait for a decision based on real traces.

## 9. Robotics (future)

Devices integrate as MCP servers, never as bespoke daemon code: an ESP32 pan-tilt rig, later an SO-101-class arm. Two constraints are fixed now:

1. The model plans and issues high-level actions only. Firmware enforces joint limits, speeds, and e-stop. The LLM never commands motors directly.
2. Device MCP servers are separate processes with their own lifecycle. `arcd` treats them like any other tool source.

Both constraints fit the registry shape, but Phase 5 adds the MCP source when the first device exists. Its confirmation flow is designed there against a real actuator, not inherited from Phase 3: a servo that moved cannot be un-moved, which is a different problem from a shell command. §3.1's orphan contract already covers the hard case: a durable call with no durable result means the outcome is *unknown*, never failed, because an actuator that moved cannot be un-moved by a retry.

No robotics code lands before Phase 5.

## 10. Security, backup, and running

- **Always-on** means a systemd user unit (`arcd/arcd.service`): starts with the machine, restarts on failure, logs to the journal. `SIGTERM` is a clean stop, and the sidecar dies with it either way because systemd kills the whole control group. Nothing is left holding the GPU.
- Runtime state lives under one data directory: log, index, identity, traces, secrets. `data/` in a checkout, `~/.local/state/arc/` once installed, with config at `~/.config/arc/arc.toml`. Installed layout matters beyond tidiness: a data directory inside a checkout sits within a root the workspace tools can be granted.
- Backup is rustic, encrypted at the repository level, covering `log/` and `identity.md` in the data directory. `index.db` and `traces/` are excluded — both are rebuildable.
- Credentials live in the OS keychain or an encrypted secrets file under `secrets/` in the data directory (0700 and excluded from backups). They never enter the log or backups. Phase 3 uses credentials for the concierge and the executor.
- **Workspace tools run with a scrubbed environment** (§4.3). `bash` is the first thing ARC runs that could read its own process environment, and the answer is that there is nothing there to read — no keys, no tokens. arcd holds credentials; the tools it spawns do not inherit them.
- The WebSocket binds localhost only. Remote access is Tailscale's problem, by design.

## 11. Phases

Each phase ends in something used daily. No phase starts until the previous one is a daily driver, because real usage is the input to the next phase's design — especially for memory.

**Phase 0 — Scaffold.** *Done.* Workspace, empty crates, empty schemas, build/test/fmt/lint targets.

**Phase 1 — Walking skeleton.** *Done 2026-08-13.* `arcd` with the local provider, linear sessions, the event log with `SessionEvent`, the SQLite projection, the TUI with streaming, the identity file in context, Perfetto spans on LLM calls. Memory is *only* the identity file. Exit criterion — ARC replaces a chat app for daily use — is met where it counts: ARC gets the simple questions, daily. The gaps left are not Phase 1's to close; tools arrive with memory and devices, and a better model is a config line.

**Phase 2 — Memory.** *Done 2026-08-22.* `MemoryEvent`, distilled records and the always-loaded index, the five memory and archive tools, FTS5 over messages, explicit `memory_write` plus end-of-session consolidation, `arcd memory-replay` with a versioned prompt, the weekly TUI review, Perfetto spans on every memory operation. Exit criterion: "what do you know about X" and "what did we say about X" both work on real history.

**Phase 3 — Development.** ARC becomes the way its own code gets written, and runs in production. Configured concierge, executor, and archivist roles; jobs (§4.1); workspaces (§4.2); builtin, web, and workspace tools with containment (§4.3); and `arcd rebuild` proven against the real log. Installed as a systemd user unit with a data directory that survives a rebuild. Exit criterion: a week of real development done through ARC rather than through another harness, and a full rebuild matching live state.

**Phase 3.5 — Tree.** Session forking with §4's branch semantics, rewind, and tree navigation in the TUI. Split out of Phase 3 and kept immediately after it because rewind is a development feature: recovering from a bad edit path without re-prompting from scratch is what makes a cheap `executor` model affordable. Exit criterion: branching gets used naturally.

**Phase 4 — Voice + remote.** `arc-voice` per §7 — wake word, local ASR, text on the wire, local TTS — the daemon reached from a phone over Tailscale (the mobile client can start as the TUI over SSH), and rustic backup automated. Exit criteria: a restore drill rather than a backup existing, and voice degrading to the local provider when the network is gone.

**Phase 5 — Devices.** The first device MCP server (ESP32 pan-tilt) as a source in §4.3's registry, device-tool safety conventions designed against the first real actuator, then the arm. A wake-word room satellite, if one appears, is a §7 *client* and not a device — same board, different integration path, and conflating them would put a special case in the device layer. sqlite-vec embeddings land here, or earlier only if Phase 2–4 usage shows FTS falling short.

## 12. Open questions

Deferred on purpose. Decide when the phase forces it.

- **Consolidation triggering:** idle timeout vs explicit session close vs continuous. The v1 placeholder is a configurable idle timeout, so the pass has something to hang on. Traces judge it. (Phase 2.)
- ~~**Model routing.**~~ **Decided.** Configuration assigns static roles; there is no runtime difficulty classifier, and every trace span records its role. Two questions remain: whether roles need task-specific labels (for example, consolidation and titling may need different timeouts and concurrency), and whether the concierge can dispatch reliably enough or needs a stronger model just for dispatch. Phase 3 traces should answer both.
- **Compaction and the log.** A long job (§4.1) will exceed any context window, and summarising its own history is a durable decision that changes what the model sees. Replay must reproduce it, so it cannot be an in-memory convenience. Likely a new kind inside `SessionEvent`, recording what was compacted and the prompt version that did it — the same shape as `SessionConsolidated` in §5.4. Decide when the first job hits the limit, not before. (Phase 3.)
- **Voice stage placement.** With the concierge hosted, the GPU is free during a voice turn — the sidecar is asleep and consolidation only runs on idle sessions. The Phase 4 plan's "whisper on CPU so the GPU stays free" constraint may no longer apply, which would allow a larger ASR model or GPU-side TTS for faster first audio. Measure in Phase 4 rather than inheriting the assumption. (Phase 4.)
- **Identity edits in the log.** Revisit if hand-editing becomes a bottleneck.
- **Embeddings model for sqlite-vec,** local or API. (Phase 4/5.)
- **Multi-machine beyond backup/restore** (log sync). (Post-v1.)
- **Startup recovery** is a full replay today. A checkpoint bounds it when the log grows. (When startup time or traces say so.)
