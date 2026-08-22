# ARC — Autonomous Robotic Core

## Design Document

**Status:** v1, governs initial implementation. Amend this file before diverging from it.

---

## 1. What ARC is

ARC is a personal AI assistant: an always-on Rust daemon (`arcd`) with thin clients (TUI, voice, mobile) on a WebSocket. It has durable memory, a stable identity, pi-style branching sessions, and swappable LLM providers. Long term it is the control plane for the machine, cloud tools, and a robotics workbench, where devices appear as MCP servers.

Priorities, in order:

1. **Durability.** Memory and history survive machine, provider, and schema changes. One source of truth; everything else is rebuildable.
2. **Observability.** Every LLM call, tool call, and memory operation is traceable. ARC emits Perfetto traces of itself.
3. **Speed.** No GC, small idle footprint, protobuf on disk and on the wire.
4. **Independence.** Providers, auth, and models sit behind one interface. Local models are a planned path, not an afterthought.

Not in v1: multi-user, cloud hosting, plugin sandboxing, robotics code. The architecture leaves room; the code does not attempt them.

## 2. Repository layout

Five crates:

- `arc-proto` — every protobuf schema (`events.proto`, `memory.proto`, `wire.proto`, package `arc.v1`) and its prost types. The single schema authority: log and wire share the generated types, and no other crate defines a serialized format. One schema is not ours: `perfetto.proto` is a trimmed copy of Perfetto's, in package `perfetto.protos`. Its field numbers are upstream's and must match exactly, so §3's additive rules do not govern it. Nothing in it is ever written to the log.
- `arc-core` — all logic: log, projections, provider abstraction, memory tools, tracing. Testable without a running daemon.
- `arcd` — the daemon. Owns the log, runs projections, serves the WebSocket, holds credentials, runs consolidation.
- `arc` — the TUI client.
- `arc-voice` — the wake-word voice client.

A future `arc-mobile` speaks the same protocol and depends only on `arc-proto`.

## 3. The event log

The log is the single source of truth for everything durable except the identity file. It is an append-only run of length-prefixed protobuf records under `data/log/`. Each record carries a CRC32 of its payload: the length prefix catches truncation, the CRC catches corruption. Files are split by size so backups stay cheap.

Segment mechanics, load-bearing for durability:

- **Naming.** A segment is named for the seq of its first event, padded to 20 digits (`00000000000000004711.log`). Name order is log order, and any seq is locatable without opening a file. If a segment dies on its first record, its replacement takes a `_1`, `_2`, … suffix. Ordering still holds. This is deliberate.
- **Sealing.** A segment is sealed when a later one exists. Creating the successor is the seal; there are no marker files.
- **Recovery seals, never truncates.** A torn tail — an append the machine died inside — stays on disk untouched, and the next segment starts at the recovered seq. Replay tolerates torn bytes and proves integrity by seq instead: seqs are gapless across every boundary, and a full replay must start at seq 0. So neither a tear nor a lost segment can hide missing records.
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

Three rules make the model work:

1. **Only an append changes durable state.** Model writes, user hand-edits, and migrations all append. None edits prior bytes.
2. **Everything else is a projection.** The SQLite index, current memory state, and session trees are deterministic replays. Delete any of them and rebuild.
3. **Schema changes are additive.** Fields are never renumbered or repurposed; old events must always decode. Forward compatibility has one boundary: a new *kind* inside an existing payload arm is skipped safely on replay, but a new top-level `Event.payload` arm decodes as empty on an older binary and reads as corruption. So replaying a log needs a binary at least as new as its newest payload arm. That is fine while writer and reader ship together; revisit if they stop.

**Durability.** v1 fsyncs every append; batching needs trace data to justify it. Expect the question in Phase 2/3, when tool loops turn one user turn into many events. The shape to evaluate: a fixed coalescing window (~200 ms) draining to one fsync per batch, with an explicit flush before a turn is called complete. Any batching keeps the writer's contract — seq stamped internally, a written-but-unfsynced record still consumes its seq, a failed writer is rebuilt and never retried.

**Consequences.** Backup is "copy the segments." Moving machines is "copy log + identity file, replay." There is no live-database backup problem, because SQLite is disposable.

### 3.1 Tool-call events

Tool use appends two kinds inside `SessionEvent`: `ToolCallIssued` and `ToolResultRecorded`.

Not fields on `MessageAppended`: its `content` is the prose the user saw and what §5.3 indexes, and a sometimes-empty `content` beside a calls field would force every reader to learn a new shape. Not a new top-level payload arm either, because rule 3 makes that a replay hazard while a new kind inside an existing arm is skip-safe.

**One turn.** Reasoning, two parallel calls, their results, final text:

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

**Turns are an id, not events.** No `TurnStarted`/`TurnEnded`. A `turn_id` is minted when a user message opens a turn and carried by every event in it. That groups just as well, without two extra fsyncs and without a second source of truth that can disagree with the messages. Phase 1 events decode with `turn_id` empty, which reads as "one message, one turn" — exactly true of a log with no tools.

Within a turn, grouping is by seq: an assistant text message plus the calls after it, with no result between, is one step. That works only because events are filtered by `turn_id` first. The raw log interleaves sessions and payload arms, so adjacency in it means nothing.

**Per-call identity.** A call is named by `call_id`, the provider's own id recorded verbatim, and ordered by `index`, dense from 0, restarting each step. Parallel calls are real, and a parser keyed on anything but `index` silently merges them.

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

**Write order and resume.** `ToolCallIssued` is appended and fsynced before its tool runs, and a step's whole batch is durable before any of them runs. That is what makes the log's silence meaningful: nothing ran that is not on disk.

It also creates the one case that matters: a durable call with no durable result. A replayer concludes exactly one thing — **the outcome is unknown.** Not failed. The tool may never have started, may have run and had its effect before dying, or may have had its result torn off by the crash. The bytes cannot tell these apart, and a tool that moved an actuator (§9) moved it either way. So the call is never silently re-dispatched and never silently dropped. Detection is cheap: replay carries a set of open `call_id`s and removes each on its result. What remains is orphaned.

Only arcd may act on an orphan, at startup, before it dispatches anything for that session. An in-flight call in a live daemon looks identical on disk to an abandoned one, so "orphaned" is a property of the log *plus* nobody running. That is why the repair is an appended event rather than something each reader invents: `arcd rebuild`, `memory-replay`, and the projection all read an orphan as unknown and append nothing.

At startup arcd appends, per orphan, a `ToolResultRecorded{outcome: UNKNOWN}` with `Event.source = SYSTEM`, whose content is a fixed sentence: the daemon restarted before the result was recorded, and the call may or may not have run. The call is now closed durably, the next replay is clean, and every `tool_call_id` in the rebuilt transcript has an answer. The closer lands at the log tail, maybe hours later — the second reason correlation is by `call_id` and not adjacency.

arcd does not re-drive the model. A restarted daemon resuming a turn the user walked away from is a surprise, and the cost of not doing it is that the user types "continue." The turn resumes on the next user message, which follows a tool result perfectly well.

**Tool errors are results.** A tool that fails — bad arguments, missing file, timeout, no such tool, denied — produces `ToolResultRecorded{outcome: ERROR}` with the text the model should see, and the loop continues. §4's "errors go to clients, never archived" covers ARC failing at its own job: provider unreachable, malformed frame, log write failed.

The boundary is one line: **if the model will see it, it is durable; if only the user sees it, it is a wire `Error`.** A tool error changes the conversation and the model must reason about it. A provider outage changes nothing and the fix is to ask again. If the log append itself fails, nothing durable happened, the turn is abandoned, and the client gets an `Error`. Readers treat an unrecognized `ToolOutcome` as UNKNOWN, never ERROR — unknown carries the safe behaviour.

**`partial` does not extend to calls.** It stays on `MessageAppended` and stays about text. A call is appended only once its arguments are complete, so a half-streamed call is never appended and there is nothing to mark. A result arrives whole. What marks a turn cut mid-loop is the orphaned call. Two mechanisms for two different failures: cut text loses only what the user did not see, while a cut loop may have left an effect in the world.

**Size.** The 16 MiB cap is the backstop, not the policy. The tool registry truncates before the event is built, to a configurable `max_tool_result_bytes` — 32 KiB to start, because the real constraint is an 8k-context model, not the disk. Truncation sets `truncated` and leaves a marker in the content. It is lossy on purpose: nothing else stores the full result, which is the rule `partial` already encodes — the log keeps what was seen. The registry enforces it because it knows the tool and can cut meaningfully; a log-layer refusal would leave the loop holding a result it cannot record. An event still over 16 MiB is a registry bug, and the log's refusal catches it.

**Secrets.** Invariant 5 holds at the tool boundary: a result entering the log contains no secret, and **the tool owns that.** Not the log writer, which cannot tell. Not a regex scrubber, which is a false promise. A tool that touches credentials returns a reference, never a value. Arguments are covered from the other side — the model can only echo what it was given, and credentials never enter model context (§10). Airtight only for tools that cannot read their own environment; Phase 2 has none.

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

## 4. Sessions

Sessions are pi-style: a tree, not a list. Forking at any message creates a child with a `parent_session` and `fork_point`. The tree is just parent pointers in the projection. Clients render it; the daemon only stores it.

Branches interact with memory (§5.4): only the main line and branches marked *real* feed consolidation. Abandoned branches stay searchable in the archive but never write distilled memory.

A reply cut mid-stream is appended with `partial = true` — the log records what the user actually saw. Errors go to clients and are never archived as messages.

## 5. Memory

"Feels alive" splits into three subsystems with different storage, update rules, and retrieval paths. They are deliberately not unified.

### 5.1 Identity

One small human-owned file: `data/identity.md`. Who ARC is, how it talks, stable facts about its user. Loaded into context every session, unconditionally. A few KB — small enough that loading it is never a decision.

Identity is exempt from event-sourcing. It is a plain file, versioned in git and backed up beside the log. ARC may propose edits as ordinary session output; the user applies them by editing the file. `IdentityEvent` is reserved in case this changes. v1 never emits it.

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

Provenance is load-bearing: every fact can answer "where did you learn that" by pointing into the archive.

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

## 6. Providers

One trait in `arc-core`:

```rust
trait Provider {
    fn complete(&self, req: CompletionRequest) -> impl Stream<Item = CompletionDelta>;
    // model listing, token counting, capability flags
}
```

v1's provider is local: a llama.cpp `llama-server` sidecar supervised by `arcd`, spoken to as an OpenAI-compatible endpoint (`/v1/chat/completions`, HTTP + SSE, no auth). The same implementation covers vLLM or any OpenAI-compatible server by config. The sidecar releases device memory after an idle window (`--sleep-idle-seconds`), so an always-on daemon holds tens of MiB of VRAM between turns instead of several GiB, and pays about 1.5 s to wake. That is what makes a local default workable on a machine the user also games on.

Hosted providers (Anthropic, Gemini) come later behind the same trait, as plain HTTP + SSE (`reqwest` + rustls), never vendor SDKs. Auth is a swappable layer; API keys first. Nothing hosted ships today: the original Google path was removed outright — provider, OAuth flow, `arcd login`, token file — after hidden rate limits made it unreliable and its ToS gray area stopped being worth carrying. `data/secrets/` remains (0700, empty) as the seam for the first keyed provider.

Provider choice is per-completion: a global default, optionally overridden per request. Sessions do not own a provider; the log records what actually ran. Tool-calling and system-prompt differences are normalized in `arc-core`, never leaked to clients. Providers are hot-swappable mid-session — by voice, or by the agent picking a cheap model for a subtask.

## 7. Wire protocol and clients

Protobuf over WebSocket (`wire.proto`), served by `arcd` on localhost. Remote access is Tailscale reaching the same socket. ARC does not implement its own tunnel, TLS termination, or auth beyond a local token in v1.

The protocol is client-agnostic: subscribe to a session, send a message, fork, receive streamed deltas and tool-call events, query the tree. Sessions are created implicitly — send with an empty session id and the daemon replies with the assigned one. Clients hold no durable state.

- `arc` (TUI): first client, exercises everything — tree navigation, streaming, memory tool visibility. Should use UDS when local, WebSocket when not.
- `arc-voice`: wake word (openWakeWord or Porcupine), local ASR (whisper.cpp), local or hosted TTS — decide later. A thin client: audio in, text over the socket, audio out. No model logic.
- Mobile: same protocol over Tailscale. Last, after the protocol has been stable under two other clients.

## 8. Observability: Perfetto

ARC emits Perfetto protobuf traces of its own operation: a track per session and branch, spans for LLM calls (with token counts as counters), tool calls, memory reads and writes, consolidation passes, and log replays. Traces land in `data/traces/` and open directly in the Perfetto UI.

This is not decoration. It is the debugging surface for the two genuinely hard subsystems — consolidation quality and retrieval behaviour — and it makes latency and token spend measurable from day one. Implementation: a `tracing` subscriber in `arc-core` that renders `TracePacket` protos.

## 9. Robotics (future)

Devices integrate as MCP servers, never as bespoke daemon code: an ESP32 pan-tilt rig, later an SO-101-class arm. Two constraints are fixed now:

1. The model plans and issues high-level actions only. Firmware enforces joint limits, speeds, and e-stop. The LLM never commands motors directly.
2. Device MCP servers are separate processes with their own lifecycle. `arcd` treats them like any other tool source.

No robotics code lands before Phase 5.

## 10. Security, backup, and running

- **Always-on** means a systemd user unit (`arcd/arcd.service`): starts with the machine, restarts on failure, logs to the journal. `SIGTERM` is a clean stop, and the sidecar dies with it either way because systemd kills the whole control group. Nothing is left holding the GPU.
- Runtime state lives under `data/`: log, index, identity, traces.
- Backup is rustic, encrypted at the repository level, covering `data/log/` and `data/identity.md`. `data/index.db` and `data/traces/` are excluded — both are rebuildable.
- Credentials live in the OS keychain or an encrypted secrets file. Never in the log, never in backups. Nothing holds one today; the local provider has no auth. `data/secrets/` (0700, excluded from backups) is the seam, and keychain integration arrives with the first provider that needs a key.
- The WebSocket binds localhost only. Remote access is Tailscale's problem, by design.

## 11. Phases

Each phase ends in something used daily. No phase starts until the previous one is a daily driver, because real usage is the input to the next phase's design — especially for memory.

**Phase 0 — Scaffold.** *Done.* Workspace, empty crates, empty schemas, build/test/fmt/lint targets.

**Phase 1 — Walking skeleton.** *Done 2026-08-13.* `arcd` with the local provider, linear sessions, the event log with `SessionEvent`, the SQLite projection, the TUI with streaming, the identity file in context, Perfetto spans on LLM calls. Memory is *only* the identity file. Exit criterion — ARC replaces a chat app for daily use — is met where it counts: ARC gets the simple questions, daily. The gaps left are not Phase 1's to close; tools arrive with memory and devices, and a better model is a config line.

**Phase 2 — Memory.** *Done 2026-08-22.* `MemoryEvent`, distilled records and the always-loaded index, the five memory and archive tools, FTS5 over messages, explicit `memory_write` plus end-of-session consolidation, `arcd memory-replay` with a versioned prompt, the weekly TUI review, Perfetto spans on every memory operation. Exit criterion: "what do you know about X" and "what did we say about X" both work on real history.

**Phase 3 — Tree + second provider.** Session forking with §4's branch semantics, tree navigation in the TUI, a Gemini provider, per-session provider choice, and `arcd rebuild` proven against the real log. Exit criterion: branching gets used naturally, and a full rebuild matches live state.

**Phase 4 — Voice + remote.** `arc-voice` (wake word, local ASR/TTS), the daemon reached from a phone over Tailscale (the mobile client can start as the TUI over SSH), and rustic backup automated. The exit criterion is a restore drill, not a backup existing.

**Phase 5 — Hands.** The first device MCP server (ESP32 pan-tilt), device-tool safety conventions, then the arm. sqlite-vec embeddings land here, or earlier only if Phase 2–4 usage shows FTS falling short.

## 12. Open questions

Deferred on purpose. Decide when the phase forces it.

- **Consolidation triggering:** idle timeout vs explicit session close vs continuous. The v1 placeholder is a configurable idle timeout, so the pass has something to hang on. Traces judge it. (Phase 2.)
- **Model routing.** The ambition is our own router, picking the best model per request across local and hosted (OpenRouter). §6's per-completion choice is the seam. Not now: a router needs usage data, and Phase 2 generates it. hermes-agent converged on aux-default = the main model after shipping the opposite, and hangs its whole routing surface off a per-task label at one chokepoint — so the only obligation now is that background requests carry a task name that reaches their spans. (Post-Phase 2.)
- **Identity edits in the log.** Revisit if hand-editing becomes a bottleneck.
- **Embeddings model for sqlite-vec,** local or API. (Phase 4/5.)
- **Multi-machine beyond backup/restore** (log sync). (Post-v1.)
- **Startup recovery** is a full replay today. A checkpoint bounds it when the log grows. (When startup time or traces say so.)
