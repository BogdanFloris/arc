# ARC — Autonomous Robotic Core

## Design Document

**Status:** v1, governs initial implementation. Amend this file before diverging from it.

---

## 1. What ARC is

ARC is a personal AI assistant built as an always-on Rust daemon (`arcd`) with thin clients (TUI, voice daemon, mobile app) connected over a WebSocket. It has durable memory, a stable identity, pi-style branching sessions, and multi-provider LLM support with no vendor lock-in. Long term, it is the control plane for everything the user wants to interact with: the underlying os, cloud tools, robotics workbench: physical devices are exposed to ARC as MCP servers, starting with small actuators and eventually a robotic arm.

Design priorities, in order:

1. **Durability.** Memory and history survive machine changes, provider changes, and schema evolution. One source of truth, everything else rebuildable.
2. **Observability.** Every LLM call, tool call, and memory operation is traceable. ARC emits Perfetto traces of its own behavior.
3. **Speed.** Systems-level engineering: no GC, small idle footprint, protobuf on disk and on the wire.
4. **Independence.** Providers, auth methods, and models are swappable behind one interface. Local models are a planned path, not an afterthought.

Non-goals for v1: multi-user support, cloud hosting, plugin sandboxing, and any robotics code. The architecture leaves room for these; the code does not attempt them.

## 2. Repository layout

Cargo workspace with five crates:

- `arc-proto` — all protobuf schemas (`events.proto`, `memory.proto`, `wire.proto`, package `arc.v1`) and generated types via prost. The single schema authority: the event log on disk and the WebSocket wire protocol use the same generated types. No other crate defines serialized formats. One schema there is not ours (added 2026-08-13): `perfetto.proto` is a hand-trimmed subset of Perfetto's own, in its own `perfetto.protos` package. Its field numbers are upstream's and must match them exactly, so §3's additive rules do not govern it; nothing in it is ever written to the log.
- `arc-core` — shared library: event log reader/writer, projections (SQLite index, memory state), provider abstraction, memory tools, tracing integration. All logic lives here so the daemon stays a thin composition layer and logic is testable without a running daemon.
- `arcd` — the daemon binary. Owns the event log, runs projections, serves the WebSocket, holds provider credentials, runs the consolidation pipeline.
- `arc` — the TUI client.
- `arc-voice` — the wake-word voice client.

A future `arc-mobile` app consumes the same wire protocol; it is a separate (non-Rust or Rust-core + native shell) project and only depends on `arc-proto` schemas.

## 3. The event log

The event log is the single source of truth for everything durable except the identity file. It is an append-only sequence of length-prefixed protobuf messages on disk under `data/log/`, each record carrying a CRC32 of its payload (truncation is caught by the length prefix; corruption by the CRC), segmented into files by size for backup friendliness.

Segment mechanics, fixed by implementation and load-bearing for durability:

- **Naming.** A segment is named by the seq of its first event, zero-padded to 20 digits (`00000000000000004711.log`): name order is log order, and any seq is locatable without opening files. If a name is spent by a segment that died on its very first record, the replacement takes a `_1`, `_2`, … suffix — ordering still holds; this is deliberate, not a bug.
- **Sealing.** A segment is sealed exactly when a later segment exists. No marker files; creating the successor is the seal.
- **Recovery seals, never truncates.** A torn tail (an append the machine died inside) stays on disk untouched; the next segment starts at the recovered seq. Replay tolerates torn bytes and proves integrity through seq continuity instead: gapless seqs across every boundary, and a full replay must start at seq 0 — so neither a tear nor a lost segment (head included) can hide missing records.
- **Record cap.** One encoded event is at most 16 MiB. This is a product constraint, not framing trivia: no memory record or tool result may exceed it, and it is what lets the reader refuse an absurd length prefix before trusting it with an allocation.

```proto
message Event {
  uint64 seq = 1;            // monotonic, gapless
  google.protobuf.Timestamp ts = 2;
  Source source = 3;         // MODEL, USER, SYSTEM
  oneof payload {
    SessionEvent session = 10;    // message appended, session created, branch forked, tool call + result
    MemoryEvent memory = 11;      // record created / updated / superseded / deleted
    IdentityEvent identity = 12;  // reserved; see §5.1
  }
}
```

Rules that make the model work:

1. **Nothing mutates durable state except by appending an event.** Model-initiated memory writes, user hand-edits, and system migrations all append; they never edit prior bytes.
2. **All state is a projection.** The SQLite index, the current distilled-memory state, and session trees are all deterministic replays of the log. Any projection can be deleted and rebuilt at any time.
3. **Schema evolution is additive.** Fields are never renumbered or repurposed; old events must always decode. Forward compatibility (new events on an old binary) has one deliberate boundary: a new *kind* inside an existing payload arm is skipped safely during replay, but a new top-level `Event.payload` arm decodes as empty on an older binary and reads as corruption — replaying a log requires a binary at least as new as its newest payload arm. Acceptable for a single-user system whose writer and reader ship together; revisit if that stops being true.

Durability policy: v1 fsyncs every append — durability beats throughput, and batching is an optimization that needs trace data to justify it. The expected trigger is Phase 2/3, when tool loops turn one user turn into many events. When traces justify it, the shape to evaluate is a small fixed coalescing window (~200ms) draining to one fsync per batch, with an explicit flush checkpoint before a turn is claimed complete (cf. DeepSeek Harness's bounded write batching). Any batching change must preserve the writer's existing contract: seq stamped internally, a written-but-unfsynced record still consumes its seq, failed writer is rebuilt, never retried.

Consequences: backup is "back up the log segments" (rustic handles append-only files efficiently), transfer to a new machine is "copy log + identity file, replay," and there is no live-database backup problem because SQLite is disposable.

### 3.1 Tool-call events

Tool use appends first-class events inside `SessionEvent`: `ToolCallIssued` and `ToolResultRecorded`. Not fields bolted onto `MessageAppended`, whose `content` is the prose the user saw and is what §5.3's FTS indexes — a sometimes-empty `content` beside a repeated calls field would make every existing reader learn a new shape. Not a new top-level `Event.payload` arm either: §3 rule 3 makes that a replay hazard, while a new kind inside an existing arm is skipped safely by an older binary. That skip-safety is the whole reason the vocabulary lives here. Sketches below carry no field numbers; 2.2 owns numbering.

**The event set.** One turn — reasoning, two parallel calls, their results, final text — appends exactly this:

```
user asks                    MessageAppended{USER, content, turn_id=T}
model reasons                — nothing durable
step 0 text, if any          MessageAppended{ASSISTANT, content, turn_id=T}
step 0 call index 0          ToolCallIssued{T, call_id=a, index=0, name, arguments_json}
step 0 call index 1          ToolCallIssued{T, call_id=b, index=1, name, arguments_json}
                             → both tools dispatch, b finishes first
                             ToolResultRecorded{T, call_id=b, OK, content}
                             ToolResultRecorded{T, call_id=a, OK, content}
step 1 final text            MessageAppended{ASSISTANT, content, partial=false, turn_id=T}
```

Reasoning appends nothing: streamed, never durable (banked 2026-08-14). A step that only calls tools appends no `MessageAppended` at all — the call events are the assistant's utterance for that step, and 1.1 found text and calls mutually exclusive in all 71 captures, so this is the ordinary case, not the exotic one. Results append in completion order, not index order, because the log records what happened when; the provider transcript sorts them by the index of the call each one closes. `Event.source` is `MODEL` on a call (the model asked for it) and `SYSTEM` on a result (arcd ran it).

**Turn boundaries are an id, not events.** No `TurnStarted`/`TurnEnded`. A `turn_id`, minted when a user message opens a turn and carried by every event that turn produces, does the grouping turn events would do, without two extra fsyncs per turn and without a second source of truth about whether a turn finished that can disagree with the messages. `MessageAppended` gains `turn_id` as a new field; Phase 1 events decode with it empty, and a projection reads empty as "one message, one turn," which is exactly true of a log with no tools in it. Grouping *within* a turn is by seq order — an assistant text message plus the calls that follow it with no result in between is one assistant step — and that rule is only sound because events are filtered by `turn_id` first. The raw log interleaves sessions and payload arms, so adjacency in it means nothing.

**Per-call identity.** A call is named by `call_id`, the provider's own id recorded verbatim, and ordered by `index`, the provider's index within its step, dense from 0 (1.1: parallel calls are real, and a parser keyed on anything but `index` silently merges them). `index` restarts at 0 each step; `call_id` is the unique key, scoped to the session. If a provider gives no id, arcd mints one; if an incoming id collides with any call the session has already logged — open or closed — arcd mints a replacement, because the projection's join and the rebuilt provider transcript both meet the whole session, and either one meeting the same string on two calls is an ambiguity the open-call set never sees. Either way the log records the id that was used, replay reads it rather than regenerating it, and the transcript arcd rebuilds uses the same string throughout, so the provider never sees a mismatch. A result names its call by `call_id` and nothing else — no denormalized tool name, because a second copy of a fact is a second thing that can be wrong, and the projection joins once. The dialect's `type: "function"` is not recorded: it has exactly one value, and a second one arrives as a new field, not a new string.

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

**Write order and the resume contract.** `ToolCallIssued` is appended and fsynced before its tool is dispatched, and the whole batch of a step's calls is durable before any of them runs. That write-ahead is what makes the log's silence meaningful: nothing ran that is not on disk. It also creates the only case that matters here — a durable call with no durable result. A replayer concludes exactly one thing from it: **the outcome is unknown.** Not failed. The tool may never have started, may have run and had its effect and died before its result was appended, or may have had its result torn by the crash and dropped by recovery; the bytes cannot tell these apart, and a tool that moved an actuator (§9) has moved it either way. So the call must never be silently re-dispatched, and it must never be silently dropped. Detection is cheap: replay carries a set of open `call_id`s, removes each on its result, and whatever remains at the end of the log is orphaned.

Only arcd, at startup, may act on an orphan, and only before it dispatches anything for that session. An in-flight call in a live daemon is byte-identical on disk to an abandoned one, so "orphaned" is not a property of the log alone — it is a property of the log plus the fact that nobody is running. That is why the repair is an appended event rather than something each reader synthesizes: `arcd rebuild`, `memory-replay`, and the projection all read an orphan as unknown-outcome and append nothing. At startup, arcd appends for each orphan a `ToolResultRecorded{outcome: UNKNOWN, content: a fixed sentence saying the daemon restarted before the result was recorded and the call may or may not have run}`, `Event.source = SYSTEM`. This closes the call durably, so the next replay is clean and every `tool_call_id` in the reconstructed transcript has a tool message answering it. The closer lands at the log tail, possibly hours after the call it closes — which is the second reason correlation is by `call_id` and not by adjacency. arcd does not then re-drive the model: a restarted daemon resuming a turn the user walked away from is a surprise, and the cost of not doing it is that the user types "continue." The turn resumes on the next user message, which follows a tool result perfectly well. This is 4.3's contract, in full.

**Tool errors are results, not wire errors.** A tool that fails — bad arguments, missing file, timeout, no such tool, denied — produces `ToolResultRecorded{outcome: ERROR}` with the error text the model should see, and the loop continues. §4's "errors are reported to clients, never archived" governs ARC failing to do its job: provider unreachable, malformed frame, log write failed. The boundary is one line: **if the model is going to see it, it is durable; if only the user sees it, it is a wire `Error`.** A tool error changes the conversation and the model must reason about it; a provider outage changes nothing and the fix is to ask again. The corollary bites in the right direction too — if the log append itself fails, nothing durable happened, the turn is abandoned, and the client gets an `Error`. Readers must treat an unrecognized `ToolOutcome` as UNKNOWN, not as ERROR: unknown is the value with the conservative behaviour attached.

**The partial rule does not extend.** `partial` stays on `MessageAppended` and stays about text. A call is appended only once its arguments are complete — 3.2 accumulates fragments by index and the JSON is valid only concatenated — so a half-streamed call is never appended and there is nothing to mark. A result is produced whole by the tool. What marks a turn cut mid-loop is the orphaned call itself, closed as above. Two mechanisms because two different failures: cut text loses only what the user did not see, while a cut loop may have left an effect in the world, and only the second needs UNKNOWN to say so.

**Size.** §3's 16 MiB record cap covers tool results, but it is the backstop, not the policy. The tool registry (4.1) truncates before the event is built, to a configurable `max_tool_result_bytes` far below the cap — 32 KiB to start, because the real constraint is an 8k-context model, not the disk. Truncation sets `truncated` and leaves an explicit marker in the content. It is lossy and deliberate: nothing else stores the full result, and that is the same rule `partial` already encodes — the log keeps what was actually seen. Enforcement belongs to the registry because it knows the tool and can cut meaningfully; a log-layer refusal would leave the loop holding a result it cannot record and force it to invent the same truncation later with less context. An event that still exceeds 16 MiB is a registry bug, and the log's refusal is the assertion that catches it.

**Secrets.** Invariant 5 holds at the tool boundary: a tool result entering the log must contain no secret, and **the tool that produces it owns that** — not the log writer, which cannot tell, and not a regex scrubber, which is a false promise. A tool that touches credentials returns a reference, never a value. Arguments are covered by the same contract from the other side: the model can only echo what it was given, and credentials never enter model context (§10 — they live in the keychain or `data/secrets/`, and the provider layer injects auth into headers, never into prompt text). The contract is airtight only for tools that cannot read their own environment; Phase 2 has none.

**Reserved, not built.** Reserve one number in the `SessionEvent.event` oneof for a future reasoning event. Reasoning is streamed-only by decision, and this keeps "durable after all" a schema addition rather than a migration — same move as `SessionCreated`'s reserved fork fields. Reserving a oneof number beats reserving a field on `MessageAppended`, because a call-only step has no `MessageAppended` to hang reasoning on. Three field-level reserves are worth taking while the messages are new: on `ToolCallIssued`, a tool *source* (an MCP server id — §9's devices are MCP servers, and two servers can both offer `read`) and the model that issued the call (§6 makes provider choice per-completion, and today only `SessionCreated` records what ran); on `ToolResultRecorded`, a structured-content field, since `content` is a string and an image or binary result needs its own. Not reserved: call duration. Latency is trace material (§8), and putting it in the log would make every replay assert timing it cannot reproduce.

**What the projection will need.** 5.1's messages shape stops being `(role, content)` rows: it needs tool calls and results as their own rows (or a sibling table) keyed by `call_id`, `turn_id` on every row so a reopened session can rebuild both the display and a valid provider transcript, the tool name and outcome as columns so "when did you last write a memory record" is a query, and the reserved `partial` and new `truncated` flags so a cut reply and a cut result are distinguishable from whole ones. FTS should index tool result content, but tagged by row kind and excluded from `sessions_search`'s default: a 30 KB directory listing the model read would otherwise outrank the sentence the user wrote. Arguments are not indexed — they are small JSON, and the call is already findable by name.

**Prior art (DeepSeek Harness, session persistence).** Taken: the write-ahead checkpoint — "a recorded top-level call before tool dispatch" is exactly our durability point. Taken: closing an unanswered call with a synthetic result so the resumed transcript stays valid, except that theirs injects a risk-classified *error* and ours records UNKNOWN, because unknown is the honest classification and the one that forbids silent retry. Rejected: `turn/start` and `turn/end` events — `turn_id` over a gapless seq gives the same grouping without two fsyncs a turn. Rejected: persisting `assistant/chunk` delta runs — the log stores what the user saw, not how it arrived. Rejected: a generated catalog of string event types with an `ignorable` escape hatch — proto oneof numbers plus §3 rule 3 already give skip-safety, and string types would put a second schema authority outside `arc-proto`.

Open, and deliberately left to the task that hits them:

- open: whether an UNKNOWN call may ever be re-dispatched automatically for a tool that declares itself idempotent. 4.1's registry trait has no idempotence declaration; if one lands, this contract gains a branch.
- open: the FTS default for tool-result rows is 5.1/5.2's call to confirm against real queries; the sketch only insists the rows be tagged so the choice stays a query change, not a re-projection.
- open: the redaction policy for a tool that can incidentally capture its environment (a shell tool, a device tool that echoes config). None exists in Phase 2; the first one that does decides it, and it is a tool-side policy either way.

## 4. Sessions

Sessions are pi-style: a tree, not a list. Forking a session at any message creates a child session with a `parent_session` and `fork_point`; the tree structure is just parent pointers in the projection. Clients render the tree; the daemon only stores it.

Branch semantics interact with memory (see §5.4): only the main line and branches explicitly marked *real* feed the consolidation pipeline. Abandoned experimental branches remain searchable in the archive but never write distilled memory.

A model reply cut mid-stream is appended with `partial = true`; the log records what the user actually saw, and errors are reported to clients, never archived as messages.

## 5. Memory

"Feels alive" decomposes into three subsystems with different storage, different update rules, and different retrieval paths. They are deliberately not unified.

### 5.1 Identity

One small human-owned file: `data/identity.md`. Who ARC is, how it talks, stable facts about its user. Loaded into context unconditionally, every session. Budget: a few KB — small enough that loading it is never a decision.

Identity is exempt from event-sourcing. It is a plain file, versioned in git, backed up by rustic alongside the log. ARC may propose edits (as ordinary session output); accepted edits are applied by the user editing the file. `IdentityEvent` is reserved in the schema in case this policy changes, but v1 does not emit it.

### 5.2 Distilled tier

A flat set of structured records ARC writes deliberately. Flat, not hierarchical: namespaces and links provide organization; a folder tree does not.

```proto
message MemoryRecord {
  string id = 1;
  Kind kind = 2;              // PERSON, PROJECT, PREFERENCE, FACT, DECISION
  string namespace = 3;       // "global" or a project id (per-project memory)
  string title = 4;
  string summary = 5;         // one line; appears in the always-loaded index
  string body = 6;            // markdown, freeform
  repeated string links = 7;  // related record ids
  Provenance provenance = 8;  // session ids + timestamps where learned
  Status status = 9;          // ACTIVE, SUPERSEDED — never hard-delete
}
```

Current record state is a projection of `MemoryEvent`s. The always-loaded context contains an index of ACTIVE records — `namespace + kind + title + summary` only — so the model always knows what exists without loading bodies. Superseding rather than deleting preserves history ("you used to live at X") and keeps replay honest; a user-requested purge is a `DELETED` event, and the projection then excludes the record entirely.

Provenance is load-bearing: every fact can answer "where did you learn that" by pointing into the archive.

### 5.3 Archive tier

The raw session tree, fully indexed, derived from the log into SQLite (`data/index.db`, rusqlite, bundled):

- `sessions(id, parent_session, fork_point, project, title, started_at)`
- `messages(session_id, seq, role, content, ts)` + FTS5 index over `content`
- later: `chunks(session_id, span, embedding)` via sqlite-vec — only added if FTS proves insufficient in practice, and addable at any time by re-projecting the log

The distilled tier answers "what do you know about X"; the archive answers "what did we say about X in March." Both are needed; neither substitutes for the other.

### 5.4 Write pipeline (consolidation)
 
Storage is the easy half; deciding what gets remembered is the hard half. v1 keeps it embarrassingly simple:
 
1. **Explicit:** "remember this" → the model calls `memory_write` immediately.
2. **End-of-session extraction:** when a session (main line or *real*-marked branch) goes idle, a cheap model pass extracts durable facts, merges them with existing records, and resolves contradictions by superseding. Runs async on the daemon.
Every consolidation decision emits Perfetto spans, so tuning is done against traces of real behavior, not guesses. Expected failure modes to watch for: hoarding noise (too eager) and remembering nothing useful (too strict). Rules get complexity only after real usage shows where these bite.
 
**Tuning loop.** Because sessions live in the append-only log, consolidation is re-runnable — this is the primary tuning mechanism:
 
- The consolidation prompt is versioned. `arcd memory-replay` re-runs a given prompt version over historical sessions and diffs the resulting memory states against another version. This is the regression suite for memory: every prompt change is evaluated against real history before it goes live.
- A weekly review in the TUI shows records created and superseded that week; each is accepted, fixed, or deleted. Reviews are the ground truth.
- Corrections feed back as few-shot examples in the consolidation prompt — fixed records are exactly the examples that encode the user's standards.
- Three metrics tracked in traces: records created per session (hoarding detector), supersede rate (contradiction handling), and retrieval hit rate — how often a `memory_search` result is actually used in a response. The last is the honest measure of whether memory earns its tokens.
- Once enough reviews accumulate, a hand-labeled sample of sessions ("what should have been remembered") yields precision/recall on real usage.

**Coverage and atomicity** (amended 2026-08-21, at 7.1). What the pass has already covered is durable state, so it lives in the log like everything else: a `SessionConsolidated { session_id, through_seq, prompt_version }` kind inside `SessionEvent` (skip-safe for old binaries per §3 rule 3), appended by the pass when it finishes a session — including when it extracted nothing, because "looked and found nothing durable" is a decision. `through_seq` is the last event the pass read; a session is due when it has been idle past the configured window and has events after its latest marker. The projection derives per-session coverage from these events, so "what is due" is a query, never daemon memory. The pass itself is atomic and lock-shaped for a shared sidecar: it reads the session and releases the engine, runs the model with nobody blocked, then re-checks under the lock that the session is still idle before appending its records and marker together — new activity since the read discards the pass whole (a fresh idle timeout will re-run it over the longer history). Model-written records append with `Event.source = SYSTEM`: arcd initiated this write, not the user's turn.

Prior art: hermes-agent's production curation policy — its do-not-capture rules, nudge mechanics, and search/background-call lessons — is distilled in `docs/prior-art-hermes.md` (read 2026-08-17); the 5.x/6.x/7.x briefs should consult the sections keyed to them.

### 5.5 Retrieval

Memory access is tools, not silent RAG injection. The model calls:

- `memory_read(id)` — fetch a record body
- `memory_search(query, namespace?)` — search distilled records
- `memory_write(record)` / `memory_supersede(id, record)` — emit MemoryEvents
- `sessions_search(query, project?)` — FTS over the archive, returns snippets + session ids
- `session_read(id, range)` — pull actual past context

One pattern throughout: search cheap, read targeted. Lookups are visible in traces and debuggable like any other tool call. Nothing is injected into context automatically except the identity file and the distilled-record index.

## 6. Providers

One trait in `arc-core`:

```rust
trait Provider {
    fn complete(&self, req: CompletionRequest) -> impl Stream<Item = CompletionDelta>;
    // model listing, token counting, capability flags
}
```

v1's provider is local: a llama.cpp `llama-server` sidecar, supervised by `arcd`, spoken to as an OpenAI-compatible endpoint (`/v1/chat/completions` over HTTP + SSE, no auth). The same implementation covers vLLM or any OpenAI-compatible server by config. The sidecar releases its device memory after an idle window (`--sleep-idle-seconds`), so an always-on daemon costs tens of MiB of VRAM between turns instead of the model's several GiB, and pays about a second and a half to wake on the next one — that is what makes a local default compatible with a machine the user also uses for other things.

Hosted providers (Anthropic, Gemini) come later behind the same trait, as plain HTTP + SSE (`reqwest` + rustls), never vendor SDKs. Auth is a swappable layer; API keys first.

Amended 2026-08-14: Phase 1's original hosted path — Google via the Antigravity OAuth flow, a documented ToS gray area accepted for personal use — was first demoted to config-only when hidden short-term rate limits (429 `RESOURCE_EXHAUSTED` with quota showing available, endemic across community harnesses) made it unreliable as a daily driver, then removed outright: provider, OAuth flow, `arcd login`, token file. The gray area was not worth carrying for a backend nothing relied on. This paragraph is the only place the name survives; `data/secrets/` remains (0700, empty) as the seam for the first keyed provider.

Provider choice is per-completion: a global default, optionally overridden per request. Sessions don't own a provider; the log records what actually ran. Tool-calling and system-prompt differences are normalized in `arc-core`, not leaked to clients. Providers are hot-swappable mid-session — by a voice request, or by the agent itself deciding to use a cheap model for a subtask.

## 7. Wire protocol and clients

Protobuf over WebSocket (`wire.proto`), served by `arcd` on localhost. Remote access (mobile) is Tailscale reaching the same socket; ARC does not implement its own tunnel, TLS termination, or auth beyond a local token in v1.

The protocol is client-agnostic: subscribe to a session, send a message, fork, receive streamed deltas and tool-call events, query the session tree. Sessions are created implicitly — a message sent with an empty session id starts a new session, and the daemon replies with the assigned id. Clients hold no durable state.

- `arc` (TUI): first client, exercises everything — session tree navigation, streaming, memory tool visibility. Should connect over UDS for fast communication if local, websocket if not
- `arc-voice`: wake word via openWakeWord or Porcupine, local ASR via whisper.cpp/faster-whisper, TTS local, or gemini voice api, decide later. It is a thin client: audio in, text over the socket, audio out. No model logic.
- Mobile: same protocol over Tailscale. Last client, after the protocol has stabilized under two others.

## 8. Observability: Perfetto

ARC emits Perfetto protobuf traces of its own operation: a track per session (and per branch), spans for LLM calls (with token counts as counters), tool calls, memory reads/writes, consolidation passes, and log replays. Traces are written to `data/traces/` and open directly in the Perfetto UI.

This is not decoration. It is the debugging surface for the two genuinely hard subsystems — consolidation quality and retrieval behavior — and it makes latency and token spend measurable from day one. Implementation: `tracing` subscriber in `arc-core` that renders to Perfetto's `TracePacket` protos.

## 9. Robotics (future)

Physical devices integrate as MCP servers, never as bespoke daemon code: an ESP32 pan-tilt rig, later an SO-101-class arm, eventually larger. Two constraints are fixed now:

1. The model plans and issues high-level actions only; firmware enforces joint limits, speeds, and e-stop. The LLM never commands motors directly.
2. Device MCP servers are separate processes with their own lifecycle; `arcd` treats them like any other tool source.

No robotics code exists in this repo until Phase 5.

## 10. Security, backup, and running

- "Always-on" (§1) means a systemd user unit (`arcd/arcd.service`): started with the machine, restarted on failure, logging to the journal. `SIGTERM` is a clean stop — `arcd` handles it, and the llama.cpp sidecar dies with it either way, since systemd kills the whole control group. Nothing is left holding the GPU.
- Runtime state lives under `data/` (log, index, identity, traces).
- Backup: rustic, encrypted at the repository level, covering `data/log/`, `data/identity.md`, and snapshots. `data/index.db` and `data/traces/` are excluded — both are rebuildable/disposable.
- Provider credentials live in the OS keychain or an encrypted secrets file, never in the log and never in backups. Nothing holds one today — the local provider has no auth; `data/secrets/` exists (0700, excluded from backups) as the seam for the first provider that needs a key. Keychain integration comes when one does.
- The WebSocket binds localhost only; remote access is Tailscale's problem, by design.

## 11. Phases

Each phase ends in something used daily. No phase begins until the previous one is a daily driver, because real usage is the input to the next phase's design decisions (especially memory).

**Phase 0 — Scaffold.** *Done.* Workspace, empty crates, empty schemas, build/test/fmt/lint targets.

**Phase 1 — Walking skeleton.** *Done 2026-08-13.* The exit criterion is met in the sense that matters: ARC is what gets asked the simple questions, daily, instead of a chat app. It is not a full replacement — no tools, and an 8B local model is not a coding assistant — but neither gap is Phase 1's to close: tools arrive with memory (§5) and devices (§9), and a better model is a config line. `arcd` with a default local provider (llama.cpp sidecar; the hosted Google path was demoted after its rate limits proved unreliable — amended 2026-08-13 — then removed outright, see §6), linear sessions only, event log with `SessionEvent`, SQLite projection, the TUI with streaming, identity file loaded into context, Perfetto spans on LLM calls. Memory is *only* the identity file. Exit criterion: ARC replaces a chat app for daily use.

**Phase 2 — Memory.** `MemoryEvent`, distilled records + always-loaded index, the five memory/archive tools, FTS5 over messages, explicit `memory_write` plus end-of-session consolidation, `arcd memory-replay` with a versioned consolidation prompt, the weekly review flow in the TUI, Perfetto spans on all memory operations. Exit criterion: "what do you know about X" and "what did we say about X" both work on real history.

**Phase 3 — Tree + second provider.** Session forking with branch semantics from §4, tree navigation in the TUI, Gemini provider, per-session provider choice, replay/rebuild command (`arcd rebuild`) proven against the real log. Exit criterion: branching used naturally; a full index rebuild from the log matches live state.

**Phase 4 — Voice + remote.** `arc-voice` (wake word, local ASR/TTS), Tailscale-reached daemon from the phone (mobile client can start as the TUI over SSH; the real app comes when the protocol has been stable for a while), rustic backup automated and restore tested — a restore drill is the exit criterion, not a backup existing.

**Phase 5 — Hands.** First device MCP server (ESP32 pan-tilt), device-tool safety conventions, then the arm when the workbench reaches it. sqlite-vec embeddings land here or earlier only if Phase 2–4 usage shows FTS falling short.

## 12. Open questions

Deferred deliberately; decide when the phase forces them:

- Consolidation triggering: idle-timeout vs explicit session close vs continuous. (Phase 2, from traces. v1 placeholder decided 2026-08-14: a configurable idle timeout, so the pass has something to hang on — the question stays open; traces judge it.)
- Model routing. The ambition is a router of our own — pick the best model per request across local + hosted (OpenRouter), aiming at what a Claude Code 100-style tier delivers. §6's per-completion provider choice is the seam it plugs into. Deliberately not now: a router needs to know what usage looks like, and Phase 2 is what generates that data. (Post-Phase 2, from usage. Prior art banked 2026-08-17: hermes-agent converged on aux-default = the main model after shipping the opposite, and its whole routing surface hangs off a per-task label at one chokepoint — so Phase 2's only routing obligation is that background requests carry a task name that reaches their spans; see `docs/prior-art-hermes.md` §3.)
- Tool-call events. Answered 2026-08-15 in §3.1: `ToolCallIssued` and `ToolResultRecorded` as kinds inside `SessionEvent`, a `turn_id` instead of turn events, and a write-ahead resume contract that closes an unanswered call as UNKNOWN. The three questions §3.1 leaves open are scoped to the tasks that hit them and listed there.
- Whether identity edits ever move into the event log. (Revisit if hand-editing becomes a bottleneck.)
- Embeddings model choice for sqlite-vec, local vs API. (Phase 4/5.)
- Multi-machine story beyond backup/restore (log sync). (Post-v1.)
- Startup recovery is a full log replay today; a checkpoint bounds it when the log grows. (When startup time or traces say so.)
