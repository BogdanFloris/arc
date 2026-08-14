# TASKS — Phase 1 walking skeleton

**Phase 1 closed 2026-08-13.** All 26 tasks built and reviewed; the exit criterion was called met on real use — simple questions go to ARC daily instead of to a chat app. Not a full replacement (no tools, and an 8B local model does no coding), and neither gap belonged to this phase. This file is the frozen record; the live list is `TASKS.md` (Phase 2).

Working agreement: Bogdan assigns each task (`bogdan` or `claude`). The implementer's work is reviewed by the other. Statuses: `todo` → `in progress` → `in review` → `done`.

Tasks are ordered by dependency; anything at the same number can go in parallel.

Loose ends (fold into the next touch of the relevant file, no own task): `log::Error::Io`'s field doc says "segment" but the variant also carries directory paths (from `sync_parent_dir`, `discover_segments`).

Wire-protocol friction, noted by 5.4 for the next `wire.proto` evolution (Phase 3): `Delta`/`StreamEnd.session_id` are redundant given `request_id` correlation; no explicit "session was created" signal; `request_id = 0` overloaded (unsolicited vs undecodable-frame errors); `Error` doesn't say whether the connection survives (clients know `bad_frame` is terminal out-of-band); text WS messages are refused as `bad_frame` (5.4's call, unspecified in the schema comments). ~~Added by 6.1: no session-history fetch — a client opening an old session renders an empty transcript (new messages only); accepted for Phase 1.~~ (retired: 6.3 adds the frame — Phase 1's exit criterion needs it.) Added after the Antigravity 429s: the wire's `provider` error code is too coarse to tell a client "rate limited, retry in Ns" apart from a hard provider failure, and no layer owns a retry policy.

## 1. Schemas (`arc-proto`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 1.1 | `events.proto`: `Event` envelope (seq, ts, source, oneof payload) + `SessionEvent` (session created, message appended; fork fields reserved) | bogdan | done |
| 1.2 | `wire.proto`: minimal protocol — send message (empty session_id creates a session), streamed deltas, list sessions, error frame | bogdan | done |
| 1.3 | prost generation in `build.rs` + a round-trip encode/decode smoke test | claude | done |

## 2. Event log (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 2.1 | Segment writer: length-prefix + CRC32 framing, protobuf append, fsync policy, monotonic gapless seq. Refuse to write an `Event` with `payload: None` | claude | done |
| 2.2 | Segment reader: iterate events across segment files, detect/stop at torn tail. Truncation detection comes from the length prefix, corruption from the CRC — never from decode failure (empty/partial bytes decode "successfully" in proto3); `payload: None` on a full-length record is a hard error | bogdan | done |
| 2.3 | Segment rollover by size + segment file naming. Add a shared `MAX_RECORD_LEN` sanity cap to `log::format` (writer enforces, reader rejects) | claude | done |

## 3. SQLite projection (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 3.1 | Schema: `sessions` + `messages` tables, projection struct over rusqlite | claude | done |
| 3.2 | Replay: log in → state out, resumable from last projected seq; deterministic replay test | bogdan | done |

## 4. Provider (`arc-core`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 4.1 | `Provider` trait + `CompletionRequest` / `CompletionDelta` types | claude | done |
| 4.2 | Google OAuth: loopback flow with the community-documented public client, token cache in `data/secrets/` (0600), refresh | claude | done |
| 4.3 | Antigravity provider: `loadCodeAssist`/`onboardUser` onboarding, request building + required headers against `cloudcode-pa.googleapis.com` | claude | done |
| 4.4 | SSE stream parsing → `CompletionDelta` stream, with parser unit tests against captured fixtures (no secrets in fixtures) | claude | done |

## 5. Daemon (`arcd`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 5.1 | Skeleton: config, `data/` layout, tracing subscriber init, `arcd login` subcommand | claude | done |
| 5.2 | Session engine: create session / append user message → drive provider → append model message, all via log events | bogdan | done |
| 5.3 | Identity file: load `data/identity.md` into system context (read-only) | bogdan | done |
| 5.4 | WebSocket server on localhost speaking `wire.proto`, streaming deltas to the client | claude | done |
| 5.5 | systemd user unit for arcd: always-on, restart on failure, journal logging. The unit file lands in the repo; installing and enabling it on erebor is Bogdan's call, after he reads it | claude | done |

## 6. TUI (`arc`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 6.1 | Connect + session picker + send message + streaming render (absorbs old 6.2: "message pane, input line, session picker, nothing else") | claude | done |
| 6.2 | TUI polish: bottom-anchored transcript, persistent wordmark, wrap indent, picker labels, markdown rendering, `--help` | claude | done |
| 6.3 | Session history over the wire: fetch on session open so the picker lands in a full transcript | claude | done |
| 6.4 | TUI punch list, gathered by Bogdan while using it (see below) | claude | done |

6.4 is a running list — Bogdan adds items as daily use turns them up, and they land in small batches rather than waiting for a task boundary. Done so far (2026-08-13): one blank row between the last message and the status rule; syntax highlighting in fenced code blocks; page-key scrolling; a scrollbar in the right margin; sessions ordered by last activity with a relative "x ago".

Recency needed no new architecture — `messages.ts` was already projected, so last activity is a `MAX(ts)` subquery plus one additive `SessionInfo.last_at`. The daemon still answers in its stable oldest-first order (that is the order a log replays in); the client sorts, because "what did I last use" is a client's question and the wire contract in 5.4's brief says oldest first. If a second client ever wants the same order, move the sort to `Projection::sessions` and amend that brief.

6.4 decisions and costs, for review:
- **No mouse wheel — settled, do not re-add.** It was built, tried for an afternoon, and removed the same day (Bogdan's call). Answering the wheel means mouse capture, and crossterm's `EnableMouseCapture` requests five modes at once: `?1000h` press/release (all the wheel actually needs) plus `?1002h`/`?1003h` motion tracking. Once an app asks for motion the terminal stops doing its own selection, so selecting text out of the pane fell back to Shift-override, which ignores pane boundaries and grabs whole physical rows. The narrower `?1000h`+`?1006h` pair might have kept selection working, but the wheel was not worth a terminal-dependent maybe: scrolling is `j k`, `ctrl-d/u`, `G gg`, the page keys, and the scrollbar.
- **Untagged fences are sniffed.** The local model drops the info string constantly, and leaving those blocks flat was the original complaint. Only markers belonging to one language count and a tie declines, because a wrong guess miscolours code (reads as a bug) while no guess reads as plain code. A tag that *is* present is always believed, even an unknown one.
- Two lexer bugs came out of daily use, both fixed with tests: a one-line `"""docstring"""` (and one-line `/* */`) had only its opening marker checked, so it painted the rest of the block as one string; and plain tokens inside a highlighted block were emitted as `CODE` (aqua), colliding with the type role — they are the terminal's own foreground now, so the coloured tokens have something to read against.

Scope for 6.2 (2026-08-13, from driving the app together — six items, all `arc`-only):
1. **Bottom-anchored transcript.** Content grows up from the input rule instead of down from the top; a short turn no longer leaves ~18 rows of dead space. Once the transcript is taller than the pane, it pins to the bottom and the existing `j k` / `ctrl-d/u` / `G gg` scrolling takes over.
2. **Persistent wordmark.** The ASCII `arc` wordmark stays at the top of the pane always, not just on the empty state (reverses the 6.1 "empty state only" call — Bogdan likes it there). The transcript scrolls under it; the wordmark does not scroll away.
3. **Wrap indent.** Continuation lines of a wrapped message indent past the speaker-label column, so a long paragraph is never mistaken for a new turn.
4. **Picker labels.** Sessions list as their first user message (elided to fit) instead of `2026-08-13 18:22  398f1cde`. `title` stays empty in Phase 1 — this is a read of the projection at the client, not title generation, which is Phase 2.
5. **Markdown rendering.** Bold, italic, inline code, fenced code blocks, bullet/numbered lists, ATX headings, blockquotes, horizontal rules. Styled through `theme.rs` semantic roles only — terminal palette indices, never RGB, per the 6.1 decision — so it reads as gruvbox in Bogdan's terminal and stays correct in any other. Headings get weight/color, not box-drawing.
6. **`arc --help`.** Prints the usage string and exits 0. Today it returns an `anyhow::Error`, so the first thing anyone types produces a 30-frame backtrace.

Scope for 6.3 (2026-08-13):
- Additive `wire.proto` change: a client request for a session's messages and a server frame carrying them. New oneof numbers only — nothing renumbered or repurposed (invariant 3). Lands as its own schema commit, separate from the code that uses it.
- `arcd` answers it from the SQLite projection's `messages` table. No new subsystem, no log reads on the request path.
- `arc` fetches on session open and renders the result before the first new delta. Decided: the daemon sends **the whole history, unpaginated**, matching `ListSessions`. One user, sessions a person could scroll, and the alternative — a tail — invents a cutoff the client would then have to explain. `FetchHistory` reserves field 2 for a paging cursor so the answer can change without a schema bump. Revisit when a real session makes the frame big enough to notice.

6.2/6.3 as built (2026-08-13): picker labels needed the daemon's help, so `SessionInfo` gained a `preview` field (the first user message, from a subquery in `Projection::sessions`) rather than overloading `title`, which stays empty for Phase 2's real titles. Markdown is hand-rolled in `arc/src/markdown.rs`, not a parser crate: it has to render half-typed input on every delta, and unclosed markup has to stay literal instead of reflowing the screen when the closing `**` finally arrives. Underscores are not emphasis — `snake_case` beats `_this_` in this codebase's conversations. Loose end: the projection has no `partial` column, so a reopened session cannot mark a cut reply; `HistoryMessage` reserves field 3 for it.

Decisions for 6.1 (2026-08-13):
- ratatui + crossterm. The wire client (connect, frame correlation, turn events) lives in `arc-core` so it is testable without a terminal and reusable by `arc-voice`; the `arc` binary is rendering and key handling only.
- Look: pure ASCII — no box-drawing, no nerd-font glyphs, no borders; structure from whitespace and `--` rules. Terminal palette colors only (never RGB), so the user's gruvbox terminal theme maps through; semantic color roles in one small theme module. Orange is the single accent — indexed 208, since the ANSI 16 has no orange slot (xterm-256's 208 ≈ gruvbox `fe8019`; gruvbox setups that define indexed colors map it exactly). Small lowercase wordmark on the empty state only.
- Vim-native controls (Bogdan, mid-6.1): modal input — starts in insert, `Esc` to normal (`h l 0 $ w b`, `i I a A`, `x D dd`), `j k` / `ctrl-d/u` / `G gg` scroll the transcript, `s` opens the picker, `:q` quits (unknown commands answer `E492`). Cursor shape follows the mode (bar/block); `-- insert` sits at the rule's left edge.
- Transcript: speaker labels on their own line, `you` dim / `arc` orange, no message boxes or timestamps. Errors render inline as red `!`-prefixed blocks; a `partial` reply ends with a dim `-- cut --`. One `--` status rule above the input line carries state words (`streaming`, `disconnected`, error codes) at its right edge.
- 7.x is assigned after 6.1 lands (candidate for an implementer agent — spec-transcription with a checkable output: a trace file that opens in the Perfetto UI).

Client-author notes from 5.4 (the 6.1 brief):
- Connect `ws://` to `config.bind` (default 127.0.0.1:8787), plain WebSocket, no auth. One `ClientFrame` per binary message, one `ServerFrame` back; text messages are refused as `bad_frame`.
- Pick a nonzero `request_id`; every answer frame echoes it — correlate on it, not session id. `session_id = ""` creates a session; the real id arrives in `MessageAccepted`.
- A turn is exactly: `MessageAccepted` → zero or more `Delta` → one `StreamEnd` or one `Error`. `StreamEnd.partial = true` means the reply was cut and what rendered is what got logged; tokens are 0 when the provider reported no usage.
- Error codes `empty_message`, `empty_reply`, `provider`, `internal`: connection survives, send the next frame. `bad_frame` is terminal: the daemon closes.
- Requests answer in order; the daemon runs one completion at a time process-wide. `ListSessions` returns id, title (empty in Phase 1), started_at — oldest first, no paging.

## 7. Local provider (llama.cpp)

Decided 2026-08-13 after Antigravity's hidden rate limits made it unreliable as a daily driver (DESIGN.md §6 amendment). Local is the new default; Antigravity stays behind config. Test model: Qwen3-8B Q4_K_M GGUF (~5.5GB VRAM — fits the RTX 5070 with headroom; swap is a config line). arcd owns the sidecar process (Bogdan's call, over a systemd unit).

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 7.1 | OpenAI-compat provider in `arc-core`: `/v1/chat/completions` request building + SSE stream → `CompletionDelta`, fixture-based parser tests (reuse the 4.4 `FrameDecoder`) | claude | done |
| 7.2 | Sidecar supervision in `arcd`: spawn `llama-server`, wait for ready, clean shutdown with the daemon; config for binary path, model path, port | claude | done |
| 7.3 | Provider selection in config: `provider = "local" (default) \| "antigravity"`, endpoint/model per provider; daemon wires the chosen one | claude | done |

Idle VRAM, settled 2026-08-13: `llama-server` holds its device allocation for the process lifetime, which is wrong for a daemon that is always up. This build has `--sleep-idle-seconds`; measured on erebor with Qwen3-8B Q4_K_M on Vulkan1, it drops 5764 MiB to 54 MiB after the idle window and takes ~1.5s to wake on the next request, with `/health` answering 200 throughout. It is a pass-through flag, so `arcd` needs no code — `data/arc.toml` now sets 300s. This is what makes 5.5 (always-on arcd) cheap enough to leave running.

7.x loose ends: sidecar restart policy is deliberately absent (unexpected exit is logged loudly, turns fail until the daemon restarts); decide it when it hurts. ~~Fixture recapture~~ and ~~the live streaming check~~ both done 2026-08-13 against the Vulkan `llama-server` on the RTX 5070 (131 tok/s; nixpkgs' default `llama-cpp` is CPU-only — the dotfiles now build `llama-cpp.override { vulkanSupport = true; }`, and the sidecar needs `--device Vulkan1` to skip the iGPU).

## 8. Observability

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 8.1 | Perfetto `TracePacket` output from `tracing` spans, written to `data/traces/` | claude | done |
| 8.2 | Spans + token counters on LLM calls (lands with 4.x/5.2, verified in Perfetto UI) | claude | done |

8.x as built (2026-08-13). The subset of Perfetto's schema ARC emits is vendored in `arc-proto/proto/perfetto.proto` — third-party field numbers, each checked against upstream, and the header says how the schema invariants read differently there. The layer is `arc-core/src/trace/`; `arcd` layers it beside the stderr one over the same filter.

Decisions, and what they cost:
- **A track per span instance, not per thread.** ARC's work is async: a span is entered and exited on whatever thread polls it, so thread tracks would draw polls instead of work, and one LLM call would be thousands of slivers. Tracks nest the way spans do, so the UI still draws the span tree. The cost is a track per span instance — verbose in the track list, exact in the timings.
- **A span's packets are written when it closes.** `session.send_message` only learns its `session_id` after it opens, and token counts arrive at the end; emitting at close is what lets a slice carry them. Perfetto sorts by timestamp, not by file order, so a late write costs nothing. The cost: a span that never closes (daemon killed mid-turn) leaves no slice at all.
- **The span that first names a session opens that session's row.** DESIGN.md §8's "track per session", and the first trace proved the naive rule wrong: parent-first put every turn under `client connected` and no session track was ever created.
- **Fields named `counter.*` are counter samples, not annotations.** The layer stays ignorant of tokens; Phase 2's memory metrics get counters for free by naming a field. The cost is one uglier field name in the stderr log (`counter.output_tokens=387`).
- **A `ClockSnapshot` is not optional.** The first real trace parsed but was empty: without one, Perfetto reads timestamps as boot time and drops every packet it cannot convert (41 of 41). Timestamps are REALTIME so a slice can be matched against a log event; a clock step during a turn would skew that turn.

Verification: `trace_processor_shell` is in the dev shell (`flake.nix`, upstream prebuilt pinned by hash — nixpkgs has no perfetto). Against a trace of a real turn on erebor: no import errors or data loss, the tree reads `session 6873fcb9` → `session.send_message` (5.19s) → `openai.complete` (5.18s), and the counter tracks hold 13 in / 387 out. Opening the same file in the Perfetto UI is Bogdan's check — `trace_processor` is the UI's own parser, so this is the same answer minus the eyes.

Cost of leaving it on, measured 2026-08-13 (100k synthetic turns, release build): 8.8 µs and 601 bytes per turn — two millionths of a 5-second turn, and ~60 KB a day at a hundred turns. A whole daemon run with one turn is 4.8 KB. Cheaper than the stderr log would be if anyone read it. So tracing stays on always: a trace you have to switch on is a trace you don't have when the interesting thing happens. `RUST_LOG` is the dial if a subsystem ever gets chatty, since it filters the trace and the log together. Two things that would change the answer, neither true yet: a span per delta or per token (nothing per-chunk is instrumented today), and file accumulation — one file per run, nothing prunes `data/traces/`, which is fine at kilobytes per run and worth revisiting if it ever isn't. Traces carry no message content, checked against a real one: spans record ids, counts and outcomes, never the text.

Found while shutting the daemon down for that test, fixed in the same batch: arcd only handled Ctrl-C, so a `SIGTERM` — what any service manager sends — killed it before the shutdown path ran and orphaned llama-server holding 5.7 GiB of VRAM. It now stops on both.

Next session picks up here (banked 2026-08-13, second batch): all 26 Phase 1 tasks are built and reviewed. Nothing is in flight. What is left is not code — it is daily use answering the exit criterion ("ARC replaces a chat app"), which is also what Phase 2's memory design is supposed to be built from. When it holds up, mark Phase 1 done in DESIGN.md §11 and open Phase 2. The unit is installed but not enabled (`systemctl --user enable --now arcd`), and the release binary it points at is built.

Open on purpose, none of them Phase 1's problem: the wire friction banked above for Phase 3; no `partial` column in the projection, so a reopened session cannot show a cut reply (`HistoryMessage` field 3 is reserved); no sidecar restart policy; no retention on `data/traces/`; `log::Error::Io`'s field doc. Machine notes that bit us live in the host dotfiles, not here: GNOME idle-suspend disabled for SSH work, `llama-cpp` built with Vulkan.