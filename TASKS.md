# TASKS — Phase 1 walking skeleton

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

## 6. TUI (`arc`)

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 6.1 | Connect + session picker + send message + streaming render (absorbs old 6.2: "message pane, input line, session picker, nothing else") | claude | done |

| 6.2 | TUI polish: bottom-anchored transcript, persistent wordmark, wrap indent, picker labels, markdown rendering, `--help` | claude | in review |
| 6.3 | Session history over the wire: fetch on session open so the picker lands in a full transcript | claude | in review |
| 6.4 | TUI punch list, gathered by Bogdan while using it (see below) | claude | in review |

6.4 is a running list — Bogdan adds items as daily use turns them up, and they land in small batches rather than waiting for a task boundary. Done so far (2026-08-13): one blank row between the last message and the status rule; syntax highlighting in fenced code blocks; mouse-wheel and page-key scrolling; a scrollbar in the right margin; sessions ordered by last activity with a relative "x ago".

Recency needed no new architecture — `messages.ts` was already projected, so last activity is a `MAX(ts)` subquery plus one additive `SessionInfo.last_at`. The daemon still answers in its stable oldest-first order (that is the order a log replays in); the client sorts, because "what did I last use" is a client's question and the wire contract in 5.4's brief says oldest first. If a second client ever wants the same order, move the sort to `Projection::sessions` and amend that brief.

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
- `arc` fetches on session open and renders the result before the first new delta. Decide and write down what happens when history is long (all of it vs. a tail) — the wire has no paging and Phase 1 shouldn't add one.

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

7.x loose ends: sidecar restart policy is deliberately absent (unexpected exit is logged loudly, turns fail until the daemon restarts); decide it when it hurts. ~~Fixture recapture~~ and ~~the live streaming check~~ both done 2026-08-13 against the Vulkan `llama-server` on the RTX 5070 (131 tok/s; nixpkgs' default `llama-cpp` is CPU-only — the dotfiles now build `llama-cpp.override { vulkanSupport = true; }`, and the sidecar needs `--device Vulkan1` to skip the iGPU).

## 8. Observability

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 8.1 | Perfetto `TracePacket` output from `tracing` spans, written to `data/traces/` | — | todo |
| 8.2 | Spans + token counters on LLM calls (lands with 4.x/5.2, verified in Perfetto UI) | — | todo |

Next session picks up here (banked 2026-08-13): 6.1 and 7.1–7.3 reviewed and done. In flight: 6.2 and 6.3 (claude). Then assign 8.x — the earlier suggestion was an implementer agent with a checkable output (a trace that opens in the Perfetto UI). Also pending, no task yet: how arcd runs long-term (currently a hand-started tmux session; a systemd user unit is the natural shape now that arcd supervises its own sidecar). Machine notes that bit us today live in the host dotfiles, not here: GNOME idle-suspend disabled for SSH work, `llama-cpp` built with Vulkan.
