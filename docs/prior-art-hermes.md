# Hermes-agent — prior-art notes for Phase 2

**What this is.** Lessons from reading NousResearch/hermes-agent (commit `1ed94d2`, read 2026-08-17), a production personal-agent harness close to ARC in ambition. Read for prompts, policies, and config shapes only. Its storage architecture — SQLite as source of truth, with the migration/repair machinery that entails — is the one ARC rejected, and nothing structural was taken; where their code fights a problem our event log deletes, that is noted and skipped. Skill self-improvement, subagents, the platform gateway, and cron were deliberately not read (Phase 4+ concerns).

Three sections, each keyed to the ARC work it informs: memory curation (§5.4, tasks 7.x, 8.1), session search (§5.3, tasks 5.x), background model calls (§12 routing, tasks 7.x). File:line references are into their repo at the pinned commit.

## 1. Memory curation — prompts and nudge policy

Their curation is three actors: an always-loaded write policy in the memory tool's description, a post-reply background review pass every N turns, and a periodic consolidation pass. This maps onto ARC's explicit `memory_write` + end-of-session extraction + weekly review, so their prompt text is the closest thing to a production-tested draft of 7.2's consolidation prompt that exists.

**The write policy lives in the tool description, not injected reminders.** No per-turn nudge text ever enters the conversation; the tool schema carries the policy (`tools/memory_tool.py:1173`):

> "WHEN: save proactively when the user states a preference, correction, or personal detail, or you learn a stable fact about their environment, conventions, or workflow. Priority: user preferences & corrections > environment facts > procedures. The best memory stops the user repeating themselves."
> "SKIP: trivial/obvious info, easily re-discovered facts, raw data dumps, task progress, completed-work logs, temporary TODO state (use session_search for those)."

This is invariant-6 compatible: tool descriptions are already a sanctioned channel, and it is where they got the most leverage per token. 6.3's `memory_write` description should carry a WHEN/SKIP policy of this shape.

**Prompt content worth stealing for 7.2.** Four rules, each a patch over an observed failure:

1. **A do-not-capture list against self-poisoning** (`agent/background_review.py:349`): no environment-dependent failures ("command not found" — the user can fix these), no negative claims about tools ("'X is broken' hardens into refusals the agent cites against itself for months after the actual problem was fixed"), no transient errors that resolved ("if retrying worked, the lesson is the retry pattern, not the original failure"), no unresolved failures dressed up as workflow ("never the dead ends, and never dressed up as best practice"). Supersede does not save us here — poison phrased as a durable fact looks like memory working.
2. **Declarative facts, not imperatives** (`agent/prompt_builder.py:171`): "'User prefers concise responses' ✓ — 'Always respond concisely' ✗... Imperative phrasing gets re-read as a directive in later sessions and can cause repeated work or override the user's current request." A phrasing constraint for record bodies and a checklist line for 8.1's review.
3. **The staleness test with an escape hatch**: "If a fact will be stale in a week, it does not belong in memory" — and every SKIP category points at session search instead. ARC has the same two tiers; the consolidation prompt should say the archive already remembers everything, so only extract what must be in the always-loaded index. This aims squarely at §5.4's hoarding failure mode.
4. **The umbrella bar for merging** (`agent/curator.py:432`): "Pairwise distinctness is the wrong bar. The right bar is: 'would a human maintainer write this as N separate skills, or as one skill with N labeled subsections?'" — their hardest-won extraction lesson. 7.2's merge step should ask "does an existing record cover this class?" before creating, and 8.1 should watch for narrow-sibling proliferation, not just record count.

**Nudge policy, as rules.** Turn counter (default 10) fires a review *after* the reply is delivered, "so it never competes with the user's task for model attention"; the counter resets when the model uses the memory tool organically, so nudges only fire when nothing is being saved. Suppressed for interrupted turns and background contexts; the review pass itself cannot recurse. The review prompt is deliberately short — two questions (what did the user reveal about themselves; what did they express about how the agent should operate) and an explicit "if nothing is worth saving, say 'Nothing to save.' and stop."

**Mechanism lessons, keyed to tasks:**

- **Memory never blocks the reply** — bounded retries on a failed write, then a terminal "save skipped." For ARC the analog is 7.1: a failed consolidation pass logs and yields; it never wedges the daemon or the session.
- **Terse, terminal tool results.** Their write-success response deliberately does not echo the saved entries — echoing caused observed thrash (repeated redundant saves). Shape 6.3's results the same way.
- **A hard budget forces curation.** Their always-loaded memory is char-capped; at capacity, adds are rejected with the full list and an instruction to consolidate. ARC's always-loaded index (6.2) needs a budget from day one — an 8k-context model makes this non-optional.
- **Frozen snapshot per session.** Memory is injected as a session-start snapshot; mid-session writes hit disk but not the live prompt, preserving the prefix cache. 6.2 should refresh the index at session boundaries, not per turn.
- **Fence injected memory as reference, not instruction.** Their recalled context is wrapped in "[System note: ... NOT new user input. Treat as authoritative reference data]". 6.2's index injection should carry the same frame — it compounds with rule 2 above.
- **Curation output is user-visible.** Every background write surfaces in the UI. Same contract for 7.1: consolidation activity renders in the TUI (the banked "tool activity is visible" decision extends to the async pass).
- **The review pass is sandboxed.** Their background pass once wrote its own harness prompt into the real session, which the next turn re-read as a standing instruction ("curator takeover"). ARC's pass writes `MemoryEvent`s only, never session events — architecture already forbids this; keep it that way.

**Tension with our priors — direction of failure.** They needed *anti-passivity* pressure ("Be ACTIVE... A pass that does nothing is a missed learning opportunity") because "nothing to save" won by default. §5.4 expects hoarding first. Expect to tune in both directions; the records-per-session counter decides, not the prior.

**Trigger.** Their per-N-turns mid-session trigger exists because messaging sessions never end; it does not move our idle-timeout placeholder. But their post-reply pass rides the still-warm prompt cache (measured ~26% cost cut), while our idle-time pass replays a cold context and pays full prompt tokens every run. Not a reason to change now — a cost the §5.4 tuning loop should watch in traces.

## 2. Session search — FTS5 and retrieval shape

**They shipped LLM summarization over search hits and deleted it.** The module history records a summary-mode split that was later removed; the current tool advertises "No LLM calls — every shape returns actual messages from the DB" (`tools/session_search_tool.py:29`, `:1133`). Their LLM spend moved to write-time metadata (titles) instead of read-time summarization. This is production validation of §5.5's search-cheap-read-targeted: **5.2 should not grow a summarize-hits pass.**

**What replaced it — the return shape for 5.2.** Overfetch raw FTS rows, dedupe by session (one result slot per session — their lineage walk maps to our `parent_session` chain), return the top few *sessions*: each with a snippet plus the anchor message (for ARC: the seq, which `session_read(id, range)` needs to target), and the top hit hydrated with a small message window plus **bookends** — the session's first and last few user/assistant messages. Bookends are the summarization substitute: a hit anywhere in a long session yields the goal (opening) and the resolution (closing) in one call. Per-message char budgets with explicit truncation flags. Worth folding into 5.2's design: an "ends" shape for `session_read` is cheap and covers the same need.

**Query sanitization — steal as policy and as test fixtures.** Raw user text in FTS5 `MATCH` raises, and a swallowed error is a silent empty result. Their sanitizer (`hermes_state_search.py:1178`): cap length; extract quoted phrases with a linear scan and protect them; strip the FTS5 special-char class (`:` matters — single-column FTS turns `TODO: fix` into a column error); trim dangling `AND/OR/NOT`; quote dotted/hyphenated/underscored terms so `my-app.config.ts` and `P2.2` match as phrases instead of tokenizer-split AND-terms; and *still* catch the syntax error at the execute site — never trust the sanitizer alone. Their measured failure list is a ready-made fixture set for 5.2: `it's`, `gateway/run.py`, `user@host`, `a,b`, `50%`, `TODO: fix`.

**Indexing and ranking lessons:**

- **Tool-result rows poison both size and ranking** — in their store, ~90% of message bytes and almost entirely machine noise. Strongly validates §3.1's tagged-and-excluded-by-default note. Do the exclusion at the query layer (row-kind filter), not the index, so explicit tool-output search stays possible.
- **Bare BM25 misranks when sources have asymmetric volume.** Their automation sessions' repetitive vocabulary caused "recall blindness" for human sessions; the fix was demote-don't-exclude — a stable sort ranking interactive above machine-heavy, BM25 preserved within each class. ARC's Phase 2 analog is tool-heavy sessions; same policy, kept as a query-layer choice.
- **Machine-authored artifacts stay out of previews and recall surfaces** — their compaction summaries were re-entering fresh sessions via search, and sessions got titled after scaffolding. ARC's consolidation output and injected artifacts must never surface as session content in search results.
- **Titles are search infrastructure.** Two-stage titling: an instant derived slice of the first user message, then a one-shot small-model upgrade (tiny max_tokens, strict shape), with provenance ordered derived < llm < user so machine never overwrites the user. Their discovery path checks title match before FTS. ARC's `sessions.title` column currently has no writer; a titling pass on the local model is cheap, improves both browse and search, and is a natural second background task after consolidation. Copy their reject-not-truncate guard: a title over the word cap means the model answered instead of titling — a real small-model failure mode, relevant to our 8B.
- **Slow searches log the routing path taken.** One span field on `sessions_search` saying which path served the query makes the next latency regression a grep. Fits Perfetto-first observability at zero cost.
- **FTS-first, no embeddings: validated.** A mature production agent runs recall entirely on FTS5; no vector index anywhere in their session store. The sqlite-vec seam stays banked, unhurried.

## 3. Background model calls — the routing seam

Phase 2 grows ARC's first background calls (consolidation; titling is the obvious second). Hermes is the argument for landing the seam alongside them rather than retrofitting.

**The seam is a task label, not a router.** Every hermes background call goes through one chokepoint keyed by a task name (`agent/auxiliary_client.py`), and that string keys everything: per-task config (provider, model, timeout, concurrency, reasoning effort, fallback chain), accounting, and logs. The whole Phase 2 obligation for ARC is: every background `CompletionRequest` carries a task name, and the name lands on the tracing span. Per-task config sections and provider choice bolt onto that label later without touching call sites — it plugs into §6's per-completion provider choice exactly.

**They reversed their default to "aux = main model."** Their original default routed background tasks to a cheap hosted model; the current code inherits the user's main provider and model, with the cheap chain only as fallback — "no surprise switches to a cheap fallback model for side tasks... silently overriding that choice makes the selected model cosmetic." The banked "consolidation uses the same local model" is the position they *converged on*, not a compromise to grow out of. Fast-model routing survives as per-task opt-in, and only titling qualifies (a title is ~8 tokens; a big model wastes seconds generating it).

**Aux-call policy, as rules for 7.1/7.2:**

- **Background timeouts are their own config, generous for summarization-class work** (their default 30s, with a 300s floor for compression). Consolidation gets its own timeout, not the interactive one.
- **Passes are atomic.** A user interrupt must not tear a pass into a half-applied state. Events make the commit side trivial — a pass's records append together or the pass is discarded whole — but 7.1 should state that contract explicitly.
- **Validate model output before acting on it.** Their compressor once accepted a provider's 200-OK error string as the summary. A consolidation pass whose output does not parse into records writes nothing and logs; never best-guess writes into the distilled tier.
- **Three strikes, then skip.** One consistently unparseable session must not wedge the queue forever — bounded retries, then mark it skipped and move on.
- **Bound concurrency.** One consolidation pass at a time is fine for v1; the point is that the bound exists.
- **Attribute the spend.** Their aux usage was invisible until they recorded it at the chokepoint. ARC's spans already carry token counters (§5.4 metrics require them); make sure the task name is on the consolidation span from day one.
- **Tight, shaped output budgets, reject-not-truncate** — small max_tokens per task, structural validation, reject and retry later rather than store garbage (their titling guard, above).

**Thinking as a per-task dial.** We banked "consolidation keeps thinking." Their production finding: reasoning tokens on *mechanical* summarization buy nothing and cost a lot (~31s/pass on a local 7B on a contended box). The distinction that keeps our decision: their compression is mechanical merging; our consolidation is judgment (what is durable, what contradicts, what supersedes). But thinking should stay a per-request dial — it already is, via the engine's `/no_think` mechanism — and 7.3's memory-replay is the instrument to diff thinking-on vs thinking-off on real history before assuming it earns its latency.

**Contention is real on a shared sidecar.** One model serves interactive turns and consolidation; their measured pass times say a pass can visibly delay a turn. Idle-timeout triggering mostly dodges this; 8.2's counters should include pass duration and any interactive-turn wait so a collision is visible when it happens.

**Micro-compaction (their docs/micro-compaction.md)** is mostly not ours — ARC's log never discards, and context compression is a later phase's problem if ever. Two principles are portable to any future session condensation: **user messages are never summarized** ("paraphrasing 'use the existing retry helper, don't add a new one' into a summary is exactly how an agent ends up confidently doing the thing you told it not to, six turns later" — assistant output is derived narration; user prompts are the intent it derives from), and honest metrics — they judge compaction by context occupancy, not by tokens-saved theater.

## What was deliberately not taken

The storage architecture and its migration/repair machinery (the majority of their search module is the tax of SQLite-as-truth; our disposable projection deletes that problem class — the reading confirms the choice). Mid-session context compression. Skills, subagents, gateway, cron: unread, Phase 4+.
