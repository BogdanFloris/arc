# Hermes-agent — prior-art notes for Phase 2

Lessons from NousResearch/hermes-agent (commit `1ed94d2`, read 2026-08-17), a production personal-agent harness with similar goals. This review covers prompts, policies, and configuration shapes only. ARC rejects its SQLite-as-source-of-truth storage design, so it adopts no structural storage work. Notes identify problems that ARC's event log removes. Skill self-improvement, subagents, the platform gateway, and cron were out of scope as Phase 4+ work.

The sections map to ARC work: memory curation (§5.4; tasks 7.x and 8.1), session search (§5.3; tasks 5.x), and background model calls (§12 routing; tasks 7.x). File and line references point to the pinned commit.

## 1. Memory curation — prompts and nudge policy

Their curation has three parts: an always-loaded write policy in the memory tool description, a background review after every N replies, and periodic consolidation. ARC maps this to explicit `memory_write`, end-of-session extraction, and weekly review. Their prompt text is the closest available production-tested input for task 7.2's consolidation prompt.

**The write policy belongs in the tool description, not injected reminders.** No per-turn nudge enters the conversation. The tool schema carries the policy (`tools/memory_tool.py:1173`):

> "WHEN: save proactively when the user states a preference, correction, or personal detail, or you learn a stable fact about their environment, conventions, or workflow. Priority: user preferences & corrections > environment facts > procedures. The best memory stops the user repeating themselves."
> "SKIP: trivial/obvious info, easily re-discovered facts, raw data dumps, task progress, completed-work logs, temporary TODO state (use session_search for those)."

This follows invariant 6. Tool descriptions are already sanctioned context, and this is where Hermes gained the most leverage per token. Task 6.3's `memory_write` description should use a similar WHEN/SKIP policy.

**Prompt content for task 7.2.** Four rules address observed failures:

1. **A do-not-capture list against self-poisoning** (`agent/background_review.py:349`): no environment-dependent failures ("command not found" — the user can fix these), no negative claims about tools ("'X is broken' hardens into refusals the agent cites against itself for months after the actual problem was fixed"), no transient errors that resolved ("if retrying worked, the lesson is the retry pattern, not the original failure"), no unresolved failures dressed up as workflow ("never the dead ends, and never dressed up as best practice"). Supersede does not save us here — poison phrased as a durable fact looks like memory working.
2. **Declarative facts, not imperatives** (`agent/prompt_builder.py:171`): "'User prefers concise responses' ✓ — 'Always respond concisely' ✗... Imperative phrasing gets re-read as a directive in later sessions and can cause repeated work or override the user's current request." A phrasing constraint for record bodies and a checklist line for 8.1's review.
3. **The staleness test with an escape hatch**: "If a fact will be stale in a week, it does not belong in memory" — and every SKIP category points at session search instead. ARC has the same two tiers; the consolidation prompt should say the archive already remembers everything, so only extract what must be in the always-loaded index. This aims squarely at §5.4's hoarding failure mode.
4. **The umbrella bar for merging** (`agent/curator.py:432`): "Pairwise distinctness is the wrong bar. The right bar is: 'would a human maintainer write this as N separate skills, or as one skill with N labeled subsections?'" — their hardest-won extraction lesson. 7.2's merge step should ask "does an existing record cover this class?" before creating, and 8.1 should watch for narrow-sibling proliferation, not just record count.

**Nudge policy.** A turn counter (default 10) runs review *after* the reply, so it does not compete for model attention. It resets when the model uses memory organically, so nudges run only when nothing is being saved. Suppress them for interrupted turns and background contexts; the review pass cannot recurse. The prompt asks two questions—what the user revealed about themselves and how they want the agent to operate—and says to stop with “Nothing to save.” when appropriate.

**Mechanism lessons, keyed to tasks:**

- **Memory never blocks a reply.** Hermes retries a failed write a bounded number of times, then reports “save skipped.” ARC task 7.1 should log and yield on a failed consolidation pass; it must not block the daemon or session.
- **Terse, terminal tool results.** Their write-success response deliberately does not echo the saved entries — echoing caused observed thrash (repeated redundant saves). Shape 6.3's results the same way.
- **A hard budget forces curation.** Their always-loaded memory has a character cap. At capacity, adds are rejected with the full list and an instruction to consolidate. ARC's always-loaded index (6.2) needs a budget from day one because an 8k context makes it mandatory.
- **Frozen snapshot per session.** Memory is injected as a session-start snapshot; mid-session writes hit disk but not the live prompt, preserving the prefix cache. 6.2 should refresh the index at session boundaries, not per turn.
- **Fence injected memory as reference, not instruction.** Their recalled context is wrapped in "[System note: ... NOT new user input. Treat as authoritative reference data]". 6.2's index injection should carry the same frame — it compounds with rule 2 above.
- **Curation output is visible to the user.** Every background write appears in the UI. Task 7.1 should render consolidation activity in the TUI; the decision that tool activity is visible extends to the asynchronous pass.
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
- **FTS-first, no embeddings: validated.** A mature production agent runs recall entirely on FTS5, with no vector index in its session store. Keep sqlite-vec as a future option; it is not urgent.

## 3. Background model calls — routing design

Phase 2 adds ARC's first background calls: consolidation, then likely titling. Hermes supports adding the routing design with those calls rather than retrofitting it later.

**Use a task label, not a router.** Every Hermes background call passes through one function keyed by task name (`agent/auxiliary_client.py`). The task name selects its configuration—provider, model, timeout, concurrency, reasoning level, and fallback chain—and appears in accounting and logs. ARC needs every background `CompletionRequest` to carry a task name and every trace span to record it. Later per-task configuration can use that label without changing call sites.

**They changed the default to “aux = main model.”** Their original default sent background work to a cheap hosted model. The current code inherits the user’s selected provider and model, with cheaper models only as fallback. ARC’s decision to use the same local model for consolidation matches that conclusion. Fast-model routing remains an opt-in per task; titling qualifies because a title is about eight tokens.

**Aux-call policy, as rules for 7.1/7.2:**

- **Background timeouts are their own config, generous for summarization-class work** (their default 30s, with a 300s floor for compression). Consolidation gets its own timeout, not the interactive one.
- **Passes are atomic.** A user interrupt must not tear a pass into a half-applied state. Events make the commit side trivial — a pass's records append together or the pass is discarded whole — but 7.1 should state that contract explicitly.
- **Validate model output before acting on it.** Their compressor once accepted a provider's 200-OK error string as the summary. A consolidation pass whose output does not parse into records writes nothing and logs; never best-guess writes into the distilled tier.
- **Three strikes, then skip.** One consistently unparseable session must not wedge the queue forever — bounded retries, then mark it skipped and move on.
- **Bound concurrency.** One consolidation pass at a time is fine for v1; the point is that the bound exists.
- **Attribute the spend.** Their background use was invisible until they recorded it centrally. ARC spans already have token counters; record the task name on consolidation spans from day one.
- **Tight, shaped output budgets, reject-not-truncate** — small max_tokens per task, structural validation, reject and retry later rather than store garbage (their titling guard, above).

**Thinking is a per-task setting.** ARC currently keeps thinking enabled for consolidation. Hermes found that reasoning adds cost without helping mechanical summarization (about 31 seconds per pass on a busy local 7B). ARC consolidation requires judgment about durable facts and contradictions, so it may benefit. Keep thinking configurable per request, and use `memory-replay` to compare it on real history before assuming the latency is worthwhile.

**Contention is real on a shared sidecar.** One model serves interactive turns and consolidation; their measured pass times say a pass can visibly delay a turn. Idle-timeout triggering mostly dodges this; 8.2's counters should include pass duration and any interactive-turn wait so a collision is visible when it happens.

**Micro-compaction (their docs/micro-compaction.md)** is mostly not ours — ARC's log never discards, and context compression is a later phase's problem if ever. Two principles are portable to any future session condensation: **user messages are never summarized** ("paraphrasing 'use the existing retry helper, don't add a new one' into a summary is exactly how an agent ends up confidently doing the thing you told it not to, six turns later" — assistant output is derived narration; user prompts are the intent it derives from), and honest metrics — they judge compaction by context occupancy, not by tokens-saved theater.

## What was deliberately not taken

The storage architecture and its migration/repair machinery (the majority of their search module is the tax of SQLite-as-truth; our disposable projection deletes that problem class — the reading confirms the choice). Mid-session context compression. Skills, subagents, gateway, cron: unread, Phase 4+.
