# Hermes-agent — historical lessons

Reviewed NousResearch/hermes-agent at commit `1ed94d2` on August 17, 2026. These are prompt, retrieval, and background-call lessons, not current ARC requirements. [DESIGN.md](DESIGN.md) governs. ARC did not adopt SQLite-as-source-of-truth or its migration/repair machinery; skills, gateway, cron, and subagents were outside this review.

## 1. Memory curation

Source anchors: `tools/memory_tool.py:1173`, `agent/background_review.py:349`, `agent/prompt_builder.py:171`, `agent/curator.py:432`.

- Put save/skip policy in the memory tool description, not recurring conversational nudges.
- Save durable preferences, corrections, and environment/workflow facts. Skip task progress, raw dumps, temporary failures, and facts easily rediscovered through archive search.
- Write declarative facts, not instructions that later sessions may mistake for authority.
- Do not turn unresolved failures into permanent prohibitions. A fixed “command not found” is not evidence that a tool should never be used.
- Before adding a narrow sibling, ask whether an existing record already covers the subject. Pairwise distinctness alone produces clutter.
- Keep success results terse; echoing saved records caused redundant saves.
- Bound always-loaded memory and freeze its session-start snapshot to preserve the prompt cache. Frame recalled material as reference, not fresh user input.
- Background extraction must not block replies, recurse, or write operational instructions into the conversation. Make curation visible to the user.

Hermes needed pressure against saving nothing; ARC initially expected hoarding. Measure both rather than encode either assumption as policy. Its review ran after every N replies, resetting on organic memory use and skipping interrupted/background turns. That trigger did not replace ARC's idle-timeout decision.

The reviewed implementation reported about 26% savings from post-reply review on a warm cache. Treat that as historical evidence to measure in ARC, not a promised saving.

## 2. Session search

Source anchors: `tools/session_search_tool.py:29,1133`, `hermes_state_search.py:1178`.

- Hermes removed LLM summarization of search hits. Return actual messages; spend model work on write-time titles instead.
- Overfetch FTS rows, deduplicate by session, and return snippets with anchors. Opening/closing messages provide goals and outcomes without inventing summaries.
- Bound previews explicitly. Keep full context available through targeted reads.
- Sanitize FTS input without silently swallowing syntax errors. Preserve quoted phrases and punctuation-bearing terms. Regression examples included `it's`, `gateway/run.py`, `user@host`, `a,b`, `50%`, and `TODO: fix`.
- Tool output was about 90% of message bytes in the reviewed store. Exclude it by default at query time, not from the index, so explicit tool searches still work.
- Repetitive machine sessions can overwhelm BM25. Demote noisy sources rather than remove search access; keep injected artifacts out of previews.
- Titles improve both browsing and search. Reject a model answer masquerading as a title rather than truncating it into one; never overwrite user titles.
- Trace the search path and latency. FTS was sufficient there; embeddings were not a prerequisite.

## 3. Background calls

Source anchor: `agent/auxiliary_client.py`.

Use task labels for configuration/accounting, not a difficulty router. Keep timeouts, concurrency, output budgets, and retry bounds explicit. Validate before committing; a malformed session must not wedge the queue. Reject a raced pass rather than apply half of it.

Hermes moved from a cheap auxiliary default to inheriting the main model. That is historical evidence, not ARC's current selection rule: ARC uses its configured archivist. Measure thinking on real extraction tasks instead of assuming mechanical-summary results transfer to durable-fact judgment.

Shared local inference creates contention. Trace pass duration and interactive waits. The review observed roughly 31-second local 7B passes; this is not an Erebor benchmark.

The reviewed micro-compaction policy retained all user messages verbatim. ARC's later DESIGN §4.4 instead preserves originals in the log and bounds the copied user tail. The portable lesson is to measure context occupancy and preservation of user intent, not just claimed token savings.
