# TASKS — Phase 3.5, tree

This is the live list. `TASKS-phase1.md` through `TASKS-phase3.md` are historical records. `DESIGN.md` defines the phase: session forking with the §4 branch semantics, rewind, and tree navigation in the TUI — split out of Phase 3 and kept immediately after it because rewind is a development feature: recovering from a bad edit path without re-prompting from scratch is what makes a cheap executor affordable.

**Phase goal:** branching gets used naturally. **Carried gate:** Phase 3's proving week — a week of real development through ARC — runs through this list; building this phase through ARC is that week.

Bogdan assigns each task; the other reviews it. Statuses are `todo` → `in progress` → `in review` → `done`. Assignees below are intentionally unset.

| # | Task | Assignee | Status |
|---|------|----------|--------|
| 1.1 | The proving week: every row below is developed through ARC — dispatch, `:code`, steers, handbacks. Failures fold into this list the way the drives always did. Closes Phase 3's last exit criterion when seven days of real work have run | — | in progress |
| 1.2 | Counsel wired as a tool — carried from Phase 3 at Bogdan's call: spike 1.2's two verified read-only templates (`claude -p` and `codex exec`, argv in TASKS-phase3.md section 4) become a registry tool the executor and concierge can call for plans and reviews. Read-only is already enforced; what needs deciding is the tool shape and which sessions hold it | — | todo |
| 2.1 | Fork schema, its own commit: the abandoned forking design left reserved field numbers on `SessionCreated` (see 6.34's note) and `fork_point`/`parent_session` columns already in the projection — reuse or re-reserve deliberately, and record what a branch is: parent session, fork seq, and whether it is *real* (feeds consolidation) or scratch | — | todo |
| 2.2 | Fork and rewind in the engine: create a branch from a message, continue it under the same role and provider — same cache economics as `continue_job`. Rewind is fork-at-an-earlier-seq plus opening the branch; no history is ever rewritten (invariant 1) | — | todo |
| 2.3 | Tree navigation in the TUI: see a session's branches, jump between them, mark a branch real or abandoned. Compounds with the picker and `Ctrl-t`; the visual language should stay ASCII-minimal like everything else | — | todo |
| 2.4 | Branch semantics meet memory: only the main line and branches marked real feed consolidation (DESIGN §5.4); abandoned branches stay searchable in the archive and never write distilled memory. The 8.2 role gate composes with this — decide the query, not new machinery | — | todo |

## Standing watches, carried from Phase 3

Not tasks until evidence arrives: whether the continue-vs-dispatch and cancelled-stays-stopped words keep holding; whether inline memory writes keep their stated scope (the next judgment miss triggers the 3.7-low concierge trial); 8.7's week of dedup and review-queue numbers; 9.9's edit-tool friction; 9.2's grant-widening arm; compaction the first time a real job exceeds its window (DESIGN §12 — it becomes a schema task the day it happens).

## Not in this phase

- **A sandboxed worker** — still the honest replacement for the approval gate; revisit once tree work says what the sandbox must allow.
- **Voice (Phase 4), devices (Phase 5), embeddings** — unchanged.
