# TASKS

The live list. Frozen records: `TASKS-phase1.md` (walking skeleton, closed 2026-08-13), `TASKS-phase2.md` (memory, closed 2026-08-22). DESIGN.md §11 is the phase contract; Phase 3's tasks are not yet cut.

Working agreement, unchanged: Bogdan assigns each task (`bogdan` or `claude`). The implementer's work is reviewed by the other. Statuses: `todo` → `in progress` → `in review` → `done`.

## Carried forward (fold into the next touch, or Phase 3's cutting)

- **Prompt v2**, when extraction quality warrants it, has two pinned regression cases: kill storytelling-class one-off inference; never miss an explicit correction ("use less emojis", session `c4781d89…`). The review verdicts in the log are its few-shots; `memory-replay --against` is its gate.
- **Sidecar restart policy** (from Phase 1) — more pressing now that background passes depend on the sidecar.
- **Token budget watch**: seven tool schemas ≈1.9k tokens + the always-loaded index on a 16k context; measure before anything else always-on is added.
- **Session titling pass** — `sessions.title` still has no writer; hermes says titles are search infrastructure and the natural second background task (`prior-art-hermes.md` §2).
- ~~**Provider routing**~~ — decided 2026-08-22 as static roles (DESIGN.md §6.1, `providers.md`), off the usage data Phase 2 generated. Folds into the Phase 3 cut, not carried.
- Phase 1 leftovers unchanged: `log::Error::Io` field doc; no retention on `data/traces/`; the wire-friction list (banked in `TASKS-phase1.md`) for the next `wire.proto` evolution.
