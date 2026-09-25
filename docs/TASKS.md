# TASKS — Phase 3.7, direct sessions

[DESIGN.md](DESIGN.md) governs architecture. This file keeps open work only; completed tasks live in version history.

**Goal:** develop and converse in one kind of session, with one runner, mid-turn messages, visible tool results, and event-based compaction.

**Exit:** a week of development in unified sessions, with compaction on real work and no visible context loss.

Statuses: `todo` → `in progress` → `in review` → `done`. Built does not mean deployed or live-validated.

## Open acceptance

- **Compaction quality, in review.** On September 24, 2026, v3 compaction succeeded through seq 9995 in the skia-tracing fork. Luna summarized ~207k prompt tokens in 575 bytes; the final 769-byte summary omitted computed transforms. Mechanical acceptance passed; context quality remains unproven.
- Replay the presence gate against the historical memory-flood sessions after deployment.
- Live Codex allowance acceptance remains untested.
- Confirm deployment of unified sessions and paginated archive reads.

## Standing watches

Not new implementation tasks until evidence arrives:

- Job continuation vs fresh dispatch; cancellation stays stopped; orphaned shell processes; last-substantive-reply handbacks.
- Memory scope/hoarding, dedup and review-queue quality, entity duplicates, missed facts, grounded-summary drift, post-fork dead tails.
- Edit-tool friction, executor latency, unnecessary code comments.
- `a_cut_stream_appends_a_partial_reply` flakiness.
- Archive search ranking when the current session dominates results.

## Deferred

- **Persistent shell execution:** handles, later output, stdin/interrupts, cancellation cleanup; wait for a concrete need.
- **Model-side image reading:** likely extend `read` with image-bearing provider results. Attachments are separate; Bash/base64 is not vision input.
- **Plan tool:** no `update_plan` planned.
- **Voice, sandboxed workers, devices, embeddings:** later phases, not this cleanup.
- **Jev:** revisit on early access; [ideas](ideas-jev.md), no integration planned.
