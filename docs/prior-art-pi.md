# Pi Durable — candidate improvements

Reviewed the October 1, 2026 announcement on October 2, 2026. Pi was ARC's initial inspiration and is the project Bogdan follows most closely.

These are ranked proposals, not adopted requirements or implementation-verified claims. [DESIGN.md](DESIGN.md) remains authoritative. Amend the relevant contracts before implementation.

Sources: [Pi Durable](https://earendil.com/posts/pi-durable/) and [Pi 1.0](https://earendil.com/posts/pi-1-0/). The Pi observations below come from the announcement, not a source-code review.

## Already aligned

ARC already specifies one storage-owning daemon, attached clients, concurrent forkable sessions, jobs as child sessions, persisted tool intent, and archive retention across compaction. These are not new work.

## 1. Background compaction

**Pi observation:** Summarization runs alongside work, installs at a turn boundary, and blocks only when necessary. Context overflow gets one compact-and-retry attempt.

**ARC proposal:** Prepare an archivist summary before the hard context limit instead of waiting for threshold-triggered compaction to interrupt the next step.

- Snapshot an eligible completed prefix and its cutoff; summarize without holding the engine lock.
- Install only at a safe boundary. Recheck that the prefix and active compaction lineage still match; never cover unfinished calls or newer messages.
- Keep a soft start threshold and a hard reserve for the next response and tool results. Derive reserves from configured limits and measurements, not Pi's example constants.
- Retain the current blocking path when a prepared summary is unavailable. Background failure alone should not end a turn that still fits.
- Retry a provider context-limit rejection once only if compaction advances the cutoff. Do not retry unrelated provider failures.

Keep the existing summary validation, user-tail preservation, archivist selection, and durable `SessionCompacted` event. Background preparation does not remove the prompt-cache miss.

**Design impact:** §4.4 currently makes compaction failure terminal. Specify separate preparation and required-installation failures first.

**Verify:** Messages arriving during preparation, stale summaries, forks, repeated compaction, failure with remaining headroom, and bounded overflow recovery. Measure foreground wait, wasted summaries, and interactive inference contention.

**Priority:** First daily-use experiment. Ship only if measured waits justify the extra coordination.

## 2. Reconnect-safe submissions

**Pi observation:** A stable submission ID deduplicates retries after a crash.

**ARC proposal:** Give sends a durable client-generated idempotency key, separate from the wire's request/response correlation ID.

- Record acceptance and the key through the event log; derive the lookup projection by replay.
- A repeated key with the same payload returns the original session and turn, including its current outcome. A changed payload is an error.
- Define the key's namespace and lifetime. Cover first-message session creation as well as sends to existing sessions.
- Persist acceptance before acknowledging. A lost acknowledgement must not create a second turn or a second session.
- Make reconnect recover accepted work and queued steers without silently resubmitting them. Recover durable history and then attach to live updates without a gap.

This guarantees duplicate suppression at submission, not exactly-once execution of arbitrary tool side effects.

**Design impact:** §7 and additive wire/event schemas. The existing `ClientFrame.request_id` is not a documented durable deduplication contract.

**Verify:** Disconnect before and after acceptance, daemon restart, concurrent retries, payload conflicts, new-session retries, and queued messages.

**Priority:** Before Cairn's reconnect path becomes a daily driver.

## 3. Tool progress and interrupted output

**Pi observation:** Tools publish live output; interrupted calls can retain output produced before the interruption.

**ARC proposal:** Let a long Bash command show bounded progress instead of remaining opaque until completion.

Start with UI-only streaming and an explicit transient status. Preserve the current single, capped durable result as the model's result. Do not let the model act on unlogged progress.

If crash-surviving partial output proves useful, design additive progress events keyed by session, turn, and call, with ordered chunks and one total cap. Define which retained prefix reaches an UNKNOWN result. Progress is evidence, not proof that an operation succeeded.

**Design impact:** §3.1 currently records whole results, not partial tool streams. Durable progress needs an explicit retention, replay, and secret-handling contract; per-line fsync would also need measurement.

**Verify:** Output caps, cancellation, crash between chunks, client reattachment, secret exclusion, and final-result ordering.

**Priority:** After submission reliability; useful for development, not a reason to build a general task framework.

## 4. Reproducible context changes

**Pi observation:** Changed prompt sections are recorded at their transcript position; supported providers can receive only the change.

**ARC proposal:** Make changes to identity, project instructions, and advertised tools explainable across resume and fork.

First audit what ARC snapshots, reloads, and retains today. Then define whether each input is frozen or updated at a boundary. Persist the permitted content or durable reference needed to reconstruct a change; a hash alone cannot reproduce deleted file contents.

Keep identity human-owned. This is recording supplied context, not allowing ARC to edit identity. Do not log credentials. Provider-specific incremental updates are an optimization only after replay semantics are correct.

**Design impact:** §§4.3, 5.1, and 6. Never imply that recording context makes historical model outputs deterministic or guarantees cache preservation.

**Verify:** Editing `AGENTS.md` during work, identity changes, tool configuration changes, restart, forks before/after an update, and provider cache observations.

**Priority:** Audit before adding hot reload or further context-update machinery.

## 5. Explicit crash-resumption policy

**Pi observation:** Checkpointed work resumes automatically; interrupted tools rerun only when declared safe.

**ARC proposal:** Keep today's conservative recovery until interrupted unattended jobs become a demonstrated problem.

Separate two decisions: resuming model work and rerunning an interrupted tool. Neither follows automatically from the other.

- Default tools to no automatic replay. Audit a narrow set of read-only tools before declaring them safe; repeatable does not mean they return the same data.
- Bash, file edits, dispatch, and physical actions remain non-replayable by default. Side-effecting integrations need stable operation IDs and external deduplication or reconciliation.
- Consider opt-in job recovery before automatic recovery of direct conversations. Recover child identity and accepted follow-ups without redispatching children or duplicating handbacks.
- Bound attempts and expose the recovery decision to clients and traces.

**Design impact:** §§3.1 and 4.1 explicitly forbid automatic turn restart. UNKNOWN must remain distinct from failure. Revise those contracts before adding resume machinery.

**Verify:** Crash after tool intent, after an external effect, before result persistence, during child creation, and during handback delivery.

**Priority:** Later. Do not trade honest uncertainty for a more impressive restart demo.

## 6. Cancellation ownership

**Pi observation:** Ownership distinguishes foreground work from background work and determines cancellation propagation.

**ARC proposal:** Document and test ARC's cancellation matrix before changing behavior: stop a reply, cancel a job, cancel parent-owned work, disconnect a client, and stop voice playback.

Detached dispatch is useful. Do not make every child die when its parent's reply ends. If a blocking child-workflow use case appears, add explicit ownership rather than inferring it from the session tree. Voice playback interruption must still leave work running.

**Design impact:** §§4.1, 7, and 7.1. Any new ownership policy must survive replay and client reconnection.

**Verify:** Parent cancellation with detached children, child cancellation with queued steers, disconnect, handbacks after parent cancellation, and playback interruption.

**Priority:** Clarify during mobile/voice work; defer new machinery until a concrete workflow needs it.

## Defer rather than copy

- **Context reset/handoff:** Pi offers a fresh model context with a handoff while retaining history. ARC already has compaction, forks, and archive retrieval. Add a distinct reset only if those fail a real workflow; define fork and memory-consolidation semantics first.
- **Durable application documents:** Use domain events and projections if ARC needs plans, reminders, or actuator state. Do not introduce a generic JSON document store beside the log.
- **Remote tool environments:** Keep tools local until a concrete remote-machine need appears. Preserve scrubbed credentials and make the execution host explicit.
- **Extensions and generic durable tasks:** ARC is a personal assistant, not a harness framework. Borrow individual mechanisms, not Pi's full extension/task system.

Recommended order: measure compaction stalls, build submission deduplication for mobile, then improve tool visibility. Audit context capture and cancellation as correctness work. Revisit automatic recovery only with evidence from unattended jobs.
