# Jev: deferred experiments

**Revisit when Bogdan gets early access.** These are experiment ideas, not an implementation plan or a change to `DESIGN.md`. No integration is approved for the current phase.

Starting point: https://typesafe.ai/blog/introducing-system-one-models-and-jev

## Hypothesis

Use a narrow model to choose among allowed actions or score supplied candidates, beside ARC's main model rather than replacing it. Verify the available API, guarantees, latency, pricing, and data handling when access arrives. None of these experiments has been tested.

Valid structured output is not necessarily a correct decision. Treat confidence as an uncalibrated signal until measured on our tasks.

## ARC candidates

- **Memory triage:** classify passages as durable preferences, corrections, temporary task state, or nothing worth saving. Let the archivist perform extraction; do not give the classifier authority to write memory.
- **Search-result ranking:** rank retrieved sessions or memory records against a question before giving results to the main model.
- **Job monitoring:** flag repetition, lack of progress, blockers, or a need for user input. Start as an observer, not something that cancels or steers jobs.
- **Voice intent resolution:** map short commands such as “stop that” to a small set of targets, with an ambiguous option that falls back to conversation. Deferred with voice.

Keep any helper separate from the session's pinned conversational provider. Preserve explicit memory access and append-only durable changes. Amend `DESIGN.md` before adopting an integration that needs new architecture.

## First experiment: memory triage in shadow mode

1. Manually label a small set of past passages, including ambiguous cases and corrections. Check data handling before sending private archive material.
2. Compare Jev against the labels and a simple baseline using held-out passages.
3. Measure missed durable facts, false positives, confidence calibration, latency, and cost.
4. Let predictions write nothing and suppress nothing during evaluation.
5. Adopt only if it reduces work without hiding worthwhile memories; otherwise keep the existing path.

## Robotics candidate: select behaviours, not motor commands

Try instruction-following behaviour selection in a simulator, then bounded ESP32 pan-tilt motion:

```text
ARC:   interpret the goal and propose a plan
Jev:   select an allowed behaviour from structured observations
ESP32: execute bounded motion and enforce limits and timeouts
```

Example choices: `scan_left`, `scan_right`, `hold_position`, `return_home`, `ask_for_help`.

Supply sensor readings and recent observations rather than assuming camera understanding. Keep precise motion timing, obstacle limits, stale-command rejection, and stopping local and independent of model or network availability.

This remains a future device experiment under `DESIGN.md` §9: a separate device MCP server, high-level actions only, and firmware-enforced safety. It is not a reason to add robotics code to ARC before Phase 5.
