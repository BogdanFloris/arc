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

## Model routing and tool auto mode (local-only)

Bogdan also wants to explore model routing and automatic tool-call decisions, conditional on running the decision model locally. Early access alone does not meet that condition. The public introduction and documentation reviewed on 2026-09-16 describe an API; local weights, licensing, hardware requirements, and self-hosted inference remain unconfirmed.

- **Model routing:** select a configured model preset when creating a session or dispatching a job, based on task requirements, cost, and urgency. Keep the selected model pinned for that session; per-step switching is not proposed. Compare against static role defaults, including routing overhead and total task cost, not just per-call price. DESIGN §12 currently chooses static roles, so adopting runtime routing needs a design amendment.
- **Tool auto mode:** assess whether a proposed action matches the user's request and an explicitly configured scope. Supply the exact tool arguments, working directory, relevant state, and policy. Ask separate questions about intent, destructive effects, scope, and uncertainty rather than one blanket “is this safe?” question. Candidate outcomes are allow within policy, deny, or defer for clarification at a turn boundary.
- **Boundary:** explore reducing manual project scoping, but do not make a model's approval the sole filesystem or execution boundary. A future broader policy must still be enforced by deterministic permissions or a sandbox; the model cannot grant itself new authority. Current structured file tools enforce granted roots, while Bash starts in the project but is not filesystem-confined. A classifier does not fix that gap.
- **Evaluation:** start in shadow mode. Include misleading instructions in files/tool output, opaque scripts, indirect subprocess effects, destructive commands, and state changes between assessment and execution. Measure dangerous approvals separately from unnecessary refusals. Missing context, low confidence, or model failure must not widen permissions.

Local execution is a privacy and availability requirement for this proposal, not evidence of decision correctness. Benchmark latency and resource contention on Erebor. DESIGN §4.3 and invariant 8 currently use contained tools rather than model approval or mid-turn prompts; adopting auto mode requires an explicit design change, not silently replacing grants.

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
