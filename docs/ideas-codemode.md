# Codemode: deferred thought

Codemode lets the model write a small program that composes tool calls, processes intermediate results, and returns only what matters. Dependent calls can run without another model turn. Jev is a separate decision-model component, not a requirement.

Starting point: https://x.com/mitsuhiko/status/2105033145188020493

**Possible fit for ARC:** optional programmatic access to existing tools, not a replacement for direct calls. Bash already covers file processing; the interesting addition is composing memory and archive tools through their normal execution path.

A first experiment could search several phrases, deduplicate sessions, read bounded excerpts, and return an evidence bundle with session references. Start stateless and read-only, without Jev or MCP. Compare latency, model tokens, and answer quality against direct calls. Benefits are unmeasured.

Before implementation:

- Inspect result shapes; structured data is easier to compose than display text.
- Bound runtime, memory, concurrency, call count, and output. Keep credentials outside the script.
- Design durable identities and results for nested calls without putting every intermediate result into the model transcript.
- Preserve UNKNOWN outcomes after crashes; replay must not rerun scripts. Timeouts do not roll back actions.

This is an experiment idea, not an approved integration. Amend `DESIGN.md` before changing tool execution or transcript contracts. Leave runtime choice, persistent state, and write-capable scripts for later.
