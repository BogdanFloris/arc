# Providers

[DESIGN §6](DESIGN.md#6-providers) defines roles and session pinning.
This file records configuration and measurements, not a live price or terms feed.

## 1. Principles

1. **No vendor lock-in.** Reasoning providers sit behind `Provider`; voice backends are replaceable adapters.
2. **ToS-clean only.** Use API keys or subscriptions explicitly open to third-party tools. No whitelist workarounds or consumer OAuth by default.

   **Codex exception, decided 2026-09-06:** permit ChatGPT-plan OAuth through Codex, based on OpenAI's public endorsement of third-party harnesses and named support for pi, OpenCode, Cline, and OpenClaw. This decision does not extend to Anthropic or Google. Public endorsement is not a permanent terms guarantee: keep the exception revocable behind `Provider`, and review it when OpenAI's position changes.
3. **Route by role, not difficulty.** Models are configured, not chosen by a runtime classifier.
4. **Keep prefixes stable.** Long sessions are dominated by cache reads. Pin models; render identity/index first and put volatile context after them.
5. **Measure cost per completed task**, not price per token.
6. **Allow-list models.** Unknown models fail closed.

## 2. Roles

| Role | Optimize for |
| --- | --- |
| chat | Latency, voice, vision, judgment. |
| code | Interactive judgment and collaboration. |
| executor | Cost per completed delegated task. |
| archivist | Extraction/compaction quality and bulk cost. |

## 3. Recorded configuration — 2026-09-24

Live configuration is `~/.config/arc/arc.toml`. Durable role selections can override these configured defaults for new sessions; open sessions keep their pins.

| Role | Default preset | Access |
| --- | --- | --- |
| chat | astra | Codex |
| code | sol | Codex |
| executor | sol | Codex |
| archivist | luna | Codex |

Compaction uses the selected archivist. Only oversized summaries get one bounded shrink call; empty text or failed repair fails visibly, without fallback to the session model. See DESIGN §4.4.

The local llama.cpp configuration remains available but no configured role uses it, so this configuration starts no sidecar.

### Configuration rules

Interactive development and workers select independently. With presets already declared:

```toml
[roles.code]
choices = ["astra", "sol"]

[roles.executor]
choices = ["sol", "astra"]
```

First choice is default until a durable role selection overrides it. The session model menu creates/forks under a preset; changing the role default is a separate action. Omitted `roles.code` inherits the executor menu, not its selection. Legacy sessions retain their role; forks keep that role but choose a new pin.

Codex defaults to `read`, `bash`, `apply_patch`; other providers to `read`, `bash`, `edit`, `write`. Presets and inline roles may override with `editing = "patch"` or `"replacement"`. The editing interface is pinned at creation too.

## 4. Provider-specific operation

**Codex.** `provider = "codex"`; `key` names the credential under `data/secrets/`. Run `arcd login codex` for device-code login, then restart the daemon after a fresh login. Refresh updates the credential in place. Responses use `store: false`; encrypted reasoning is preserved in tool-call roundtrip bytes. `thinking` maps to reasoning effort with `low` as floor; `default` omits it. Usage-limit errors report reset times. The plan meters rolling allowance windows, not token dollars.

```toml
[roles.executor]
provider       = "codex"
model          = "gpt-5.6-sol"
key            = "codex"
thinking       = "medium"
context_window = 272000
```

**OpenAI-compatible / OpenCode Go.** Configure the base endpoint without `/v1`; arcd appends `/v1/chat/completions`. Go uses bare model IDs. From the 2026-09-06 integration, requests carry ARC's session ID as `x-opencode-session` and identify as `arc/<version>`. Turns, compaction, titles, and extraction keep the source session ID stable across calls/restarts; probes supply their own conversation ID. DeepSeek's seed range is `[0, 2^63)`, so serialization masks the unsigned seed's top bit.

Go spillover can use prepaid Zen credit; keep auto-reload off to preserve a hard spending ceiling. Recheck caps and retention before returning to it as the default. The earlier configuration excluded Muse Spark for training on traffic and Grok/Luna for 30-day retention; that was a dated plan policy, not a current provider-wide claim.

**Gemini.** Direct API key. The August measurements below informed the old chat choice; do not assume those capabilities or rates apply to another model.

## 5. Historical measurements

The earlier budget target was under $50/month: Go workers, Gemini chat, local Qwen archivist. That stack is no longer the recorded configuration above.

Measured 2026-08-26–28, from durable turn usage (archivist from spans). Two days included an unusual 31-minute arena turn; this is calibration, not a steady-state monthly forecast.

| Role/model then | Turns/calls | Input | Output | Latency avg/max |
| --- | --- | --- | --- | --- |
| Executor, GLM-5.3-Flash on Go | 28 | 13.4M | 211k | 244s / 1837s |
| Chat, Gemini 3.6 Flash minimal | 45 | 228k | 7.0k | 3.1s / 7.5s |
| Archivist, local Qwen3-8B | 34 on August 28 | 24.3k | 214 | unmeasured here |

Executor steps showed about 99% cached input in spans/journal; the projection totals did not store the cached share. Provider dashboards remain the authority for actual charges. The old Claude workload was about 4.4M input, 19M output, and 640M cache-read tokens/month; extrapolating that output volume to ARC proved too high.

**Gemini probe, 2026-08-24:** five runs gave 28–33 output tokens on 3.6 `minimal`, 31–337 on 3.7 `low`, and 334–410 on 3.6 `low`. `minimal` was unsupported on 3.7 in the tested paths. Thinking was billed but not streamed; use `total_tokens - prompt_tokens`, not just `completion_tokens`, when accounting for it. These results supported low-latency chat, not a blanket recommendation for today's model lineup.

## 6. Rejected options

Revisit when the reason changes, not because the old price table looks attractive.

| Option | Reason |
| --- | --- |
| Claude/Gemini consumer OAuth in ARC | No approved exception under principle 2. |
| Antigravity gateway | Unpublished endpoint; removed after Phase 1. |
| Approved-tool-only coding plans | ARC is not an approved client. |
| Unexplained flat-rate resellers | Trust, retention, and quantization uncertainty. |
| Kimi as the old Go default | Measured/planned token consumption exceeded that budget; escalation only. |
| Reproducing the old Claude subscription at API rates | Subscription subsidy made the price comparison unrealistic. |

The former blanket rejection of speech-to-speech is superseded by DESIGN §7.1's bounded Phase 4 prototype. No voice integration is implied here.

## 7. Review triggers

- Any change to ChatGPT-plan third-party harness permission: re-evaluate principle 2.
- Provider terms, retention, pricing, or allowance changes; first rate-limit/spend problem on a real task.
- Reusing Go/DeepSeek: the recorded zero-retention agreement expired August 31, 2026; successor terms are not established by this file.
- The previously recorded Gemini price change on January 1, 2027, if Gemini returns to the stack; verify rather than rely on the old estimate.
- A representative week of completed-task measurements, or a GPU upgrade that makes local workers worth retesting.

## 8. Implementation boundaries

Role labels, stable prefixes, model allow-lists, explicit provider failures, and pinned sessions are current requirements. Do not revive old plans for automatic model failover or model-authored job budgets: DESIGN governs those decisions.
