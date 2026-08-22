# Providers

**What this file is.** `DESIGN.md` §6 defines the seam — four roles, sessions pinned to a provider, counsel as a tool. This file records which model fills each role *today*, what it costs, and what would change the answer. The top half is durable. The bottom half is a dated snapshot and is expected to rot; when it conflicts with reality, fix it here rather than in DESIGN.md.

**Status:** current as of 2026-08-22. Written against a move from a $100/month Claude subscription to a target of under $50/month, with no loss of capability on the work that matters.

---

## 1. Principles

These outlive any particular plan.

1. **No vendor lock-in.** Every provider sits behind the `Provider` trait and is swappable by config. The expert is an argv template. Voice stages are traits. Nothing in `arc-core` names a vendor.
2. **ToS-clean only.** API keys, or subscriptions that are explicitly any-tool by design. No consumer OAuth driven from our own harness, no whitelist workarounds, no unpublished endpoints. Learned three times: the Claude OAuth ban of January 2026, Antigravity, the Kimi whitelist.
3. **Route by role, not by difficulty.** A static task-to-model mapping in config. No runtime classifier deciding how hard a request looks.
4. **Caching is load-bearing.** Roughly 96% of the real workload is cache reads. Caches are model-scoped and prefix-matched, so a session that switches models pays for its whole context again. This is why sessions pin (DESIGN.md §6.1) and why counsel is a tool rather than a route.
5. **Cost is measured per completed task, not per token.** A model that is cheaper per token but burns more tokens finishing the same job is not cheaper. On a dollar-metered plan this compounds.
6. **Permitted models are an allow-list.** Never a deny-list. A router's lineup changes without notice, and a new model with a bad retention policy must fail closed.

---

## 2. Roles

The stable shape, per DESIGN.md §6.1. Which model fills each slot is §3.

| Role | Carries | Selection criteria, in order |
| --- | --- | --- |
| **face** | Conversation, recall, job dispatch. Identity file + record index. | Latency, voice, vision, judgment. Volume is small. |
| **hands** | Job execution. Almost all tokens. | Cost per completed task. Nothing else comes close. |
| **counsel** | Plans, reviews, and unsticking. Read-only, bounded. | Capability. Called a few times per job, not per turn. |
| **local** | Consolidation, extraction, offline fallback for face. | Free and resident. Latency-insensitive. |

---

## 3. Current stack

| Role | Filled by | Access | Est. monthly |
| --- | --- | --- | --- |
| **face** | Gemini 3.7 Flash | Direct API key | ~$10 |
| **hands** | DeepSeek V4 Pro, via OpenCode Go | Go subscription | $10 (plan) |
| ↳ escalation | GLM-5.3 for long-horizon multi-file work; Kimi K3 rarely | same | — |
| **counsel** | Opus via `claude -p`, read-only tools, for both `plan` and `review`. Degrades to Sonnet only under budget pressure | Claude Pro | $20 |
| **local** | Qwen3-8B Q4_K_M on the RTX 5070 | llama.cpp sidecar | $0 |
| **reserve** | Prepaid Zen credit for Go spillover | — | ~$10 |
| | | | **~$50** |

### Why the face is not on Go

Go's lineup is open-weight coding models plus Grok 4.5 and GPT 5.6 Luna. There is no Claude and no Gemini. Three reasons to keep the face on a separate key, none of which depend on claiming those models converse badly:

- **Cost isolation.** Go meters in dollars. Every conversational turn — and every camera frame, once vision is in the loop — competes with the coding budget.
- **Latency.** Kimi K3, the most general-purpose model on the plan, runs about 38 tokens/second and has a thinking mode. Once §7's voice client lands, time-to-first-audio is the binding constraint, and that is not the shape for it.
- **Vision.** The face needs it for screenshots now and the pan-tilt camera later, and Google's spatial grounding is the strongest cheap option.

Thinking stays off or minimal on the face. It is pure latency for a conversational turn — the same reason `no_think` exists in the local config.

### Why hands is DeepSeek V4 Pro and not the best model on the plan

Because Go meters dollars, the cost leader buys the most completed work. Against a workload of roughly 19M output tokens a month:

| Candidate | Character | Est. monthly against the workload |
| --- | --- | --- |
| **DeepSeek V4 Pro** | Cost leader by a wide margin. 80.6% SWE-bench Verified. MIT. | **~$17–25** — fits inside the cap with room |
| GLM-5.3 | Trained for long-horizon agentic tool use. 1M context. ~92 tok/s. | ~$35–45 — fits, tighter |
| Kimi K3 | Highest intelligence index on the plan. Also a documented heavy token consumer, and 38 tok/s. | ~$70+ — exceeds the monthly cap |

Kimi K3 is the best model in the lineup and the one we can least afford as a default: it costs more per token *and* spends more tokens per task. It stays an escalation for work that has already failed on something cheaper. GLM-5.3 is the middle setting for jobs that span many files, where its long-horizon tuning is worth the rate.

These are estimates from published rates, not measured spend. Phase 3's role tag on every span (DESIGN.md §8) replaces them with real numbers, and this table should be rewritten from traces once a month of data exists.

---

## 4. What each plan actually meters

The mechanics that decide behaviour, as distinct from the marketing.

**OpenCode Go — $10/month, $5 first month.**

- Limits are **dollar-denominated**: $12 per 5 hours, $30 per week, $60 per month. The monthly figure is the binding one; the weekly cap is what lets a single heavy Saturday consume half the month.
- OpenAI-compatible endpoint at `https://opencode.ai/zen/go/v1`, model ids of the form `opencode-go/<model>`. Our existing `provider/openai` reaches it with a base URL and a key.
- **Spillover:** with "use balance" enabled, requests fall through to prepaid Zen credit instead of blocking at the cap. Auto-reload stays **off**, which makes the ceiling a hard cap by construction. Free models remain available at the cap regardless.
- It exists because Anthropic blocked third-party tools from using subscription credentials, so a key-based any-client layer is the product rather than a loophole. Terms are own-internal-use, which personal ARC satisfies.

**Excluded on Go, off-config for personal traffic:**

| Model | Reason |
| --- | --- |
| Muse Spark 1.2 Contributor | Trains on prompts and completions. Strictly worse than retention. |
| Grok 4.5, GPT 5.6 Luna | 30-day retention. |

Everything else on the plan is 0-day retention today. DeepSeek's zero-retention agreement is dated — see §7.

**Claude Pro — $20/month.** Used only through `claude -p` as the counsel tool, with read-only tools, in the project directory. First-party CLI, which is the sanctioned path.

A coding job costs one `plan` plus up to *N* `review` invocations, so counsel consumption scales with jobs times rounds rather than with conversation. That sounds like more than it is: each invocation is a short read-only run over a few files, far smaller than the interactive Opus/Sonnet sessions that already fit comfortably inside Pro for a full day of coding. Treat it as a thing to measure, not a thing to design around.

**Both modes run on Opus.** Review benefits from the strongest available model as much as planning does — finding the real bug is the whole job — so there is no reason to spend the difference on a cheaper reviewer while headroom exists.

Sonnet is the second rung of counsel's ladder (DESIGN.md §6.1), entered at roughly 70% of the window's allowance and left when the window resets. Whether that threshold is *measurable* is open: it needs visibility into Pro consumption that `claude -p` may not expose. Spike 1.2 settles it. If no usage signal exists, the degrade becomes reactive — stay on Opus until a rate-limit signal appears, then Sonnet for the rest of the window — which is the same ladder with a different trigger.

**Gemini direct key.** Metered per token, no plan. Cached input is 90% off the base rate, which matters because the face's prefix — identity file plus record index plus recent history — is the most stable prefix in the system.

---

## 5. Economics

The profile the decisions were made against, measured from a month of real Claude Code usage: **~4.4M input, ~19M output, ~640M cache reads per month.** Cache reads are ~96% of it.

Priced at Opus 5 list rates ($5/$25 per MTok, cache reads at roughly a tenth of input) that workload is about **$800/month**, against $100 paid. The subsidy on the old plan was closer to 8× than the 4× previously assumed — which is why no combination of routing reproduces it at $30, and why the plan is instead to move the bulk of those tokens onto a model that costs an order of magnitude less per token.

The bet, stated plainly: **most of those 19M output tokens are mechanical** — edits, tool calls, re-reads — and a cheap model does them acceptably when the harness is strict. The arithmetic works on DeepSeek V4 Pro and does not work on Kimi K3. What determines whether it holds in practice is not the model but the harness: strict `edit` with a staleness check, a shell tool that can run the test suite, and rewind for bad paths (DESIGN.md §4.3, §11).

The face is cheap and was nearly over-engineered. At roughly 700k output tokens a month of conversation it is about $3–5 on Flash, $4–6 on Haiku 4.5, $13–17 on Sonnet 5. Voice will raise turn count, plausibly two to three times, because talking is lower-friction than typing — which is an argument for Flash on its own.

---

## 6. Rejected

Each with the date and the one-line reason. Revisit only when the reason changes.

| Option | Reason |
| --- | --- |
| Claude / Gemini consumer OAuth driven in-harness | ToS, revocation risk. Principle 2. |
| Antigravity gateway | Unpublished endpoint; removed after Phase 1. |
| Native Claude Code replacement at the same cost | Cannot beat an ~8× subscription subsidy. Confirmed by §5's arithmetic, not assumed. |
| DevPass (LLM Gateway) — $29/$79/$179, ~3× value, frontier models any-tool | The only option that solves face and hands together with frontier models, and still rejected: cheapest tier alone exceeds the budget, the multiple is half of Go's, and it is the flat-rate-reseller category whose economics are unexplained. Now rejected with a number rather than a feeling. |
| GLM Coding Plan (Z.ai) | Keys restricted to approved tools — the blocker for ARC specifically. Also loses the DeepSeek cost floor. |
| Kimi Code plan via the Anthropic surface | Documented but revocable policy. Not load-bearing. |
| Other flat-rate resellers | Trust and quantization opacity. |
| Kimi K3 as the default hands model | Output price times token consumption exceeds Go's monthly cap. Retained as an escalation. |
| Speech-to-speech APIs for voice | Would own the conversation loop and displace the log as the source of truth. See DESIGN.md §7. |

---

## 7. Review triggers

Events, not a schedule. Three are already dated:

- **2026-08-31 — DeepSeek zero-retention agreement expires.** This is the default `hands` model. Confirm the successor terms or move the default to GLM-5.3.
- **2026-08-31 — Sonnet 5 introductory pricing ends** ($2/$10 → $3/$15). Only matters if the face moves to Sonnet.
- **2027-01-01 — Gemini Flash prices double.** Re-price the face; Flash-Lite and Haiku 4.5 are the alternatives.

Counsel rate-limiting is a trigger rather than a prediction: if a job ever stalls on it, retune the round bound and severity gate, or split counsel by mode, before moving anything else.

Otherwise: any provider ToS or pricing change, Go leaving beta or changing its caps, a GLM-5.5-class release, a GPU upgrade (a 24 GB card makes a local `hands` tier worth re-testing), the first month of real trace data, and the first time Go's monthly cap is actually hit.

---

## 8. What this stack requires of `arc-core`

The implementation obligations that follow. These are Phase 3 work.

- **A role label on every `CompletionRequest`, landing on its span.** hermes-agent's per-task chokepoint is the proven shape. Without it, none of §3's numbers can ever be replaced by measurements.
- **Session-pinned providers.** Role is chosen at session or job creation and does not change for its lifetime.
- **Prefix stability for the face.** Identity file and record index render first and byte-identically; anything volatile goes after them. A timestamp near the front of the prompt silently costs the entire cache discount.
- **A failover chain that distinguishes credit exhaustion from rate limiting.** A 402 at Go's cap is not a retryable 429. Exhaustion falls through to spillover credit if enabled, then to the `local` role, and says so in the client.
- **Per-job budgets**, declared at dispatch and enforced by arcd (DESIGN.md §4.1).
- **The expert as an argv template** — command, working directory, timeout — with read-only enforcement a property of how it is invoked, and one template per mode (`plan`, `review`).
- **A bounded review loop**: a configured maximum number of rounds, a severity gate deciding which comments justify another round, and honest termination — done-with-unresolved is a reportable outcome, not a failure to hide (DESIGN.md §6.2).
- **An allow-list of permitted models** per role, so a lineup change fails closed.
- **Cost accounting per completed task**, not per request, since that is the figure §3 is chosen on.
