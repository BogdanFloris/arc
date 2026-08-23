# Providers

`DESIGN.md` defines the provider architecture: four roles, session pinning, and counsel as a tool. This file records the current model for each role, its cost, and when to change it. The principles and role definitions are durable; the rest is a dated snapshot. Update it when it no longer matches reality.

**Status:** Current as of 2026-08-22. The target is under $50/month, down from a $100/month Claude subscription, without reducing capability where it matters.

---

## 1. Principles

These outlive any particular plan.

1. **No vendor lock-in.** Every provider sits behind the `Provider` trait and is swappable by config. The expert is an argv template. Voice stages are traits. Nothing in `arc-core` names a vendor.
2. **ToS-clean only.** API keys, or subscriptions that are explicitly any-tool by design. No consumer OAuth driven from our own harness, no whitelist workarounds, no unpublished endpoints. Learned three times: the Claude OAuth ban of January 2026, Antigravity, the Kimi whitelist.
3. **Route by role, not difficulty.** Config maps each task type to a model. There is no runtime difficulty classifier.
4. **Caching matters.** Roughly 96% of the workload is cache reads. Caches are tied to a model and prompt prefix, so switching models makes a session pay for its full context again. Sessions therefore stay on one provider, and counsel is a tool rather than a routing choice.
5. **Measure cost per completed task, not per token.** A cheaper model that consumes more tokens to finish the same job is not cheaper.
6. **Permitted models use an allow-list.** Never use a deny-list. Provider lineups change without notice, so unknown models must fail closed.

---

## 2. Roles

These roles are stable. The next section records the current model for each one.

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
| **hands** | DeepSeek V4 Flash, via OpenCode Go | Go subscription | $10 (plan) |
| ↳ escalation | DeepSeek V4 Pro first; GLM-5.3 for long-horizon multi-file work; Kimi K3 rarely | same | — |
| **counsel** | Opus via `claude -p`, read-only tools, for both `plan` and `review`. Degrades to Sonnet only under budget pressure | Claude Pro | $20 |
| **local** | Qwen3-8B Q4_K_M on the RTX 5070 | llama.cpp sidecar | $0 |
| **reserve** | Prepaid Zen credit for Go spillover | — | ~$10 |
| | | | **~$50** |

### Why face does not use Go

Go offers open-weight coding models plus Grok 4.5 and GPT 5.6 Luna. It does not offer Claude or Gemini. Face uses a separate key for three reasons:

- **Cost isolation.** Go meters in dollars. Every conversational turn — and every camera frame, once vision is in the loop — competes with the coding budget.
- **Latency.** Kimi K3, Go's most general model, runs at about 38 tokens/second and uses a thinking mode. Voice makes time to first audio the limiting constraint.
- **Vision.** The face needs it for screenshots now and the pan-tilt camera later, and Google's spatial grounding is the strongest cheap option.

Keep thinking off or minimal on face. It adds latency to conversational turns, which is why the local configuration has `no_think`.

### Why hands uses DeepSeek

Go meters dollars, so the cost leader completes the most work. The expected workload is roughly 19M output tokens a month:

| Candidate | Character | Est. monthly against the workload |
| --- | --- | --- |
| **DeepSeek V4 Flash** | The cheaper of the two DeepSeek tiers and the current default. Its rates are not priced here yet. | **under Pro** — rewrite this row from traces |
| DeepSeek V4 Pro | Cost leader among the frontier-class models by a wide margin. 80.6% SWE-bench Verified. MIT. First escalation. | ~$17–25 — fits inside the cap with room |
| GLM-5.3 | Trained for long-horizon agentic tool use. 1M context. ~92 tok/s. | ~$35–45 — fits, tighter |
| Kimi K3 | Highest intelligence index on the plan. Also a documented heavy token consumer, and 38 tok/s. | ~$70+ — exceeds the monthly cap |

Start on Flash and escalate to Pro when a job fails on it. Kimi K3 is the strongest model in the lineup but cannot be the default: it costs more per token and uses more tokens per task. Reserve it for work that has failed on a cheaper model. Use GLM-5.3 for multi-file jobs where its long-horizon tuning justifies the price.

The latest DeepSeek versions on Go are hosted in China and need an explicit opt-in in the workspace settings, done 2026-08-23. Acceptable for hands while the retention terms hold; it is the zero-retention agreement below that governs, not the hosting region.

These are published-rate estimates, not measured spend. Phase 3 adds a role label to every trace span. Rewrite this table from traces after a month of data.

---

## 4. What each plan actually meters

The relevant operating rules.

**OpenCode Go — $10/month, $5 first month.**

- Limits are **dollar-denominated**: $12 per 5 hours, $30 per week, and $60 per month. The monthly cap is binding; a heavy Saturday can consume half the weekly budget.
- OpenAI-compatible endpoint at `https://opencode.ai/zen/go/v1`, model ids of the form `opencode-go/<model>`. Our existing `provider/openai` reaches it with a base URL and a key.
- **Spillover:** with "use balance" enabled, requests fall through to prepaid Zen credit instead of blocking at the cap. Auto-reload stays **off**, which makes the ceiling a hard cap by construction. Free models remain available at the cap regardless.
- It exists because Anthropic blocked third-party tools from subscription credentials. Its key-based, any-client access is the product, not a workaround. Personal ARC meets the own-internal-use terms.

**Excluded on Go, off-config for personal traffic:**

| Model | Reason |
| --- | --- |
| Muse Spark 1.2 Contributor | Trains on prompts and completions. Strictly worse than retention. |
| Grok 4.5, GPT 5.6 Luna | 30-day retention. |

Everything else on the plan has 0-day retention today. DeepSeek's zero-retention agreement has an expiry date; see the review triggers below.

**Claude Pro — $20/month.** Used only through `claude -p` as the counsel tool, with read-only tools, in the project directory. First-party CLI, which is the sanctioned path.

A coding job uses one `plan` and up to *N* `review` calls. Counsel use therefore scales with jobs and review rounds, not conversation. Each call is a short, read-only run over a few files. Measure its use before changing the design.

**Both modes run on Opus.** Review benefits from the strongest available model as much as planning does — finding the real bug is the whole job — so there is no reason to spend the difference on a cheaper reviewer while headroom exists.

Sonnet is counsel's fallback. Enter it at roughly 70% of a window's allowance and return to Opus when the window resets. Spike 1.2 determines whether `claude -p` exposes the needed usage signal. Without one, remain on Opus until rate limited, then use Sonnet for the rest of the window.

**Gemini direct key.** Metered per token, no plan. Cached input is 90% off the base rate, which matters because the face's prefix — identity file plus record index plus recent history — is the most stable prefix in the system.

---

## 5. Economics

These decisions use one month of real Claude Code usage: **about 4.4M input tokens, 19M output tokens, and 640M cache reads per month.** Cache reads are about 96% of the workload.

Priced at Opus 5 list rates ($5/$25 per MTok, cache reads at roughly a tenth of input) that workload is about **$800/month**, against $100 paid. The subsidy on the old plan was closer to 8× than the 4× previously assumed — which is why no combination of routing reproduces it at $30, and why the plan is instead to move the bulk of those tokens onto a model that costs an order of magnitude less per token.

The bet is that **most of those 19M output tokens are mechanical**: edits, tool calls, and rereads. A cheaper model can handle them if the harness is strict. The budget works for either DeepSeek tier but not Kimi K3. The key controls are exact edits with a staleness check, a shell tool that runs the test suite, and rewind for bad paths.

Face is inexpensive. At about 700k monthly output tokens, it costs $3–5 on Flash, $4–6 on Haiku 4.5, and $13–17 on Sonnet 5. Voice may increase turns two- to threefold because speaking is easier than typing. That alone supports using Flash.

---

## 6. Rejected

Revisit an option only when its reason changes.

| Option | Reason |
| --- | --- |
| Claude / Gemini consumer OAuth driven in-harness | ToS, revocation risk. Principle 2. |
| Antigravity gateway | Unpublished endpoint; removed after Phase 1. |
| Native Claude Code replacement at the same cost | Cannot beat an ~8× subscription subsidy. The cost calculation above confirms it. |
| DevPass (LLM Gateway) — $29/$79/$179, ~3× value, frontier models any-tool | The only option that solves face and hands together with frontier models, and still rejected: cheapest tier alone exceeds the budget, the multiple is half of Go's, and it is the flat-rate-reseller category whose economics are unexplained. Now rejected with a number rather than a feeling. |
| GLM Coding Plan (Z.ai) | Keys restricted to approved tools — the blocker for ARC specifically. Also loses the DeepSeek cost floor. |
| Kimi Code plan via the Anthropic surface | The policy is documented but revocable. Do not depend on it. |
| Other flat-rate resellers | Trust and quantization opacity. |
| Kimi K3 as the default hands model | Output price times token consumption exceeds Go's monthly cap. Retained as an escalation. |
| Speech-to-speech APIs for voice | Would own the conversation loop and displace the log as the source of truth. |

---

## 7. Review triggers

Review on events, not a schedule. Three triggers are already dated:

- **2026-08-31 — DeepSeek zero-retention agreement expires.** This is the default `hands` model. Confirm the successor terms or move the default to GLM-5.3.
- **2026-08-31 — Sonnet 5 introductory pricing ends** ($2/$10 → $3/$15). Only matters if the face moves to Sonnet.
- **2027-01-01 — Gemini Flash prices double.** Re-price the face; Flash-Lite and Haiku 4.5 are the alternatives.

Counsel rate-limiting is a trigger rather than a prediction: if a job ever stalls on it, retune the round bound and severity gate, or split counsel by mode, before moving anything else.

Otherwise: any provider ToS or pricing change, Go leaving beta or changing its caps, a GLM-5.5-class release, a GPU upgrade (a 24 GB card makes a local `hands` tier worth re-testing), the first month of real trace data, and the first time Go's monthly cap is actually hit.

---

## 8. What this stack requires of `arc-core`

The implementation obligations that follow. These are Phase 3 work.

- **Add a role label to every `CompletionRequest` and trace span.** One central label makes the estimates above replaceable with measurements.
- **Session-pinned providers.** Role is chosen at session or job creation and does not change for its lifetime.
- **Prefix stability for the face.** Identity file and record index render first and byte-identically; anything volatile goes after them. A timestamp near the front of the prompt silently costs the entire cache discount.
- **A failover chain that distinguishes credit exhaustion from rate limiting.** A 402 at Go's cap is not a retryable 429. Exhaustion falls through to spillover credit if enabled, then to the `local` role, and says so in the client.
- **Per-job budgets**, declared at dispatch and enforced by arcd.
- **The expert as an argv template** — command, working directory, timeout — with read-only enforcement a property of how it is invoked, and one template per mode (`plan`, `review`).
- **A bounded review loop:** configure the maximum rounds and which severity starts another round. A job with unresolved blocking comments reports that outcome honestly.
- **An allow-list of permitted models** per role, so a lineup change fails closed.
- **Cost accounting per completed task**, not per request. That is the metric used to choose a model.
