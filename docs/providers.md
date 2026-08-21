docs/providers.md — LLM Provider Decisions
Status line:

1. Principles (the invariants behind every choice)
   - No vendor lock-in: every provider behind the Provider trait, swappable by config
   - ToS-clean only: API keys or explicitly any-tool subscriptions; no consumer
     OAuth, no whitelist workarounds, no unpublished endpoints (learned 3x:
     Claude OAuth ban Jan 2026, Antigravity, Kimi whitelist)
   - Route by role, not difficulty: static task→model mapping; no runtime
     difficulty classifiers
   - Caching is load-bearing: ~96% of real workload is cache reads (own ccusage
     data, Aug 2026); providers without prompt caching are disqualified for
     high-volume roles

2. Current stack (the decision)
   - background/easy → local llama.cpp (Qwen-class ~14B), daemon-managed sidecar
   - chat + implement → OpenCode Go ($10/mo, ~6x usage value, any-tool by design,
     "Use balance" overflow from prepaid Zen credits = hard cap by construction;
     auto-reload off)
   - plan → Opus/Fable via Claude Code delegation (claude -p, read-only tools,
     Pro plan; first-party CLI = sanctioned)
   - expert/think -> Opus/Fable via Claude Code delegation. The idea here is for ARC
     to consult an expert or something when I ask it to so he has access to a more
     powerful model
   - reserve → DeepSeek direct API key (cache-heavy coding bursts; note the
     Aug 17 2026 repricing + peak/off-peak windows)
   - exclusions: log-retaining models on Go (Grok 4.5, GPT 5.6 Luna) off-config
     for personal traffic

3. Role table (mirror of the actual config, kept in sync)
   - the [roles.*] TOML block + escalation limits, verbatim

4. Rejected options (dated, with the one-line reason)
   - Claude/Gemini consumer OAuth in-harness — ToS, revocation risk
   - Antigravity gateway — unpublished endpoint, removed after Phase 1
   - GLM Coding Plan — keys restricted to approved tools; value decline
   - Kimi Code plan via Anthropic surface — documented but revocable policy;
     not load-bearing
   - Flat-rate resellers (CheapestInference etc.) — trust/quantization opacity
   - K3 as default tier — output pricing kills it at scale ($15/M × real volume)
   - Full native Claude Code replacement — can't beat ~4x subscription subsidy

5. Economics reference (the numbers decisions were made against)
   - own usage profile: ~4.4M input / ~19M output / ~640M cache reads per month
   - the three scenario costs (V4 Pro first-party vs DeepInfra, K3) as computed
   - what would change the math: cache pricing shifts, Go cap changes, GPU upgrade
     (24GB card ⇒ local coding tier becomes real)

6. Implementation requirements the stack imposes on arc-core
   - emit cache_control / prompt_cache_key per session
   - provider failover chain incl. 402-credit exhaustion (Hermes pattern)
   - Perfetto: role tag on every LLM span; counter on Go rate-limit headers
   - peak/off-peak awareness for batchable background work

7. Review triggers (not a schedule — events)
   - any provider ToS/pricing change, Go leaving beta, GLM-5.5-class releases,
     hardware upgrade, ARC becoming the daily coding driver
