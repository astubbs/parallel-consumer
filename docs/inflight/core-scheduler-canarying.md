# Scheduler canarying: A/B a scheduling policy over 1% of ordering domains

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - wants two schedulers worth comparing first; the direct-pull engine (astubbs#361) is the first candidate pair -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)).

When a new scheduling policy ships, assign it a small fraction of *ordering domains* (never
records - the domain is the unit that keeps ordering intact by construction) and compare
throughput, residence, retry rate and commit progress before expanding. Runtime A/B testing of PC
itself - the adaptive controller's experiment-first principle applied to the engine's own
algorithms, making optimizer changes materially safer to roll out. The repo already believes in
this shape: astubbs#333 ships OBSERVE before ENFORCE, and the shipped-vs-direct-pull engine pair
(astubbs#361) is the first real candidate comparison.

**The statistical caveat that decides whether results mean anything:** the 1% of domains is not
the same workload as the 99% - key skew means the canary can draw a hot domain or miss all of
them. Compare like with like (stratify by domain activity, or rotate assignment), or the
comparison reports the sampling, not the scheduler.
