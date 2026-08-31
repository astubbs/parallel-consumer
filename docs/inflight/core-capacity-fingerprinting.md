# Capacity fingerprinting: remember what the controller learned

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - needs the controller in production first; persistence home is an open design question -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)) - its author's favourite of the batch, and it
names a recurring project pattern: *PC learns things transiently because execution needs them; the
cheap feature is remembering.*

A function's runtime fingerprint - parallelism source, useful-concurrency range, handler profile,
retry amplification, scale-out response, the p99 knee - is what the adaptive controller
(astubbs#333) discovers and discards on every restart. Persist it and three things fall out:

- **Warm starts.** A restart (or a deploy to a new environment) bootstraps near the learned
  operating region and verifies experimentally, instead of ramping from ignorance - directly
  shortening the post-rebalance cooldown windows [`core-auto-scaling.md`](core-auto-scaling.md)
  already designs around.
- **Empirical workload models** - observed, not configured; "CPU-heavy" as a measurement rather
  than a label.
- **Semantic regression detection.** "useful concurrency fell 210 -> 85 after release 4.8, and
  downstream latency sensitivity doubled" - production itself reports that the application's
  execution characteristics changed, no benchmark run required.

**Open question to settle before building: where the fingerprint lives.** Commit metadata is
capped at 4096 chars and already contended (astubbs#306); local disk dies with the pod; a
compacted topic is the Kafka-native answer but is new infrastructure. Also: a prior must expire -
a fingerprint from last month's key distribution can be worse than ignorance, so the verify step
is mandatory, not an optimisation.
