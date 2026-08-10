# Whole-batch drain time may narrow the "no penalty" claim

**Open question, and it may falsify something we are close to publishing.** Raised 2026-08-11.

The Kafka Streams module claims there is no penalty when Parallel Consumer cannot parallelise - the case
where every record shares one key, so key ordering permits at most one in flight and there is nothing to
gain. Wake-on-work moved that control arm from about 0.69x to about 1.00x, and that figure is already
written into the module README and the plan.

**That figure is the median fast record. Nobody has measured whole-batch drain time after the fix.**

The demonstration example, built on the spike branch before wake-on-work, reports three statistics for the
single-key control:

| single-key control | stock | PC | ratio |
|---|---|---|---|
| fastest record | 1543ms | 1562ms | 0.99x |
| median record | 1879ms | 2688ms | 0.70x |
| **whole batch drained** | **2247ms** | **3909ms** | **0.57x** |

Per-record latency and end-to-end drain can genuinely disagree: the pool handoff and completion feedback
cost something per record, so a workload can improve individual latency while getting slower overall. If
drain time is still materially below parity after wake-on-work, the claim holds only for one statistic and
must be narrowed to say so.

**This is the number a sceptic computes first.** Total wall clock needs no explanation of percentiles, and
being shown it by a reader after publication is much worse than finding it ourselves.

The realistic-domain benchmark work has been asked to measure drain time in every arm and to re-measure
the single-key case specifically. Until that returns, treat "no penalty" as unverified for drain time.

## Delete when

Drain time is measured post-wake-on-work and the claim is either confirmed or narrowed to the statistic it
actually holds for, in both the module README and the plan.
