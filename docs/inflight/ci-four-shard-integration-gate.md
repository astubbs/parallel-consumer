# Four integration-test shards: built, measured, deliberately not taken

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->
<!-- inflight-state: deferred - a cost trade: 61s of critical path for +526 runner-seconds and four lists to maintain instead of one. Revisit if runner-minutes stop mattering, the catch-all shard dominates again as the suite grows, or the gate has to come in under ~6 minutes for a specific reason -->

The `Integration Tests` lane runs as two shards (astubbs/parallel-consumer#442). A four-shard
arrangement was built on `ci/shard-integration-four`, measured green, and not merged. It is kept
here as future work rather than a rejected idea: it works, it is faster, and the reason it is not
in use is a trade that could reasonably be revisited. Everything below was measured, not modelled;
the reasoning that led here, and the two arithmetic traps, are in
[`../solutions/performance-issues/shard-count-buys-nothing-while-one-class-sets-the-floor-2026-09-07.md`](../solutions/performance-issues/shard-count-buys-nothing-while-one-class-sets-the-floor-2026-09-07.md).

| arrangement | critical path | runner-seconds | per-shard walls |
|---|---:|---:|---|
| 2 shards + split + rebalanced (**in use**) | **416s** | 792s | 416 / 376 |
| **4 shards + probe split** | **355s** | 1318s | 355 / 311 / 332 / 320 |

## If it is picked up

- **Re-derive, do not restore.** The four-way lists on that branch were sized from
  `Rebalance857CommitSyncDeadlockProbeIT` as one class and are stale by construction - the probe
  is now four classes, and per-class times have moved since.
- **It needs a four-way calculator, which does not exist.** `bin/check-integration-shard-balance.mjs`
  deliberately models only the shipped shape - one named heavy set plus a catch-all - and searches
  only two-way "largest N classes" splits, because that is the choice the guide in
  `bin/ci-integration-test.sh` offers a maintainer. Its LPT packer already takes a bin count
  (`lpt(classes, n)`), so extending it means parameterising the shard count and the reporting, not
  writing a new packer. Do that first: reading its current two-way number as a four-way optimum
  gives the wrong partition shape.
- **Keep the safe-direction shape.** Whatever the count, one shard stays defined by subtraction so
  a new class can never belong to no shard. Four *balanced* bins is the chaos suite's shape and was
  rejected for this lane; `bin/ci-integration-test.sh`'s header owns the reasoning.
- **Do not raise `forkCount` inside a shard to compensate.** Measured harmful at 6; the script
  header and the 2026-09-03 plan document carry the numbers.
