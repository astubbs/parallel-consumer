# `ProcessingShard`'s available-work counter undercounts after a stale replacement, and the deficit accumulates

<!-- inflight-type: bug -->
<!-- inflight-impact: throughput -->
<!-- inflight-state: deferred - a gauge inaccuracy that loses no records; needs a decision on whether the counter is worth keeping at all -->

`ProcessingShard.addWorkContainer`'s stale-replacement branch puts the fresh container into `entries`
without touching `availableWorkContainerCnt` - correct for a replacement, except that both routes into
that branch have **already spent this offset's decrement**: either the stale entry was taken as work
(`getWorkIfAvailable` decremented at take time), or the poller's `removeStaleContainers` sweep removed
it between the `get` and the `put`.

**The deficit is not self-healing.** The next ordinary add increments for its own new entry, so it
survives that; it resyncs only when the shard drains far enough for
`dcrAvailableWorkContainerCntByDelta`'s clamp to floor the counter at zero, or the shard is removed.

**No record is lost, which is why this is deferred rather than fixed.** It errs towards fetching
sooner rather than starving: `getWorkIfAvailable` scans `entries` directly instead of gating on the
count, and `handleFutureResult` drops a stale in-flight result without touching the shard, so it
cannot remove the fresh entry.

## Before fixing it, ask whether the counter should exist

The reasoning above is already in the method's own comment - what is *not* settled is the counter's
purpose. It is a cache of `entries`' size-minus-taken that nothing reads for correctness, so the
candidate fixes are not just "re-increment here": derive it, or delete it and let the callers ask
`entries`. A re-increment closes today's instance of the defect class without answering that.

Surfaced while building the confluentinc#909 reproduction (`RegistrationRaceStaleResidentIT`), which
drives the stale-replacement path on purpose - so that test is the cheapest place to observe the
drift.
Related: [`bug-857-family.md`](bug-857-family.md) is a different failure - a Class 2 stall, not a
gauge that reads low.
