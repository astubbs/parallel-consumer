# The retry-queue purge scans the whole queue every control-loop tick, and nobody has measured it

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->
<!-- inflight-labels: concurrency -->
<!-- inflight-state: deferred - meter the scan first; the gate is not worth adding on an unmeasured cost -->

`ShardManager.purgeDepartedRetryEntries()` walks the **entire** retry queue once per control-loop
pass and does a shard lookup plus a reference comparison per entry. It is the first unconditional
full scan of that structure on the control loop: every other reader there early-stops on the
`retryDueAt` sort order - `RetryQueue.getNumberOfFailedWorkReadyToBeRetried` breaks at the first
not-ready entry, `ShardManager.getLowestRetryTime` returns at the first not-in-flight one - and
`ProcessingShard`'s `removeAll` is bounded by the batch it just took. Shard residency has no
relationship to that sort order, so the purge **cannot** early-stop, and on the overwhelming majority
of ticks it finds nothing.

**Two independent reviewers found this, which is why it is written down rather than assumed cheap.**
Neither found a correctness problem. Both landed on the same fix and one of them sharpened it.

## The shape of the fix, if it is ever taken

**A monotonic counter incremented on departure, read plainly by the controller** - a `LongAdder` in
the shape `RecordPopulation` already uses at those same call sites. Not a boolean dirty flag: a
counter is the same cost class, and it keeps the one-tick bound trivially, because a departure
landing between the read and the scan costs one extra tick and can never lose an entry.

## Why it was NOT taken with the purge

- **It puts poll-thread-written state back on the rebalance path**, in the change whose whole point
  was taking the poll thread off that structure. Every departure is created by a rebalance callback,
  so any such signal is unavoidably cross-thread - on a class whose entire difficulty is cross-thread
  state, and under the `@GuardedBy`/`@ThreadConfined` rules in
  `parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md`.
- **The cost is unmeasured.** It is bounded by retry-queue size, which is bounded by the in-flight
  target, so this is a fixed tax rather than a blow-up - of unknown size. `DispatchScanMeter` is this
  very class's precedent for metering exactly this kind of scan, and
  `ShardManager.purgeDepartedRetryEntries`'s javadoc says a meter belongs inside it rather than at its
  one call site.

**So the order is: meter, then decide.** Adopting the counter without the measurement would trade a
known-safe design for an unknown gain.

The design it belongs to, and the alternative it was weighed against, are in
[`../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md`](../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md)
under "Rejected alternatives" - that write-up owns the reasoning; what is here is that the decision is
open and what would settle it.
