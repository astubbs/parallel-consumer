# What is left of the pump's failure-settle wait, after the measurement that reshaped it

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

`parallel-consumer-streams` (astubbs#255). The patched `StreamTask.pcProcess` waits, bounded, for the
outcome of work already in flight when a pump hands out nothing - which is what makes a worker's
failure reach Kafka's recovery from the same `runOnce` that dispatched the record.

**The first version of that wait was unconditional, and it cost sixteen-fold intake throughput.** With
the pause switched off so that nothing else bounded inflow, peak occupancy over a 600-record backlog
measured 36 with the wait and 596 without it - one term changed, same broker, topology, data and
machine. The wait was firing in the *saturated* case, where a pump consumes nothing because the pool
is full rather than because there is nothing to do, and the StreamThread's next act would have been to
poll. Worse than the throughput: it silently supplied a second memory bound, which made the
memory-bound proof's own control arm look almost bounded.

It is now gated on whether the pump stopped for want of work or for want of a pool slot
(`PcTaskDispatcher.awaitOutcomeIfIdle`), and the control arm is back where it belongs. Two things
remain open.

## The residual the gate creates

At `poolSize` 1 a single in-flight record fills the pool, so the gate declines to wait and a failure is
delivered on the next pump rather than this one. **The same-`runOnce` guarantee is therefore a
guarantee about a pool with a spare slot** - every default configuration and every arm this module
measures, but not every configuration a user could choose. Closing it needs a discriminator finer than
"was there a free slot": something like "did the WorkManager have anything selectable", which PC does
not currently expose per shard in a form that is honest under KEY ordering.

## The cost in the idle regime is still not measured

What the sixteen-fold figure measures is the saturated case, which the gate now excludes. In the
*starved* regime - low arrival rate, pool not full, PC's shards empty - the gated wait still fires,
and the StreamThread waits for a completion instead of returning to `poll()` to pick up records that
would have been dispatchable. The cost there is per-record latency, bounded by one record's processing
time or by the settle budget, whichever is smaller. Nothing measures it.

An arm on the benchmark suite that varies exactly the settle budget (zero against its default), on a
fixture with a low arrival rate rather than a backlog, would settle it. That suite is a separate rung
of this workstream, which is why this is recorded rather than done here - and why the budget is a
named constant on the dispatcher rather than a literal at its call site.

## Delete when

The benchmark suite carries a settle-budget arm on a low-arrival-rate fixture and its result is
recorded, and the `poolSize` 1 residual is either closed or accepted in writing.
