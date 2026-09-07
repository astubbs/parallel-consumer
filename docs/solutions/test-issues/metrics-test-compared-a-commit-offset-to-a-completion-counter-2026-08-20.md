---
title: A metrics test compared a contiguous commit offset to an out-of-order completion counter, and read as a confluentinc#857 stall
date: 2026-08-20
category: test-issues
module: parallel-consumer-core
problem_type: assertion_assumes_ordering_the_mode_does_not_provide
component: testing
symptoms:
  - "`expected: 1214.0 but was: 1207.0 within 2 minutes` - the shortfall is always a handful of records, never all of them"
  - "`expected: 205.0 but was: 203.0 within 2 minutes` on a different run, same shape, different size"
  - "Passes in isolation, fails under load - because load is what makes completions arrive out of offset order"
  - "The `atMost` budget is burnt in full (120-140s) on every failure, because the gap is permanent rather than slow"
  - "Reads as a stalled or lost record, which is the confluentinc#857 revoke-path signature"
root_cause: assertion_compared_partition_latest_committed_offset_to_a_shared_completion_counter_under_UNORDERED
resolution_type: rewrote_the_test_harness_to_freeze_on_offsets_instead_of_on_a_counter
severity: medium
tags:
  - unordered
  - metrics
  - offset-semantics
  - misattribution
  - permanent-gap
  - quarantine
---

# A metrics test compared a contiguous commit offset to an out-of-order completion counter, and read as a confluentinc#857 stall

## Problem

`PCMetricsTest.metricsRegisterBinding` froze a run part-way through by latching every worker past a
count, then asserted the partition offset metrics against that count:

```java
assertThat(registeredGaugeValueFor(PCMetricsDef.PARTITION_LAST_COMMITTED_OFFSET, 1))
        .isEqualTo(counterP1.get() + p1StartingOffset);
```

`counterP1` counts **completions**. `PARTITION_LAST_COMMITTED_OFFSET` is
`getOffsetHighestSequentialSucceeded() + 1` - the offset one past the **lowest incomplete** one,
because a Kafka commit is a single contiguous watermark and cannot express a hole. The suite runs
`UNORDERED` (`AbstractParallelEoSStreamProcessorTestBase`, `.ordering(UNORDERED)`), so the two are
equal only when completions happen to arrive in offset order.

Worse, the harness guaranteed they sometimes would not. The gate was

```java
if (counter.get() >= numberToBlockAt.get()) { latch.await(); } else { Thread.sleep(5); }
counter.incrementAndGet();
```

Sixteen workers evaluate `counter.get()` concurrently while holding different offsets. Whichever
ones read a value below the threshold complete; whichever read one above latch **before**
incrementing, so their offsets never complete at all. When a latched worker holds a lower offset
than a completed one, the commit watermark stops below the completion count and **stays** there -
the gap is permanent, not slow. The 120-second `atMost` could never close it; it only made each
failure cost 140 seconds of CI.

## Symptoms

- `expected: 1214.0 but was: 1207.0 within 2 minutes` - 214 completions against 207 contiguous.
- `expected: 205.0 but was: 203.0 within 2 minutes` on an earlier sighting: same shape, gap of 2.
- Passes run after run in isolation, fails under a loaded box - because load is what widens the
  window in which workers race across the threshold with offsets out of order.
- **It reads as a lost or stalled record**, which is why it was logged as a confluentinc#857
  (revoke-path) sighting. The discriminators say otherwise: every real member of that family is a
  chaos or rebalance *integration* test with a broker and a chaos seed, and this is a MockConsumer
  unit test with no broker, no rebalance and no revoke path.

## Root cause

The assertion asserted a property `UNORDERED` does not provide. `LAST_COMMITTED_OFFSET`,
`HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET`, `HIGHEST_COMPLETED_OFFSET` and `INCOMPLETE_OFFSETS` exist
precisely *because* the completed set can have holes; a completion count collapses all four into one
number and can only be right when there are none.

The same confusion ran through the sibling assertions, which all derived from `counterP0.get() - 1`
as though the completed set were the prefix `[0, count)`.

## Solution

**Freeze on offsets, not on a counter** - and deliberately leave a hole.

The user function now decides by the record's own offset relative to its partition's first offset, so
the completed set has a fixed *shape* rather than one settled by a race. Shards hand work out in
ascending offset order (`ProcessingShard`'s `entries` is a `ConcurrentSkipListMap`, iterated by
`getWorkIfAvailable`), so latching two ranges open gives each partition a prefix with one known hole in
it:

- a two-offset **gap** at 50, so completions are provably non-contiguous;
- everything from 250 upward, which is what eventually parks the whole worker pool.

The four offset metrics then take four *different* values, and two of them are pinned to constants no
completion count can reach: highest sequential succeeded 49, last committed 50, highest completed
`count + 1`, incomplete `quantity - count`. Each assertion can only pass for the right reason, where
the old ones asserted the same number four times.

**What is deliberately not asserted is where the run stops.** Both partitions share one worker pool, so
whichever reaches the freeze offset first parks the pool and can leave the other tens of records short -
measured across 30 runs, the leading partition always stops at 248 and the lagging one anywhere from
161 to 248. That is why the gap sits at 50 rather than near the freeze point: the shape only holds for a
partition that got past the gap, and the margin has to cover the worst lag. An intermediate version of
this fix pinned the count at 248 and failed on its second run at 222.

**Proof it can still fail** - three deliberate breakages, each red on demand:

| Breakage | Result |
|---|---|
| New harness, OLD comparand (`LAST_COMMITTED == completion count`) | RED, `expected: 221.0 but was: 100.0` |
| `PartitionState.getOffsetToCommit()` returning `getOffsetHighestSucceeded() + 1` | RED, `expected: 100.0 but was: 248.0` |
| `PARTITION_HIGHEST_COMPLETED_OFFSET` gauge rebound to `getOffsetHighestSequentialSucceeded` | RED, `expected: 216.0 but was: 99.0` |

The second is the one worth noting: committing the highest *succeeded* offset rather than the highest
*sequential* succeeded one skips a hole and loses records. The old test could not have caught it, because
its expectation was a completion count that both versions satisfy.

## Prevention

- **A test that freezes a concurrent run must freeze it on something the workers do not race on.** A
  shared counter read before the work is the classic wrong choice: the value that decides is not the
  value that ends up recorded.
- **When a metric names an ordering property, do not compare it to a count.** `contiguous`,
  `sequential`, `highest`, `lowest incomplete` are all different numbers the moment anything runs out of
  order - which under `UNORDERED` is always. Where they must differ, build a case where they do, or the
  assertions cannot tell which metric they are checking.
- **Parking workers to freeze a run costs threads from a shared pool**, so a per-partition freeze offset
  does not stop both partitions at the same place. Derive expectations from the *shape* of what
  completed, never from where it happened to stop.
- **A shortfall under load is not automatically a stall.** Before filing one against a revoke-path
  family, check the family's discriminators; this sighting had none of them.
