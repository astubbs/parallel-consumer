# Parked: two attempts at the shard dispatch scan, both measured, both rejected

<!-- inflight-type: parked -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

Parked 2026-08-21, **because it was measured and does nothing** - not because it was abandoned
half-finished. Branch `perf/resume-shard-scan`, reverted from `integration/v6`.

## What it does

`ProcessingShard.getWorkIfAvailable` opens a fresh iterator at the head of the shard's
`ConcurrentSkipListMap` on every dispatch pass. A record stays in that map until it **succeeds**, not
until it is dispatched, and `isOrderRestricted()` is false for `UNORDERED` so the scan does not stop
early - so every pass walks past every in-flight container before reaching selectable work. That is
O(in-flight) per pass over a skip list, and it grows exactly as concurrency does.

The branch makes the scan resume from where the last one stopped, positioned through
`NavigableMap.tailMap` in O(log n), and wrap over the skipped range. `KEY` and `PARTITION` are
untouched: for those the lowest offset *is* the next record, so there is nothing to resume from.

## Why it is parked: no measurable effect

Like-for-like, 500,000 records, real broker, three runs each:

| 100ms / 1,000 concurrent | Runs | Mean |
|---|---|---:|
| Unmodified | 8,841 / 8,791 / 8,770 | **8,800** |
| With the branch | 8,807 / 8,784 / 8,868 | **8,819** |

**+0.2%.** Unchanged at 100ms/5,000 and 2ms/1,000 too. The rescan is real in the source; it is not
costing anything measurable at any operating point tested.

**It was written on a false premise.** The trigger was a claimed 21% `UNORDERED`-versus-`KEY` deficit,
which turned out to be a 100,000-record run compared against a 500,000-record one. At equal record
counts the two modes are 0.9% apart. Full retraction in
[`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md).

## The second attempt: split the shard's state - better in every way except the one that counts

**Branch `perf/split-shard-inflight`.** Proposed by the owner after the resumer was reverted, on the
reading that the resumer navigates *around* in-flight records when the better move is to take them out
of the path. **That reading was right.**

`entries` holds only selectable work; a second map holds what is out for processing. **A record is in
exactly one of them**, so it is a move rather than a second copy - not the parallel-state shape that
accumulates sync cascades, because there is nothing that can disagree with itself. Dispatch takes from
the head and there is nothing in the way. A failed record goes back into `entries` at its offset,
which restores offset order for free.

**The resume point, the wrap and the starvation guard do not exist in this design.** The previous
attempt needed all three and got two of them wrong.

**Dispatch cost, 20,000 records taken in batches and never completed:**

| | `KEY` | `UNORDERED` |
|---|---:|---:|
| Single map | 43ms | **97ms** |
| Split state | 43ms | **10ms** |

**About ten times cheaper**, turning `UNORDERED` from 2.3x worse than `KEY` into 4x better.

### What it cost to learn: the walk is load-bearing

The first run failed **ten tests**, all one cause:

```java
works = wm.getWorkIfAvailable(max);
assertOffsets(works, of());   // should be blocked by in flight
```

**In-flight records staying in the shard is how ordering is enforced.** The scan reaches the in-flight
container at the head and breaks - *that is the block*. Move them out and it silently disappears:
records dispatched out of order, no error, no warning, compiles clean.

**So the "wasteful walk past in-flight records" is not overhead the scan drags around. It is the
mechanism, doing double duty.** That reframes the original observation completely and is the single
most useful thing either attempt produced.

Repairable by stating the block instead of discovering it - `isOrderRestricted()` plus a non-empty
in-flight map returns nothing, O(1) and arguably clearer. **All 366 core tests then pass.** But it
swaps the enforcement mechanism for the library's most important guarantee, which is not a free change
however green the suite is.

### And it changes nothing end to end

Three runs each at a 100ms handler, 500,000 records, real broker:

| | Baseline | Split state |
|---|---:|---:|
| 1,000 concurrent | 8,800 | **8,760** (-0.5%) |
| 5,000 concurrent | 19,566 | **19,755** (+1.0%) |

In-flight still plateaus around 2,750. **A tenfold dispatch win is completely invisible, because
dispatch is not where the time goes** - the same conclusion the four-arm comparison reached, arrived at
from the opposite direction and therefore worth twice as much.

### The zero and 2ms cases too - and they explain the whole thing

**The 100ms result alone was not enough**, because a 100ms handler is where an engine win is most
easily hidden. At a near-free handler, dispatch is the largest fraction of per-record time it will ever
be, so that is where a tenfold dispatch win should show. Three runs each:

| Delay / concurrency | Baseline | Split state | |
|---|---:|---:|---|
| 0ms / 1,000 | 109,709 | 105,832 | **-3.5%** |
| 2ms / 1,000 | 66,624 | 65,919 | -1.1% |
| 0ms / 5,000 | 101,370 | 101,452 | +0.1% |
| 2ms / 5,000 | 30,041 | 29,470 | -1.9% |

**Flat to slightly worse, everywhere.** Not one operating point rewards it.

**And the reconciliation matters more than the verdict.** The dispatch measurement said 97ms -> 10ms,
which is roughly 4.9µs per record down to 0.5µs. At 0ms the engine does ~100,000 records/second, so
~10µs per record - meaning the walk should have been half the budget and its removal should have been
enormous. It was nothing. The reason:

**The walk is O(in-flight), and in-flight is small at every delay that matters.** The unit measurement
lets in-flight grow to 20,000 because nothing ever completes. The real runs never get near that - peak
in-flight was **83 to 780** at 0ms, and even at the very top end, 100ms with 5,000 concurrent, it
plateaus around **2,750** because the Java client cannot feed it faster. At those depths the walk is
short and its cost is immaterial.

**So the scan is quadratic in a quantity that is bounded well below where it would hurt**, and the
bound is imposed by something entirely outside PC. That is why all three attempts return zero, and it
is a stronger result than any of them: **the cost cannot become significant until the client stops
being the limit.**

**A caveat this exposes about our own guard test.** `OrderingModeDispatchParityTest` measures with
20,000 records in flight - a condition no real run reaches. It is a **shape** guard, deliberately
extreme so a superlinear regression shows up at all, and it must not be read as a workload. A ratio
moving there says the algorithm changed; it does not say throughput changed.

**Verdict: not proposed for merge.** Zero measured benefit does not buy a change to how ordering is
enforced. The branch is committed so the work and the evidence survive.

## The third design, not built: an index over the single collection

**The owner's proposal after the split broke ordering, and it is the best of the three.** Keep one
ordered collection as the base, so every record stays in offset order and the ordering block is
**completely untouched** - `KEY` and `PARTITION` walk to the head and break exactly as they do today.
Add an index of what is *not* in flight, and let only the `UNORDERED` dispatch path consult it.

**It is strictly safer than the split.** The split *removed* records from the scanned map, which is
what silently broke ordering; an index never touches the base, so the enforcement mechanism cannot be
affected. The ten failures that design cost simply cannot occur.

**Not built, because the answer is already known.** Three separate mechanisms have now been measured
against the same question and the low-delay runs above explain why they must all return zero: the walk
is O(in-flight), and in-flight is bounded well below where it hurts by the Kafka client. A third
mechanism removes the same cost the other two removed.

### Built after all, 2026-08-22 - and the "already known" answer was known about the wrong engine

`ShardOccupancy` is this design: one ordered collection as the base, untouched, plus an index of what
is not in flight that **only the `UNORDERED` dispatch path consults**. Both cautions above were
correct and both were designed for - the index means "not in flight" rather than "takeable now", and it
sits alongside `availableWorkContainerCnt` rather than replacing it, because that counter is read on
the broker-poll hot path and a set's `size()` is O(n). **Collapsing the two is still the real design
work and is still not done.**

**What changed is not the reasoning, it is which engine the reasoning was about.** Everything in this
note about the shipped engine still holds: the walk is O(in-flight), in-flight is bounded by the Kafka
client well below where it hurts, and three mechanisms all returned zero end to end. The direct-pull
engine breaks the bound's premise - it pays the walk once per record on every worker instead of once
per batch on one thread - and there the same walk costs **440 examinations per record dispatched at
5,000 in flight, measured with a single scanner so that claim contention is not in the answer.** With
the index it is 1.00.

The restart conditions listed below were therefore met by the second one: *an operating point where
in-flight per shard is far larger than anything tested*. Measurement, control arm and the open
end-to-end question:
[`perf-direct-pull-collapse-is-the-scan.md`](perf-direct-pull-collapse-is-the-scan.md).

**Two things to know if it is ever built:**

- **The index means "not in flight", not "takeable now".** A record whose retry delay has not elapsed
  is still in it and still gets walked past. The win is large, not total.
- **It would want to replace `availableWorkContainerCnt`, not join it.** That counter drifts - there is
  a clamp in `dcrAvailableWorkContainerCntByDelta` resetting it to zero "in case of possible race
  condition", which is the parallel-state symptom exactly. But the counter is read on the broker-poll
  hot path and `ConcurrentSkipListSet.size()` is O(n), so a naive index sits *alongside* it and makes
  three pieces of state where there are two. **Solving that is the actual design work**, and it is
  worth more than the dispatch win: a counter that needs a clamp is a bug waiting to be found.

## What would have to be true to restart it

Do **not** restart either of these from reading the source again - the source argument is what
produced both, twice, and the source argument is correct and irrelevant. Restart only if a measurement
points here:

- A profiler over a real operating point attributes meaningful time to the shard scan.
- An operating point is found where in-flight per shard is far larger than anything tested - this
  workload put roughly 275 in flight per shard at the top end, and the cost is linear in that.
- Batching lands, which changes how much is taken per pass and therefore how often the walk happens.

## What was kept, and why

`UnorderedShardScanResumeTest` stays on the branch it was merged with and **passes against the
unmodified code**. It pins a property nothing else covered: a record that becomes selectable again
*behind* the dispatch scan must still be offered while the shard is continuously fed.

It is worth keeping because it is hard to get right. The first version passed with the fix deleted -
it failed one record and looked for it, which lets an idle tail reset the scan position and hide the
bug. Keeping the shard fed is what makes it load-bearing, and if anyone reattempts this optimisation
that test is the one that will catch the obvious wrong version.

## Correct base for any retry

Branch from **`origin/master`**, which already carries the `bz.stub` package rename (astubbs#294,
merged). A local `master` may be far behind it - this branch was cut from `rename/master-packages`
because a stale local ref made it look as though the rename was not on master yet.
