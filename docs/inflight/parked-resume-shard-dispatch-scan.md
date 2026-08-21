# Parked: two attempts at the shard dispatch scan, both measured, both rejected

<!-- inflight-type: parked -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

Parked 2026-08-21, **because it was measured and does nothing** - not because it was abandoned
half-finished. Branch `perf/resume-shard-scan`, reverted from `research/market-analysis-recut`.

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

**Verdict: not proposed for merge.** Zero measured benefit does not buy a change to how ordering is
enforced. The branch is committed so the work and the evidence survive.

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
