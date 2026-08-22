# Is the shard key required under UNORDERED? And is indexing around it the wrong fix?

<!-- inflight-type: feature -->
<!-- inflight-impact: architecture -->

**Antony's question, 2026-08-22, asked while a fix for the symptom was being built.**

## The shard enforces nothing under UNORDERED

Read from the code rather than assumed:

- `ShardKey.of` maps `KEY -> ofKey(rec)`, and **`PARTITION, UNORDERED -> ofTopicPartition(rec)`**.
- `ShardManager`'s entire surface to `WorkManager` is selection and counting - `getWorkIfAvailable`,
  the awaiting-selection and retry counts, `getLowestRetryTime`, `onFailure`, `onAbandoned`. **Offsets
  are `PartitionStateManager`'s job, not the shards'.**
- Under `UNORDERED`, `isOrderRestricted()` is false, so the `break` that enforces one-record-at-a-time
  never fires.

**So under `UNORDERED` a shard is a container keyed by topic-partition that enforces nothing.** The
sharding is vestigial: it exists for `KEY` and `PARTITION`, and `UNORDERED` inherits it because it
happens to reuse the same key function.

## Which makes the measured fix an index around a structure that should not hold those records

`ShardOccupancy` (branch `perf/direct-pull-scan-collapse`) indexes the offsets no worker is holding,
so the unordered scan walks that instead of the raw prefix. **It works and it is measured** - entries
examined per record dispatched at 5,000 in flight, single scanner, went from **440.13 to 1.00**, and
the same test showed the collapse is the walk rather than claim contention (adding scanners at fixed
depth barely moved it).

**But it indexes around a problem instead of removing it.** A record leaves a shard on *success*, not
on being taken, so the shard accumulates every in-flight record and the scan must step over them.
Under `UNORDERED` there is no reason for those records to be there at all - nothing is ordering them.

**Answer by subtraction**: if `UNORDERED` held *available* work in one structure, records leaving when
taken and returning when they fail, there would be no prefix to walk and no index to maintain. That
is the same test this repository applied to the other proposals in this family - a design that ADDS a
structure has to justify itself against one that removes a need.

## Antony's cursor idea, which is lighter than an index and probably sufficient

> "Can't we cache what the first claimable record is? A thread takes the cached one and, as we knew
> the cached entry's place in the queue via an index of some sort, the scan for the next one is
> significantly shorter."

**Yes, and the arithmetic is better than "shorter" - it is amortised O(1).** The cursor holds the
lowest unheld offset. A worker takes it; the entry it just took is now held; the next unheld entry is
almost always the very next one, because workers take *from* the cursor and the cursor advances with
them. The block of held records grows at the cursor rather than in front of it.

Cost: **one atomic long per shard**, against a concurrent set of offsets. Materially cheaper to
maintain and to read.

**The one case that needs care, and it is the same one every proposal here trips on:** a record that
fails or is abandoned becomes available again *behind* the cursor. The cursor must therefore move
backwards - a compare-and-set to the minimum - or that record is stranded until something else moves
it. An index handles re-entry naturally; a cursor has to be told. That is the honest trade, and it is
small.

## What to do with this

**Do not undo the measured fix.** It is evidence-backed, it removes three quarters of the collapse,
and the collapse is real today.

**But record the question against it**, because the sequence matters: if `UNORDERED` stops keeping
in-flight records in a shard, `ShardOccupancy` becomes dead weight rather than a win, and the cursor
would too. Anything built here should be cheap enough to delete.

Ranked, cheapest first:

1. **Per-worker scan cursor** instead of `ShardManager`'s single shared `iterationResumePoint`
   (line 105, one volatile field - every worker starts at the same shard by construction).
2. **The first-claimable cursor above**, per shard, with CAS-to-minimum on release.
3. **Remove in-flight records from the unordered selection structure entirely**, which makes 1 and 2
   unnecessary rather than faster.

See also: [`next-selectable-shard-queue.md`](next-selectable-shard-queue.md),
[`next-work-manager-as-a-thread.md`](next-work-manager-as-a-thread.md),
[`perf-unordered-dispatch-rescans-the-inflight-prefix.md`](perf-unordered-dispatch-rescans-the-inflight-prefix.md).
