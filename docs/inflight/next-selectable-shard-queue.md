# A queue of SELECTABLE SHARDS, so taking work never scans

<!-- inflight-type: feature -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

**Antony's proposal, 2026-08-22.** Under `KEY` ordering, a shard with a record in flight cannot yield
another one - so no thread should ever look at it. Keep a **queue of shards that are selectable**,
maintained as work is taken and returned. A worker takes the head. **Nothing scans anything.**

This is a smaller idea than [`next-pre-rendered-work-order-list.md`](next-pre-rendered-work-order-list.md)
and probably strictly better: one entry per SHARD rather than per record, and the order *within* a
shard is already maintained by its `ConcurrentSkipListMap`, so there is nothing to re-render.

## Why it fixes both problems at once, which no other proposal does

The direct-pull collapse has two compounding causes (see the contention investigation):

1. **Volume** - `core` walks the in-flight prefix once per pass on one thread; direct pull walks it
   once per worker. At 5,000 in flight and 5,000 workers that is up to 25 million examinations to
   hand out 5,000 records.
2. **Claim contention** - workers that scan the same shard mostly lose the CAS and retry.

**A shard queue removes both by construction.** Taking is O(1), so there is no scan to multiply. And
taking a shard off the head REMOVES it, so two workers cannot land on the same shard at all - the
claim stops being a contention point rather than being tuned into one that contends less.

It also subsumes the busy-shard in-flight count (commit `b73f8b97e`): that count made "is this shard
selectable" an O(1) *check*; this makes it something nobody has to ask, because an unselectable shard
is not in the queue. **The count becomes the predicate that drives enqueue and dequeue** rather than
something consulted on every pass.

## THE TRAP: UNORDERED must not be exclusive

**A naive implementation serialises `UNORDERED` to one worker.** Under `UNORDERED` there is one shard
per topic-partition holding every in-flight record for it. If taking a shard removes it from the
queue, then a single-partition `UNORDERED` topic has exactly one shard, one worker holds it, and
every other worker waits - turning the mode with the most available parallelism into a serial one.

So the queue has two kinds of entry, and the distinction already exists in the code as
`isOrderRestricted()`:

| Shard kind | On take | Why |
|---|---|---|
| Ordered (`KEY`, `PARTITION`) | **Removed**, re-enqueued when its in-flight count returns to zero | Only one record may be out at a time, so the shard is genuinely unavailable |
| `UNORDERED` | **Stays**, many workers may hold it simultaneously | Never blocked; concurrent takers are already safe - the map is lock-free and the claim is a CAS |

`UNORDERED` therefore keeps its contention and its prefix walk, and this proposal does nothing for it.
That is worth stating plainly rather than discovering later: **this is a fix for the ordered modes.**
`UNORDERED`'s waste is a separate question - see
[`next-direct-pull-unordered-selection.md`](next-direct-pull-unordered-selection.md).

## The transitions, which are the whole design

- **Record taken** from an ordered shard - the shard is already off the head, nothing to do.
- **Record completes, fails, or is abandoned** - if the shard's in-flight count is now zero and it has
  remaining eligible work, enqueue it.
- **New work registered** - enqueue the shard if it is selectable and not already queued.
- **Retry delay expires** - a failed record makes its shard eligible at a *future time*, with no event
  to hang the enqueue on. `RetryQueue` already tracks exactly this and is the natural trigger.
- **Shard emptied and removed** - it must not be left in the queue as a stale entry.

**Guard against double-enqueue with a flag on the shard**, not a `contains()` scan - an
`AtomicBoolean` compare-and-set on the shard object is O(1) and is the only correct way to make
"enqueue if not already queued" atomic under concurrent returns.

## What has to be measured, and the prior result that does NOT settle it

`parked-2022-central-queue-rework.md` records a central-queue attempt as "1/3 as fast". **That does
not apply here and must not be cited as though it does**: it was a queue of RECORDS, taken with
`centralQueue.take()`, and it measured contention on a structure every worker hit for every record.
A queue of shards is a different structure - one entry per key-group, not per record - and under
`KEY` with many distinct keys there are many shards, so takers spread across them rather than
queueing behind one head.

That said, **the head IS a single point every taker touches**, so it has to be measured, not reasoned
about. Prototype the taking path alone, no Kafka, in the shape of `bench/threads/ThreadCeiling.java`:
N takers against a shard queue, and see where it bends.

**Fairness changes, and probably for the better.** Re-enqueueing at the tail gives round-robin across
keys; today's `LoopingResumingIterator` walks in shard-map order, which is an accident of iteration
rather than a policy. That is a behaviour change worth stating in a release note if it lands.

See also: [`next-pre-rendered-work-order-list.md`](next-pre-rendered-work-order-list.md),
[`next-starvation-is-the-signal-not-queue-depth.md`](next-starvation-is-the-signal-not-queue-depth.md),
[`perf-unordered-dispatch-rescans-the-inflight-prefix.md`](perf-unordered-dispatch-rescans-the-inflight-prefix.md).
