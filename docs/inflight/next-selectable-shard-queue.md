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

## The merry-go-round, which removes the mode branch entirely

**Antony, 2026-08-22: "in unordered the shards simply play merry go round - a shard taken in
unordered immediately goes to the back of the queue."** That is better than the two-kinds-of-entry
scheme this note originally proposed, because it collapses into **one rule with no mode branch**:

> Take the head shard. Take one record from it. **If the shard is still selectable, put it at the
> tail.** Otherwise it re-enters when it becomes selectable again.

- **Ordered** - after the take its in-flight count is 1, so it is not selectable: not re-enqueued, and
  it re-enters on completion.
- **`UNORDERED`** - never blocked, so still selectable: straight to the tail.

Same line of code. `isOrderRestricted()` drops out of the decision because the selectability
predicate already encodes it, and a single-partition `UNORDERED` topic degenerates to one shard
cycling head-to-tail - correct, rather than the serialisation the exclusive scheme would have caused.
It also gives round-robin across partitions for free.

**The queue head becomes the contention point**, since `UNORDERED` does two queue operations per
record on a structure that may hold one element. That is a CAS on a head pointer instead of a walk
past thousands of in-flight entries, so it should be orders of magnitude cheaper - but it is a single
point and has to be measured, not assumed. It is also exactly what the 2022 per-thread variant
(`5dcd39bb3`, `Map<Long, BlockingQueue<ProcessingShard>>`) was trying to avoid, which is worth
reading before concluding a single queue is fine.

## Ring buffer instead of a queue - and the cheaper thing it points at

**Antony, 2026-08-22: "instead of a queue of the shards what about a ring buffer? No head
contention."**

**Partly.** A ring buffer is genuinely better than a linked queue on the axes that matter here -
pre-allocated slots so there is no node allocation per take, contiguous memory so the scan is
prefetch-friendly, and claiming a slot is a single `getAndIncrement` rather than a CAS retry loop
against a head pointer. Under heavy contention that difference is large.

**But it is not "no contention".** A shared sequence counter still ping-pongs one cache line across
every core that touches it, and at 5,000 workers that line is the hottest address in the process. It
is *cheaper* contention, not absent contention.

**Where it fits and where it does not**, and the split is the opposite of the shard queue's:

| Mode | Shard membership | Ring buffer? |
|---|---|---|
| `UNORDERED` | **Static** - one shard per assigned partition, always selectable | **Excellent.** The ring never needs republishing: `getAndIncrement() % shardCount` **is** the merry-go-round. No queue, no re-enqueue, no allocation |
| `KEY` / `PARTITION` | **Dynamic** - shards leave when a record is out and return when it completes | **Poor.** A fixed ring cannot express "not currently selectable" without a tombstone, and skipping tombstones is scanning again |

So a ring helps exactly the mode the shard queue does not, and vice versa. **Together they cover
both** - which is a reason to treat them as complementary rather than as competing proposals.

### The cheaper idea underneath it: a PER-WORKER cursor, with no shared state at all

If the only thing the shared sequence buys is "workers start in different places", then **give each
worker its own cursor** and drop the shared atomic entirely. Collisions become possible but rare, and
under `UNORDERED` a collision is already safe and already handled - the claim decides, and a loser
skips.

**This is not hypothetical, and the current code is the worst case of it.** `ShardManager` line 105:

```java
private volatile Optional<ShardKey> iterationResumePoint = Optional.empty();
```

**One shared field.** `LoopingResumingIterator` resumes from it, so under direct pull every one of N
workers begins its scan at the same shard. They are pointed at the same place by construction, which
guarantees the maximum possible collision rate rather than merely permitting it.

**Making that resume point per-worker is a very small change with no new data structure**, and it
should be measured before anything larger is built here - if most of the direct-pull collapse is N
workers starting at the same shard, a ring buffer and a shard queue are both solving a problem that a
`ThreadLocal` would have removed. It would not fix the prefix walk below it, which remains the other
half.

## THE REMAINING GAP: which RECORD within the shard

**A shard queue answers "which shard", not "which record".** Under `UNORDERED` one shard holds every
in-flight record for its partition, so having taken the shard, a worker still has to find the next
*available* record inside it - walking past everything already out. **That is the quadratic prefix
walk, untouched.**

So on its own this proposal fixes the ordered modes and leaves `UNORDERED`'s real cost exactly where
it was.

**The fix is the same idea one level down**: the shard keeps its *available* records in a queue, not
just in an offset-ordered map. Then taking is O(1) at both levels and **nothing scans anywhere**.

That combination is what
[`next-pre-rendered-work-order-list.md`](next-pre-rendered-work-order-list.md) was reaching for, but
decomposed into two small structures instead of one global one - which is materially better, because
each is maintained locally by the thing that already owns the fact:

| Level | Structure | Maintained when |
|---|---|---|
| Which shard | queue of selectable shards | a shard's in-flight count reaches or leaves zero |
| Which record | per-shard queue of available records | a record is taken, returned, or its retry delay expires |

The offset-ordered map does not go away - it is still needed for stale sweeps and for offset lookups -
so this **is** an added structure per shard, and it has to answer the same charge as every other
proposal here: it is a second view of the same records, and this session has already paid twice for
that class. The mitigating argument is that it is per-shard and local, so the sync is one insertion
and one removal by the object that owns both views, rather than a global rendering.

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
