# A pre-rendered work-order list, as an alternative to both direct pull and queue pre-filling

<!-- inflight-type: feature -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

**Antony's proposal, 2026-08-22.** Instead of trying to push the right amount of work into the
`ThreadPoolExecutor`'s queue, keep **one ordered list of the work that should be taken next, across
the entire buffered set** - a linked list for order, with a hash map from record to node so that
removal and re-entry are O(1). Workers take from the head. As work is taken, retried, failed,
abandoned, or newly fetched, the list is kept in sync.

## The problem it is aimed at

**The current design has to guess how much to buffer.** `DynamicLoadFactor` steps a multiplier up and
down trying to keep the pool queue at the right depth; `isPoolQueueLow()` reads the queue to decide.
That machinery is a control loop guessing at a quantity it cannot observe directly, and it is
reactive to neither load nor data shape quickly enough. It is also the thing that blocked virtual
threads, because a virtual-thread executor has no queue to read.

**Both existing answers have a measured problem.**

| Approach | Measured |
|---|---|
| Control loop fills a pool queue (shipped) | The UNORDERED dispatch scan re-walks the whole in-flight prefix every pass - `BATCH x (1+2+...+PASSES)`. See `perf-unordered-dispatch-rescans-the-inflight-prefix.md` |
| Direct pull (workers scan for themselves) | 3.2x at 10 workers; **at 5,000 it is catastrophic** - a sweep on 2026-08-22 drove one-minute load to **890** on twelve cores and every run hit a 60s cap without completing. N pullers all scan the same shard set, mostly lose the claim, and park again |
| Central queue (2022 branch) | Recorded as "1/3 as fast", but that measurement used `centralQueue.take()` and measured **queue contention**, not the idea. See `parked-2022-central-queue-rework.md` - the number does not settle this |

**The list is a central queue without the buffering problem**, which is the point: it is not a
bounded prefetch that something has to size, it *is* the whole eligible set, in order.

## Why it might be much faster

**It moves the cost from read to write, and the ratio favours writes.** Today every dispatch pass
scans; a scan examines many entries to find a few. With a rendered order, a take is O(1) at the head
and there is no scan at all - not for the control loop, and not for N direct-pull workers either. The
maintenance cost is bounded by state changes: roughly one insertion when a record becomes eligible
and one removal when it is taken, so **two writes per delivery** against a scan that examines
hundreds of thousands of entries per hundred thousand records.

It would also make the **busy-shard count redundant in the ordered modes** - a blocked shard's
successors simply are not in the list - and it is the only proposal so far that addresses
`UNORDERED`'s waste, where N workers walk the same in-flight prefix.

## The hard part, which is ordering

Under `KEY` ordering only one record per key may be in flight. So **the list must contain at most one
record per blocked shard**, and the interesting question is not the data structure, it is the
transitions:

- When a record is taken, its shard becomes blocked - its successors must not be reachable from the
  list.
- When it completes, the next record for that key becomes eligible. **Where does it go?** Head is
  unfair to older keys; tail starves a hot key's ordering guarantee. This is a scheduling policy
  decision that the current scan makes implicitly by walking offset order, and it would have to
  become explicit. That is arguably an improvement - it is currently an accident of iteration order -
  but it is a decision, not a detail.
- Retry has a *time* component. A failed record is not eligible until its delay passes, so either the
  list holds a not-yet-eligible entry that takers must skip - which reintroduces scanning - or there
  is a second time-ordered structure feeding it. `RetryQueue` already exists and may be exactly that.

## The risk that has to be stated

**This is a third representation of the same data**: the shards map, the retry queue, and now the
order list. This repository's own recorded rule is to **collapse parallel state when bugs recur**,
because every additional structure holding the same facts is a sync cascade waiting to happen - and
this session has already paid for that class twice (the drift counter, and the check-then-act claim).
A proposal that ADDS a structure has to answer that charge.

**The strongest version of the idea answers it by subtraction, not addition.** If the list contains
exactly the eligible work, then the shards stop being a work store and become only an index of what
blocks what. That is a genuine simplification - one place to look for "what is next", one place to
look for "why is this not next" - rather than a cache in front of the existing design. **If the
design cannot reach that, it is probably not worth doing**, because a fourth structure to keep in
step will cost more in correctness than it wins in throughput.

## How to settle it

1. **Do not trust the 2022 "1/3 as fast" figure.** It measured `centralQueue.take()` contention on a
   different design. Re-measure or discard it.
2. **The contention question is the whole question.** A single list mutated by many takers is exactly
   where a central queue lost before. Prototype the taking path alone, no Kafka, in the shape of
   `bench/threads/ThreadCeiling.java` - N takers against a rendered list - before building anything
   into the engine.
3. Measure at the operating points that discriminate: **0ms and 2ms at high concurrency**, where
   there is no handler time for dispatch to hide behind. At 100ms nothing in this area is visible.

See also: [`parked-2022-central-queue-rework.md`](parked-2022-central-queue-rework.md),
[`next-direct-pull-unordered-selection.md`](next-direct-pull-unordered-selection.md),
[`perf-unordered-dispatch-rescans-the-inflight-prefix.md`](perf-unordered-dispatch-rescans-the-inflight-prefix.md),
[`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md).
