# Next: ordered modes are safe by construction. UNORDERED is the one to think about.

<!-- inflight-type: next -->
<!-- inflight-impact: architecture -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-22. **Supersedes a bug report that was wrong** - filed and retracted within the hour;
the retraction is the first section because the reasoning is the useful part.

## Retracted: ordered modes do not need a shard lock

I claimed direct pull breaks `KEY` and `PARTITION` ordering, because `DirectPullWorkerPool`'s only lock
guards the parking protocol and its javadoc says the scan is deliberately outside it. **That was wrong,
and the owner's counter-argument is the correct reading: a second thread cannot even try, so there is
nothing to lock out.**

Two mechanisms hold the invariant, and both are already present:

```java
if (workContainer.onQueueingForExecution()) {
    workTaken.add(workContainer);
} else {
    addToSlowWorkMaybe(slowWork, workContainer);
}

if (isOrderRestricted()) {
    break;
}
```

1. **The `break` fires after examining the head record, taken or not.** A thread arriving at a busy
   `KEY` shard examines the head, finds it in flight, falls into the `else`, and **breaks without ever
   reaching the next offset.** The shard is self-excluding.
2. **The claim decides, and it is now the only thing that decides.** Two threads that both find the
   head available both attempt the compare-and-set; the loser gets `false`, takes nothing, and breaks.
   This read `isAvailableToTakeAsWork() && onQueueingForExecution()` when the above was written, and
   the gap between the two halves turned out to be a real double-delivery defect - the availability
   check has since been folded into the claim.

**So the invariant is "at most one record examined per ordered shard per scan, and the claim decides".**
Both halves are required and both are there.

**What survives, and it is not nothing:** the guarantee lives in a `break` placement in one class plus a
CAS return value in another, **and nothing in the code names it as the ordering lock.** That is why the
split-shard experiment failed ten tests without anyone predicting it
([`parked-resume-shard-dispatch-scan.md`](parked-resume-shard-dispatch-scan.md)). **A fragility, not a
defect** - and the reason an explicit invariant test is worth having whether or not it currently passes.

## The real question: UNORDERED

**Owner's framing: prove `KEY` correct first, then think carefully about making `UNORDERED` both fast
and safe. It is a special case and should be treated as one.**

**Why it is different.** In `UNORDERED` the shard key is the **topic-partition**
(`ShardKey.of` maps `PARTITION` and `UNORDERED` to `ofTopicPartition`), so ten partitions means ten
shards - and `isOrderRestricted()` is false, so **the scan does not break.** A worker walks a shard
holding every in-flight record for its partition, and keeps walking.

Everything protecting ordered modes is therefore absent, by design:

| | `KEY` / `PARTITION` | `UNORDERED` |
|---|---|---|
| Records per shard | one key's worth | **an entire partition's** |
| Scan stops after | the first container examined | **nothing - it walks** |
| Concurrent workers on one shard | cannot get past the head | **all of them, walking together** |
| Correctness risk | none: claim decides, then break | **none either - the CAS still decides** |
| Performance risk | none | **this is where it all is** |

**Safety is not the issue - the CAS makes double-delivery impossible in both modes, and `UNORDERED`
promises no ordering to break. The issue is entirely waste.**

## The measured shape of that waste

Direct pull's collapse is an `UNORDERED` measurement: **-87% at 1,000 workers, -95% at 5,000**
([`perf-direct-pull-measured.md`](perf-direct-pull-measured.md)). The cause, predicted before the
control ran and confirmed: shard selection walks from the head, in-flight records stay in the map until
they succeed, so finding one record costs about `in-flight / shards` skips. **The shipped engine pays
that once per batch; direct pull pays it once per record, on every thread.**

So with ten shards and 5,000 in flight, **every worker walks ~500 already-claimed entries to find one
record, and most of that walking is workers rediscovering each other's claims.**

## Answered, 2026-08-22: it is shape 2, and the reasoning above was right about the mechanism

The framing in this note - that the waste is `in-flight / shards` skips paid once per record on every
thread - is exactly what a controlled measurement found, and the number is larger than the note
guesses. **With a single scanner, so that claim contention cannot exist at all**, examinations per
record dispatched rise from 1.00 at ten in flight to **440.13 at five thousand**. Adding scanners at a
fixed depth barely moves it (97.71 at one scanner, 106.56 at a hundred). So of the three shapes below:

- **Shape 1, a shard lock for selection, would have serialised the wrong thing.** The objection this
  note warns against dismissing - "a lock will be slower" - turns out not to be the point either way:
  a lock removes contention, and contention is not the cost.
- **Shape 2, O(1) selection, is what landed.** `ShardOccupancy` indexes the offsets no worker is
  holding and the unordered path walks that. The same 5,000-in-flight arm now costs 1.00, and the
  engine-shaped 5,000-workers-and-5,000-in-flight arm costs 1.60 against 1,621.89.
- **Shape 3, per-worker shard affinity, is unnecessary** rather than merely insufficient - this note's
  own reasoning about ten shards against thousands of workers stands.

The sequencing at the bottom is otherwise unchanged, and its step 3 is now the open item: **the
end-to-end runs at 10, 100, 1,000 and 5,000 have not been taken.** See
[`perf-direct-pull-collapse-is-the-scan.md`](perf-direct-pull-collapse-is-the-scan.md).

## Three shapes to weigh, none yet measured

1. **A shard lock for selection.** Turns N wasteful walks into one walk plus N-1 short waits.
   Serialises selection (microseconds) while the work stays parallel. **Simplest, one mechanism for all
   modes, and would make the per-record CAS, the `AtomicBoolean` and the volatile resume point
   unnecessary.** The objection - "a lock will be slower" - is exactly the intuition that was wrong
   three times today ([`perf-hypothesis-register.md`](perf-hypothesis-register.md)), so **measure it
   before dismissing it.**
2. **O(1) selection** - the measurement note's own proposal. Keep the shard's selectable records in a
   structure that hands one out without walking. Removes the waste rather than serialising it, at the
   cost of a second structure to keep in step - **and the conservation work today is a worked example of
   how expensive "keep two things in step" gets**
   ([`docs/solutions/logic-errors/`](../solutions/logic-errors/)).
3. **Per-worker shard affinity** - a worker prefers its own shard, so workers rarely collide. Cheap and
   partial; degrades to the current behaviour when shards are fewer than workers, **which is exactly the
   `UNORDERED` case that hurts** (ten shards, thousands of workers). Probably not sufficient alone.

## Sequencing

1. **Prove `KEY` correct with an explicit test**, since the guarantee is currently implicit -
   `test/direct-pull-coverage` is doing this.
2. **Then measure the three shapes at 1,000 and 5,000 concurrent**, where the collapse is, and at 10 and
   100, where direct pull is **3.2x faster** and must not regress.
3. **Do not pick on argument.** Every performance intuition in this investigation that was not measured
   turned out backwards.
