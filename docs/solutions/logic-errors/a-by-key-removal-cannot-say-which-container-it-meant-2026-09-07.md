---
title: "The poller's stale sweep removed by key, so it evicted the fresh replacement that landed inside it (astubbs#468)"
date: 2026-09-07
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: internal / work-state
symptoms:
  - "One offset stops being worked after a rebalance, while `pc.partition.incomplete.offsets` still counts it - the record is neither in any shard nor redeliverable until that partition is re-polled"
  - "`pc.partition.latest.committed.offset` stalls for a single partition while its siblings advance, with no exception, retry or timeout"
  - "Reproduces only when a rebalance and a poll-batch registration overlap, so it never appears in a single-threaded test"
root_cause: a_removal_keyed_on_the_offset_evicts_whatever_occupies_it_not_the_container_that_was_inspected
resolution_type: code_fix
severity: high
tags:
  - rebalance
  - work-state
  - stale-epoch
  - shard
  - concurrency
  - check-then-act
  - offset-wedge
  - logic-error
---

# A by-key removal cannot say which container it meant

## Problem

`ProcessingShard.removeStaleWorkContainersFromShard` walked the shard's map, asked each occupant
whether it was stale, and removed the stale ones with `removeWorkAtOffset(entry.getKey())` - **by
key**. The staleness answer is about one container; the removal is about one offset. Between the two
statements, the offset's occupant can change.

It does change, and the two sides are genuinely different threads. The sweep is reached from
`PartitionStateManager.onPartitionsRemoved` / `onPartitionsAssigned`, i.e. inside a rebalance
callback on the **broker-poll** thread. The racing writer is `ProcessingShard.addWorkContainer`'s
stale-replacement branch - the fix for `confluentinc#909` - on the **controller**. Nothing orders
them.

    poller     : entry = next()                     -> (100, staleWc); stale? yes
    controller : addWorkContainer(freshWc)          -> workMap.put(100, freshWc), accounting settled
    poller     : removeWorkAtOffset(100)            -> evicts freshWc, by key

**The harm is the lost record.** `freshWc` is gone from the shard while `PartitionState` still
carries offset 100 as incomplete, so nothing selects it again and nothing completes it - the commit
high-water mark cannot advance past it until that partition is re-polled. This is the same
fresh-replacement-at-one-offset family as
[`stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md`](stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md),
reached from the other side: there the fresh container was *dropped on arrival*, here it arrives
successfully and is *evicted moments later*.

The accounting half was already closed by astubbs#336 and astubbs#373 - every exit path retires and
releases the claim of whatever the map actually gave up, never of the container the caller was
holding - so the counters settled correct while the record was being lost. Correct counters are not
evidence the right object left.

## What didn't work

**The obvious fix is `computeIfPresent` with an identity check in the remapping function, and it is
wrong.** It reads as airtight - the function sees the current value and only asks for a removal if
that value *is* the container the sweep inspected - and it would have shipped looking correct,
because no test written at the production seam can tell it apart from the real fix.

Measured against the JDK 17 source and by experiment. `ConcurrentSkipListMap.computeIfPresent`
commits a removal through `doRemove(key, v)`, and `doRemove` **re-reads the node's value** and gates
on `value.equals(reRead)` before its compare-and-set:

```java
else if (value != null && !value.equals(v))   // v was just re-read from the node
    break outer;
else if (VAL.compareAndSet(n, v, null)) { ... }
```

`WorkContainer.equals` is topic, partition and offset only, so a replacement landing between the
function's decision and that gate is equal-by-offset, passes it, and is removed. Reproduced
directly: with an offset-only-equals value type, a put landing inside the remapping function leaves
the map **empty** - the replacement destroyed - while the identical program with an identity-equals
value type returns the replacement and keeps it.

So the identity check narrows the window from "the whole staleness check" to "a few instructions
inside the JDK" and closes nothing. Sweeping harder only moves the window, which is the same
conclusion the 909 write-up reached about sweeping again.

Two other shapes were rejected on the way:

- **`workMap.remove(offset, container)`** - the JDK's compare-and-remove is *defined* by `equals`,
  so with offset-only equality it answers "yes, that is the one" about a container that is not there.
  No API can rescue this; the value type's equality **is** the contract.
- **A remove-then-put-back repair**, and **a claim the sweep takes before removing which the writer
  must then wait out** - both reintroduce a window, one in the map and one on the controller.

## Solution

The map stores a `ProcessingShard.Residency` token rather than the container itself. It overrides
neither `equals` nor `hashCode`, so its equality is reference identity, and
`Map.remove(key, value)` - one atomic step, fully specified, no dependence on which side the
implementation calls `equals` on - means exactly "remove this occupancy, or nothing".

```java
private WorkContainer<K, V> evictIfStillResident(long offset, Residency<K, V> inspected) {
    return workMap.remove(offset, inspected) ? retire(inspected) : null;
}
```

**Nothing evicted is a correct outcome, not a failure**: the replacement won the offset, so the call
changed nothing, retires nothing, and reports nothing. That matters downstream -
`ShardManager.removeStaleContainers` feeds the returned list to the retry queue, and astubbs#437
pinned that the queue removal is reached only through a real shard removal. Reporting a container
this call did not remove would break that gating *and* take the retry entry at coordinates the fresh
container now owns.

`ProcessingShard.getWorkIfAvailable`'s last-resort stale removal has the same shape and is written
the same way, though the race is not reachable there today: `addWorkContainer` is the only writer of
a shard map and runs on the controller, which is also the thread that runs that scan. The
conditional form costs the same and does not rest on a thread-confinement claim nothing checks.

## Why this works

The decision and the removal are the same operation. There is no interval for a writer to land in,
so there is no window to narrow. Every other candidate kept the two apart and argued about how small
the gap was.

The deeper version of the fix is to give `WorkContainer` identity equality, which would delete the
token and make every value-conditional operation on a container correct at once. It is breaking -
`WorkContainer` is public and its `compareTo` orders by offset, which would become inconsistent with
equals - so it is queued in [`docs/refactoring.md`](../../refactoring.md) under the next major.

## Prevention

- **`ShardStaleSweepReplacementEvictionTest`** carries three arms. The defect arm drives the
  interleaving deterministically through `ShardSeamTestBase` - a spied
  `PartitionStateManager.getPartitionState` that runs the other thread's action on the way past - so
  the replacement lands at an exact instruction inside the sweep rather than being raced for. Its
  control arm runs the same sweep with nothing racing it: same magnitude, different position. The
  third arm is the **premise**, pinning that no removal keyed on container equality can express
  which container it meant, `computeIfPresent` included - and it is the tripwire that goes red the
  day `WorkContainer.equals` becomes identity-based, saying the token can then be deleted.
- **Verified from the red side.** Restoring `removeWorkAtOffset(entry.getKey())` in the sweep and
  changing nothing else sends exactly the defect arm red, with the other two green.
- **Every assertion about WHICH container is resident uses reference identity**, never Truth's
  `hasValue` or `containsExactly`. Those compare with `equals`, so written that way the assertions
  pass on the defective behaviour and the test asserts nothing - the defect's own mechanism hiding
  the defect.
- **The generalisable shape**: when two threads can both act on one slot, a removal keyed on the
  slot is a check-then-act however atomic each individual access is. Name the *thing* you are
  removing, and make sure the collection can tell it apart from its replacement - which is a
  property of the value type's `equals`, not of the collection.
- **The sibling instance found by the same-defect-class sweep and NOT fixed**:
  `ExternalEngine.holdingDispatchPermit` is a `Set<WorkContainer>` whose permit accounting is
  per-record while its membership is per-offset, so two containers at one offset leak a dispatch
  permit. Open, with its reachability argument, in
  [`docs/inflight/bug-dispatch-permit-set-cannot-tell-two-containers-at-one-offset-apart.md`](../../inflight/bug-dispatch-permit-set-cannot-tell-two-containers-at-one-offset-apart.md).

## Related issues

- astubbs/parallel-consumer#468 - this fix.
- astubbs/parallel-consumer#336 and astubbs/parallel-consumer#373 - the accounting half, closed
  earlier: every exit path accounts for what the map gave up, not for what the caller inspected.
- astubbs/parallel-consumer#437 - the retry-queue pairing whose invariant decides what the sweep may
  report.
- [`stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md`](stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md)
  (`confluentinc#909`) - the other side of the same class, and the fix that created the replacement
  branch this one races.
