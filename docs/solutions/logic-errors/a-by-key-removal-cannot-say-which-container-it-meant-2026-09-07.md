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

**Everything in this section is about the code as it was, with `WorkContainer.equals` still topic,
partition and offset.** That is what the alternatives were judged against, and it is why the fix that
landed changes the equality rather than the removal.

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

- **`workMap.remove(offset, container)` while equality was by coordinates** - the JDK's
  compare-and-remove is *defined* by `equals`, so it answered "yes, that is the one" about a container
  that was not there. No API can rescue this; the value type's equality **is** the contract - which is
  the sentence the fix below acts on, since the same call becomes correct the moment the equality
  does.
- **A remove-then-put-back repair**, and **a claim the sweep takes before removing which the writer
  must then wait out** - both reintroduce a window, one in the map and one on the controller.

## Solution

**`WorkContainer`'s equality is now reference identity** - the `equals` and `hashCode` overrides are
deleted - so `Map.remove(key, value)` on the shard's map is a true compare-and-remove: one atomic
step, fully specified, no dependence on which side the implementation calls `equals` on, meaning
exactly "remove this container, or nothing".

```java
private WorkContainer<K, V> evictIfStillResident(long offset, WorkContainer<K, V> inspected) {
    return workMap.remove(offset, inspected) ? retire(inspected) : null;
}
```

**The interim shape was a `ProcessingShard.Residency` token** - a wrapper stored as the map value,
overriding neither `equals` nor `hashCode` so that the map's comparison was identity - because
identity equality on the container itself is a breaking change and `WorkContainer` is public.
It was replaced before astubbs#468 merged, on the maintainer's call: `0.6.0.0` is the breaking
release being cut, `WorkContainer` is internal in all but its modifier, and a token per collection
is one workaround per site for a defect the value type owns.

The objections that had queued the change were checked and each fell:

- **`compareTo` becomes inconsistent with `equals`.** `Comparable` only *recommends* that
  consistency; it is *required* by `SortedSet` and `SortedMap`, and nothing in main code puts a raw
  container in either - `RetryQueue` sorts by its own `WorkContainerSortKey` and de-duplicates by its
  own `WorkContainerKey`. The ordering is for retry scheduling and display; identifying a container
  is a different question, and the class now answers the two separately.
- **Collections keyed on containers elsewhere silently change meaning.**
  `ExternalEngine.holdingDispatchPermit` is the only one, and it is *already* an
  `IdentityHashMap`-backed set with a javadoc saying why. Identity equality makes the container agree
  with that code rather than changing it.
- **The public break.** Real, and named in the release notes:
  `RecordContext`'s Lombok `@EqualsAndHashCode` covers the container it wraps, so two contexts built
  from different containers for one record no longer compare equal. Narrower than it sounds -
  `ConsumerRecord` does not override `equals` either, so `RecordContext` equality was already partly
  identity, and two contexts from two *polls* were never equal. What changes is two contexts over the
  same `ConsumerRecord` instance and different containers.

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

And it is fixed once, in the value type, rather than once per collection. A token is a workaround a
future site has to remember to repeat; identity equality makes every value-conditional operation on a
container correct by default, which is why the token did not survive to the merge. The break it costs
is recorded in
[`docs/refactoring.md`](../../refactoring.md)'s breaking-change section for `0.6.0.0`.

## Prevention

- **`ShardStaleSweepReplacementEvictionTest`** carries three arms. The defect arm drives the
  interleaving deterministically through `ShardSeamTestBase` - a spied
  `PartitionStateManager.getPartitionState` that runs the other thread's action on the way past - so
  the replacement lands at an exact instruction inside the sweep rather than being raced for. Its
  control arm runs the same sweep with nothing racing it: same magnitude, different position. The
  third arm, `twoContainersAtOneOffsetMustNotBeInterchangeable`, is the **premise**: it asserts the
  equality contract directly (two containers at one offset are not equal, and each hash code is the
  identity hash) and then the map behaviour that follows from it, `computeIfPresent`'s re-read gate
  included. **It is the tripwire for a reintroduced coordinate-based `equals`** - restore that pair
  and every assertion in it inverts.
- **Verified from the red side, twice.** Restoring `removeWorkAtOffset(entry.getKey())` in the sweep
  and changing nothing else sends exactly the defect arm red, with the other two green. Restoring the
  old coordinate-based `equals`/`hashCode` on `WorkContainer` and changing nothing else sends the
  defect arm *and* the premise arm red, with the control arm green - the control arm does not move,
  because a sweep with nothing racing it never asks which of two containers it meant.
- **Every assertion about WHICH container is resident uses reference identity**, never Truth's
  `hasValue` or `containsExactly`. Those compare with `equals`, so written that way the assertions
  pass on the defective behaviour and the test asserts nothing - the defect's own mechanism hiding
  the defect.
- **The generalisable shape**: when two threads can both act on one slot, a removal keyed on the
  slot is a check-then-act however atomic each individual access is. Name the *thing* you are
  removing, and make sure the collection can tell it apart from its replacement - which is a
  property of the value type's `equals`, not of the collection.
- **The same-defect-class sweep's one apparent hit was a FALSE POSITIVE, and how it happened is the
  lesson.** `ExternalEngine.holdingDispatchPermit` was written up as a `Set<WorkContainer>` leaking a
  dispatch permit per offset collision. It is not: the field has been
  `Collections.newSetFromMap(Collections.synchronizedMap(new IdentityHashMap<>()))` since
  astubbs#342, with a javadoc saying it is by identity and why. The sweep matched the declared type
  `Set<WorkContainer<K, V>>` and stopped there - **a collection's semantics live in its
  initialiser, not its declaration**, so a search over declarations reports the safe sites and the
  unsafe ones identically. Read the initialiser, or the search is a filter rather than a finding.

## Related issues

- astubbs/parallel-consumer#468 - this fix.
- astubbs/parallel-consumer#336 and astubbs/parallel-consumer#373 - the accounting half, closed
  earlier: every exit path accounts for what the map gave up, not for what the caller inspected.
- astubbs/parallel-consumer#437 - the retry-queue pairing whose invariant decides what the sweep may
  report.
- [`stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md`](stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md)
  (`confluentinc#909`) - the other side of the same class, and the fix that created the replacement
  branch this one races.
