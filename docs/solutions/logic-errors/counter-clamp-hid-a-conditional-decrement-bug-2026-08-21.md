---
title: "A clamp on a counter hid a conditional-decrement bug, in the code that gates record intake"
date: 2026-08-21
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: service_object
related_components:
  - state_management
severity: medium
symptoms:
  - "ProcessingShard.availableWorkContainerCnt was corrected by a clamp to zero, commented 'in case of possible race condition' - a suspected race, never a described one"
  - "Revoking a partition holding a record parked in retry back-off left the counter permanently one too high, deterministically and on a single thread - no race required"
  - "Drift high is the direction the clamp never caught: it throttles record intake, and can stop AbstractParallelEoSStreamProcessor.drain() transitioning to closing"
  - "The counter fed WorkManager.isSufficientlyLoaded(), which pauses and resumes the broker poller, so a wrong value gated intake rather than merely misreporting"
root_cause: logic_error
resolution_type: code_fix
tags: [counter-drift, clamp, conservation-invariant, parallel-state, broker-poller, backpressure, longadder, mutation-testing]
---

# A clamp on a counter hid a conditional-decrement bug, in the code that gates record intake

## Problem

`ProcessingShard` maintained `availableWorkContainerCnt`, an `AtomicLong` meaning "how many of this shard's
entries are selectable as work", and corrected it after every decrement:

```java
availableWorkContainerCnt.getAndAdd(-1 * ByNum);
// in case of possible race condition
if (availableWorkContainerCnt.get() < 0L) {
    availableWorkContainerCnt.set(0L);
}
```

That number was summed across shards by `ShardManager.getNumberOfWorkQueuedInShardsAwaitingSelection()` and
added to `WorkManager.numberRecordsOutForProcessing` to decide, in `isSufficientlyLoaded()`, whether the broker
poller pauses or resumes. **A wrong value did not misreport, it mis-gated**: low and the poller resumes when it
should throttle, high and it throttles when it should fetch. And the clamp only ever caught the low side.

## Diagnosis

The comment was wrong about the mechanism. There was no race - there were two **conditional decrements whose
conditions did not match the condition the counter was incremented on**, both reproducible single-threaded:

- **`ProcessingShard.remove(long)`**, reached from the partition-revocation sweep, deducted only when the
  removed record was `isAvailableToTakeAsWork()` - which additionally requires the retry delay to have passed.
  A record parked in retry back-off had already been counted *in* by `markAvailableAgain()`, so revoking it
  left the increment behind. The retry-queue entry that normally nets that increment out is removed on the same
  code path, so nothing was left to cancel it. **Permanent, high, uncaught.**
- **Both stale-container sweeps** deducted unconditionally, so sweeping a record that was out at a worker -
  already deducted when it was selected - deducted it a second time. **Low, and clamped**, which is why the
  clamp existed.

The underlying shape is the one the codebase already names as *parallel state*: a counter and the collection it
describes, updated at different moments by different threads, with no single owner of the predicate. Every
removal site had to independently re-derive "was this record counted", and two of them got it wrong.

## Resolution

**Derive the number the gate actually wants, instead of maintaining it.** `isSufficientlyLoaded()` only ever
consumed the *sum* of "awaiting selection" and "out for processing", never the split - and that sum is just
"records inside the system", which conservation gives for free:

```
records in system = records admitted from the broker - records finished with
```

`RecordPopulation` holds that as two monotonic `LongAdder`s. A record is admitted when its `WorkContainer` is
inserted into a `ProcessingShard`'s entry map and retired when it is removed from it - the only two ways the
population changes, both inside the one class that owns the map, which was made private so that stays true.
There is no predicate at a removal site to get wrong, and no clamp: `getInSystem()` reads `retired` **before**
`admitted`, and since no record can be retired without having been admitted first, the difference is
non-negative by construction.

The two conditional-decrement defects were fixed in the same pass (`isNotInFlight()`, which is what the counter
is actually keyed on, rather than `isAvailableToTakeAsWork()`), and the clamp deleted - a clamp is only
defensible while something depends on the value being non-negative, and once the gate stopped reading it,
nothing did.

## Lessons

- **A clamp on a counter is a bug that was observed and papered over.** Treat "in case of possible race
  condition" as an unfinished diagnosis, not a mitigation. This one was not a race at all, and the direction it
  did not clamp was the harmful one.
- **Conservation beats correction, but only if the enumeration is complete.** The whole risk of the change was
  a missed removal path: unlike the clamped counter, a conservation figure that leaks has nothing to catch it.
  The mitigations were structural (make the collection private so every mutation must pass the counter) and
  evidential (mutation-test every retirement site).
- **Mutation-test a new invariant, one site at a time.** Removing each of the five admission/retirement calls
  in turn - and reverting the fixed predicate to the old one - was what proved the tests could see a leak. Four
  of the five were caught by the `WorkManager`-level invariant test; the fifth, the last-resort sweep inside
  `ProcessingShard.getWorkIfAvailable`, is only reachable in a race and needed a shard-level test written for
  it. Without that pass it would have shipped uncovered and looked tested.
- **Prove a drift red before replacing the state that drifts.** Replacing state that never disagrees is a
  refactor; replacing state that does is a fix. The revoke-a-parked-retry sequence turned this from the former
  into the latter, and it took three lines of test to find.
