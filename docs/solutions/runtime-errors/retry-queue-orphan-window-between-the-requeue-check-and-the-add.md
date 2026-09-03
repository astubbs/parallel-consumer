---
title: "The retry-queue orphan window between the re-queue's live epoch check and its add"
date: 2026-09-03
category: runtime-errors
module: parallel-consumer-core/state
problem_type: runtime_error
component: background_job
severity: high
symptoms:
  - "A draining close never transitions to closing and hangs to its drain timeout, with nothing assigned, nothing in flight and no records in any shard"
  - "`workIsWaitingToBeProcessed()` / `isRecordsAwaitingProcessing()` read true forever on an idle instance"
  - "The retry queue holds an entry whose container is resident in no shard, so no scan, sweep or completion can ever remove it"
  - "Only under PARTITION or UNORDERED ordering - KEY ordering garbage-collects the emptied shard, and the re-queue is skipped"
  - "Requires a rebalance to complete while a failed record's result is being handled"
root_cause: race_condition
resolution_type: code_fix
related_components:
  - ShardManager
  - ProcessingShard
  - RetryQueue
  - WorkManager
  - RetryQueueRequeueWindowTest
tags:
  - concurrency
  - check-then-act
  - rebalance
  - retry-queue
  - stall
---

# The retry-queue orphan window between the re-queue's live epoch check and its add

## The window

`WorkManager.onFailureResult` re-validates staleness against the LIVE partition map immediately
before `sm.onFailure(wc)` - astubbs/parallel-consumer#346 added that check precisely because
staleness checkpoint 3's answer cannot carry the decision. But the re-validation is itself a
check-then-act, and its own comment said so: no epoch check placed there can be atomic with the
actions that follow it.

So a rebalance completing on the broker-poll thread **between that check and the add** leaves this:

1. the controller passes the live epoch check - the partition is still owned;
2. the poll thread increments the epoch and runs the revoke sweep, which removes the container from
   its shard and then removes it from the retry queue, finding nothing there yet;
3. the controller runs `ShardManager.onFailure`. Under PARTITION or UNORDERED ordering the emptied
   shard object survives - only KEY ordering garbage-collects one - so `getShard` still answers
   present, and the container is added to the retry queue.

The result is a **queue-only orphan**: a container in the retry queue and in no shard. Work is handed
out by scanning shards, so it is never selected, never completed and never swept - and every route
that removes a retry-queue entry reaches it *through* shard contents, so nothing can ever take it
out again. It is there for the life of the instance.

## What it actually costs - and the claim that did not survive measurement

The defect was recorded, both in `WorkManager.onFailureResult`'s comment and in the in-flight note
raised on astubbs/parallel-consumer#431, as misleading the **broker-poller load gate**:
`ShardManager.getWorkableRecords()` subtracts a parked-for-retry figure from a shard population that
no longer contains the orphan, so the gate is told the system holds less than it does - a
confluentinc#857-family stall.

**That is wrong twice over, and the measurement is in
`RetryQueueRequeueWindowTest.aQueueOnlyOrphanCostsTheDrainFigureAndNotTheLoadGate`.**

- **Not permanent.** `parkedForRetry` is `queueSize - readyToRetry`. The orphan contributes to it
  only while its retry delay is still running; once the delay passes it counts in *both* terms and
  the contribution is exactly zero, for good.
- **Not the stall direction.** While the contribution is non-zero it makes `workable` read LOW, so
  `isSufficientlyLoaded()` reads false and the consumer fetches MORE. A stall needs the figure to
  read HIGH.

**The real cost is the other figure.** `getNumberOfWorkQueuedInShardsAwaitingSelection()` is
`readyToRetry + max(0, shardCounters - queueSize)`. The orphan's two contributions cancel only while
`shardCounters - queueSize` is positive. Drained - which is exactly when it matters - that term
floors at zero and the `readyToRetry` contribution survives alone and permanently. That figure is
`WorkManager.isRecordsAwaitingProcessing()`, which `AbstractParallelEoSStreamProcessor.drain()`
requires to be false before it transitions to closing. **A single orphan holds a draining close open
to its timeout on an instance with no work in it.**

This is the same consequence family as the inline stale eviction that leaked a retry-queue entry -
that one is fixed and its note retired, trace at
`git show a80f2bbd1:docs/inflight/bug-retry-queue-orphaned-by-inline-stale-removal.md` - but reached
by a different door, and paid for at a different gauge than either note claimed.

## The fix: add first, confirm residency second

**A residency test *before* the add would only narrow the window.** It is another check-then-act, and
the sweep can land between that answer and the add exactly as it lands between the epoch check and
the call. That is what made this a note rather than a patch.

**Reversing the order closes it**, with no lock and nothing waiting. `ShardManager.onFailure` now adds
to the retry queue and *then* asks whether the container is still resident in its shard, undoing the
add if it is not. The last thing to happen is a removal driven by a read taken *after* the add, and
the competing sweep's own action is also a removal - so at least one of the two always observes the
entry. The four interleavings:

- the sweep completes before the add - the residency read sees a departed container and undoes it;
- the sweep starts after the residency read - it finds the container in the shard, so
  `removeWorkAtOffset` hands it back non-null and the paired queue removal runs;
- the sweep lands between the add and the residency read - its queue removal finds the entry this
  time and takes it, and the read then undoes an add that is already gone (a no-op);
- the sweep's queue removal races the add itself - `RetryQueue` serialises them under its own write
  lock, reducing this to one of the two above.

Once the read sees a departed container the answer cannot go stale in the dangerous direction:
residency is by **reference**, and nothing ever re-inserts the same container instance.

**This is the shape `ProcessingShard.includeInSelection` already used** for the selection claim -
take the claim, then confirm residency, then hand it back if the container has left. The residency
predicate is now `ProcessingShard.isResident`, shared by both.

### It does not collide with astubbs/parallel-consumer#431

That PR established that the **rebalance callbacks may not wait on the retry queue's lock** - they run
on the broker-poll thread inside `consumer.poll()`, where the whole group waits, and it made them
decline the lock rather than block on it. The obvious "proper" fix here - moving the shard map and the
queue under one lock - would reverse that decision, because it puts the poll thread's revoke path
behind the retry queue's fair lock.

The fix above adds no lock, and changes nothing on the poll thread's side of the interleaving. The
controller does one extra map read and, in the rare losing case, one extra queue removal.

## How it was established

Deterministic white-box driver, not a raced reproduction:
`RetryQueueRequeueWindowTest.RequeueWindowWorkManager` overrides the live epoch check, computes the
real answer, then runs the full production revoke path before returning it - landing the rebalance at
the exact instruction the window opens at. Every arm asserts two preconditions (the race fired, and
the live check answered *not stale*), so an arm that stops exercising the window fails rather than
passing vacuously.

**Predictions were stated before the run and all held, including the refutation.** Against unmodified
master, three of the six arms were red - the two window arms and the drain arm - and the three that
were predicted green were green: the load-gate characterisation, and both control arms.

**Control arms, same magnitude and different position.** The same rebalance one seam *earlier*
(checkpoint 3's lookup) is caught by the live check and produces no orphan - which is what isolates
this to the gap between the check and the add rather than to "a rebalance during a failure result" in
general. The same rebalance one seam *later* leaves the pair whole, because the sweep then finds the
container in its shard and takes the queue entry with it.

**Ablation.** With the fix in place all six arms pass. Removing *only* the post-add residency undo,
leaving every other change in place, turns exactly those three arms red again and no others. One term
changed, outcome flips.

## What would reopen it

Any future caller that adds to the retry queue without a residency confirmation *after* the add, or
that removes a container from a shard without removing it from the queue.

**One such site exists today and is a different, unfixed defect**:
`ProcessingShard.addWorkContainer`'s displacement branch retires the container it displaced and
releases its selection claim, but cannot remove its retry-queue entry - the shard holds no reference
to the queue. Demonstrated during this work's defect-class sweep; tracked in
`docs/inflight/bug-shard-displacement-orphans-the-retry-queue-entry.md`.
