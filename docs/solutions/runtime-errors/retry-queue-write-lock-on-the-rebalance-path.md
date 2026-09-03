---
title: "The retry queue's write lock was taken on the poll thread inside every rebalance callback"
date: 2026-09-03
category: runtime-errors
module: parallel-consumer-core/state
problem_type: runtime_error
component: background_job
severity: high
symptoms:
  - "A rebalance callback can sit inside `consumer.poll()` waiting for `RetryQueue`'s write lock while the controller thread scans the queue under its read lock"
  - "The wait is spent out of `max.poll.interval.ms`, which is the budget whose overrun evicts the member"
  - "No deadlock: no cycle was found, so the worst case is an unbounded wait rather than a permanent stop"
  - "Invisible to the ArchUnit rule on three of its roots, because the reach was through a method reference"
root_cause: thread_violation
resolution_type: code_fix
related_components:
  - RetryQueue
  - ShardManager
  - ProcessingShard
  - PartitionStateManager
  - ArchitectureTest
tags:
  - rebalance
  - poll-thread
  - trylock
  - readwritelock
  - fair-lock
  - retry-queue
  - archunit
  - method-reference-blind-spot
  - issue-857
---

# Declining a lock is easy; declining it without splitting a pair is the work

Found on 2026-08-31 by the defect-class sweep at the merge prep for the confluentinc#857 revoke-path
fix, once `ArchitectureTest.rebalanceCallbacksMustNotBlock` learned to recognise
`ReentrantReadWriteLock`. Fixed on 2026-09-03. It is the second member of the class that rule exists
for: a blocking acquire on the broker-poll thread inside a rebalance callback.

## The defect

`RetryQueue.remove` took `lock.writeLock().lock()` - unbounded, blocking - and was reachable from
every rebalance callback. A rebalance callback runs on the poll thread inside `consumer.poll()` with
the whole group waiting on it, so anything it cannot get immediately it must decline rather than wait
for.

The wait is not theoretical. `RetryQueue.iterator()` acquires the READ lock and hands it to the
caller, released only when the iterator is closed - its javadoc says it is "really important for it
to be closed in timely fashion". `ShardManager.getLowestRetryTime` is that caller, on the controller
thread, for a whole scan. And the lock is constructed fair (`new ReentrantReadWriteLock(true)`), so a
waiting writer queues behind the scan rather than interleaving with it.

No cycle was found, so this was never a second AB-BA deadlock - the claim was, and remains, "an
unbounded wait on the poll thread whose worst case is unmeasured".

## What the reachability walk actually found

Only `remove()` is reachable from a callback. `add()` and `removeAll()` are controller-thread only,
and `clear()` has no production caller at all.

It is reachable **twice**, and the second reach was invisible:

1. `ShardManager.removeWorkFromShardFor` - a direct call, from `onPartitionsRevoked` and
   `onPartitionsLost` through `WorkManager` and `PartitionStateManager`. This is the one the six
   exemptions in `KNOWN_BLOCKING_VIOLATIONS` named.
2. `ShardManager.removeStaleContainers` - which mapped the METHOD REFERENCE `retryQueue::remove` over
   the swept containers, and is reached from `onPartitionsAssigned` as well as from the two above.

ArchUnit models a method reference as a `JavaMethodReference`, which the rule's walk
(`getMethodCallsFromSelf()`) does not return. Measured: with every exemption deleted, the unfixed
tree reported **six** violations - all through reach 1, none on `onPartitionsAssigned` - and after
the fix rewrote that method reference as a lambda over a direct call, the same probe (temporarily
adding `WriteLock.tryLock()` to the deny list) reported **nine**. An exemption list that looks
complete is evidence about what the walk can see, never about what the callback reaches.

## Why "decline and move on" was not enough

The obvious fix - `tryLock`, mirroring `AbstractParallelEoSStreamProcessor.tryCommitOffsetsOnRevoke`
- has a trap in it, because the removal is half of a pair. The unfixed order was shard first, queue
second:

```java
WorkContainer<K, V> removedWC = shardOpt.get().removeWorkAtOffset(consumerRecord.offset());
if (Objects.nonNull(removedWC)) {
    this.retryQueue.remove(removedWC);   // <- the blocking half
}
```

Declining *there* leaves the container out of its shard and still in the retry queue. **That entry is
then removed by nothing, ever.** Work is handed out by scanning shards, so a container in no shard is
never selected, never completed, and never swept - while
`RetryQueue.getQueueSizeAndNumberReadyToBeRetried` keeps counting it as parked for retry, and the
broker-poller load gate keeps subtracting that count from the shard population. The same orphan
reached from a different door is what `WorkManager.onFailureResult` re-validates against, and what
`ShardPopulationRaceTest.theInlineStaleSweepTakesTheRecordOutOfTheRetryQueueToo` pins.

So the epoch check does **not** make an orphan harmless. It covers the two things usually asked
about - `PartitionState.couldBeTakenAsWork` refuses to hand a stale container out, and the revoked
partition's state is replaced by `RemovedPartitionState`, so no offset of its is committed - but
neither the queue's size/ready count nor `ShardManager.getLowestRetryTime` applies any epoch filter,
and those are what gate the poller and time the control loop.

## The fix

**Ask the retry queue FIRST, let it refuse, and abandon the whole pair on a refusal.**

- `RetryQueue.tryRemove(topic, partition, offset)` takes the write lock with `tryLock()` or returns
  false having changed nothing. It is keyed by the record's coordinates rather than by a container
  precisely so a caller can ask before it has removed anything. `tryLock()` barges - it ignores the
  fairness policy - which is the property wanted: it returns immediately either way.
- `ShardManager.removeWorkFromShardFor` and `ProcessingShard.removeStaleWorkContainersFromShard`
  (now taking the queue, so the pair is maintained in one place) skip their shard removal when
  refused.
- What is left behind is a container that is already stale - both callers run after
  `PartitionStateManager.onPartitionsRemoved`/`onPartitionsAssigned` has incremented the epoch - and
  staleness is a state the engine is built to tolerate. `ProcessingShard.getWorkIfAvailable`'s
  last-resort stale branch then retires it from **both** structures, on the controller thread, where
  waiting for the lock is permitted.

Nothing is moved off-thread, nothing is queued for later, and no caller contract changes except that
a removal may be deferred. **How long that delay is, `ShardManager.removeWorkFromShardFor`'s javadoc
owns** - it was measured on 2026-09-03 and is not the one control-loop tick this paragraph used to
claim: under KEY or PARTITION ordering the shard scan breaks as soon as it hands out a container, so
a stale entry behind a takeable head is not inspected that tick, and the delay ends when the head in
front leaves. What is bounded is that the pair stays whole for the whole wait.

### What would reopen it

The abandonment is only ever a delay, and exactly one thing ends it. If
`ProcessingShard.getWorkIfAvailable`'s stale branch stops removing from the retry queue, or stops
being reached at all, the delay becomes permanent and the orphan is back.
**Nothing fails if that happens**: `rebalanceCallbacksMustNotBlock` only checks that nothing on the
callback path WAITS, and it is green either way. The reasoning is recorded at the site, on
`ShardManager.removeWorkFromShardFor`.

## Rejected alternatives

- **`tryLock` and accept the orphan.** Rejected on the evidence above: the counters have no epoch
  filter, and the orphan is permanent.
- **Record the revocation on the poll thread and have the controller thread perform the removal.**
  The larger shape the rule's own advice offers. Rejected as unnecessary once the pair could simply
  be left intact - it adds a second piece of cross-thread state to a class whose whole difficulty is
  cross-thread state.
- **Shorten the read-lock hold** so the write acquire is short rather than declined (e.g. snapshot in
  `getLowestRetryTime` instead of scanning under the lock). Does not fix anything: the acquire is
  still a wait, and the ArchUnit rule is still red on merit.
- **A bounded `tryLock(timeout)`.** Still a wait, still spent out of `max.poll.interval.ms`.

## How it was verified

`RetryQueueRebalancePathTest` holds the read lock the way the controller thread does - through a live
`RetryQueue.iterator()` - and drives the production callback on another thread. All three tests were
red against the unfixed tree and they fail differently, which is why all three are there: two time
out on the write lock, and `aDeclinedRevokeLeavesTheShardAndTheRetryQueueInStep` observes the split
state directly, because the unfixed order was shard-first.

The six `ReentrantReadWriteLock$WriteLock.lock()` entries were then deleted from
`KNOWN_BLOCKING_VIOLATIONS`, and the rule is green on merit.
