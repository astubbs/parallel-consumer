---
title: "The retry queue's write lock was taken on the poll thread inside every rebalance callback"
date: 2026-09-08
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
  - readwritelock
  - fair-lock
  - retry-queue
  - archunit
  - method-reference-blind-spot
  - garbage-collection
  - issue-857
---

# Two designs for the same defect: decline the lock, or never ask for it

Found on 2026-08-31 by the defect-class sweep at the merge prep for the confluentinc#857 revoke-path
fix, once `ArchitectureTest.rebalanceCallbacksMustNotBlock` learned to recognise
`ReentrantReadWriteLock`. It is the second member of the class that rule exists for: a blocking
acquire on the broker-poll thread inside a rebalance callback.

**Two fixes were written for it.** The first, astubbs/parallel-consumer#431, kept the poll thread on
the queue and taught it to DECLINE. The second, which shipped, took the poll thread off the queue
entirely and made the controller thread collect what it leaves. This write-up records both, because
the first was correct and its reasoning is the reason the second is safe.

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
   `onPartitionsLost` through `WorkManager` and `PartitionStateManager`.
2. `ShardManager.removeStaleContainers` - which mapped the METHOD REFERENCE `retryQueue::remove` over
   the swept containers, and is reached from `onPartitionsAssigned` as well as from the two above.

ArchUnit models a method reference as a `JavaMethodReference`, which the rule's original walk
(`getMethodCallsFromSelf()`) did not return. Widening the walk was extracted and landed first, as
astubbs/parallel-consumer#465: `getMethodReferencesFromSelf()` is followed beside the calls, a
`@ControllerThreadOnly` marker makes the thread contract a declaration the rule reads, and
`RebalanceCallbackRuleControlTest` is the standing control for both. It shipped RED rather than
green - the eighteen `root => target` keys went into `KNOWN_BLOCKING_VIOLATIONS` with an owner named
- so the gate that catches the class was in place, and measured, before the instance was fixed.

**An exemption list that looks complete is evidence about what the walk can see, never about what
the callback reaches.**

## Why "decline and move on" is not enough on its own

The removal is half of a pair, and the original order was shard first, queue second:

```java
WorkContainer<K, V> removedWC = shardOpt.get().removeWorkAtOffset(consumerRecord.offset());
if (Objects.nonNull(removedWC)) {
    this.retryQueue.remove(removedWC);   // <- the blocking half
}
```

Declining *there* leaves the container out of its shard and still in the retry queue. **At the time,
that entry was then removed by nothing, ever.** Work is handed out by scanning shards, so a container
in no shard is never selected, never completed, and never swept - while every route that removed a
retry-queue entry reached it THROUGH shard contents. What that costs was measured by
astubbs/parallel-consumer#437 and is recorded in
[`retry-queue-orphan-window-between-the-requeue-check-and-the-add.md`](retry-queue-orphan-window-between-the-requeue-check-and-the-add.md):
it is **not** the broker-poller load gate, which reads the figure LOW and so fetches sooner; it is
`ShardManager.getNumberOfWorkQueuedInShardsAwaitingSelection`, which floors its shard term at zero,
so a drained instance reads the orphan's ready-to-retry contribution forever and a draining close
hangs to its timeout.

The epoch check does not make an orphan harmless. It covers the two things usually asked about -
`PartitionState.couldBeTakenAsWork` refuses to hand a stale container out, and the revoked
partition's state is replaced by `RemovedPartitionState`, so no offset of its is committed - but
neither the queue's size/ready count nor `ShardManager.getLowestRetryTime` applies any epoch filter.

**Both designs below are answers to this one sentence.** The first keeps the pair together so no
orphan is ever created; the second lets the orphan be created and collects it.

## Design A - prevent the split (astubbs/parallel-consumer#431, superseded)

**Ask the retry queue FIRST, let it refuse, and abandon the whole pair on a refusal.**

- `RetryQueue.tryRemove(topic, partition, offset)` takes the write lock with `tryLock()` or returns
  false having changed nothing. Keyed by the record's coordinates rather than by a container,
  precisely so a caller can ask before it has removed anything. `tryLock()` barges, which is the
  property wanted: it returns immediately either way.
- `ShardManager.removeWorkFromShardFor` and `ProcessingShard.removeStaleWorkContainersFromShard`
  (which then had to take the queue, so the pair was maintained in one place) skip their shard
  removal when refused.
- **The queue is asked a SECOND time, after the shard removal.** Asking first is what lets a refusal
  abandon; it is not sufficient, because `ShardManager.onFailure`'s re-queue can land in the gap
  between the two removals and its residency confirmation still sees a resident container. That is
  the window astubbs#437 closed from the other side.
- **A refused SECOND ask cannot abandon** - the shard entry has already gone - so the shard puts the
  container back exactly as it was, population and selection claim included.
- What is left behind is a whole stale pair, which the engine tolerates.
  `ProcessingShard.getWorkIfAvailable`'s last-resort stale branch retires it from both structures on
  the controller thread.

**It was correct, and it was proven.** Its `RetryQueueRebalancePathTest` was red on master in two
different ways - a timeout on the write lock, and the split state observed directly - and its
ordered-mode arm measured the retirement bound.

**Why it was superseded: cost, not correctness.** Over the same three main-code files it is
`+347 -72` against Design B's `+185 -67` - and most of Design B's additions are the javadoc stating
the invariant, so the machinery gap is wider than the line counts. `git diff --numstat` against each
branch's merge base reproduces both. More to the point is what those lines are: a second, non-blocking entry point on
`RetryQueue` keyed differently from the blocking one; a queue reference threaded into a second
`ProcessingShard` method; two asks per removal with a put-back on the second refusal; and a
retirement bound that varied by ordering mode - "until the takeable head in front of it leaves the
shard" under KEY and PARTITION, because the shard scan breaks at the first container it hands out and
the retiring branch lives past that break. Every one of those is a thing the next reader has to hold
in their head to answer "is this still safe".

## Design B - never ask (shipped)

**The poll thread does not touch the retry queue at all. The controller thread collects retry-queue
entries whose container is resident in no shard.**

- The rebalance callbacks remove from the SHARDS only.
  `ShardManager.removeWorkFromShardFor` drops its `retryQueue.remove(removedWC)`;
  `ShardManager.removeStaleContainers` drops its `.map(retryQueue::remove)` stage. The shard map is
  a `ConcurrentHashMap` of `ConcurrentSkipListMap`s - there is no lock to wait for and nothing to
  decline, so the callback cannot fail and needs no refusal contract.
- `ShardManager.purgeDepartedRetryEntries()` scans the queue under the read lock, collects the
  entries whose container `ProcessingShard.isResident` says has gone, closes the iterator, and
  removes them under the write lock. It runs at the top of `ShardManager.getWorkIfAvailable`, which
  the control loop reaches once per pass through `retrieveAndDistributeNewWork` in both `RUNNING` and
  `DRAINING` - and before `drain()` reads the awaiting-selection figure in that same pass, which is
  the consequence that matters.

**The invariant, stated at the purge and nowhere else:** a retry-queue entry with no resident
container is garbage the controller collects; it may exist for at most one control-loop tick; the
callbacks are free to create it.

**The bound is one tick in every ordering mode**, which is where it differs from Design A. The purge
scans the retry queue rather than the shards, so the ordered modes' shard-scan break cannot delay it.
`RetryQueueRebalancePathTest.underOrderedProcessingADepartedEntryIsStillCollectedOnTheSameTick`
asserts both halves: the head is taken, the scan does stop there, and the departed tail's entry goes
on that same tick.

### Why the scan and the removal can be two steps

They are a check-then-act, and it is safe for two independent reasons, both of which have to hold:

- **Departure is monotonic.** `isResident` is reference identity, and nothing re-inserts the same
  container instance, so "not resident" cannot go stale in the dangerous direction.
- **Nothing else writes the queue's keys.** `RetryQueue` removes by topic/partition/offset, so the
  removal cannot say which container it meant - the defect class of astubbs#468, recorded in a note
  that PR retired when it merged
  (`git show 7c95b75ce^:docs/inflight/bug-stale-sweep-iterator-evicts-fresh-replacement.md`). Here there is no race to
  narrow: `RetryQueue.add` is `@ControllerThreadOnly`, its only production caller is
  `ShardManager.onFailure`, and the purge runs on that same thread.

**What would reopen it is a second writer of the retry queue**, which is exactly what
`@ControllerThreadOnly` declares and what the ArchUnit rule reports a rebalance-callback reach into.
The general runtime guard does not exist and is tracked in
`docs/inflight/core-retry-queue-needs-a-runtime-controller-ownership-guard.md`.

### What the counters do during the window

`getWorkableRecords` is `inShards - parkedForRetry`. Between a callback's shard removal and the
purge, `inShards` has dropped and `parkedForRetry` has not, so the figure reads one **LOW** - it goes
negative in the single-record case. Low is the safe direction and the only one worth asserting:
`isSufficientlyLoaded()` is `workable > threshold`, so a low figure fetches sooner, while a high one
would pause the poller with nothing to process - the confluentinc#857-family stall shape.
`WorkManagerTest.theLoadGateReadsLowNotHighForTheTickAfterARevocation` pins the direction and that
one controller pass settles it. This re-confirms on this tree what astubbs#437 measured about the
same figure.

### What astubbs#437's residency confirmation became

`ShardManager.onFailure` adds to the retry queue and then confirms shard residency, undoing the add
if the container has left. Against a shard-only sweep it still closes two of the three
interleavings, and the third - the sweep starting after the residency read - is the one the purge
collects a tick later. **So it is now belt-and-braces, kept deliberately**: it costs one reference
comparison to save a tick on the common interleaving. Whether it earns that is a live question and
must not be assumed from the fact that both exist.

## Rejected alternatives

- **Design A**, above: correct, proven, and more machinery than a controller that collects garbage.
- **`tryLock` and accept the orphan.** Rejected before either design: the counters have no epoch
  filter, and at the time nothing could ever remove the entry. Design B is not this - it makes the
  orphan collectable rather than accepting it.
- **Reuse astubbs/parallel-consumer#466's control-thread hand-off.** That mechanism posts a request
  from the poll thread, wakes the mailbox, and has the poll thread WAIT on a `CompletableFuture` for
  the controller to serve it - which is the right shape for a revocation commit, because the
  revocation is not correct until the commit has happened. It is the wrong shape here: the purge
  needs no hand-off at all, since the controller can discover departed entries by scanning, and
  reusing that PR's shape would reintroduce the very poll-thread wait being removed.
- **A dirty signal the callbacks set and the purge reads**, to skip the scan on ticks where nothing
  departed. Rejected for this change as a second piece of cross-thread state on a class whose whole
  difficulty is cross-thread state - written by the poll thread, on the rebalance path, in the change
  that is removing the poll thread's coupling to this structure.

  **The cost it would avoid is real, and both reviewers of the shipped design found it
  independently**, so the rejection is a judgement rather than a dismissal. The purge is the FIRST
  unconditional full scan of the retry queue on the control loop: every other reader of it there
  early-stops on the `retryDueAt` sort order (`getNumberOfFailedWorkReadyToBeRetried` breaks at the
  first not-ready entry, `getLowestRetryTime` returns at the first not-in-flight one) and
  `ProcessingShard`'s `removeAll` is bounded by the batch. Shard residency has no relationship to
  that sort order, so the purge cannot early-stop, and it finds nothing on the overwhelming majority
  of ticks.

  **What the reviews sharpened, and it is not what was rejected:** the right primitive is a
  *monotonic counter* incremented on departure and read plainly by the controller, not a boolean
  flag - a `LongAdder` in the shape `RecordPopulation` already uses at those same call sites, with no
  lock and an uncontended increment. A counter also keeps the one-tick bound trivially: a departure
  landing between the read and the scan costs one extra tick and can never lose an entry.

  **What should happen before it is adopted: a measurement.** The scan's cost is bounded by
  retry-queue size, which is bounded by the in-flight target, so this is not a blow-up - it is a
  fixed tax whose size nobody has measured. `DispatchScanMeter` is the precedent for metering exactly
  this class of cost in this class, and the honest order is to meter the scan before optimising it.
- **Shorten the read-lock hold** so the write acquire is short rather than declined. Does not fix
  anything: the acquire is still a wait, and the ArchUnit rule is still red on merit.
- **A bounded `tryLock(timeout)`.** Still a wait, still spent out of `max.poll.interval.ms`.

## How it was verified

**Red first, on the unfixed tree.** `RetryQueueRebalancePathTest` holds the read lock the way the
controller thread does - through a live `RetryQueue.iterator()` - and drives the production callback
on a thread named `broker-poll`. Six arms, five red against master: two callbacks time out on the
write lock (`aRevokeDoesNotTouchTheRetryQueueAndSoCannotWaitForItsWriteLock`,
`theStaleSweepDoesNotTouchTheRetryQueueEither`), and three assert collection that does not happen.
The sixth, `anEntryWhoseContainerIsStillResidentIsNotCollected`, is the control and is green
throughout.

**Ablations.** Deleting the purge call turns six arms red across three classes; inverting its
residency predicate turns seven red - the same six plus the control, which is what the control is
for.

**The ArchUnit rule was proven red and then green.** With `KNOWN_BLOCKING_VIOLATIONS` emptied and
the callbacks still reaching the queue it reports **24** violations (18 exemption keys; the
`@ControllerThreadOnly` half fires once per route, so the revoke and lost roots report through both
`removeWorkFromShardFor` and `removeStaleContainers`). With the callbacks off the queue it is green
with the set still empty. The set is kept, empty, with its keying and its rules intact: an exemption
is how a newly-found reach is recorded as a defect on the books rather than deleted from the report.
