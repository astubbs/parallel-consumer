---
title: "The shard-displacement retry-queue orphan is UNREACHABLE in production, and the invariant that carries it is not in ProcessingShard"
date: 2026-09-08
category: logic-errors
module: parallel-consumer-core/state
problem_type: logic_error
component: internal / work-state
symptoms:
  - "A demonstrated pairing gap - a container leaves a shard without its paired retry-queue removal - with no established production path to it"
  - "`RetryQueue.getLowestRetryTime` and the ready-to-retry count reading one entry high, carrying a departed container's retry-due time"
root_cause: not_a_defect_in_production_the_reachability_condition_cannot_be_satisfied
resolution_type: proof_of_unreachability
severity: low
related_components:
  - ProcessingShard
  - ShardManager
  - RetryQueue
  - PartitionState
  - PartitionStateManager
  - ShardDisplacementOrphanReachabilityTest
tags:
  - concurrency
  - rebalance
  - retry-queue
  - shard
  - reachability
  - root-cause-method
---

# The shard-displacement retry-queue orphan is unreachable, and the invariant that carries it is not in `ProcessingShard`

Companion to
[`retry-queue-orphan-window-between-the-requeue-check-and-the-add.md`](../runtime-errors/retry-queue-orphan-window-between-the-requeue-check-and-the-add.md),
which fixed the *other* door into the same orphan (astubbs#437) and, in its defect-class sweep,
found this one and left it open with the reachability question unanswered. This record answers it.

**The claim.** `ProcessingShard.addWorkContainer`'s displacement branch - anchor
`A real replacement after all` - retires the container it displaced and gives back its selection
claim, but cannot remove its `RetryQueue` entry, because the shard holds no handle on the queue.
**The pairing gap is real and demonstrated.** What was never established is whether production can
reach that branch *with a queue-resident container*. It cannot. The branch itself is reachable -
confluentinc#909's late drain does it routinely - but only ever with a container that has never
been in the retry queue.

## The four things that must be true at once

At the instant `addWorkContainer` runs for a container `B` at `(topic, partition, offset)`:

1. a container `A` is resident at that offset;
2. `A` is observed **stale** - otherwise the arrival is dropped and there is no displacement;
3. `A` holds a `RetryQueue` entry;
4. `B` exists at all - a *second* container at coordinates already occupied.

## Each leg, traced in source rather than inferred

**L1 - the displaced container is always the one that was inspected.** `workMap` has exactly two
insertion sites: `addWorkContainer`, and `plantResident`, which carries `// visible for testing` and
has no production caller. `addWorkContainer` is control-thread-only - the broker-poll thread's
`registerWork` only posts the batch to the mailbox, which `PartitionState`'s
`Thread model, derived from the callers rather than declared` javadoc states outright. So between
the `workMap.get` and the `workMap.put` only *removals* can interleave, and a removal makes the
`put` return null. `displaced` is therefore either null or exactly `residentBeforePut`, and (2) is a
statement about the container that actually leaves.

**This is the one leg astubbs#468's defect class does not reach here.** A removal keyed by offset
can hit a different occupant than the one inspected; an *insertion* keyed by offset cannot, because
there is only one inserting thread.

**L2 - a queue entry implies non-stale-and-resident when it was made.** `RetryQueue.add` has exactly
one production caller: `ShardManager.onFailure`. Its only caller is `WorkManager.onFailureResult`,
behind the live `checkIfWorkIsStale(wc)` re-validation astubbs#346 added - and since astubbs#437,
followed by a residency confirmation that undoes the add if the container has left its shard. It is
also the only way in at all: `PartitionState.couldBeTakenAsWork` refuses a stale container, so a
container that is stale when it reaches a shard can never be selected, can never fail, and can never
be queued.

**L3 - staleness is monotone per container.** `PartitionState.checkIfWorkIsStale` is
`fencedForRevocation || isPartitionRemovedOrNeverAssigned() || partitionsAssignmentEpoch != workEpoch`.
`WorkContainer`'s `private final long epoch` and `PartitionState`'s
`private final long partitionsAssignmentEpoch` are both final; `fencedForRevocation` is set and
never cleared; a replaced state carries a strictly higher epoch or is the `RemovedPartitionState`
singleton. Once true, true forever for that container.

**L4 - so `A` must cross the staleness boundary between L2 and the displacement, and exactly three
transitions can do it.**

- **T1** `partitionStates.put(tp, RemovedPartitionState.getSingleton())` in
  `PartitionStateManager.resetOffsetMapAndRemoveWork`;
- **T2** `this.partitionStates.putAll(partitionStates)` in `onPartitionsAssigned`;
- **T3** `PartitionState.fenceForRevocation()`.

**`incrementPartitionAssignmentEpoch` alone is NOT one of them**, and this is the leg most likely to
be got wrong by reading: the state's own epoch is final and staleness is only ever asked *through
the state object*, so bumping the manager's epoch map changes no answer until the state is replaced.
That is the same fact
[`stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md`](stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md)
established from the other direction.

**L5 - T1 and T2 each carry a paired sweep, on the same thread, before the callback returns.** T1's
swap is immediately followed in the same loop iteration by `partition.onPartitionsRemoved(sm)` -
`sm.removeAnyShardEntriesReferencedFrom(incompleteOffsets.values())`, which removes from the shard
and then from the queue - and `onPartitionsRemoved` ends with `sm.removeStaleContainers()`. T2 is
the last statement before `sm.removeStaleContainers()` in `onPartitionsAssigned`. Both sweeps reach
`A`: `processingShards` is never replaced (`ShardMapIsNeverReplacedArchTest` pins it), shards are
only ever removed when empty, and both iterators are weakly consistent, which still guarantees every
element present for the whole traversal.

**L6 - T3, the fence, carries no sweep at all**, and it is genuinely a production path:
`AbstractParallelEoSStreamProcessor` calls `wm.fenceForRevocation(...)` on the **control thread**
inside the produce lock, and truncation follows later on the poll thread. So
**(stale AND resident AND queued) is reachable through the production API**, with no white-box
planting. It is (4) that cannot be supplied.

**L7 - (4) requires the offset to be delivered twice, which requires a re-assignment.** Within one
assignment generation the consumer's fetch position never goes backwards: nothing in main calls
`seek` (`grep -rn '\.seek(' parallel-consumer-core/src/main` returns nothing), and shards are
partition-scoped in every ordering mode, so two records cannot collide on one offset from different
partitions - `ShardKey.KeyOrderedKey` owns that reasoning and states it as
`Offsets are only unique WITHIN a partition`. A second container at the same coordinates therefore
needs the partition to be revoked and re-assigned, which is T1 followed by T2, sweeps included, both
completing on the poll thread inside `poll()` before the `poll()` that yields `B` can return.

**Conclusion: (2) AND (3) AND (4) is unsatisfiable.**

## Why this is worth recording rather than just closing

**The guard on the last leg is outside the class.** Legs L1-L5 are properties of this engine; L7 is
a property of the Kafka consumer's fetch position. The pairing gap in the displacement branch is one
arrival away from being real, not absent - and nothing in `ProcessingShard`, `ShardManager` or the
test suite would notice if that arrival became possible.

**What would reopen it**, none of it guarded:

- any in-generation replay of an already-registered offset - a `seek`, or an offset-reset /
  truncation replay that re-registers offsets the shards still hold;
- a topic-scoped shard key, which would restore the cross-partition offset collision
  `ShardKey.KeyOrderedKey` was written to prevent;
- a second insertion site on `workMap`, or `addWorkContainer` reached from a second thread, either of
  which breaks L1;
- a staleness transition that stops carrying its sweep - or a new one, joining T3, that never had
  one.

## How it was established

`ShardDisplacementOrphanReachabilityTest`, three arms, deliberately control-armed because a single
green arm here proves nothing - it is green whether the sweep works or the branch is simply never
reached.

- **The disproof.** Build (resident AND queued AND not stale) through the production failure path,
  run the production rebalance, and assert `A` has left **both** structures before the poll that
  could supply a replacement can run. Then register the redelivery and assert it is an ordinary
  insertion.
- **Control - the branch does fire.** confluentinc#909's late drain: a stale container inserted after
  both sweeps have passed, then displaced by a fresh one. Asserts the displacement happened *and*
  that the retry queue was empty throughout. Without this the first arm's green is ambiguous.
- **Positive control - the orphan, and the one missing input.** Reach (stale AND resident AND queued)
  through the production API using T3, the fence - no planting - then inject the single arrival
  production cannot supply. The orphan appears immediately, and the assertion is by *reference*: the
  queue holds the displaced instance while the shard holds its replacement. This is what makes the
  first arm's assertions demonstrably able to see an orphan.

**Ablation, with predictions stated before the run - the two sweeps are redundant with each other,
and together they are the responsible term.** Arm one asserts that the revocation clears both
structures; the question is whether it can fail at all.

| Ablated in `PartitionStateManager` | Predicted | Observed |
|---|---|---|
| `sm.removeStaleContainers()` alone | green - `removeAnyShardEntriesReferencedFrom` covers it | green |
| `partition.onPartitionsRemoved(sm)` alone | green - `removeStaleContainers()` covers it | green |
| both | **red** | **red**, at `the revocation must have taken the container out of its shard`, `expected: null but was: WorkContainer(tp:myTopic-0:o:0:k:key-0)` |

Same magnitude, different position: neither sweep alone is what makes the arm green, and removing
both flips exactly the one assertion the argument turns on. An arm that could not be made to fail
would be asserting nothing.

**What was refuted on the way.** The first hypothesis was that the epoch bump itself opens the
window - bump on the poll thread, sweep a few instructions later, control thread free-running in
between. It does not, because the bump changes no staleness answer (L4); the window opens at the
state *replacement*, which is the same statement as the sweep that closes it. The second was that
KEY ordering could collide two partitions' records on one offset inside a shard; `ShardKey` already
prevents that and says so.

**What is NOT claimed.** No load-level reproduction was attempted and none is needed: the result is
a disproof, and a soak that finds nothing is the expected outcome whether the argument holds or not.
Nothing here says the pairing gap should be left unpaired if the branch is ever touched - it says the
gap costs nothing today, and names what would change that.
