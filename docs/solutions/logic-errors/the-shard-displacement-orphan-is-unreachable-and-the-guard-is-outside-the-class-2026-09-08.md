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

**The same single-writer fact is now recorded on master from the other direction**, and finding it
twice independently is the strongest thing this leg has. astubbs#468 merged making `WorkContainer`
equality reference identity and every shard removal a compare-and-remove
(`ProcessingShard.evictIfStillResident`), and it clears the same suspicion at
`getWorkIfAvailable`'s last-resort sweep with the same discriminator: `addWorkContainer` is the only
writer of `workMap` outside tests and runs on the controller, which is also that scan's thread. **So
the two cleared suspicions share a single reopening condition** - anything that puts into a shard
off the controller thread - and neither has a gate. That is the coincidence worth knowing: a change
nobody would think of as touching either one invalidates both at once.

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

**L5 - T1 and T2 each carry a sweep that takes `A` out of its SHARD, on the same thread, before the
callback returns.** T1's swap is immediately followed in the same loop iteration by
`partition.onPartitionsRemoved(sm)` - `sm.removeAnyShardEntriesReferencedFrom(incompleteOffsets.values())` -
and `onPartitionsRemoved` ends with `sm.removeStaleContainers()`. T2 is the last statement before
`sm.removeStaleContainers()` in `onPartitionsAssigned`. Both sweeps reach `A`: `processingShards` is
never replaced (`ShardMapIsNeverReplacedArchTest` pins it), shards are only ever removed when empty,
and both iterators are weakly consistent, which still guarantees every element present for the whole
traversal.

**Residence is all this leg needs, which is why astubbs#481 does not move it.** That PR takes the
rebalance callbacks off the retry queue entirely - they remove from the shards only, and
`ShardManager.purgeDepartedRetryEntries()` collects departed entries on the controller thread a tick
later. So these sweeps no longer remove `A`'s queue entry, and for one control-loop tick a
queue-only entry survives the callback. **It survives with `A` resident in no shard**, and the
displacement branch requires a *resident* to displace, so that window is not a way in. The argument
above was originally written as "removes it from both structures"; the queue half was never the part
carrying it.

**And astubbs#481 is a second, independent answer to the same question, from the other direction.**
Even if the branch did orphan an entry, the purge collects it within one control-loop tick, in every
ordering mode. Bound and unreachability are worth keeping separately: the bound holds whatever
happens, and the proof says the case does not arise.

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

**And it is ordering-mode independent, which the review of astubbs#483 asked to have stated rather
than assumed.** Legs L1-L6 never consult the shard key: they are about the inserting thread, the
retry queue's single add site, the finality of two epoch fields, and which callbacks sweep - none of
which varies with `ProcessingOrder`. **The mode is visible in exactly one place, L7's second half**,
and it is safe in every mode for the same underlying reason: a shard is partition-scoped throughout.
Under `PARTITION` and `UNORDERED` the shard *is* a topic-partition, so an offset identifies one
record by construction; under `KEY` the shard is topic-plus-partition-plus-key, so it is
partition-scoped too and the offset is again unique within it. A topic-scoped key would break that
half - and only that half - by letting two records collide on one offset with **no re-assignment
anywhere in the story**, which is the leg the whole argument leans on.

`ShardDisplacementOrphanReachabilityTest` now says this rather than asserting it: its first arm is
parameterised over every `ProcessingOrder`, and
`underKeyOrderingOneOffsetOnTwoPartitionsDoesNotCollideInOneShard` exercises the `KEY` half directly.
That arm was verified by sabotage per `docs/testing-at-write-time.md` - making `ShardKey`'s
key-ordered form topic-scoped turns exactly that arm red, on the assertion that carries the leg.

The one behavioural difference between the modes is benign here, and worth naming so it is not
mistaken for a gap: under `KEY`, `removeShardIfEmpty` garbage-collects an emptied shard, where the
other two keep the object. That makes the conclusion **stronger** under `KEY`, not weaker - a
collected shard holds no resident at all, so there is nothing to displace.

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

**Re-run after merging astubbs#481, and again after astubbs#468 landed on master**, because both
change what those sweeps do - astubbs#481 makes them shard-only, astubbs#468 makes each removal a
compare-and-remove on the container the sweep inspected - and an ablation measured against a shape
that no longer exists is evidence about nothing. Same result all three times, same assertion: either
sweep alone green, both red. That is the expected outcome and it is stated because it was not
obvious in advance - the arm asserts residence, which is the half both PRs leave untouched.

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
