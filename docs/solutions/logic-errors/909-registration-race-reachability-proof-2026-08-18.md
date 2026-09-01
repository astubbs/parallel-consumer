---
title: "Proving the confluentinc#909 registration race is REACHABLE - two unsynchronised threads, not a structural bug (confluentinc#909)"
date: 2026-08-18
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: internal / work-state
symptoms:
  - "A causal story that reads as complete but was never checked for reachability"
  - "Two successive root-cause explanations for the same defect, each retracted"
tags:
  - rebalance
  - epoch-fencing
  - check-then-act
  - root-cause-method
---

Companion to
[`stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md`](stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md),
which owns the symptom and the fix. That record is dated and stands as written; this one records
what was **established later** - the reachability proof the original write-up assumed, and the two
wrong mechanisms asserted on the way to it.

## What was actually proven, and how

The defect is a **check-then-act race between two threads with nothing serialising them**. Every
link below was traced in source rather than inferred.

- **Rebalance callbacks run on the broker-poll thread.** The PC registers itself as the
  `ConsumerRebalanceListener` via `consumer.subscribe`, and Kafka invokes those callbacks only
  inside `poll()`. `poll()` is reached only through `BrokerPollSystem`, whose loop names its thread
  `pc-broker-poll`. Those callbacks do the damage: `incrementPartitionAssignmentEpoch`, installing a
  new `PartitionState`, and `removeStaleContainers`.
- **Registration runs on the control thread.** The poll thread's `registerWork` only does
  `workMailBox.add` - the real `wm.registerWork` happens at the mailbox drain, on `pc-control`.
- **Nothing serialises the two.** `WorkManager` contains no `synchronized`, no `Lock`, no `Atomic`.
  The concurrent collections in the state package give visibility, not atomicity.
- **The gap is the loop.** `maybeRegisterNewRecordAsWork` looks the state up live
  (`partitionStates.get`), holds it in a local, and `maybeRegisterNewPollBatchAsWork` runs
  `epochIsStale` **once** before looping. Each iteration inserts through `getShardManager()`, which
  resolves the **live** `ShardManager`, while `addNewIncompleteRecord` writes to a state object that
  may already be orphaned.

**Two sub-cases, both real:**

1. The looked-up state has itself gone stale. `partitionsAssignmentEpoch` is a `final long` set at
   construction, so it compares its own old epoch against the batch's old epoch, they match, and the
   guard passes. This is what the regression test drives.
2. The guard passes **legitimately** against the then-live state, and the rebalance lands mid-loop -
   after the check, before the remaining inserts. This is the timeline the upstream reporter
   described.

They are the same race; (1) only widens how late a doomed batch can still be waved through. The fix
is indifferent to which occurred, because it acts at the insert.

## The dead end, recorded so it is not re-walked

`EpochAndRecordsMap` carries a comment about *"poll() returned records for a partition before
`onPartitionsAssigned()`"*, and `EpochAndRecordsMapRaceTest` covers it. **That is a different race.**
It concerns a **null** epoch - records arriving before any epoch exists - and is handled safely by
skip-and-redeliver, nothing having been committed. confluentinc#909 requires a **non-null but stale**
epoch. Different precondition, different consequence, different handling.

## The method failure this record exists for

Two explanations were asserted before this one, and both were retracted:

- *"The guard cannot fence, the race is structural."* Wrong: `maybeRegisterNewRecordAsWork` looks the
  state up **live**, so a rebalance landing before the lookup fences correctly.
- *"It treats the symptom, not the cause."* Wrong: no check earlier than the insert can close a
  check-then-act window, and the architecture tolerates stale entries by idiom.

Both were derived by reading structure and neither was checked for **reachability** - whether the
interleaving the story required could actually occur given the threading model. Reading which
objects hold which fields tells you what a race *would* look like; it tells you nothing about
whether two threads can ever be in those positions at once.

**The check that settles it is cheap and was skipped twice: name the threads, then look for what
serialises them.** Here that was four greps - the thread names, the mailbox hop, the lock count in
`WorkManager`, and where the rebalance callbacks are invoked from.

## What remains unproven

The reporter's exact interleaving is their own reconstruction, offered as such. What is established
is that **the interleaving is permitted by the code**, and that the production evidence they gave -
one offset never completed, commit pinned behind it, cured only by a restart - matches the predicted
signature. That is corroboration, not proof that this race is what bit them.

There is still **no load-level reproduction**. The chaos suite already asserts the invariant this
defect violates (per-record completeness over unique keys, in `ChaosScenarioBase`), but it is green
on master **with the defect present** - so no existing soak reproduces it. A calibrated A/B arm
remains open work.
