# The Kafka Streams task lifecycle under PC dispatch: four of seven divergences closed, three re-recorded

For `parallel-consumer-streams` (astubbs#255), and **relevant to the Connect module too** - it wraps a
task whose lifecycle the framework drives the same way, so most of these questions transfer.

Recorded here rather than only in the plan because a downstream branch scans this directory and will
not read another branch's plan. Planned as U10 (taken together with pile B, the close/suspend/recycle
failures in Kafka's own suite).

## What is proven now

**Multi-partition, multi-instance, one rebalance.** `RebalanceUnderPcDispatchTest` runs two
`KafkaStreams` instances in one application id over a 4-partition topic with cooperative assignment, the
second joining mid-run, and asserts no loss, capacity-bounded duplicates, that the handover actually
happened, and that ownership moved rather than being shared. That replaces the previous honest headline
("one partition, one task, one instance") for the rebalance case specifically.

**Still unexercised:** standby replicas, task recycling end-to-end, and revival. See below.

## Pile B, measured rather than assumed

The plan called pile B "the ready-made check on whether the lifecycle fixes landed" and listed five
cases. Measured on this branch with the seam ON, before any change: **four failing, not five** -
`shouldThrowIfRecyclingDirtyTask` already passed.

More importantly, **the four were not close/suspend bugs**. Every one failed at an assertion that runs
*before* any close or suspend code, on `assertTrue(task.commitNeeded())` immediately after
`task.process(...)`, or on the `prepareCommit` that gates on the same predicate. They were the
asynchronous-dispatch artefact already diagnosed for pile A.

That relocated the defect rather than dissolving it - see divergence 1.

| | Before | After |
|---|---|---|
| Pile B failing | 4 | **1** |
| `StreamTaskTest` failing, seam ON | 35 / 101 | **30 / 101** |
| Cases fixed | - | 5 |
| Cases regressed | - | **0** |

The one remaining pile-B case is `shouldThrowExceptionOnCloseCleanError`, and it is now failing *later*
than it was: `commitNeeded()` passes, and `closeClean()` throws `TaskMigratedException` from
`validateClean()` where the test wants `ProcessorStateException`. That is **by design, and the design is
load-bearing**. The test reaches `closeClean` having called `postCommit(true)` but never
`updateCommittedOffsets` - so no commit success was ever acknowledged. Stock passes because `postCommit`
ends in `clearCommitStatuses()`, which clears the stock field. PC deliberately clears only on the genuine
success-only seam, and making this test pass would mean acknowledging on `postCommit` - which is exactly
the silent-data-loss defect recorded in
`docs/solutions/integration-issues/kafka-streams-task-lifecycle-callbacks-do-not-mean-what-they-are-named.md`.
**This failure is evidence that fix is still in place.**

## The seven divergences

1. **The suspend-drain pump runs after every flow's final commit.** ~~Duplicates on routine rebalances.~~
   **Re-recorded, with a measurement.** The drain stays: `suspend()` is the orderly teardown on the
   StreamThread, where waiting is legitimate and bounded, not the abandon-in-place revocation path.
   Drained output post-dating the final commit produces at-least-once duplicates, and
   `RebalanceUnderPcDispatchTest` now bounds them by capacity rather than leaving them unmeasured.
   Moving the drain ahead of the commit is a teardown-ordering change in Kafka's own `TaskManager`,
   outside this patch surface.

   **What this investigation did fix in the same area** is the predicate the teardown gates on.
   `pcAwareCommitNeeded()` asked "is there *completed* work no commit has covered". On the stock path
   that is the same question as "is there uncommitted work", because processing is synchronous and there
   is no third state. Under asynchronous dispatch there is, and `validateClean()` is exactly the caller
   that must see it: **a clean close while records were still inside the processor chain succeeded
   silently**, where Kafka's contract is to throw `TaskMigratedException` so the TaskManager closes the
   task dirty. Now `PcTaskDispatcher.hasUncommittedWork()` counts in-flight work, and the four pile-B
   cases moved on the strength of it.

2. **A revived task keeps its closed dispatcher.** **Still open, still loud.** Patched
   `StreamTask.revive()` throws rather than accepting records into a dispatcher that will never hand them
   out. Recreating the dispatcher is the real fix and was not attempted here: a silent re-create restores
   a *working* dispatcher carrying none of the closed one's in-flight state, which converts a diagnosable
   crash into another silent loss. `PcTaskDispatcher.bindToCurrentThread()` now refuses to bind a closed
   dispatcher, which closes the same hazard on the new hand-off path before it could open.

3. **`prepareRecycle()` never closes the dispatcher.** **Fixed.** It now routes through the same
   `TaskManager.executeAndMaybeSwallow(..., pcDispatcher::close, ...)` call `close(boolean)` makes, so
   the two teardown routes cannot drift apart again. Four things leaked per recycle - the static `ACTIVE`
   registry entry, the worker pool, the `PcWorkSignal` registration, and the WorkManager's partition
   state - and `closingReleasesEveryResourceARecycleUsedToLeak` pins all four, because a teardown that
   forgets one is only caught by whichever the author did not think about.

   **Honest limit:** no test drives a real active-to-standby recycle, because no test configures standby
   replicas - which is precisely why this stayed dormant. What is tested is the contract the recycle path
   now invokes.

4. **A timed-out drain falls through to `closeTopology()`.** **Re-recorded as covered, by mechanism
   rather than by new code.** With divergence 1's predicate fixed, a dispatcher that did not quiesce
   reports `hasUncommittedWork() == true`, so `validateClean()` throws and Kafka's own `TaskManager`
   closes the task dirty. No new guard was added, because the existing machinery now reaches the right
   outcome and an unnecessary second guard is worse than none.

   **Residual, and it is real:** `PC_DRAIN_TIMEOUT` is 30 seconds and `suspend()` runs *inside* the
   revocation callback, so a stuck worker blocks the rebalance for 30s. That is inside
   `max.poll.interval.ms` (300s) but far outside the 15s dwell bound core's `ProgressProbe` treats as
   healthy. Lowering it trades duplicate-freedom for rebalance latency, which is a product call.

5. **`updateInputPartitions()` never reaches the dispatcher.** **Fixed.** It now calls
   `PcTaskDispatcher.updatePartitions()`, which revokes then assigns against PC's `WorkManager` - core's
   own order, and the mechanism that supplies the epoch fence. Without it a partition gained by a
   cooperative rebalance had no assignment epoch, so `EpochAndRecordsMap` dropped every one of its
   records: zero registered, no exception, a topology that just looks idle. Tested in both directions,
   including that an identical partition set is a no-op rather than a spurious epoch bump.

6. **`onOffsetCommitSuccess` has no epoch guard.** **Re-recorded as unreachable, now with a guard rather
   than an assertion.** Divergence 5 changes a live dispatcher's partition set, which is exactly what
   could have made this reachable, so it needed showing rather than assuming. Both
   `updateCommittedOffsets` (which acknowledges) and `updateInputPartitions` (which revokes) run on the
   StreamThread, and the owner-thread guard enforces that, so the two cannot interleave and no
   acknowledgement can cross a revocation boundary.
   `theCommitAckAndThePartitionUpdateAreBothOwnerThreadOnly` pins both halves, so a later change that
   moves either call off the StreamThread fails loudly instead of silently reopening this.

7. **The dispatcher's owner-thread guard binds at construction.** **Fixed, and the premise was wrong in a
   way worth recording.** The bind moved to `bindToCurrentThread()`, called from the constructor and
   again wherever a task is handed to a thread; the `PcWorkSignal` registration moves with it, because
   moving the guard alone yields a dispatcher whose guard admits the new thread while its wake still goes
   to the old one - a stall rather than an exception, and therefore worse.

   **But "one owner thread per task" was never the real model.** Kafka Streams' `DefaultStateUpdater`
   calls `StreamTask.maybeCheckpoint` *from its own thread* for restoring and standby tasks. So the
   correct model is two surfaces, now stated in `PcTaskDispatcher`'s class javadoc: a **mutating** surface
   that is owner-thread-only because it touches the non-thread-safe `WorkManager`, and a **read-only**
   surface answerable from any thread. The rule that keeps them apart is that **a question is not allowed
   to mutate** - a query that drained the completion mailbox "just to be accurate" is how a plain field
   read became a cross-thread write.

## What this blocks, and what it does not

**Not the technical preview.** The preview's contract is at-least-once inside a stated envelope, and
rebalance duplicates sit inside that contract. What the preview owes is **disclosure** - and it can now
say more than "unexercised": one rebalance shape is exercised and its duplicate window is bounded.

**Production, still no.** Standby replicas, recycling and revival remain untested or refused.

## Delete when

Divergences 2 (revival) and 3's standby-recycle arm are covered end-to-end, and the drain-timeout
residual in 4 has a decided answer.
