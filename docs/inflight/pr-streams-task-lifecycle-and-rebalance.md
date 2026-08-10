# Rebalance under PC dispatch: one shape proven, and the shapes that are not

For `parallel-consumer-streams` (astubbs#255), and **relevant to the Connect module too** - it wraps a
task whose lifecycle the framework drives the same way, so most of these questions transfer.

This entry used to list seven open lifecycle divergences. U10 closed or re-recorded all seven, so the
old list is now history and lives in
`docs/plans/2026-08-11-001-feat-ks-streams-task-lifecycle-and-rebalance-plan.md`. **What is left is the
rebalance surface, which is larger than the part that was tested**, and that is what this entry is now
for.

## Resolved, and by what

| Divergence | Outcome |
|---|---|
| 1. Suspend-drain runs after the final commit | **Re-recorded.** The drain stays: `suspend()` is orderly teardown on the StreamThread, not the abandon-in-place revocation path. Duplicates are now *bounded and measured* rather than unmeasured. The predicate half of it was a real defect and was fixed - see below. |
| 2. A revived task keeps its closed dispatcher | **Still open, still loud.** `revive()` throws rather than silently re-creating, because a silent re-create restores a working dispatcher carrying none of the closed one's in-flight state. `bindToCurrentThread()` also refuses a closed dispatcher, closing the same hazard on the new hand-off path. |
| 3. `prepareRecycle()` never closes the dispatcher | **Fixed.** Routes through the same close call as `close(boolean)`. Four leaks per recycle - the static `ACTIVE` entry, the worker pool, the `PcWorkSignal` registration, the WorkManager partition state - pinned by `closingReleasesEveryResourceARecycleUsedToLeak`. |
| 4. A timed-out drain falls through to `closeTopology()` | **Covered by mechanism, not by new code.** Once the predicate counted in-flight work, a non-quiescent dispatcher reports uncommitted work, so `validateClean()` throws and Kafka's own `TaskManager` closes the task dirty. No guard was added, deliberately: an unnecessary second guard is worse than none. |
| 5. `updateInputPartitions()` never reaches the dispatcher | **Fixed.** `PcTaskDispatcher.updatePartitions()` revokes then assigns against PC's WorkManager - core's order, and the source of the epoch fence. Without it a cooperatively-gained partition had no epoch and every record was dropped silently. |
| 6. `onOffsetCommitSuccess` has no epoch guard | **Re-recorded as unreachable, with a guard instead of an assertion.** Both the acknowledgement and the partition update are owner-thread-only, so they cannot interleave. `theCommitAckAndThePartitionUpdateAreBothOwnerThreadOnly` fails loudly if a later change moves either off the StreamThread. |
| 7. Owner-thread guard binds at construction | **Fixed, and the premise was wrong.** The bind moved to hand-off and the wake signal moves with it. But "one owner thread per task" was never Kafka's model - `DefaultStateUpdater` calls `maybeCheckpoint` from its own thread. The real fix was splitting the dispatcher into a mutating surface (owner-thread-only) and a read-only surface (any thread), under the rule that **a question may not mutate**. |

**The defect that pile B actually found.** `pcAwareCommitNeeded()` asked *"is there **completed** work no
commit has covered"*. On the stock path that is the same question as *"is there uncommitted work"* -
processing is synchronous, so there is no third state. Asynchronous dispatch creates one, and
`validateClean()` is the caller that must see it: **a clean close while records were still inside the
processor chain succeeded silently**, where Kafka's contract is to throw `TaskMigratedException` so the
TaskManager closes dirty. Pile B went 4 failing to 1, with 5 cases fixed and 0 regressed.

## Do not "fix" `shouldThrowExceptionOnCloseCleanError`

**It fails on purpose, and it is a regression guard.** Someone will eventually see one red case in Kafka's
own suite and try to close it. This is the note that should stop them.

The test calls `postCommit(true)` and then `closeClean()`, but never `updateCommittedOffsets` - so no
commit success was ever acknowledged. Stock passes because `postCommit` ends in `clearCommitStatuses()`,
which clears the stock field. PC deliberately clears its dirty state **only** on the genuine success-only
seam, so `validateClean()` correctly refuses the clean close.

Making this test green requires acknowledging the commit in `postCommit`. That is precisely the
silent-data-loss defect recorded in
`docs/solutions/integration-issues/kafka-streams-task-lifecycle-callbacks-do-not-mean-what-they-are-named.md`:
Kafka reaches `postCommit` after a **swallowed commit failure** (`TaskManager.tryCloseCleanActiveTasks`)
and with **no commit attempted at all** (`closeDirtyAndRevive`). Acking there marks work durably
committed that the broker never accepted, and releases the state needed to redo it - with no error, no
retry and no log line.

So this red case is evidence that the earlier fix is still in place. If it ever goes green, check why
before celebrating.

## What is proven, precisely

`RebalanceUnderPcDispatchTest`: two `KafkaStreams` instances, one application id, a 4-partition topic,
the **cooperative** assignor, the second instance **joining** mid-run. Asserts no loss, capacity-bounded
duplicates, that the handover actually happened (via an `assign`+`seek` reader scoped past a boundary
captured when B starts, so it cannot be satisfied by pre-handover output), and that ownership moved
rather than being shared.

Four runs, all green: `produced=240 uniqueConsumed=240 totalOutputs=240 duplicates=0 bothInstances=0`.

**That is one shape of one event.** Everything below is a different shape.

## Open: the `duplicates=0` result is not yet evidence of correctness

The test permits 76 duplicates and measured **zero, four times running**. Two readings, and this entry
does not yet know which is right:

1. The frontier commit is genuinely doing its job and the handover window is clean.
2. **The rebalance is not landing where duplicates arise.** B joins after A has been running a while, and
   the work is 240 tiny records with no artificial processing cost - so the pool drains almost as fast as
   it fills, and there may be nothing in flight at the moment partitions are revoked.

Reading 2 is the more likely one, and the reason to suspect it is structural: **a test that never
approaches its own bound is not exercising the thing it bounds.** The bound is currently unfalsified
rather than satisfied.

The cheap discriminator is a control arm: give the topology a real per-record cost (as
`PcDrivenStreamsDispatchTest` does with `PROCESSING_COST`) so that records are demonstrably in flight
when B joins, and assert the in-flight count at the revocation instant is non-zero. If duplicates stay at
zero *with* work in flight, reading 1 is established. Until then, treat `duplicates=0` as "the window was
probably never entered", not as a correctness claim.

## Open: the rebalance shapes with no coverage at all

Roughly in value order. The first is the one that matters most.

1. **Revocation while records are in flight in the worker pool.** The highest-value gap, because it is
   where the seam is *structurally* different from stock: **Kafka does not know the worker pool exists.**
   Stock revokes a partition whose records are, by construction, either fully processed or untouched -
   there is no third state for `TaskManager` to reason about. Under PC there is, and the design answer
   (adopted from `parallel-consumer-core`) is to **abandon in-flight work behind an epoch fence** rather
   than drain or block: the late outcome is recognised as stale and dropped, and the new owner re-reads
   from the last commit. `anOutcomeForARevokedPartitionIsDroppedRatherThanCommitted` covers that at the
   dispatcher level, but **nothing covers it end-to-end through a real broker rebalance**, which is where
   the interaction with `suspend()`'s drain, the commit ordering and Kafka's own teardown lives.
2. **B leaving rather than joining.** Only the join path is tested. Departure is the path that runs
   `suspend()` under revocation, which is where divergence 1's duplicate window and divergence 4's drain
   timeout both live - so it exercises strictly more of the changed code than the join does.
3. **An instance crashing mid-rebalance.** Not a clean `close()`: a kill, so the group must fence and
   redistribute without a revocation callback ever running. `PcTaskDispatcher.abortAllActive()` is the
   existing crash-injection surface and `CommitFrontierCrashRestartTest` is the existing pattern; nobody
   has combined them with a second live instance.
4. **Repeated rebalances in sequence.** One handover proves the transition; it does not prove the state
   after it is a valid starting point for the next one. This is where a leaked dispatcher or a stale
   partition set would accumulate rather than merely occur - which is exactly what divergences 3 and 5
   were.
5. **The eager assignor.** Every assertion here is under `CooperativeStickyAssignor`, pinned at the call
   site. Eager revokes everything and re-assigns, so `updateInputPartitions` is not even the path taken -
   tasks are closed and rebuilt. Different code, zero coverage.
6. **Standby tasks and state restoration during handover.** Still the case that **no test configures
   standby replicas**, which is why the recycle leak was dormant for so long. Restoration during a
   handover also brings `DefaultStateUpdater` - a second thread on the task - into contact with a task
   that is changing hands, and the read-only surface added in divergence 7 is exactly what that thread
   uses.
7. **Repetition under load.** Four runs of 240 records is an **existence proof, not a reliability
   claim.** No statement about flake rate is supportable from it. The repeat count and the record volume
   both need to go up before "rebalance works" is a claim rather than an observation.

## Open: the upstream evidence base is behind one dependency decision

Kafka publishes 78 Streams integration tests, and **64 of them need `EmbeddedKafkaCluster`**, which pulls
in the Scala broker (`kafka.server.KafkaServer`) and `EmbeddedZookeeper`. This module runs on
Testcontainers, so adopting it means a second broker mechanism in one module.

That is **one decision unlocking a large evidence base**, and it was deliberately not taken as a side
effect of U10. Worth deciding on its own merits.

Note that the specific upstream rebalance test would not have covered the gap above anyway:
`RebalanceIntegrationTest` has a single case,
`shouldCommitAllTasksIfRevokedTaskTriggerPunctuation`, whose subject is punctuation-triggered commit on
revocation - not loss or duplicate accounting across a handover. The argument for the dependency is the
other 63 tests, not that one.

## Delete when

The in-flight-revocation gap has end-to-end coverage, the `duplicates=0` reading is settled by a control
arm with work demonstrably in flight, and departure and repeated-rebalance shapes are covered.
