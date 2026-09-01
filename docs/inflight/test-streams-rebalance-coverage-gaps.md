# Rebalance under PC dispatch: one shape proven, and the shapes that are not

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

For `parallel-consumer-streams` (astubbs#255), and **relevant to the Connect module too** - it wraps a
task whose lifecycle the framework drives the same way, so most of these questions transfer.

The module's lifecycle work gave `PcTaskDispatcher` a life beyond the task instance it was built with:
partitions that follow a cooperative rebalance, a teardown the recycle path can no longer bypass, and a
replacement dispatcher on revival. What is open is the *rebalance surface*, which is larger than the part
that got tested.

## Proven

`RebalanceUnderPcDispatchTest`: two `KafkaStreams` instances, one application id, a 4-partition topic, the
**cooperative** assignor, the second instance **joining** mid-run. No loss, capacity-bounded duplicates, a
handover that demonstrably happened, ownership that moved rather than being shared.

That is one shape of one event. Everything below is a different shape.

## Do not "fix" `shouldThrowExceptionOnCloseCleanError`

It is red in Kafka's own suite with the seam on, and **it fails on purpose**. Someone will eventually try
to close it; this is the note that should stop them.

The test calls `postCommit(true)` then `closeClean()`, but never `updateCommittedOffsets` - so no commit
success was ever acknowledged. Stock passes because `postCommit` ends in `clearCommitStatuses()`, which
clears the stock field. PC clears its dirty state only on the genuine success-only seam, so
`validateClean()` correctly refuses the clean close, and now says so with `TaskMigratedException` rather
than letting the close through.

Making it green requires acknowledging the commit in `postCommit`, and that is a silent-data-loss defect
rather than a fix: **Kafka reaches `postCommit` after a swallowed commit failure, and with no commit
attempted at all.** Work would be marked durably committed while the broker still holds an older offset.
If this case ever goes green, find out why before celebrating.

The full write-up of that finding - every call site audited, and the general rule about trusting a
framework hook's *name* - is `docs/solutions/integration-issues/kafka-streams-task-lifecycle-callbacks-do-not-mean-what-they-are-named.md`
on the unmerged branch `feats/ks-streams-task-lifecycle-and-rebalance` (`git show 3770afe24:<path>`). It
was not carried onto this stack: astubbs/parallel-consumer#312 recovered part of that learnings set to
master and this was not in that part, and porting it here would mean repairing its own citations into
plan documents that are equally unmerged.
<!-- file-refs: N/A - names a document that deliberately lives only on an unmerged branch, with the command to read it -->

## Not carried, and where to find it

The same applies to `docs/solutions/test-issues/a-restart-assertion-satisfiable-by-pre-crash-data-proves-nothing.md`
at the same commit. It is the reason `RebalanceUnderPcDispatchTest` scopes its handover reader with
`assign` plus `seek` instead of subscribing from earliest: a crash-restart assertion in this module was
once satisfiable by data the *pre*-crash phase had already produced, so it was green whether or not the
mechanism under test existed. Whoever next recovers learnings from that branch should take both.
<!-- file-refs: N/A - same, a document on an unmerged branch named so the pointer is not lost -->


## Open: `bindToCurrentThread()` has no production caller

The owner-thread guard can now follow a task to a new thread, and refuses to bind a closed dispatcher. But
**nothing calls the rebind**: in Kafka 3.9.2 a reassigned task is closed and rebuilt rather than handed
across threads, so the constructor's bind is the only one that happens. Four independent reviewers flagged
this as an overclaim in the first draft of this work.

It is not dead code - it removes an unstated assumption, and the cross-thread hazard that actually bit
(the state updater calling `maybeCheckpoint`) is handled by the dispatcher's query surface instead.
But treat it as unexercised capability, not a closed gap. If a real hand-off point is ever identified,
wire it there; until then no test can prove more than that the method works when called directly.

**The module's unit suite structurally cannot catch this class.** It has no Kafka Streams in it, so no
second thread ever asks the dispatcher anything - which is why the state-updater defect reached the
integration suite before anything went red. Cross-thread properties here need either a test that drives a
foreign thread by hand, or an integration arm.

## Open: the zero-duplicate result is not yet evidence of correctness

The test permits a capacity-derived duplicate bound and has measured **zero every time it has been run**,
including on the branch that first opened this note. Two readings:

1. The frontier commit is doing its job and the handover window is clean.
2. **The rebalance is not landing where duplicates arise.** The topology has no per-record cost, so the
   pool drains about as fast as it fills and there may be nothing in flight when partitions are revoked.

Reading 2 is the more likely one, and the reason to suspect it is structural: **a test that never
approaches its own bound is not exercising the thing it bounds.** The bound is unfalsified, not satisfied.

The discriminator is a control arm: give the topology a real per-record cost (as
`PcDrivenStreamsDispatchTest` does with `PROCESSING_COST`) so records are demonstrably in flight when the
second instance joins, and assert the in-flight count at the revocation instant is non-zero.

## Open: the rebalance shapes with no coverage at all

Roughly in value order.

1. **Revocation while records are in flight in the worker pool.** The highest-value gap, because it is
   where the seam is *structurally* different from stock: **Kafka does not know the worker pool exists.**
   Stock revokes a partition whose records are either fully processed or untouched - there is no third
   state for `TaskManager` to reason about. Under PC there is, and the design answer (taken from
   `parallel-consumer-core`) is to abandon in-flight work behind an epoch fence. That is covered at the
   dispatcher level; nothing covers it end-to-end through a real broker rebalance, where it meets
   `suspend()`'s drain, the commit ordering, and Kafka's own teardown.
2. **An instance leaving rather than joining.** Only the join path is tested. Departure runs `suspend()`
   under revocation, so it exercises strictly more of the changed code than the join does.
3. **An instance crashing mid-rebalance.** A kill, not a clean close, so the group must fence and
   redistribute with no revocation callback. `PcTaskDispatcher.abortAllActive()` is the existing piece;
   nobody has combined it with a second live instance.
4. **Repeated rebalances in sequence.** One handover proves the transition, not that the state after it is
   a valid start for the next. This is where a leaked dispatcher or stale partition set would accumulate
   rather than merely occur.
5. **The eager assignor.** Everything here is under `CooperativeStickyAssignor`. Eager revokes everything
   and rebuilds tasks, so `updateInputPartitions` is not even the path taken.
6. **Standby tasks and state restoration during handover.** No test configures standby replicas, which is
   why the recycle leak stayed dormant. Restoration also brings `DefaultStateUpdater` - a second thread on
   the task - into contact with a task that is changing hands.
7. **Repetition under load.** A handful of runs of a few hundred records is an existence proof, not a
   reliability claim.

## Open: no test drives `prepareRecycle` through a real task

`prepareRecycle` now closes the dispatcher, and `closingReleasesEveryResourceARecycleUsedToLeak` pins the
close contract it invokes - but nothing asserts the wiring itself. This does **not** need standby
replicas, contrary to the first assumption: Kafka's own `shouldPrepareRecycleSuspendedTask` reaches
`prepareRecycle` on a SUSPENDED task, so the same scaffolding would do.

## Open: the upstream evidence base is behind one dependency decision

Kafka publishes a large Streams integration suite, and most of it needs `EmbeddedKafkaCluster`, which
pulls in the Scala broker and ZooKeeper. This module runs on Testcontainers, so adopting it means a second
broker mechanism in one module. One decision unlocking a large evidence base, deliberately not taken as a
side effect of the lifecycle work. Count the affected classes with a grep over the published test-sources
jar rather than trusting a number written here.

The specific upstream rebalance test would not have covered the gaps above anyway:
`RebalanceIntegrationTest` has a single case about punctuation-triggered commit on revocation. The
argument for the dependency is the rest of the suite.

## Also open, smaller

- **`suspend()` drains for up to 30s inside a revocation callback, and `prepareRecycle`/`close` can then
  wait another 30s** on the same worker pool with no way to know the first drain already gave up. Inside
  `max.poll.interval.ms`, far outside the 15s dwell bound core's `ProgressProbe` treats as healthy.
- **`bindToCurrentThread()` is not safe against two threads racing to bind.** Unreachable today (no
  caller), but the read-then-two-writes sequence is unsynchronised.

## Delete when

The in-flight-revocation gap has end-to-end coverage, the zero-duplicate reading is settled by a control
arm with work demonstrably in flight, and the departure and repeated-rebalance shapes are covered.
