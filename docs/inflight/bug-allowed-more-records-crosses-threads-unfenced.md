# `PartitionState.allowedMoreRecords` crosses threads with no fence

<!-- inflight-type: bug -->
<!-- inflight-impact: throughput -->
<!-- inflight-labels: concurrency -->

The offset-encoding back-pressure flag is a plain `boolean`, written on the broker-poll thread and
read on the control thread with no happens-before edge between them. A stale read decides whether
work is taken.

## The pair

**Writer, broker-poll thread.** `setAllowedMoreRecords` is reached only from `tryToEncodeOffsets`
and `updateBlockFromEncodingResult`, and those only from `getCommitDataIfDirty` -
`PartitionStateManager.collectDirtyCommitData` - `AbstractOffsetCommitter.retrieveOffsetsAndCommit`.
In both consumer commit modes that collection runs on the thread that claimed the committer:
`ConsumerOffsetCommitter.commit()` routes a non-owner's request through `commitRequestQueue` and
only the owning poll thread executes it. In `PERIODIC_TRANSACTIONAL_PRODUCER` the same collection
runs inline on the control thread through `ProducerManager`, so that mode is not exposed.

**Reader, control thread.** `couldBeTakenAsWork` reads it through `isAllowedMoreRecords()`, called
from `ProcessingShard.getWorkIfAvailable`. Nothing earlier on that path is an acquire load that the
writer thread later releases: `getPartitionState` is a `ConcurrentHashMap` lookup of a reference
published at assignment, and `checkIfWorkIsStale` reads `getPartitionsAssignmentEpoch` - a `final
long` captured at construction - plus `isPartitionRemovedOrNeverAssigned`.

## Why the `dirty` volatile does not cover it

`dirty` fences the opposite direction: its release store is `onSuccess` on the control thread and
its acquire load is `getCommitDataIfDirty` on the poll thread. This pair runs poll-to-control.

The poll thread does perform a release store of `dirty` after writing this field - `setClean` on
commit success - but the reader never performs the paired load. `couldBeTakenAsWork` does not touch
`dirty`, and the only control-thread load of it is `wm.isDirty()` inside `maybeAcquireCommitLock`,
which sits behind `isTimeToCommitNow()` in an `&&`, so on control-loop iterations that are not
commit-time it never executes. Even when it does, an edge exists only if that read observes the
poll thread's write rather than the control thread's own `setDirty(true)`. That is an incidental
edge, not a guaranteed one, and it is the shape this repository has been repeatedly wrong about.

## What a stale read costs, in both directions

- **Stale `true` where the writer set `false`.** The control thread keeps taking work for a
  partition already at the encoding pressure threshold - exactly the condition the flag exists to
  stop. The incomplete set keeps growing, and the next encode is closer to `DefaultMaxMetadataSize`,
  where `updateBlockFromEncodingResult` strips the payload and records are replayed on rebalance.
- **Stale `false` where the writer set `true`.** The partition takes no new work; only records
  below `offsetHighestSucceeded` get through, via the `isBlockingProgress` arm. It clears whenever
  the reader observes a later commit's write.

**Why `throughput` and not `stall`:** neither direction loses a record, and the blocked direction
self-clears rather than staying stopped. Under the JMM the window is unbounded, so `stall` is
defensible - the tag is a judgement, and the reasoning is here to be overridden rather than
re-derived.

## A scanner does see it; the recorded list does not

SpotBugs names this field under fb-contrib's `AT_STALE_THREAD_WRITE_OF_PRIMITIVE`, along with
`stateChangedSinceCommitStart` and `bootstrapPhase` in the same class. Reproduce with
`./mvnw -o spotbugs:spotbugs -pl :parallel-consumer-core` and read the SpotBugs XML report it
writes into that module's build output.

The offender list under that rule in [`docs/refactoring.md`](../refactoring.md) names
`AbstractParallelEoSStreamProcessor.lastWorkRequestWasFulfilled`,
`ConsumerManager.commitRequested` and `RetryQueue.closed`, and none of the `PartitionState` fields.
**That entry owns the list** - correcting it there is the fix, not restating the fields here. The
lane runs `spotbugs:check` with `-Dspotbugs.failOnError=false`, so the finding annotates and never
blocks: the signal was present and the ledger was wrong about it, which is worse than the analyser
having been silent.

## Two more fields in the same class, same class of defect

Named because the sweep found them, not diagnosed here:

- **`stateChangedSinceCommitStart`** is plain and written from BOTH threads - `setDirty` on the
  control thread, and cleared in `getCommitDataIfDirty` on the poll thread - and read by `setClean`
  on the poll thread. A missed control-thread write lets a commit be marked clean over state that
  changed during the commit window, which is the burnt-commit-cycle shape `dirty` was fenced
  against. This one is a lost-update risk as well as a visibility one, so `volatile` alone is not
  obviously the fix.
- **`bootstrapPhase`** is flagged by the same rule and has not been walked.

## Settling it, and what not to do

The honest instrument is a probe pair in `jcstress-poc/` modelling
encode-writes-then-commit against take-work-reads, in the shape `CommitPathVisibilityProbes`
already uses for the `dirty` pair - `docs/inflight/test-jcstress-probe-module-open-items.md` records
that nothing binds those probes to the real code, which applies to any new one too.

Before fixing piecemeal, read `docs/inflight/core-control-thread-contract-debts.md` and
[`docs/refactoring.md`](../refactoring.md)'s caution that these fields may be absorbed by the
shared-nothing rework (confluentinc#200, mirror astubbs#142) rather than fixed one at a time.

<!-- post-merge: checked-begin -->
Found during merge prep for astubbs#349, which fenced `PartitionState.dirty` against measured
jcstress evidence and deliberately left this field alone: there is no equivalent measurement for
this pair and the reader path is a different one, so fencing it in that change would have shipped an
unmeasured edit under a measured one's justification.
<!-- post-merge: checked-end -->
