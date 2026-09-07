---
title: "The revoke-path commit did not drain the mailbox - and declining, the first proposed fix, was refuted by experiment before it was written"
date: 2026-09-07
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: transactional_commit
root_cause: commit_path_skipped_the_drain_the_other_commit_path_relied_on
resolution_type: code_fix
severity: high
symptoms:
  - "In PERIODIC_TRANSACTIONAL_PRODUCER mode a rebalance can publish a transaction whose committed offsets omit a record that transaction contains - the output is committed, the input offset is not"
  - "The next owner of the partition reprocesses that input and produces the output again: a duplicated result visible to a read_committed consumer, exactly-once degraded to at-least-once on the revoke path"
  - "ProducerManagerTest#aRevokeTimeCommitIncludesTheOffsetOfEveryRecordItAlreadyProduced red 5/5, sending offset 1 where 2 is required, with the control arm green"
  - "With the drain fixed and nothing else, RebalanceEoSDeadlockTest still read two or three duplicated results per rebalance from the output topic with a read_committed consumer, 5/5 - a second, narrower window between the revocation commit releasing the produce lock and the truncation"
applies_when:
  - Two commit initiators share a sequence and one of them skips a step the other treats as load-bearing
  - A rebalance callback runs on a thread that does not own the state it needs to complete correctly
  - A proposed fix rests on one unverified question, and the cheapest experiment is to ask the question before writing the fix
  - Choosing between draining on the wrong thread, declining, aborting, and delegating at a two-thread commit seam
tags:
  - exactly-once
  - transactions
  - rebalance
  - revoke
  - mailbox-drain
  - thread-confinement
  - controlled-experiment
  - claim-register
  - issue-436
related_components:
  - AbstractParallelEoSStreamProcessor
  - ProducerManager
  - WorkManager
  - PartitionStateManager
  - TransactionalClaim
  - RebalanceEoSDeadlockTest
---

# The revoke-path commit did not drain the mailbox - and declining, the first proposed fix, was refuted by experiment before it was written

## Context

astubbs/parallel-consumer#436 (merged 2026-09-07) was a diagnosis, deliberately without a fix. It
proved that in transactional mode the revocation-time commit published transactions whose offsets
omitted records those transactions contained, refuted claims C9 and C4 of the transactional claim
register, quarantined the red arm with a passing control, and put a `CAUTION` in the README. This
write-up records the defect, why the first fix position was wrong, the experiment that showed it,
and the fix that landed.

## The defect

The control loop commits like this: take the producer write lock, **drain the work mailbox**, then
collect offsets and commit. Draining first is what makes the transactional guarantee hold. A record
produced into the open transaction has its success sitting in the controller's mailbox before its
produce lock is released - `cleanUpContext` is the single release point and runs in
`runUserFunction`'s `finally`, after the batch is mailboxed - and only the drain
(`processWorkCompleteMailBox` -> `WorkManager#handleFutureResult` -> `PartitionState#onSuccess`)
marks the partition dirty so that `collectCommitDataForDirtyPartitions` includes the offset.

The revoke path (`onPartitionsRevoked` -> `tryCommitOffsetsOnRevoke`) took the same lock and never
drained. So a revoke-time commit could publish a transaction whose offsets omitted work whose
success was still queued. Output committed, input offset not, and the next owner of the partition
reprocesses the input and produces the output again.

**The reachability argument in the first version of the diagnosis was backwards, and that is the
most reusable lesson here.** "The produce read lock is held across the send and its acks, and a
commit cannot start while any read lock is held, so the mailbox may be empty of produced work
whenever a commit can begin" sounds right and is inverted: the ordering is send -> mailbox ->
release lock, so a returned produce lock *guarantees* the work is already queued. The lock
discipline does not close the window. It guarantees the window is open whenever a commit is granted
the write lock.

## The experiments, in the order they were run

All in `ProducerManagerTest`, against the hand-driven control loop and a mocked producer - no
broker, no load, no timing, so each arm is deterministic.

**1. The defect, with its control (astubbs#436).** Offset 0 produced and drained (partition dirty),
offset 1 produced into the same open transaction and its success left undrained, every produce
lock returned. Prediction: the revoke-time commit sends `{partition -> 1}` rather than `2`.

| Arm | One term changed | Result |
|---|---|---|
| `aRevokeTimeCommitIncludesTheOffsetOfEveryRecordItAlreadyProduced` | revoke commits as it did | **RED**, sends 1 |
| `aRevokeTimeCommitIncludesThatOffsetWhenTheMailboxIsDrainedFirst` | `processWorkCompleteMailBox(ZERO)` inserted before the revoke | **GREEN**, sends 2 |

**2. Abort or defer - the question the first fix position rested on.** The recorded position at
astubbs#436's merge prep was to *decline* the commit unconditionally in transactional mode: no
cross-thread mutation, no gap for a poll-thread emptiness check to land in, and a decline branch
that already existed. The note said in terms that this rested on one unverified question: when the
revoke commits nothing, what becomes of the open transaction's already-produced output? If aborted,
exactly-once is preserved and the decline is correct. If committed later by the control thread
without the revoked partition's offset, the decline is the same defect through a different door.

Prediction, stated before the run: **defer**. Nothing on the revoke path aborts - the only caller of
`abortTransaction` in main is `ProducerManager#close`.

| Arm | Wrapper calls, in order | Offsets the next commit sent |
|---|---|---|
| `afterARevokeThatCommitsNothingTheNextCommitPublishesTheRevokedPartitionsOutputWithoutItsOffset` | begin, send, send, send, sendOffsets, commit | surviving partition only |
| `anAbortAfterTheRevokeKeepsTheRevokedPartitionsOutputOutOfTheNextCommit` (one term changed: an abort after the revoke) | begin, send, send, abort, begin, send, sendOffsets, commit | surviving partition only |

In the first arm the revoked partition's output is the second `send`, inside the one transaction
the commit closed, and the commit carried no offset for its partition. The prediction held: a
decline defers the duplicate rather than preventing it. The control arm shows the instrument
distinguishes the two outcomes. **Both arms stay in the suite**, because they are the reason the
fix has the shape it has.

**3. The fix, with its control.** The same three tests against master's main code: the proof red at
the same assertion, the timeout arm fails to wait, the drain-first control green. With the fix: all
green. One file changed, the outcome flips.

**4. The broker-level check found a second door.** `RebalanceEoSDeadlockTest` forces a revocation
into the middle of a control-thread commit, and now also reads the output topic with a
`read_committed` consumer after the revoked partitions return to PC and fails on any repeated value
(every input value is unique, so a repeat is a duplicated result and nothing else). With the
delegated commit alone it was **red 5/5, two or three duplicates in about 110 results** each run.
The callback timings showed the delegated commit had run, so the duplicates came from somewhere the
unit arms cannot see: once the served commit releases the producer write lock, a worker parked on
the produce lock resumes with a record of a partition that is about to be truncated - its staleness
was checked *before* it parked - produces its output into the next transaction, and has its
completion dropped as stale at truncation. Output published, offset never committed, and the
partition's next owner reprocesses it. With `PARTITION` ordering and two partitions that is at most
one in-flight record per partition, which is the two-or-three the run counted.

With the fence in place, the same five repetitions:

| Main code | Duplicate check, 5 repetitions | Duplicated results per run |
|---|---|---|
| master (inline revoke commit, no drain) | **RED 5/5** | 3, 3, 3, 4, 5 of about 110 |
| revoke commit delegated to the control thread, no fence | **RED 5/5** | 2, 2, 2, 2, 3 of about 110 |
| delegated, and the revoked partitions fenced inside the write lock | **GREEN 5/5** | 0 of 106 to 108 |

The first row is the broker-level observation the claim register's C14 said would settle it: a
duplicated result in the output topic, seen rather than argued, on the code the diagnosis
described. The served pass's own INFO line put the cost where it is paid: every revocation in this
test found one or two completions queued in the mailbox - the records an inline commit would have
published without their offsets - so under steady flow the drain matters on essentially every
rebalance, not on a rare one.

The epoch scheme cannot close it: a partition state's epoch is `final`, captured at construction,
so nothing short of truncation makes existing containers stale, and truncation is what runs too
late. The fix is an explicit fence: `PartitionState#fenceForRevocation`, set by the served pass on
the control thread **inside the write lock, after the drain and before the commit**, read by
`checkIfWorkIsStale` so that nothing starts for the partition afterwards - and checked by
`ParallelEoSStreamProcessor#acquireProduceLockRefusingRevokedWork` right after the produce lock is
acquired, which is the one point both the default and the eager-processing modes pass through
before producing. A worker that gets through the lock after the commit finds the fence and fails
its batch with `PCRetriableException`; nothing is produced, and at the retry the record is stale
and dropped, because the partition belongs to someone else. Checking before the lock would be a
check-then-act across exactly the commit the fence protects.

## Why the other candidates were out

- **Drain on the poll thread.** The obvious one-line fix. The drain mutates `WorkManager` and
  `PartitionStateManager` state that every other mutation reaches from the control thread, and
  nothing gates the control thread's own drain during a rebalance, so two threads would drain one
  mailbox concurrently - the mutation class that corrupted `numberRecordsOutForProcessing` in
  astubbs#29.
- **Decline.** Refuted by experiment 2.
- **Abort on decline.** An abort discards *every* partition's uncommitted output while their
  completions stay recorded in the state, so the surviving partitions' offsets would then be
  committed for output that was never published. That turns a duplicate into a loss. Kafka Streams
  can afford its `closeDirtyAndRevive` only because it resets every task's state along with the
  abort - a much larger change than this defect warrants.
- **Shared nothing (confluentinc#200 / astubbs#142).** The real answer to the seam, and its own
  piece of work.

## The fix: hand the commit to the control thread and wait

In transactional mode `onPartitionsRevoked` no longer commits on the poll thread. It posts a
request (`revokeCommitRequest`), wakes the control loop, and waits, bounded by
`commitLockAcquisitionTimeout`. The control loop takes the request at the top of a pass and runs its
ordinary sequence - write lock, flush, **drain**, collect, commit - which is complete by
construction, then completes the request; a pass that throws fails it, so the callback never waits
out its deadline for a commit that will not come. Truncation runs after the callback returns
either way. `AbstractParallelEoSStreamProcessor#commitOnRevokeViaTheControlThread` carries the full
reasoning at the seam.

**Why waiting on the control thread is safe here, and only here.** The AB-BA deadlock behind
confluentinc#857 is the control thread blocking on something only the poll thread can produce: in
the consumer-commit modes its `commitSync` needs `poll()` serviced. A transactional commit needs
nothing from the poll thread - the group metadata is `ConsumerManager#groupMetadata()`'s cache
(load-bearing since AK 2.7 blocks live concurrent access), every other call is to the producer,
which has its own thread, and the only control-to-poll wait in main is at shutdown. So this is the
reverse edge, and bounded besides. The consumer-commit modes keep today's inline `tryLock` commit.

**The close path.** `maybeCloseConsumer` closes the consumer from the committing thread precisely
so the callbacks it fires can commit inline, and a thread cannot wait on itself - so a callback
that arrives on the control thread runs the sequence directly, drain included.

**On timeout or failure the commit is declined, at WARN** - and the log line says what experiment
2 established: on this path a decline is the deferred duplicate, exactly-once degraded for that
rebalance. The `isRebalanceInProgress` gate on the periodic commit stays for the consumer modes and
is overridden by the request in transactional mode, because the gate existed to keep the control
thread off the poll thread's inline commit and there is no such commit any more.

**The fence, and where it is checked.** Delegating the commit closed the wide door and the
broker-level check found the narrow one behind it (experiment 4). `PartitionState#fenceForRevocation`
is set on the control thread between the served pass's drain and its commit, so the offsets it
collects are this instance's last word on those partitions; `acquireProduceLockRefusingRevokedWork` is where
a worker meets it, after the produce lock, in both transactional modes.

**The confinement declaration the diagnosis said it owed.** `processWorkCompleteMailBox` now carries
`@ThreadConfined(CONTROL_THREAD)` with `assertOnControlThread` at its entry, and
`MailboxDrainConfinementTest` is what fails when the two disagree. It could not be written truthfully
while the poll thread committed inline; the delegation is what made it true. `lastCommitTime` stays
`volatile` and undeclared: the consumer modes still write it from the poll thread.

**What the fix changed at neighbouring seams.** astubbs/parallel-consumer#408 bounds the *wait* on
the transaction lock in the same method (confluentinc#803). Under the delegation the poll thread
never takes that lock in transactional mode, so astubbs#408's contended-decline branch has no seam left in
that mode; what it still owns is whether the five-minute bound is right, and its amendment to
`RebalanceEoSDeadlockTest`. That note carries the detail.

## The register, the README, and the quarantine

C9 `NO_PRODUCE_WITHOUT_ITS_OFFSET` and C4 `OFFSET_AND_RECORDS_ATOMIC` return to `PROVED`, each with
RED and GREEN observed and the controls named in their evidence. C14
`RESULTS_EXACTLY_ONCE_UNDER_FAILURE` stayed `PROVED` throughout by ruling - no duplicate had been
observed, and refuting it on reasoning would have been the register's first argued status. The
broker-level reproduction that would have settled it now runs as a guard: `RebalanceEoSDeadlockTest`
reads the output topic with a `read_committed` consumer after the revoked partitions return and
fails on a repeated result. The quarantined proof left the registry and carries `@ProvesClaim` for
both claims; the README caution came out.

## What to reuse

- **Ask the question the fix rests on before writing the fix.** The decline position was recorded
  at merge prep with its one unverified question named; the experiment that answered it cost two
  test methods and refuted the position. The diagnosis it followed had itself been needed because a
  plausible argument about reachability was backwards until someone measured.
- **When two initiators share a sequence, put the sequence in one place.** Here the control loop's
  pass *is* the sequence, and the poll thread asks for a pass rather than reimplementing it.
- **A unit proof that goes green is not the end of the measurement.** The unit arms proved the
  drain was the term and could not see the window after it; the broker-level duplicate check, run
  on the "fixed" build first, is what found the second door. Run the outcome-level instrument on
  the fix before declaring it, not only the mechanism-level one.
- **Which direction the wait runs decides whether a two-thread seam deadlocks.** The fatal edge at
  this seam is control-blocks-on-poll; poll-blocks-on-control is safe exactly when the control
  thread needs nothing from the poll thread to finish, which is true of a transactional commit and
  false of a consumer one. Check the direction before disqualifying a wait.
