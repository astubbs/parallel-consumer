# Recoverable producer fencing: what is still open once the implementation is on master

<!-- inflight-type: feature -->
<!-- inflight-impact: crash -->


<!-- post-merge: checked -->
The feature was implemented by astubbs/parallel-consumer#410 against the plan
`docs/plans/2026-09-02-001-feat-recoverable-producer-fencing-plan.md`, which owns the requirements, the
decisions and the mechanisms (its "Inherited from astubbs#262" section records what the merge brought).
Tracked as astubbs#225. This note keeps only what a later PR has to do, because it depends on which of
the PRs that were open beside it lands second.

## Merge-time reconciliation with astubbs/parallel-consumer#352 (KTD10)

That PR's R6 - a fenced transactional producer stays immediately fatal without consulting its handler -
was true of the code it was written against and is untrue once recovery exists. Whichever of the two
lands second rewrites, in that PR's own terms:

- the two fencing tests in its `ProducerManagerCommitBudgetTest`
  (`producerFencedOnSendOffsetsStaysFatalAndNeverReachesTheCommitLoop` and
  `recoveryAbortFailureStaysFatalAndHandlerFree`) - on the PC-built path a fencing condition now
  unwinds with `ProducerInvalidatedException` and is recovered; on the producer-instance path it is
  still fatal, which those tests can keep asserting if they pin that path;
- its R6 line, the matching statements in its commit-failure-seam feature file, and its README section.

Both PRs edit `ProducerManager.commitOffsets`; a textual conflict in the fencing branch there is
<!-- post-merge: checked -->
expected. The reasoning being overridden is recorded in the astubbs#410 commit that changed
`commitOffsets` (`feat(core) astubbs#225: recognise an invalidated producer on both paths`), so the
change does not read as an oversight.

<!-- post-merge: checked-begin - describes a reconciliation already made on the child branch, which
     reads the same once both have landed -->
## Reconciliation with astubbs/parallel-consumer#408 (KTD11) - done on that branch

That PR makes the revoke path decline both locks with `tryLock` instead of spinning. It used to
rethrow `ProducerFencedException | InvalidProducerEpochException` from the revoke-path commit so
fencing stayed fatal; with recovery in place that became record-and-decline, and astubbs#408 removed
the rethrow when it merged this branch: `ProducerManager.commitOffsets` records the condition and
unwinds with `ProducerInvalidatedException`, `tryCommitOffsetsOnRevoke`'s catch logs it, and the
control thread recovers on its next pass - and that branch had to add the wake for that pass: the
revoke path produces no mailbox event, so unlike the produce path (KTD4) nothing woke the control loop,
and it sat out the rest of its commit-interval wait first. The revoke path now calls
`notifySomethingToDo()` after releasing both locks when the manager is replacing, pinned by
`ProducerRecoveryTest.fencedDuringTheRevokePathCommitIsRecordedAndDeclinedThenRecoveredByTheControlThread`.
Its three `ProducerManager` lock helpers coexist with the waiting entry `beginReplacement` uses. The
bounded wait and holder-deadlining that recovery makes viable are named in the plan's KTD11 and
deliberately not taken; `RebalanceEoSDeadlockTest` on that branch asserts the decline.
<!-- post-merge: checked-end -->

## Still outside this work

- The wrapped-send-future spin on the producer-instance path:
  `bug-411-wrapped-send-failure-spins-forever.md`, now pinned by
  `ParallelEoSStreamProcessorTest#instancePathWrappedSendFailureStaysAliveAndRetriesAgainstTheSameProducer`.
- A transaction poisoned by a cause outside the recoverable set (a `RecordTooLargeException`):
  `bug-poisoned-transaction-not-aborted-while-running.md`.
- The unbounded revoke wait itself: `bug-857-transactional-revoke-wait.md`.
