# The deferred-commit WARN says a commit was postponed without saying which offsets

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-state: deferred - astubbs/parallel-consumer#352 owns the method and is adding the field the fix needs -->

`internal/ConsumerOffsetCommitter.java`, anchor `Offset commit deferred (postponed, not dropped)` -
both catch blocks log the exception and **no partition and no offset**. An operator reading it learns
that some commit was postponed and cannot tell which, so the line reads as a report while reporting
nothing identifying: misdirection, the same shape as the ERROR line one method down that
astubbs#168 fixed.

This is the sync-mode path for the two exceptions the async callback receives instead - a
`RebalanceInProgressException` or a `CommitFailedException` out of `consumerMgr.commitSync`. In
`PERIODIC_CONSUMER_ASYNCHRONOUS`, the reporter's mode in astubbs#168 (confluentinc#629), the same
failures arrive at the callback and the fixed ERROR line covers them; in `PERIODIC_CONSUMER_SYNC`
and on the poll thread's own commits they land here instead, unidentified.

**Why it is not fixed alongside astubbs#168.** The offsets are not in scope at the deferral site:
they are a local of `AbstractOffsetCommitter.retrieveOffsetsAndCommit()`, one frame down, so the
clean fix needs a field. astubbs/parallel-consumer#352 (the commit-failure seam) is rewriting
`commitDeferringOnRebalance` and already adds `lastAttemptedOffsets` for the deferral accounting -
which is exactly the state these two lines need. A second copy of that field on another branch would
put two PRs into PC's most carefully reasoned method for overlapping purposes, so this is handed
there rather than fought over.

**The fix, when astubbs/parallel-consumer#352's field exists:** render it with
`RecordBatchSummary.summariseCommit(lastAttemptedOffsets)`, so the deferral WARN and the
commit-failure ERROR say the same thing in the same shape. Assert on the emitted line with
`LogCapture`, filtered on the test's own topic name, the way
`ConsumerOffsetCommitterAsyncFailureLoggingTest` does.

**The same gap, same path:** `internal/ConsumerManager.java`, anchor
`Encountered SaslAuthenticationException while committing offset` - a WARN on the sync commit's
retry loop that also names no partition or offset. It has the offsets in scope, so it does not wait
on astubbs/parallel-consumer#352; it waits only on somebody being in this file.

**Delete when** both anchors name the offsets they are about.
