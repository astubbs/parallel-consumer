# The commit path's write-lock wait treats the control thread's wake-up interrupt as a shutdown

<!-- inflight-type: bug -->
<!-- inflight-impact: crash -->
<!-- inflight-labels: concurrency -->

Found 2026-09-02 while writing the recovery test for the same shape on the recovery path (astubbs#225,
where it was fixed for `beginReplacement`); traced, not reproduced, and deliberately not fixed there.

`notifySomethingToDo` wakes the blocked control thread by interrupting it whenever the producer write lock is
not held. The commit path's own wait for that lock - `maybeAcquireCommitLock` reached through
`preAcquireOffsetsToCommit` - is a timed `tryLock`, and the lock is by definition not held while it waits, so
the interrupt lands there as an `InterruptedException`. Nothing on that path catches it as the wake-up it is:
it escapes to `supervisorLoop`, whose `InterruptedException` arm calls `doClose` with no failure reason. The
mailbox poll and the end-of-pass sleep both catch the same interrupt and merely log, and the recovery path now
does too; the commit wait is the remaining arm.

The window is a worker holding the produce read lock inside a slow user function while a commit is due and
something fires the wake-up - partition assignment at the end of a rebalance, `requestCommitAsap`, pause or
resume. The recovery test used a single record, so it never attempted a commit and never reached this wait.

To settle it: a unit test that parks a worker in a user function, requests a commit, fires
`notifySomethingToDo`, and asserts the instance stays RUNNING. The fix is the one the recovery path took -
catch the interrupt at the wait, clear it, and let the pass retry.
