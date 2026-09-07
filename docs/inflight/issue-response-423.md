# Draft response to astubbs#423 - posted by the pre-release sweep, not by this PR

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-state: deferred - until the pre-release sweep, or an explicit instruction to post -->
<!-- post-merge: exempt-file - a drafted issue reply, held until the sweep posts it. It deliberately
     outlives the PR that wrote it, so it cannot be written in post-merge terms. -->

Not posted. Post only on explicit instruction; delete this file when it is posted, not when its PR
merges.

---

Both holes are fixed, one commit each, and both fixes have a unit test that was observed failing
before it.

**1. The `InvalidPidMappingException` arm now rethrows.** Wrapped as a `PCInternalRuntimeException`
the way the generic arm beside it wraps, so the batch is failed and returned to the mailbox and its
offsets can never become commit payload. The instance still closes with the same failure cause and
reaches the same CLOSED state.

Worth recording, because it is the reason this survived review for years: the wrong verdict never
reached a commit on master, and not because the verdict was right. `closeOnException` blocks the
worker in `waitForClose` until the instance is CLOSED, and that worker is what `innerDoClose`'s
`awaitTermination` waits on - so the whole shutdown runs before the catch arm resumes, and the
succeeded containers land in a mailbox nobody drains again. A wrong verdict protected by an
unrelated accident is a live defect, not a latent one, because whatever refactor moves the accident
ships the data loss. Which is exactly what astubbs/parallel-consumer#410 is doing to that close path.

Making the batch verdict observable therefore needed the accident stubbed out: the test replaces
`closeOnException` with a no-op so the instance stays alive. Red before the fix on all three of its
assertions - the record produced once and never re-dispatched, its offset committed, and the
incomplete-offset count at zero.

**2. `ProducerManager#close(Duration)` now closes the producer whatever the transaction cleanup
throws.** The cleanup is contained, the commit lock is released only if it was actually taken, and
`closeProducer` runs from a `finally`.

Naming the defect class rather than the symptom - *a cleanup step whose throw skips a later, more
important teardown step* - turned up two more instances, both fixed with it: the commit-lock timeout
path released a lock it never took (`IllegalStateException("Not held be me")`, same leak by a
different door), and `innerDoClose`'s producer step was the one shutdown step without the
try/catch(WARN) its three neighbours have. That last one costs more than the leak: unguarded, the
throw reaches the control task's catch, which overwrites `failureReason` with "Error from poll
control thread", re-runs `doClose` over already-closed subsystems, and makes the user's `close()`
throw.

Checked and dismissed by the same sweep: `ProducerManager#commitOffsets`, the two metrics steps in
`doClose`'s finally, `RetryQueue`'s lock pairs, and `BrokerPollSystem#closeAndWait`. Where each was
checked is in
`docs/solutions/logic-errors/a-catch-that-closes-and-continues-reports-the-batch-succeeded-2026-09-03.md`,
which is also where the defect class itself is written up.

**What is deliberately not in this.** The wrapped-`ExecutionException` shape - the one the field
report in confluentinc#830 actually hit - still takes the generic arm. That is correct for the batch
verdict and wrong for liveness, and the retry spin it causes belongs with
astubbs/parallel-consumer#411 and the recovery work in astubbs/parallel-consumer#410. Unwrapping it
into the close arm here would have converted a spin into a shutdown, which is a behaviour change
that PR is already replacing with something better.
