---
title: A rebalance callback's navigator bookkeeping sat after the fallible commit and truncation, so a failed revoke kept minting a stale partition share
date: 2026-09-07
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: background_job
symptoms:
  - A revoke whose commit-of-ready-offsets or work-manager truncation throws leaves navigatorHeldPartitions still counting the revoked partition as held
  - The next onPartitionsAssigned then publishes held-plus over that stale, too-high numerator, so the instance's minted navigator share exceeds the partitions it actually owns
  - The instance keeps spending navigator credits against partitions it no longer holds - the one direction (over-mint) the documented fleet-wide rate bound does not cover
  - "Pre-fix red-proof failure signature: an allocation-history assertion on the held set mismatches with the revoked partition still present, e.g. \"unexpected (1): orders-2 ... but was [orders-0, orders-1, orders-2]\""
  - No exception surfaces the drift on its own - onPartitionsLost had no try/catch at all, so the only visible symptom is the fleet quietly over-minting its rate bound
root_cause: bookkeeping_ordered_after_fallible_rebalance_work
resolution_type: code_fix
severity: high
related_components:
  - AbstractParallelEoSStreamProcessor
  - PartitionShareResourceAllocator
  - AdmissionController
  - WorkManager
  - NavigatorPartitionShareRebalanceTest
tags:
  - navigator
  - partition-share
  - rebalance
  - rebalance-callback
  - held-partitions
  - over-mint
  - callback-ordering
  - red-proof
related:
  - "../runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md - the sibling defect on the same revoke path - what a rebalance callback must never do (take a lock the control thread can hold), as opposed to what it must do first"
  - "stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md - the same family one mechanism over - a mirror of a rebalance decision goes stale and the instance acts on the stale copy"
  - "../best-practices/a-conservation-check-must-sum-the-exact-grant-never-the-gauge-that-averages-it.md - the same feature's other lesson - the instrument that would have to catch this over-mint"
---
# A rebalance callback's navigator bookkeeping sat after the fallible commit and truncation, so a failed revoke kept minting a stale partition share

> Extracted from PR astubbs/parallel-consumer#456 and
> `docs/plans/2026-09-05-1046-feat-navigator-partition-share-plan.md` (R4, KTD2, KTD3).

## Problem

The Navigator's partition-share allocator learns each instance's assignment from the engine's three
`ConsumerRebalanceListener` callbacks. As first written, the callback updated the held-partition set
and published it to the allocator only *after* the callback's own fallible work - the offset commit
and the work manager's truncation in `onPartitionsRevoked`, the truncation alone in
`onPartitionsLost`. If that fallible work threw, the publish never ran, and the held set kept
claiming partitions the broker had already taken away.

## Symptoms

- An instance that had just failed a commit during a revoke still minted its full pre-revoke share
  at the next quantum - the fleet exceeded its rate bound in the one direction Partition-share's
  documented bound does not cover (over-mint; under-mint from the eager revoke-all gap is expected
  and bounded).
- The problem was invisible on any clean run. `onPartitionsRevoked`'s commit and truncation only
  throw when something is already wrong (`InternalRuntimeException`, or an unhandled exception from
  `wm.onPartitionsLost`), so an ordinary rebalance never exercised the path that mattered - the bug
  only showed up once an exception actually crossed the callback.
- A reviewer reading the callback in isolation would see the held-set update and publish sitting
  with the "rest of the post-commit bookkeeping" and read it as fine, because nothing about the
  ordering looks wrong until you ask what happens if the line above it throws.

## What Didn't Work

**The original ordering read as reasonable because it grouped like with like.** In
`onPartitionsRevoked`, `publishNavigatorAssignmentAfterLoss(partitions)` sat after the `try/catch`
around `commitOffsetsThatAreReady()` and `wm.onPartitionsRevoked(partitions)`, alongside the rest of
the callback's cleanup. In `onPartitionsAssigned` the publish sat after `wm.onPartitionsAssigned`
and the admission controller's own delta-gate call. Nothing about that grouping looks unsafe under a
clean rebalance, and a clean rebalance is what every ordinary test run exercises - so the ordering
shipped, was reviewed, and passed CI, without anything forcing the throw path to run.

The code review's cross-model (Codex) pass flagged the same line, but framed it differently: as a
*slow-callback* timing issue - what if the commit takes too long before the publish lands? That
framing turns out to already be covered by design. Partition-share deliberately uses a
lease-unchanged model (R4, resolved against KTD2 and U2 during the plan's own review): a revoked
partition's share is last minted for the quantum the revocation occurs in and excluded from the
next quantum on, so an ordinary slow callback just delays when the publish becomes effective - it
does not let the instance mint past its entitlement. The real defect was narrower and sharper than
"slow": it was the *exception* path specifically, where the publish did not run at all rather than
running late.

## Solution

The fix moves the publish to the first line of all three callbacks, before anything that can throw,
sitting beside the admission controller's own precedent for the same discipline. Both the correctness
reviewer and the reliability reviewer found this independently, and the cross-model pass corroborated
it as one finding even while framing it differently (above).

`AdmissionController.onPartitionsRevoked` already did this for its own bookkeeping:

```java
// bz/stub/parallelconsumer/internal/admission/AdmissionController.java
public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    if (mode == AdaptiveConcurrencyMode.DISABLED) {
        return;
    }
    synchronized (assignmentLock) {
        trackedAssignment.removeAll(partitions);
    }
}
```

`AbstractParallelEoSStreamProcessor.onPartitionsRevoked` calls it first, commented "FIRST, before
anything below can throw: pure set bookkeeping". Before the fix, the Navigator publish did not follow
that same discipline - it ran after the fallible commit and truncation:

```java
// before (shape)
public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    module.admissionController().onPartitionsRevoked(partitions);   // FIRST, can't throw
    isRebalanceInProgress.set(true);
    ...
    try {
        commitOffsetsThatAreReady();          // can throw
        wm.onPartitionsRevoked(partitions);   // can throw
    } catch (Exception e) {
        throw new InternalRuntimeException("onPartitionsRevoked event error", e);
    } finally {
        isRebalanceInProgress.set(false);
    }
    publishNavigatorAssignmentAfterLoss(partitions);   // never reached if the try threw
    ...
}
```

After the fix, the publish moves up beside the admission controller's call, with the reasoning
written into the callback itself:

```java
// bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java
public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    if (isAdaptiveConcurrencyActive()) {
        // FIRST, before anything below can throw: pure set bookkeeping for the KTD9 assignment-delta gate.
        module.admissionController().onPartitionsRevoked(partitions);
    }
    // Also FIRST: the navigator's held set loses the revoked partitions before the commit and the
    // truncation below, either of which can throw. Kafka has already decided the partitions are
    // leaving, so a failure here must not leave the held set claiming them - the next assign would
    // publish held-plus over a stale numerator and this instance would mint a share it no longer
    // owns (the one direction the fleet bound does not cover). Publishing here also ends the
    // revoked lease at the quantum the revocation STARTS in, which is R4's "the quantum the
    // revocation lands in".
    publishNavigatorAssignmentAfterLoss(partitions);
    isRebalanceInProgress.set(true);
    ...
    try {
        commitOffsetsThatAreReady();
        wm.onPartitionsRevoked(partitions);
    } catch (Exception e) {
        throw new InternalRuntimeException("onPartitionsRevoked event error", e);
    } finally {
        isRebalanceInProgress.set(false);
    }
    ...
}
```

The same move happened in `onPartitionsAssigned` (publish moves ahead of `wm.onPartitionsAssigned`
and the admission controller's delta-gate call) and in `onPartitionsLost` (publish moves ahead of
`wm.onPartitionsLost`, which has no `try/catch` at all around it). `publishNavigatorAssignmentAfterLoss`
and `publishNavigatorAssignmentAfterAssign` themselves are unchanged - only where the callbacks call
them moved. `PartitionShareResourceAllocator.publish` is documented as safe to call this early:

> Lock-free - an atomic append to an immutable history - so a callback never waits on the control
> loop.

The regression test, `NavigatorPartitionShareRebalanceTest.aRevokeWhoseCommitThrowsStillPublishesTheHeldSetWithoutTheRevokedPartitions`,
builds the processor as an anonymous subclass that overrides the protected `commitOffsetsThatAreReady()`
to throw unconditionally:

```java
pc = new ParallelEoSStreamProcessor<String, String>(options, module) {
    @Override
    protected void commitOffsetsThatAreReady() {
        throw new IllegalStateException("commit refused (test)");
    }
};
...
assertThrows(RuntimeException.class, () -> pc.onPartitionsRevoked(UniLists.of(tp(ORDERS, 2))));

AssignmentSnapshot afterFailedRevoke = effectiveNextQuantum();
assertWithMessage("the revoked partition left the held set although the commit threw")
        .that(afterFailedRevoke.getHeldPartitions()).containsExactly(tp(ORDERS, 0), tp(ORDERS, 1));

pc.onPartitionsAssigned(UniLists.of(tp(ORDERS, 3)));
assertWithMessage("the next assign publishes held-plus over the corrected numerator")
        .that(effectiveNextQuantum().getHeldPartitions())
        .containsExactly(tp(ORDERS, 0), tp(ORDERS, 1), tp(ORDERS, 3));
```

This is a genuine red-proof, not an assumed one: run against the pre-fix ordering, it fails with

> `unexpected (1): orders-2 ... but was [orders-0, orders-1, orders-2]`

- the held set still carries the revoked partition because the publish never ran - and passes clean
against the fix.

## Why This Works

The group coordinator's rebalance decision is final and irreversible from inside the callback: by
the time `onPartitionsRevoked` or `onPartitionsLost` runs, Kafka has already decided those partitions
are leaving this instance, whatever the callback body goes on to do. A mirror of that decision -
the Navigator's held set - has to be made true unconditionally, before anything that can fail gets a
chance to leave it stale. Putting the publish after the fallible work made the mirror's correctness
conditional on the commit succeeding, when the fact it mirrors was never conditional at all.

Publishing first also lines up with R4's rule about *when* a revoked lease ends: the share is last
minted for the quantum the revocation lands in, and excluded from the next quantum on. Moving the
publish to the start of the callback is what makes that true regardless of how long the rest of the
callback takes or whether it throws - the quantum boundary the fix cares about is measured from the
publish, not from the callback's return.

Moving the publish earlier cannot introduce the AB-BA deadlock that
`docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md`
describes on the sibling commit-lock path, because `PartitionShareResourceAllocator.publish` is
lock-free - an atomic append to an immutable history - and never touches the allocator's
`stateLock`, the monitor the control thread's quantum read takes. There is no lock for an earlier
call site to acquire and no monitor for the poll thread to block on, so there is nothing here for
lock ordering to get wrong.

## Prevention

**In a rebalance callback, state that mirrors a decision the broker has already made goes before
any fallible I/O in that callback, or into a `finally`.** The commit, the truncation, the work
manager registration - none of those can undo the group's decision; they can only fail to keep up
with it. Bookkeeping that exists purely to track that decision should not be reachable only through
the success path of something else.

**The callbacks carry two standing rules now, one for each direction (session history).** What a
callback must NOT do is the older rule: nothing on the poll thread inside these three callbacks may
take an unbounded blocking lock or wait, because the poll thread is the only thread that can
service some in-flight operations - the confluentinc#857 family's AB-BA deadlock, fixed by declining
the commit lock rather than blocking on it, and encoded on master as the ArchUnit rule
`rebalanceCallbacksMustNotBlock` in `ArchitectureTest.java` (not yet on this branch's tree, which
predates it). What a callback MUST do first is this document's rule. A change to any of the three
callbacks should be read against both.

**Grep the three callbacks whenever new callback-scoped state is added.** `onPartitionsRevoked`,
`onPartitionsAssigned` and `onPartitionsLost` in `AbstractParallelEoSStreamProcessor.java` are the
only entry points; a new field or publish call added to one without checking the others is exactly
how this one crept in three-fold (revoke, loss and assign each had their own instance of the same
ordering mistake).

**A red-proved test - one that fails against the pre-fix code by construction, not by chance - is
the only thing that catches an exception-path defect like this**, because no ordinary rebalance
exercises the throw. `aRevokeWhoseCommitThrowsStillPublishesTheHeldSetWithoutTheRevokedPartitions`
forces the throw with an anonymous override rather than waiting for a flaky failure to happen to
line up with a rebalance. One caution from an earlier test on this same callback (session history):
a test that hooked `commitOffsetsThatAreReady` to observe that the revoke path committed broke when
the fix moved that logic into another method, so it could not see a correct implementation. This
test uses the override only to force the failure and asserts the observable outcome - the held set
the next quantum mints from - which is the shape that survives a refactor.

**Two independent review passes finding the same line, corroborated by a cross-model pass - even
one that framed the finding differently - is the signal that a defect class is worth a standing
rule**, not just a one-off fix. All three converged on "the ordering can leave state stale", even
though the cross-model framing (slow callback) and the actual mechanism (exception path) were not
the same thing; the convergence on the shape is what earns this document rather than a one-line fix
note.

## Related Issues

- `docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md` -
  the sibling defect on the same revoke path: what a rebalance callback must never do (take a lock
  the control thread can hold), as opposed to this document's rule about what order it must do
  things in.
- `docs/solutions/logic-errors/stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md` -
  the same family one mechanism over: state that mirrors a rebalance decision goes stale and the
  instance acts on the stale copy (there, a point-in-time sweep racing a concurrent add).
- `docs/solutions/best-practices/a-conservation-check-must-sum-the-exact-grant-never-the-gauge-that-averages-it.md` -
  the same feature's other lesson, on the instrument that would have to catch this over-mint.
- astubbs#228 - the feature this rung belongs to; the issue stays open for the controller rung.
- astubbs/parallel-consumer#456 - the pull request the fix, the regression test and this analysis
  were committed on; open and unmerged as of this writing.
