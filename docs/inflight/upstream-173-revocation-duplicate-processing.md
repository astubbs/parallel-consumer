# confluentinc#777: revocation duplicates are the contract, and two records call them a bug

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

Mirror: [astubbs/parallel-consumer#173](https://github.com/astubbs/parallel-consumer/issues/173).
Upstream: [confluentinc/parallel-consumer#777](https://github.com/confluentinc/parallel-consumer/issues/777).

## The tracked claim is false, and it will close this issue for the wrong reason

`src/docs/development/upstream-pr-analysis.adoc` ranks confluentinc#777 sixth and states, verbatim:

> Fixed by merging PR #893 + #909 <!-- issue-refs: exempt - quoted from upstream-pr-analysis.adoc; requalifying a quote falsifies it -->

with the verdict "Verify after cherry-pick". Verified 2026-08-20: **refuted**. The prediction was
stated before checking, and it was that both halves would miss.

confluentinc#909 is the testable half, because the fork already carries it (astubbs#31, merged). If
it were the fix the behaviour would be gone. It is not: `WorkManager.handleFutureResult`
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/WorkManager.java`, grep
`Dropping work from revoked partition`) still discards a result whose partition moved, which is
verbatim the reporter's step 3. confluentinc#909 governs which container wins a *registration* race;
it is silent about a result arriving after revocation.

<!-- post-merge: checked-begin -->
confluentinc#893 (carried in astubbs#337, split out of astubbs#57 on 2026-08-24) makes
`getOffsetToCommit()` accurate so a commit cannot run
*ahead* of completion. confluentinc#777 is the opposite shape: the offset committed at revocation is
already correct, the in-flight record is correctly encoded as incomplete, and the redelivery follows
from that correctness. Nothing is lost.

**So merging the confluentinc#893 cherry-pick must not be read as closing confluentinc#777.**
The adoc entry carrying that correction is already in place - `upstream-pr-analysis.adoc`, grep
`Do not close confluentinc#777 when the confluentinc#893 cherry-pick merges` - so what remains here
is the standing rule, not an open task.
<!-- post-merge: checked-end -->

## Draft answer - postable as-is once a maintainer agrees with the closing rationale

> Your read of the mechanism is exactly right, including step 3, and the behaviour is deliberate
> rather than an oversight - but the reason is worth stating, because it is not simply
> "at-least-once, live with it".
>
> The divergence is over what PC treats as the unit of the delivery guarantee. You expected that
> *starting* to process a record makes its completion durable. PC's guarantee is attached to
> partition ownership: a completion can only be recorded by the instance that still owns the
> partition when the result comes back. Once the partition is revoked, this instance has no standing
> to record anything about it, so the result is dropped.
>
> That drop is load-bearing, not a gap. The new owner may already be processing that same offset. A
> returning stale result that was allowed to write would remove the fresh work container that
> replaced it - and *that* is a dropped record, which is strictly worse than a duplicated one. It is
> the defect confluentinc/parallel-consumer#909 describes, and the guard in `handleFutureResult`
> is what prevents it.
> Honouring the completion of work on a revoked partition would reintroduce it.
>
> On mitigation, there is something concrete, and it is not the obvious thing. We measured the
> assignor and stop-mode combinations against the same rebalance storm (250,000 records, one seed):
> eager plus abrupt stop gave 2421 duplicates, eager plus draining stop 2007, and cooperative plus
> abrupt stop 405. The cooperative-plus-draining cell has not been run. **The assignor accounts for
> essentially all of it; draining is second-order.** That is what "duplicates are a product of revocation rather
> than of departure" predicts: the eager assignor revokes every partition from every member on any
> membership change, so most of the abandoned work never belonged to the member that left. Draining
> on redeploy - your instinct, and the advice most people would give - buys almost nothing on its
> own. Switching to `CooperativeStickyAssignor` is the change that moves the number.
>
> One correction to something you may have read here: EOS is not an escape hatch for this. Kafka
> transactions give effectively-once *results in Kafka output topics*, not exactly-once processing,
> and PC's own README says so. For non-idempotent work outside Kafka it does not help, so
> idempotency really is the right call for your case.
>
> A revocation grace period (finish in-flight work for revoked partitions before releasing them) is
> the feature that would reduce this further. It needs partition-scoped submission suppression plus
> in-flight tracking, a crash still voids it, and nobody has built it. Tracked, not planned.

## Also stale in the mirror body

astubbs#173's `## Fork status` is otherwise sound - astubbs#29 is still open, `commitOffsetsThatAreReady`
still takes the `synchronized (commitCommand)` monitor on master
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java`,
grep `Synchronizing on commitCommand` - the bare `synchronized (commitCommand)` appears four times in
that file, so it is not an anchor), and the chaos suite really does assert bounded rather than zero
duplicates. Two corrections:

- *"Only the transactional (EOS) commit mode gives exactly-once"* contradicts our own README
  (`src/docs/README_TEMPLATE.adoc`, grep `does not prevent _duplicate message replay_`; the template
  is the source, `README.adoc` is generated). Offering it as the answer here is misleading.
- astubbs#29 is named as the closest fork work, which is true, but its `tryCommitOffsetsOnRevoke()`
  deliberately *skips* the revocation commit under lock contention, trading redelivery for killing
  the deadlock. It moves this symptom the wrong way, and the mirror reads as though it helps.

## Next

<!-- post-merge: checked -->
The measurements are from the astubbs#57-family chaos work, seed `4734674029169027864`, recorded
with the matrix in `parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/chaostests/ChaosRevokeUnderWorkCooperativeDrainIT.java`.
That javadoc records three cells and predicts the fourth rather than measuring it, so the
cooperative-plus-draining figure must not be quoted until the cell is actually run. Nothing in
`src/docs/README_TEMPLATE.adoc` tells a user any
of it: the offset-map section states the replay caveat without saying what changes its magnitude,
and there is no assignor guidance anywhere.

1. README section on revocation redelivery, drawn from the draft above. The evidence exists; this is
   writing, not investigation.
2. Run the cooperative-plus-draining cell, so the fourth number can be stated rather than predicted.
3. Post the answer, then close astubbs#173 and relabel it off `bug`.

**Maintainer decision, and only theirs:** whether PC should offer a revocation grace period at all.
Upstream declined it as complexity for a benefit a crash voids. If the answer is no, confluentinc#777
is a documentation obligation rather than a defect, and step 3 above is unblocked.
