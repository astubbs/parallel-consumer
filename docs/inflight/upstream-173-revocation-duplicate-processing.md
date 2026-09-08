# confluentinc#777: revocation duplicates are the contract, and the answer saying so is unposted

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-vetted: 2026-09-08 - applied: shrunk to the unposted draft and the two mirror-body corrections. The adoc misdirection it was filed against is corrected, so that section is now one paragraph of premise; next-steps 1 and 2 shipped and are recorded as done; the draft's mitigation paragraph updated because the fourth matrix cell has since been measured; checked: the adoc reads REFUTED 2026-08-20 and carries the do-not-close rule, `README_TEMPLATE.adoc` has the `reducing-duplicate-replay` section with all four cells including 369 for cooperative plus draining, `Dropping work from revoked partition` is still in `WorkManager.handleFutureResult`, `synchronized (commitCommand)` still appears four times in `AbstractParallelEoSStreamProcessor`, and astubbs#173 is still OPEN with the draft unposted - its two 2026-09-01 comments are about astubbs#346, astubbs#345 and astubbs#29, not this answer -->

Mirror: [astubbs/parallel-consumer#173](https://github.com/astubbs/parallel-consumer/issues/173).
Upstream: [confluentinc/parallel-consumer#777](https://github.com/confluentinc/parallel-consumer/issues/777).

## Why the answer is not simply "at-least-once, live with it"

`WorkManager.handleFutureResult`
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/WorkManager.java`, grep
`Dropping work from revoked partition`) still discards a result whose partition moved, which is
verbatim the reporter's step 3 - and that drop is load-bearing rather than a gap. The draft below
states the reasoning; this note exists to hold it until somebody posts it.

<!-- post-merge: checked-begin -->
**The record that used to contradict this has been corrected.**
`src/docs/development/upstream-pr-analysis.adoc` ranked confluentinc#777 sixth under
"Fixed by merging PR #893 + #909" <!-- issue-refs: exempt - quoted from upstream-pr-analysis.adoc; requalifying a quote falsifies it -->
with the verdict "Verify after cherry-pick". Verified 2026-08-20: refuted, and the adoc now says so
and carries the standing rule with it - grep
`Do not close confluentinc#777 when the confluentinc#893 cherry-pick merges`. confluentinc#909
governs which container wins a *registration* race and is silent about a result arriving after
revocation; confluentinc#893 makes `getOffsetToCommit()` accurate so a commit cannot run ahead of
completion, which is the opposite shape - here the committed offset is already correct, the in-flight
record is correctly encoded as incomplete, and the redelivery follows from that correctness.
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
> On mitigation, there is something concrete, and it is not the obvious thing. We measured all four
> assignor and stop-mode combinations against the same rebalance storm (250,000 records, one seed):
> eager plus abrupt stop gave 2421 duplicates, eager plus draining stop 2007, cooperative plus abrupt
> stop 405, and cooperative plus draining 369. **The assignor accounts for essentially all of it;
> draining is second-order.** That is what "duplicates are a product of revocation rather
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
>
> The full table, with the caveats on how it was measured, is in the README's
> "Reducing duplicate replay" section.

## Also stale in the mirror body

<!-- post-merge: checked -->
astubbs#173's `## Fork status` is otherwise sound - it names astubbs#29 as the fork's work on this, `commitOffsetsThatAreReady`
still takes the `synchronized (commitCommand)` monitor on master
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java`,
grep `Synchronizing on commitCommand` - the bare `synchronized (commitCommand)` appears four times in
that file, so it is not an anchor), and the chaos suite really does assert bounded rather than zero
duplicates. Two corrections:

- *"Only the transactional (EOS) commit mode gives exactly-once"* contradicts our own README
  (`src/docs/README_TEMPLATE.adoc`, grep `does not prevent _duplicate message replay_`; the template
  is the source, `README.adoc` is generated). Offering it as the answer here is misleading.
<!-- post-merge: checked -->
- astubbs#29 is named as the closest fork work, which is true, but its `tryCommitOffsetsOnRevoke()`
  deliberately *skips* the revocation commit under lock contention, trading redelivery for killing
  the deadlock. It moves this symptom the wrong way, and the mirror reads as though it helps.

## Next

1. Post the answer, then close astubbs#173 and relabel it off `bug`.

The two steps that used to come first are done: `src/docs/README_TEMPLATE.adoc` has a
`reducing-duplicate-replay` section carrying the assignor and close-mode guidance drawn from the
draft (the template is the source, `README.adoc` is generated), and the cooperative-plus-draining
cell has been run rather than predicted, so all four figures above are measurements. The seed and
lane are unchanged: `4734674029169027864`, recorded with the matrix in
`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/chaostests/ChaosRevokeUnderWorkCooperativeDrainIT.java`.

**Maintainer decision, and only theirs:** whether PC should offer a revocation grace period at all.
Upstream declined it as complexity for a benefit a crash voids. If the answer is no, confluentinc#777
is a documentation obligation rather than a defect, and posting is unblocked.
