# A record becomes selectable before its offset is in `incompleteOffsets`

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-labels: concurrency -->

**astubbs/parallel-consumer#370 owns this.** Read it before acting; what is here is what a session
scanning this directory needs in order to know the defect exists.

**Open, and latent rather than observed.** `PartitionState#maybeRegisterNewPollBatchAsWork` calls
`addWorkContainer` before `addNewIncompleteRecord`, so a record is visible to shard scanners before
its offset is in `incompleteOffsets`.

**What is left to do:** the ordering is safe only because registration and completion both run on
the control thread. Nothing states that invariant and nothing tests it, so it holds by accident of
today's callers. State it and enforce it - astubbs/parallel-consumer#370 carries the two candidate
ways of settling it.

<!-- post-merge: checked-begin -->
**If you are here from a failing assert:** this is the one shape that fires
`PartitionState#onSuccess`'s `assert (removedFromIncompletes)` with **no double delivery**, so reach
for it once the claim is ruled out - that has been one atomic transition since
astubbs/parallel-consumer#335, pinned by `WorkClaimStateMachineTest`.
<!-- post-merge: checked-end -->
