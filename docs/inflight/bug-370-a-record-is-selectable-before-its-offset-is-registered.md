# A record becomes selectable before its offset is in `incompleteOffsets`

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-labels: concurrency -->

**astubbs/parallel-consumer#370 owns this** - the ordering, the invariant that closes it today, and
the two ways of settling it are all written up there. Read it before acting; what is here is only
what a session needs in order to know the issue exists.

**Latent, not observed.** `PartitionState#maybeRegisterNewPollBatchAsWork` makes a record visible to
shard scanners before its offset is in `incompleteOffsets`. Harmless while registration is
control-thread-only, which nothing states and nothing tests.

<!-- post-merge: checked-begin -->
**Why it is in front of you at all:** it is the one shape that fires `PartitionState#onSuccess`'s
`assert (removedFromIncompletes)` with **no double delivery**. Anyone meeting that assert again,
having correctly ruled out the claim (one atomic transition since astubbs#335, pinned by
`WorkClaimStateMachineTest`), needs this on the list rather than in front of them for the first
time.
<!-- post-merge: checked-end -->
The 2026-08-22 sightings were double deliveries - `deliveryCount == 2` said so - but it was that
evidence, not the assert, that made the inference correct.
