# The two `PartitionState` flags astubbs#349 left alone

<!-- inflight-type: task -->
<!-- inflight-labels: concurrency -->
<!-- inflight-impact: coordination -->

<!-- post-merge: checked-begin - every sentence below names astubbs/parallel-consumer#469 rather
     than "this PR", and reads correctly once that PR has merged and its branch is gone -->
Working note for astubbs/parallel-consumer#469. The durable knowledge has already moved to
[`docs/solutions/logic-errors/volatile-is-the-fix-for-a-one-writer-field-not-a-shared-one-2026-09-07.md`](../solutions/logic-errors/volatile-is-the-fix-for-a-one-writer-field-not-a-shared-one-2026-09-07.md)
and into each field's own javadoc, so what is left here is only what a reviewer of that PR needs
while it is open. It goes when it merges.

## The decision astubbs#469 takes that the next reader is most likely to reverse on sight

**`stateChangedSinceCommitStart` is not fixed with `volatile`, even though the field beside it is.**
The jcstress arm `CommitWindowLostUpdateProbes.VolatileStateChangedFlagAcrossTheCommitWindow` exists
solely to hold that line - it is the volatile fix, measured, moving the anomaly by nothing. Re-run it
before proposing the modifier. The two notes astubbs#469 retired
(`bug-allowed-more-records-crosses-threads-unfenced.md`,
`bug-state-changed-since-commit-start-written-from-both-threads.md`) carried a superseded reasoning
kept deliberately - the `throughput`-vs-`stall` misclassification review caught on astubbs#349 - and
the solution write-up above carries what outlived them.

## Two things a reviewer should push back on

- **The seam the pinning test drives is new production code**, not one master already had. Master's
  protocol was correct in program order at every overridable call on the commit path; the defect is
  an interleaving inside `setClean()`'s check-then-act, between a read and a write with nothing
  between them. So `PartitionState.onCommitWindowClosing()` was added - an overridable no-op - and
  the redness is shown two ways rather than one: a permanent control arm in
  `PartitionStateCommitWindowSeamTest` that reverts only the protocol, and a transient revert of the
  production protocol whose failure output is in astubbs#469's fix commit. **If a reviewer can see a
  seam master already had, say so** - the added one loses its justification.
- **The field collapse touches what astubbs#349 measured.** `dirty` stops being a `volatile boolean`
  and becomes a comparison of two counters. The release/acquire edge that PR measured is preserved
  (an `AtomicLong` RMW is a strictly stronger release than a volatile store), but its modifier
  tripwire had to move rather than be deleted - which is what that test's own failure message asked
  for. It is now `PartitionStateCrossThreadFieldFenceTest`.

## Collisions, noted rather than dodged

Open drafts astubbs/parallel-consumer#410 and astubbs/parallel-consumer#460 both touch
`PartitionState`. Neither is reshaped around; whichever merges second resolves the overlap.
<!-- post-merge: checked-end -->
