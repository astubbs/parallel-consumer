# The two `PartitionState` flags astubbs#349 left alone

<!-- inflight-type: task -->
<!-- inflight-labels: concurrency -->
<!-- inflight-impact: stall -->
<!-- inflight-state: open -->

Working note for `fix/partition-state-cross-thread-flags`. Deleted when that PR merges; what
outlives it moves to `docs/solutions/`, and this note says which parts those are.

## What this branch inherits, and what it must not undo

Two `docs/inflight/` notes, both deleted by this branch once their fields are fixed:
`bug-allowed-more-records-crosses-threads-unfenced.md` and
`bug-state-changed-since-commit-start-written-from-both-threads.md`. Read them before touching
either field - they carry a superseded reasoning kept deliberately (the `throughput`-vs-`stall`
misclassification review caught on astubbs#349) that a tidy-up pass would delete as noise.

**The decision this branch takes that the next reader is most likely to reverse on sight:**
`stateChangedSinceCommitStart` is *not* fixed with `volatile`, even though the field beside it was.
The jcstress arm `CommitWindowLostUpdateProbes.VolatileStateChangedFlagAcrossTheCommitWindow` exists
solely to hold that line - it is the volatile fix, measured, moving the anomaly by nothing. Re-run it
before proposing the modifier.

## Two things a reviewer should push back on

- **There is no deterministic seam test that is RED on unmodified master for the commit-window
  defect, and it is not for want of looking.** Master's protocol is correct in program order at every
  overridable call on the commit path; the defect is an interleaving inside `setClean()`'s
  check-then-act, and master offers no override point between that read and that write. The
  redness is shown with a control arm instead - the seam kept, the protocol reverted - which is
  recorded in the fix commit's body. If a reviewer sees a seam master already has, say so.
- **The field collapse touches what astubbs#349 measured.** `dirty` stops being a `volatile boolean`
  and becomes a comparison of two counters. The release/acquire edge that PR measured is preserved
  (an `AtomicLong` RMW is a strictly stronger release than a volatile store), but the modifier
  tripwire it left had to move rather than be deleted, which is what its own failure message asked
  for.

## Collisions, noted rather than dodged

Open drafts astubbs/parallel-consumer#410 and astubbs/parallel-consumer#460 both touch
`PartitionState`. Neither is reshaped around; whichever merges second resolves the overlap.
