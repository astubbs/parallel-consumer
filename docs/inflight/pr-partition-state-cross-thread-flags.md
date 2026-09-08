# The two `PartitionState` flags astubbs#349 left alone

<!-- inflight-type: task -->
<!-- inflight-labels: concurrency -->
<!-- inflight-impact: coordination -->

<!-- post-merge: checked-begin - every sentence below names astubbs/parallel-consumer#469 rather
     than "this PR", and reads correctly once that PR has merged and its branch is gone; the first
     line is post-merge-correct by removal, since the file it describes is deleted before the merge
     and so never reaches master -->
**This note is deleted in astubbs/parallel-consumer#469's final commit at merge prep, so it never
lands on `master`** - [`docs/inflight/AGENTS.md`](AGENTS.md) forbids leaving a "delete this when it
merges" marker behind, because the merge is exactly when nobody is looking here.

Working note for astubbs/parallel-consumer#469. The durable knowledge has already moved to
[`docs/solutions/logic-errors/volatile-is-the-fix-for-a-one-writer-field-not-a-shared-one-2026-09-07.md`](../solutions/logic-errors/volatile-is-the-fix-for-a-one-writer-field-not-a-shared-one-2026-09-07.md)
and into each field's own javadoc, so what is left here is only what a reviewer of that PR needs
while it is open.

## The decision astubbs#469 takes that the next reader is most likely to reverse on sight

**`stateChangedSinceCommitStart` is not fixed with `volatile`, even though the field beside it is.**
The jcstress arm `CommitWindowLostUpdateProbes.VolatileStateChangedFlagAcrossTheCommitWindow` exists
solely to hold that line - it is the volatile fix, measured, moving the anomaly by nothing. Re-run it
before proposing the modifier. The two notes astubbs#469 retired
(`bug-allowed-more-records-crosses-threads-unfenced.md`,
`bug-state-changed-since-commit-start-written-from-both-threads.md`) carried a superseded reasoning
kept deliberately - the `throughput`-vs-`stall` misclassification review caught on astubbs#349 - and
the solution write-up above carries what outlived them.

**And a second one, in the probe module: the remaining duplication between the arms is load-bearing.**
Answering the duplicate-code check, astubbs#469 hoisted the shared scaffolding into a base class each
arm extends, and stopped at a line a dedupe pass would happily cross - the measured field's own
declaration per `@State` class, and each actor's literal sequence of accesses. The two probe classes'
javadocs **own** that split and the rule behind it (a member may be hoisted only if it contains no
access to the arm's measured field); what is here is only the warning that the remaining duplication
is deliberate, so re-read those javadocs before removing any more of it.

## The vocabulary astubbs#469 leaves behind

Dirty is **derived**, so nothing sets or clears it, and the names say so:

- **`recordCompletion()`** is what `setDirty()` became. The completing thread's only write to the
  commit protocol; it stamps a new version on the partition and says nothing about whether the
  partition is dirty. Its one caller is still `onSuccess`.
- **`isDirty()`** is the derived query, and its javadoc carries the definition: a completion has
  landed that no acknowledged commit has covered.
- **`isDirtyAt(long)`** holds that comparison once. `isDirty()` passes a fresh load;
  `getCommitDataIfDirty()` passes the sample it then stashes, because the collecting cycle must
  publish the very count it tested - a second load could cover a completion whose offset the encode
  never saw, which is the burnt commit cycle astubbs#469 exists to close.

A reader arriving at `setDirty` from an older document is at the right method under the new name;
the dated records under `docs/plans/` and `docs/solutions/` that describe the retired two-flag
protocol are left as written.

## What a reviewer should push back on

- **SETTLED - the seam the pinning test drives is new production code, and no pre-existing one was
  found.** astubbs#469 asked a reviewer to say so if master already had a seam at that instruction;
  the review at
  https://github.com/astubbs/parallel-consumer/pull/469#issuecomment-5575529118 searched for an
  overridable hook on the commit path between the flag-read and the `dirty` write and found none,
  reporting no counter-evidence. `PartitionState.onCommitWindowClosing()` therefore keeps its
  justification, and astubbs#469's redness is still shown two ways: a permanent control arm in
  `PartitionStateCommitWindowSeamTest` that reverts only the protocol, and a transient revert of the
  production protocol whose failure output is in astubbs#469's fix commit.
- **The field collapse touches what astubbs#349 measured.** `dirty` stops being a `volatile boolean`
  and becomes a comparison of two counters. The release/acquire edge that PR measured is preserved
  (an `AtomicLong` RMW is a strictly stronger release than a volatile store), but its modifier
  tripwire had to move rather than be deleted - which is what that test's own failure message asked
  for. It is now `PartitionStateCrossThreadFieldFenceTest`.

## Collisions, noted rather than dodged

Open drafts astubbs/parallel-consumer#410 and astubbs/parallel-consumer#460 both touch
`PartitionState`. Neither is reshaped around; whichever merges second resolves the overlap.
<!-- post-merge: checked-end -->
