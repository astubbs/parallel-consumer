# astubbs#337 handoff - the fix is settled; the coupling with the encoder branch is the live part

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

<!-- post-merge: checked-begin -->
Written 2026-08-25 for whoever finishes this PR. **Delete this file in this PR at merge prep.**
Everything below is state at time of writing; verify against the live PR before acting.
<!-- post-merge: checked-end -->

## What this branch is

The confluentinc#893 carry (fixes confluentinc#894 / astubbs#121) plus the behavioural evidence
upstream never had: `PartitionStateCommitEncodeShift894Test`,
`PartitionStateCommitShiftCompounding894Test`, the shared `RacingCommitCycleState` double, and the
write-up at `docs/solutions/logic-errors/commit-offset-read-twice-shifts-every-encoded-incomplete-offset.md`.
Split out of astubbs#57 on 2026-08-24. **Auto-closing astubbs#121 on merge is now correct** - the
reproduction the issue was being held open for is in this PR.

## The one live coupling: the racing seam vs astubbs#344

This PR's `RacingCommitCycleState` injects its race by overriding
`getIncompleteOffsetsBelowHighestSucceeded()`. astubbs#344's fix makes the encoder call the new
bounded `getIncompleteOffsetsBelow(long)` instead. **On a tree with both merged, part of this PR's
seam goes dead**: `PartitionStateCommitEncodeShift894Test` fails LOUDLY at its `raceHasFired`
precondition (the guard working), but the compounding test arms per-cycle races without a
per-cycle fired-assertion and can go silently green. Whichever PR merges second owns the re-hook:
point the double at the bounded overload, and consider per-cycle fired-guards in the compounding
test while there. Recorded from the other side in `bug-torn-read-family.md` (astubbs#344's branch).

## Claims discipline - do not widen them back

The PR body and write-up were deliberately **narrowed** after review refuted "every path is closed":
the encoder one layer down had the same defect class (now astubbs#344). The accurate claim is that
the offset-to-commit read is single-sampled; the payload-contents tear was a separate fix. The
write-up's "Still open" sentence about the compounding bound (regularity, not proof) is intentional.

## At merge: post the drafted issue responses (operator review first)

`pr-337-issue-response-drafts.md` (this branch) holds drafts for astubbs#121, confluentinc#894 and
confluentinc#893 - written now because this is when the context exists. Posting requires the
operator's explicit go-ahead per the never-post-unasked rule; the drafts also carry the
class-assurance summary (the family hunt, Lincheck calibration, jcstress probes, cross-model
review) the operator wants the reporters to see.

## Merge mechanics

- Commits are atomic; rebase-merge as-is is viable. The evidence commit `f4d4dcbf5` carries an
  overstated "replay, never skip" body corrected by the later `818aa1f75` - if squashing, write the
  narrowed claim, not the original.
- `src/docs/development/upstream-map.yaml` `cherry-pick-893-offset-reset`: **this branch's version
  (`prs: [337]`) wins** the designed conflict with astubbs#57's edit of the same entry. Set
  `status: merged` in the branch before merging - `check-upstream-map-merged.sh` denies otherwise.
- `docs/inflight/release-0.6.0.0.md` gained the "Marked for 0.6.0.0" entry here (post-merge marker
  in place) - it asks the release note to say "silent record loss possible, narrowly conditioned",
  not just "offset accuracy". astubbs#344's branch edits a different section of the same file;
  expect a trivial or clean merge.
- The red arm lives on `test/torn-read-candidates-reproduction` and the earlier
  `test/894-reproduce-offset-encode-shift`; neither ever merges. The hunt branch also carries the
  forced-open bootstrap-tear demonstration this write-up cites.

## Verification already done - do not repeat, do not trust either; re-run is cheap

7/7 red against unfixed master, 7/7 green here, controls both directions, seam guard proven by arm
deletion, all independently re-run at review. `bin/lincheck-test.sh` on astubbs#347's branch refinds
this bug unaided - when this PR merges first, that harness goes red by design; flip it per
astubbs#347's body.
<!-- file-refs: N/A - bin/lincheck-test.sh lives on astubbs#347's branch, named deliberately -->
