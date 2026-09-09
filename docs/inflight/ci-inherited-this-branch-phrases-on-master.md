# Unattributable self-references already on master, and what the new gate arm will not do

<!-- inflight-type: task -->
<!-- inflight-impact: process -->
<!-- inflight-vetted: 2026-09-07 - the tracked half is resolved - neither `pr-323-docs-outstanding.md` nor `pr-324-tooling-outstanding.md` is on master any more, and no note there carries a delete-when-it-merges marker; the gate arm is still branch-scoped in `bin/check-branch-self-reference.sh`, so the inherited-phrase gap it describes is unchanged and the not-defects record is what the note is now for. Dropped the now-satisfied "delete this note when both are resolved" line and dated the thirteen-file figure -->

`bin/check-branch-self-reference.sh` now catches the bare phrase spelling as well as the branch name
and the PR number - but **only on lines the current branch added**, because a phrase names nobody and
demanding an attestation for a sentence you did not write turns `post-merge: checked` into noise. The
gate header owns that reasoning.

The consequence is that everything already on master is invisible to it. A sweep of `docs/inflight/`
found the phrase in thirteen files (a 2026-08-26 snapshot - re-run the sweep at the foot of this
note rather than trusting it). Most are fine; the two below were the ones that were not.

## Two notes outlived the PRs that owned them - done on master, twice over

<!-- post-merge: checked-begin -->
`pr-323-docs-outstanding.md` and `pr-324-tooling-outstanding.md` both carried a "delete this note
when it merges" marker after their PRs merged - the residue `docs/inflight/AGENTS.md` forbids on
master. **Master fixed it**, and this branch had fixed it independently; the merge kept master's,
which is the better version in two specific ways worth recording, because both are judgements this
branch got wrong and would otherwise get wrong again:

- **`docs/quarantined-tests.md`'s overstatement was corrected in place, not tracked.** This branch
  opened a note to track the wrong sentence. Master edited the sentence. *Do not track what you can
  correct* - a tracking note for a one-line fix is a second thing to maintain and a claim that stays
  wrong until someone reads the tracker.
- **The unverifiable observations moved INSIDE the notes they are about**, rather than into a third
  note tracking other notes' defects - which is what this branch built, and which puts the caveat
  where nobody reading the affected note will see it.

The convergence itself is the other finding: two sessions independently swept for the same residue
within days. That is the rule working, not duplicated effort - but it argues for the sweep being a
gate rather than a habit.
<!-- post-merge: checked-end -->

## The other eleven are not defects

Recorded so the next sweep does not re-litigate them:

<!-- post-merge: checked-begin -->
- `docs/inflight/AGENTS.md` is the directory's rules doc and the gate excludes it by name; its
  phrasing is generic instruction, not a self-reference.
- `static-spotbugs-latent-findings.md` and `ci-agent-self-review-as-blocking-pr-comments.md` use the
  phrase generically - "classes changed in this PR" describes what a scanner scopes itself to, for
  any PR. Correct today, correct after any merge.
- The rest are live notes about branches that are still open, where the sentence is true now and the
  note is deleted when the branch lands.
<!-- post-merge: checked-end -->

(Those markers are the gate being demonstrated on the document describing it: a phrase quoted in
backticks is still a phrase, because unlike a marker there is no way to tell quoting from use.)

## What would close the gap properly

Nothing here catches an inherited phrase that has *become* false, and no grep can - it needs a reader
who knows which branch was meant. The cheap version is a periodic sweep
(`grep -rniE "this (branch|pr)" docs/inflight/`), which is what produced this note.
