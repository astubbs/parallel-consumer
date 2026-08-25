# Unattributable self-references already on master, and what the new gate arm will not do

<!-- inflight-type: task -->
<!-- inflight-impact: process -->

`bin/check-branch-self-reference.sh` now catches the bare phrase spelling as well as the branch name
and the PR number - but **only on lines the current branch added**, because a phrase names nobody and
demanding an attestation for a sentence you did not write turns `post-merge: checked` into noise. The
gate header owns that reasoning.

The consequence is that everything already on master is invisible to it. A sweep of `docs/inflight/`
found the phrase in thirteen files. Most are fine; these are the ones that are not. Delete this note
when both are resolved.

## Two notes outlived the PRs that owned them - now shrunk and renamed

<!-- post-merge: checked-begin -->
`pr-323-docs-outstanding.md` and `pr-324-tooling-outstanding.md` both carried a "delete this note
when it merges" marker and both PRs had merged - the exact thing `docs/inflight/AGENTS.md` forbids
leaving on master, because "the merge is exactly when nobody is looking here".

Repaired the way that doc prescribes - shrink to the surviving items and rename, never rewrite into
an accurate past tense. **Each surviving item was re-verified rather than copied**, and that halved
them: the hook self-test now builds `mktemp` fixtures instead of asserting against the live
`docs/inflight/` corpus, so astubbs#324's first item was already fixed; astubbs#323's flagged
PCMetrics figures are now traceable to the run they came from. What was left became
`docs-claims-their-own-evidence-does-not-support.md` and
`branch-agent-hook-commit-bodies-only-on-backup.md`. The merge ordering that only astubbs#323's note
held moved to `pr-blockers-and-collisions.md`, which owns standing coordination facts, and the one
citation of it was repointed there.
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
