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

## Two notes outlived the PRs that owned them

<!-- post-merge: checked-begin -->
`docs/inflight/AGENTS.md` says a note is deleted when its work lands, that a PR deletes its own note,
and that a `delete this when #NN merges` marker must never be left on master - "the merge is exactly
when nobody is looking here". Two notes carry exactly that marker and both PRs have merged:

- **`pr-323-docs-outstanding.md`** (astubbs#323, merged 2026-08-20). Its "Closed" section describes a
  window that closed, in the present tense, and the surviving content is three editorial items with
  nothing to do with astubbs#323 as an event.
- **`pr-324-tooling-outstanding.md`** (astubbs#324, merged). Same shape. Two items genuinely survive:
  the `agents: hook self-tests` case that asserts against the live `docs/inflight/` corpus rather
  than a fixture, and five agent-hook commit bodies that exist only on `backup/pre-split-322`.

The prescribed repair is not to rewrite them into an accurate past tense - the directory's rules
forbid that explicitly - but to **shrink each to its surviving items and rename it** to an area
prefix, then repoint the one citation that survives the rename
(`bug-shared-collections-across-the-poll-boundary.md` cites astubbs#323's note for a merge ordering).
Left undone here deliberately: it decides the fate of two other workstreams' records, and this branch
has no standing to do that quietly.
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
