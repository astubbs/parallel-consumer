# PR astubbs#293: settle the trailing `(confluentinc#154)` at merge time

The last open item from the code review of the spike and freeze cluster (`2b53104e3..6a2309c28`),
parked by an explicit call to stop review-and-fix cycles and spend the remaining budget on the
language fan-out (astubbs#242, 2026-08-14). Every other finding it returned has since been fixed by
the commit that resolved it.

**81 of this branch's 86 `astubbs#242` commits end their subject with `(confluentinc#154)`, and
`AGENTS.md` reserves that trailing parenthetical for the squash-added PR number, "never an issue".**
The ambiguity the rule guards against is absent here, because the reference is repo-qualified rather
than a bare number - so nothing is wrong today, and this is not a request to rewrite 86 subjects.

What must not happen is inheriting the shape by accident. On a squash merge GitHub appends its own
`(#293)`, so whichever subject merges ends in two parentheticals, and the release-note generator
reads exactly those subjects. Decide it with the merge strategy rather than after
([`docs/merge-checklist.md`](../merge-checklist.md)), and apply the same check to the PR title, which
on a squash *is* the merged subject - astubbs#293's title carries no trailing parenthetical today.

## And the reference itself is wrong, which settles what to do

Owner, 2026-08-15: **since the issue-mirror sweep ran, upstream numbers should not be referenced at
all where a fork mirror exists** - an upstream number is only correct when nothing mirrors it, and
the same rule governs PRs.

A mirror exists here, and it is the one every commit already names at the front: **astubbs#242 is the
fork mirror of confluentinc#154** (its title is literally `confluentinc#154: Integrate into a Proxy
so can support any language`). So the trailing parenthetical is wrong twice - it occupies the slot
reserved for the PR number, *and* it cites upstream when the mirror is already in the same subject.

**The fix at recut is therefore deletion, not relocation**: the subject becomes
`type(scope) astubbs#242: <subject>`, and the trailing slot is left empty for GitHub to fill. Nothing
is lost, because the fork mirror is the reference and it is already there.

The commits are being re-cut anyway, so this needs no separate pass - it is a rule for whoever writes
the new subjects.

**One open question for that person**, deliberately not decided here: the `Upstream-Issue:
confluentinc/parallel-consumer#154` trailer these commits also carry. A trailer named for upstream is
provenance metadata rather than prose - the same thing the manifest records - so it may be the one
place an upstream number still belongs. [`docs/upstream.md`](../upstream.md) owns the trailer
convention and should answer it.
