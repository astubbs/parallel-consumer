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
