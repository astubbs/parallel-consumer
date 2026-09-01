# The branch-behind guard derives its branch from the session, not the merge it refuses

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`.claude/hooks/check-branch-behind-its-own-remote.sh` still reads
`git rev-parse --abbrev-ref HEAD` from the hook process's own directory - the SESSION's branch,
never the branch of the tree the guarded `git merge`/`git rebase` runs in. It is a **deny** hook,
so both failure directions are live: a refusal citing a fabricated stale-branch reason about a
branch the command never touches, and - the half nobody investigates - a silent pass of a merge
onto a branch that genuinely is behind its own remote, because the session's branch happened to be
current.

This is the same defect class astubbs/parallel-consumer#382 fixed in the two refusing guards and
the two push reminders (mechanism:
[`a-hook-processes-own-directory-describes-the-session-not-the-command-2026-08-31.md`](../solutions/workflow-issues/a-hook-processes-own-directory-describes-the-session-not-the-command-2026-08-31.md)).
It was found by that PR's review round as a surviving instance and deliberately left out of the PR:
merge/rebase has no refspec naming a branch, so the fix is the *directory* derivation
(`git -C` -> leading `cd` -> payload `cwd` -> labelled last resort) feeding the `rev-parse`, plus
the guard's SessionStart fetch arm deciding which repository to fetch in - a change with its own
test surface, not a rider.

The fix inherits ready-made parts: the derivation order and its self-test idioms (the `-` cwd
sentinel, the two-worktree fixture, the negative control asserting the pre-fix answer cannot
reappear) are all in `bin/test-check-agent-hooks.sh` after astubbs#382. Because this hook refuses,
it inlines rather than sourcing `hook-common.sh` -
[`ci-pr-lookup-is-copied-into-three-hooks.md`](ci-pr-lookup-is-copied-into-three-hooks.md) owns
that dividing line and counts the copies.
