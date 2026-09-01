# The branch-behind guard derives its branch from the session, not the merge it refuses

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`.claude/hooks/check-branch-behind-its-own-remote.sh` reads
`git rev-parse --abbrev-ref HEAD` from the hook process's own directory - the SESSION's branch,
never the branch of the tree the guarded `git merge`/`git rebase` runs in. It is a **deny** hook,
so both failure directions are live: a refusal citing a fabricated stale-branch reason about a
branch the command never touches, and - the half nobody investigates - a silent pass of a merge
onto a branch that genuinely is behind its own remote, because the session's branch happened to be
current.

<!-- post-merge: checked-begin -->
This is the one recorded surviving instance of the defect class astubbs/parallel-consumer#382
fixed in the four hooks it touched - the two refusing guards and the two push reminders
(mechanism:
[`a-hook-processes-own-directory-describes-the-session-not-the-command-2026-08-31.md`](../solutions/workflow-issues/a-hook-processes-own-directory-describes-the-session-not-the-command-2026-08-31.md)).
It stayed out of that change on purpose: a merge/rebase has no refspec naming a branch, so the fix
here is the *directory* derivation (`git -C` -> leading `cd` -> payload `cwd` -> labelled last
resort) feeding the `rev-parse`, plus the guard's SessionStart fetch arm deciding which repository
to fetch in - a change with its own test surface, not a rider on another one.

Whoever picks this up inherits ready-made parts from `bin/test-check-agent-hooks.sh`: the
derivation order and its self-test idioms - the `-` cwd sentinel that omits the payload cwd, the
two-worktree fixture, the negative control asserting the pre-fix answer cannot reappear - plus the
cwd-preserving-joiner and composed-`-C` rules the guards apply, which this fix must apply too.
<!-- post-merge: checked-end -->

Because this hook refuses, it inlines rather than sourcing `hook-common.sh` -
[`ci-pr-lookup-is-copied-into-three-hooks.md`](ci-pr-lookup-is-copied-into-three-hooks.md) owns
that dividing line and counts the copies.
