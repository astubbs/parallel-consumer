---
title: "A hook process's own directory describes the session, never the command it is guarding"
date: 2026-08-31
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: high
status: "Fixed in astubbs/parallel-consumer#382 for the two guards that refuse. check-merge-outstanding-work.sh was checked against the same defect and found correct, not merely untouched - see below."
applies_when:
  - Writing or reviewing a Claude Code hook that reads `$CLAUDE_PROJECT_DIR`, the hook process's own
    `PWD`, or `git rev-parse --abbrev-ref HEAD` with no explicit directory
  - Several worktrees of one repository are checked out at once
  - A subagent (its own working directory, distinct from the session's) can trigger the hook
  - A hook's refusal or gate result needs to name the tree or branch it actually measured
symptoms:
  - A guard names a branch nobody is touching, confidently and with no error
  - A gate blocks (or passes) a commit by checking a different worktree than the one it runs in
  - The failure is silent both ways - a wrong refusal looks like a real one, a wrong pass looks like
    a clean result
tags:
  - agent-harness
  - hooks
  - worktree-isolation
  - subagents
  - silent-failure
  - claude-code
---

# A hook process's own directory describes the session, never the command it is guarding

## The mechanism

A Claude Code hook is a separate process from the tool call it fires on. Anything the hook reads from
its **own** environment or working directory - `$CLAUDE_PROJECT_DIR`, `git rev-parse --abbrev-ref
HEAD` with no `-C`, the hook's own `PWD` - describes the **session's** state, not the command's. That
is usually harmless, because in the common case the session and the command share one working tree.
It stops being harmless the moment either of two things is true: several worktrees of this repository
are checked out at once (routine here - see `AGENTS.md`, *Worktree ownership*), or the tool call comes
from a **subagent**, which has its own working directory the session-level environment cannot see.

Both conditions hold constantly in this repository, and the failure it produces is the worst kind: not
an error, a **confident wrong answer**. A refusal that names the wrong branch reads exactly like a
refusal that named the right one. A gate that passes because it measured the session's clean tree
instead of the command's dirty one leaves no trace at all - nobody investigates a commit the gate
allowed.

## Three sightings, one session, 2026-08-31

**1. The history-rewrite guard named the wrong branch.**
`.claude/hooks/check-history-rewrite.sh` derived "the branch" from `git rev-parse --abbrev-ref HEAD`
run in the hook process's own directory. With a dozen worktrees checked out, that answers about
whichever branch the *session* sits on. Twice, both reported against `docs/god-branch-decomposition-plan`,
the worktree that session occupied:

- a force-push of `feats/proxy-verdict-free-return`, which had an open PR **with review history**
  (astubbs/parallel-consumer#295) - exactly the case the guard exists to name - was refused with *"the
  lookup ran and came back empty: no open pull request has `docs/god-branch-decomposition-plan` as its
  head branch"*;
- a `git commit --amend` inside the `feats/ks-streams-fork-machinery` worktree was reported against
  that same unrelated branch.

**2. The pre-commit gate gated the wrong working tree.**
`.claude/hooks/pre-commit-gate.sh` resolved both the gate script and its working directory from
`$CLAUDE_PROJECT_DIR`, which names the session's project root. A review comment once dismissed this as
harmless, reasoning that `if: Bash(git commit *)` matches the command as written, so only a bare
commit in the session's own cwd - where session repo and commit repo are the same one - reaches the
hook. That premise is false for a **subagent**: it has its own working directory while
`$CLAUDE_PROJECT_DIR` still names the session's most recent worktree, and a subagent's `git commit ...`
is bare, so it matches the registration and arrives anyway.

A subagent committing in `.claude/worktrees/proxy-server-shell` was gated against
`.claude/worktrees/bench-harness`: `bin/check-file-refs.sh` failed on citations to `bench/` files
absent from the agent's own branch, while that branch's own tree ran the same gate at exit 0. Five
commits went through with `--no-verify` as the only way past a block that was never really about the
tree being committed. **The dangerous half is the mirror image, and it leaves no trace**: a red tree
passes because the session's tree is green, so a violation lands with the gate reporting success.

**3. The quarantine registry gate refused silently** - a related but distinct defect (a swallowed
`set -euo pipefail` exit, not a wrong-directory read); see astubbs/parallel-consumer#382 for the fix.
Not part of this mechanism and not covered further here.

## The fix's derivation order

Both wrong-directory guards now derive the tree or branch from the **command**, strongest source
first, and their refusal names which source answered:

1. **Something the command itself names unambiguously.** For a push, the refspec's destination
   (`git push origin src:dst` publishes `dst`, which is what a PR has as its head branch) - free,
   because the command was already tokenised for other reasons. For a commit, `git -C <path>`, used
   only when every commit in the payload names the same directory; a payload committing in two
   repositories has no single answer and falls through rather than guessing.
2. a leading `cd <path> &&`,
3. the payload's own `cwd` - where a subagent's actual directory arrives,
4. `$CLAUDE_PROJECT_DIR`, then the hook process's own directory - labelled **last resorts**, kept
   because with nothing in the payload saying where the command runs, they remain the best answer
   available.

For `pre-commit-gate.sh`, the chosen directory then climbs to its repository root before the gate
script and its working directory are both taken from there - running the right script with the wrong
directory was the second half of the same defect, since `.githooks/pre-commit` itself opens with its
own `git rev-parse --show-toplevel`.

The full design reasoning for each guard - why `if: Bash(git commit *)` cannot be trusted to have
filtered, why the directory is read in the same payload scan that decides a bypass rather than a
second one, why the last resorts stay rather than being removed - lives in `docs/agent-harness.md`
(the `pre-commit-gate.sh` and `check-history-rewrite.sh` entries) and in the two hooks' own headers;
it is not repeated here.

**Reproduced live, while the fixing commit for sighting 2 was itself being made**: the harness ran
`pre-commit-gate.sh` out of a stray `bench-harness` worktree and blocked the commit on that tree's
citations, while the worktree actually being committed to ran the same gate at exit 0. Same payload,
same `$CLAUDE_PROJECT_DIR`, the only variable was which hook file answered:

```
pre-fix hook  -> exit 2, blocked on the other tree
fixed hook    -> exit 0, gated the right tree, nothing to say
```

That commit was made with `--no-verify` as a result - the gate blocking it had measured a tree nobody
was committing to.

## A neighbour checked and found correct, not merely left alone

`.claude/hooks/check-merge-outstanding-work.sh` has the same shape available to it - it too can read
`git rev-parse --abbrev-ref HEAD` with no `-C` - and was checked against this defect rather than
assumed safe by association. It reads the PR number out of `gh pr merge <n>` first; the `HEAD`
fallback is reached only for a **bare** `gh pr merge`, and a bare `gh pr merge` resolves the current
branch the same way the fallback does. There is no case where the two disagree, so it was left as is.
Checking a neighbour explicitly, rather than sweeping it in on the strength of the pattern, is what
turned "presumably fine" into a verdict.

## The generalised lesson

**A hook process's own directory and environment describe the session, never the command.** Every
guard that reads either without deriving from the payload first has to be re-checked against that -
not swept, checked: the neighbour above shows the difference between believing a pattern applies and
confirming it does.

## Related

- `docs/agent-harness.md` - the registry entries for `pre-commit-gate.sh` and
  `check-history-rewrite.sh` own the day-to-day design detail (the derivation order, the payload
  self-filtering, the labelled last resorts); this document is the incident write-up, not a second
  copy of that detail.
- `docs/inflight/ci-pr-lookup-is-copied-into-three-hooks.md` - the still-open task this incident
  intersects: the branch-to-PR lookup exists three times and should exist once. The fix here adds a
  **fourth** duplicated thing for the same fail-open reason - the refspec-derivation rule, as
  `hook_push_head_ref` in `.claude/hooks/lib/hook-common.sh` (bash) and `push_head_ref` inside
  `.claude/hooks/check-history-rewrite.sh` (python) - which whoever folds the three lookups together
  inherits too.
- [`silent-cwd-reset-runs-git-in-the-wrong-checkout.md`](silent-cwd-reset-runs-git-in-the-wrong-checkout.md) -
  the same family at the level of an interactive session rather than a hook process: trusting a
  location instead of pinning it. Different actor, same fix shape - name the tree explicitly rather
  than inheriting an ambient one.
- [`compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md`](compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md) -
  the read-only sibling class: tooling that infers repository identity from its surroundings instead
  of being told, and also fails quietly.
