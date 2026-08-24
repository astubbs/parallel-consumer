---
title: "Silent cwd reset runs git in the wrong checkout - pin every command with git -C"
date: 2026-08-18
category: workflow-issues
module: git workflow
problem_type: workflow_issue
component: development_workflow
applies_when:
  - "Long sessions mixing background tasks with cwd-dependent git commands"
  - "Working from a PR worktree or any branch-specific checkout"
  - "Any history-changing git command (merge, rebase, reset, commit --amend, force push)"
symptoms:
  - "Git commands execute in the wrong checkout (main vs worktree)"
  - "Conflicts appear in files your branch never touched"
  - "Merge or rebase lands on an unrelated branch"
root_cause: missing_tooling
resolution_type: workflow_improvement
severity: high
tags: [git-safety, worktree-isolation, cwd-management, absolute-paths, background-tasks]
---

# Silent cwd reset runs git in the wrong checkout - pin every command with git -C

## Context

A long agent session was working in a per-task git worktree (machine-local, under the git-ignored worktrees directory; branch `feats/ideate-distributed-throttling`, open PR astubbs#308). Late in the session the user asked to "update from master." The agent ran `git fetch` + `git merge origin/master` without pinning the directory - trusting the session's working directory instead. The shell's cwd had silently reset to the main checkout (`/Users/astubbs/github/parallel-consumer`) at some earlier point, after the session ran background tasks; one command had even emitted "Shell cwd was reset to /Users/astubbs/github/parallel-consumer," but that note scrolled past unread. The merge ran in the main checkout, on the unrelated branch `rename/master-packages`, and produced conflicts in `README.adoc` and `src/docs/README_TEMPLATE.adoc` - files the intended branch never touched. `git merge --abort` restored the main checkout exactly; re-running the merge pinned with `git -C <worktree>` completed cleanly, with no conflicts at all. The conflicts were entirely an artifact of running the command in the wrong checkout.

This is the third recorded occurrence of the class, not the first. 2026-08-06: a piped `git checkout` failed silently in the main checkout and the following rebase rewrote an unrelated PR's branch (the incident behind AGENTS.md's "Worktree ownership" rule). 2026-08-10: a session resumed after a break with its shell silently back in the main checkout, and two merges landed on local master before being caught (auto memory [claude] - recorded only in session memory, never promoted to this store, which is likely why the class recurred: no prior-art search over `docs/solutions/` could find it).

## Guidance

Pin every git command that touches history to an absolute path with `git -C /abs/path/to/worktree ...` (or set `W=<abs path>` once and use `git -C "$W" ...` throughout). Never trust session cwd in a session that runs background tasks, uses tools that reset or leak cwd, or spans many turns. `cd`-then-run is fragile because the `cd` and the run can be separated by a cwd reset that happens invisibly in between; `git -C` makes each command self-contained regardless of what the shell's cwd currently is.

Before any history-changing command - merge, rebase, reset, `commit --amend`, push --force - verify with `git -C <path> branch --show-current` first. That one line is cheap insurance against acting on the wrong checkout.

Two tells catch this class of mistake when the pin was skipped:

1. **Conflicts in files your branch never touched.** The first move on any unexpected conflict should be `pwd` + `git branch --show-current`, because a diverged wrong checkout manufactures conflicts that look real but aren't - they're a side effect of merging on the wrong branch, not evidence of a genuine divergence on the intended branch.
2. **Reaching for `git checkout <branch>`.** This is the existing tell from this repo's AGENTS.md "Worktree ownership" section: reaching for `git checkout <branch>` is itself the tell that you are in the wrong directory. That rule traces back to a 2026-08-06 incident where a rebase in the main checkout rebased an unrelated PR's branch out from under it. This incident is the merge-shaped sibling of that checkout-shaped tell - same root cause (untrusted cwd), different git subcommand.

## Why This Matters

Backgrounded commands run detached and do not preserve the foreground shell's directory, and the harness can reset the shell cwd to the project root between commands. That reset is silent from the perspective of the next command - there is no error, just a command that runs somewhere other than where the agent believes it is. In a worktree-based workflow, "somewhere else" usually means the main checkout, which has its own branch, its own history, and its own uncommitted state. A history-changing command run there doesn't fail loudly; it succeeds, on the wrong branch, and produces output (conflicts, commits, rebased history) that looks plausible enough to investigate on its own terms before anyone thinks to check `pwd`. That's expensive: it burns investigation time chasing a phantom conflict, and in the 2026-08-06 sibling incident it corrupted an unrelated PR's branch outright.

## When to Apply

- Any session using `.claude/worktrees/<slug>` isolation, especially sessions that also run background tasks or long-lived tool calls.
- Any command in the history-changing set: `merge`, `rebase`, `reset`, `commit --amend`, `push --force`, and `checkout` (branch switches).
- Multi-turn sessions where a `cd` into the worktree happened many turns before the git command that depends on it - the two are not adjacent, so nothing guarantees the cwd held in between.
- Whenever an unexpected conflict appears, before diagnosing the conflict's content: first confirm location and branch.

## Examples

Wrong shape - trusts a `cd` that happened turns earlier and may have been silently reset:
```
# (cd into worktree happened many turns ago; background tasks ran since)
git fetch origin
git merge origin/master
# => conflicts in README.adoc and src/docs/README_TEMPLATE.adoc,
#    files the intended branch never touched
```

Right shape - self-contained, no dependency on current shell state:
```
W=/Users/astubbs/github/parallel-consumer/.claude/worktrees/throttling-ideation
git -C "$W" branch --show-current   # verify before touching history
git -C "$W" fetch origin
git -C "$W" merge origin/master
# => clean merge, no conflicts
```

Prevention habit: run `git -C <path> branch --show-current` before every history-changing command and confirm it prints the branch you expect - not the one you assume.

## Related

- AGENTS.md "Worktree ownership" (grep: `Reaching for git checkout <branch> is the tell`) - the standing rule this extends; this incident adds the merge-shaped tell and the `git -C` pinning discipline.
- `docs/solutions/workflow-issues/compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md` - the read-only sibling class: tools misreading repo identity in worktrees. Same family (location is untrusted), different fix.
