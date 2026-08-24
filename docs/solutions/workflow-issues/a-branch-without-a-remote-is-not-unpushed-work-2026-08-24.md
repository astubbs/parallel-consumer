---
module: none
tags: [git, worktrees, prior-art, false-negative]
problem_type: workflow-issue
---

# A branch without a remote is not unpushed work - ask ancestry, never listing

## The wrong conclusion, stated confidently

`perf/bench-arrival-and-key-skew` appeared in `git branch -a` with no `origin/` counterpart, and
that was read as "this work is unpushed, it exists on one machine, one disk failure from gone". A
stranded-work warning went into a design document and the owner was told to push it.

Every commit on the branch was already contained in a pushed branch - it had been merged into
`perf/engine-concurrency` under that name and pushed there. Nothing was at risk. The truth was the
*inverse* of the warning: the work was safe on the remote, and the branch doing the asking simply
had not merged its own base since the work arrived.

## The defect class

**A branch name's remote-tracking state says nothing about whether its commits are pushed.** Names
and commits travel independently: a merge carries the commits to another branch, the other branch is
pushed, and the original name stays local forever - reading as live, unpushed work to anyone listing
branches. The failure is silent in the worst way: the command succeeds, the conclusion feels
diligent, and the report it produces is confidently wrong.

## The method

Ask whether the **commits** are contained in a pushed ref:

```bash
git merge-base --is-ancestor <branch> origin/<pushed-ref> && echo contained
```

Never `git branch -a`, and never `git for-each-ref --format='%(upstream)'` - both interrogate the
name. Sweeping a family of branches, loop the ancestry test against each plausible pushed base; a
branch contained in any of them is history that is already safe, not work waiting to be pushed.

The general form of the lesson: when the question is "is this safe / merged / done", interrogate the
content, not the label attached to it. The same shape as "a squash-merged PR's sha is not on master"
- the label-level search returns a confident nothing while the content-level search returns the
truth.
