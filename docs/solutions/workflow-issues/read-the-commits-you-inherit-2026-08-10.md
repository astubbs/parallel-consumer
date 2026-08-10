---
title: "Read the commits you inherit - a rebase hands you decisions, constraints, and sometimes instructions addressed to you"
date: 2026-08-10
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: medium
status: "Convention only. Nothing checks it - a green build proves the code compiles, not that the ground under your design held."
applies_when:
  - Rebasing or merging a long-lived branch onto a moved base
  - Replaying or cherry-picking work onto a branch that advanced underneath you
  - Picking up someone else's branch, or resuming your own after the base moved
  - Starting work in a fresh worktree cut from a base that is ahead of your last session
---

# Read the commits you inherit

When you merge, rebase, or replay onto a moved branch, **read the commit messages you just took
on**: `git log --oneline <old-base>..<new-base>`, then read the *bodies* of anything touching your
area.

Inheriting commits means inheriting decisions, constraints, and sometimes instructions addressed to
you. A green build proves the code still compiles. It proves nothing about whether the ground under
your design moved.

## Three things hide there, and none of them announce themselves

**Instructions to your branch.** On the Connect spike (astubbs#240, PR astubbs#269) the parent's tip
commit said, in its body, that Connect inherits a decision and should extend the second persona
rather than re-litigate whether one exists. It was never read, so it was obeyed only by luck.

**Decisions that reshape your work.** The same rebase renamed a module and its package - discovered
by tripping over a merge conflict rather than by reading. In the same window `STRATEGY.md` landed,
naming this project's guiding approach, and was discovered only because the owner asked whether it
had been read.

**Arguments against what you are about to do.** A commit you are overriding usually explains why it
did what it did. Record that reasoning where you override it, or the next reader takes your change
for an oversight and reverts it.

## Why this bites here specifically

This repo makes the failure more likely, not less:

- Every task gets its own worktree, so a branch's base is frequently behind the master the rest of
  the work moved on to.
- Commit *bodies* are load-bearing by design: release notes are generated from the log, so the
  diagnosis, the experiment and the rejected alternatives live there rather than in a PR comment.
  The most consequential sentence in a commit is often nowhere near its subject line.
- Some inherited commits ship coordination mechanisms rather than code.
  `docs/inflight/pr-strategy-doc-merge-triggers.md` names which open branches must re-check
  `STRATEGY.md` before merging *and* which explicitly must not. A branch that never reads it cannot
  know which list it is on - and "not a trigger" is only a real answer if you looked.

## The method

1. `git log --oneline <old-base>..<new-base>` - the full list, not the top few.
2. Read the body of anything touching your files, your module, or your document.
3. Ask of each: does this instruct my branch, reshape my work, or argue against what I am about to
   do?
4. When you override an inherited decision, write down the reasoning you are overriding, where you
   override it.

Effort is not a defence. Reading a dozen commit messages takes seconds, and it is exactly the kind
of scan that is cheap for an agent and tedious for a human - so the human habit of skipping it is
calibrated to a cost that does not apply here.

## Related

- [`docs/inflight/pr-strategy-doc-merge-triggers.md`](../../inflight/pr-strategy-doc-merge-triggers.md) - the
  per-branch re-check list this failure mode routes around
- `AGENTS.md`, PR Discipline - the rule that cites this write-up
