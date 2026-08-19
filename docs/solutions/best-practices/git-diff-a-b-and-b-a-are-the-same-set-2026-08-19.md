---
title: "`git diff A B` and `git diff B..A` are the same set, so an overlap check written that way finds total overlap"
date: 2026-08-19
category: best-practices
module: development_workflow
problem_type: logic_error
component: development_workflow
severity: medium
applies_when:
  - Computing which files a branch changed versus which files another ref changed
  - Checking whether incoming commits touch the same files as your branch
  - Any comm/set operation over two `git diff --name-only` outputs
---

# `git diff A B` and `git diff B..A` are the same set

## What happened

Asked whether master's incoming commits touched any file a PR also touched, the check was:

```bash
git diff --name-only origin/master HEAD > pr-files.txt        # intended: the PR's files
git diff --name-only HEAD..origin/master > incoming.txt       # intended: master's files
comm -12 <(sort pr-files.txt) <(sort incoming.txt)            # intended: the overlap
```

It reported that **every** incoming file overlapped. That looked like an alarming result and was
pure artefact: both commands compute the symmetric difference between the same two commits, so the
two files were the same set, and `comm` intersected a set with itself.

A second, subtler version of the same error followed: `pr-files.txt` was regenerated *after* a
`git fetch`, so `origin/master` had moved and the "PR's files" now included master's newer commits
as well.

## Why it is worth writing down

The failure is silent and plausible. `comm` succeeded, the output was a believable list of real
paths, and the conclusion - "these overlap, look carefully" - was the kind of thing a careful
reviewer wants to hear. Nothing distinguished it from a true positive except knowing the identity.

## The correct forms

- **What my branch changed:** `git diff --name-only $(git merge-base <base> HEAD) HEAD`, or the
  three-dot shorthand `git diff --name-only <base>...HEAD`.
- **What the incoming commits change:** `git diff --name-only HEAD...<base>` - three dots, which is
  "changes on `<base>` since the merge base", not two.
- **Two-dot `A..B` on `git diff` is not a range.** `git diff A..B` is the same as `git diff A B`;
  only `git log` treats `..` as a range. `git diff A...B` is the one that means "since the merge
  base".

Pin the base explicitly when it matters. Recomputing against `origin/master` after a fetch silently
changes the question being asked.

## The cheap check

Before trusting any set operation over two diffs, ask what a **negative** result would look like. If
"no overlap" is not reachable from the way the sets were built - here it was not, because they were
the same set - the operation is not testing what it appears to.
