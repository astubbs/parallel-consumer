---
title: "Re-cut branch history without interactive rebase, and verify the tree is identical"
date: 2026-08-10
category: workflow-issues
module: git-history
problem_type: workflow_issue
component: development_workflow
severity: medium
applies_when:
  - "Preparing a long-running branch for merge whose commits are honest but noisy - lots of 'fix the previous commit' churn - and it needs to read as a handful of atomic commits"
  - "Re-narrating history from an agent session where git rebase -i or git add -i is not available because there is no interactive terminal"
  - "A branch needs to be regrouped into commits that each tell one story before requesting review"
tags:
  - git
  - rebase
  - history-rewrite
  - force-push
  - merge-prep
  - non-interactive
  - verification
---

# Re-cut branch history without interactive rebase, and verify the tree is identical

## Context

astubbs/parallel-consumer#271 (tracking issue astubbs/parallel-consumer#255) is a long-running branch
that accumulated a large number of small "fix the previous commit" commits - the kind of thing that is
honest history but a poor review artifact, because a reviewer has to reconstruct the real change by
reading through its own corrections. Before merge it was re-cut into a small set of commits that each
tell one story.

The obstacle: `git rebase -i` and `git add -i` both require an interactive terminal to select and
reorder hunks, and neither is available to an agent running non-interactively. `AGENTS.md`'s existing
*PR Discipline* guidance on re-cutting (`git reset --mixed <merge-base>`, then "restage into a handful
of atomic commits") also implicitly assumes an interactive staging step such as `git add -p`. This doc
is the technique for when that assumption does not hold, plus the one check that makes any re-cut - interactive
or not - safe to force-push.

## Guidance

**1. Record the original head before touching anything.**

```bash
git branch backup/pre-recut-<slug> HEAD
# or: git tag backup/pre-recut-<slug> HEAD
```

This is not optional and it is not cleanup-later. Until the new history is verified (step 4), the
backup ref is the only thing that makes the rewrite reversible.

**2. Start a fresh branch from the merge base.**

```bash
git fetch origin master
git switch -c <slug>-recut $(git merge-base HEAD origin/master)
```

Fetch first and use the merge-base, not a remembered `origin/master` - a stale ref silently drops
whatever master gained since the branch was cut, and a wrong base makes files the branch never touched
show up staged.

**3. Build each new commit by diffing two points and applying the diff to the index directly**, instead
of picking hunks interactively:

```bash
git diff --binary <from> <to> -- <paths> | git apply --index
git commit -m "the message the new narrative wants"
```

`<from>` and `<to>` are any two commits (or the working tree) on the *original* branch - they do not
have to be adjacent, which is what makes this useful for collapsing a run of fixup commits into one.
`--binary` is not decorative: a patch generated without it silently drops binary file changes, and
`git apply` on the truncated patch succeeds anyway, so the omission produces no error at any step -
only a tree that is quietly missing content.

Write the commit message for the story this new commit tells, not the message the original commit
had - the whole point of the re-cut is that the grouping changed, so the old messages usually no longer
describe what's actually in the new commit.

**4. Verify the resulting tree is identical to the original before doing anything else with the new
branch.** This is the step the rest of this doc is about - see *Why This Matters*.

```bash
git diff <original-head> <new-head>          # must print nothing
# or, equivalently:
git rev-parse <original-head>^{tree}
git rev-parse <new-head>^{tree}              # must match
```

## Why This Matters

`AGENTS.md`'s own re-cutting guidance already states this requirement for the interactive case:

> Verify the re-cut with `git diff <old-tip> HEAD` - it must be empty, proving history changed and
> content did not.

That requirement does not relax for the non-interactive technique above - if anything it matters more,
because diff-and-apply is a more mechanical, more error-prone way to reconstruct history than picking
hunks by eye. A file whose final content came from several original commits interleaved with unrelated
changes can fail to apply cleanly once those commits are regrouped, and a `git apply` failure is loud.
A *silent* loss - a hunk that applied but landed against the wrong base, or a binary file dropped by a
non-`--binary` patch - is not loud. It looks like a successful commit.

Without the tree-identity check, a re-cut is an unverified rewrite of work that already passed review
and testing, and the failure mode is specifically silent: the branch still builds, its tests still
pass, and it is simply missing a change nobody remembers adding. `git diff <original-head> <new-head>`
turns that silent loss into a mechanical, unambiguous signal - if it is non-empty, the diff itself shows
exactly what was lost or duplicated, which is more useful than a test failure days later that only
narrows the search.

This is also why this doc cites astubbs/parallel-consumer#271 rather than any commit SHA from the
session that produced it: a re-cut replaces the original SHAs with new ones by design, so a SHA
recorded here would already be dangling by the time anyone reads it. The PR number is the durable
anchor; the commits underneath it are exactly what a re-cut is entitled to change.

## When to Apply

- Only when nobody has built on top of the branch yet - a re-cut changes every commit's identity, so
  anyone with a fork or a local branch based on the old tips loses their base.
- At merge preparation, when the branch is deliberately being re-narrated for review or for the
  permanent log - not while a PR is under active review. Doing it mid-review invalidates the anchors
  that existing review comments point at (`git diff <sha>` links, "see line N of commit X").
- Force-push the result with `--force-with-lease`, never a bare `--force`, so a concurrent push from
  someone else is detected and refused rather than silently overwritten:

  ```bash
  git push --force-with-lease origin <slug>-recut:<slug>
  ```
- Run the tree-identity check (step 4 above) before the force-push, not after - the backup ref makes
  recovery possible, but checking first avoids ever needing it.

## Examples

**Recovering a file that will not apply cleanly once commits are regrouped.** If a source file was
touched by three of the original small commits and the diff-and-apply for the middle one fails against
the new commit's base, do not fight the patch. Take the file from the final state directly, since the
target is the final tree, not any intermediate one:

```bash
git checkout <original-head> -- path/to/File.java
git add path/to/File.java
```

This is legitimate specifically because step 4 checks the *end state*, not the path taken to get
there - recovering a file wholesale from the final tree and recovering it via three separate applied
diffs are indistinguishable once the tree-identity check passes.

**Reading a failed check.** If `git diff <original-head> <new-head>` is non-empty, read the diff as a
todo list, not as a failure to abandon: each hunk it shows is either a change that needs to be folded
into one of the new commits (loss) or a hunk that was applied twice (duplication). Fix the specific
new commit responsible, re-run the check, and only force-push once it is empty.

## Related

- `AGENTS.md`, *PR Discipline* - "Before merging, recommend a merge strategy" - the existing guidance
  for the interactive re-cutting case (`git reset --mixed <merge-base>`), including the same
  `git diff <old-tip> HEAD` verification this doc generalizes to the non-interactive technique.
- `AGENTS.md`, *Worktree ownership* - re-cutting should happen in a dedicated worktree, same as any
  other branch work, so a mistaken force-push cannot land on a shared checkout.
- astubbs/parallel-consumer#271 (tracking issue astubbs/parallel-consumer#255) - the branch this
  technique was developed for.
