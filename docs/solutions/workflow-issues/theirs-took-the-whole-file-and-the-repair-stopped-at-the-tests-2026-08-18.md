---
title: "`--theirs` took the whole file, and the repair stopped where the tests did"
date: 2026-08-18
category: workflow-issues
module: parallel-consumer-core
problem_type: workflow_issue
component: development_workflow
severity: high
applies_when:
  - Updating a long-lived branch from master where several files conflict on one line each
  - Resolving a conflict with `git checkout --theirs`/`--ours` rather than editing the hunk
  - Auditing a merge that crossed a package rename
  - Deciding a merge repair is finished because the suite went green
---

# `--theirs` took the whole file, and the repair stopped where the tests did

## What happened

Updating astubbs#57 from master, five files conflicted on a single comment line each. Each was
resolved with `git checkout --theirs <file>`, which takes master's **entire** version of the file,
not the conflicted hunk. Everything else the branch had done in those files was discarded.

Two casualties surfaced because they broke tests:

- the `confluentinc#905` `SHARDS_MAX_SIZE` gauge, which failed as
  `expected: 785.0 but was: -1.0` - the "no such gauge" sentinel;
- the whole `confluentinc#893` regression test, which surfaced as nothing at all until someone
  noticed the file was absent.

Both were restored. **The repair was scoped to what the tests caught, and stopped there.**

## The part that stayed broken

A later audit of the same merges found more damage that no test could see:

- the `shards.max.size` metric's reference block in `src/docs/README_TEMPLATE.adoc` and the
  generated `README.adoc` - so `CHANGELOG.adoc` advertised a metric the metrics reference did not
  document;
- two `docs/refactoring.md` backlog entries, leaving two shipped `TODO(refactor)` comments ending
  "See docs/refactoring.md" pointing at sections that no longer existed.

Nothing fails when prose vanishes. Those losses survived weeks and more than a dozen review rounds,
because a merge that takes the other side renders as **nothing at all** - there is no `-` line in
the PR diff for a reviewer to see.

## The rename makes the recommended check useless

The obvious audit - `git diff <merge-base>..<pre-merge-tip> -- <file>` - reports that the branch
rewrote the whole file when the merge crossed the `io.confluent.*` -> `bz.stub.*` rename, and
reports a file whose path moved as a pure addition with no base to compare. Read literally it says
"the branch changed everything here", for every file, which is indistinguishable from noise and
invites giving up on the check.

Normalise the namespace on both sides before diffing (rewrite `io.confluent.parallelconsumer` and
`bz.stub.parallelconsumer` to the same token in two temp copies, then `diff -u`). That turns the
output back into the handful of real changes.

## What to do

- Prefer editing the conflict hunk. Before `--theirs`/`--ours`, prove the branch changed nothing
  else in that file.
- Afterwards, `git diff <pre-merge-tip>..HEAD -- <files>` and read **every** removal.
- Audit **every file the merge's conflict list names**, not just until the suite goes green. The
  merge commit records them: `git log -1 --format=%B <merge-sha>`.
- Run the tests - a conflict resolution is a code change - but do not treat green as proof the
  repair is complete. Green only proves the *tested* losses are back.
- When a file has vanished, `git log` will not say which merge ate it without `--full-history`.
