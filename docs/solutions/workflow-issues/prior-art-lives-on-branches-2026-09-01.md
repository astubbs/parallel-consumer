---
title: Two thirds of this repo's prior art is invisible to a working-tree grep
date: 2026-09-01
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: high
root_cause: inadequate_tooling_scope
resolution_type: workflow_improvement
applies_when:
  - Running the AGENTS.md "Before you investigate anything" checks
  - About to write "no prior art found" in an investigation write-up
  - Reading the session-start index injected by inject-recorded-knowledge.sh
  - Adding any check that enumerates docs/ with find, grep -r or ls
  - Investigating from the main checkout rather than a worktree
symptoms:
  - All six prior-art checks run, all return plausible hits, and a plan on the same defect is still missed
  - A tracking note or handoff exists but appears in no session index
  - An investigation re-derives a conclusion already written down weeks earlier
  - "Nothing found" is indistinguishable from "the corpus searched did not contain it"
tags:
  - prior-art
  - false-negative
  - tooling-scope
  - worktrees
  - branch-only-documentation
  - agent-harness
---

# Two thirds of this repo's prior art is invisible to a working-tree grep

## What happened

A session investigating astubbs/parallel-consumer#44 (confluentinc#803, the transactional revoke
wait) ran every check in AGENTS.md's "Before you investigate anything" table, from master. Each
returned hits. The write-up said what each check found, as the table asks.

It had still missed:

- `docs/plans/2026-08-18-002-fix-857-revoke-path-cluster-decomposition-plan.md` - the decomposition
  of astubbs#29 into four clusters, including the A/B soak proving cluster 1
- `docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`
  - the archaeology of the exact seam under investigation, with the constraint list any fix must
  respect
- `docs/inflight/bug-retry-queue-write-lock-on-the-rebalance-path.md` - a sibling defect in the same
  class
<!-- file-refs: N/A - all three are branch-only, which IS the finding; `check-file-refs.sh` flagged
     them on this very commit, demonstrating the point from the other side. Retrieve with
     `git show bugs/857-paused-consumption-multi-consumers-bug:<path>` for the first two and
     `git show origin/handoff/enable-large-number-of-instances:<path>` for the third. -->

None of them were reachable by any command the table gave. The user asked *"did you git grep for
`#44` in all branches?"*, and one `git grep` across every ref returned all three.

## Why the checks could not find them

**Every enumeration in the retrieval layer reads the working tree.** The table said
`ls docs/plans/`, `grep -rl <mechanism> docs/solutions/`, `ls docs/inflight/`. The session-start
hook `.claude/hooks/inject-recorded-knowledge.sh` used `find docs/solutions`, `find docs/plans` and
`grep -rl ... docs/inflight`. All of them answer for the checked-out branch.

**That is the wrong corpus, by a factor of three.** Measured on 2026-09-01 across 443 refs:

| | count |
|---|---|
| Documents under `docs/` on `origin/master` | 321 |
| Documents under `docs/` across every ref | 901 |
| **Existing only on branches that have not merged** | **580 (64%)** |

Notes, plans and write-ups are authored on the branch that produced them and stay there until it
merges - and many never do. So the majority of what the repo knows is unreachable from master, and
the retrieval layer never said so.

**The index then asserted completeness over the smaller corpus.** AGENTS.md read, of the injected
titles: *"so 'I did not know it existed' is not available as an excuse"*. That converts a structural
blind spot into confidence. A partial index that announces itself as partial costs one sentence; one
that does not is worse than no index, because a completed check carries authority a skipped one does
not.

**This failure mode was already on the record and no rule was written from it.**
`docs/inflight/handoff-bound-transactional-revoke-wait.md` says the tracking note *"previously
existed only on astubbs#29's branch, so it appeared in no session index for anyone not on that
branch - a master-state defect documented only on an unmerged PR."* It was recorded as a fact about
one document rather than as a property of the retrieval layer.
<!-- file-refs: N/A - branch-only, same reason as above; that note lives on
     origin/fix/857-bound-transactional-revoke-wait, so `git show <that-ref>:<path>` retrieves it. -->

**And the gate caught this document citing them.** `bin/check-file-refs.sh` failed the first commit
attempt on all four paths above, because they resolve on no ref this branch can see. That is the
defect reproducing itself one layer up - a citation gate is a working-tree search too - and the
markers it prescribes are what the paragraphs above now carry. Worth knowing before you read a
`file-refs: N/A` marker as an author waving a gate away: in this repo it is often the only honest
way to cite branch-only material.

## The second cause, in the same incident: a stale working tree

The same session investigated from **the main checkout**, which AGENTS.md forbids because several
sessions share it. Master advanced **151 commits** underneath it. One of the documents "missing" from
`docs/solutions/` had landed on master during the session and was absent only from the snapshot the
session opened at.

So there were two ways to get a false negative from the same commands: search the wrong branch, and
search the right branch at the wrong time. Both read as "nothing found".

## The fix

- **`node bin/prior-art.mjs <mechanism> [<mechanism>...]`** runs the checks across every ref in one
  command, groups hits by path rather than by ref, and flags each path missing from
  `origin/master`. It reports the size of the corpus it searched, so "nothing, across 443 refs" is a
  result rather than a blank line - the failure named in
  [`a-check-that-reports-success-without-having-run.md`](a-check-that-reports-success-without-having-run.md).
  It also warns when run from the main checkout, when the last fetch is over an hour old, and when
  HEAD is behind the baseline. Cost: ~0.5s.
- **The session-start index now states the gap in the same breath as the list** - the branch-only
  count, and the command that closes it - instead of implying it is the whole corpus.
- **AGENTS.md's table names the script** rather than three working-tree greps, and the
  false-completeness bullet is corrected.

## Prevention

1. **Any check that enumerates `docs/` must say which corpus it searched.** `find`, `grep -r` and
   `ls` all mean "this branch, right now". In this repo that is a minority of the material.
2. **Report the denominator with every negative result.** "Nothing" is only informative alongside
   what was looked at - the same rule the repo already applies to a clean probe verdict.
3. **A retrieval mechanism that cannot be complete must not read as complete.** The sentence
   admitting the gap is cheaper than the investigation it saves.
4. **Investigate from a worktree, not the main checkout** - not only for the write hazard AGENTS.md
   names, but because a long session's reads silently answer for the commit it started at.
5. **When a failure mode is recorded as an anecdote about one document, ask whether it is a property
   of the tool.** This one sat in a handoff for a day, correctly described, and changed nothing.

## Related

- [`a-check-that-reports-success-without-having-run.md`](a-check-that-reports-success-without-having-run.md) -
  the general form: a check whose empty output is indistinguishable from one that never ran.
- [`duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md`](duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md) -
  the same shape in a different tool: a scanner green because it was not looking where the problem is.
- [`read-the-commits-you-inherit-2026-08-10.md`](read-the-commits-you-inherit-2026-08-10.md) - the
  staleness half of this incident, from the write side.
- [`compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md`](compound-tooling-breaks-in-worktrees-and-forks-2026-08-07.md) -
  the `gh` false-negative class, which `bin/prior-art.mjs` avoids by naming the repo on every call.
- `docs/agent-harness.md` - which layers fire on their own, and which are merely available.
