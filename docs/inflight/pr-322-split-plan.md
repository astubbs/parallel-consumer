# The astubbs#322 split - a stack of three, and what must happen at each merge

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

astubbs#322 was 66 commits over 206 files carrying ten unrelated workstreams. It is now a **stack of
three**, each branch cut from the one below it. Delete this note when all three have merged.

**astubbs#323 and astubbs#324 both merged on 2026-08-20.** Only astubbs#322 remains, it no longer
stacks on anything, and it needs master merged in rather than a rebase. Those merges also put the
inflight tag gate on master, so any note it ADDS must carry `inflight-type` and `inflight-impact` or
the gate fails on a diff that looks innocent.

| Order | PR | Branch (on origin) | Base | What it is |
|---|---|---|---|---|
| 1 | astubbs#322 | `fix/909-load-reproduction` | `origin/master` | product code + its tests |
| 2 | astubbs#323 | `split/docs-ledger-and-plans` | PR 1's branch | ledger, plans, solutions, upstream manifest - **MERGED 2026-08-20 as `a80f2bbd1`, squash** |
| 3 | astubbs#324 | `split/tooling-gates-and-harness` | PR 2's branch | `bin/` gates, workflows, `.claude/` hooks, and the docs that cite them - **MERGED 2026-08-20** |

(The table first named local-only working branches - `stack/1-code`, `stack/2-docs`,
`stack/3-tooling` - which exist on no remote, so a fresh clone could not run any command that cited
them. File counts are deliberately gone: three different counts were in circulation and none stayed
true.)

**2 and 3 have both merged, so only 1 is left and the bottom-up rule has nothing left to order.**
It is kept because the rest of this file still reads against it: each PR carried
`depends on astubbs/parallel-consumer#<parent>` and the PR-dependency gate enforced it, so a child
staying red until its parent merged was the **expected state, not a fault**.

## What must happen at each merge

- **At every step, re-check `git rev-list --count master..origin/master` is 0 before measuring
  anything.** This whole split was first built against a local `master` 40 commits stale, which
  silently produced 207 files of "work" when the real remainder was 109. A stale base does not error.
- **1 is small enough that squashing is defensible.** 2 and 3 were not, and were not squashed: their
  commit bodies carry the reasoning the release-note generator reads, and several are the only record
  of a diagnosis.
- **The stack as first pushed (astubbs#324 head `afbae10dc`) was verified byte-identical to
  `backup/pre-split-322`** (the preserved 66-commit original, on the remote and locally) **except
  for one deliberate correction** - the confluentinc#857 seventh-sighting supersession described
  below. Commits added to astubbs#324 since then (the review-driven fixes) diverge deliberately, so
  re-verify the byte-identity claim against `afbae10dc`, not the moving tip:
  `git diff afbae10dc backup/pre-split-322`.

## Why the stack, and why this partition

The first attempt was three *independent* branches cut from master, which was wrong twice over.

**Independent branches cannot express a dependency that exists.** Docs cite Java files that the code
branch adds; the tooling branch's gates validate content the docs branch adds. Stacking makes each
PR's tree contain everything below it, so every gate sees its inputs.

**There was a genuine dependency CYCLE, and stacking alone does not break it.** Docs referenced
scripts that live in tooling (`check-docs-data.sh` failed on the docs branch: its
`docs/features/embedded-issue-tracking.yaml` pointed at a script that was not there), while tooling's
`check-inflight-tags.sh` validates tags that live in docs. Neither could go first. It is broken by
**moving the six documents that carry forward-references into the tooling branch** -
`docs/testing.md`, `docs/refactoring.md`, `docs/inflight/AGENTS.md`,
`docs/features/embedded-issue-tracking.yaml`, `core-product-log-levels-at-info.md`, and this file.
They document the tooling, so they belong beside it; the split is by *what a file depends on*, not by
what directory it sits in.

## The confluentinc#857 seventh sighting is superseded, not deleted

`PCMetricsTest.metricsRegisterBinding` was recorded in `bug-857-family.md` as the family's signature.
It is a test defect: the assertion compares `PARTITION_LAST_COMMITTED_OFFSET` (contiguous) with a
completion counter under `UNORDERED`, and workers latch before incrementing so the gap is permanent.
The entry is retained with a supersession note rather than removed - the file already records one
contamination of this kind, and the reasoning that led there is worth seeing. Mechanism:
[`bug-pcmetrics-committed-offset-vs-completion-count.md`](bug-pcmetrics-committed-offset-vs-completion-count.md).

## Still open after astubbs#322 merges

- **A conflict-marker gate.** `bin/check-conflict-markers.sh` + its self-test, wired into Repo
  Hygiene. Prompted by finding a merge committed and pushed with 305 lines of a 392-line file inside
  an unresolved conflict, on another branch, with nothing red. Nothing in `bin/` looks for markers
  today.
  <!-- file-refs: N/A - check-conflict-markers.sh is what this entry PROPOSES to write; it does not exist yet, which is the point of it -->
- **Five hook commits could not be replayed** onto current master.
  [`branch-agent-hook-commit-bodies-only-on-backup.md`](branch-agent-hook-commit-bodies-only-on-backup.md)
  owns this: it names the SHAs and records that the reconciliation commit said to name them does not
  exist.
