# The astubbs#322 split - a stack of three, and what must happen at each merge

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

astubbs#322 was 66 commits over 206 files carrying ten unrelated workstreams. It is now a **stack of
three**, each branch cut from the one below it. Delete this note when all three have merged.

**astubbs#323 merged first, on 2026-08-20.** The two that remain no longer stack on it - their base
is master, and each needs master merged in rather than a rebase. Merging it also put the inflight
tag gate on master, so any note either branch ADDS must carry `inflight-type` and `inflight-impact`
or the gate fails on a diff that looks innocent.

| Order | PR | Branch (on origin) | Base | What it is |
|---|---|---|---|---|
| 1 | astubbs#322 | `fix/909-load-reproduction` | `origin/master` | product code + its tests |
| 2 | astubbs#323 | `split/docs-ledger-and-plans` | PR 1's branch | ledger, plans, solutions, upstream manifest - **MERGED 2026-08-20 as `a80f2bbd1`, squash** |
| 3 | astubbs#324 | `split/tooling-gates-and-harness` | PR 2's branch | `bin/` gates, workflows, `.claude/` hooks, and the docs that cite them |

(The table first named local-only working branches - `stack/1-code`, `stack/2-docs`,
`stack/3-tooling` - which exist on no remote, so a fresh clone could not run any command that cited
them. File counts are deliberately gone: three different counts were in circulation and none stayed
true.)

**Merge strictly bottom-up: 1, then 2, then 3.** Each PR carries
`depends on astubbs/parallel-consumer#<parent>`, so the PR-dependency gate enforces it - a child
staying red until its parent merges is the **expected state, not a fault**.

## What must happen at each merge

- **astubbs#325 must rename two notes when it takes master.** It was branched before status prefixes
  were removed from inflight filenames, and adds two of its own in the old shape:
  `next-truth-probes-for-internal-state.md` and `parked-chaos-crash-fidelity-variant.md`. The
  convention lands in astubbs#323, which merges first, so by the time astubbs#325 merges master those
  names contradict the rule that a filename identifies WHAT a note is and never its status. Rename to
  an area prefix - `test-` for both, on subject - as a pure `git mv` with citations repointed
  separately, and re-tag them under the current vocabulary while there. Everything else old-named on
  that branch is inherited and resolves on its own when it takes master; only these two are its own.
  Left for that branch's owner: its worktree is held by another session.


- **After 1 merges:** GitHub retargets 2 onto master automatically. Confirm 2's base actually moved
  before merging it; if the retarget did not happen, merge master into 2 rather than rebasing, so the
  stack keeps its history.
- **After 2 merges:** same for 3.
- **At every step, re-check `git rev-list --count master..origin/master` is 0 before measuring
  anything.** This whole split was first built against a local `master` 40 commits stale, which
  silently produced 207 files of "work" when the real remainder was 109. A stale base does not error.
- **Do not squash 2 or 3.** Their commit bodies carry the reasoning the release-note generator reads,
  and several are the only record of a diagnosis. 1 is small enough that squashing is defensible; the
  other two are not.
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

## Still open after these three merge

- **A conflict-marker gate.** `bin/check-conflict-markers.sh` + its self-test, wired into Repo
  Hygiene. Prompted by finding a merge committed and pushed with 305 lines of a 392-line file inside
  an unresolved conflict, on another branch, with nothing red. Nothing in `bin/` looks for markers
  today.
  <!-- file-refs: N/A - check-conflict-markers.sh is what this entry PROPOSES to write; it does not exist yet, which is the point of it -->
- **Five hook commits could not be replayed** onto current master (all collide with astubbs#299,
  `da049f703`). Their content is present and verified; their bodies live on `backup/pre-split-322`
  and are named in the tooling branch's reconciliation commit.
