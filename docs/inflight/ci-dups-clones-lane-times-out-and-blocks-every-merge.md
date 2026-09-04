# `dups: clones` no longer finishes inside its cap, and it is a required check

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->

The `dups: clones` job carries `timeout-minutes: 5` and no longer completes inside it. It is a
**required** check, so while this holds **no pull request in the repository can merge** - the row
reads `cancelled`, which is neither success nor a failure anyone can act on from the checks list.

## It is deterministic, not a flake

Recorded outcomes of the lane, by date rather than by branch:

<!-- post-merge: checked-begin -->
- **2026-09-03**: two attempts on astubbs#433, both **success**, each posting the full report with
  both engines' tables.
- **2026-09-04**: three attempts on astubbs#433 and one on astubbs#443, all **cancelled**.

astubbs#443 is unrelated work on its own branch, which is what rules out branch content as the
cause. The boundary in time is master gaining astubbs#419 (the docs context query, which adds
`bin/inflight.mjs` and a large docs corpus) and astubbs#116; the first cancelled head on
astubbs#433 is the first one carrying that master.
<!-- post-merge: checked-end -->

<!-- post-merge: checked-begin -->
**This note was first written as a flake-ledger row saying the lane "tips over on a slow runner".
That was wrong** - it was drafted from a single sighting, before a fourth attempt on a fresh five
minutes failed identically.
<!-- post-merge: checked-end --> Two successes and four failures split cleanly by date, not by
runner, and the row is retired from
[`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md) because filing a deterministic
breakage among flakes invites exactly the re-run-until-green response that would hide it.

## What the evidence is

Each cancelled attempt runs the action for a few seconds over five minutes and dies with jscpd
still going - the runner's cleanup names it: `Terminate orphan process: pid (2693) (npm exec
jscpd@)`. The action runs PMD CPD **and** jscpd across the whole tree, twice (PR and base), inside
one five-minute budget, so growth in the corpus is paid four times over.

Its sibling `dups: similarity` succeeds in the same run, so this is not a superseded workflow.

**Master history cannot be used here.** The job is `if: github.event_name == 'pull_request'`, so on
master it is `skipped` - the absence of evidence, which reads as green in the checks list. Compare
against another PR, never against master.

## Not the failure the other note owns

[`ci-duplication-report-can-fail-to-post.md`](ci-duplication-report-can-fail-to-post.md) is about
`dups: clones` **finding a real clone** and being unable to post it. This is the lane **never
finishing**. In the checks list a cancelled tick and a failed tick look alike and mean opposite
things; read the job's step timing before assuming either.

## Done when

The lane completes inside its budget on an ordinary PR. The options, in the order they are worth
trying: raise `timeout-minutes` (cheapest, and buys time to measure); split the two engines into
separate jobs so neither pays the other's runtime; or scope what jscpd walks, since its
ignore-pattern currently excludes build output but not the docs corpus that grew.

Any of those is a CI change on master, which is why nothing is attempted from the feature branch
that found it.
