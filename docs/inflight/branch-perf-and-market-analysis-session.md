# Branches: the performance and market-analysis session

<!-- inflight-type: branch -->
<!-- inflight-impact: coordination -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-21. **What each branch is for and what state it is in** - the part no git command
answers. Divergence, ahead/behind counts and whether a branch merges cleanly are all `git` questions
and are deliberately not recorded here; they go stale within a day.

| Branch | On origin | What it is |
|---|---|---|
| `feats/classic-vertx-demo` | yes | The 2021 asciinema demo, rescued from the stranded `presentation` branch and made to run again. First commit is the original verbatim, author and date preserved. |
| `perf/throughput-regression-since-0-3` | yes | The version bisect, the `bench/` harness, and the five-year `ExternalEngine` regression it found. **History was rewritten and force-pushed** - see below. |
| `research/market-analysis-recut` | no | The market analysis, the four-arm benchmark comparison, licensing, franz-go, the landing page and its content plan. Two commits, re-cut clean. **Not pushed.** |
| `research/market-analysis` | no | **Superseded. Delete.** The pre-re-cut version, whose history carries material that was redacted. Kept only until the re-cut is accepted. |
| `perf/resume-shard-scan` | no | **Parked, measured, does nothing.** First attempt at the shard dispatch scan - resume rather than restart. See [`parked-resume-shard-dispatch-scan.md`](parked-resume-shard-dispatch-scan.md). |
| `perf/split-shard-inflight` | no | **Parked, measured, 10x dispatch and 0% end to end.** Second attempt - split the shard into selectable and in-flight state. Same note. Found that the in-flight walk is how ordering is enforced. |

## The force-push, and what it does not fix

`perf/throughput-regression-since-0-3` was rewritten on 2026-08-21 to remove a note that named an
individual and quoted a private exchange, across the six commits that carried it. `master` and
everything below `c71b07002` are untouched. Verified after pushing: the file is absent from the branch,
the name appears in no commit and no commit message.

**The old commit `0473ea520` is still fetchable from GitHub by SHA.** A force-push makes commits
unreachable, not absent. Removing them needs a GitHub Support request to purge unreachable objects,
**which has not been made**. No PR ever existed on that branch, which is the one piece of luck -
PR-referenced commits are retained permanently regardless.

## Branch bases, and a trap that cost time here

**Branch from `origin/master`, not from a local `master`.** A local `master` in this repo was found 90
commits behind, which made it look as though the `bz.stub` package rename was not yet on master. It
is - astubbs#294 is merged. A branch cut on that false reading lands in the wrong package namespace,
and merging it into a renamed branch produces **duplicate classes rather than a conflict**, which
nothing warns about.

`rename/master-packages` is the (merged, undeleted) branch behind astubbs#294. It is not a mainline
and should not be used as a base.

## Cleanup owed

- `research/market-analysis` - delete once the re-cut is accepted.
- `.claude/worktrees/perf-regression` - still pinned to the orphaned pre-rewrite `0473ea520`, which is
  a re-push hazard for the redacted content. Needs resetting to the new head.
- The GitHub Support purge request, if the old objects should actually be gone.
