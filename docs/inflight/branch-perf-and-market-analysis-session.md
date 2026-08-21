# Branches: the performance and market-analysis session

<!-- inflight-type: branch -->
<!-- inflight-impact: coordination -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-21. **What each branch is for and what state it is in** - the part no git command
answers. Divergence, ahead/behind counts and whether a branch merges cleanly are all `git` questions
and are deliberately not recorded here; they go stale within a day.

| Branch | On origin | What it is |
|---|---|---|
| `feats/classic-vertx-demo` | **yes** | The 2021 asciinema demo, rescued from the stranded `presentation` branch and made to run again. First commit is the original verbatim, author and date preserved. **The only branch here with finished, shippable work on it.** |
| `perf/throughput-regression-since-0-3` | **yes** | The version bisect, the `bench/` harness, and the five-year `ExternalEngine` regression it found. **History was rewritten and force-pushed** - see below. |
| `research/market-analysis-recut` | no | The market analysis, the four-arm benchmark comparison, the thread-ceiling investigation, licensing, franz-go, the landing page. **The trunk for everything below.** |
| `research/market-analysis` | no | **Superseded. Delete.** Pre-re-cut, its history carries redacted material. |

**Experiments - each a hypothesis, an implementation, a measurement, and a reason it did not ship.**
None is a merge candidate; they exist so the negative results keep their evidence.

| Branch | Base | What it measured |
|---|---|---|
| `perf/resume-shard-scan` | `rename/master-packages` | Resume the dispatch scan instead of restarting it. **+0.2%.** Also on the wrong base - see the base trap below |
| `perf/split-shard-inflight` | recut | Split shard state into selectable and in-flight. **10x cheaper dispatch, 0% end to end.** Found that in-flight records staying in the shard is how ordering is enforced |
| `perf/lock-free-worker-queue` | recut | `LinkedTransferQueue` for the pool queue, then a counted variant. **69% and 71% WORSE** - a lock can be the cheap way to park |
| `perf/lock-free-mailbox` | recut | Lock-free mailbox. **+3.3% at 100ms, -2.7% at 0ms** - real, tiny, a trade |

**In flight, dispatched 2026-08-21.**

| Branch | What it is doing |
|---|---|
| `fix/conservation-load-gate` | Replace the drifting per-shard counter feeding the broker-poller gate with a conservation figure. The risk is exhaustiveness: revocation and stale-removal paths must be counted or it drifts silently, with no clamp |
| `perf/direct-pull-measured` | Finish the blocking wait the 2022 branch left commented out, and measure the one objection that survives - whether N-way concurrent shard access costs anything |
| `test/bench-all-engine-arms` | Add Reactor, Mutiny and `ProxyProcessor` arms to the harness. **Every cross-engine claim currently rests on Vert.x plus an assumption** |

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
