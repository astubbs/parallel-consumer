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
They exist so the negative results keep their evidence.

**One of them DID ship, contradicting what this section used to claim.** `perf/lock-free-mailbox` is
merged into the integration branch: `AbstractParallelEoSStreamProcessor#workMailBox` is a
`CountedTransferQueue` today, carrying an `EXPERIMENT:` comment. Its measurement is **+3.3% at 100ms
and -2.7% at 0ms**.

**Read that trade the right way round.** A 0ms delay means the user function does nothing - it is a
DIAGNOSTIC operating point, where PC's own overhead becomes visible because nothing else is in the
measurement. It is not where anybody runs. A user with no user function has no reason to use this
library. So the change is worth +3.3% where real work happens and costs 2.7% where none does: **keep
it and delete the `EXPERIMENT:` label.** It should not sit in the tree labelled as an open question
indefinitely.

**Dispatched 2026-08-21, returned by 2026-08-22.** Each was a separate agent and **none of them
could see any of the others' code**, which is where the merge conflicts and the open regression
below came from.

| Branch | What it did | Landed on the trunk? |
|---|---|---|
| `fix/conservation-load-gate` | Replaced the drifting per-shard counter feeding the broker-poller gate with an `admitted - retired` conservation figure. The risk was exhaustiveness - every in and out path must be counted or it drifts silently, with no clamp. Also found the drift clamp's "race condition" comment was **wrong**: two deterministic conditional-decrement bugs | **yes** |
| `perf/direct-pull-measured` | Finished the blocking wait the 2022 branch left commented out. **3.2x at 10 workers** (26,869 to 86,179 msg/s) and **-95% at 5,000** | **yes** |
| `test/direct-pull-coverage` | The tests that should have been written before direct pull was merged, not after | **yes** |
| `feats/virtual-threads` | `ExecutionMode` option and the pool-queue gate that blocked virtual threads. **1.8x at 100ms/5,000 and 3.0x at 2ms/5,000** | **yes**, `cb657b07b` |
| `test/bench-all-engine-arms` | Reactor, Mutiny and `ProxyProcessor` arms. Every cross-engine claim previously rested on Vert.x plus an assumption | **not yet** |
| `fix/reactor-empty-publisher-stall` | The defect the arms branch found. **Cut from `master`, so it is independently mergeable** | **yes**, and it has its own PR |

## Independent fixes get their own branch off master, and their own PR

**Antony's rule, 2026-08-22.** A fix that stands on its own and applies to `master` is cut **from
`master`** and then **merged into** the active work - never developed on top of it. It keeps the fix
isolatable, so it can land on `master` long before the large research branch it happened to be found
during.

**And it earns its own PR, which is the exception to "PRs are expensive."** The asymmetry is the
argument: a complex PR may not merge for a long time, while *every* open PR and `master` benefit from
the independent fix the moment it lands. Folding it into the big PR makes the big PR's merge date the
fix's merge date, for everyone.

**Expect the merge back into the trunk to drag in whatever `master` has moved on by, and expect
conflicts where `master` and the trunk touched the same methods. That cost is the point, not a reason
to cherry-pick** - cherry-picking looks cleaner and destroys the property being bought. Merging
`fix/reactor-empty-publisher-stall` produced four semantic conflicts, none of them caused by the
reactor change itself; see commit `416ee19f6` for what each one was and how it was resolved.

| Branch, cut from `master` | PR | What it is |
|---|---|---|
| `fix/reactor-empty-publisher-stall` | astubbs#329 | `ReactorProcessor` completed a record on its first emitted item rather than the terminal signal, so an empty publisher stalled the consumer silently |
| `docs/release-note-trailer-convention` | astubbs#330 | The `Release-Note:` commit trailer. It existed only on this trunk, so it was invisible to any branch cut from `master` - which is exactly how astubbs#329's author came to look for it, find nothing, and correctly decline to invent it |
| `fix/close-shuts-down-worker-pool` | - | In flight. `innerDoClose` shuts the worker pool down outside any `finally`, so a close whose drain throws leaks non-daemon threads for the life of the JVM |

## The open regression the merge produced

`OrderingModeDispatchParityTest` **fails on `cb657b07b`** and is deliberately left failing rather than
tuned or disabled - it was written to catch this shape and it caught one.

`UNORDERED` dispatch is **2 to 2.3x slower** than before the three branches merged (111-223ms across
three runs, against a 97ms baseline) while **`KEY` is unchanged or faster** (24-45ms against 43ms).
Noise moves both arms; this moves one. `UNORDERED` is the mode that walks the whole in-flight prefix
instead of breaking after the head record, so the suspects are the two changes that added per-entry
work to that walk: **conservation's admission and retirement bookkeeping**, and **direct pull's claim
CAS** (`isAvailableToTakeAsWork() && onQueueingForExecution()` now runs on every entry examined, which
in `UNORDERED` is hundreds per pass).

The three branches landed as separate merges, so `git bisect` over them answers it directly. The
busy-shard in-flight count in `next-direct-pull-unordered-selection.md` would not fix this one -
`UNORDERED` ignores it by design.

`DirectPullEngineParityTest` also timed out once in the full suite. It passes 4/4 in isolation; that
is machine contention, not a merge break.

## What the engine-arms branch found on its way

**A correctness defect in a shipped module**, written up in
`bug-reactor-stalls-on-a-publisher-that-emits-nothing.md` **on that branch**: `ReactorProcessor.react`
subscribes without an `onComplete` consumer, so a user function returning `Mono.empty()` or
`Mono<Void>` never completes its record and the consumer stalls silently. Mutiny does it correctly,
which makes it a Reactor defect rather than an `ExternalEngine` one.

**Three harness defects, one of which invalidates earlier bench data.** `prepare()` cached compiled
classes, so a stale `BENCH_WORK` directory could make a whole sweep run a build from hours earlier -
`BENCH_ASYNC_STUB` ignored and all three new modes falling through the old template's `else` branch
into **the Vert.x arm**. Four "engines" that were one engine. It was caught only because the Vert.x
control disagreed with a committed figure. Also: arm artifacts were per-sweep rather than per-mode, so
adding `mutiny` to a version bisect made *every* mode report `COMPILE_FAILED`; and `run_one` sent
stderr to `/dev/null`, discarding exactly the signal `bench/conf/logback.xml`'s own header says it is
pinned at WARN to preserve.

**No numbers were published from that branch, deliberately** - everything gathered was taken while
another session held ~1,000% CPU against the same broker, and the same operating point returned
9,050 msg/s and then 1,883 four minutes later. `bench/README.md` now records that `peak_in_flight` is
the load-robust column and `msg_per_sec` is not. See `branch-parallel-measurement-contamination.md`.

## Every branch from this session, pushed 2026-08-22 - and what each is FOR

All on origin now. Divergence and merge state are `git` questions and are deliberately not recorded
here; **what each branch means is not.**

**Merged into `research/market-analysis-recut`** - kept as named refs so a bisect can reach them:

| Branch | What it is |
|---|---|
| `fix/conservation-load-gate` | Replaced the drifting per-shard counter with an `admitted - retired` figure. Found the drift clamp's "race condition" comment was wrong - two deterministic conditional-decrement bugs |
| `perf/direct-pull-measured` | Finished the blocking wait the 2022 branch left commented out. 3.2x at 10 workers, -95% at 5,000 |
| `test/direct-pull-coverage` | The tests that should have preceded the merge above, not followed it |
| `feats/virtual-threads` | `ExecutionMode`, and the pool-queue gate that blocked virtual threads |
| `test/bench-all-engine-arms` | Reactor, Mutiny, Vert.x and proxy arms, plus three harness defects - one of which could silently run three "engines" as the Vert.x arm |
| `fix/reactor-empty-publisher-stall` | astubbs#329. A user function returning an empty publisher stalled the consumer silently |
| `perf/direct-pull-scan-collapse` | `ShardOccupancy`. 440 entries examined per record dispatched at 5,000 in flight, down to 1.00 |

**Not merged, awaiting a decision:**

| Branch | Why it is not merged |
|---|---|
| `perf/unordered-available-queue` | Deletes `ShardOccupancy`, -91 lines, **zero throughput change** - and adds a second retirement rule plus a revocation-timing change in `confluentinc#857` territory. Its own verdict: marginal, take it for the code not the numbers |
| `fix/close-shuts-down-worker-pool` | Cut from `master`, correct, and **superseded** - Antony has his own close-path PRs. Do not open another |
| `docs/release-note-trailer-convention` | astubbs#330, against `master`. Arrives here with the next master merge |
| `docs/direct-pull-claim-check-then-act-diagnosis` | The diagnosis that preceded the atomic-claim fix. Its findings are already on the integration branch; the branch is the working record |

**Experiments** - each a hypothesis, an implementation, a measurement, and a reason it did not ship.
Pushed so the negative results survive the machine, not because any is a merge candidate:

| Branch | What it measured |
|---|---|
| `perf/split-shard-inflight` | Split shard state into selectable and in-flight. **10x cheaper dispatch, 0% end to end** - and it found that in-flight records staying in the shard is *how ordering is enforced*, at the cost of ten failing tests |
| `perf/lock-free-worker-queue` | `LinkedTransferQueue` for the pool queue, then a counted variant. **69% and 71% WORSE** - a lock can be the cheap way to park |
| `perf/lock-free-mailbox` | Lock-free mailbox. **+3.3% at 100ms, -2.7% at 0ms**, and it is **merged into the integration branch** carrying an `EXPERIMENT:` comment. The trade favours keeping it: 100ms is where a real user function sits, 0ms is where there is no user function at all. Needs the label removed, not the change reverted |
| `perf/resume-shard-scan` | Resume the dispatch scan instead of restarting it. **+0.2%**, and cut from the wrong base |

**Deliberately NOT pushed**: `research/market-analysis`. Superseded by the re-cut, and its history
carries redacted material.

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
- The GitHub Support purge request has been **decided against** - the old objects stay unreachable
  rather than purged.
- The returned agent worktrees under `.claude/worktrees/agent-*` still pin their branches. Leave them
  until the `UNORDERED` bisect is done - the bisect needs those branch tips.
- `~/.m2` was refreshed by the engine-arms agent installing six modules from the same base another
  session had installed from an hour earlier. Benign, recorded because shared state should be.
