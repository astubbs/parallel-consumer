# Integration gate wall time - the ce-optimize run, and what is still open

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->
<!-- inflight-vetted: 2026-09-07 - the probe split has MERGED (astubbs#442, `c668acbfa`, which also sharded the gate), so that open question is retired and the Settled entry rewritten. Still open and verified against the tree: `PartitionStateCommittedOffsetIT.triggerCompactionProcessing` still sleeps flat with its `// or wait?` beside it and `optimize/ig-exp004-compaction-poll` still exists; the 857 probe still runs twenty repetitions in total; no workflow step prints `nproc` in `.github/workflows/maven.yml`. The 10m20s headline is now unmeasured against the sharded lane -->

<!-- post-merge: checked-begin - both sentences name astubbs/parallel-consumer#439 explicitly and
     are written in the past tense, so they stay true once that PR has merged and its branch is
     gone. Nothing here says "this branch" or "this PR". -->
The `Integration Tests` lane is the PR build's critical path at 10m20s on master, against ~8m for
the next slowest. A `ce-optimize` run against it landed in astubbs/parallel-consumer#439.

**What astubbs/parallel-consumer#439 landed:** the measurement of where the time goes, the 2x2
factorial that closed `forkCount` as a lever, two corrected stale claims, and a flake sighting.
<!-- post-merge: checked-end -->
The full write-up
is [`docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md`](../plans/2026-09-03-001-investigate-integration-gate-wall-time.md);
this note carries only what is still moving.

## Settled, so nobody re-runs it

- **`forkCount` 4 -> 6 is HARMFUL here.** 469s of failsafe against a 420s baseline, because the
  same tests cost 11% more CPU time under six forks, plus a first-ever timeout failure in
  `ManagedPCInstanceLifecycleTest`. With thread-parallelism already closed in 2026-07, **both
  parallelism directions for this lane are now measured and closed.** The lever is work reduction.
- **Splitting `Rebalance857CommitSyncDeadlockProbeIT` four ways is done.** It landed with
  astubbs/parallel-consumer#442, which also sharded the gate into a heavy set and a catch-all. Four
  package-private classes of five repetitions each now live in one file; the base class holds the
  body. It removed a tail that was never the binding constraint at forkCount=4, so it was a
  precondition for later work-reduction wins rather than a win itself.

## Open

- **What the lane costs now.** The 10m20s figure above predates astubbs/parallel-consumer#442's
  shard and astubbs/parallel-consumer#457's job batching, and nothing here has re-measured it. Take
  the numbers below as the pre-shard picture until a fresh sample says otherwise.
- **The compaction poll.** `PartitionStateCommittedOffsetIT.triggerCompactionProcessing()` sleeps a
  flat 20s from two call sites in a seven-test class - 60s+ of that class's 159s - with the author's
  own `// or wait?` beside it. Built and smoke-clean on `optimize/ig-exp004-compaction-poll`,
  unmeasured. Turns the 20s into a deadline rather than a duration.
- **Cutting the 857 probe's per-PR repetitions from 20.** The largest single work reduction
  available (~205s), but it trades per-PR detection power and needs the full-20 lane to survive
  somewhere. Author's call.
- **`compiler:testCompile` at 60s.** This lane passes `-DskipUTs=true`, so it compiles all of
  `src/test/java` and runs none of it. Feasibility genuinely open - shared utilities and the
  generated Truth subjects live there.
- **The "2-core hosted runner" premise.** Flagged, not settled: the runner reports `ubuntu-24.04`
  and GitHub moved public-repo runners to 4 vCPU in early 2024, while `docs/self-hosted-runner.md`,
  `docs/ci.md` and the prior `unit-gate` run all reason from 2 - that run built a 2-CPU Docker
  replica to imitate this runner. One `nproc` line in the CI step would settle it permanently.

## For whoever measures this lane next

The harness is a `workflow_dispatch` of `maven.yml` with `suite=integration`, one sample per
branch (the concurrency group keys on the ref with `cancel-in-progress`, so a second dispatch at
the same ref kills the first). Two things it cost real time to learn:

- **Rank on `core_failsafe_seconds`, never `job_seconds`.** A build that fails at `verify` skips
  the other ten modules, so its `job_seconds` omits ~120s. The worst arm of the four read as the
  best one on `job_seconds`.
- The scratch space is under `.context/`, which is **gitignored and does not travel with the
  branch**. A prior CI-measurement run (`chaos-ci-perf`, whose sample branches are still on origin)
  left no reusable harness for exactly this reason.
