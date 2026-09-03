# Integration gate wall time - the ce-optimize run, and what is still open

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

The `Integration Tests` lane is the PR build's critical path at 10m20s on master, against ~8m for
the next slowest. This branch carries a `ce-optimize` run against it.

**What has LANDED on this branch:** the measurement of where the time goes, the 2x2 factorial that
closed `forkCount` as a lever, two corrected stale claims, and a flake sighting. The full write-up
is [`docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md`](../plans/2026-09-03-001-investigate-integration-gate-wall-time.md);
this note carries only what is still moving.

## Settled, so nobody re-runs it

- **`forkCount` 4 -> 6 is HARMFUL here.** 469s of failsafe against a 420s baseline, because the
  same tests cost 11% more CPU time under six forks, plus a first-ever timeout failure in
  `ManagedPCInstanceLifecycleTest`. With thread-parallelism already closed in 2026-07, **both
  parallelism directions for this lane are now measured and closed.** The lever is work reduction.
- **Splitting `Rebalance857CommitSyncDeadlockProbeIT` four ways is green, free, and buys nothing
  at forkCount=4** - it removes a 339s tail that was never the binding constraint (work/4 = 382s
  was). It is a precondition for later work-reduction wins, not a win. Measured on
  `optimize/ig-exp002-probesplit`, which is NOT merged - see below.

## Open

- **Whether to take the probe split at all.** It is proven green and costs nothing, but it edits a
  calibrated instrument for zero measured gain today. That is a judgement for the author, not
  something to merge silently. Branch `optimize/ig-exp002-probesplit` holds it.
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
