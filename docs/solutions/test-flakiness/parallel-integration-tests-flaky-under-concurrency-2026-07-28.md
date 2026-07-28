---
title: Integration tests flake under parallel execution due to timing-sensitive races, not resource contention
date: 2026-07-28
category: test-flakiness
module: parallel-consumer-core
problem_type: flaky_test
component: testing
symptoms:
  - "~2 of 104 integration tests fail per run when run with parallel-tests=true"
  - "A different set of tests fails on every run (non-deterministic)"
  - "java.util.concurrent.TimeoutException: Timeout while waiting to get produce lock (was set to PT2S)"
  - "awaitility ConditionTimeoutException on 30s throughput assertions ('All keys ... should be processed within time')"
  - "InternalRuntimeException: No progress beyond N records after 1 rounds"
  - "On GitHub-hosted 2-core runners the same parallelism produces ~28 failures (CPU starvation), not ~2"
root_cause: concurrency_race
resolution_type: pending
severity: medium
tags:
  - flaky-tests
  - parallel-tests
  - junit-parallel
  - testcontainers
  - kafka
  - timeouts
  - ci
  - self-hosted-runner
---

# Integration tests flake under parallel execution due to timing-sensitive races, not resource contention

## Problem

The `ci` Maven profile disables JUnit parallel test execution (`parallel-tests=false`) with only folklore
as the reason: "it caused flaky tests." Re-enabling it (`-Dparallel-tests=true`, JUnit
`dynamic.factor=20`) makes the suite dramatically faster but the **integration** suite does not run
cleanly. This doc records what the flakiness actually is, so the reason for `parallel-tests=false` is no
longer folklore, and so nobody re-diagnoses it from scratch.

## Symptoms

- With parallelism on, ~2 of 104 integration tests fail per run, and **which** tests fail changes every
  run - `TransactionAndCommitModeTest`, `MultiTopicTest`, `DrainCloseTest`, `TransactionTimeoutsTest`,
  `CloseAndOpenOffsetTest`, `PartitionOrderProcessingTest`, `PartitionStateCommittedOffsetIT`, … rotate.
- Every failure is a timing/timeout signature, not a logic assertion:
  - `TimeoutException: Timeout while waiting to get produce lock (was set to PT2S). Commit taking too long?`
  - awaitility `ConditionTimeoutException` on 30s throughput assertions
  - `InternalRuntimeException: No progress beyond N records after 1 rounds`
- On **GitHub-hosted 2-core runners** the same parallelism is far worse: ~**28** failures, all
  `TimeoutException` / "No progress" - i.e. wholesale CPU starvation (20 Kafka clusters vs 2 cores).

## Investigation (measured 2026-07-28)

Baseline (sequential, current CI) integration ≈ 11.5 min. Parallel runs:

| # | Machine | Docker RAM | Factor | Failures | Wall-clock |
|---|---|---|---|---|---|
| 1 | Mac M2, 12 core | 8 GB | 20 | 2 | ~92 s |
| 2 | Mac M2, 12 core | 8 GB | 6 | 2 | ~71 s |
| 3 | Mac M2, 12 core | 8 GB | 20 | 5 | ~67 s |
| 4 | Mac M2, 12 core | 16 GB | 20 | 2 | ~69 s |
| — | GitHub hosted | (hosted) | 2 core | 28 | n/a (fails) |

On real cores the suite is **~7-10× faster** (≈70-92 s vs 11.5 min) and the wholesale starvation is
**gone** (28 → ~2). Unit and performance suites run parallel cleanly and ~20-30% faster; only integration
flakes.

## What didn't work

- **Lowering the parallelism factor** (`dynamic.factor` 20 → 6): no reduction in failures (run 2).
- **Doubling Docker RAM** (8 GB → 16 GB on the 32 GB Mac): no reduction in failures (run 4).

Both "resource contention" knobs came back negative. Failures stayed at ~2/run and kept hitting
different tests.

## Root cause

Insensitivity to both RAM and parallelism factor, combined with a different failing set every run and
uniformly timeout-shaped failures, points to **genuine concurrency races in the tests themselves**, not
hardware/resource contention. A handful of integration tests carry **tight, absolute per-operation
deadlines** (e.g. a `PT2S` produce-lock timeout, 30s throughput assertions) and/or **shared broker/topic
state** that is safe sequentially but races when many `ParallelConsumer` + TestContainers Kafka clusters
run at once. Under concurrent scheduling any one of them can occasionally miss its deadline - so the
victim rotates.

This is the concrete, measured form of the original folklore: "parallel caused flaky tests" is real, and
it is a **test-quality** problem, not a runner-size problem (though a 2-core runner turns the same
weakness into a 28-failure catastrophe via CPU starvation).

## Recommendation (fix pending)

1. **Keep `parallel-tests=false` in the `ci` profile.** Do not enable it globally - on GitHub-hosted
   2-core runners it makes integration unusable (28 failures).
2. **Enable parallelism only on a self-hosted runner with real cores** (`self-hosted-tests.yml` passes
   `-Dparallel-tests=true`), where the ~7-10× speedup is real. Expect ~2/run flakes until (3).
3. **The actual fix is test hardening** - loosen or make-relative the tight per-op timeouts, and isolate
   per-test broker/topic state so tests don't race under concurrent load. Track the offenders as they
   surface (they rotate). Related in-flight flaky-test work: #63 (topic-creation consolidation),
   `fix/flaky-ensure-topic-timeout`.

## Prevention

- When adding an integration test, avoid **absolute wall-clock deadlines** (`PT2S`, "within 30s"); these
  are the first to break under any concurrency or slower hardware. Prefer generous/relative timeouts.
- Give each test its **own topics/consumer group** (unique names) so nothing is shared across the
  concurrent set.
- Treat "passes sequentially, fails under `-Dparallel-tests=true`" as a **test bug to fix**, not a reason
  to keep parallelism disabled forever.

## Related

- Experiment PR that produced the GitHub-hosted numbers: #66 (`ci/reenable-parallel-tests`, do-not-merge).
- Self-hosted runner setup + speedup context: [`docs/SELF_HOSTED_RUNNER.md`](../../SELF_HOSTED_RUNNER.md).
- In-flight flaky-test fixes: #63, `fix/flaky-ensure-topic-timeout`.
