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
resolution_type: config_change
severity: low
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

### Verified: sequential-on-runner is offload-only; the real win is parallel-after-hardening (2026-07-28)

The `laptop-sequential-poc.yml` workflow ran the integration suite **sequentially** on `mac-laptop`
(GitHub run 30342379083) and was **green** - 0 flakes. Apples-to-apples on the **same commit**, both
sequential:

| Integration (sequential) | Duration |
|---|---|
| GitHub-hosted, 2-core (PR #68) | 11m38s (698s) |
| `mac-laptop`, 12-core | 12m00s (720s) |

**The laptop is ~3% *slower*, not faster** - and both pass. Sequential is I/O-bound (one Kafka
round-trip at a time), so the 12 cores sit idle waiting; real hardware gives **no speed or reliability
edge over GitHub for sequential**. The laptop's only sequential benefit is *offload* (no GitHub minutes).
Its real advantage is **parallel** (~7-10×) - which is the flaky path.

**Conclusion / how to use this:** sequential-on-laptop is not itself a win (GitHub already runs sequential
just as reliably, slightly faster). Keep the self-hosted runner + this PoC as the **harness / building
block for hardening the parallel tests** - a place to iterate on parallel reliability on real hardware
(fast feedback: a full parallel run is ~90 s there). We may end up **not needing the runner** once the
tests are hardened; keep it in back pocket. The real unlock remains (3): harden the timing-sensitive
integration tests so *parallel* goes green.

## Resolution: forked (per-broker) parallelism — and a real bug it surfaced (2026-07-28)

The root cause is **one shared broker**, so the fix is to stop sharing it. Running failsafe with
**`-DforkCount=4 -DreuseForks=true`** (process-level parallelism) gives **each JVM fork its own
TestContainers broker**; with JUnit thread-parallelism off (`-Dparallel-tests=false`, the `ci` default)
each broker serves its fork's tests **sequentially → uncontended**. Result:

| Integration suite | Reliability | Wall-clock |
|---|---|---|
| thread-parallel (`-Dparallel-tests=true`), 1 shared broker | ~2/104 flake per run | ~90 s (when it passed) |
| **forked, `forkCount=4`, broker-per-fork — Mac** | **5/5 green** | **~4:06** |
| **forked, `forkCount=4` — GitHub-hosted CI** | **green** | **6:16** (vs ~11:38 sequential) |

So forked mode is **reliable AND faster than sequential everywhere** (Mac ~38% faster than GitHub, but
GitHub-hosted forked also works — the self-hosted runner is *extra* speed, not required). It also does
**not mask anything**: each test runs on an uncontended broker, like sequential, just N-way in parallel.

`forkCount` is opt-in via CLI (`-DforkCount=4`); the failsafe default (unset ⇒ `forkCount=1`) preserves
today's single-fork behaviour, so builds that don't pass it are unchanged. Memory scales with fork count
(one broker per fork), so tune it to the runner — 4 fits the 12-core/16 GB Mac and GitHub-hosted; a
2-core/low-RAM box should use fewer.

**Crucially, this is only Step 1.** Removing contention makes the *functional* suite reliable, but a
contended/slow broker is a real production condition and **"contended brokers must not cause failures"**
is the real bar. Triaging the contended failures showed most were **test-tightness** (e.g.
`TransactionTimeoutsTest`'s intentional 1s/2s lock timeouts firing under load), **but**
`RebalanceEoSDeadlockTest.noDeadlockOnRevoke` maps to a **genuine main-code deadlock — #857**
(`onPartitionsRevoked` blocking on `synchronized(commitCommand)`), already being fixed in PR #29 with
`ReentrantLock.tryLock()`. So the contention was *exposing a real bug*, not just flaky tests — which is
exactly why we did **not** loosen timeouts to go green (see AGENTS.md "Be EXTREMELY careful modifying
tests").

**The road (two steps):**
1. **(done)** Adopt forked/per-broker parallelism for a reliable, fast functional suite.
2. **(DEFERRED — do not start yet)** Once #857/#29 is actually finished and merged **on its own merits**,
   retry **full thread-parallelism on a shared broker** — the deliberate contended-broker stress test — to
   *validate* the deadlock is gone rather than avoided. Deferred because #29 is a ~454-line WIP concurrency
   refactor (new `ThreadConfinedConsumer`, "root cause still open", chaos test 9/10) on a different base
   with merge conflicts — merging unfinished main code just to test parallel would violate the "be
   extremely careful modifying tests/main code under stress" rule. `-Dparallel-tests=true` (thread-parallel
   on one broker) is the reproducer to re-run **after** #857 lands.

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
