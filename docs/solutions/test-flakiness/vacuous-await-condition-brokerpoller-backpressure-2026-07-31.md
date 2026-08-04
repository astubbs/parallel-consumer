---
title: Await conditions that are vacuously true before the system reaches its initial state mask unsatisfiable assertions
date: 2026-07-31
category: test-flakiness
module: parallel-consumer-core
problem_type: flaky_test
component: testing
symptoms:
  - "BrokerPollerBackpressureTest.brokerPollPausedWithEmptyShardsButHighInFlight: ConditionTimeoutException on the FIRST await (10s) under CI load"
  - "Same test green on quiet boxes and on the gating GitHub-hosted run of the same sha"
  - "Failure rate tracks machine load, but raising the timeout does not help"
root_cause: test_design_bug
resolution_type: test_rewrite
severity: low
tags:
  - flaky-tests
  - awaitility
  - vacuous-condition
  - backpressure
  - ci
---

# Await conditions that are vacuously true before the system reaches its initial state mask unsatisfiable assertions

## Problem

`BrokerPollerBackpressureTest.brokerPollPausedWithEmptyShardsButHighInFlight` failed on the highcpu
lane (run 30603617471, 2026-07-31) with a 10s Awaitility timeout on its first await, while the
gating GitHub-hosted Integration run on the SAME head sha was green. The inflight ledger flagged it
"diagnose, don't dismiss".

## Root cause: the awaited condition was never satisfiable - green runs passed vacuously

The test produced 200 unique-key records (KEY ordering → 200 single-record shards), blocked all 10
workers on a latch, then awaited:

```java
await().atMost(10s).until(() -> pc.getWm().getNumberOfWorkQueuedInShardsAwaitingSelection() == 0);
```

- `messageBufferSize(150)` pins the load factor **statically** at 15
  (`PCModule.initDynamicLoadFactor`: buffer / concurrency), so the control loop takes at most
  10 x 15 = **150** records out of the shards (`calculateQuantityToRequest`).
- With every worker latch-blocked nothing completes, so `awaitingSelection` **floors at 50** - the
  awaited `== 0` is unsatisfiable once records have arrived. No timeout increase can fix that.
- The test passed on quiet boxes only through a race: the await's first checks run **before
  partition assignment** (~1-2s), when the shard map is empty and `awaitingSelection` returns 0
  **vacuously**. Under CI load, produce/setup latency pushed the await start past record arrival
  (assignment 04:16:25.126, await start ~04:16:25.6) - from there the timeout was deterministic.
- Structural corollary: the test's named scenario (poll paused while shards are EMPTY and in-flight
  is high) is unreachable by design - the take-cap and the pause threshold are the same number
  (`messageBufferSize`), so empty shards implies the sum is at the threshold, never above it.

## How it was proven (technique worth reusing)

1. **Arithmetic impossibility**: the same-sha green run (30603617430) finished the whole 2-test
   class in 5.518s including two deliberate 1s sleeps. A non-vacuous first-await pass requires
   draining 200 records through a 150-record cap - impossible - so every green pass HAD to be the
   vacuous window.
2. **Deterministic reproducer (no load needed)**: keep the broken await but precede it with an
   arrival-sync (`await until awaitingSelection > 0`). The original condition then times out every
   run on any machine - RED reproduced locally first try (13.07s = ~3s setup + full 10s timeout).
   The load-dependence was only a race inversion, so removing the race exposes the bug directly.

## Fix

Rewrote the test (`brokerPollPausedWhenBlockedInFlightFillsBuffer`) to await the real, satisfiable
steady state - `outForProcessing == 150 && awaitingSelection == 50`, derived from named constants -
then assert the pause. This also verifies upstream #836's actual behaviour non-vacuously for the
first time: the 50 shard-queued records alone are below the pause threshold, so the pause firing
proves blocked in-flight records are counted. The exact 150/50 split doubles as a regression pin on
the static-buffer take-cap. Await bounds raised to 30s (conditions got stronger; only the
wall-clock allowance grew - free when green, absorbs busy boxes).

## Classification: what this was NOT

Recorded because the wrong classification was the likely failure mode here - three plausible verdicts
were checked and rejected before the rewrite:

- **Not the tight-absolute-timeout contention pattern**
  ([parallel-integration-tests-flaky-under-concurrency-2026-07-28](parallel-integration-tests-flaky-under-concurrency-2026-07-28.md)).
  Same family (load exposed it) and identical symptom, but hardening the 10s bound would NOT have
  fixed this test - the condition is unsatisfiable after arrival at any timeout. "Contention
  sensitivity, loosen the bound" would have masked the real defect.
- **Not a main-code bug.** The 50-record shard floor is the static buffer cap behaving as designed,
  and the pause/resume machinery worked (the sibling test passed in the same failing run).
- **Not already tracked.** No fork or upstream (`confluentinc/parallel-consumer`) issue mentions the
  test; not in the quarantine lane; not covered by #63, `fix/flaky-ensure-topic-timeout`, or the
  `PartitionStateCommittedOffsetIT` work.

## Prevention

- An Awaitility condition of the form `X == 0` (or any emptiness/absence check) where `X` **starts
  at 0 and only later becomes non-zero** is a red flag: it can pass vacuously before the system
  reaches its initial state, testing nothing. Await the non-zero intermediate state first
  (arrival-sync), or await a convergent steady state instead of an absence.
- When a test only fails under load, classify before touching timeouts (AGENTS.md stress-failure
  discipline). This one LOOKED like the tight-absolute-timeout contention pattern
  ([parallel-integration-tests-flaky-under-concurrency-2026-07-28](parallel-integration-tests-flaky-under-concurrency-2026-07-28.md))
  but loosening bounds would have masked a broken condition; the mechanisms are different even
  though the symptom (timeout under load, green when quiet) is identical.
- Before "hardening" any await, do the arithmetic: can the condition actually be reached from the
  system's caps and the test's configuration? Here 200 produced vs a 150 cap made it impossible on
  paper before any run confirmed it.

## Related

- Fix + full diagnosis trail: PR #98
- Failing run: highcpu 30603617471 (Integration job 91071293663); green same-sha run: CI 30603617430
- Test provenance: upstream confluentinc/parallel-consumer #682 (configurable buffer), #836
  (in-flight counts toward backpressure)
- Adjacent-but-different pattern: [parallel-integration-tests-flaky-under-concurrency-2026-07-28](parallel-integration-tests-flaky-under-concurrency-2026-07-28.md)
