---
title: Parallelise the unit suite by FORKING JVMs, not by JUnit threads - process isolation avoids the shared-static-state races
date: 2026-07-29
category: test-flakiness
module: parallel-consumer-core
problem_type: slow_test_suite
component: testing
symptoms:
  - "Sequential unit (surefire) suite for core is ~5:14 - slow feedback on every PR"
  - "Enabling JUnit thread parallelism (parallel-tests=true) is fast (~2:32) but flaky"
  - "ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown fails intermittently only under thread parallelism"
  - "On GitHub 2-core runners, thread parallelism (factor 20) CPU-starves and mass-fails"
root_cause: shared_static_state_under_threads
resolution_type: fixed
severity: medium
tags:
  - flaky-tests
  - parallel-tests
  - forking
  - surefire
---

# Parallelise the unit suite by forking JVMs, not by threading

## Problem

The core unit (surefire) suite runs ~**5:14** fully sequential (the `ci` profile sets
`parallel-tests=false`). We want it faster on every PR without reintroducing flakiness.

There are **two independent** ways to parallelise a Maven/JUnit test run, and they are not equivalent:

1. **JUnit thread parallelism** - `junit.jupiter.execution.parallel.enabled=true` (our `parallel-tests`
   property), running many test *methods/classes concurrently as threads inside a single JVM*.
2. **Surefire process forking** - `forkCount=N`, running test *classes across N separate JVM processes*,
   single-threaded within each fork.

## Measurements (core unit suite, 12-core Mac, `ci` profile)

| Strategy | Wall-clock | Reliable? |
|---|---|---|
| Sequential (`parallel-tests=false`, `forkCount=1`) | **5:14** | ✅ |
| Fork ×4 (`forkCount=4`, threads off) | **2:10** | ✅ 0 failures |
| **Fork 1C = 12 (`forkCount=1C`, threads off)** | **1:38** | ✅ 0 failures |
| Thread ×20 (`parallel-tests=true`, `forkCount=1`) | ~2:32 | ❌ intermittent failure |

Forking is both **faster** (1:38 vs 2:32) **and** reliable. `1C` (one fork per core) auto-scales:
2 forks on GitHub's 2-core runner, 12 on this Mac, N on the self-hosted box.

The floor is now ~**59s**: a single class, `RunLengthEncoderTest`, whose `testSimultaneousWithOverflowErrors`
INT case genuinely walks ~2.1B offsets inside `OffsetSimultaneousEncoder.invoke()`. Forking can't split one
class, so more forks past ~8 give diminishing returns (98s at 1C is already near that floor).

## Why forking works where threading does not

**Threads share process-global state; separate processes do not.** Parallel Consumer's tests (and some of its
machinery) lean on JVM-wide static state - static singletons, static test fixtures/counters, DI wired per-JVM,
and process-global resources. When JUnit runs methods as **threads in one JVM**, those concurrent methods
**share and race on** that static state:

- `ParallelEoSStreamProcessorTest`'s shutdown/commit assertion (tracked separately as a possible real
  concurrency bug, to be examined with the confluentinc#857 locking work) fails only under thread parallelism - a
  shutdown-timing/commit race that a single-threaded run never exposes.
- On GitHub's 2-core runners, factor-20 threads also **CPU-starve** (20 runnable threads, 2 cores), turning
  tight in-test deadlines into mass timeouts (~28 failures) - a *second*, resource failure mode on top of the
  race.

When surefire **forks**, each parallel unit is a **separate OS process with its own copy of all statics**.
There is nothing to race on across forks, so the shared-static-state class of flakiness simply cannot occur;
and `1C` matches fork count to cores, so there is no oversubscription/starvation either. Forking trades a
little JVM-startup cost (amortised by `reuseForks=true`) for correctness-by-isolation.

Rule of thumb for this codebase: **parallelise the unit suite by forking, not by threading.** Thread
parallelism stays available for local experiments (`-Dparallel-tests=true`) but is not the CI strategy.

## Prerequisites that made this land

- **`RunLengthEncoderTest` overflow fix** - its v2 case used to loop ~2.1B times (~85s); reduced to a single
  delta jump (0.087s). Without this, the heaviest fork was ~136s and forking barely helped.
- **Integration tests kept out of surefire** - an ArchUnit rule (`TestConventionRules`) forces any
  Testcontainers/`BrokerIntegrationTest` test into an `integrationTest` package (failsafe). This guarantees
  the *unit* forks are pure-mock and Docker-free, so N forks don't each try to start a broker.

## The change

`forkCount` is a property, defaulting to `1` (no forking, unchanged local behaviour), raised to `1C` in the
`ci` profile - alongside the existing `parallel-tests=false`, so CI forks with threads off:

```xml
<!-- default properties -->
<surefire.forkCount>1</surefire.forkCount>

<!-- ci profile -->
<parallel-tests>false</parallel-tests>
<surefire.forkCount>1C</surefire.forkCount>

<!-- maven-surefire-plugin configuration -->
<forkCount>${surefire.forkCount}</forkCount>
<reuseForks>true</reuseForks>
```

CI (`-Pci`) therefore gets fork-parallelism on any hardware; the GitHub 2-core PR gate goes ~5:14 → ~half,
the self-hosted/multi-core box goes ~5:14 → ~1:40, both reliable.

## Known caveat / follow-up

- **JaCoCo under `forkCount>1`**: `prepare-agent` uses a single `jacoco.exec` with append mode; multiple
  forks appending concurrently can corrupt/undercount coverage. If CI coverage numbers look wrong, give each
  fork its own exec file (`destFile` with `${surefire.forkNumber}`) and `jacoco:merge` before reporting.
  Tracked in `docs/inflight.md`.
- The ~59s `RunLengthEncoderTest` INT floor is a separate, optional main-code optimisation (a delta-aware
  `OffsetSimultaneousEncoder.invoke()`), also in `docs/inflight.md`.
