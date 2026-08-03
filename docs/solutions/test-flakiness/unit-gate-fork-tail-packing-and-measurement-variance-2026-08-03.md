---
title: Pack the fork tail - run the slowest test classes first, and beware that one un-splittable class dominates both the wall time and the measurement noise
date: 2026-08-03
category: test-flakiness
module: all
problem_type: slow_test_suite
component: testing
symptoms:
  - "CI Unit Tests gate takes ~6:40 on the 2-core GitHub runner"
  - "One surefire fork sits idle at the end of the run while the other finishes a long class"
  - "Repeat runs of the same commit differ by 50-90s, so small improvements cannot be measured"
  - "Maven -T module parallelism makes the 2-core gate SLOWER and induces awaitility timeouts"
root_cause: fork_scheduling_and_unsplittable_slow_class
resolution_type: fixed
severity: medium
tags:
  - surefire
  - forking
  - runOrder
  - benchmarking
  - parallel-tests
---

# Pack the fork tail - slowest test classes first

Follow-up to
[unit-tests-parallelise-by-forking-not-threading](unit-tests-parallelise-by-forking-not-threading-2026-07-29.md),
which established *forking* (not JUnit threads) as this project's safe parallelism axis. This entry is
about what limits that forked suite next, and about a measurement trap that will mislead anyone who
tries to optimise it.

## The fix that worked: LPT fork packing

Surefire's forks pull test classes from **one shared queue**. With the default
`runOrder=filesystem`, core's slowest class (`RunLengthEncoderTest`) happened to be scheduled
**last**, so one fork idled for a minute while the other ground through it. Classic
longest-processing-time scheduling: put the long jobs first and the tail packs tight.

```xml
<!-- ci profile -->
<surefire.runOrder>balanced</surefire.runOrder>
<surefire.runOrder.statisticsFile.checksum>pc-unit-times</surefire.runOrder.statisticsFile.checksum>
```

`runOrder=balanced` sorts classes by recorded runtime, slowest first. Historically it was useless on
CI because the statistics file lived at a config-hash-derived path that could not be committed.
**Surefire 3.5.5+ adds `runOrderStatisticsFileChecksum`**, which pins the filename - so the stats live
at `<module>/.surefire-pc-unit-times`, are **checked into git**, and work on a cold CI checkout.

Gotchas worth knowing:

- The files are **rewritten in place after every `-Pci` run**. A killed/interrupted run truncates them
  to only the classes that completed; a full green run restores them.
- A module with no stats file degrades gracefully to the unordered scan. A **newly added test class is
  unranked and sorts last** - which can displace the genuinely-slow class out of its good slot. Refresh
  the stats when adding slow tests (this bit us when a merge introduced `AmbientProbeExtensionTest`).
- Forking cannot split a single class, so ordering can only pack *around* the slowest class; it can
  never get under it.

## The trap: benchmarking a suite whose slowest class is un-splittable

`RunLengthEncoderTest` measured anywhere from **67s to 166s for the same code** depending on whether it
landed first in a fresh JVM or inherited a fork that had already run other classes (worked-over heap /
GC pressure on a ~2.1B-iteration allocation-heavy loop).

Because that one class is 16-40% of the whole wall time, **single-run wall time carries ±50-90s of
noise**. Consequences:

- An early measurement of 363s looked like a 16% win. Repeat runs put the honest figure near 410s
  (~5%). The 363 was a favourable-variance outlier, not a result.
- Any hypothesis worth less than ~90s **cannot be resolved by a single run at all**. Either take a
  repeat-median, or remove the noise source first.

**If you optimise this suite, fix the dominant slow class before trusting any other measurement.**

### Corollary: don't let the harness mutate its own input

While measuring, each run overwrote the very `.surefire-pc-unit-times` files that determine the
ordering - so every measurement silently depended on the previous one's fork placement. Any benchmark
harness must snapshot and restore state the run mutates, or it measures a feedback loop rather than the
change under test.

## What did NOT work: Maven `-T` module parallelism

Overlapping the independent downstream modules (vertx / reactor / mutiny / examples, which depend only
on core) looks like free wall time - roughly 90s of strictly sequential work. On the 2-core gate it
**lost**, going from ~363s back to ~432s, *and* it induced a flake:
`VertxBatchTest.averageBatchSizeTest` blew its 30s awaitility window and cost a 69s rerun.

The reason is that the box was **already saturated**: `-T 1C` (2 module threads) × `forkCount=1C`
(2 forks each) oversubscribes 2 CPUs roughly 4:1 including the Maven threads themselves. Well-packed
forks had already claimed the available parallelism; adding another layer only added contention - and
CPU starvation pushes wait-heavy tests past their real-time timeouts, manufacturing flakiness that
looks like a product bug.

**Rule of thumb: pick ONE parallelism axis per core budget.** On a 2-core runner, forked surefire is
that axis. This also demotes fork oversubscription (`forkCount=3+`) for the same reason.

## Where that leaves the gate

Scheduling levers are now exhausted; the box is CPU-bound. Further gains must **reduce CPU work**, not
redistribute it - which points at the `OffsetSimultaneousEncoder.invoke()` full-range scan behind
`RunLengthEncoderTest` (tracked separately), plus jacoco report generation on the critical path and
ArchUnit classpath-scan cost.
