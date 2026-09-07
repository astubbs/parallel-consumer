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
- **The opposite direction is the one nothing warns you about: a ranked class that gets much FASTER,
  or disappears, leaves an ordering built around a class that no longer sets the tail.** Adding a
  class at least makes something appear on disk unranked; a class that shrinks changes nothing you
  can see. `balanced` keeps sorting, by numbers that describe a suite which no longer exists, and the
  packing decays toward the unordered scan it replaced - with no red build, because ordering is not
  something any assertion can be wrong about. The live case is `RunLengthEncoderTest`, core's
  heaviest class: astubbs#106 collapses it by an order of magnitude, after which the tail is set by
  the next class down (`ParallelEoSStreamProcessorTest` today). **So refresh the stats after any
  change that materially speeds up a class near the tail, not only after adding one** - a full
  `bin/ci-unit-test.sh` run rewrites every module's file.
- **Read that file per CLASS, never per line.** Each line is one test METHOD, and a heavy class is
  spread over many of them, so the longest *line* is not the longest *class* - and the class is the
  scheduling unit, because `balanced` sorts classes and forking cannot split one. Sorting the raw
  lines badly overstates how far the top class stands above the next; it is the mistake to expect,
  and a review of this very change made it. Aggregate first, and take the numbers from the tree
  rather than from here:

  ```bash
  awk -F, '{t[$3]+=$2} END {for (c in t) printf "%9.0f  %s\n", t[c], c}' \
      parallel-consumer-core/.surefire-pc-unit-times | sort -rn | head -5
  ```
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
`RunLengthEncoderTest` (astubbs#106), plus jacoco report generation on the critical path and
ArchUnit classpath-scan cost.

**Those two overlap rather than sum**, and it is worth being explicit about which. Packing removes the
idle fork at the end of the run; astubbs#106 removes the work that made that fork long. Neither change
makes the other pointless, and adding their measured gains together would double-count.

**How much packing survives astubbs#106 is a question about the SECOND-heaviest class, and per class
the gap between the top two is small** - `ParallelEoSStreamProcessorTest` sits just under
`RunLengthEncoderTest`, not at a third of it (aggregate with the `awk` line above rather than trusting
a ratio written here, which goes stale at the next refresh). So the residual tail after astubbs#106
is most of the present one, and packing keeps most of its value rather than being largely obsoleted -
a substantial un-splittable class scheduled last still strands a fork for its own length. Forking
cannot split a class, so the floor moves down to the next class and no further; that is the same
result the integration lane measured in
[`shard-count-buys-nothing-while-one-class-sets-the-floor`](../performance-issues/shard-count-buys-nothing-while-one-class-sets-the-floor-2026-09-07.md).
