---
title: "Shard count buys nothing while one unsplittable class sets the floor, and shards manufacture work: split the class first, then re-derive the partition, and measure each step"
date: 2026-09-07
category: performance-issues
module: build-system
problem_type: performance_issue
component: development_workflow
severity: medium
symptoms:
  - "The Integration Tests job was the PR build's critical path at ~620s against ~500s for the next slowest lane"
  - "Splitting the lane into more runner jobs was modelled to help and, with the probe class intact, measured to help far less than the arithmetic said"
  - "Total test time rose from 1545s at two shards to 2074s at four for the identical suite"
root_cause: unsplittable_class_sets_the_critical_path_floor
applies_when:
  - "Sharding a test lane across runner jobs, or raising a shard count that already exists"
  - "A partition model treats total work as constant across shard counts, or divides one class's time by the number of pieces it will be split into"
  - "Deciding the ORDER of several CI speed-ups whose effects interact"
tags:
  - ci-sharding
  - critical-path
  - repeated-test
  - per-shard-fixed-cost
  - measure-dont-model
  - integration-tests
related_components:
  - bin/ci-integration-test.sh
  - bin/check-integration-shard-balance.mjs
  - .github/workflows/maven.yml
---

# Shard count buys nothing while one unsplittable class sets the floor, and shards manufacture work

The `Integration Tests` lane was the PR build's critical path. astubbs/parallel-consumer#442 landed
it as two shards with a ~416s critical path, measured from a ~620s single job. This is the record of
how that number was reached, because the ORDER of the steps was the finding, and two of the
intuitions a reader will bring to re-deriving it were measured wrong.

## The finding: order matters more than count

A shard's wall is the longest thing it holds, and forks cannot split a class. So a single class
that is longer than every other shard's total is the critical path for ANY shard count, and adding
shards around it buys nothing.

`Rebalance857CommitSyncDeadlockProbeIT` was one `@RepeatedTest(20)` class at ~356s. With it intact,
two shards and four shards both modelled at 516s, because whichever shard held it was the floor;
the two-shard shape measured 519s, within 3s of the model. Splitting the probe four ways moved the
floor down to the next-largest class (`PartitionStateCommittedOffsetIT`, ~160s), and only THEN was
a larger heavy set worth deriving. Measured end to end, each step cumulative on the last:

| arrangement | critical path | runner-seconds | per-shard walls |
|---|---:|---:|---|
| single job | **620s** | 620s | - |
| 2 shards, probe intact | **519s** (516s modelled) | 870s | 476 / 519 |
| 2 shards + probe split | **450s** (440s modelled) | 780s | 330 / 450 |
| 2 shards + split + re-derived heavy set (**shipped**) | **416s** | 792s | 416 / 376 |
| 4 shards + probe split | **355s** (337s modelled) | 1318s | 355 / 311 / 332 / 320 |

The split bought 69s (519 to 450) and the re-derived heavy set another 34s (450 to 416), and those
two are cumulative rather than independent: the rebalance moves the probe into the heavy shard,
which is only possible because the split made it a ~134s class. At its pre-split ~356s it would
have blown the heavy shard past the catch-all immediately. The split created the granularity the
rebalance needed.

## Two things the arithmetic gets wrong, measured

**Splitting a repeated class does not divide its time.** The pre-split model assumed a ~356s
`@RepeatedTest(20)` class split four ways gives four ~89s classes. The measured pieces are
**138-166s**: the repetitions carry per-class fixed cost (broker, fixtures, JVM warm-up) that was
paid once and is now paid four times. So the split buys less than division suggests, and splitting
FURTHER would pay less again. An earlier estimate that credited the split with 161s and the shard
count with 18s came from that model; the measured attribution above replaces it.

**Shards manufacture work.** Total test time was 1545s at two shards and 2074s at four for the
same tests, because per-shard fixed costs - JVM start, broker start, fixture setup - are paid per
shard, exactly as per-fork costs were when `forkCount` went from 4 to 6 and cost 11% more CPU for
the same suite. Any model that holds total work constant across shard counts over-promises, and the
one used here did.

## Why sharding was the remaining lever, and why the serial build had to shrink first

Within-job overlap was exhausted: ~1528s of test time ran in ~420s of wall on four forks, about 91%
parallel efficiency, only possible because these tests mostly wait on a broker. Six forks dropped
to 75% and inflated total work 11%, so `forkCount=4` is a ceiling. More overlap therefore meant
more JOBS - but each job re-pays the serial build (~136s of the 604s Maven step: `testCompile`,
`compile`, javadoc, delombok, Truth codegen), so a four-way split spends roughly 400 runner-seconds
before saving anything. And test work converts to wall at about 1:3.6 while serial-build work
converts 1:1, so a second off the build is worth ~3.6x a second off the tests AND is the exact cost
sharding multiplies. Cutting the build first is what made sharding pay. The measurements are in
[`../../plans/2026-09-03-001-investigate-integration-gate-wall-time.md`](../../plans/2026-09-03-001-investigate-integration-gate-wall-time.md),
including the 119s noise floor that makes any single-job effect under ~2 minutes unmeasurable on
this lane.

## The decision: two shards, not four

Four shards was built, measured green at 355s, and deliberately not taken: 61s of critical path for
+526 runner-seconds is a 15% gain for a 66% increase in machine time, plus four lists to maintain
instead of one. It is kept as deferred work in
[`../../inflight/ci-four-shard-integration-gate.md`](../../inflight/ci-four-shard-integration-gate.md)
with what would change the answer.

Sharding buys critical path and spends aggregate runner-minutes. That was an acceptable trade when
this work was scoped (public repository, hosted minutes), but it is a real trade, and the
build-overhead multiplication above is the part that makes it worse than it first looks.

## Method

- **Measure each step on its own, cumulatively, in the order taken.** A model calibrated on the
  first step (two shards, 516s predicted, 519s measured) does not stay calibrated once a class is
  split, because the split changes per-class fixed costs the model did not carry.
- **Compare on the failsafe seconds, not the job seconds** - the plan document above owns that rule
  and the noise measurement behind it.
- **A partition is right on the day it is measured.** `bin/check-integration-shard-balance.mjs`
  recomputes the best two-way split from recorded per-class times and reports the drift as a
  number, so the same runs that make the lists stale also say how stale they are.

## Related

- [`../best-practices/a-guard-that-greps-java-must-read-what-javac-decided.md`](../best-practices/a-guard-that-greps-java-must-read-what-javac-decided.md)
  - the completeness guard that makes the subtraction-defined catch-all safe.
- [`../best-practices/attribute-a-red-only-after-a-control-arm-on-the-gates-own-configuration.md`](../best-practices/attribute-a-red-only-after-a-control-arm-on-the-gates-own-configuration.md)
  - the chaos suite's sharding, whose four balanced bins are the shape this lane deliberately did
  not copy; `bin/ci-integration-test.sh`'s header owns why.
- [`../../ci.md`](../../ci.md), "The Integration Tests lane runs as two shards" - the lane as it
  runs today.
- astubbs/parallel-consumer#442 - the PR, with three review rounds in its record.
