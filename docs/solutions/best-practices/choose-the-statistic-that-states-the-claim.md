---
title: Assert on the statistic that states your claim, not the one that sounds most rigorous
date: 2026-08-10
category: best-practices
module: parallel-consumer-streams
problem_type: best_practice
component: testing_framework
severity: medium
applies_when:
  - Choosing which statistic (min, mean, p50, p99) a test assertion should be asserted on
  - A percentile-based assertion fails narrowly and lowering the threshold is being considered
  - Sample size is small (tens of items, not thousands) and a high percentile is in play
  - Writing or reviewing a benchmark whose claim is about every item, the luckiest item, or a typical item
symptoms:
  - A percentile-based assertion fails narrowly against its threshold
  - At small n a p99 (or p95) assertion is numerically identical to the maximum
  - Lowering an assertion's threshold is proposed as the fix for a narrow, unexplained failure
related_components:
  - development_workflow
tags:
  - benchmark-design
  - percentiles
  - small-sample-statistics
  - test-assertions
  - head-of-line-blocking
  - measurement-validity
  - kafka-streams
---

# Assert on the statistic that states your claim, not the one that sounds most rigorous

## Context

`HeadOfLineBlockingBenchmarkTest` (`parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/HeadOfLineBlockingBenchmarkTest.java`)
is part of the `ks-spike` work on astubbs/parallel-consumer#271 (issue astubbs#255): does routing a Kafka
Streams topology's dispatch through PC's `WorkManager` remove head-of-line blocking? Stock Kafka Streams
hands a partition's records over one at a time (`PartitionGroup.nextRecord()`); PC dispatches by key
through a worker pool. The benchmark puts one 1500ms "blocker" record at the head of a single partition
and twenty-four 25ms records on other keys behind it, then times every fast record's completion. Both
arms run in the same JVM against the same patched classes, switching only `PcDispatchSwitch`
(test:60-63, 192-200) - so a difference is attributable to the seam, not to JVM, broker, or warm-up
differences. That control-arm discipline is its own lesson; see
[Related](#related) for the sibling doc.

The plan that specified this test
(`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md`, section U8, "Predictions, stated before
running") wrote the original assertions in terms of p99: *"A1 (stock): fast-record p99 latency `>= S`"*,
*"A2 (PC): fast-record p99 latency `<< S`"* (plan.md:713-715). The implemented test initially followed
that prediction. Measured against a `MIN_LATENCY_IMPROVEMENT` threshold of 3.0x, the p99 ratio came in at
2212ms (stock) vs 740ms (PC) - 2.99x - and the assertion **failed**, by a hair, on a prediction that had
looked comfortably safe on paper.

The full measured distribution, from the commit that fixed this (`0b0a8125a`), was:

| statistic | stock  | PC    | ratio |
|-----------|--------|-------|-------|
| min       | 1541ms | 27ms  | 57x   |
| p50       | 1858ms | 232ms | 8x    |
| p99       | 2205ms | 637ms | 3.5x  |

The instinct was to lower the 3.0x threshold to whatever the run happened to clear. That would have been
treating the symptom. The actual diagnosis, recorded in the test's own assertion comment
(test:120-126), was that **p99 was the wrong statistic for this claim** - not merely an unlucky run of
it.

## Guidance

**For every assertion, ask: "what would have to be true for my claim to be false, and which statistic
shows that?"** Pick the statistic that answers that question, not the one that sounds most rigorous in
isolation. A percentile carries an aura of statistical sophistication that can silently paper over a
mismatch between what it measures and what the test claims.

Two traps to check for explicitly:

1. **At small n, a high percentile degenerates into the maximum - and the maximum is often dominated by
   a different mechanism than the one under test.** `Latencies.percentile()`
   (test:381-384) computes `index = ceil(percentile / 100.0 * size) - 1`. For `percentile=99` and
   `size=24` (`FAST_RECORDS`, test:84), that is `ceil(23.76) - 1 = 23` - the last index in a
   24-element sorted array, i.e. the maximum. That fast record isn't unlucky because it waited for the
   blocker; it's unlucky because it was the last one handed to a four-worker pool
   (`POOL_SIZE = 4`, test:65). p99 here was measuring **queueing depth against pool size**, a real but
   different property from "does a fast record wait for a slow one on another key" - and it would move
   with `POOL_SIZE`, not with whether the seam removes head-of-line blocking at all.

2. **When an assertion fails narrowly, the honest first question is "is this the wrong metric?", not
   "is the threshold too tight?"** Lowering a threshold to make a failing run pass answers a different
   question than the one being asked, and does it silently: the test keeps its old name and its old
   comment, but it stops meaning what it says. The fix here was to identify the statistic that actually
   states the claim, not to relax the number attached to the wrong one.

**Find the statistic by writing out the claim in words first**, then match it to a statistic that would
be false under exactly the same conditions:

- Claim: *"a fast record does not have to wait for the slow one."* This is falsified if **even the
  luckiest fast record** waited, and demonstrated if **any single one** did not. That is a claim about
  the best case, which is the **minimum** - not a percentile at all.
- Claim: *"and the typical record benefits, not just a lucky few."* That is a distribution claim, and
  p50 (median) states it directly.
- The tail (p99) is real information and worth *reporting* - it says something true about pool queueing
  - but it is not the statistic that states either of the two claims above, so it should not carry an
  assertion for them.

**This is not an argument against percentiles.** Report the whole distribution - min, p50, p99, whatever
is useful to a reader. The discipline is in choosing, deliberately, which part of that distribution the
*assertion* rests on, separately from which parts get *logged*.

## Why This Matters

A benchmark's credibility rests on its assertion measuring what its comment claims. When the two drift
apart - as p99 drifted from "does a fast record avoid waiting" to "how deep did the worker-pool queue
get" - the test keeps passing or failing for reasons unrelated to its stated purpose, and every future
reader who trusts the comment is misled.

The failure mode compounds specifically at small n, which is common in integration-test benchmarks that
can't afford thousands of samples: any percentile at or above `1 - 1/n` (99th percentile at n=24, 95th at
n=20, and so on) *is* the maximum, dressed up in percentile notation. It reads as a rigorous, standard
statistical measure while actually reporting on one single, potentially anomalous, unlucky sample - here,
literally the last item queued through a pool of four workers.

The narrow failure (2.99x against a 3.0x threshold) was the useful signal, not an inconvenience. Had the
threshold simply been dropped to 2.5x or 2.0x, the test would have kept passing indefinitely while
silently asserting something closer to "pool size is roughly adequate" than "head-of-line blocking is
removed" - and a future regression that broke the actual claim (say, a change that made the seam
occasionally block anyway) could easily still clear a loosened tail threshold, because the tail was never
sensitive to that failure mode. The corrected test (test:127-148) asserts `stock.min()` is close to the
blocker's cost and `pc.min()` is close to the fast record's own cost - a comparison that is false if any
single record slips through unblocked or still waits, which is exactly the claim.

## When to Apply

- Writing or reviewing any test or benchmark assertion built on a percentile, especially p95/p99/p999.
- Before adjusting a threshold to make a narrowly-failing assertion pass - check whether the metric
  itself answers the claim before touching the number.
- Any time sample size is in the tens (not thousands): compute what index the percentile resolves to and
  check whether it's actually the max (or min, or close to it).
- Designing a new benchmark: write the claim in one sentence first, then pick the statistic that would be
  false under the same conditions as the claim, before choosing what to assert on.
- Reviewing an existing benchmark's assertions during a refactor or a threshold tweak - drift between
  what a comment claims and what the assertion measures is easy to introduce silently over time.

## Examples

**Before** - following the plan's original p99-based prediction (plan.md:713-715), the assertion would
have looked like:

```java
assertThat((double) stock.p99() / pc.p99())
        .as("fast records should be dramatically quicker under PC dispatch")
        .isGreaterThan(MIN_LATENCY_IMPROVEMENT); // measured 2212ms/740ms = 2.99x -> FAILS at 3.0
```

This fails narrowly, and on a metric that - per the `percentile()` math above - is measuring the last
record's wait through a four-worker pool, not whether any fast record waited on the blocker.

**After** - the actual assertions in `HeadOfLineBlockingBenchmarkTest.java:127-148`, asserting on the
statistic that states each claim, with the reasoning captured in the comment above them:

```java
// A1 and A2 are asserted on the MINIMUM, because that is the statistic that states the claim. "A fast
// record does not have to wait for the slow one" is falsified if even the luckiest fast record waited,
// and demonstrated if any single one did not. The percentiles are reported, and p50 is asserted as the
// distribution claim, but the tail is deliberately not: at n=24 the p99 IS the single worst sample, and
// the worst sample here is the last record queued through a pool of four - a measure of queueing depth,
// not of blocking.
assertThat(stock.min())
        .as("A1: under stock dispatch the partition is handed over one record at a time, so EVERY fast "
                + "record queues behind the %dms blocker - including the luckiest.", SLOW_COST.toMillis())
        .isGreaterThanOrEqualTo((long) (SLOW_COST.toMillis() * 0.8));

assertThat(pc.min())
        .as("A2: under PC dispatch a fast record waits for a worker, not for the blocker - so the "
                + "quickest should cost about its own %dms and nothing else", FAST_COST.toMillis())
        .isLessThan(SLOW_COST.toMillis() / 2);

double medianImprovement = stock.p50() / (double) Math.max(1, pc.p50());
assertThat(medianImprovement)
        .as("the typical fast record should be dramatically quicker. Stock p50 %dms vs PC p50 %dms",
                stock.p50(), pc.p50())
        .isGreaterThan(MIN_LATENCY_IMPROVEMENT);
```

p99 is still logged every run (test:140-144, `"p99 {}x"`) so the queueing-depth signal is visible - it
just doesn't gate the test. Result at the corrected statistics: min 1541ms -> 27ms (57x), p50 1858ms ->
232ms (8x), against p99's noisy 3.5x that had nearly failed the build for the wrong reason.

## Related

- `docs/solutions/best-practices/control-arms-vary-exactly-one-term.md` - the companion discipline this
  same benchmark applies: both arms run in one JVM on the same patched classes, switching only
  `PcDispatchSwitch`, so a measured difference can be attributed to the seam.
- `docs/solutions/best-practices/chase-refuted-predictions.md` - what to do when a stated prediction
  (here, the plan's original p99-based A1/A2) turns out wrong; this doc is the specific case of a
  prediction failing because it was pointed at the wrong statistic.
- astubbs/parallel-consumer#271 - the PR carrying this benchmark and the fix.
- astubbs#255 - the issue this spike answers.
- `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md`, section U8 - the plan that originally
  specified p99 for A1/A2, and the source of the "predictions stated before running" discipline that
  made the wrong choice visible instead of silently accepted.
- `parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/HeadOfLineBlockingBenchmarkTest.java` -
  the test itself; see the `Latencies` class (test:358-391) for the percentile implementation and the
  assertion comments at test:120-148 for the reasoning captured in place.
