---
title: "A timing bound used as a correctness gate manufactures its own evidence"
date: 2026-08-25
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "A suite asserts an elapsed-time bound (a stall, dwell, drain or latency deadline) and calls the result a correctness failure"
  - "A recurring failure's measured peak sits a few percent above its bound, run after run, across different machines"
  - "A test suite's failure rate rose and nobody can name a product change that caused it"
  - "Deciding whether to widen a bound, disable a detector, or quarantine the test"
tags:
  - timing-assertions
  - false-positives
  - chaos-testing
  - flaky-tests
  - calibration
  - control-arms
---

# A timing bound used as a correctness gate manufactures its own evidence

## Context

The chaos suite's `CLASS2_STALL/LAG_STAGNATION` detector asserted that no partition's committed
offset may sit still for 150 seconds. Fourteen recorded sightings later, spanning a month and several
branches, the family ledger read as a mounting body of evidence for a real product stall.

It was not. Every one of those crossings was the bound meeting the load. The three habits below are
what the investigation had to unlearn, and they generalise past chaos testing to any suite that
gates on elapsed time.

## Guidance

### 1. A clustered peak is arithmetic, not a signature

Sightings kept reporting peaks of 151.9-154.6s against a 150s bound, and successive entries cited
that tight clustering as corroboration - four peaks "within 300ms of each other" across different
branches and machines.

That number could not have been anything else. The probe sampled every 5s and the scenario
fail-fasted on the first crossing, so the recorded peak is always **bound plus detection latency**.
It is fixed by the instrument, not by the defect. A crossing by 2.8% and a crossing by a wedged
consumer produce identical numbers.

**Before reading a repeated measurement as a signature, ask what value the instrument could
possibly have produced.** If the answer is "only this one", it discriminates nothing.

### 2. Adding scenarios raises the failure rate with no product change

Any one scenario crossing fails the whole job, so job failure is `1-(1-p)^N` for N independent
scenarios. Measured across 406 CI runs of this repo's `highcpu` lane:

| Date | Scenarios | Runs/day | Job failure rate |
|---|---|---|---|
| 2026-08-13 | 3 | 62 | 20% |
| 2026-08-14 | 3 | 74 | 12% |
| 2026-08-18 | 4 | 85 | 24% |
| 2026-08-19 | 7 *(added this day)* | 114 | 13% |
| 2026-08-20 | 7 | 66 | 65% |
| 2026-08-24 | 7 | 110 | 78% |

**Load does not explain it** - 2026-08-19 carried the highest run count of any day at 13%, while
2026-08-20 had 66 runs at 65%. The suite went from 3 to 7 scenarios on 2026-08-19, and the rate
stepped up and stayed there.

So a timing gate **degrades as the suite gets more thorough**. Each added scenario is another
independent chance to cross the same bound, and each crossing costs a diagnosis. That is a property
of the gate, not of the code under test.

### 3. Gate on completion; report timing

The discriminating question is not "did it exceed the bound" but "did the work finish". Replay the
failing seed with the run allowed to continue past detection and read whether the backlog drains.

Two replays of the seeds the ledger itself nominated as its strongest evidence both crossed the
bound and then drained to zero in-flight with full key coverage - on a *contended* box, which biases
toward not draining. The bound had been measuring speed the whole time.

The durable shape is: **gate on progress, report timing.** A liveness detector that watches
*completions* cannot fire on slow-but-progressing work, because any completed unit re-arms it. An
elapsed-time bound structurally cannot tell a busy system from a wedged one.

### 4. Suppress the violation, never the measurement

When a detector is demoted, keep recording its peak. The number is what a future re-calibration
reads, and a scenario that suppressed the measurement too would delete that evidence with nothing
going red to say so. In this codebase the invariant is asserted directly rather than trusted
(`RebalanceDwellToggleIT`), with an *armed* control so the disabled case cannot pass vacuously.

## Why This Matters

The cost was not the red builds. It was that fourteen entries of careful, well-written analysis
accumulated around a measurement that could not distinguish the thing it was being read as evidence
for - and each entry made the next reader more confident. A gate that produces confident false
positives is worse than no gate: it spends diagnosis time and it builds a record that argues for
itself.

## When to Apply

Reach for this whenever a failure's evidence is *an elapsed time against a threshold*. The tell is a
measured value that sits just above its bound, repeatedly, and a diagnosis that has to reason about
plausibility because nothing in the run says which of two opposite causes occurred.

## Examples

**Verifying the fix is where this most easily goes wrong.** The obvious check - replay the seed and
watch it go green - is worthless when the crossing is probabilistic. One such replay here came back
green at a 10062ms peak against a 15000ms bound: a run that never reached the failing condition and
would have passed unfixed. A green replay is an *absence*, the weakest evidence available.

Assert the contract instead:

```java
// ARMED control first, or the disabled case below proves nothing -
// a detector that never fires either way would pass it.
boolean violated = probe.recordRebalanceDwell(OVER_BOUND, "g", PREPARING_REBALANCE);
assertThat(violated).isTrue();

// ...then the suppression, on the same crossing
ProgressProbe disabled = probe().disableRebalanceDwellViolation();
assertThat(disabled.recordRebalanceDwell(OVER_BOUND, "g", PREPARING_REBALANCE)).isFalse();
assertThat(disabled.getViolations()).isEmpty();

// ...and the half that is easy to break and impossible to notice
assertThat(disabled.getPeakRebalanceDwellMs()).isEqualTo(OVER_BOUND.toMillis());
```

**Widening the bound is usually not the fix.** Here the bound sat between a measured healthy peak of
6.7s and a measured defect peak of 20.1s. A value accommodating the scenario's legitimate 15.7s
would have left ~1.15x to the defect signature and stopped discriminating at all. When a bound
cannot be widened without blinding it, the honest options are to disable it for that scenario -
recording why, and keeping the peak - or to replace it with a detector that watches progress.

## Related

- The critique that named this before it was settled, and prescribed the experiment that settled it,
  lived at `docs/inflight/test-class2-probe-asserts-timing-not-correctness.md`. It was deleted once
  resolved, per this repo's rule that an in-flight note tracks only what is open - read it with
  `git show 77beb4f31:docs/inflight/test-class2-probe-asserts-timing-not-correctness.md`, grepping
  `What the proxy costs, measured` for the four-arm measurement this doc summarises
- `docs/inflight/bug-857-family.md` - the sighting ledger the fourteen entries accumulated in
- `docs/solutions/test-flakiness/vacuous-counting-assertion-loop-changed-its-own-precondition-2026-08-18.md` -
  the sibling failure mode: an assertion that reads as strong and cannot fail
- `docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md` - one level up:
  a check that goes green having asserted nothing
