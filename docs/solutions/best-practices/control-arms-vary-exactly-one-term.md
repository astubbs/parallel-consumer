---
title: "A control arm must vary exactly one term, not a term and whatever it silently derives from it"
date: 2026-08-10
category: best-practices
module: parallel-consumer-streams
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "Writing a negative control arm meant to falsify a measured performance or concurrency claim"
  - "A test fixture derives one property (cost, size, delay) from another (key, id, index)"
  - "A benchmark's control result is being read as proof that an effect vanished"
tags:
  - control-arm
  - benchmarking
  - test-design
  - negative-control
  - co-variation
  - kafka-streams
---

# A control arm must vary exactly one term, not a term and whatever it silently derives from it

## Context

`AGENTS.md:61-66` ("Settling it: a fix that works is not evidence of the cause") already states the
core rule:

> Confirm a cause with a control arm, not with a fix that appears to work. Change the one term you
> believe is responsible, hold everything else identical, and show the outcome flips.
> Same-magnitude, different-position beats bigger-hammer.

This doc does not restate that rule. It documents a specific way to violate it while believing you
are following it: **a test fixture that derives one property from another**, so that changing the
term you intend to vary silently drags a second, unintended term along with it. The control still
*looks* like it varies one thing - the code that sets it up only touches one parameter - but the
values it produces vary two.

## Worked evidence

`astubbs/parallel-consumer#271` (tracking issue `astubbs/parallel-consumer#255`) added
`HeadOfLineBlockingBenchmarkTest` to measure whether PC-driven Kafka Streams dispatch removes
head-of-line blocking: one slow "blocker" record at the head of a partition, many fast records queued
behind it on other keys. The negative control (`singleKeyRemovesTheAdvantage()`,
`HeadOfLineBlockingBenchmarkTest.java:156-183`) exists to falsify the experiment: put every record on
the *same* key, which makes concurrency impossible under PC's per-key ordering, and the measured
advantage should collapse.

The single term the control was meant to vary was key cardinality. But the fixture selected
per-record processing cost **by key**:

```java
sleep(SLOW_KEY.equals(key) ? SLOW_COST : FAST_COST); // the bug, pre-fix
```

Putting every record on the blocker's key satisfied that condition for every record, not just the
blocker. The control therefore varied cardinality *and* the cost distribution - two terms, not one -
and both arms it produced were running a different workload than the experiment, not a
cardinality-only variant of it.

**The tell was magnitude, not direction.** The buggy control's median was 19568ms against the
experiment's 1865ms - roughly an order of magnitude apart, not the "somewhat different, control
absorbed the effect" shape a real cardinality-only control produces. See the comment recording the
fix at `parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/HeadOfLineBlockingBenchmarkTest.java:258-263`:

```java
// Cost is chosen by VALUE, not by key. Keying it on SLOW_KEY made the single-key control change two
// terms at once: every record there carries that key, so every record became a 1500ms record and the
// control was measuring a different workload as well as a different cardinality. Measured before the
// fix: the control's p50 was 19568ms against experiment A's 1865ms. A control arm may differ in
// exactly one term, and here that term is cardinality.
sleep(BLOCKER_VALUE.equals(value) ? SLOW_COST : FAST_COST);
```

The fix selects cost by `BLOCKER_VALUE` (the record's payload), a property independent of which key
carries it. Cardinality became, and stayed, the only difference between experiment and control. See
also `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md`, section U8, "Two corrections made
during the run" (around line 774-779), which records the same fix and its numbers as part of the
benchmark's audit trail.

With the fixture corrected, the control produced a genuinely informative result: PC measured **0.69x**
on p50 with a single key - *slower* than stock, as it must be when key ordering forbids concurrency
and the pool handoff to a worker still costs something. A control that merely tied with stock would
have been weaker evidence; one that loses, for a reason the design predicts, is stronger.

## Guidance

- **State the single term explicitly before writing the control.** Not "the negative-control version
  of the experiment" - name the one property that changes: "key cardinality, and nothing else."
- **Check every value the fixture computes for a dependency on that term**, not just the ones the
  control code directly sets. `singleKeyRemovesTheAdvantage()` only set the key; the cost was computed
  from it three call frames away in the topology under test. A derived property is dangerous exactly
  because the line that introduces the co-variation is nowhere near the line that sets up the control.
- **Prefer selecting fixture properties by something orthogonal to the term under test.** Here, the
  fix was to select cost by *value* instead of *key* - value and key are independent axes in this
  fixture, so varying key cardinality can no longer touch cost. When a fixture must derive one
  property from another, that derivation itself becomes something the control has to defeat, not
  something it can lean on.
- **Compare arm magnitudes as a sanity check, before reading direction.** A cardinality-only control
  should look like the experiment's workload run under a different concurrency ceiling - same
  per-record cost, same rough order of total time - not a different order of magnitude entirely. If
  the control's numbers are 10x away from the experiment's rather than merely different, that is a
  co-variation signal, not confirmation of a stronger effect.
- **A control that goes the "wrong" way for a principled reason is stronger evidence than one that
  ties.** `0.69x` (PC slower under forced single-key serialisation, because the pool handoff still
  costs) is more convincing than "PC and stock came out equal," because it is a second, independent
  prediction the design makes and the corrected control confirms it.

## Why This Matters

A control that varies two terms is not weak evidence - it is evidence for a different claim than the
one being tested. The pre-fix control's ~10x-slower result would have been reported as "the seam gives
no advantage with one key" when it was actually measuring "the seam gives no advantage when every
record is also slow." Both experiment and control would have looked internally consistent - the
numbers checked out, the assertions passed the loosened thresholds a rushed read might apply - while
proving nothing about key concurrency at all. Derived-property co-variation is the failure mode that
survives the `AGENTS.md` control-arm rule's own checklist: someone can genuinely change "one term you
believe is responsible" and still get a fixture that varies two, because the second variation is
downstream of the first, in code the control author did not touch.

## When to Apply

- Writing or reviewing any negative control meant to falsify a measured effect (performance,
  concurrency, ordering).
- The fixture under test computes any property - cost, size, delay, retry count, payload - from an
  identifier the control also manipulates (key, partition, id, index).
- Reading a control's result and about to conclude "the effect vanished" or "the effect held" - check
  the arm's magnitude against the experiment's before trusting the direction.
- Refactoring a benchmark or test fixture: re-check that no derivation introduced by the refactor now
  ties a property to a term some other test varies independently.

## Examples

Before (co-varies silently): cost keyed off the same field the control manipulates.

```java
sleep(SLOW_KEY.equals(key) ? SLOW_COST : FAST_COST);
```

After (cost is orthogonal to the term under test): cost keyed off an independent property, so varying
key cardinality cannot also vary cost.

```java
sleep(BLOCKER_VALUE.equals(value) ? SLOW_COST : FAST_COST);
```

Sanity check to run on any control's output before trusting it: is the control's magnitude in the same
ballpark as the experiment's, adjusted only for the term you changed? `19568ms` against `1865ms` is not
"a cardinality-one workload took somewhat longer" - it is "this is a different workload."

## Related

- `AGENTS.md`, "Settling it: a fix that works is not evidence of the cause" (the general control-arm
  rule this doc specializes).
- `docs/solutions/best-practices/chase-refuted-predictions.md` - what to do when a control's own
  anomaly (not just its pass/fail) points at a real, unlooked-for effect.
- `docs/solutions/best-practices/choose-the-statistic-that-states-the-claim.md` - a companion failure
  from the same benchmark, where p99 was the wrong statistic to assert the claim on even with the
  workload correct.
- `astubbs/parallel-consumer#271` - the PR containing the fix and both benchmarks.
- `astubbs/parallel-consumer#255` - the tracking issue for the Kafka Streams dispatch spike.
