---
title: "Chase the refuted prediction: it is the highest-information event in a measurement, and it finds defects inspection cannot"
date: 2026-08-10
category: best-practices
module: parallel-consumer-streams
problem_type: best_practice
component: development_workflow
severity: medium
applies_when:
  - "A measurement is planned and predictions can be written down before it runs"
  - "A predicted-green test stays red, or a predicted count comes out at zero"
  - "A negative control lands somewhere other than where it was predicted to land"
  - "A benchmark arm is slower than the arm it was supposed to tie with"
related_components:
  - parallel-consumer-core
  - documentation
tags:
  - prediction-first
  - refutation
  - measurement
  - investigation-method
  - benchmarks
  - control-arm
status: "Worked example from the Kafka Streams on PC spike (issue astubbs#255, PR astubbs/parallel-consumer#271). The three refutations described here are all resolved; the defects they found are fixed or recorded on the shortcomings worklist."
related_prs:
  - "astubbs/parallel-consumer#271 - the Kafka Streams module spike; source of all three worked examples"
related:
  - "control-arms-vary-exactly-one-term.md - how to build the arm whose refutation is worth chasing"
  - "choose-the-statistic-that-states-the-claim.md - picking the number that can be refuted at all"
  - "../test-flakiness/unforceable-trigger-commit-lock-timeout-2026-08-07.md - the same rule in the test-flake domain, with its own refutation log"
  - "../test-flakiness/pc-silent-stall-under-contention-2026-07-29.md - prior art: a real product bug found while chasing an apparent flake"
---

# Chase the refuted prediction

## Context

[`AGENTS.md`](../../../AGENTS.md) already carries the rule, under "Before you investigate anything" >
"Settling it: a fix that works is not evidence of the cause" (`AGENTS.md:56-68`):

> **State the prediction before running it, and report the refuted ones.** A prediction that fails is
> the cheapest result you will get. If a fix works but its prediction was wrong, you have a symptom.

That rule was promoted from a test-flake investigation, and it tells you to *state* and *report*
refutations. It does not tell you what to do in the hour after one arrives. This document is the
missing half: **how to chase a refutation**, written from a different domain - a feature and
performance spike rather than a flake diagnosis.

The evidence is the Kafka Streams on Parallel Consumer spike (issue astubbs#255, PR
astubbs/parallel-consumer#271). Predictions were written into the plan **before** any measurement,
at both the test level and the suite level
([plan:706-756](../../plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md),
[plan:863-870](../../plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md)). Three were refuted. Each
refutation, when chased, paid for itself:

| Refuted prediction | Chasing it found |
|---|---|
| 12 of 14 offset/commit unit tests flip green | **0 flipped** - because those tests assert Kafka's metadata *encoding*, so the property under test was never observable there |
| The two corrupted-record tests flip green | **Two real product defects** - a lying progress signal, and a poison pill stalling its key by a full pump cycle |
| Negative control ties at ~1.0x | **0.69x** - the StreamThread's poll wait throttling dispatch, the largest single win on the board |

None of the three was findable by reading the code. In every case the code read as correct, and the
number is what disagreed.

## Guidance

**A refuted prediction is not a setback, it is a located error.** The prediction is a model with
named terms. A confirmation tells you the model is not obviously wrong, which is weak. A refutation
tells you at least one named term *is* wrong, and the prediction itself bounds which terms are
candidates. That bound is the whole value: the search space is finite, enumerable, and written down
before the result arrived, so it cannot be rationalised after the fact.

Chase it in this order.

1. **Re-read the prediction verbatim and split it into its separate claims.** Do not paraphrase it
   from memory, and do not edit it. "These two tests go green" was actually two claims - the drop
   path is synchronous, *and* the assertion is an offset check. Both were wrong, independently, and
   only one of them was a product defect. A single sentence in the plan was two hypotheses in a
   trench coat.

2. **Ask what the measurement could observe before asking whether the code is wrong.** Read the
   *assertion*, not the test name. `shouldUpdateOffsetIf...` sounds like it checks an offset; it
   compares a full `OffsetAndMetadata` including the metadata bytes, via `equalTo(...encode())`. A
   design that deliberately writes different metadata bytes fails that assertion no matter how
   correct its offsets are. The test could never have seen the property, so its redness carried no
   information about the fix at all.

3. **Confirm the run picked up your change** before believing any null result - the instrumentation
   rule in `AGENTS.md:69-77`. A refutation caused by a stale build is indistinguishable from a real
   one and wastes the entire chase.

4. **Classify the outcome before acting on it.** Four different things produce one red, and they
   have four different next actions:

   | Verdict | What it means | Next action |
   |---|---|---|
   | The fix failed | The mechanism does not do what you thought | Fix the mechanism |
   | The instrument is blind | The fix works; this measurement cannot see it | Find or build the arm that can, and say so in the write-up |
   | The model was incomplete | The fix works; another term dominates | Name the missing term, re-predict |
   | An unmodelled second defect | The fix works and exposed something else | File it separately, it is a real find |

   Collapsing all four into "it did not work, revert it" is how a correct fix gets thrown away. In
   this spike, a "0 of 14" written up as failure would have discarded a crash-safety fix that the
   integration arm proves red-then-green.

5. **Stop at a line, not at a story.** A plausible explanation is where chases go to die. Keep going
   until you can cite the code that makes the claim true or false - the `poll.ms` default, the
   `instanceof CorruptedRecord` branch, the return value of `dispatchAvailable`. If you cannot point
   at it, you have a hypothesis, not a finding, and it needs its own control arm (see
   [control-arms-vary-exactly-one-term.md](control-arms-vary-exactly-one-term.md)).

6. **Treat a control that misses in the wrong direction as the strongest signal of all.** A negative
   control predicted to tie, which instead comes out *worse*, cannot be explained by the thing you
   were measuring. The absence of concurrency explains a missing gain; it never explains a penalty.
   Something unmodelled is present, and it is present in the positive arm too, where a win was
   masking it.

7. **Write the refutation up first, and keep the prediction verbatim next to the measurement.** The
   result document for this spike leads its section with "The refuted predictions, and what each
   refutation taught"
   ([result:485-511](../../plans/2026-08-08-002-ks-on-pc-spike-result.md)), before any headline
   number. Rewriting the prediction to match the outcome destroys the bound that made the chase
   cheap, and it is invisible to a later reader.

**Budget for the chase, not just for the run.** Predictions that hold cost nothing to record.
Refutations are where the remaining investigation budget should go, and a plan that has no room to
chase one should not have stated the prediction in the first place.

## Why This Matters

Recording only confirmations is a silent way to throw away the best data a measurement produced. A
confirmed prediction moves you from "probably" to "probably, still"; a refuted one hands you a
located, bounded error in your own model of the system, for free.

What the three chases in this spike actually returned:

- **Two product defects that no review had caught**, both in code that reads perfectly well.
- **One performance mechanism** the spike was not looking for, and which turns out to be charged on
  every workload, not only the control's.
- **One correction to the evidence base itself** - a whole family of unit tests was shown to be
  incapable of demonstrating the property they were being used to judge, which redirected the proof
  to the integration arm where it is demonstrable.

The counterfactual matters as much as the finds. "0 of 14 flipped" is a natural sentence to write as
"the fix did not work". It was the opposite: the fix was exactly right, to the digit, and the
measurement was blind. One paragraph of chasing separated those two readings.

## When to Apply

- Any time a prediction is written before a measurement - which, per `AGENTS.md`, should be always.
- When a count comes out at zero. Zero is a result about the *instrument* at least as often as about
  the code.
- When a negative control does not land where it was predicted, in either direction and by any
  margin.
- When a fix is provably correct by one arm and still red in another. That gap is a fact about the
  arms, and it is worth naming explicitly in the write-up.
- **Not** when the measurement itself is unsound. Fix the arm first
  ([control-arms-vary-exactly-one-term.md](control-arms-vary-exactly-one-term.md)) or the statistic
  ([choose-the-statistic-that-states-the-claim.md](choose-the-statistic-that-states-the-claim.md));
  chasing a refutation produced by a two-term control just relocates the confusion.

## Examples

### 1. The zero that was a discovery about the instrument

**Predicted** (per-test, written before implementation, for all 14 pile-A cases -
[plan:863-870](../../plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md)): taking commit data from
PC's frontier instead of Streams' `consumedOffsets` flips the offset and commit accounting cluster
green. Only two of the fourteen were predicted to stay red, as by-design metadata-encoding
divergence.

**Measured**: `0 of 14 flipped`. 33 failures before, 33 after, stable sets, N=3 both sides
([result:517](../../plans/2026-08-08-002-ks-on-pc-spike-result.md)).

**Chased to**: Kafka's asserts compare the full `OffsetAndMetadata` *including* metadata bytes -
`equalTo(...encode())`. The design deliberately writes PC's payload there instead of Streams'
encoding, so every pile-A test that reaches its content assert fails on metadata bytes alone,
regardless of the offset ([result:498-507](../../plans/2026-08-08-002-ks-on-pc-spike-result.md)).

**Found**: the by-design pile is far larger than two of fourteen - the divergence contaminates
essentially every content assert in the cluster. The committed offset was in fact exact (offset 2,
matching stock to the digit), and crash safety is red-then-green in the three integration tests. The
zero was the measurement telling us what it could see, not what the code did
([result:513-528](../../plans/2026-08-08-002-ks-on-pc-spike-result.md)).

### 2. The two red tests that were hiding two product defects

**Predicted** with high confidence: the all-corrupted and all-invalid-timestamp tests flip green.

**Measured**: both stayed red.

**Chased**, splitting the prediction into its two claims - the drop path is synchronous, and the
assert is an offset check - and found the first claim false in two separate places:

- **Corrupted records were shipped to workers as no-op runs.** Stock handles corruption synchronously
  on the processing thread; the PC path made it asynchronous for no benefit. Under KEY ordering that
  is a liveness defect: a poison pill held up its own key's successor for a full pump cycle, roughly
  `poll.ms`. Fixed by consuming corrupted records inline during preparation
  (`parallel-consumer-streams/src/main/patch/pc-streams.patch:538-546`) and by having the pump feed
  synchronous outcomes back within the same pass, so a synchronously-consumed record's key-mate
  becomes available immediately
  (`parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcTaskDispatcher.java:264-311`).
- **`process()` reported progress as dispatched-to-pool.** Consuming a batch of corrupted records
  dispatched nothing, so `process()` returned "no progress" to a caller that paces on exactly that
  signal. Fixed: progress is records *consumed* - dispatched, dropped, or failed at preparation
  (`PcTaskDispatcher.java:244-251`).

**Found**: both defects are real, both are in the product rather than the test, and both were
invisible to inspection. The third claim - the assert being an offset check - was also false, which
is why the tests are still red *by design* even with both defects fixed.

### 3. The negative control that went the wrong way

**Predicted** (A3, a falsifier written into the plan before the benchmark existed -
[plan:716-718](../../plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md)): with every record on one
key, PC's KEY ordering permits at most one in-flight record, so PC must show **no meaningful
advantage**. If it still wins, the gain is not key concurrency and both measurements are void.

**Measured**: 0.99x on min and **0.69x on p50** - not a tie, a loss
([plan:769-772](../../plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md)).

**Chased**, on the reasoning that the absence of concurrency explains a missing gain but never a
penalty. Against an ideal serial time of 2100ms, stock overshot by ~91ms and PC by ~1786ms: about
74ms per record buying nothing.

**Found**: `StreamThread` is a single thread that both polls and processes, so blocking for
`poll.ms` (100ms by default) costs stock nothing - there is no work it could be doing instead. Under
the seam that assumption is false: workers process in the background and a blocked poll stalls
*dispatch*. Confirmed by a one-term experiment changing only `poll.ms`, which moved experiment A
from 8.0x/3.5x to 19.1x/11.8x
([plan:1315-1348](../../plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md)). The penalty is ~98% poll
wait and is charged on **every** workload; it was merely masked in the positive arm by the
concurrency win. It is now recorded as the largest single improvement available, with a wake-on-work
design.

A control that had merely tied would have passed, been ticked off, and hidden this.

## Related

- [`AGENTS.md`](../../../AGENTS.md) - "Settling it: a fix that works is not evidence of the cause".
  The rule this document extends; read it first.
- [control-arms-vary-exactly-one-term.md](control-arms-vary-exactly-one-term.md) - designing the arm
  whose refutation is worth chasing. The same spike had to correct a control that varied two terms
  before its result meant anything.
- [choose-the-statistic-that-states-the-claim.md](choose-the-statistic-that-states-the-claim.md) -
  a prediction stated against the wrong statistic cannot be usefully refuted.
- [../test-flakiness/unforceable-trigger-commit-lock-timeout-2026-08-07.md](../test-flakiness/unforceable-trigger-commit-lock-timeout-2026-08-07.md)
  - the closest sibling: the same rule applied in the test-flake domain, with its own log of
  refuted hypotheses under "What Didn't Work".
- [../test-flakiness/pc-silent-stall-under-contention-2026-07-29.md](../test-flakiness/pc-silent-stall-under-contention-2026-07-29.md)
  - prior art for the payoff: a real product bug (drain-path zombie/busy-spin) found while chasing
  what looked like a flake.
- [docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md](../../plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md)
  - the predictions, stated before the runs (U8 at §682, the pile-A classification at §1093).
- [docs/plans/2026-08-08-002-ks-on-pc-spike-result.md](../../plans/2026-08-08-002-ks-on-pc-spike-result.md)
  - the measured outcomes, refutations first (§9b).
