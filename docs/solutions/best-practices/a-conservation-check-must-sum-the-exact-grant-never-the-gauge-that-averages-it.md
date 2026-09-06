---
title: "A conservation check must sum the exact grant, never the gauge that averages it"
date: 2026-09-07
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "Writing a conservation or identity check that sums a per-instance entitlement against a minted or spent total"
  - "A gauge or metric deliberately reports a rotation- or phase-averaged share for an operator's eyes"
  - "A proof harness samples a value at a fixed interval and a participant can stop or die between samples"
  - "An identity check fails by a small, bounded amount that tracks which participant held a contiguous or rotating share"
  - "Deciding whether to widen a conservation check's slack instead of fixing what it samples"
root_cause: test_design_bug
resolution_type: test_fix
related_components:
  - PartitionShareResourceAllocator
  - QuantumArithmetic
  - ChildPcMain
  - NavigatorProofEnvelope
  - ChildLedgerRecord
tags:
  - conservation-check
  - rotation-averaged-gauge
  - control-arm
  - sampler-gap
  - quantum-arithmetic
  - navigator
  - partition-share
  - credit-accounting
related:
  - "a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md - the sibling rule one mechanism over: a check crosses its bound by an offset the instrument itself produces (detection latency there, rotation phase here), and the fix corrects the check's reference value rather than widening the bound"
  - "a-stress-probe-is-an-instrument-you-built-not-a-test.md - the same genre: a hand-built harness is an instrument with its own calibration, and the instrument decides whether the bug exists, so check the instrument first"
  - "../logic-errors/counter-clamp-hid-a-conditional-decrement-bug-2026-08-21.md - the same discipline on the product side: derive the number the check wants from the mechanism, never maintain or correct an approximation of it"
  - "../workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md - the class this is an instance of: before trusting what a check reports, confirm it was comparing against the right quantity"
  - "../../plans/2026-09-05-1046-feat-navigator-partition-share-plan.md - the dated plan whose R10 and AE7 name the fleet identity this lesson comes from"
---

# A conservation check must sum the exact grant, never the gauge that averages it

> Extracted from the churn ladder built in astubbs/parallel-consumer#456.
> `docs/plans/2026-09-05-1046-feat-navigator-partition-share-plan.md` (R10, AE7) is the dated record.

## Context

The navigator's partition-share rung mints a Credit per quantum by Partition-share: a holder's
fraction of the subscription's partitions, divided through a remainder that rotates across quantum
indexes so no partition's share is ever silently dropped
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/navigator/QuantumArithmetic.java`,
`shareFor`). A holder of six contiguous slots of twelve is entitled to 2 credits on most indexes,
0 on most of the rest, and 1 only on the two indexes where the rotating pair straddles the block's
edge - averaging 1.0 across the rotation while minting 1.0 on just those two of every twelve.

A multi-process integration harness runs real child JVMs and, at each child's stop, emits a
conservation ledger record to the broker: minted, spent, expired, overdraft, outstanding, and
`sharesSummed` - the harness's own observation of what the child was entitled to, summed over every
quantum index it lived through. The parent checks the fleet identity: minted plus overdraft across
every tagged child stays at or under `ceil(sharesSummed)` plus one Credit of slack per child
(`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/utils/NavigatorProofEnvelope.java`,
`conservationSlack`, `fleetIdentity`). The identity descends from the in-process rung's rule that
credit accounting is derived by conservation from monotonic counters, never a maintained count
(session history) - so its slack is priced against a named cause, not chosen because the check
passed at it.

The twelve-rung churn ladder (fleets of 2, 3, 4; RANGE and COOPERATIVE_STICKY; zero-offset and
skewed clocks) passed the per-instance identity and the overshoot bound on every rung of every run.
The fleet identity did not: two of the three pre-fix runs failed it, three failing rungs in
thirty-six, always on an N=2 rung where the surviving joiner holds a contiguous six-of-twelve block.
Run 1 was green by phase luck (rung 4 landed 29 against 29, rung 6 landed 48 against 48); run 2 lost
rung 4 (minted 32 against a ceiling of 31); run 3 lost rung 2 (35 against 31) and rung 3, at
**zero** clock offset (32 against 31). Every per-instance identity balanced - minted equalled spent
equalled fired everywhere. Nothing was over-minted. (The churn-ladder commit's body on the branch
says three of three runs failed; that was written from the failure count, three rungs, not the run
count. The run-by-run record read at the time is what this doc reports, and the CI-produced record
under `docs/test-hardening/` will be the committed evidence.)

The control arm: apply the six-slot rotation to the failing joiner's actual life phase and predict
the deviation before touching anything. Predicted +2. Observed +2. The zero-offset failure ruled
clock skew out on its own, since skew contributes nothing at zero offset and the identity still
failed.

## Guidance

1. **A conservation check compares minted against the exact per-interval grant, never an average of
   it.** The sampler was summing the view's rotation-averaged gauge - the right number for a human
   asking "why am I at 1Hz", the wrong number for an identity that must close exactly. Averaged over
   a whole rotation period the two agree; over any partial window - which is what a ladder rung is -
   the cumulative average deviates from the cumulative actual mint by the rotation's phase, up to
   half the slots a contiguous block holds. That deviation is not noise: it is a deterministic
   function of which indexes the run happened to land on.

2. **When an instrument disagrees with a mechanism that is supposed to balance, predict the
   deviation from the mechanism before touching the tolerance.** If the prediction matches the
   observation, the instrument is wrong, not the mechanism. Here the six-slot rotation predicted
   exactly the observed overshoot on the failing joiner's own phase. The fix that worked is not
   itself evidence of the cause; the prediction, checked before the fix, is.

3. **Never price an instrument defect into slack.** The repo forbids widening a bound to make a red
   run green, and an instrument bug is the same trap under a different name: raising
   `conservationSlack` would have made the ladder pass while asserting a weaker and wrong thing
   about the fleet. The fix instead gave the allocator a pure read,
   `entitledCredits(resourceName, quantumIndex)`, returning exactly what minting that index produces
   - the same computation `readQuantum` mints from, not a projection of it
   (`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/navigator/PartitionShareResourceAllocator.java`,
   `entitledCredits`) - and pointed the sampler at that instead of the gauge. The javadoc there
   states the distinction: the gauge is deliberately averaged for `localRatePerSecond` and the views
   built on it; a conservation sum needs the exact grant.

   Before, in the child's share sampler as first committed (the averaged gauge):
   ```java
   OptionalDouble credits = view.creditsPerQuantum(resource);
   // ... the current quantum index on the child's own clock ...
   creditsByQuantum.computeIfAbsent(resource, ignored -> new ConcurrentHashMap<>())
           .merge(quantumIndex, credits.getAsDouble(), Math::max);
   ```
   After, in
   `parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/utils/ChildPcMain.java`
   (`class ShareSampler`):
   ```java
   double entitled = allocator.get().entitledCredits(resource, quantumIndex);
   creditsByQuantum.computeIfAbsent(resource, ignored -> new ConcurrentHashMap<>())
           .merge(quantumIndex, entitled, Math::max);
   ```

4. **An exact sum exposes the next flaw, so re-run the instrument after fixing it rather than
   trusting the first green.** The runs on the exact sampler closed the identity to the credit on
   every rung (33 against 33, 29 against 29) - and that exactness then surfaced a second instrument
   defect the averaged version had hidden inside its own slack: a child stopped inside the sampler's
   first interval of an index had minted that index without ever sampling it, and a full holder's
   final index is worth the whole grant. Two runs landed exactly on the ceiling by borrowing a
   sibling's slack (one child minted 29 against 27 entitled; another 13 against 12). The fix takes
   one more sample synchronously after the processor closes, so the last minted index is never
   missed
   (`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/utils/ChildPcMain.java`,
   `sampler.sample()` inside `emitLedger`) - after which the one-Credit-per-child slack covers only
   a sampler pass starved past a whole index, and both incidents are recorded in
   `NavigatorProofEnvelope`'s `conservationSlack` javadoc so nobody re-derives either one.

5. **Say plainly what the identity can and cannot catch.** `ChildLedgerRecord`'s javadoc names the
   limit: `sharesSummed` sums entitlement over every index the child lived through, read or not, so
   the fleet identity is a coarse check - minted can never exceed the entitlement of the indexes a
   child existed for, but it cannot see one child over-minting an index while a sibling under-mints
   the same one. The sharper check is a per-index sum across children against the grant, recorded as
   a follow-up in astubbs/parallel-consumer#456 rather than built; the ladder's window bound on the
   broker's clock is what stands as the defence against over-mint.

## Why This Matters

An averaged instrument fails in both directions and both are expensive. It can red a working
mechanism - two of three pre-fix runs failed a fleet identity that was, per instance, balancing the
entire time, and the natural response to a repeated red is to widen the bound, which would have
shipped a weaker check while looking like a fix. It can also green a broken one: because the
deviation is a function of rotation phase, a run that lands on a favourable phase reports a clean
identity even where the per-index mechanism over-minted - run 1's exact landings were phase luck, not
proof. Once the check compares against the exact grant, both failure modes close together.

## When to Apply

Any check that sums a rate, gauge or average to bound an integer-valued mechanism that mints in
whole units per interval - credits, permits, slots, token-bucket refills, sharded quotas, rotating
remainders. If the mechanism mints unevenly across intervals by design (a remainder that rotates, a
burst that arrives late, a lease that starts mid-window), the sum of an average over a partial
window will not match the sum of what was minted, and the gap is bounded by the mechanism's own
period - calculable, not measurement noise.

## Examples

Measured on 2026-09-05 on the implementing machine and read from the session's run logs (no
committed record carries these per-rung numbers yet), the twelve-rung churn ladder (fleets of 2, 3,
4 x RANGE/COOPERATIVE_STICKY x zero-offset/skewed clocks), fleet identity only (per-instance
identities balanced throughout):

| Runs | Sampler | Fleet identity result |
|---|---|---|
| 1-3 | averaged gauge (`creditsPerQuantum`) | run 1 green by phase luck (rung 4 at 29 against 29); run 2 lost rung 4 (32 against ceiling 31); run 3 lost rungs 2 and 3 (35 against 31; 32 against 31 at zero clock offset) |
| 4-6 | exact per-index entitlement (`entitledCredits`), sampled mid-loop only | every rung closed to the credit (33 against 33, 29 against 29), but two runs landed exactly on the ceiling from the end effect: one child minted 29 against 27 entitled, another 13 against 12 |
| 7-9, and three later runs | exact entitlement plus the closing sample after the processor closes | no child minted above its exact entitlement on any rung; every residual gap was an under-mint from a control-loop pass that read no quantum, the safe direction |

The control arm that settled the diagnosis: the six-slot rotation applied to the failing joiner's
actual life phase predicted +2, and the observed overshoot was +2. The zero-offset failure in run 3
ruled clock skew out as a necessary ingredient, since the rotation-phase deviation reproduces with no
skew term at all.

## Related

- `a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md` - the sibling rule one
  mechanism over: detection latency there, rotation phase here; in both, the instrument produces the
  offset and the fix corrects the reference value, never the bound
- `a-stress-probe-is-an-instrument-you-built-not-a-test.md` - the harness is an instrument with its
  own calibration; check the instrument first
- `../logic-errors/counter-clamp-hid-a-conditional-decrement-bug-2026-08-21.md` - the same discipline
  on the product side: derive the number from the mechanism, never maintain an approximation of it
- `../workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md` - the class
  this is an instance of
- astubbs/parallel-consumer#228 - the feature this rung belongs to; the defect and its fix live in
  astubbs/parallel-consumer#456's body and the plan, not on the issue
