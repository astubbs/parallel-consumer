---
title: "A stress probe's calibration is a claim about one machine: measure the per-iteration rate, then validate the model by prediction"
date: 2026-08-25
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "Choosing an iteration, repetition, duration or seed budget for a test that finds a race probabilistically"
  - "A concurrency probe found a defect on some runs and not others, and a bound is about to be picked from that"
  - "Reporting a hit rate, a flake rate, or a miss probability measured on the machine you happen to be on"
  - "Reviewing a claim of the form 'this bound misses about one run in N'"
  - "A probe is being promoted from a local lane onto CI hardware, or onto a maintainer's slower box"
related_components:
  - WorkManagerLincheckTest
  - ShardManagerLincheckTest
  - PartitionStateLincheckTest
tags:
  - calibration
  - lincheck
  - jcstress
  - stress-testing
  - maximum-likelihood
  - flake-budget
  - machine-dependence
  - concurrency-testing
related:
  - "ablate-your-own-change-not-only-the-baseline.md - the sibling rule for attribution; this doc is about the rate a probe finds anything at all"
  - "../workflow-issues/a-check-that-reports-success-without-having-run.md - the false-green class this lane's own roster guard belongs to"
  - "../workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md - why an under-budgeted bound cannot be papered over with a retry here"
---

# A stress probe's calibration is a claim about one machine

> Extracted from the Lincheck lane built in astubbs#347. The measurements below are that PR's;
> `docs/plans/2026-08-25-001-test-lincheck-poc-plan.md` is the dated record and owns the full tables.

## Context

A probe that finds a race by *exploring* rather than by *construction* - Lincheck in stress mode,
jcstress, a fuzzing loop, a soak test - does not have a pass/fail answer. It has a hit *rate*, and
whatever budget you give it (`iterations`, reps, seconds) converts that rate into a miss probability.
Pick the budget too low and you have not written a test, you have written a flake. In this repo that
matters more than usual, because **a flake fails the build with no retry, by design**
(`docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md`), so there is
no mechanism that quietly absorbs an under-budgeted bound.

Two errors were made in sequence while calibrating one such probe, and they are the same error at two
different scales - a sample of one generalised to a population.

## Guidance

### 1. Measure the per-iteration rate; do not infer the bound from outcomes at the bound

Running the harness at its candidate bound and counting passes cannot separate the rates you care
about. Three runs at 200 iterations, all green, is equally consistent with a 0% miss rate and a 10%
one. Worse, the runs are expensive precisely where they are most informative.

**Deliberately starve the harness instead.** Drop the budget until it misses most of the time; the
miss fraction then estimates the per-iteration probability, and that single number prices *every*
bound at once. Concretely, `WorkManagerLincheckTest` was run at `iterations(25)` where it misses more
often than not, rather than at the committed 1,000, where it missed none of 8 runs and so teaches almost nothing.

Fit the rate by maximum likelihood over all runs at all budgets, and report an interval, not a point:

| Quantity | Value |
|---|---|
| Per-iteration hit probability | 2.33% (95% profile-likelihood interval 1.38-3.72%) |
| Miss rate at 200 iterations | 0.89%, and 6.2% at the pessimistic end of the interval |
| Miss rate at 1,000 iterations | ~6e-9%, and ~9e-5% at the pessimistic end |

The pessimistic end is the number that decides a bound. A bound justified by the point estimate is a
coin-flip on the estimate being good.

### 2. Validate the independent-trials model by prediction, rather than assuming it

Converting a per-iteration rate into a miss rate at a different budget assumes each iteration is an
independent trial. That assumption is doing all the work and it is not obviously true - a large share
of Lincheck's generated scenarios cannot tear at all, because both actors land on the same operation.

Do not argue about it; **test it**. Fit at one budget and check the fit's predictions at budgets far
away from it:

- predicted 17.7 misses out of 32 at `iterations(25)` - 18 observed
- predicted 0.07 misses out of 8 at 200 - 0 observed

Agreement across a 40x span of budgets is the evidence that nothing else is going on. The scenarios
that cannot tear are already inside the measured marginal probability; they do not need modelling
separately. Without this check the interval in section 1 is arithmetic, not a measurement.

### 3. Eight runs is not a rate

The first attempt at section 1 used 8 starved runs: 2 hits in 8, implying 1.14% per iteration.
Twenty-four more runs moved it to 14 hits in 32 and 2.33% - **the first estimate was wrong by a
factor of two**, and its 8-sample interval was wide enough to contain almost anything.

The consequence is symmetric, which is the part worth remembering. Eight runs were not enough to
*condemn* the incumbent bound, and they were not enough to *bless* its replacement either. A
recalibration justified by an 8-run estimate replaces one unfounded number with another.

### 4. The rate is machine-specific, so a bound is a claim about a machine

This is the finding that outlives the particular bound. The same harness was measured on a second
machine - 8 runs, all at the 200 budget, against the first machine's 48 across three budgets - and it
is **3.4x slower to find the tear**: 0.69% per iteration against 2.33%. A likelihood-ratio test rejects the two machines being equal (LR 6.42 on 1 df, p = 0.011), so
this is a real difference and not sampling noise.

**Therefore no single-machine calibration of a stress arm transfers.** A bound that misses one run in
10^9 on the machine it was priced on can be missing one in a thousand somewhere else, and the place
it will surface is CI or a maintainer's laptop. State the machine alongside any hit rate you report,
the way you would state units.

The honest response when you have not measured the other machine is to **record the gap, not to
paper over it**. Inflating the bound to cover the tail of an 8-sample estimate from the other machine
is section 3's error pointing the other way. astubbs#347 left `iterations(1_000)` in place, wrote the
open item and the ~10 minutes of measurement that settles it into
`docs/inflight/test-lincheck-lane-open-items.md`, and left the decision to the machine's owner.

## Why This Matters

- **An under-budgeted probe is indistinguishable from a flaky one**, and it arrives as a red build on
  somebody else's PR, months later, with no diagnosis attached. The rule against loosening a test to
  go green then has nothing to work with, because the test was never calibrated in the first place.
- **The bound is the coverage.** Weakening it is not a cosmetic change to a test, it is a reduction in
  the probability that the test finds the class of bug it exists to find - which is why a scenario
  budget gets the same protection as an assertion.
- **A hit rate without a machine reads as a property of the code.** That is how a measurement stops
  being reproducible without anyone noticing it has: the number is right, the claim around it is not.

## When to Apply

- Any test whose success depends on a scheduler, a seed, or a timing window rather than on a
  constructed interleaving.
- Any time a bound, budget or timeout is being raised or lowered and the justification is "it passes
  now".
- Any time a rate is about to be written into a comment, a doc, or a PR body.
- When promoting a probe from a developer machine to CI - different core counts, different
  contention, and in this repo a self-hosted runner that may be running several suites at once.

## Examples

**Not this** - a bound chosen from outcomes at the bound, and a rate stated as if it were universal.
The snippet is a composite illustration, not a quotation of anything committed; the real predecessor
made no miss-rate claim at all, which is its own version of the problem:

```java
// Found the tear in 3 of 3 runs at 200 iterations. Misses about 1 run in 1000.
.iterations(200)
```

Three runs cannot separate a 10% miss rate from 0%, and the second sentence is a per-machine
measurement wearing the clothes of a property of the code. This bound was in fact a latent flake.

**This** - the committed form in
`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/state/WorkManagerLincheckTest.java`,
whose comment records how the number was priced (the starved `iterations(25)` measurement, the fit,
the prediction check, and the fact that the rate is one machine's rather than the harness's) instead
of only the number itself:

```java
.iterations(1_000)
```

The lane it belongs to, and how to run it without hand-rolling the five flags that each fail
silently, is `bin/lincheck-test.sh`; `docs/testing.md` owns the lane's documentation.

## Related

- Same shape, one level up: a *tool class* is not exhausted by the one configuration you ran. The
  same PR claimed "no static analysis can see this bug class" on the strength of stock SpotBugs
  alone, and astubbs#356 measured fb-contrib reaching one of the four targets statically.
- astubbs#348 raised the identical question from the jcstress side ("what machine is this?"), which
  is why this doc is written about probabilistic probes generally rather than about Lincheck.
- `docs/investigating.md` owns the surrounding method - control arms, instrumentation traps, and why
  a fix that works is not evidence of the cause.
