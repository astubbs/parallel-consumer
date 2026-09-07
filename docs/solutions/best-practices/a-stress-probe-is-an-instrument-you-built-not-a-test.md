---
title: "A hand-written stress probe is an instrument you built, not a test: what its zero, its rate and its copied code are each worth"
date: 2026-08-25
category: best-practices
module: tooling
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "Adding a jcstress, Lincheck or any other hand-written concurrency probe"
  - "About to write `0 anomalies` or `no violations` into a plan, a PR body or a decision"
  - "Quoting a per-sample or per-iteration anomaly rate that came off one machine"
  - "A probe or benchmark hand-copies a field layout, a statement order or an access mode from product code instead of importing it"
  - "Reviewing a probe harness that is deliberately unwired from the reactor build"
  - "Tempted to deduplicate near-identical arms because a clone detector flagged them"
related_components:
  - development_workflow
  - documentation
tags:
  - concurrency
  - jcstress
  - lincheck
  - false-negative
  - positive-control
  - negative-control
  - measurement
  - test-drift
  - java-memory-model
---

# A hand-written stress probe is an instrument you built, not a test

## Context

A unit test is a claim about behaviour. A stress probe is an **instrument**: you assemble it, point it
at a pattern, and read a number off it. Instruments have calibration, denominators and drift, and
none of those are things a test suite normally makes you think about — so a probe harness gets read
with a test suite's habits, and three separate mistakes follow.

The worked case is `jcstress-poc/`, the probe module added in astubbs/parallel-consumer#348 to answer
whether `PartitionState`'s plain non-volatile `long`s misbehave on real hardware. It answered the
question. It also demonstrated all three mistakes, which is why this is written up as the technique's
honest ceiling rather than as a success story.

`docs/solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md`
**owns the general class** — before trusting a negative, produce a positive; prefer instruments that
report their denominator. What is here is the part specific to stress probes, where the asymmetry is
not a tooling misconfiguration but *intrinsic*: a hit proves a reordering exists, and no run length
can prove one does not.

## Guidance

### 1. A hit is proof; a zero is a bound at N — and the runner grades them the same

jcstress grades an `ACCEPTABLE_INTERESTING` outcome with **zero observations** as `PASSED`. A run
whose actors never actually raced therefore prints `[OK]` for every probe, and is indistinguishable
test-by-test from a run that looked hard and found nothing. `jcstress-poc/pom.xml` carries the warning
in its header as `READ THE CALIBRATION BEFORE BELIEVING ANY ZERO`. (Observed on jcstress 0.16 while
building that module, and recorded there; not re-derived from the runner's grading source, which is
not vendored in this tree. Confirm it against your own version before relying on it.)

**So the positive control is read first, or nothing else in the run means anything.**
`CalibrationProbes.PlainFieldStoreLoadReordering` exists only to be non-zero: its `0, 0` bucket
landed 3,555,520,156 times (54.5% of samples), which is what licenses reading any other number from
that run. If that bucket is zero, every other zero is uninterpretable.

**Write zeros with their denominator, never bare.** "Word tearing is absent" is not a result;
"0 in 1.02e10 samples across four JVM configurations" is. Absence is at N, and saying so is what stops
a reader upgrading a bound into a proof.

### 2. Make the zero you care about *fatal*, and the zero becomes an assertion

A zero on an `ACCEPTABLE_INTERESTING` outcome is a bound. A zero on a `FORBIDDEN` outcome is an
assertion the run would have failed had it been violated — the same distinction as a test that asserts
versus a test that observes. That is the whole reason `volatile` can be claimed *sufficient* here
rather than merely *unfalsified*:

```java
@Description("Control arm: only the dirty flag volatile - the payload should ride the release/acquire edge")
@Outcome(id = "1, 0", expect = FORBIDDEN,
        desc = "Publication through the volatile flag failed - would invalidate the cheap fix")
```

0 in 4.29e9 samples against a `FORBIDDEN` declaration is a *checked* claim. The same 0 against an
`ACCEPTABLE_INTERESTING` declaration would have been an unchecked one. **Every claim about a fix needs
a control arm whose failure is fatal.**

### 3. A stress rate is a property of the machine, not of the code

Rates off one box do not transfer, and the size of the effect is measured rather than feared.
astubbs/parallel-consumer#347 fitted the same class of hit rate for its Lincheck arms across two
machines and found them **3.4x apart** — 0.69% against 2.33% per iteration, with a likelihood-ratio
test rejecting equality (LR 6.42 on 1 df, **p = 0.011**).

Two consequences:

- **Name the machine wherever a rate appears**, including in the verdict line. "Yes, on this machine"
  is a claim a reader can neither evaluate nor reproduce, and the fix is one clause: the hardware, the
  JDK build, the tool version.
- **Never calibrate a bound from one box and ship it as universal.** An iteration count or timeout
  fitted on a fast machine is a different bound on a slow one, and the arm goes from
  "asserts the race is findable" to "flakes".

Architecture makes this worse in a *directional* way, which is easy to misreport: arm64 permits
store-store reordering that x86-64's TSO forbids, so **a zero on x86-64 does not contradict a hit on
arm64** — it shows the hardware half of the effect is absent there while the compiler half remains.
Reporting that zero as "failed to reproduce" is the mistake.

### 4. A probe that replicates code instead of importing it decays silently

This is the technique's real ceiling, and it is structural. In `jcstress-poc/`, **no probe imports a
product class**: every import in every probe is `org.openjdk.jcstress.*` or `java.*`, the module's
only dependency is `jcstress-core`, and the sole `bz.stub.parallelconsumer` token in each file is its
own `package` line. Each probe is a hand-copied replica of a field layout, an access mode and a
statement order, bound to the real code by nothing but a human having copied it correctly on the day.

Unwiring makes it worse in exchange for something real. The module has no `<parent>` and is absent
from the root `<modules>`, which is deliberate and correct — a full run is measured in hours and must
never enter the normal build. But the same unwiring means **CI does not even compile it**, so the
replicas can rot for months with nothing going red, and its third-party pin sits outside every
dependency and CVE lane.

**Buy back what you can, cheaply, and be explicit about the rest:**

- **Prefer importing the real class** and probing it directly. Where the memory model *is* the subject
  that is often impossible — the probe has to control field declarations the product does not expose —
  but establish that it is impossible rather than assuming it.
- **When you must replicate, add a correspondence check** in the style of `bin/check-file-refs.sh`:
  assert the modelled fields are still non-volatile, and that each quoted snippet still greps in the
  file it claims to model. It costs the main build nothing and it is the only thing that fails when
  the model and the code diverge.
- **Treat that check as the prerequisite for growing the harness**, not a nicety. The growth model is
  one hand-written probe per suspected field pair; adding probes before the check exists multiplies
  the exposure linearly.
- **Say in the probe's javadoc which lines it models**, so a reader can re-verify by hand when no
  check exists. Prose is weak, but it is not nothing, and it is what a future correspondence check
  will be generated from.

### 5. Near-identical arms are the measurement — a clone detector cannot see that

A probe harness is *supposed* to contain near-copies: a reduced arm and a faithful arm differing only
in their surrounding accesses, and a volatile control arm that is a near-copy of the arm it controls.
In this case the ~130x suppression between the reduced and faithful arms (1.9e-5 down to 1.4e-7) **is
the finding** — it is the evidence that the surrounding code was not already closing the hole by
accident. Collapse the arms to quiet a similarity report and you delete the result.

Measure before arguing: here similarity peaked at 59.7% against an 80% fail threshold and both
duplication caps held, so there was nothing to answer. The standing hazard is that these tools have no
allowlist for duplication that is *correct*
(`docs/inflight/ci-dup-similarity-cannot-accept-known-duplication.md`), so the pressure arrives
eventually. **Refuse it, and record why in the harness rather than only in the PR that argued it.**

## Why This Matters

The three failures compound in one direction: **towards believing the code is fine.**

A vacuous run reads as green. A bare zero reads as proof. A rate from a fast machine reads as an
upper bound when it is a lower one. A rotted replica reads as a passing model of code it no longer
resembles. Nothing in any of those outputs invites a second look — which is exactly the property that
made this repo name the class in the first place.

And the stakes are asymmetric in the opposite direction from a normal test. A flaky test costs a build.
A probe harness that reports a clean memory model retires a *real* concurrency bug from the backlog,
in a library whose entire purpose is to preserve ordering guarantees. The instrument gets to decide
whether the bug exists, so the instrument has to be the thing you check first.

## When to Apply

- **Before writing any "0 anomalies" or "no violations" into a durable record** — attach the
  denominator and the positive control's own number.
- **Before quoting a rate** — name the machine, the JDK build and the tool version in the same
  sentence.
- **Before fitting an iteration count, a timeout or a sample budget** from one machine's runs.
- **When reviewing a probe, benchmark or harness that hand-copies product code** — ask what fails when
  the original moves. If the answer is "nothing", that is the finding.
- **When a clone or similarity report flags a probe harness** — check whether the duplication is the
  measurement before touching it.
- **When a probe module is deliberately unwired** from the reactor build — enumerate what else stops
  covering it: compilation, dependency updates, CVE scanning, cache keys.

## Examples

Reading a run, in order. The first command is the one nobody runs because the answer feels obvious:

```
# 1. The positive control, FIRST. Non-zero, or stop - nothing else in this run is interpretable.
CalibrationProbes.PlainFieldStoreLoadReordering  "0, 0"  ACCEPTABLE_INTERESTING  3,555,520,156  (54.5%)

# 2. A hit. This is proof the reordering exists; it needs no defending.
CommitPathVisibilityProbes (faithful arm)        "1, 0"  ACCEPTABLE_INTERESTING            298  / 2.12e9

# 3-4. Two zeros, both on FORBIDDEN. Checked claims: the run would have failed had either appeared.
CommitPathVisibilityProbes.VolatileDirtyPublishesPlainSucceeded  "1, 0"  FORBIDDEN  0  / 4.29e9
CalibrationProbes.PlainLongWordTearing                          (torn)  FORBIDDEN  0  / 1.02e10
```

Every zero this run reports is a checked one, and that is the design rather than luck. Had either
outcome been left `ACCEPTABLE_INTERESTING`, the identical `0` would have been a bound at N — and the
run would have printed `[OK]` for it either way, which is why the grade in the declaration matters
more than the number beside it.

How the verdict line should read, since the record is what outlives the run:

```
- WRONG: **Yes, on this machine, and the cheap fix is demonstrated to close it.**
- RIGHT: **Yes, on the machine these runs were taken on - Apple M2 Pro (arm64, 8P+4E, 12 logical),
  Temurin OpenJDK 17.0.18+8, jcstress 0.16 - and the cheap fix is demonstrated to close it.**
```

## Related

- [`../workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md`](../workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md)
  — **owns the general class**: prove the instrument could have said yes, and prefer instruments that
  report their denominator. This doc is the stress-probe instance, where the asymmetry is intrinsic to
  the method rather than a misconfiguration.
- [`../workflow-issues/a-check-that-reports-success-without-having-run.md`](../workflow-issues/a-check-that-reports-success-without-having-run.md)
  — the sibling for *gates*. A jcstress run grading a zero-observation outcome as `PASSED` is the same
  shape one layer down: a tool whose own failure mode is to exit zero.
- [`ablate-your-own-change-not-only-the-baseline.md`](ablate-your-own-change-not-only-the-baseline.md)
  — the same discipline for benchmarks: two arms say whether the whole thing is better, never which
  part did the work. The reduced-versus-faithful pair here is that ablation.
- [`../test-issues/dormant-regression-test-uncollected-by-surefire-2026-08-07.md`](../test-issues/dormant-regression-test-uncollected-by-surefire-2026-08-07.md)
  — a test that never ran and stayed green for four months. An uncompiled probe module is the same
  hazard with the collection step removed entirely.
- [`../logic-errors/boundary-claim-tested-only-on-friendly-samples.md`](../logic-errors/boundary-claim-tested-only-on-friendly-samples.md)
  — a documented claim the code did not implement, whose test sampled only confirming cases. The
  correspondence-drift risk in section 4 is that failure waiting to happen by hand-copy.
- [`../../inflight/test-jcstress-probe-module-open-items.md`](../../inflight/test-jcstress-probe-module-open-items.md)
  — what remains open on this specific harness, including the correspondence check that does not exist
  yet.
- [`../../plans/2026-08-25-002-test-jcstress-poc-plain-long-visibility.md`](../../plans/2026-08-25-002-test-jcstress-poc-plain-long-visibility.md)
  — the dated measurements, outcome tables and full environment.
- astubbs/parallel-consumer#348 — the probe module. astubbs/parallel-consumer#347 — the Lincheck lane
  and the cross-machine rate measurement in section 3.

## A rate is a claim about one machine

The jcstress and Lincheck arms were calibrated on different boxes, and the difference is measurable
rather than notional: for the Lincheck stress arms the detection rate came out **3.4x apart** -
0.69% against 2.33% per iteration, likelihood-ratio 6.42 on 1 df, **p = 0.011** - so equality is
rejected, not merely unproven (astubbs/parallel-consumer#347).

Two consequences worth carrying into any new probe:

- **A bound priced on the machine that wrote the harness is not a bound on the machine that gates
  it.** A probe tuned until it passes 3 times out of 3 locally can miss on CI at a rate nobody
  measured, and the failure arrives as a flake rather than as a finding.
- **Prefer the deterministic model checker over a stress arm where the shape allows it**, because it
  needs no bound priced at all. Where only stress will do, price the bound by starving the harness
  deliberately, fit the rate, and then *validate the model by prediction* rather than assuming it -
  and say which machine the number came from.

This is also the reason the arm64-versus-x86-64 caveat in a probe's own write-up is necessary but not
sufficient: architecture is one source of variance between boxes, and it is not the only one.
