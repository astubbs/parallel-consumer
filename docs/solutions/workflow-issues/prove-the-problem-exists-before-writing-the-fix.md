---
title: "Prove the problem exists before writing the fix - deletion is a legitimate outcome"
date: 2026-08-18
category: workflow-issues
module: parallel-consumer-core
problem_type: workflow_issue
component: development_workflow
severity: high
applies_when:
  - About to write a fix whose premise is a symptom reading rather than a measurement ("the counter must be drifting", "the flag must be stale")
  - Inheriting a fix nobody measured, and deciding whether to keep, prove, or delete it
  - A fix targets an invariant (a counter equals reality, a cache matches its source) that nothing in the tree can currently observe
  - Writing the regression test for a fix, and choosing what it should observe
  - A regression test starts failing exactly when the fix lands
tags:
  - measurement-first
  - unverified-fix
  - ground-truth-probe
  - deletion-as-fix
  - regression-test
  - inverted-instrument
  - issue-857
---

# Prove the problem exists before writing the fix - deletion is a legitimate outcome

## Context

One commit on the confluentinc#857 fix branch bundled four independent changes, written months
earlier from a plausible reading of the symptoms. When each was finally measured, **two of the four
were fixes for problems that do not exist** - and both had made things worse in exactly the
dimension they claimed to repair. Both were deleted, and the deletions were accepted as results, not
as failures of the work.

[two-threads-one-consumer](../architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md)
owns the full decomposition and both measurements; what follows is only what each case contributes
to the method.

**The counter adjustment.** An in-flight-work counter read high after rebalances, and a comment in
the tree even names a "counter-drift signature", so a revoke-time adjustment was written to correct
the drift. A purpose-built probe - the real engine over a fake consumer, records genuinely held by
the worker pool, revoke driven through the engine's own listener, then quiesce and compare the
counter against ground truth - measured the unfixed code at **zero drift over five
revoke/reassign cycles**. The same probe measured the "fix" driving the counter to **-20, reading
zero while four records were genuinely in flight**. The premise had never been measured: the high
reading it was written against was the counter *truthfully* reporting work that never came back,
because the workers were wedged behind the deadlock that was the branch's real bug. The symptom had
one cause, already fixed elsewhere, and the second "fix" was correcting an honest reading.

**The pause reset.** A stale back-pressure flag was reasoned about from the eager rebalance
protocol, and a reset hook written accordingly - which under the cooperative protocol reintroduced
the paused-consumption symptom the branch existed to fix. Nobody had measured which protocol the
flag actually misbehaved under, or run the hook under both.

## Guidance

**Build the instrument that distinguishes the claim from reality, and run it against the unfixed
code, before writing the fix.** Not to settle what caused a problem - that is
[`docs/investigating.md`](../../investigating.md)'s territory - but one step earlier: to settle
whether the problem is there at all. A fix whose premise was never measured has no baseline, so
nothing can ever show it helped, and - as the counter showed - it can corrupt the very quantity it
adjusts while every test stays green.

- **The instrument compares the system's claim against ground truth the instrument itself
  establishes.** The counter probe worked because it knew, independently of the counter, exactly
  how many records were out with the pool. An instrument that reads back the system's own
  bookkeeping measures nothing.
- **"The premise is false" is a first-class result, and deletion is its fix.** Two deletions here
  each removed a live defect that had shipped inside a correction. Treat "we measured, the problem
  is not there, the fix is deleted" as a finding to record with the same care as a fix - otherwise
  the plausible reading that produced the fix will produce it again. (The AGENTS.md testing rule
  against loosening a failing test to green is the same discipline pointed at the other kind of
  edit.)
- **Keep the probe when you delete the fix.** The probe is the only artifact that can tell that
  story again cheaply; here it stayed as the regression test for the counter's honesty - the one
  thing in the repo that can tell the counter from reality.
- **A symptom consistent with your premise is also consistent with others.** The high counter fit
  "drift" and fit "honest reading of wedged work" equally well; only the ground-truth comparison
  separated them. Before fixing the bookkeeping, ask whether the books might be right and the world
  wrong.

### Validate the instrument on both arms - the inverted-instrument signature

The same branch carried the mirror-image failure: its own reproducer test, measured properly, ran
**5/5 green on the defective code and 5/5 red on the fixed code**. It observed the fix's mechanism
- it counted invocations of an internal method the fixed path no longer calls, in a mode the defect
cannot occur in - so the fix registered as a regression.

A clean inversion like that is a recognisable diagnostic signature, worth naming so it is read
correctly on sight: **a test that starts failing exactly when a fix lands, and was passing before
it, may be pinned to the old implementation rather than to the behaviour.** The reflex response -
the fix broke something - is exactly backwards in this case, and acting on the reflex reverts a good
fix. The check is the same both times: run the instrument on both arms before trusting it on either.
An instrument that cannot go red on the defect, or cannot stay green on the fix, is not measuring
the defect.

### Getting the defect arm: a hardcoded boolean beats reverting the commit

The obvious way to build the defect arm - `git revert` the fix - fails exactly when you most want
the answer, and the failure is confusing rather than obvious.

**It breaks when the tests call API the fix introduced.** Nine tests were written for a three-state
ownership guard, and every one of them calls `releaseOwnership()` or `tryClaimOwnership()`. Revert
the fix and those methods do not exist, so the arm does not compile, so the tests cannot be run
against it at all. The conclusion looks like "these tests cannot be red-black verified", which is
the wrong conclusion.

**It also drags in unrelated breakage.** Reverting a production change can cascade into call sites,
stale generated sources and build plugins - a Truth-assertion generator failing on a type the revert
removed reads as a test failure if you are not watching closely, and one such red bar was misread
as `0/3 passing` when nothing had run.

**Instead, disable the fix in place behind a hardcoded flag:**

```java
/** RED-BLACK SWITCH (temporary): false = pre-fix behaviour. */
private static final boolean TRI_STATE_ENABLED = false;

boolean isReleased() {
    return TRI_STATE_ENABLED && phase == Phase.RELEASED;
}
```

Both arms then compile identically, the tests are byte-for-byte the same on each, and the only
variable is the flag. In the case above this immediately gave the answer reverting could not:
**2 of the 9 tests failed** - precisely the two written for the new behaviour - while the other
seven kept passing, because they cover behaviour that survives the switch. That per-test resolution
is itself useful: it says which tests guard the fix and which were already passing anyway.

Put the flag at the narrowest point that changes behaviour, not at the call site, so the arms differ
by one expression. Delete it afterwards - it is scaffolding for one experiment, not a feature
toggle, and a flag left in the tree becomes a second code path nobody tests.

## Why This Matters

An unmeasured fix is not neutral-until-proven. Both of these were *active* defects: one corrupted a
load-gating counter in the over-fetch direction and made a single-threaded field cross-thread; the
other re-created the user-facing symptom under one of the two rebalance protocols. They survived
months of CI because the invariants they broke - counter equals reality, flag equals broker state -
were observed by nothing. That is the general condition: **a fix aimed at an unobserved invariant
can only be judged by an instrument built for the purpose**, and if that instrument is worth
building after the fix, it was worth building before.

## When to Apply

- The fix's justification contains "must be", "presumably", or a symptom narrative with no
  measurement attached.
- The thing being fixed is bookkeeping - a counter, a cache, a flag - whose correctness no test
  asserts against ground truth.
- You inherit a plausible-looking change with no record of the experiment that motivated it
  (the [read-the-commits-you-inherit](read-the-commits-you-inherit-2026-08-10.md) moment: if the
  body names no measurement, assume there was none).
- A regression test flips red at the moment a fix lands: check for the inverted instrument before
  blaming the fix.

## Related

- [`docs/investigating.md`](../../investigating.md) - **owns the method for settling a cause once a
  hypothesis exists**: control arms, stated predictions, rates and conditions. This document is the
  step before it: the same experimental discipline pointed at whether the problem exists, not what
  causes it.
- [negative-results-need-an-instrument-that-could-have-said-yes.md](negative-results-need-an-instrument-that-could-have-said-yes.md) -
  the sibling about trusting instruments: there, a "no" from an instrument that could not say yes;
  here, a fix from a premise no instrument ever tested, and an instrument wired to say the opposite
  of the truth.
- [two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md](../architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md) -
  owns both worked cases in full: the probe design, the drift table, the pause-reset mechanism, and
  the reproducer's two independent reasons for observing nothing.
- [../architecture-patterns/a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md](../architecture-patterns/a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md) -
  the defect class the pause reset belonged to; its "treat a reset hook as a symptom" is this
  document's conclusion arrived at from the design side.
