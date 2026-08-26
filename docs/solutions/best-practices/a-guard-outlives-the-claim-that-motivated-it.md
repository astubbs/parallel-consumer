---
title: "A guard outlives the claim that motivated it: record the refutation at the guard, or the strong claim grows back"
date: 2026-08-26
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: error_handling
severity: medium
applies_when:
  - "A defensive guard was added because of a failure mode nobody has reproduced"
  - "An ablation refuted the motivating claim but the guard is being kept anyway"
  - "A PR description, changelog entry or javadoc is about to explain why a guard exists"
  - "A defect class is being swept across sibling implementations that look alike"
related_components:
  - ExternalEngine
  - VertxParallelEoSStreamProcessor
  - WorkContainer
  - ThrowableUtils
tags:
  - ablation
  - negative-result
  - defence-in-depth
  - defect-class-sweep
  - error-handling
  - claim-drift
status: "In flight as of 2026-08-26. Extracted from the astubbs/parallel-consumer#267 handoff note before that note is deleted at merge; the PR is open."
related_prs:
  - "astubbs/parallel-consumer#267 - the PR whose guards this describes (open)"
related:
  - "ablate-your-own-change-not-only-the-baseline.md - which arms to run at all; this doc is about what to do with the result when an arm refutes your own motivation"
  - "../../inflight/core-blanket-safe-logging.md - the costed-and-declined decision that depends on knowing which sites are genuinely at risk"
  - "../../inflight/core-unmailboxed-container-recovery.md - the open question these guards do not answer"
---

# A guard outlives the claim that motivated it

> Extracted from `docs/inflight/pr-267-handoff.md`, which is deleted when astubbs#267 merges.

## Context

astubbs#267 started as a `ConcurrentModificationException` and became a defect class: **PC runs
user-supplied code in places where its own bookkeeping cannot survive a throw.** Several of the
changes it made are per-container guards - `try`/`catch` around each of `onUserFunctionFailure` and
`addToMailbox`, inside the loop that walks a batch, in core and in each engine.

The sentence that motivated them was: *if a throw escapes here, the remaining records stay in flight
forever.* It is a good sentence. It explains the guard, it names a consequence a user would care
about, and it is the kind of thing that writes itself into a PR description.

**It was ablated three ways across two engines, and the records came back every time.** Something
recovers an un-mailboxed container; what, nobody has established
([`core-unmailboxed-container-recovery.md`](../../inflight/core-unmailboxed-container-recovery.md)
owns that question). So the premise behind the guards is unproven, and the guards were kept anyway,
as defence in depth.

## Guidance

**1. When an ablation refutes your motivation but you keep the change, the refutation is now part of
the change.** It belongs next to the code, in the javadoc of the test that exercises it - not only in
the branch's commit messages, and certainly not only in a reviewer's memory. A guard with no recorded
refutation is indistinguishable from a guard whose claim held, and the next person to explain it will
reach for the strongest available explanation, because that is the one that reads best.

**2. The strong claim grows back on its own, so say plainly that it must not be restored.** Every
later artefact - a rewritten PR body, a changelog entry, a doc pass, a summary for a reviewer - is an
opportunity for "defence in depth against a failure we could not reproduce" to be tightened into
"fixes records stuck in flight forever". Nothing goes red when that happens, and the sentence is more
persuasive than the true one, which is why it wins. The handoff note this was extracted from said it
as an instruction - *do not "restore" a stronger claim* - and that imperative form is the part worth
copying.

**3. Keep the proven and the unproven separately labelled, at the same altitude.** In this case what
*is* measured is: the `runUserFunction` reorder (21s against 2.3s), and the `retryDelayProvider`
return-value validation. What is not is the in-flight-forever premise behind the loop guards. Both
live in the same PR and read alike from outside, so a summary that does not separate them silently
lends the measured result's credibility to the unmeasured one.

**4. Deciding to keep an unmotivated guard is a real decision, so cost it rather than defaulting.**
[`core-blanket-safe-logging.md`](../../inflight/core-blanket-safe-logging.md) is the worked example
from the same PR going the other way: a wider version of the same defensiveness was costed and
declined, on the finding that the exposure is a handful of render sites rather than every
raw-throwable log call. Counting the sites is what made the decision available. A guard kept without
that count is a guard kept by inertia.

## Sweeping a defect class: by shape, not by module

The same PR is also the evidence for how the sweep goes wrong, and it is worth stating separately
because it is not about evidence at all - it is about search.

Round after round of review found the same defect class at a **sibling site the previous round had
missed**, and vert.x was missed repeatedly. The reason is specific and reusable: **sweeping by module
keeps finding the implementations that look alike.** Reactor and Mutiny handle a batch per failure and
so answer every batch-shaped grep. Vert.x handles one container per failure, so it falls out of that
grep while being the same defect.

- **Enumerate by the shape of the hazard, not by the list of places you expect it.** Here the shape is
  *user code running before bookkeeping that must not be skipped*. That description finds the vert.x
  handler; "the engines' `onError` methods" does not.
- **A sweep that found something is evidence the sweep is incomplete, not that it is finished.** Each
  round that turned up a new sibling should have lowered confidence in the round before it. Assume
  incompleteness until a round finds nothing new, and say so where the next reader will look.
- **Where the sweep converged, remove the duplication so the next fix cannot be applied to only one
  copy.** `ExternalEngine.onAsyncFailure` exists because Reactor's and Mutiny's versions differed only
  by a log string, and keeping two copies is how the ordering and the guard drifted apart in the first
  place.
