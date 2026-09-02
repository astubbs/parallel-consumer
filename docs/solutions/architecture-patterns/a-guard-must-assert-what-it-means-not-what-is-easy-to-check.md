---
title: "A guard must assert what it means, not what is easy to check"
date: 2026-08-18
category: architecture-patterns
module: parallel-consumer-core/internal
problem_type: architecture_pattern
component: background_job
severity: high
applies_when:
  - Writing a runtime guard, assertion, or confinement check for an invariant ("only one user at a time", "never after close", "only on this thread")
  - The invariant is about a lifetime or usage ("still in use") but the available observable is an identity (a thread, a connection, a session object)
  - A guard is firing on operations you believe are legal, and the choice is between relaxing it and restructuring it
  - Declaring a guard-related bug fixed because the violation count dropped to zero
  - A guard's failure is caught and logged rather than propagated
related_components:
  - ThreadConfinedConsumer
  - ConsumerOwnership
  - BrokerPollSystem
tags:
  - guard-predicate
  - proxy-observable
  - thread-confinement
  - assertion-design
  - swallowed-exception
  - zero-residue
  - issue-857
---

# A guard must assert what it means, not what is easy to check

## Context

A thread-confinement guard added to the Kafka consumer meant "no one else is still using this". What
it *checked* was "the calling thread is the thread object that claimed ownership" - the observable
that was one field-read away. Those are different claims, and the gap between them is exactly a
pooled thread: it outlives the task that claimed ownership, so the claim outlived the usage. The
guard rejected the close-time handoff from the poll thread to the control thread - a **legal,
provably sequential** transfer, since the close path first waits for the poll loop to finish - 88
times in one CI run, and because the rejection was caught and downgraded to a warning, the consumer
was silently never closed and sixteen integration tests failed on the fallout.

The mechanism, the measurements, and the three-state ownership fix are owned by
[two-threads-one-consumer](two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md); this
document is the general rule the incident instantiates.

## Guidance

**Write the guard's predicate from the sentence of the invariant, then compare.** Say the invariant
out loud - "reject use while someone else is still using this" - and read the predicate against it
word by word. Here, "still using" is a *lifetime*, and the predicate contained no lifetime at all,
only an identity comparison. A predicate missing a word of the sentence is guarding a different
sentence.

- **An identity is a proxy for a usage; proxies drift where lifetimes differ.** Thread identity
  stood in for "in use" and diverged as soon as a thread outlived its task. The same shape appears
  wherever the checkable handle outlives or predates the thing it stands for - and the divergence
  is systematic, not random: here roughly three quarters of violations reported a "live" owner,
  because a pooled thread is *usually* still alive after its task ends. A proxy does not merely
  misfire occasionally; it misfires in a pattern that looks like evidence of a real bug.
  ([a-type-gate-is-a-claim-about-a-hierarchy-you-did-not-write](a-type-gate-is-a-claim-about-a-hierarchy-you-did-not-write.md)
  is the same lesson for `instanceof` as a proxy for capability.)
- **When the invariant needs a word the predicate lacks, add state, do not relax the check.** The
  fix was not accepting more callers; it was giving ownership the missing lifetime - claimed,
  released-by-the-user-as-its-last-act, re-claimed - so the predicate could finally say "in use"
  directly. A guard that needs relaxing to stop misfiring usually needs *restructuring*: the
  misfires are the predicate telling you which word it is missing.
- **A guard whose failure is swallowed converts a loud wrong into a silent worse.** Every one of
  the 88 rejections was caught and logged, so the guarded operation - closing the consumer -
  simply did not happen, and the visible failure moved downstream to where nothing pointed back at
  the guard. If a violation is survivable enough to log-and-continue, the *skipped operation* is
  now the failure mode, and it needs its own handling; if it is not, the guard must propagate.
- **After fixing a guard, the expected result is a small non-zero residue - zero is suspicious.**
  The close path here has a genuine failure branch (a poll loop that never finished shutting down,
  closed from another thread anyway) that the guard *should* still catch. A fix that takes the
  violation count from 88 to exactly 0 is therefore ambiguous between "the false positives are
  gone" and "the guard is disarmed" - and only the negative control separates them: break the
  invariant deliberately and watch the guard fire.
  [`docs/investigating.md`](../../investigating.md) owns that rule ("an assertion nobody has seen
  fail is decoration"); what this incident adds is the prediction discipline around it - state
  *which* violations are false positives and which are real before fixing, so the residue you
  expect is written down and a zero can be interrogated instead of celebrated.

## Why This Matters

A guard is a claim generator: every firing asserts "the invariant was violated", and everything
downstream - diagnosis, fixes, the decision that a subsystem is racy - inherits that claim's
authority. A proxy predicate mass-produces *false* claims with the full authority of a real guard,
and the systematic pattern of its misfires reads as corroborating evidence. Here the 88 identical
reports looked like a grave concurrency bug and were nearly treated as one; the actual defect was
the guard's vocabulary. The cost is double: the phantom bug absorbs investigation, and when the
firings are finally dismissed as noise, the guard's *real* catches are dismissed with them.

## When to Apply

- At design time, for any new assertion or confinement wrapper: does the predicate contain every
  word of the invariant's sentence, or a checkable stand-in for one of them?
- At diagnosis time, when a guard fires implausibly often or in a suspiciously uniform pattern:
  before investigating the "bug", re-derive what the predicate actually tests.
- At fix time: decide relax versus restructure by asking which word is missing, name the expected
  residue in the commit, and prove the guard still fires by negative control.

## Related

- [two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md](two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md) -
  owns the worked case: the 88 firings, the sequential-handoff proof, the three-state ownership
  model, and why release must happen inside the poll task.
- [a-type-gate-is-a-claim-about-a-hierarchy-you-did-not-write.md](a-type-gate-is-a-claim-about-a-hierarchy-you-did-not-write.md) -
  sibling: a type test as a proxy for a capability, drifting where the hierarchy grows.
- [a-query-must-never-mutate-derive-thread-safety-from-callers.md](a-query-must-never-mutate-derive-thread-safety-from-callers.md) -
  sibling: derive the contract a guard enforces from real callers, not from a javadoc's model of
  them - the step before this document's, since a guard built on a wrong contract fails the same
  way however good its predicate.
- [`docs/investigating.md`](../../investigating.md) - **owns the negative-control rule** this
  document's zero-residue corollary depends on.
