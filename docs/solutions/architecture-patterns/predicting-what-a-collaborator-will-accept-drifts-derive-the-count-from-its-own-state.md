---
title: "Predicting what a collaborator will accept drifts from what it accepted - derive the count from its own state"
date: 2026-09-01
category: architecture-patterns
module: parallel-consumer-streams
problem_type: architecture_pattern
component: state_management
severity: high
applies_when:
  - You need a count of what a collaborator is holding, and it does not expose one in the shape you want
  - Tempted to compute "how many of this batch will it take" just before handing the batch over
  - A counter is incremented at one event and decremented at another, and the two are decided by different code
  - A gate (a pause, a throttle, an admission check) is driven by a number you maintain rather than one the authority owns
  - An objection to deriving from the authority's number is "it counts X for ever" - check whether the derivation can cancel X out
tags:
  - counter-drift
  - parallel-state
  - backpressure
  - derived-state
  - idempotence
  - kafka-streams
related_components:
  - PcTaskDispatcher
  - PartitionState
  - ProcessingShard
---

# Predicting what a collaborator will accept drifts from what it accepted

## Problem

`parallel-consumer-streams` has to answer, per partition, "how many records are we holding that no
worker has started" - Kafka Streams compares exactly that against `buffered.records.per.partition`
to decide whether to pause the consumer. Parallel Consumer does not expose that number, so the first
design maintained one: **raise it at registration by predicting what PC would take on, lower it as
records were handed out.**

The prediction was careful. It asked PC, per record, which of the batch it had not already completed
- deliberately not the delta in PC's incomplete-offset map, which is keyed differently from the
decrement and had already produced one drift bug of its own.

It was still wrong, in the direction that has no symptom. PC accepts an offset into its incomplete
set *and* hands the record to a shard, and the shard **drops** an arriving container when a live one
for that offset is already resident. That is what happens to a re-delivered offset - after a `seek`
backwards, an offset reset, corruption recovery - which is not exotic on this path. The prediction
counts it, the record is never handed out, nothing ever decrements it back, and since the count is
what pauses the partition, **that partition is paused for good**. No exception, no log line: a
topology that has simply gone quiet.

## The shape of the mistake

The counter was incremented on a decision made *here* and decremented on an event decided *there*.
Any rule the collaborator applies that the prediction does not model is permanent drift, and every
such rule is one you have to know about, in a class you do not own, for ever.

**One authority, or two things that can disagree.** The register-side prediction was the second
authority.

## Resolution

Derive instead:

```
buffered(partition) = PC's incomplete offsets for it
                    - records we handed out that PC still counts as incomplete
```

The subtrahend is incremented where records are taken from the collaborator and decremented where
their **success** is folded back - success being the only outcome that removes an offset from the
incomplete set. Both mutations sit on one thread and on two events this code fully owns, so there is
nothing left to predict.

Three defects go with it rather than being fixed one at a time:

- **The re-delivered offset.** The authority's map is keyed by offset and its insert is idempotent, so
  a re-delivery does not move the number at all.
- **The refused record.** A record the collaborator declines never enters its incomplete set, so it
  is excluded without anyone deciding to exclude it.
- **The unit mismatch.** Both sides of the subtraction are now counted in the same events.

## The objection that had blocked it, and why it does not survive

Deriving was rejected earlier for a real reason: with retries disabled, a **failed** record stays in
PC's incomplete set for ever, so a count taken from it would pause that partition permanently.

The derivation answers it rather than ignoring it. A failed record is never subtracted back out of
the handed-out tally - success is the only decrement - so the failure cancels itself out of the
subtraction and contributes zero. The objection was correct about `incompleteOffsets` used *raw* and
did not carry over to `incompleteOffsets` used as a *minuend*.

Worth stating on its own: **an objection to a derivation is usually an objection to one particular
derivation.** Check whether a different term makes it vanish before accepting it as a design
constraint - this one had stood since the original implementation.

## Keep a detector, and pick the direction that has no other symptom

Drift **low** (the count reads high) pins a partition paused, and a test that drives records through
and asserts the count returns to exactly zero catches it. Drift **high** (the count reads low or
negative) stops the pause firing at all - the memory bound silently absent - and nothing observes it,
so the negative result is clamped to zero, **counted and logged once**, and asserted zero by the
tests. A bare clamp with no counter is what hid a conditional-decrement bug in this project's own
intake gate; see
[`counter-clamp-hid-a-conditional-decrement-bug-2026-08-21.md`](../logic-errors/counter-clamp-hid-a-conditional-decrement-bug-2026-08-21.md).

## The test that would have caught the original

Register a batch of same-key records so the collaborator keeps them all resident, register **the same
batch again**, and assert the count did not move - then drain and assert it returns to zero. The
second half is the one that matters: a permanent residue cannot come back to zero, and every other
assertion in the suite was satisfied by a count that never did.

## See also

- [`a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md`](a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md)
- [`a-query-must-never-mutate-derive-thread-safety-from-callers.md`](a-query-must-never-mutate-derive-thread-safety-from-callers.md) -
  the same class, one step earlier: the safest publication is the one you do not have to remember to perform
