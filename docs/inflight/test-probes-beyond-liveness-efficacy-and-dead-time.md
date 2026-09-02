# Two probe kinds the suite does not have: does the remedy fire, and how much of the run was dead

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-labels: concurrency -->

**Every probe here measures one thing: is the system still progressing.** `NO_PROGRESS`,
`INSTANCE_STALL`, `LAG_STAGNATION`, the drain bound - all sample a counter over time and ask whether
it moved. That question has a blind spot, and a real defect sat inside it for weeks.

## The defect that motivated this, and the two numbers that diagnosed it

A branch lost the per-poll refresh of `ConsumerManager`'s pause cache, so the cache was written only
when a poll ENDED. The control thread's back-pressure wakeup reads that cache to decide whether the
poller is asleep in a paused poll - and during every such sleep it read "not paused". The wakeup
never fired, so each pause cost a full long-poll timeout with the pipeline drained.

Throughput fell four- to tenfold. The `Performance Tests` lane went red and stayed red. **Every
liveness probe stayed green**, correctly: the system WAS progressing, in bursts, between sleeps. A
deadline-based test called it flaky for weeks.

Two measurements identified it, neither of which any probe here takes:

- **Wakeups fired against pauses seen**: 2 against 68 on the broken arm; 15 against 21 on a healthy
  one. One run, unambiguous.
- **Dead time**: 10-24 gaps over 200ms in the per-instance progress timeline, totalling 50-70% of
  wall clock. The healthy arm had **zero** gaps over 200ms.

## 1. An efficacy probe - does a remedy fire in proportion to the condition it remedies?

A wakeup exists to break a paused sleep. A retry exists to recover a failure. A timeout exists to
bound a wait. Each is a remedy with a triggering condition, and **each can silently stop firing
while everything downstream still looks alive** - which is exactly what a liveness probe cannot see,
because the system does eventually make progress the slow way.

The check: count the condition and count the remedy, and assert the ratio has not collapsed. It
needs no absolute threshold - the healthy ratio is whatever the other arm does - which is what makes
it robust where a timing bound is not.

The generalisation worth keeping: **a mechanism whose failure mode is "does not fire" needs a
counter, not a watchdog.** Nothing goes red when a wakeup is skipped; the run just takes longer.

## 2. A dead-time probe - what fraction of the run made no progress at all?

"Did it finish inside the deadline" and "was it working the whole time" are different questions, and
only the second distinguishes a slow system from one that alternates between saturated and asleep.
The broken arm above spent most of its wall clock in gaps while passing every progress check between
them.

Dead time is also **load-normalised in a way a deadline is not**: a busy machine makes everything
slower, but it does not make a healthy system idle for seconds at a time. That property is what a
wall-clock bound never had, and it is why the deadline read as flakiness -
[`../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md)
owns that argument.

## What this is NOT

- **Not the throughput gate.** That is
  [`perf-throughput-regression-gate.md`](perf-throughput-regression-gate.md), it is about comparing
  rates between trees, and it would also have caught this defect. These two are cheaper and answer
  "why", where the rate answers "whether".
- **Not the coherence check** added to `describeProgress()`, which asks whether PC's own accounts
  agree. That one has still never fired and is unarmed; treat it as unproven.
- **Not static analysis.** The defect class here is "a decision keyed on a cache refreshed after the
  blocking call it describes", which is a temporal property across a blocking boundary. ArchUnit
  matches call graphs and cannot see it, and inventing a rule that appears to would be worse than
  admitting the gap.

## Before either becomes a gate

Both must be shown FIRING on a tree that should fail before their silence is trusted anywhere - the
standing rule here, and one this session has now broken twice. The pause-cache regression is a ready
made red arm for exactly that: revert the entry refresh and both detectors should trip, loudly.
