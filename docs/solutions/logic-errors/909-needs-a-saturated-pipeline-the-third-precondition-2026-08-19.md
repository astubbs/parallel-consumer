---
title: "confluentinc#909 needs a SATURATED pipeline - the third precondition, and why random chaos could never find it"
date: 2026-08-19
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: internal / work-state
symptoms:
  - "A race that is provably reachable by inspection, yet a chaos suite asserting the exact violated invariant stays green on defective code"
  - "A hand-built reproduction of a known race passes, for reasons unrelated to the fix"
tags:
  - rebalance
  - epoch-fencing
  - reproduction
  - chaos
---

Companion to
[`909-registration-race-reachability-proof-2026-08-18.md`](909-registration-race-reachability-proof-2026-08-18.md),
which proved the race **reachable** but left open why nothing reproduced it. That record stands as
written; this one adds the missing precondition, found by building a reproduction that works.

## The third precondition

The loss needs **three** coincidences, not the two previously recorded:

1. a rebalance lands inside `maybeRegisterNewPollBatchAsWork`'s insert loop;
2. the partition returns to the same consumer, so the uncommitted tail re-delivers and collides;
3. **no take-scan of the shard happens between the stale inserts and the fresh registration.**

Three is the one nobody had named, and it is decisive. `ProcessingShard.getWorkIfAvailable`
**lazily evicts stale containers as it scans** - the not-takeable branch does an `iterator.remove()`
(anchor: `there are still stale work container`). So an ordinary take-scan *heals* the collision
before the fresh arrival can hit it. The stale resident only survives long enough to eat a fresh
record if the worker pipeline is **at target** - control-loop `delta <= 0`, so `ShardManager`'s scan
loop never runs at all.

## What it explains

- **Why the Chaos Pain Suite is green on master with the defect present.** Not an unlucky seed. The
  suite churns hard, and churn *is* the healing mechanism: every take-scan clears the residents that
  the race just planted. Reproduction by random chaos is not merely improbable, it is fighting
  itself.
- **Why the first hand-built attempt passed.** It reproduced preconditions 1 and 2 faithfully and
  still went green, because its pipeline was idle. That was caught by reading a debug-logged autopsy
  run, not by assuming the test was right - and it is the reason this record exists.
- **The production risk profile, which is narrower than "any rebalance".** This bites a **busy**
  consumer, not an idle one. A fleet under load at the moment of a rebalance is the exposed case.

## The reproduction

`RegistrationRaceStaleResidentIT` drives all three deliberately: a gated user function saturates the
worker pool so `delta <= 0`, a fixed message-buffer size pins the load factor so the target cannot
step mid-experiment, a second group member forces an eager revoke/re-assign, and the insert is
parked mid-loop until re-delivery has fully registered. Deterministic and event-gated - no sleeps,
no seed.

It discriminates: with `addWorkContainer`'s staleness check neutralised it fails naming the exact
lost records (25 of them, matching 25 `already exists in shard queue, dropping record` lines at the
defect site); with the fix it passes in ~15s.

## The method note worth keeping

The chaos scenario was run, once, at a fixed seed against the defect - and it **passed**. The
instinct then is to reroll seeds. That instinct was wrong, and rerolling would have burned hours
proving nothing: the scenario cannot produce the interleaving *by construction*, because its own
churn heals it. **A soak that does not reproduce is telling you something about the mechanism -
read it as evidence, not as bad luck.**
