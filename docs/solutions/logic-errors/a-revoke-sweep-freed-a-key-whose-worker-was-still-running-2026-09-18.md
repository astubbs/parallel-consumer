---
title: "A revoke sweep freed a key whose worker was still running, so a re-delivery ran the same key on a second thread (astubbs#178, confluentinc#843)"
date: 2026-09-18
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: internal / work-state
symptoms:
  - "Under KEY or PARTITION ordering, one key's user function is executing on two `pc-pool-1-thread-*` threads at once in one instance, shortly after a partition was revoked and handed straight back to it"
  - "Nothing PC counts goes wrong: no duplicate commit, no lost record, the old result is dropped at DEBUG as `Dropping work from revoked partition` - only the user's own invariant (a counter, an idempotency key, a per-key state machine) breaks"
  - "The chaos ledger reported nothing, because its overlap check was scoped to one assignment epoch and the two executions were in different epochs"
root_cause: the_stale_sweeps_evict_on_staleness_alone_and_the_ordering_restriction_consults_only_shard_residency
resolution_type: code_fix
severity: high
tags:
  - rebalance
  - work-state
  - stale-epoch
  - shard
  - key-ordering
  - concurrency
  - logic-error
---

# A revoke sweep freed a key whose worker was still running

## Problem

PC promises that records of one key are executed one at a time, in order, within one consumer. In the
ordered modes that promise is kept by residency: a taken container stays in its shard's `workMap`, and
`ProcessingShard#getWorkIfAvailable` stops at an in-flight head. Nothing else serialises the key.

A revoke does not interrupt a running worker (deliberately - draining inside the callback is the
confluentinc#857 recipe). The revoke and epoch-change sweeps (`removeWorkForRevokedRecord`,
`removeStaleWorkContainersFromShard`) evict a container on **staleness alone** and never ask whether it
is in flight. So when the partition comes straight back to the same instance, its uncommitted offset
is re-delivered into a shard that no longer holds the running container - under KEY ordering into a
brand-new shard, because the emptied one was garbage-collected - and the fresh container is takeable.
One key, two threads. The `Replacing stale entry` branch of `addWorkContainer` reaches the same state
by a second door.

PC's own bookkeeping stays right: `WorkManager#handleFutureResult` drops the old result as stale. That
is why this was invisible - only the user's ordering invariant breaks, and only the user can see it.

## The ruling that unblocked it

The inflight note that triaged this had parked it on a maintainer call: is an undrained old-epoch
delivery running beside the same key's new-epoch delivery a violation, or legitimate at-least-once?
The ruling (2026-09-17): **it is a violation.** The README promises strong ordering by key, a user
cannot tell an epoch boundary from any other moment, and the same instance is in a position to wait.

## Reproduction

`KeyOrderAcrossRebalanceTest` - single-threaded, no broker, no seed. The callbacks and the scan run
inline; "still in flight" is a container whose claim was won and never returned. Through the real
registration path (`wm.registerWork`) so the KEY-mode shard collection is in play, in both ordered
modes, plus the displacement route reached white-box with `plantResident`. Red on the unfixed tree in
five arms, green with the fix, red again with only the two engine files reverted (the mutation check).

## Fix

`ProcessingShard` remembers containers that left it while still in flight (`flightsOwed`, an identity
set - `WorkContainer` equality is identity, so it means "these containers", never "these coordinates").
`retire` is the one exit path every departure route shares, so it is recorded there and the three
routes are covered without any of them having to remember. An ordered scan settles the flights whose
workers have returned and takes nothing while any is still out; `isEmpty` derives from the same
question, so a shard owed a flight survives `removeShardIfEmpty` and the scan collects it once the
debt is paid. UNORDERED records nothing and pays nothing.

## Rejected alternatives

- **Leave in-flight containers resident and mark them stale.** The map is keyed by offset, so the
  re-delivered record at that offset either displaces the resident (the second door, same bug) or is
  dropped (confluentinc#909's loss-and-wedge). And the scan's last-resort stale sweep would evict it
  anyway. Every variant needed a side structure plus changes at three sites; the fix needs the side
  structure and one.
- **Drain in-flight work on revoke.** Spends the poll-interval budget inside `poll()`, and is the
  deadlock family the sweeps were made shard-only to escape. Not chosen, and not to be re-proposed.

## The detector

`KeyOrderLedger`'s overlap check is now scoped to incarnation, partition and key - **not** epoch - so
the same shape under real churn is `LEDGER_KEY_CONCURRENCY`, with the sample naming both windows. The
bound its javadoc once called "the whole job" is zero within an incarnation, because the engine now
makes it zero; across incarnations and instances the straggler is at-least-once and is not judged.
The order check keeps its epoch scope, because a revoke legitimately re-runs lower offsets.

## What this does not settle

The original report (confluentinc#843) had no rebalance. This is the one route in the engine that
puts one key on two threads in one instance, and it needs one. The no-rebalance case stays with
`docs/inflight/test-no-disturbance-duplicate-scenario.md`.
