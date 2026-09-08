---
title: "largeNumberOfInstances' residual failure is the group coordinator's join phase held open by churn, not a PC defect"
date: 2026-09-05
category: test-flakiness
module: parallel-consumer-core
problem_type: measurement_not_a_defect
component: rebalance
severity: low
root_cause: aggressive_membership_churn_keeps_the_consumer_group_join_phase_open_long_enough_that_the_stall_detector_fires_before_it_completes_which_is_a_property_of_the_protocol_and_hardware_timing_not_of_pc
resolution_type: measured_and_classified_not_fixed_the_profile_moved_from_a_gating_correctness_check_to_a_scheduled_capacity_measurement
status: "EXPLAINED 2026-09-05, ACTED ON 2026-09-07 (moved off the required Performance Tests gate onto @Tag(\"capacity\")). Nothing here was a PC bug to fix; the deterministic correctness twin scriptedChurnRoundsCompleteWithoutStall keeps the same code paths gated."
applies_when:
  - "A chaos/capacity test churns group membership aggressively and occasionally shows the whole assignment frozen (FLAT), not merely slow"
  - "A stall detector times out at a fixed window and you are tempted to read every hit as the same cause without measuring the coordinator's own request latencies"
  - "You are about to fix a stall by adding another poll, another retry, or a bigger timeout before naming which call is actually blocked"
  - "A candidate mechanism looks right by code inspection (a lock on the hot path, a known defect class) but has never actually been measured against this failure"
symptoms:
  - "Multiple instances sit in state=CLOSING / closePending=true for 20-25s while two or three survivors keep polling and see no progress (the ambient probe's ZOMBIE_MEMBER / REBALANCE_BLOCKED)"
  - "Thread dump of a stuck instance's poll thread: ClassicKafkaConsumer.close -> ConsumerCoordinator.close -> AbstractCoordinator.close -> ConsumerNetworkClient.awaitPendingRequests, parked in EPoll.wait"
  - "Coordinator loggers show every individual LeaveGroup/JoinGroup answered within one heartbeat interval (~2.7-3.0s) even in a failing run - no single request is ever slow"
  - "Rate is hardware-dependent: reproduces at roughly 1 in 15 on a self-hosted Linux runner under load, 0 in 22 on an idle M2 desktop, at unchanged test parameters"
tags:
  - rebalance
  - consumer-group-protocol
  - chaos-monkey
  - control-arm
  - capacity-measurement
  - refuted-hypothesis
---

`MultiInstanceRebalanceTest.largeNumberOfInstances` (astubbs#857's closest in-repo reproduction of
the "every other run fails" report) had an unexplained residual failure rate: the class javadoc
asserted the cause was "the Kafka consumer group protocol under extreme membership churn, not a PC
bug", but nobody had measured it - the claim and the test's own name disagreed for months. This
write-up is the mechanism, once it was actually pinned down, and - as importantly - the two
plausible-looking candidates that were tested and refuted along the way, so nobody re-chases them.

## The mechanism

A chaos monkey restarts up to 6 of 11 secondary instances at a time, on a 0-500ms cadence, against a
broker under contention. The chain, every link independently observed:

1. **A member closing calls `pc.close()` -> `closeDontDrainFirst()` -> `transitionToClosing()`.**
   `BrokerPollSystem.handlePoll()` is guarded on `runState == RUNNING || DRAINING`, so the instance
   **stops calling `consumer.poll()` the instant it enters `CLOSING` - while its `KafkaConsumer` is
   still an open member of the group.** The consumer itself is closed later, inside `doClose()`.
2. A member that does not poll cannot send JoinGroup, so the coordinator dwells in
   `PreparingRebalance` waiting on it - the recorded `ZOMBIE_MEMBER`/`REBALANCE_BLOCKED` signature.
3. The close cannot finish either: `doClose() -> consumerManager.close() -> consumer.close() ->
   AbstractCoordinator.close() -> awaitPendingRequests()` waits for that member's own **LeaveGroup**
   response - and a LeaveGroup sent while the member's own JoinGroup is still pending is not answered
   until the join phase completes (measured deterministically at ~2.7s in a healthy group).
4. Under continuous arrivals and mid-join departures, the join phase itself can stay open far longer
   than one heartbeat - 17 seconds was observed directly (generations 18-21 each completed within a
   heartbeat; the one opened mid-storm did not). Kafka's server-side reason for holding a join phase
   open that long is outside what client-side evidence can answer; that it does is not in doubt.
5. While the phase is open, **`consumer.poll()` returns no records to ANY member**, including
   survivors that never toggled - that is the whole-fleet `FLAT` count the 11-round detector fires
   on at ~12s. Each closing member burns its full close budget waiting on the phase; `waitForClose`
   (a shorter budget) gives up first with a `TimeoutException` while the instance is still `CLOSING`.

**Not a PC defect**: every candidate PC-side cause on the critical path was checked and cleared
(`ConsumerManager.commitAsync` never touches `pendingRequests`; every closer's frame is inside
Kafka's own `close()`, after LeaveGroup was already sent; every survivor's frame is an ordinary
`pollForFetches`). The mechanism is real and reproducible, but it lives in how long the *group
protocol* keeps a join phase open under this churn rate, on this hardware - not in PC's code.

## Two candidates that looked right and were refuted by measurement, not by argument

**Candidate 1: the fair `RetryQueue` lock on the rebalance-callback path (2026-09-03/04).**
`RetryQueue.remove()` takes an unbounded, fair write lock reachable from rebalance callbacks on the
broker-poll thread - exactly the kind of hot-path lock that could make a callback (and so JoinGroup)
wait behind record processing. astubbs/parallel-consumer#431 fixes precisely this. **Prediction,
stated before the run**: a control tree (without astubbs#431) and a treatment tree (with astubbs#431, nothing
else) run sequentially, 30 iterations each, on the same idle self-hosted box. Result: **control
2/30, treatment 1/30** - no difference worth a claim, and the treatment failure carried the
control's signature line for line. *The candidate was refuted; astubbs#431 is a real fix for a different
defect, but it does not touch this one.* (A measurement trap nearly inverted this result: the
runner's `/tmp` persists across dispatches, so the treatment artifact's tally briefly looked like
"3 failures in 60" - all 60 rows, both arms - until the per-run `ref=` column in the tally script
separated them.)

*Citation note, 2026-09-08: astubbs#431 has since closed as superseded by astubbs#481, which fixes the same lock defect by a different design - the poll thread no longer touches the retry queue at all, rather than declining its lock - so `RetryQueueRebalancePathTest` now exists on master under that PR. The refutation above stands unchanged: a second design for the same defect does not make the mechanism more likely, and nothing here should be re-run against it expecting a different answer. Both designs: [`../runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md`](../runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md).*

**Candidate 2: "poll once more before closing" (2026-09-05).** Once the mechanism above was named,
the obvious fix was a discharge poll in `doClose()` plus a `ConsumerManager` one-attempt allowance to
let it fire. Measured result: **2/60, the same rate as every pre-fix tree** (2/30 control, 1/30 with
astubbs#431, 3/60 and 3/60 on two diagnostics-only trees - six trees, one rate, ~5%). The signature was
unchanged too: failing runs still showed instances parked in `CLOSING` with the same
`ZOMBIE_MEMBER`/`REBALANCE_BLOCKED` read. *Overturned*: one poll is not enough to discharge what the
coordinator is waiting for - a rebalance is JoinGroup and SyncGroup as separate exchanges, and a
single low-latency poll can complete neither reliably while ten other members are churning. What the
attempt correctly kept: the diagnosis that closing members were stuck in `awaitPendingRequests`.

A **deterministic reproducer**, `ClosingMemberRebalanceIT`, closes members forced to the exact
`PreparingRebalance` window instead of catching it by chance, and confirms the shape further: closing
one member, or three at once, mid-rebalance is handled cleanly by both Kafka and PC (LeaveGroup
answered in ~10ms, survivors resynced in seconds) - **when the closing member's own JoinGroup is
already answered.** Only when a member is closed *while its own first JoinGroup is still pending*
does its LeaveGroup wait for the full join phase (~2.7s, measured directly with coordinator loggers
raised) - the one state the discharge-poll fix could never help, because the wait is for a response
the coordinator will not send regardless of how many times the closer polls.

## What was not chased further, and why

Reproducing the profile's actual ~25-second freeze (rather than the ~2.7s single-event cost above)
needs the storm itself at reduced scale, with coordinator loggers raised - a different experiment
from a single-event arm, and untested. The leading candidate for what extends one join phase from
3s to 25s under continuous churn is a member closed while its *own* first JoinGroup is unanswered (a
just-restarted instance, where `generation.hasMemberId()` is false so `maybeLeaveGroup` sends
nothing and the coordinator waits on a member that has already gone) - stated as the next arm, not a
finding, and nobody has run it. Since the residual is the protocol's, not PC's, the decision made
2026-09-07 was to measure it as a capacity number rather than keep chasing a PC-side fix for a
defect that measurement had already ruled out.

## The decision this measurement enabled

Because the residual failure carries no information a PC change can move, it does not belong in a
required gating lane. `docs/inflight/test-largenumberofinstances-cannot-gate-a-merge.md` is the
decision record: the three capacity profiles (`largeNumberOfInstances`,
`cooperativeStickyRebalanceShouldNotStall`, `gentleChaosRebalance`) carry `@Tag("capacity")`, which
`bin/performance-test.sh` (the required `Performance Tests` check) excludes, while the scheduled
`experiments` GitHub Actions workflow still runs them on a weekly cadence and tallies the rate.
`MultiInstanceRebalanceTest#scriptedChurnRoundsCompleteWithoutStall`, the deterministic correctness
twin, keeps the same rebalance/close code paths gated - it carries no capacity tag and is unaffected
by any of the above.

## What generalises

- **A stall detector firing on "no progress" does not mean anything is slow.** Every individual
  request in every failing run answered within one heartbeat interval; what varied was whether the
  group as a whole could complete a join phase before the detector's window ran out. Measure the
  protocol's own request latencies before reading a timeout as "something hung".
- **A candidate mechanism that fits the code is not evidence until it is measured against the actual
  failure**, with a stated prediction and a control arm. Two different fixes (the retry-queue lock,
  the discharge poll) both looked mechanistically plausible and both measured at the unfixed rate.
- **A shared, persistent `/tmp` on a self-hosted runner can silently merge two experiment arms.**
  Tag every row with the tree that produced it (the `ref=` column) whenever two dispatches could
  land in the same output directory.
- **"Close" and "leave the group" are different events, and conflating them hides where a wait
  actually is.** `DRAINING` already kept polling through a rebalance (astubbs/parallel-consumer#80);
  `CLOSING` did not, and that asymmetry - not any lock or retry - is the whole first half of the
  chain above.

## Related

- [`docs/inflight/test-largenumberofinstances-cannot-gate-a-merge.md`](../../inflight/test-largenumberofinstances-cannot-gate-a-merge.md) -
  the decision this measurement fed: move the capacity profiles to a scheduled, non-gating lane.
- [`docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md`](pc-silent-stall-under-contention-2026-07-29.md) -
  astubbs/parallel-consumer#80, the earlier fix for `DRAINING` not polling through a rebalance; the
  same invariant `CLOSING` was still missing.
- `parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/ClosingMemberRebalanceIT.java` -
  the deterministic reproducer built during this investigation.
- `bin/exp-measure-large-instances-failure-rate.sh`, `bin/exp-sweep-large-instances-scale.sh` - the
  experiment runners that produced the rates cited above; `docs/testing.md`'s "Experiment runners"
  table is the discovery path to both.
