---
title: "A saturation test's second stage was never fetched - its own saturation had closed the record-intake gate"
date: 2026-09-07
category: test-flakiness
module: parallel-consumer-core
problem_type: test_design_bug
component: testing
severity: medium
root_cause: stage_one_produced_more_records_than_the_out_for_processing_target_so_the_permanent_surplus_kept_isSufficientlyLoaded_true_and_the_partition_paused_for_back_pressure_with_no_path_to_resume
resolution_type: test_fix_derives_the_stage_size_from_the_buffer_size_and_asserts_the_intake_gate_before_the_second_stage
status: "SOLVED - diagnosed by control arm, fixed, and the fixed test re-proved red against the defect it reproduces. Quarantine entry removed. Nothing enforces the rule beyond this write-up and the test's own assertion."
applies_when:
  - "A test saturates PC's worker pipeline by gating the user function, then expects MORE records to arrive from the broker"
  - "An integration test times out on a SETUP step rather than on the assertion it exists to make, always at the deadline and never short of it"
  - "A test passes locally and on most CI runs but fails at a branch-independent rate, including on branches that compile no Java"
  - "You are about to conclude a mid-test await is 'slow under CI load' without checking whether the thing it waits for can happen at all"
symptoms:
  - "assertion 'control thread must reach the mid-loop pause point (offset 25)' with awaitPausePoint(...) expected to be true"
  - "Failing runs sit at the guard's full 30s budget (30.7-30.9s); passing runs of the same test take 14-19s"
  - "The ambient probe reports clean - no rebalance dwell, no lag stagnation, no frozen partitions"
  - "Recorded failures on unrelated branches minutes apart, including documentation-only heads, with sibling branches green in the same hour"
tags:
  - back-pressure
  - saturation
  - reproduction
  - quarantine
  - control-arm
---

`RegistrationRaceStaleResidentIT` is the deterministic broker-level reproduction of the
confluentinc#909 registration race. For about a fortnight it failed across CI at a rate the ledger
put near one run in two on one shard, always on its own **setup guard** and never on the
confluentinc#909 assertion it exists to make - so every failure was evidence about nothing.
astubbs#440 quarantined it on a sighting ledger, correctly, because rule 1 of
[`docs/quarantined-tests.md`](../../quarantined-tests.md) accepts a ledger in place of a mechanism.
This is the mechanism.

## The mechanism

The test stages itself in two produces. Stage 1 fills the worker pipeline and parks every worker
behind a closed latch, which is precondition 3 of the reproduction (`delta <= 0`, so no take-scan
evicts the stale residents). Stage 2 then arrives and its registration is parked mid-loop at
offset 25.

**Saturation is achieved by never completing a record, and that is what makes the surplus
permanent.** `WorkManager.isSufficientlyLoaded()` gates record intake on

    recordsInShards > targetAmountOfRecordsInFlight * loadingFactor

and `RecordPopulation` counts a record as in-shards from the moment its container enters a shard's
map until it leaves - **being out at a worker does not remove it**. With `maxConcurrency=4` and
`messageBufferSize=8` the threshold is 8. Stage 1 produced **12** records, described in the code as
"just enough to fill the out-for-processing target (8) with a margin". Eight went out for
processing; the other four sat queued in a shard. Nothing ever retired any of them, so
records-in-shards was 12 for the rest of the run, `isSufficientlyLoaded()` was permanently true, and
`BrokerPollSystem.managePauseOfSubscription()` paused the data partition for back pressure.

**Nothing could resume it.** Both resume paths - `BrokerPollSystem.resumeIfPaused` and
`AbstractParallelEoSStreamProcessor.maybeWakeupPoller` - are conditioned on
`!isSufficientlyLoaded()`. So the stage-2 records, offset 25 among them, were never fetched, the
control thread never entered the insert that parks it, and the guard waited out its full 30 seconds.

**Why it passed at all**, which is the part that made it look like load sensitivity: pausing takes
effect only at the *top* of a poll iteration, and `BrokerPollSystem`'s long poll is 2000ms. A
`consumer.poll()` already in flight when the gate closed went on delivering for up to two seconds
afterwards. Winning that race - the poll thread starting its poll before the control thread finished
registering stage 1 - delivered the stage-2 records anyway. Losing it produced the mute 30s timeout.
The debug timeline of a winning run, to the millisecond:

    17:44.223 [pc-broker-poll] isSufficientlyLoaded=false (inShards=0)
    17:44.223 [pc-broker-poll] Subscriptions are paused: false        <- poll entered here, unpaused
    17:44.229 [pc-control]     isSufficientlyLoaded=true (inShards=12) <- gate closes
    17:44.345 [test]           stage 2 produced
    17:44.359 [pc-broker-poll] Subscriptions are paused: true          <- pause applied, too late
    17:44.359 [pc-control]     Pausing control thread ... offset 25    <- the in-flight poll delivered them

## The control arms, and the one that was refuted first

The prediction written before any arm was run was that forcing `isSufficientlyLoaded()` true before
producing stage 2 would make the failure deterministic. **It did not: 3 of 3 passed.** The arm was
wrong, not the hypothesis, and the refutation is what located the real window - *the intake gate
closing is not the same event as the partition being paused*, and only the second matters. Correcting
the arm to wait on `BrokerPollSystem.isSubscriptionsPausedForBackPressure()` produced the CI failure
on demand.

| Arm | The one term that moved | Result |
|---|---|---|
| baseline | nothing | PASS, 15.6s |
| A | wait for `isSufficientlyLoaded()` before stage 2 | **3/3 PASS - prediction refuted** |
| A' | wait for `isSubscriptionsPausedForBackPressure()` before stage 2 | 3/3 FAIL at 36.8-37.4s, `sufficientlyLoaded=true, recordsInShards=12, threshold=8, outForProcessing=8, freshEpochInserts=0` |
| B | stage 1 produces 8 rather than 12 | 10/10 PASS, 13.6-14.3s |
| C | arm B with `ProcessingShard.addWorkContainer`'s stale-replacement branch disabled | FAIL after 104s on the confluentinc#909 signature, naming exactly the 25 keys `k-25`..`k-49` |

Arms A' and B differ by one record either side of the threshold, in the same position, and the
outcome flips. Arm C is the one that says the repair did not cost the reproduction: it still goes red
against the defect, on the right assertion, naming the same 25 records the original write-up recorded.

## The fix

- `STAGE_1_RECORDS` is now **derived** from `MESSAGE_BUFFER_SIZE` rather than written as a literal,
  so the two cannot drift. At exactly the target, records-in-shards equals the threshold, the
  comparison is strictly greater-than, and the gate stays open.
- The precondition is **asserted** immediately before the stage-2 produce, naming the gate and its
  arithmetic. It was previously implicit, and the failure was mute: a paused partition and a slow
  broker produce the same 30-second timeout on the same line.
- The guard reads the intake state **after** its wait and reports it either way, so a future failure
  says which of the two it was.

## What generalises

- **A test that saturates a pipeline has told the system to stop fetching.** Any later stage that
  needs more records from the broker is fighting the back-pressure the earlier stage switched on.
  This is not specific to PC: it is the shape of every "fill the buffer, now send more" staging.
- **"Always at the deadline, never short of it" is a shape, and it means the awaited thing cannot
  happen** - not that it is slow. A genuinely slow precondition produces a spread of durations under
  the bound and occasional overruns; this produced 30.7s, 30.8s, 30.9s and nothing else. That
  distinction was visible in the ledger for a fortnight before anyone read it as a claim.
- **A mute guard costs more than the flake.** Nothing in the failure named the gate, the pause, or
  the fetch, so eleven recorded sightings across three weeks could establish only that it was
  master-state. One assertion message would have closed it on the first.
- **Correct an arm before abandoning a hypothesis.** Arm A refuted a *prediction*, not the mechanism,
  and treating a refuted arm as a refuted theory would have sent the next session looking at broker
  timing. What separates the two is asking which event the arm actually forced.

## Related

- [`docs/solutions/logic-errors/909-needs-a-saturated-pipeline-the-third-precondition-2026-08-19.md`](../logic-errors/909-needs-a-saturated-pipeline-the-third-precondition-2026-08-19.md) -
  why the reproduction needs saturation in the first place. The fourth precondition this record adds
  belongs to the staging, not to the defect.
- [`docs/inflight/test-909-reproduction-cannot-observe-the-collision.md`](../../inflight/test-909-reproduction-cannot-observe-the-collision.md) -
  the opposite worry, still open: the same test going silently *green* with the defect unexercised.
- [`docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`](vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md) -
  the same subsystem seen from the other side, where the await was satisfiable but meaningless.
