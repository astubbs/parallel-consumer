---
title: "SOLVED: committedOffsetRemoved[latest] 'silent stall' was an offset-reset nudge race in the test harness - not a product bug"
date: 2026-07-30
category: test-flakiness
module: parallel-consumer-core
problem_type: flaky_test
component: testing / consumer bootstrap
symptoms:
  - "PartitionStateCommittedOffsetIT.committedOffsetRemoved[1] (latest) fails with ConditionTimeoutException 'not to be empty' - 10s, 60s, or 120s bound, timeout size irrelevant"
  - "ONLY the [1]=latest parameter ever fails; earliest/none never do"
  - "Correlates with box contention (forkCount=16, loaded highcpu runner) but reproduces on any environment"
  - "All PC-side diagnostics healthy: fresh RUNNING PC, counters 0, poller not throttle-paused, work-selection clean"
  - "Still fired with the drain fix AND astubbs#29 AND astubbs#31 all applied - no product fix touched it"
root_cause: latest_offset_reset_resolves_after_single_pre_await_nudge_record
resolution_type: test_harness_fix
severity: medium
status: SOLVED
related:
  - "docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md (the investigation that hunted this)"
  - "docs/solutions/test-flakiness/uber-stall-experiment-results-2026-07-30.md (proved no product fix affected it)"
  - "branch debug/committedoffset-firstpoll-stall (the instrumented hunt that captured it)"
tags: [flaky-tests, offset-reset, latest, awaitility, test-harness, solved]
---
  <!-- file-refs: N/A - quotes an earlier pointer; that write-up was never committed to master -->

# SOLVED: the `committedOffsetRemoved[latest]` stall was an offset-reset race in the test harness

> The last open mystery of the silent-stall investigation, closed. **Independent of the uber merge and of
> every product fix** - found *during* the uber experiment, but the race exists on plain master.

## The mechanism (one paragraph)

A consumer with `auto.offset.reset=latest` and no committed offset resolves its start position at
whatever the log end offset is **at the moment the reset executes**. `runPcUntilOffset` produced exactly
ONE "poll-bumper" record, *before* its await, relying - implicitly, by luck - on the consumer's reset
resolving *before* that bumper landed. Under contention (fork16, loaded highcpu runner), consumer-group bootstrap
takes 1-3s instead of <1s, the reset resolves *after* the bumper, and the consumer is positioned **past
every record that will ever exist** (the helper produces nothing else). From that moment the await -
`seenOffsets isNotEmpty` - is **unwinnable at any timeout**. Not a hang, not a stall, not a broker
problem: a perfectly healthy consumer, correctly parked at the tail of a topic that never grows again.

## The capture that named it

Hunt setup (branch `debug/committedoffset-firstpoll-stall`): the Kafka client's own internal state-machine
narration - which the repo's test logback silences via `org.apache.kafka -> WARN` - selectively re-enabled
(coordinator, subscription state, fetcher, metadata; **no Kafka code modified**), plus a pcId-tagged await
marker. Run 3 of a fork16 loop captured it:

```
14:29.006  DIAG await START (PC1, topic ..., latest)
14:29.301  JoinGroup ok -> 14:29.367 SyncGroup ok -> partition assigned
14:29.400  "Found no committed offset for partition ..."
14:29.484  SubscriptionState: Resetting offset ... to position FetchPosition{offset=201}   <-- THE LINE
14:32-15:26  nothing but healthy heartbeats (2-6ms round trips)
15:29      timeout
```

Topic contents: 200 batch records + 1 bumper = offsets 0-200. **Position 201 = past everything,
including the nudge.** One suppressed INFO line plus subtraction named a mechanism that had survived
weeks of PC-side investigation.

Three eliminations made it stick: bootstrap fast and successful (kills "slow join"); heartbeats healthy
throughout (kills "broker/network distress"); every PC-internal diagnostic previously clean (kills all
in-process theories).

### Correction to the earlier record

The investigation's "0 polls in 120s - forward progress stopped" reading was a misinterpretation: the
instrumentation counted first *records*, not poll invocations. The PC polled the (correctly empty) tail
happily the whole time. "Oversubscription only slows things, it doesn't stop them" was right - the
apparent stop was a test that could no longer be satisfied.

## Why it presented as a roaming cross-environment mystery

- **Only `[1]`/latest can lose this race** - earliest/none position at offset 0 and see everything.
  Observed failures were [1] in 100% of cases (highcpu runner, local fork16, uber arms) - the tell nobody read.
- Contention **correlates** because it widens bootstrap past the ~1s pre-bumper sleep - so it tracked
  load like a real stall would.
- Timeout size was irrelevant (10s/60s/120s all failed) - which *looked* like a hard hang.
- Every product-side fix (drain zombie, astubbs#29's set, astubbs#31) left it untouched - because there was nothing
  wrong with the product.

## The fix (test harness - and DRY'd, not copied)

`runPcCheckStartIs` had already solved this exact problem years ago - it produces bumpers **inside** its
await loop ("must go before failing assertion, otherwise won't be reached"). `runPcUntilOffset` never got
the same treatment. Per the repo's shared-helper rule (the `ensureTopic` drift lesson), the pattern is now
extracted once instead of copied:

- **`BrokerIntegrationTest#awaitWithTopicNudge(pc, pollInterval, atMost, counter, assertion)`** - awaits
  an assertion while producing one nudge record before each attempt; fail-fast on PC death; on timeout,
  logs a **self-diagnosis** (topic end offset vs group committed offset vs nudges sent) so this race
  class names itself in CI logs instead of presenting as a generic empty-collection timeout.
- Both `runPcUntilOffset` and `runPcCheckStartIs` now delegate to it. **No timeout was enlarged, no
  assertion weakened.**
- **`LatestResetTailNudgeIT`** bottles the race deterministically: all records produced before the PC
  starts, group awaited to STABLE (+grace) so the leapfrog is *certain*, empty-so-far verified, then the
  nudged await must succeed. RED with the old pattern (fails in ~23s, every run), GREEN with the
  primitive (~10s, 2 nudges). The wild race, now a caged regression guard.

## Diagnosability lessons (so the next one costs an hour, not a week)

1. **`SubscriptionState` stays at INFO in test logging** - offset-reset decisions are rare, one line
   each, and contain the exact number that solves this class of problem. The blanket
   `org.apache.kafka=warn` was hiding it.
2. **The kafka-client DEBUG set is now a standing, labelled (commented) harness** in
   `logback-test.xml` - coordinator/subscription/fetch/metadata - flip on when hunting bootstrap or
   first-poll issues. Note for kafka-clients 3.5+: the fetch path logs under
   `internals.AbstractFetch`, not only `internals.Fetcher` (a gap in the original hunt config).
3. **Awaits over "records should arrive" must either nudge inside the loop or self-diagnose on
   timeout** - `awaitWithTopicNudge` does both; prefer it over bare `await().untilAsserted(isNotEmpty)`
   in integration tests.
4. The client narrates everything already - **instrument by un-silencing, not by patching**.

## What this does and does not close

- ✅ Closes the only *recurring, cross-environment* CI failure from the silent-stall investigation.
- ✅ With this + the drain fix, the fork16 recipe's acceptance rerun measures whether ANY reproducible
  suite hang remains (see hunt results in the PR).
- ❌ Does not touch the two single-occurrence uber-run stalls (`KafkaSanityTests`,
  `TransactionMarkersTest` - one sighting each; plausibly this same race class via their own await
  patterns, unverified) nor the known `MultiInstanceMetricsTest` lock-timeout flake.
- ❌ Production confluentinc#857 reports remain astubbs#29's territory - unrelated to this test race.
