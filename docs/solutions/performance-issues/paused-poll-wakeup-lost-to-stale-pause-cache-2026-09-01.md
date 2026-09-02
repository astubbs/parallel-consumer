---
title: Transactional throughput collapsed 4-10x because the paused-poll wakeup read a pause cache refreshed one poll too late
date: 2026-09-01
category: performance-issues
module: parallel-consumer-core
problem_type: performance_regression
component: BrokerPollSystem / ConsumerManager back-pressure
symptoms:
  - "TransactionAndCommitModeTest#testTransactionalDefaultMaxPoll 4-10x slower on the confluentinc#857 branch than master; fails its 30s deadline with ~1/3 of records unconsumed, load-dependently"
  - "Ingestion arrives in bursts of one max.poll.records batch separated by dead stalls of up to the 2s long-poll timeout"
  - "Executor pipeline drains to zero mid-run while records sit unconsumed on the topic ('requesting 187' answered by 'New work incoming: 1')"
  - "Control loop's 'Found Poller paused ... ensuring poller is awake' almost never fires (2 firings against 68 pauses; master: 15 against 21)"
root_cause: stale_cross_thread_cache
tags: [confluentinc-857, back-pressure, pause-resume, long-poll, wakeup, thread-confinement]
---

# The mechanism

The broker-poll loop makes its pause/resume decision (`managePauseOfSubscription`) immediately
BEFORE each `consumer.poll()`. When back-pressure pauses the subscription, the next poll is a blind
sleep for the full long-poll timeout (2s): a paused consumer returns nothing and nothing but
`wakeup()` can end the wait early. The system's escape hatch is the CONTROL thread:
`maybeWakeupPoller` fires `wakeupIfPaused` when the pipeline runs low - but, forbidden from touching
the consumer (thread confinement), it reads the pause state from
`ConsumerManager.pausedPartitionSizeCache` via `BrokerPollSystem#isSubscriptionsPausedForBackPressure`.

The confluentinc#857 branch (astubbs/parallel-consumer#29) had moved `updateCache()` from the entry
of `ConsumerManager.poll()` to its exit only, to stop `wakeup()` racing the cache's consumer calls
(a real ConcurrentModificationException fix). Side effect: the cache was now only ever captured at
the END of a poll - a moment at which the fatal sleep's pause had not yet been applied. Every paused
sleep therefore ran with the cache reading "not paused", the control thread never woke the poller,
and each pause cost the full 2s with the pipeline drained. Steady-state throughput degraded to
roughly one `max.poll.records` burst per pause cycle. In `PERIODIC_TRANSACTIONAL_PRODUCER` with the
pipeline saturated enough to keep crossing the pause threshold, that was a 4-10x regression and a
deadline failure whenever ambient load stretched the cycles.

# Why every earlier hypothesis missed

Cluster-2 overhead, paused consumption, "it is idle", and epoch fencing were each refuted in turn
(`docs/inflight/test-857-branch-red-lanes-cause-unestablished.md` records the sequence). The system
is neither slow per-record nor permanently paused nor idle: it OSCILLATES - burst, drain, 2s dead
sleep, burst - so any single-moment sample supports whichever wrong reading it lands on. The
diagnostic that broke it open was per-instance TIMELINES from the siloed debug logs: gaps in each
worker pool's "Produce lock acquired" stream, and the ratio of "Found Poller paused" wakeups to
"Pausing subs" events.

# The differential (same machine, same command, alternated arms, debug on, 5 parallel repetitions)

| arm | outcome | rec/s | produce gaps >200ms per instance | poller wakeups / pauses |
|---|---|---|---|---|
| branch, unfixed | 3/5 FAIL | 649-1482 | 10-24 gaps, 10-22s total (50-70% of span) | 2 / 68 |
| master control | 5/5 pass | 3000-3107 | zero | 15 / 21 |
| branch, fixed | 5/5 pass | 1777-1833 | at most 2 gaps, <=0.6s | 16 / 22 |

A second alternated pair repeated the result: master 2465-2491 rec/s, fixed branch 1444-1471 rec/s
(at higher ambient load), both 5/5 green - twenty fixed-or-master repetitions, zero failures, against
3/5 failing unfixed. A residual sub-2x gap to master survives the fix and is tracked separately in
`docs/inflight/test-857-branch-red-lanes-cause-unestablished.md`.

The fix restored `updateCache()` at poll ENTRY - after the caller's pause decision, BEFORE
`pollingBroker` is set (so `wakeup()` still cannot race it and the CME fix is preserved) - keeping
the exit refresh for post-rebalance freshness. Controlled experiment: one term changed, outcome
flipped. `ConsumerManagerPauseCacheTest` holds the contract deterministically with
`MockConsumer.schedulePollTask` observing the cache from inside the poll it describes.

# The reusable lessons

- **A cache consumed by another thread must be refreshed at the START of the blocking call it
  describes, not after it.** An exit-only refresh reports every state change one blocking call late,
  and the reader who needed it reads it DURING the call. Sweep result on this class: the only
  decision-consumer of these caches was `isSubscriptionsPausedForBackPressure` (fixed); the
  metrics gauge and diagnostics share the staleness but decide nothing; `metaCache`/`assignment`
  change only inside `poll()` itself, so the exit refresh is the correct one for them.
- **An oscillating system defeats point samples.** Three confident wrong readings each came from one
  counter at one instant. Rates and event-timeline gaps (produce-lock acquisition gaps per pool) were
  what made the burst-stall cycle visible.
- **Watch the guard's ratio, not its silence**: wakeups-per-pause (2/68 vs 15/21) named the dead
  mechanism directly.
