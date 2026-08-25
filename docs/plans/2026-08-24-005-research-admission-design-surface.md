---
title: The Admission Design Surface - What the Engine Can Observe and Actuate
type: research
date: 2026-08-24
topic: admission-design-surface
execution: knowledge-work
---

# The Admission Design Surface

**The inventory `2026-08-24-003-feat-admission-control-law-design.md`'s plant section rests on.**
Produced 2026-08-24 by a worktree-scoped survey of the engine source; every accessor named below was
verified to exist at that date. Point-in-time: re-grep before relying on a specific name.

## 1. Observables

### In-flight / records out for processing

`WorkManager#getNumberRecordsOutForProcessing()` - **records**, not batches. `AtomicInteger`,
any-thread, O(1). Incremented in `getWorkIfAvailable`, decremented in the `handleFutureResult`
family. **Already sampled** once per control-loop pass by `sampleAdmissionInFlight()`, positioned
after dispatch and before `tickAdmissionController()`. Related:
`isWorkInFlightMeetingTarget()` deliberately reads the **pinned static** target, not the live one.

### Worker-pool task queue and active tasks

`UserFunctionTaskAccounting` - `getQueued()`, `getActive()` (four `LongAdder`s; **tasks = batches**).
`getActive()` is the **true deployed concurrency**; in-flight records stop tracking it the moment
batching or queueing intervenes. **Not sampled by admission.** Only consumers: `isPoolQueueLow()`,
`checkPipelinePressure()`.

### Buffered work in the shards

- `WorkManager#getNumberOfRecordsInShards()` - O(1) conservation counters (`admitted - retired`).
- `ShardManager#getNumberOfRecordsParkedForRetry()` - O(n) worst case (read lock, sorted-map walk).
- `getNumberOfWorkQueuedInShardsAwaitingSelection()` - O(shards); under KEY ordering shards ~
  distinct keys.
- `getNumberOfWorkableRecordsInSystem()` = inShards - parked; the **left side of the poller gate**.

### Selectable work right now

`WorkManager#getUpperBoundOnSelectableWork()` - ordering-aware. UNORDERED: awaiting-selection count.
Ordered: two full shard-map passes, `min(awaitingSelection, shardsThatCouldYield)`. **Unused by
admission**; its only consumer is the direct-pull worker wake-up - the one engine that refuses
adaptive concurrency.

### Was a dispatch request fulfilled?

`lastWorkRequestWasFulfilled()` (package-private, control thread only) - set in
`retrieveAndDistributeNewWork`: got >= asked. The direct "shards gave less than requested" signal.
**Unused by admission**; sole consumer is the load-factor step-up guard. Direct-pull equivalent:
`DirectPullWorkerPool#consumeStarvationSignal()`.

### Service time

Bracketed by `System.nanoTime()` around `usersFunction.apply(context)` on the **worker thread** -
**queue wait excluded**, mailbox hop excluded, produce/commit excluded. Per invocation = per batch;
normalised per-record by `admissionServiceTimeSampleNanos(elapsed, batch)`. Exclusions that are
load-bearing: a throwing function yields **no sample**; a batch containing any previously-failed
record yields **no sample** (whole-batch retry exclusion).

### Completions / outcomes

`WorkManager#recordAdmissionOutcome(WorkContainer, boolean)` from `handleFutureResult` on the
control thread, **per record**, after the superseded-delivery and stale-partition filters. Retries
DO produce outcome signal (unlike latency). `AdmissionOutcomeClassifier.classifyFailure` returns
`IGNORE` for every cause in v1, so `OVERLOAD_DROP` is **unreachable from production code**. Richer
signals not wired to admission: `WorkContainer#getResidenceTime()` and the `RECORD_RESIDENCE_TIME`
timer (success, failure AND abandonment - end-to-end including queueing and retries),
`getTimeInFlight()`, per-topic-partition success/failure counters, `getLastFailureReason()` (reaches
the classifier, which discards it).

### Consumer lag

**Not available in-process anywhere.** No call to `endOffsets` / `currentLag` / `position` in the
main tree. Nearest proxies, per partition: `getOffsetHighestSeen()` vs
`getOffsetHighestSequentialSucceeded()`; `getNumberOfIncompleteOffsets()` is O(partitions) with an
O(n) skip-list size call per partition. True lag means a broker round trip with no existing seam.

### Other load-bearing signals

| Signal | Accessor | Note |
|---|---|---|
| Offset-encoding back-pressure | `PartitionState#isBlocked()` / `isAllowedMoreRecords()` | a partition refusing records because commit metadata would not fit - a REAL admission constraint the controller cannot see |
| Dispatch scan cost | `DispatchScanMeter#getEntriesExamined()` | monotonic, survives shard turnover; test-only today |
| Poller state | `BrokerPollSystem#isPausedForThrottling()` | the controller actuates the poller but never reads it - so "starved because we throttled" and "topic empty" are indistinguishable in the window |
| Load factor | `DynamicLoadFactor#getCurrentFactor()` etc. | pinned static under requested ENFORCE |
| Mailbox depth | `workMailBox.size()` | read in `processWorkCompleteMailBox`, not exposed |

## 2. Actuators

### The admission target seam - the one seam

`PCModule#admissionTargetRecords()`: under active ENFORCE returns `slots * batchSize` (live target
while RUNNING, `effectiveMaximum()` otherwise); else the static target. Read by exactly three
consumers: `isSufficientlyLoaded()` (intake, x loadFactor), `getPoolLoadTarget()` ->
`calculateQuantityToRequest()` (dispatch, NOT multiplied by factor under ENFORCE), and
`isPoolQueueLow()`. Target changes bind next pass (dispatch) / next poller iteration (intake), with
`maybeWakeupPoller()` on growth. Clamped `[1, enforceCeiling]`;
`enforceCeiling = leftAtLibraryDefault ? 64 : maxConcurrency`.

### The worker pool - fixed, and the design's central fact

Built once at construction: `new ThreadPoolExecutor(poolSize, poolSize, 0L, MILLISECONDS,
new LinkedBlockingQueue<>(), ...)` with `poolSize = maxConcurrency`. **Core == max, unbounded
queue, nothing ever calls `setCorePoolSize`/`setMaximumPoolSize`.** So the target controls
*feeding*, not concurrency; above the pool size, admitted records queue. Under virtual threads the
pool is unbounded (thread per task, no queue), `isPoolQueueLow()` permanently true, and the target
is the **only** bound - the loop is closed by construction there and open above the pool size on
platform threads. JDK 9+ `setCorePoolSize` **throws above `maximumPoolSize`** - any
pool-follows-target mechanism must construct with max at the ceiling (inert under the unbounded
queue) and steer core alone. Rejection contract: `requireRejectionIsVisible` demands `AbortPolicy`.

### The poller

Pause via `managePauseOfSubscription()` when `isSufficientlyLoaded()` (rate-limited 1/s); resume
un-rate-limited; `wakeupIfPaused()` from the control loop top and from `tickAdmissionController()`
on growth. `setLongPollTimeout` is a **static** intake-rate actuator nothing adaptive touches.

### The feedback map - what lowering the target actually does

1. Dispatch requests go to <= 0 while in-flight exceeds the new target: **no dispatch at all**
   until drain-down (lag >= one service time + mailbox hop).
2. **Buffered work goes UP relative to in-flight** - records are still admitted, just not
   dispatched - so `isSufficientlyLoaded()` more likely true, the poller pauses sooner, intake
   falls **second-order through a different actuator on a different thread**.
3. The in-flight distribution **narrows**, and the committed law tests median/spread as fractions
   of the limit - **a contraction manufactures its own starvation evidence** (the documented reason
   the probe-up arm exists).
4. Fewer completions per window pushes windows toward the min-samples hold.
5. Lower concurrency can lower measured latency -> gradient relaxes -> regrowth (the intended
   primary loop).

Raising the target adds queue depth, not threads, above the pool size (platform); adds real
concurrency 1:1 under virtual threads. Poller pause starves dispatch fulfilment, which BLOCKS the
load-factor step-up (its guard needs the last request fulfilled) - a poller-starved consumer is
indistinguishable from an idle one in the current aggregates.

## 3. Timing and lifecycle

- One control thread (`pc-control`); per-pass order: wake poller -> commit check -> **blocking
  mailbox poll** -> commit -> dispatch -> `sampleAdmissionInFlight()` -> `tickAdmissionController()`
  -> hooks -> state switch -> poller supervise -> `Thread.sleep(1)`.
- Cadence: ~1ms busy, up to `timeBetweenCommits` idle (interruptible). **Windows drift**:
  `windowOpenedAt = now` at tick time, so an idle consumer produces one long window and restarts
  the clock - a fixed count of windows is a variable wall-clock span.
- Clock: controller uses injected `Clock` (testable); service-time uses raw `nanoTime`;
  `DynamicLoadFactor` and `RateLimiter` hardcode wall clock (not injectable).
- Rebalance: callbacks on the broker-poll thread set `assignmentDeltaPending`; the reset runs on
  the control thread - window dropped, **law reconstructed** from the retained builder seeded with
  the current target, 30s cooldown. The target itself carries over.
- Pause poisons the open window (`discardWindow()` on first post-resume tick). Drain/close release
  the seam to `effectiveMaximum()` purely by state read - no tick required.

## 4. The committed law, as mechanism (the Gradient2 port this design replaces)

Arms in precedence order: (1) sample-starved hold (< 10 samples: ALL state untouched);
(2) AIMD backoff on any overload drop (x0.9, once per window - **unreachable**, classifier returns
IGNORE always); (3) failure-limited growth freeze (> 0.2 non-success); (4) starvation probe-up
(+1 when median < limit/4, spread small, latency flat); (5) app-limited hold (median < limit/2);
(6) probe-down (x0.9 every 5 windows at cap with flat latency, or in recovery mode);
(7) gradient fall-through: `limit*(1-s) + (limit*clamp(1.5*long/short, 0.5, 1.0) + 4)*s`, s=0.2.

**RISE = headroom (+0.8/window effective) + probe-up (+1). FALL = gradient floor (x0.9+0.8 worst
case), AIMD (dead), probe-down, failure freeze.** State across windows: fractional limit, 600-window
latency EWMA, probe cadence/pending/pre-probe snapshot, recovery mode. Only reset: total
reconstruction on rebalance. The long baseline folding in self-inflicted degradation is the ratchet
the closed-loop IT observed (band walking 17/18 -> 20, knee 12); the anti-drift decay only rescues
a stale-HIGH baseline.

## 5. Capabilities unused by admission (the opportunity list)

1. `getActive()` - true deployed concurrency.
2. `lastWorkRequestWasFulfilled()` - direct under-served signal.
3. `getUpperBoundOnSelectableWork()` - ordering-aware could-have-yielded.
4. Shard population / parked / workable counts - never sampled into a window.
5. `getResidenceTime()` / `RECORD_RESIDENCE_TIME` - end-to-end latency incl. queueing; strictly
   richer than the service-time tap.
6. Failure cause detail - reaches the classifier, discarded.
7. `PartitionState#isBlocked()` - offset-encoding back-pressure, invisible to the controller.
8. Incomplete-offset depth - in-process commit-frontier proxy.
9. `isPausedForThrottling()` - self-inflicted-emptiness discriminator.
10. `DispatchScanMeter` - selection CPU cost per pass.
11. Batch size - a fixed multiplier nothing varies at runtime.
12. Pool sizing - never touched; the design's actuator fix.

The three signals the design's law section leans on for "the limit is binding" are items 2, 3 and 9
together - separating *topic empty* / *ordering-blocked* / *we throttled our own intake*, which the
current window aggregates cannot.
