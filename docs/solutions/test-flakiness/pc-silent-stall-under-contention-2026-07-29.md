---
title: "Drain-path zombie/busy-spin under contention (confluentinc#857 family): a real product bug found while chasing the PartitionStateCommittedOffsetIT flake - the flake itself turned out to be a separate test-harness nudge race"
date: 2026-07-29
category: test-flakiness
module: parallel-consumer-core
problem_type: research_report
component: internal / poll-control
symptoms:
  - "PartitionStateCommittedOffsetIT.committedOffsetRemoved fails with awaitility ConditionTimeoutException 'not to be empty within 10 seconds' at runPcUntilOffset first-poll"
  - "Under heavy fork contention a PC instance polls ZERO records for the whole window (still 0 with a 120s bound) - no exception, consumer alive"
  - "The stall roams: committedOffsetRemoved, TransactionTimeoutsTest, KafkaSanityTests, TransactionMarkersTest all stall intermittently under forkCount=16"
  - "A closing/draining PC busy-spins its poll loop at ~10k iterations/s (92,880 polls in 9s) without ever calling consumer.poll()"
  - "A draining PC stays a live group member (background heartbeats) but cannot participate in rebalances - zombie-holds its partition assignment for up to max.poll.interval.ms"
root_cause: drain_path_shutdownRequested_short_circuit_plus_contention
resolution_type: research_report_plus_diagnostics_committed
severity: medium
status: "drain-path zombie/busy-spin FIXED in PR astubbs#80 (guarded by BrokerPollSystemDrainTest); the committedOffsetRemoved flake that led here was a SEPARATE test-harness nudge race, SOLVED in latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md"
related_prs:
  - "PR astubbs#75 (ci/grumpy-runner-workflow) - where the red surfaced (this report stacks on it)"
  - "PR astubbs#29 / bugs/857-paused-consumption-multi-consumers-bug - silent stall after rebalance (confluentinc#857), root cause OPEN"
  - "PR astubbs#31 / fix/909-stale-container-replacement - stale container at same offset after rebalance (confluentinc#909)"
upstream:
  - "confluentinc/parallel-consumer#857 (paused consumption after rebalance)"
  - "confluentinc/parallel-consumer#548 (same deadlock family)"
tags:
  - flaky-tests
  - silent-stall
  - "857"
  - do-not-mask
  - contention
---

# Drain-path zombie/busy-spin: a real product bug found while chasing the PartitionStateCommittedOffsetIT "flake" (the flake itself was a separate test-harness nudge race)

> **UPDATE 2026-07-30 - the `committedOffsetRemoved` mystery is SOLVED**, with a final twist: it was an
> `auto.offset.reset=latest` **nudge race in the test harness** - the reset resolving after the test's
> single pre-await bumper leaves the consumer positioned past all data forever, making the await
> unwinnable at ANY timeout. NOT a product stall, NOT broker distress (read this report's "broker-side
> hypothesis" as since-disproven). The drain-path zombie found and fixed along the way remains a real
> product bug. Mechanism, capture, fix (shared `awaitWithTopicNudge` + deterministic
> `LatestResetTailNudgeIT` guard) and diagnosability lessons:
> `docs/solutions/test-flakiness/latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md`.

> **Research report, not a fix.** This documents what the diagnosis found so the finding is not lost, and
> ties it into the open confluentinc#857 silent-stall investigation. **No test was masked and no timeout was bumped** -
> doing so would hide a real stall (see AGENTS.md "be EXTREMELY careful modifying tests under stress").
> This PR is stacked on **PR astubbs#75** (which merges first); it deliberately references the **unmerged**
> confluentinc#857 / confluentinc#909 branches.

## TL;DR

- The claim under assessment - *"Integration's red is the known flake (`PartitionStateCommittedOffsetIT`,
  already in inflight, fails on GitHub too) - not the runner"* - is **true about the highcpu runner** (reproduced locally;
  the runner is exonerated) but **understates the cause**: it is **not a benign awaitility timeout**.
- Under real CPU/IO contention a Parallel Consumer instance **silently stops making progress** - zero
  records polled for the entire window, **still zero with a 120s bound**, no exception, consumer alive.
  Oversubscription only *slows* forward progress; a 120s bound would absorb pure slowness. It didn't, so
  something genuinely **stalls**.
- The stall is **not specific to `committedOffsetRemoved`** - under `forkCount=16` it roams across
  timeout-sensitive integration tests (`TransactionTimeoutsTest`, `KafkaSanityTests`, `TransactionMarkersTest`).
- Diagnostics added in this PR **ruled out** the "work stuck in the queues" theory (captured `returned 0`
  cases show the work genuinely **in-flight**, not stuck-selectable or missing).
- The chase instead **root-caused a concrete drain-path defect**: `drain()` sets `shutdownRequested`
  *before* entering `DRAINING`, which makes `ConsumerManager.poll()` short-circuit without ever calling
  `consumer.poll()` - defeating the code's own intent that the paused 2 s long poll act as the drain loop's
  sleep. Measured: **92,880 poll-loop iterations in 9.0 s (~10 kHz busy-spin)**. Worse, with
  `consumer.poll()` never invoked the draining consumer **cannot participate in rebalances** while its
  background heartbeat keeps it a **live group member zombie-holding its partitions** - for up to
  `max.poll.interval.ms` (5 min default) if the drain sticks. A stuck drainer therefore starves same-group
  siblings (partition hold) *and* the whole box (a core per zombie) - a strong candidate contributor to
  **confluentinc#857's production symptom** (hypothesis credit: Antony).
- `committedOffsetRemoved` itself stalls as a fresh **RUNNING** PC on its first poll (so it is *not*
  throttle-paused; its group is fresh so the partition-hold channel is indirect) - the leading explanation
  is broker/CPU starvation amplified by zombie drainers from other tests in the same fork; its own trace
  stayed elusive under instrumentation.
- **Action: do not bump the await to go green.** The drain-path fix has **landed on this PR** (state
  collapse: the duplicated shutdown flag is deleted, abort derives from the poll system's `runState`;
  guarded by `BrokerPollSystemDrainTest`, characterisation-first → RED → GREEN). The broker-side
  fail-safe release and the single-thread end-state remain with the confluentinc#857 investigation, along with
  `committedOffsetRemoved` + the reproduction recipe + the diagnostics.

## Background

PR astubbs#75 adds an optional, non-gating high-CPU self-hosted ("highcpu runner", 24c/48t) fast-feedback workflow. Its
**Integration** matrix job (`bin/ci-integration-test.sh -DforkCount=16 -DreuseForks=true`) went red. The
sole failure was:

```
PartitionStateCommittedOffsetIT.committedOffsetRemoved(OffsetResetStrategy)[1]  (LATEST)
  -> org.awaitility.core.ConditionTimeoutException: ... not to be empty within 10 seconds
```

which maps to the **only bare-default (10s) `Awaitility.await()`** in the file, in `runPcUntilOffset(...)`
(`assertThat(seenOffsets).isNotEmpty()`) - the wait for a freshly-built PC's **first poll** after a
`setupCompactingKafkaBroker()` cold bootstrap. Every other await in that file already sets an explicit
30s+ bound; this one is the lone outlier.

The obvious "fix" is to give that await a generous bound. **That would be wrong** - the sections below show
the underlying PC genuinely stalls, so a bigger deadline just hides it.

## Reproduction and evidence

Local box: 12 logical cores, 32 GB (highcpu runner: 48 threads). "forks/core" is the oversubscription ratio;
the highcpu runner's integration job runs 16 forks / 48 threads = 0.33.

| Run | forks/core | await bound | Result |
|-----|-----------|-------------|--------|
| Baseline | 1 (none) | 10s | **7/7 green** |
| Fair | 8 → 0.67 | 120s | **7/7 green**, worst first-poll **1.0s** (others ~0.5s) |
| Contended | 16 → 1.33 | 10s | **stall** - `committedOffsetRemoved` ConditionTimeout (the highcpu runner's exact signature) |
| Contended | 16 → 1.33 | **120s** | **still fails - 0 polls in 120s** |

Two facts are decisive:

1. **Normal first-poll latency is ~0.5s** (9 samples, 528-574 ms), with huge headroom under 10s. So this is
   not a slow-but-progressing poll.
2. **A 120s bound still yields zero polls.** Pure CPU oversubscription would only slow the poll; 120s would
   be plenty. Forward progress **stopped**. (Credit: this was the observation that redirected the whole
   diagnosis - "oversubscribing shouldn't cause test fails, only slow downs.")

### The stall is general, not one test

Under `forkCount=16` with DEBUG, the stall moved around run-to-run:

- Run A: `committedOffsetRemoved` (0 polls / 120s)
- Run B: `TransactionTimeoutsTest`
- Run C: `KafkaSanityTests` **and** `TransactionMarkersTest`

Whichever timeout-sensitive PC test gets starved is the one that reddens. `KafkaSanityTests` stalling is
notable - it is the most basic PC test in the suite.

### What the instrumentation ruled in and out

Two hypotheses were tested with committed diagnostics (see "Instrumentation added" below):

**Ruled OUT - "work is stuck in the queues".** The new under-served detector in `ShardManager.getWorkIfAvailable`
logs whenever it hands back less than requested while work is tracked, with a breakdown. Across a stalling
run every `returned 0` case looked like:

```
Work retrieval under-served: requested N, returned 0, but 3 tracked across 3 shard(s)
    [awaitingSelection=0, inFlight=3, retryQueue.size=0, retryQueue.readyToRetry=0]
```

i.e. the tracked work is genuinely **in-flight** (being processed), **not** selectable-but-unhandled and
**not** missing. So the "asked for work, got none, but it's sitting in the queues" hypothesis does **not**
hold for what was captured - the shard/work-selection layer is behaving.

**Captured and ROOT-CAUSED - the drain path busy-spins and zombie-holds its partitions.** The one stall
captured under DEBUG (`TransactionTimeoutsTest`) showed the broker poller paused and spinning - and chasing
it found a concrete, code-confirmed defect in the close/drain path. The evidence:

- `Subscriptions are paused: true` **× 92,880** vs `false` × 21; `Got 0 records` × 92,894; **`Resuming
  consumer, waking up` × 0**; `Run state: DRAINING` × 92,880.
- Timestamps: `signalStop` at 40:58.547 → spin starts 40:58.575 → last spin poll 41:07.570. That is
  **92,880 poll-loop iterations in 8.995 s ≈ 10,300/s** - a hard busy-spin, while the control thread waited
  out `workerThreadPool.awaitTermination` on 1 in-flight work item.

The causal chain (all in main code):

1. `AbstractParallelEoSStreamProcessor.innerDoClose()` calls `brokerPollSubsystem.drain()` **first**, then
   blocks awaiting in-flight user work; only after that does `closeAndWait()` actually close the poller.
2. `BrokerPollSystem.drain()` calls `consumerManager.signalStop()` → sets `shutdownRequested=true` - and
   then enters `DRAINING`, in which the poll loop **keeps calling poll by design**: the comment on
   `handlePoll()` says *"if draining - subs will be paused, so use this to just sleep"* - i.e. the 2 s long
   poll is **intended to be the drain loop's sleep**.
3. But `ConsumerManager.poll()` wraps `consumer.poll()` in `while (!shutdownRequested.get())` - so once
   `signalStop()` has run, **`consumer.poll()` is never invoked again**: the method returns an empty result
   immediately. The intended 2 s sleep never happens → the loop spins at ~10 kHz, burning a full core per
   draining PC. **Intent vs implementation mismatch: the 2 s long-poll "sleep" should be honoured.**
4. Because `consumer.poll()` is no longer called, the consumer **cannot participate in any rebalance**
   (rejoin/revoke-ack happen inside `poll()`), while the Kafka client's **background heartbeat thread keeps
   it a live group member**. Result: a draining PC is a **zombie member that holds its full partition
   assignment**, consuming nothing, until close completes - or until the broker evicts it at
   `max.poll.interval.ms` (**default 5 minutes**).

**Why this matters beyond one test (the partition-hold hypothesis - credit: Antony).** A stuck/slow-draining
PC couples to the rest of the system through two channels:

- **Channel 1 - partition hold (same consumer group).** Any other member - a REUSE_GROUP successor PC in a
  test, or a sibling instance in production - cannot take the zombie's partitions. A joiner's rebalance
  stalls waiting on the zombie for up to the rebalance timeout (= `max.poll.interval.ms`, 5 min default),
  which dwarfs every await in the test suite (10 s / 30 s / even the 120 s diagnostic bound). This maps
  *directly* onto **confluentinc#857's production symptom**: chaos-stop / restart an instance → close drains slowly →
  zombie member holds partitions → "paused consumption after rebalance, lag accumulates, only restart
  fixes".
- **Channel 2 - CPU/broker parasitism (any group).** Each draining PC burns a core at ~10 k iterations/s.
  Under `forkCount=16` with tests constantly closing PCs, that is a fleet of core-burning zombies starving
  brokers and fresh PCs - and it is **self-reinforcing**: contention slows drains → more zombie-time →
  more contention. This is the plausible amplifier behind fresh-group stalls like `committedOffsetRemoved`
  (whose group is new, so Channel 1 does not apply directly).

**`committedOffsetRemoved`'s own mechanism - still untraced, but narrowed.** It stalls as a **fresh RUNNING
PC on its very first poll**, before it has processed or revoked anything - so
`numberRecordsOutForProcessing` is 0 and `awaitingSelection` is 0, which means `isSufficientlyLoaded()` is
false and the poller should **not** be throttle-paused at all. That points **away** from the pause/resume
and counter-drift mechanisms and **towards the broker side**: the freshly-started, aggressively-compacting
broker (`setupCompactingKafkaBroker()`, 2 extra brokers, `max.compaction.lag.ms=1`,
`min.cleanable.dirty.ratio=0`) not serving the first fetch within the window under 1.33-forks/core
starvation. This remains a hypothesis - the test passes every time under DEBUG, so its own internal trace
was not captured. Whether that is truly distinct from confluentinc#857, or a third facet of the same fragility, is the
open question for the confluentinc#857 harness (which reproduces a running stall deterministically). Channel 2 above
(zombie drainers from *other* tests in the same fork starving CPU + the fork's shared broker) is the
leading amplifier candidate.

## Drain-path design review: is the drain fail-safe?

The drain system's job is to stop taking new work, finish what is in flight, commit, and leave. The core
robustness requirement - which the current implementation does not clearly meet - is:

> **The drain must release its hold on broker-side resources (group membership, partition claim) within a
> hard bound, regardless of whether user work ever completes.** Today, release depends on user functions
> finishing (or the worker-pool timeout firing) and, failing that, on the *broker* evicting the zombie at
> `max.poll.interval.ms`. Relying on broker-side eviction as the backstop is the opposite of fail-safe - it
> is the slowest possible release path, and it is exactly what a sibling consumer experiences as "paused
> consumption".

Options to consider (not mutually exclusive), roughly in order of increasing invasiveness:

1. **Honour the 2 s long poll during DRAINING (restore the design intent) - ✅ IMPLEMENTED on this PR,**
   in the stronger *state-collapse* form: `ConsumerManager.shutdownRequested` + `signalStop()` are
   **removed entirely** (the flag was a private shadow of `BrokerPollSystem.runState`, settable out of
   phase - the desync WAS the bug). `ConsumerManager` now derives "abort retries/polling" from an injected
   `closeInProgressSignal` reading the poll system's `runState == CLOSING/CLOSED` - one source of truth,
   so the desync *class* is structurally impossible. The paused `consumer.poll(2s)` again provides the
   drain loop's sleep (as `handlePoll()`'s comment intends), killing the 10 kHz spin **and** keeping the
   drainer rebalance-responsive. Guarded by `BrokerPollSystemDrainTest` (committed first as a
   characterisation of the defect, then flipped RED→GREEN by the fix).
2. **Hard drain deadline with explicit group exit (fail-safe only - NOT the normal path).** During a
   healthy drain the member **should** keep its partitions (see option 3's rejection: that is what lets it
   finish and commit, avoiding duplicates). The guarantee to add is only for the *pathological* case:
   when `drainTimeout` expires (or `waitForClose` times out) the drain is being abandoned anyway, so
   explicitly `consumer.close(bounded)` / unsubscribe so a **LeaveGroup** is sent and the partitions free
   immediately - never leave departure to session/poll-interval eviction, and accept the bounded
   duplicates as the lesser evil versus an indefinite hold. Audit the timeout/exception paths in
   `close()`/`waitForClose()` for gaps where the supervisor gives up but the consumer object (and its
   heartbeat thread) lives on.
3. **~~Leave the group eagerly at drain start~~ - REJECTED (2026-07-30).** Holding the partitions through
   the drain is the *point* of draining: the member keeps its assignment so in-flight work can finish and
   its offsets can be **committed before leaving** - that is what delivers no/near-zero duplicates (and,
   with transactions, a clean final commit). Releasing at drain start hands the partitions to a sibling
   that immediately **reprocesses all in-flight work** - guaranteed duplicates, i.e. `DONT_DRAIN`
   semantics smuggled into `DRAIN`. The defect was never the hold; it was the hold *combined with*
   protocol-unresponsiveness. Post-fix behaviour is correct on both paths: undisturbed drain → finish,
   commit, leave (zero duplicates); rebalance forced mid-drain → participate, commit-what's-done on
   revoke, hand over cleanly (duplicates bounded to the not-yet-committable tail). Explicit LeaveGroup
   belongs only in option 2's **deadline-expiry fail-safe**, where the drain is being abandoned anyway and
   bounded duplicates are the lesser evil versus an indefinite hold.
4. **Spin watchdog.** The poll loop has no self-observation; a 9-second 10 kHz spin was invisible until
   this capture. A cheap rate check (iterations/sec, rate-limited WARN) in `BrokerPollSystem`'s control
   loop would have flagged this class of bug years ago, and would catch regressions.
5. **End-state: single-thread consumer ownership (defer to the confluentinc#857 stream).** Merge the broker-poll
   thread into the control thread so ONE thread owns the consumer (the KafkaConsumer reference pattern -
   `wakeup()` is its only sanctioned cross-thread call). Close then becomes a linear function on that
   thread - pause → bounded wait for workers → commit → `consumer.close()` (LeaveGroup) - and DRAINING is
   just a loop mode: the CME class, the `commitCommand` locking, the wakeup choreography, and *all*
   cross-thread lifecycle flags evaporate. This "merge poll + control threads" refactor is already being
   weighed in the confluentinc#857 investigation; the uber-branch experiment's results should inform whether it is
   justified.

## Honest caveats / what is NOT yet proven

- **`committedOffsetRemoved`'s own internal trace was never captured** - it passes every time under DEBUG
  (instrumentation perturbs the timing). Its broker-side hypothesis is inference from *when* it stalls
  (fresh RUNNING PC, first poll, counters at 0), not a captured trace.
- **The drain-path defect (spin + zombie hold) is code-confirmed, but its *share of blame* for each test
  failure is not.** The drain being *slow* in the captured run was partly by-design of
  `TransactionTimeoutsTest` (deliberately slow in-flight work). What generalises is the window's behaviour:
  every closing PC spins a core and holds its partitions until the drain completes. How often that window
  is long enough to matter in production (confluentinc#857) vs in the suite is unmeasured.
- **Channel 2 (zombie amplification) as `committedOffsetRemoved`'s cause is inference**, consistent with
  the data (fresh group, counters at 0, non-linear onset at forks>cores) but not directly traced.
- The under-served detector only observed **normal in-flight back-pressure**, not stuck-selectable or
  missing work - so the queue/shard layer is exonerated *for the scenarios captured*, which does not prove
  it is healthy in the (uncaptured) `committedOffsetRemoved` stall.
- What **is** firmly established: it is a real stall (0 polls / 120s), not runner-specific, not a benign
  timeout, and in the confluentinc#857 silent-stall family. The exact per-scenario mechanism is **open**.

## Relationship to the in-flight lock-up work

- **confluentinc#857 / PR astubbs#29** (`bugs/857-paused-consumption-multi-consumers-bug`) - "Paused consumption after
  rebalance". Two documented mechanisms (see `docs/BUG_857_INVESTIGATION.md (deleted 2026-08-18; retrieve with `git show 262629aab:docs/BUG_857_INVESTIGATION.md`)` on that branch): (1) a
  `ConcurrentModificationException` when `close()` races the poll thread; (2) the **silent stall** from
  `numberRecordsOutForProcessing` counter drift. Commit message: provisional fixes **do not eliminate the
  stall** (10-20% of chaos runs still stall); **root cause still open**. The **zombie-drainer partition
  hold** root-caused here is a *third* mechanism in the same family - and note it composes with (1): a
  rebalance touching a draining member runs revoke on the spinning poll thread, right where the CME/commit-
  lock races live.
  - **VERIFIED (2026-07-29): PR astubbs#29 does NOT fix the drain defect.** On its branch, `drain()` still calls
    `consumerManager.signalStop()` first (in `BrokerPollSystem.drain()`) and `ConsumerManager.poll()`
    still has the `while (!shutdownRequested.get())` short-circuit - the ~10 kHz spin and zombie hold
    survive unchanged. What it *does* fix is a **sibling** mechanism: `onPartitionsAssigned()` now resets
    `pausedForThrottling` (a RUNNING-state throttle-pause stall on re-assignment - same "poller paused when
    it shouldn't be" family, different state). It even **adds trace logging inside the very loop that
    spins** (`handlePoll`: "Poll returned 0 records. assignment=..., paused=...") - instrumentation that
    would log through the 10 kHz spin without recognising it.
    - (Citation repair: the two findings above each carried a line number, one into `BrokerPollSystem`
      and one into `ConsumerManager`, both read against the
      `bugs/857-paused-consumption-multi-consumers-bug` branch on 2026-07-29. That branch has since
      merged master, which carries the fix this write-up called for (`bd7172418`, PR astubbs#80), so
      `signalStop` and `shutdownRequested` no longer appear on it at all and both cited lines are now
      blank. The method names are the durable anchors; for the code as verified, `git show
      bd7172418^:parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/BrokerPollSystem.java`
      and the sibling `ConsumerManager.java`, grep `signalStop` and `shutdownRequested`. Re-running the
      check on the branch today would find nothing - because the branch moved, not because the
      2026-07-29 finding was wrong. The repair establishes where to look; it does not re-verify.)
  - **Residual-stall link:** astubbs#29 reports 10-20% of aggressive-chaos runs *still* stalling with its fixes
    applied. The chaos monkey's stop/start cycling is a drain-window factory, and the unfixed zombie-drain
    path is a prime candidate for exactly that residue - a testable prediction for the uber-branch
    experiment below.
- **confluentinc#909 / PR astubbs#31** (`fix/909-stale-container-replacement`) - **VERIFIED: no overlap.** Its main-code diff
  is only `ProcessingShard.java` (11 lines, stale-container replacement); it touches nothing in the
  drain/poll/shutdown paths. Adjacent rebalance-correctness work on a fourth, independent mechanism (stale
  container at the same offset blocking that shard's progress).
- **Complementary, not competing.** All three efforts chase the same *symptom* - "PC alive but not
  progressing" - via **different, non-conflicting mechanisms**: astubbs#29 = close-race CME + counter drift +
  assign-time throttle-pause; astubbs#31 = per-shard stale-container block; this report = drain-time zombie
  spin/hold. The minimal drain fix (defer `signalStop()` to `CLOSING`) conflicts with **neither** branch.
- Base master (and therefore PR astubbs#75) contains **none** of these fixes, so the sharpened stress suite
  (forked-per-broker integration, PR astubbs#68; unit forking, PR astubbs#69) is now surfacing the unsolved stall.
- Prior art: `docs/solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`
  (the RebalanceEoSDeadlockTest contention → real confluentinc#857 deadlock finding, and the "diagnose before masking"
  rule it produced).

## Recommendations

1. **Do not mask.** Leave `committedOffsetRemoved`'s await as-is; do not bump it to go green. A generous
   bound here would hide a real production stall - the exact failure this library exists to prevent.
2. **Keep the highcpu runner Integration job non-gating** (it already is). The runner is fine; the red is a real
   product stall that also affects GitHub-hosted runs intermittently (via sibling flakes like
   `MultiInstanceMetricsTest`).
3. **Route to confluentinc#857.** Add `committedOffsetRemoved` and `KafkaSanityTests` as additional reproductions under
   the confluentinc#857 investigation. To finally capture `committedOffsetRemoved`'s own trace, run it on a box that
   stalls it *without* DEBUG first, then attach the (now committed) `ShardManager` / `WorkManager` /
   `BrokerPollSystem` loggers - the `isSufficientlyLoaded=...` line will show whether the poller is
   throttle-paused (counter/`numberRecordsOutForProcessing` drift) or the broker is simply not serving.
4. **Fix the drain path - ✅ DONE on this PR** (design-review option 1, in its stronger state-collapse
   form: the duplicated `shutdownRequested` flag is deleted; `ConsumerManager` derives abort from the
   poll system's `runState` via an injected signal). Guarded by `BrokerPollSystemDrainTest`
   (characterisation-first, then flipped RED→GREEN by the fix). The fail-safe release guarantee (options
   2-3) and the single-thread end-state (option 5) remain deeper design work, best coordinated with confluentinc#857
   since they touch the same close/rebalance paths.
5. **Uber-branch experiment: merge all the partial fixes and measure.** The three efforts fix
   non-conflicting mechanisms of the same symptom, so combine them on one integration branch -
   **astubbs#29 (857) + astubbs#31 (909) + this PR (diagnostics + drain fix)** - and run both reproductions: (a) this
   report's `forkCount=16` stress recipe, and (b)
   astubbs#29's `MultiInstanceRebalanceTest` chaos run, which currently still stalls 10-20% of the time. That
   turns three partial theories into one measurable experiment: if the residual chaos-stall rate drops to
   ~0 with the drain fix added, the zombie-drainer mechanism explains the residue; whatever remains is a
   fifth mechanism, now observable via the committed diagnostics.
6. **When the fixes land,** re-run this reproduction recipe to confirm the stall is gone (not merely
   timing-hidden), then the `committedOffsetRemoved` await can be revisited on its own merits.

## Reproduction recipe

On a box where `forks/core >= ~1.3` (e.g. 16 forks on <=12 cores):

```bash
./mvnw -Pci -pl parallel-consumer-core -am verify \
  -DskipUTs=true -Dexcluded.groups=performance \
  -DforkCount=16 -DreuseForks=true
```

Expect an intermittent silent stall (0 polls for the whole window) in one of the timeout-sensitive
integration tests. `forkCount=8` on 12 cores does **not** reproduce (worst first-poll ~1s), so the trigger
is genuine resource pressure, not a fixed slow path.

### Instrumentation added (committed in this PR - observability for the ongoing hunt)

All at DEBUG (off by default, no CI cost) and behaviour-preserving; **no test timeout was changed** (no
masking):

- **`ShardManager.getWorkIfAvailable`** - a silent-stall detector: when it hands back less than requested
  while work is still tracked, it logs the breakdown `[awaitingSelection, inFlight, retryQueue.size,
  readyToRetry]` so "asked for work / got none / but it's in the queues (or missing)" can be told apart
  from normal in-flight back-pressure.
- **`WorkManager.isSufficientlyLoaded`** - logs the throttle decision inputs
  (`awaitingSelection + outForProcessing vs target*loadingFactor`). This gates the broker-poller
  pause/resume, so an inflated `numberRecordsOutForProcessing` (the confluentinc#857 counter-drift) shows up here as
  "loaded" with no real work.
- **`KafkaClientUtils.buildPc`** - gives every test PC a unique `myId`, so `pc-control-PCn` /
  `pc-broker-poll-PCn` thread names and the `%X{pcId}` MDC make concurrent instances distinguishable in the
  logs (the `myId` hook was otherwise never wired). Safe: `RebalanceEoSDeadlockTest` matches thread names
  with `.contains(...)`.
- **`logback-test.xml`** - commented-out `ShardManager` / `WorkManager` / `BrokerPollSystem` DEBUG loggers,
  grouped and labelled for the confluentinc#857 stall hunt; uncomment to reproduce the captures above.

## Sources

- highcpu runner failure: PR astubbs#75 run `30434423800` (failsafe report: `committedOffsetRemoved[1]` ConditionTimeout).
- GitHub-hosted Integration red on the same PR: run `30424305954` - the sibling flake
  `MultiInstanceMetricsTest.sameRegistryCanBeReusedAfterPcInstanceClosed`, not this test.
- `docs/inflight.md` - existing `committedOffsetRemoved` and `MultiInstanceMetricsTest` flake entries.
  (Pointer repair: that single file became the directory [`docs/inflight/`](../../inflight/) on
  2026-08-04, deleted in `0de96fc` - `git show 0de96fc^:docs/inflight.md` for the entries as this
  report read them. Of the two, only `MultiInstanceMetricsTest` is still a row in
  [`docs/inflight/test-load-tightness-flakes.md`](../../inflight/test-load-tightness-flakes.md). The
  `committedOffsetRemoved` entry read here is the `[latest]` nudge race, since solved and written up
  in
  [`latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md`](latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md);
  the `committedOffsetRemoved[3] none` row in that live file is a different and later
  `RebalanceInProgressException` sighting, not a successor to this one.)
- `docs/BUG_857_INVESTIGATION.md` (on `bugs/857-paused-consumption-multi-consumers-bug`).
- Code: `parallel-consumer-core/.../integrationTests/state/PartitionStateCommittedOffsetIT.java`;
  `.../internal/AbstractParallelEoSStreamProcessor.java` (`innerDoClose` - drain-then-await sequencing;
  `calculateQuantityToRequest`); `.../internal/BrokerPollSystem.java` (`drain()`, `handlePoll()` and its
  "use this to just sleep" comment, `doPause`/`resumeIfPaused`); `.../internal/ConsumerManager.java`
  (`poll()`'s `while (!shutdownRequested.get())` short-circuit, `signalStop()`).
