---
title: "PartitionStateCommittedOffsetIT 'flake' is a real silent stall under contention (#857 family), not a test-timeout problem"
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
status: OPEN - do not mask
related_prs:
  - "PR #75 (ci/grumpy-runner-workflow) - where the red surfaced (this report stacks on it)"
  - "PR #29 / bugs/857-paused-consumption-multi-consumers-bug - silent stall after rebalance (#857), root cause OPEN"
  - "PR #31 / fix/909-stale-container-replacement - stale container at same offset after rebalance (#909)"
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

# PartitionStateCommittedOffsetIT: the grumpy "flake" is a real silent stall, not a test-timeout bug

> **Research report, not a fix.** This documents what the diagnosis found so the finding is not lost, and
> ties it into the open #857 silent-stall investigation. **No test was masked and no timeout was bumped** -
> doing so would hide a real stall (see AGENTS.md "be EXTREMELY careful modifying tests under stress").
> This PR is stacked on **PR #75** (which merges first); it deliberately references the **unmerged**
> #857 / #909 branches.

## TL;DR

- The claim under assessment - *"Integration's red is the known flake (`PartitionStateCommittedOffsetIT`,
  already in inflight, fails on GitHub too) - not grumpy"* - is **true about grumpy** (reproduced locally;
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
  **#857's production symptom** (hypothesis credit: Antony).
- `committedOffsetRemoved` itself stalls as a fresh **RUNNING** PC on its first poll (so it is *not*
  throttle-paused; its group is fresh so the partition-hold channel is indirect) - the leading explanation
  is broker/CPU starvation amplified by zombie drainers from other tests in the same fork; its own trace
  stayed elusive under instrumentation.
- **Action: do not bump the await to go green.** Fix the drain path (honour the 2 s poll; bound the
  broker-side release) and feed `committedOffsetRemoved` + the reproduction recipe + the new diagnostics
  into the #857 investigation.

## Background

PR #75 adds an optional, non-gating high-CPU self-hosted ("grumpy", 24c/48t) fast-feedback workflow. Its
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

Local box: 12 logical cores, 32 GB (grumpy: 48 threads). "forks/core" is the oversubscription ratio;
grumpy's integration job runs 16 forks / 48 threads = 0.33.

| Run | forks/core | await bound | Result |
|-----|-----------|-------------|--------|
| Baseline | 1 (none) | 10s | **7/7 green** |
| Fair | 8 → 0.67 | 120s | **7/7 green**, worst first-poll **1.0s** (others ~0.5s) |
| Contended | 16 → 1.33 | 10s | **stall** - `committedOffsetRemoved` ConditionTimeout (grumpy's exact signature) |
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
  *directly* onto **#857's production symptom**: chaos-stop / restart an instance → close drains slowly →
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
was not captured. Whether that is truly distinct from #857, or a third facet of the same fragility, is the
open question for the #857 harness (which reproduces a running stall deterministically). Channel 2 above
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

1. **Honour the 2 s long poll during DRAINING (restore the design intent).** Do not `signalStop()` the
   `ConsumerManager` at drain time - only at `CLOSING`. Then the paused `consumer.poll(2s)` provides the
   loop's sleep (as `handlePoll()`'s comment intends), kills the 10 kHz spin, **and** keeps the consumer
   participating in the group protocol, so it can promptly ack revokes / rejoin instead of stalling other
   members' rebalances. This is the minimal fix and addresses both channels at once.
2. **Hard drain deadline with explicit group exit.** `drainTimeout` already bounds the *wait for user
   work*; extend the guarantee to the *broker side*: when the deadline expires (or `waitForClose` times
   out), explicitly `consumer.close(bounded)` / unsubscribe so a **LeaveGroup** is sent and the partitions
   free immediately - never leave departure to session/poll-interval eviction. Audit the timeout/exception
   paths in `close()`/`waitForClose()` for gaps where the supervisor gives up but the consumer object (and
   its heartbeat thread) lives on.
3. **Leave the group eagerly at drain start.** Since DRAINING pauses all partitions anyway (no new records
   will ever be processed), the membership is only being kept for the final offset commit. An alternative
   shape: commit what is ready, leave the group immediately (releasing partitions to siblings), then finish
   in-flight work knowing those offsets cannot be committed (they will be redelivered - the same semantics
   as `DONT_DRAIN`, applied only to the tail). Real trade-offs for EoS/transactional commit modes (commits
   require live group membership/generation), so this is a design discussion, not a quick fix.
4. **Spin watchdog.** The poll loop has no self-observation; a 9-second 10 kHz spin was invisible until
   this capture. A cheap rate check (iterations/sec, rate-limited WARN) in `BrokerPollSystem`'s control
   loop would have flagged this class of bug years ago, and would catch regressions.

## Honest caveats / what is NOT yet proven

- **`committedOffsetRemoved`'s own internal trace was never captured** - it passes every time under DEBUG
  (instrumentation perturbs the timing). Its broker-side hypothesis is inference from *when* it stalls
  (fresh RUNNING PC, first poll, counters at 0), not a captured trace.
- **The drain-path defect (spin + zombie hold) is code-confirmed, but its *share of blame* for each test
  failure is not.** The drain being *slow* in the captured run was partly by-design of
  `TransactionTimeoutsTest` (deliberately slow in-flight work). What generalises is the window's behaviour:
  every closing PC spins a core and holds its partitions until the drain completes. How often that window
  is long enough to matter in production (#857) vs in the suite is unmeasured.
- **Channel 2 (zombie amplification) as `committedOffsetRemoved`'s cause is inference**, consistent with
  the data (fresh group, counters at 0, non-linear onset at forks>cores) but not directly traced.
- The under-served detector only observed **normal in-flight back-pressure**, not stuck-selectable or
  missing work - so the queue/shard layer is exonerated *for the scenarios captured*, which does not prove
  it is healthy in the (uncaptured) `committedOffsetRemoved` stall.
- What **is** firmly established: it is a real stall (0 polls / 120s), not grumpy-specific, not a benign
  timeout, and in the #857 silent-stall family. The exact per-scenario mechanism is **open**.

## Relationship to the in-flight lock-up work

- **#857 / PR #29** (`bugs/857-paused-consumption-multi-consumers-bug`) - "Paused consumption after
  rebalance". Two documented mechanisms (see `docs/BUG_857_INVESTIGATION.md` on that branch): (1) a
  `ConcurrentModificationException` when `close()` races the poll thread; (2) the **silent stall** from
  `numberRecordsOutForProcessing` counter drift. Commit message: provisional fixes **do not eliminate the
  stall** (10-20% of chaos runs still stall); **root cause still open**. The **zombie-drainer partition
  hold** root-caused here is a *third* mechanism in the same family - and note it composes with (1): a
  rebalance touching a draining member runs revoke on the spinning poll thread, right where the CME/commit-
  lock races live. The #857 branch's provisional fixes do **not** appear to address the
  `signalStop`-short-circuit spin (its `ConsumerManager` changes are about thread confinement and CME).
- **#909 / PR #31** (`fix/909-stale-container-replacement`) - stale work container at the same offset after
  rebalance. Adjacent rebalance-correctness work.
- Base master (and therefore PR #75) contains **none** of these fixes, so the sharpened stress suite
  (forked-per-broker integration, PR #68; unit forking, PR #69) is now surfacing the unsolved stall.
- Prior art: `docs/solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`
  (the RebalanceEoSDeadlockTest contention → real #857 deadlock finding, and the "diagnose before masking"
  rule it produced).

## Recommendations

1. **Do not mask.** Leave `committedOffsetRemoved`'s await as-is; do not bump it to go green. A generous
   bound here would hide a real production stall - the exact failure this library exists to prevent.
2. **Keep the grumpy Integration job non-gating** (it already is). The runner is fine; the red is a real
   product stall that also affects GitHub-hosted runs intermittently (via sibling flakes like
   `MultiInstanceMetricsTest`).
3. **Route to #857.** Add `committedOffsetRemoved` and `KafkaSanityTests` as additional reproductions under
   the #857 investigation. To finally capture `committedOffsetRemoved`'s own trace, run it on a box that
   stalls it *without* DEBUG first, then attach the (now committed) `ShardManager` / `WorkManager` /
   `BrokerPollSystem` loggers - the `isSufficientlyLoaded=...` line will show whether the poller is
   throttle-paused (counter/`numberRecordsOutForProcessing` drift) or the broker is simply not serving.
4. **Fix the drain path (own ticket, small first step available).** The minimal fix is option 1 of the
   design review above: stop `signalStop()`-ing the `ConsumerManager` at drain time so the paused 2 s
   `consumer.poll()` acts as the loop's sleep again (as `handlePoll()`'s comment intends) - kills the
   ~10 kHz spin *and* keeps the drainer rebalance-responsive. The fail-safe release guarantee (options 2-3)
   is the deeper design work, best coordinated with #857 since it touches the same close/rebalance paths.
5. **When #857 lands,** re-run this reproduction recipe to confirm the stall is gone (not merely
   timing-hidden), then the `committedOffsetRemoved` await can be revisited on its own merits.

## Reproduction recipe

On a box where `forks/core >= ~1.3` (e.g. 16 forks on <=12 cores):

```bash
./mvnw -Pci -pl parallel-consumer-core -am verify \
  -DskipUTs=true -Dlicense.skip -Dexcluded.groups=performance \
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
  pause/resume, so an inflated `numberRecordsOutForProcessing` (the #857 counter-drift) shows up here as
  "loaded" with no real work.
- **`KafkaClientUtils.buildPc`** - gives every test PC a unique `myId`, so `pc-control-PCn` /
  `pc-broker-poll-PCn` thread names and the `%X{pcId}` MDC make concurrent instances distinguishable in the
  logs (the `myId` hook was otherwise never wired). Safe: `RebalanceEoSDeadlockTest` matches thread names
  with `.contains(...)`.
- **`logback-test.xml`** - commented-out `ShardManager` / `WorkManager` / `BrokerPollSystem` DEBUG loggers,
  grouped and labelled for the #857 stall hunt; uncomment to reproduce the captures above.

## Sources

- Grumpy failure: PR #75 run `30434423800` (failsafe report: `committedOffsetRemoved[1]` ConditionTimeout).
- GitHub-hosted Integration red on the same PR: run `30424305954` - the sibling flake
  `MultiInstanceMetricsTest.sameRegistryCanBeReusedAfterPcInstanceClosed`, not this test.
- `docs/inflight.md` - existing `committedOffsetRemoved` and `MultiInstanceMetricsTest` flake entries.
- `docs/BUG_857_INVESTIGATION.md` (on `bugs/857-paused-consumption-multi-consumers-bug`).
- Code: `parallel-consumer-core/.../integrationTests/state/PartitionStateCommittedOffsetIT.java`;
  `.../internal/AbstractParallelEoSStreamProcessor.java` (`innerDoClose` - drain-then-await sequencing;
  `calculateQuantityToRequest`); `.../internal/BrokerPollSystem.java` (`drain()`, `handlePoll()` and its
  "use this to just sleep" comment, `doPause`/`resumeIfPaused`); `.../internal/ConsumerManager.java`
  (`poll()`'s `while (!shutdownRequested.get())` short-circuit, `signalStop()`).
