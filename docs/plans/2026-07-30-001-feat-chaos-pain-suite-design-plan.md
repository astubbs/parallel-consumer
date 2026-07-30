---
title: "feat: Chaos Pain Suite - weakness-targeted stress harness for high-core boxes"
type: feat
status: draft
date: 2026-07-30
---

# feat: Chaos Pain Suite - weakness-targeted stress harness for high-core boxes

## Context

The 2026-07-29/30 silent-stall investigation (PR #80,
`docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md`) established that PC's
remaining bug class is **"alive but not progressing" under contention** - and that each known instance
lives in a different mechanism (drain-window zombie; fresh-PC first-poll starvation; throttle-pause on
assignment; `numberRecordsOutForProcessing` drift; stale containers; CME close races). Today these are
caught *accidentally*, by ordinary integration tests timing out when a loaded box happens to squeeze the
right path. This design makes that pressure **deliberate, seeded, scaled, and self-diagnosing**, sized for
the highcpu runner (Proxmox LXC, 24c/48t, large RAM) rather than the 2-core GitHub gate.

Design goal in one line: **turn "the suite sometimes reddens under load" into "the suite hunts stalls on
purpose and hands you the autopsy."**

## Weakness → scenario matrix (the core of the design)

Each scenario is aimed at a *specific* mechanism surfaced by the investigation, not generic load:

| # | Identified weakness (evidence) | Targeted pain scenario |
|---|---|---|
| W1 | **Drain-window zombie** - closing PC spun + was rebalance-unresponsive while holding partitions (fixed in PR #80, guarded by `BrokerPollSystemDrainTest`). NB: *holding* partitions during drain is correct - it is what lets the drainer finish + commit (no duplicates); the defect was unresponsiveness | **Churn storm**: high-frequency `stop(DRAIN)`/restart cycles across a large fleet; assert every drain completes within `drainTimeout + margin`, group rebalances are never blocked on a draining member, and duplicates stay bounded to the not-yet-committed tail |
| W2 | **Fresh-PC first-poll starvation** (`committedOffsetRemoved` 0-polls-in-120s; still red on the highcpu runner post-fix - mechanism untraced) | **Cold-start stampede**: N brand-new PCs + M fresh topics bootstrapping simultaneously against brokers under induced I/O+CPU pressure; SLO on p99 time-to-first-record |
| W3 | **Throttle-pause stalls** (#29's `pausedForThrottling`-on-assignment fix; `isSufficientlyLoaded` gate) | **Load oscillation**: square-wave produce rates (burst→idle) interleaved with rebalances, forcing pause/resume cycling; assert poller never stays paused while buffers are empty (our `WorkManager.isSufficientlyLoaded` DEBUG is the probe) |
| W4 | **`numberRecordsOutForProcessing` drift** (#857 doc: counter inflation → `requesting: 0` forever) | **Revoke-under-work**: rebalance storms while user functions hold in-flight work with heavy-tailed latencies (some > session timeout); assert work-request `delta` recovers after every revoke (our `ShardManager` under-served detector is the probe) |
| W5 | **Rebalance-during-drain composition** (revoke lands on drainer's poll thread → #29's CME/commit-lock territory) | **Timed collision**: chaos conductor deliberately triggers member joins *while* another member is mid-drain (now possible: post-fix drainers participate in rebalances) |
| W6 | **GC-pause / process-freeze class** (untested today; production-realistic trigger for session timeout + zombie perception) | **Freeze-thaw**: `SIGSTOP`/`SIGCONT` a PC's JVM for durations straddling `session.timeout.ms` and `max.poll.interval.ms`; assert clean fencing + recovery, no permanent partition loss |
| W7 | **Broker-side degradation** (fresh compacting broker not serving; W2's suspected cause) | **Toxiproxy layer**: latency/jitter/bandwidth-cap/timeout toxics between PCs and brokers (Testcontainers-native); plus broker restarts mid-flight |
| W8 | **Hard-kill offset safety** (close paths are tested; `kill -9` isn't) | **Assassin**: SIGKILL random instances (no close path at all); assert at-least-once holds (no lost offsets) and partitions reassign within bound |

## Harness architecture

Builds on existing infra (DRY): `ManagedPCInstance` + chaos-monkey loop from #29's branch
(`MultiInstanceRebalanceTest`), `KafkaClientUtils`, `BrokerIntegrationTest`, the PR #80 diagnostics
(per-PC `myId` log attribution, under-served detector, throttle-inputs log).

Four new pieces:

1. **ChaosConductor** - replaces the ad-hoc `Math.random()` monkey with a **seeded** schedule
   (`-Dchaos.seed=`; seed logged in the run header). Actions are drawn from a per-scenario weighted
   taxonomy (stopDrain, stopNoDrain, kill9, freezeThaw, toxic(latency|bandwidth|timeout), brokerRestart,
   produceBurst, produceIdle, memberJoin, memberLeave). Deterministic replay: same seed + same scenario =
   same action timeline (modulo timing jitter), so a stall found at 3am is reproducible at 9am.
2. **ProgressProbe** - first-class progress SLOs instead of one big await:
   - per-instance: time-to-first-record, records/sec watermark
   - group-level: end-to-end completion, no-progress windows (the #857 "11 seconds" check, generalised)
   - membership: via AdminClient - **no rebalance may stay blocked on any member beyond T** (the direct
     "zombie member" assertion this investigation lacked). Deliberately keyed on
     *protocol-unresponsiveness*, NOT on "holds partitions with zero consumption" - a legitimately
     draining member holds its assignment while finishing + committing (that is drain's purpose; see the
     option-3 rejection in the silent-stall report) and must not false-positive; drainers are exempt
     within `drainTimeout`
   - drain: every `stop(DRAIN)` completes within `drainTimeout + margin`
   - correctness ledger: produced vs consumed accounting across the fleet - **no lost records ever**
     (at-least-once), and duplicates bounded to the in-flight-uncommitted tail of disturbed
     drains/revokes (a healthy undisturbed drain contributes zero duplicates)
3. **DiagnosticBundle** - on any SLO breach, *before* failing: flip the stall-hunt loggers
   (`ShardManager`/`WorkManager`/`BrokerPollSystem`) to DEBUG **at runtime** (logback supports it) on the
   suspect instance, capture 30s, then dump: jstack of every PC JVM/thread pool, AdminClient
   `describeConsumerGroups` + lag, the chaos action timeline, and the tail of each instance's log. The
   bundle IS the test failure artifact - the 90-minute log-archaeology this investigation needed becomes
   a zip attached to the red run.
4. **ResourcePressurizer** - makes the "loaded box" trigger first-class instead of incidental:
   configurable background CPU burn (duty-cycled, pinned) and page-cache pressure during selected
   scenario windows. This is what made fork16-on-12-cores the best reproducer; the suite should own that
   dial rather than depending on whatever else the box is doing.

## Scale matrix (exploiting 24c/48t + big RAM)

| Dimension | Today (#29 chaos test) | Pain suite (highcpu runner) |
|---|---|---|
| PC instances | 12 | 32-96 (sweep) |
| Partitions | 80 | 512-1024 |
| Brokers | 1 container | 3-node cluster (+ Toxiproxy in front) |
| Messages | 500k | 10-50M (sized to RAM page cache; payload mix 100B-100KB) |
| Consumer groups | 1 | 2-4 concurrent (cross-group broker contention) |
| Duration | ~3-5 min | soak mode: 30-120 min nightly |

## Execution model

- New package `...integrationTests.chaostests`, `@Tag("chaos")` - excluded from all default/PR suites
  (like `performance`).
- **highcpu runner `workflow_dispatch` workflow** (`chaos-pain.yml`): inputs = scenario, seed, scale-preset,
  duration; plus a nightly schedule running the full matrix at default scale. Never PR-gating; failures
  file the DiagnosticBundle as an artifact + a summary comment.
- Local dev mode: same suite, `scale=laptop` preset (the fork16-equivalent pressure via
  ResourcePressurizer instead of core-count).

## Calibration: the suite must catch known bugs (test the test)

We possess something rare: **a reproducible, understood stall** (the PR #80 zombie-drain defect). Before
trusting the suite, run its W1/W5 scenarios against the *pre-fix* commit (`2eca71fa`) - the suite MUST go
red with a bundle that names the drain window. Same for W4 once #29's counter-drift reproducer exists.
A chaos suite that has never caught a known-planted bug is decoration; this one starts life calibrated
against two. (Repeatable via the uber-arm technique from this experiment: arms differing by one fix.)

## Phased delivery (skateboard first)

- **Phase 1 (skateboard)**: seeded ChaosConductor + ProgressProbe's zombie-member assertion bolted onto
  the *existing* `MultiInstanceRebalanceTest` harness, `@Tag("chaos")`, one `workflow_dispatch` job on
  highcpu runner at today's scale. No new chaos actions beyond stopDrain/stopNoDrain/join/leave. Calibrate
  against `2eca71fa` (W1).
- **Phase 2**: DiagnosticBundle (runtime DEBUG flip + jstack + admin dump) and scale presets; W2
  cold-start stampede + W4 revoke-under-work scenarios.
- **Phase 3**: Toxiproxy (W7), freeze-thaw (W6), assassin (W8), ResourcePressurizer, multi-group, soak
  mode, nightly schedule.

## Dependencies / sequencing

- **#29 (`bugs/857`) merging first is strongly preferred**: `ManagedPCInstance`, the re-enabled chaos
  test, and `ThreadConfinedConsumer` are its; building the suite on the uber-branch composition avoids
  double-porting. The uber experiment (PR #80) already validates the pieces compose.
- PR #80's diagnostics + drain fix are prerequisites for W1/W3/W4 probes (they are the probes).
- Toxiproxy adds a test-scope dependency (`org.testcontainers:toxiproxy`) - Phase 3 only.

## Risks

| Risk | Mitigation |
|---|---|
| Suite is flaky-by-design (chaos ≠ deterministic assertions) | Assert SLOs and invariants (zombie-member, drain-bound, at-least-once), never exact timings; seeded schedules for replay; failures ship bundles not just stack traces |
| highcpu runner monopolised / thermal throttling skews runs | Nightly schedule off-hours; `workflow_dispatch` concurrency group of 1; ResourcePressurizer makes pressure explicit rather than load-dependent |
| Becomes a second flaky-CI source | Never PR-gating; red = investigation food with autopsy attached, tracked in inflight like #857 findings |
| Big scale masks signal in noise | Every scenario also runs at small scale first (bisectable); one weakness per scenario, not one mega-scenario |
