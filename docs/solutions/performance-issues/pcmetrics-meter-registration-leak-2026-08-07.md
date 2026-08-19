---
title: "PCMetrics leaked a Meter.Id per registration, and closing it during a failing shutdown orphaned one more"
date: 2026-08-07
category: performance-issues
module: parallel-consumer-core
problem_type: performance_issue
component: metrics
root_cause: memory_leak
resolution_type: code_fix
severity: high
symptoms:
  - "Reporter measured the retained Meter.Id list as 96% of heap after three days of uptime (confluentinc#859)"
  - Every partition rebalance re-registers the offset-encoding meters, growing the tracking list unboundedly
  - No exception and no crash signature - visible only in a heap dump, or as an OOM days later
  - "A meter registered while close() is running stays in the Micrometer registry forever, invisible to any later close()"
tags:
  - memory-leak
  - micrometer
  - metrics
  - rebalance
  - shutdown-race
  - concurrency
---

# PCMetrics leaked a Meter.Id per registration

## Problem

`PCMetrics.registeredMeters` was an `ArrayList<Meter.Id>`, appended to on **every** call to
`getCounterFromMetricDef` / `getTimerFromMetricDef` / `gaugeFromMetricDef` /
`getDistributionSummaryFromMetricDef` - including the calls where Micrometer's registry silently
deduplicated the meter itself and returned the existing one. The registry stayed the right size; the
list tracking it did not.

Two call sites re-registered the same meters in a loop, both by constructing a throwaway
`OffsetMapCodecManager`:

| Path | Frequency | Fixed by |
|---|---|---|
| `PartitionState.tryToEncodeOffsets()` | every offset commit | upstream `confluentinc#892` (merged 2025-10-27) |
| `PartitionStateManager.onPartitionsAssigned()` | every partition assignment, i.e. every rebalance | this PR, `439ea0d8` |

Upstream's fix closed the louder path and is why the issue looked handled. It left the second site
untouched, and left the tracking collection itself unable to resist *any* duplicate registration.

## Symptoms

- The reporter's own measurement on `confluentinc#859`: the retained `Meter.Id`s were **"responsible
  for 96% of the heap size after 3 days of uptime"**.
- Their description of the trigger, from before `confluentinc#892` landed: "whenever parallel-consumer
  attempts to commit its offsets, it creates two new timers and adds them to the `registeredMetrics`
  list... even if the exact tag combination already exists." After `#892`, the surviving trigger is
  partition assignment rather than commit.
- Nothing throws. There is no log line and no failing health signal - the only in-process evidence is
  `registeredMeterCount()` climbing across rebalance cycles.

## What Didn't Work

**Upstream `confluentinc#892` alone was not the fix.** It hoists the codec manager out of the
per-commit path and cuts the *rate* of churn, but it does not stop the collection growing, and it
does not touch the assignment path. The two fixes are complementary and layered, not competing - that
framing was settled in an earlier session and carried into the CHANGELOG (session history).

**The regression tests never ran for four commits.** `d366c8ff`, `bdd1529a`, `9c5c6ecf` and
`a735b3d8` all added or modified tests in a class named `PCMetricsTest859`, which matches none of
Surefire's default include patterns. All six tests for the PR's headline fix sat dormant while the
suite reported green. The rule that came out of it is in `AGENTS.md` → *Testing*: a new test class is
not running until you have watched it run.

**`synchronized(this)` was the first synchronisation fix and was then rejected.** A review found the
`registeredMeters.add()` sites unguarded while `close()` and `remove*()` were synchronised - a
`ConcurrentModificationException` risk once the collection became a `LinkedHashSet`. All nine sites
were wrapped in `synchronized(this)` (`9c5c6ecf`), then a follow-up review flagged that locking on
`this` is fragile: any external holder of the `PCMetrics` reference can contend on the same monitor.
Nothing does today - that was checked - but it was still replaced with a private lock (session
history). A whole-method `@Synchronized` would also have pulled Micrometer's slow `register()` call
inside the critical section, which is why `track()` exists as a separate narrow helper.

**Skipping the late registration was not enough.** `618ec659` added an `isClosed` check in `track()`
that simply returned without adding to the set. That stopped the tracking set being corrupted, and
left the meter sitting in the Micrometer registry permanently - orphaned, and now *invisible* to any
future `close()` precisely because it was never tracked. A testing-persona review flagged the branch
as having zero coverage; an adversarial review reached the same conclusion from the other direction,
noting that `trackingIsSkippedAfterClose` asserted only on the internal set and so passed while the
registry still held the orphan (session history).

**The shutdown race was not suspected up front, and was very nearly backlogged.** The initial plan
was to document it as a known shutdown edge rather than fix it. It was only escalated to a fix
because unrelated shutdown-race work was in flight at the time and might be affected by it (session
history).

**One review nit was declined:** simplifying the `Tuple` parameter passing in `tryToEncodeOffsets`.
That shape is upstream `confluentinc#893`'s own design and the commit is a faithful cherry-pick;
rewriting it would cost cherry-pick fidelity for a cosmetic gain.

## Solution

**1. Make the tracking collection idempotent** (`439ea0d8`):

```java
- private List<Meter.Id> registeredMeters = new ArrayList<>();
+ private Set<Meter.Id> registeredMeters = new LinkedHashSet<>();
```

`removeMetersByPrefixAndCommonTags` was also fixed to remove from `registeredMeters`, not only from
the registry.

**2. Stop the re-registration at its source** (`439ea0d8`, `PartitionStateManager`):

```java
// before - inside onPartitionsAssigned, so once per rebalance:
OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(module); // todo remove throw away instance creation - confluentinc#233
var partitionStates = om.loadPartitionStateForAssignment(assignedPartitions);

// after - constructed once, in the constructor:
private final OffsetMapCodecManager<K, V> offsetMapCodecManager;
...
var partitionStates = offsetMapCodecManager.loadPartitionStateForAssignment(assignedPartitions);
```

**3. Serialise every mutation under one private lock** (`9c5c6ecf` → `a735b3d8`):

```java
private final Object metersLock = new Object();

@Synchronized("metersLock") private void track(Meter.Id meterId) { ... }
@Synchronized("metersLock") public void close() { ... }
@Synchronized("metersLock") private void removeMeter(Meter.Id meterId) { ... }
@Synchronized("metersLock") public void removeMetersByPrefixAndCommonTags(String prefix) { ... }
```

Micrometer's `register()` stays outside the lock in the caller; only the set mutation is guarded.

**4. Undo, don't skip, a registration that lands after close** (`618ec659` → `e9e9518b`):

```java
@Synchronized("metersLock")
private void track(Meter.Id meterId) {
    if (this.isClosed.get()) {
        // Racing a concurrent close(): it has already swept the registry and won't run again, and
        // register() ran outside this lock - so undo the late registration rather than orphan it.
        meterRegistry.remove(meterId);
        log.debug("Metrics subsystem closed; removed late-registered meter {}", meterId);
        return;
    }
    registeredMeters.add(meterId);
}
```

**5. Give the tests an accessor instead of reflection** (`3c1df762`): `registeredMeterCount()`,
itself `metersLock`-guarded, replacing a `Field.setAccessible(true)` helper that had been
copy-pasted into two test classes.

## Why This Works

Making `registeredMeters` a `LinkedHashSet` means the collection is idempotent to re-registration
**from any source, present or future**. That matters more than either call-site fix: there were at
least two such sites, `confluentinc#892` found one, this PR found the other, and nothing guarantees
there isn't a third. The set makes the leak structurally impossible rather than site-by-site absent.

The shutdown race is a genuine TOCTOU window, not a hypothetical. `register()` must run outside
`metersLock` - it writes to a shared, often user-supplied registry and is slow, and holding a lock
across it would block unrelated `close()` calls. So the ordering `register() … track()` is
interruptible by a concurrent `close()`. On the happy shutdown path this is unreachable:
`pcMetrics.close()` runs last in a `finally`, after the worker pool and broker-poll thread are joined
by `closeAndWait()`. On the **failing** path it is reachable - `closeAndWait()` times out because the
poll thread is stuck, or an earlier unguarded step (`processWorkCompleteMailBox`, `drain()`) throws -
and the `finally` still runs `close()` while the broker-poll thread is alive and still firing
rebalance callbacks that register meters (session history, confirmed by tracing the shutdown
sequence).

`close()` and `track()` share `metersLock` and therefore cannot interleave with each other - only
with the unguarded `register()`. That is exactly what makes the compensating `meterRegistry.remove()`
safe: by the time `track()` observes `isClosed`, the sweep is definitively finished and will not run
again, so removing the meter here is the last word rather than a race with the sweep.

## Prevention

- **Assert on the externally-observable state, not the internal bookkeeping.** The weaker guard
  (`618ec659`) passed a tracking-set-count assertion while still leaking the registry entry. The test
  that actually distinguishes "skip" from "undo" is `lateRegistrationAfterCloseIsNotOrphanedInRegistry`,
  which asserts on `registry.getMeters()` directly. Internal and external state diverged, and only the
  external one leaks in production.
- **Prove the fix by reverting exactly it.** The `meterRegistry.remove(meterId)` line was temporarily
  reverted and the regression test confirmed to fail, then restored and confirmed to pass - a control
  arm, not "it compiles and the suite is green" (session history).
- **When a check-then-act guard is added, verify the "act" restores the whole invariant.** `618ec659`
  compiled, ran, corrupted nothing, and was still wrong, because it handled the tracking-set half of
  the invariant and ignored the registry half.
- **Prefer a purpose-built test accessor over reflection.** `registeredMeterCount()` is lock-guarded,
  so it is also safe to call concurrently - raw field reflection is not.
- **A duplicate-registration bug deserves a collection that cannot hold duplicates**, not just a fix
  to the caller that produced them. Chasing call sites is unbounded work; changing the data structure
  is bounded.

## Related Issues

- `confluentinc#859` / astubbs#120 - the leak report, with the reporter's heap measurement.
- `confluentinc#892` - upstream's per-commit-path fix, already on master. Complementary to this one.
- `confluentinc#233` - the standing "refactor `OffsetMapCodecManager`" ask. Both throwaway-instantiation
  fixes are instances of it; `docs/refactoring.md` records what remains.
- astubbs#45 - the predecessor PR, closed stale on 2026-04-21 with no human review. Its five commits
  were recovered from an abandoned worktree and rebased into astubbs#57 (session history).
- `AGENTS.md` → *Testing* - the naming/collection rule from the four commits during which these
  regression tests never ran, and how to prove a test class actually executed.
- `docs/inflight/bug-shutdown-teardown-race.md` - the deeper root cause left open: whether
  `AbstractParallelEoSStreamProcessor.doClose()` should guarantee the broker-poll join before running
  teardown, rather than each subsystem defending itself.
