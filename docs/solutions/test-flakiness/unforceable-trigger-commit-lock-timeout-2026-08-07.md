---
title: "A test that awaits a trigger it cannot force: TransactionTimeoutsTest.commitTimeout waited 35s for a commit-lock timeout that was never guaranteed to fire"
date: 2026-08-07
category: test-flakiness
module: parallel-consumer-core
problem_type: flaky_test
component: testing
symptoms:
  - "TransactionTimeoutsTest.commitTimeout[1]: ConditionTimeoutException after 35s on `assertThat(pc).isClosedOrFailed()`, one sighting on CI"
  - "The whole 35s window is SILENT - no ERROR, no close-path logging, nothing from the PC instance"
  - "A passing run of the same parameter takes ~2.9s; there is no middle ground, it either fires in seconds or eats the full 35s"
  - "The CI run reports conclusion `success` - it is `run_attempt: 2`, and attempt 2 passed on the byte-identical tree"
root_cause: awaited_trigger_unreachable_in_some_interleavings
resolution_type: test_fix_deterministic_trigger_plus_guard
severity: low
status: "SOLVED - test-side fix merged to master in astubbs#220 (rebase-merged 2026-08-07, `c429d8b6`). PC is healthy; no product defect. Which of the two paths CI hit was NOT established (no DEBUG in that job); the fix closes both."
last_updated: 2026-08-07
related_prs:
  - "astubbs#220 - this fix, plus the investigation rules and probe correction added to AGENTS.md; astubbs#272 has since moved the probe thresholds to docs/testing.md and the settling method to docs/investigating.md"
  - "astubbs#110 - the SIBLING flake on this same producerTransactionLock (ProducerManagerTest), fixed 2026-08-03; source of the control-arm method"
  - "astubbs#86 - introduced AmbientProbeExtension/ProgressProbe, whose 'probe clean' verdict this work qualified"
  - "astubbs#98 - the backpressure test that only passed by racing its own setup (the sibling rule, opposite direction)"
  - "astubbs#80 - drain-path zombie/busy-spin fix; its signature is what this failure was wrongly matched against"
  - "astubbs#115 - the nudge race, the other flake-family member that was solved and left"
  - "astubbs#68 - the forked per-broker integration work that produced the flake-family roster"
  - "astubbs#219 - the unrelated PR this surfaced on, excluded by tree hash (see What Didn't Work)"
related:
  - "docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md - the only prior investigation into this same lock; its §11 control-arm method is what settled this one"
  - "vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md - SIBLING rule, opposite failure direction (see Related)"
  - "docs/inflight/test-load-tightness-flakes.md - the family this was wrongly filed under"
upstream:
  - "confluentinc#803 / astubbs#44 - a REAL production commit-lock timeout, but a different bug (rebalance-vs-normal-commit race)"
  - "confluentinc#809 / astubbs#175 - sporadic ConsumerOffsetCommitter timeouts, likely the same defect as confluentinc#803"
  - "confluentinc#833 / astubbs#177 - PC exits on InternalRuntimeException(Timeout), likely the same cluster"
tags:
  - flaky-tests
  - awaitility
  - unforceable-trigger
  - transactional-commit
  - commit-lock
  - test-design
  - ci
---

# A test that awaits a trigger it cannot force

## Problem

`TransactionTimeoutsTest.commitTimeout[1]` waits 35 seconds for PC to die from a commit-lock timeout.
It failed on CI having waited the full 35s. PC was **healthy the whole time** - not stalled, not
deadlocked, no product defect. The test was waiting for a *consequence* whose *trigger* it had only
made possible, never guaranteed.

## Symptoms

- `org.awaitility.core.ConditionTimeoutException ... expected to be 'closed or failed'` after 35s,
  at the `await()` in `commitTimeout`.
- **35 seconds of complete silence** from the PC instance in the job log. No close path, no error.
- Binary timing: a healthy run of param `[1]` finishes in ~2.9s. There is no gradual degradation.
- The run's overall conclusion is `success`, because it is `run_attempt: 2` and the retry passed on
  the identical tree.

## What Didn't Work

This section is the valuable one - five plausible reads, all wrong, and why.

1. **"It's a stall, like confluentinc#857 / the astubbs#80 drain zombie."** The reasoning was: a 1s
   mechanism cannot stretch past 35s, so PC must never have shut down - therefore a stall. The first
   half is right and the conclusion does not follow. A trigger that *never fires* is externally
   identical to one that stalls, and they have opposite fixes. This framing sends you into drain and
   rebalance code, which is a dead end here.

2. **"No ERROR was logged, so the failing record never ran."** Refuted by reading the code:
   `FakeRuntimeException extends PCRetriableException`, and `AbstractParallelEoSStreamProcessor`
   routes a `PCRetriableException` cause to **DEBUG**, not ERROR. Silence is the expected behaviour.
   Absence of the line proves nothing.

3. **"The commit interval pushes the attempt too late."** Tested and **refuted**: 0/6 failures at
   `commitInterval=3s`. `requestCommitAsap()` sets `isCommandedToCommit`, which short-circuits the
   interval check entirely - the interval was never gating this scenario.

4. **Brute-force soaking.** 0/12 at stock settings under load. Repetition cannot reproduce a window
   this narrow at any practical count. Only a controlled experiment could.

5. **"The ambient probe says clean, so the fault is in the test."** The verdict was **vacuous**.
   `ProgressProbe` needs `LAG_STAGNATION_MIN_LAG` (50) of lag sustained past `LAG_STAGNATION_BOUND`
   (150s), or `REBALANCE_DWELL_BOUND` (15s). A 15-record test failing in 35s cannot trip either
   detector. See [`docs/testing.md`](../../testing.md) for the standing caveat - **do not duplicate
   it here**.

6. **Searching CI history filtered on `conclusion == "failure"`.** Structurally blind to this failure
   class: a run whose first attempt fails and is retried green reports `success`. The scan has to walk
   first-attempt job results.

7. **"The PR it surfaced on must have caused it."** It surfaced on an unrelated tree-wide
   issue-reference migration. Excluded by evidence rather than argument (session history): that branch
   had been re-cut, so two heads shared a byte-identical tree hash, and the **same tree passed at
   00:00:18Z and failed at 00:41:04Z**, 40 minutes apart. Worth copying as a technique - a tree-hash
   comparison settles "did my change cause this" in one command, where reading the diff only ever
   yields an opinion.

**A re-run that passes is not a resolution** (session history). The job was re-run on the identical
tree and went green, which establishes non-determinism and nothing else. It says the failure is not
reproducible on demand; it says nothing about why it happened once.

Also ruled out on principle: **widening a timeout**. It cannot fix path 2 below - there is no deadline
to widen - and `docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md` §5 records timeouts already being widened once on this same
lock, with the flake surviving.

## Solution

No production code changed. The test now *constructs* the overlap its trigger requires.

The trigger is a `TimeoutException` from `ProducerManager.acquireCommitLock`, which needs the slow
record to be holding the produce **read** lock at the moment the controller is inside its **write**
lock acquisition. Two coordination points guarantee that:

```java
// 1. Signal the moment the controller actually enters commit-lock acquisition.
//    Wired through PCModule - the DI - rather than around it. The cache matters: PCModule#producerManager()
//    caches, and a fresh ProducerManager would mean a fresh producerTransactionLock.
setup(new PCModule<>(options) {
    private ProducerManager<String, String> instance;

    @Override
    protected ProducerManager<String, String> producerManager() {
        if (instance == null) {
            instance = new ProducerManager<>(producerWrap(), consumerManager(), workManager(), options()) {
                @Override
                protected void preAcquireOffsetsToCommit() throws TimeoutException, InterruptedException {
                    // phase-one happy-path commits reach here too, and must not open the latch
                    if (slowRecordHoldsProduceLock.getCount() == 0) {
                        commitLockAcquisitionAttempted.countDown();
                    }
                    super.preAcquireOffsetsToCommit();
                }
            };
        }
        return instance;
    }
});
```

```java
// 2. Time the sleep FROM THE ATTEMPT, and guarantee a success lands during the hold.
if (offset == OFFSET_TO_GO_SLOW) {
    slowRecordHoldsProduceLock.countDown();
    awaitQuietly(commitLockAcquisitionAttempted);   // <- sleep now starts at the attempt
    ThreadUtils.sleepQuietly(1000 * multiple);
} else if (offset == OFFSET_TO_ERROR) {
    throw new FakeRuntimeException("fail");
} else if (offset == OFFSET_TO_MARK_DIRTY) {
    awaitQuietly(slowRecordHoldsProduceLock);       // <- guarantees wm.isDirty() during the hold
}
```

```java
// 3. A guard, so a future regression fails in one line instead of returning as a 35s flake.
Truth.assertWithMessage("controller must have attempted the commit lock while the slow record held the produce lock")
        .that(commitLockAcquisitionAttempted.getCount())
        .isEqualTo(0);
```

`awaitQuietly` deliberately does **not** throw on its own timeout: an exception inside the user
function would be caught by PC as a user-function failure and retried, turning a diagnosable setup
problem into noise.

**The 35s await and `isClosedOrFailed` are untouched. No assertion was weakened.**

## Why This Works

Two independent paths defeated the old test, and the fix closes both.

**Path 1 - margin.** With `allowEagerProcessingDuringTransactionCommit=false`,
`ParallelEoSStreamProcessor` takes the produce read lock *before* running the user function, so the
sleep holds it. The controller's `writeLock.tryLock(commitLockAcquisitionTimeout)` is **granted rather
than timing out** whenever:

```
(T - S) + commitLockAcquisitionTimeout  >=  sleep
```

where `S` = when the record took the lock and `T` = when the commit attempt began. At stock settings
(2s sleep, 1s lock timeout) that leaves 1000ms of headroom, usable exactly **once** - after the sleep
ends there is no contention left, so the remaining ~33s of the await is dead time. Timing the sleep
from the attempt makes the margin a property of the test, not of the scheduler.

**Path 2 - no attempt at all.** `maybeAcquireCommitLock` (`AbstractParallelEoSStreamProcessor.java:956-957`) computes:

```java
final boolean shouldTryCommitNow = isTimeToCommitNow() && wm.isDirty() && !isRebalanceInProgress.get();
```

The no-arg `setDirty()` - the mark-true path, `PartitionState.java:232-234` - has **exactly one
caller**: `PartitionState#onSuccess` (`:258-265`, call at `:265`). `onFailure` is a no-op (`:268-270`).
(There is a second `setDirty(false)` overload used by `setClean()` at `:226-228`; it clears the flag
and is not a second way to set it.) So if every other record of the batch succeeds *before* the slow
one starts, nothing marks dirty during its hold, no commit is attempted, and no timeout can fire - not
late, simply never. `requestCommitAsap()` cannot rescue this, because `isDirty` is AND-ed into the
gate, not OR-ed. Deferring one record's success into the hold guarantees the controller has something
to commit.

## Evidence

Established by controlled experiment, per the method in `docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md` §11 - a fix that
works is not evidence of the cause:

| Arm | Result |
|---|---|
| stock, before the fix | 0/12 fail |
| `commitLockAcquisitionTimeout` 2.5s vs 2s sleep | **6/6 fail** |
| `commitInterval` 3s - hypothesis **refuted** | 0/6 fail |
| latch removed + 1500ms delayed commit attempt | **4/4 fail** |
| latch present + *identical* 1500ms delay | **0/4 pass** |
| fixed, stock settings, heavy load | 0/8 fail |
| whole class, fixed, heavy load | 0/4 fail |
| guard deliberately broken | **2/2 fail, on the guard's own message** |

The 6/6 arm carries its own control: param `[2]` (50s sleep) fired correctly in the same JVM at the
same instant, so machine speed is excluded by construction rather than by argument. The DEBUG timeline
from a failing run shows the mechanism directly - `T-S = 3ms`, read lock released at `S+2005ms`, and
`Commit lock acquired.` **granted**, not timed out.

**Not established:** which path the CI failure actually hit. That job had no DEBUG logging.

## Prevention

1. **Before writing `await(...).untilAsserted(consequence)`, ask whether the test can *force* the
   trigger in every interleaving.** If the trigger depends on two threads coinciding, or on a flag the
   test does not set, the test is asserting on a race. Its failure rate is then a function of machine
   load, and no timeout is long enough - path 2 here has no deadline at all.
2. **Force the coincidence with a latch at the exact production hook**, not with sleep arithmetic.
   Override the real method (`preAcquireOffsetsToCommit`) through the DI, so the test observes the
   thing it claims to.
3. **Verify any determinism guard by negative control.** Break the mechanism it guards and confirm the
   test fails on *that assertion's message*. An assertion nobody has seen fail is decoration.
4. **Treat silent logs as inconclusive, not exculpatory.** Check what level the failure path logs at
   before using "nothing was logged" as evidence.
5. **Check a detector can structurally fire before trusting a clean result** - thresholds against the
   scenario's scale and duration.
6. **When mining CI history for a flake, walk first-attempt results.** A retried run reports `success`.

## Other instances of this defect (swept 2026-08-07)

The class is **a test awaiting a consequence whose trigger it cannot force**. Two greppable proxies
find candidates: *sleep-as-synchronisation* in integration tests, and *awaits on a failure outcome*
(as opposed to `failFast(...)` guards, which are the opposite polarity and safe - most
`isClosedOrFailed` uses in this repo are guards).

**Explicitly NOT an instance - `TransactionTimeoutsTest.produceTimeout`.** This is the most important
line in the section, because it is the nearest sibling: same file, same lock, and still listed as an
open flake in `docs/inflight/test-load-tightness-flakes.md`. It **latches its trigger and keeps a real
margin** - the injected `sendOffsetsToTransaction` counts its latch down while already holding the
commit write lock and then sleeps 5s, and the worker attempts the produce read lock at `latch + 1s`
against a 2s deadline. Its "tight assertion" classification stands. **Do not "fix" it the way
`commitTimeout` was fixed** - it does not have this defect.

Relatives found, neither the same defect:

| Test | What it is |
|---|---|
| `DrainCloseTest` (`:57`, `:60`) | Closest relative. Two bare `sleep(2000)`/`sleep(5000)` sequence a close-drain race. Not this defect, because `closeDrainFirst` is *commanded* - which is itself the smell: the await's `isClosedOrFailed` disjunct is guaranteed to fire, so the await is pure synchronisation and the real check is the `assertEquals` after it. |
| `RetriesTest` (`:78-80`) | Candidate. `throwOnHeader` and `checking` are flipped on 3s/2s sleeps against a running consumer; the test's premise holds only if the consumer got there inside those windows. |

Checked and dismissed: the chaos and probe pacing loops, `OffsetCommittingSanityTest:135` (an explicit
`JUST_SLEEP` check mode, not a synchronisation), `RebalanceEoSDeadlockTest:90` (the injected delay *is*
the mechanism under test), `TransactionMarkersTest:160` (a deliberately blocked record).

This is a point-in-time sweep, not a standing guarantee - re-run it if the class comes up again.

## This test has a prior, different flake history (session history)

`TransactionTimeoutsTest` was already known as a CI "repeat offender" in late July 2026 - **but for a
different mechanism**. Then, cross-class broker contention was spuriously tripping its *intentional*
1s/2s lock timeouts, and the fix was **forked-per-broker test isolation** (astubbs#68), not any change
to the timeout values. That history matters twice over:

- It is the reason this test's timeouts are deliberately tight, and why "just raise them" has been
  rejected before.
- It is a *third* distinct way this one test can go red. Contention tripping a real timeout, the
  trigger never firing, and a genuine product stall all present as the same red X. Classify before
  touching, every time.

**Searching for prior sightings has a false-positive trap.** A keyword search for `commitTimeout`
across sessions returns many hits that are the **`offsetCommitTimeout` config field**, or the title of
an unrelated PR, rather than this test method. Checked and discounted on that basis: this remains a
**single sighting**, not a recurrence.

## Related

- `docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md` - the only prior investigation
  into this same `producerTransactionLock`. Different defect (the test released the produce lock too
  early, opening a window production never opens), same discipline. Its §11 control-arm method is now
  promoted into [`docs/investigating.md`](../../investigating.md).
- `vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md` - **a sibling rule, not the same
  one.** That doc's hazard is an await satisfied *too early* by a vacuously-true condition (a
  false-pass). This one's hazard is an await whose trigger may *never fire* (a false-fail). Same
  symptom, opposite direction; neither fix would have solved the other.
- `pc-silent-stall-under-contention-2026-07-29.md` - names `TransactionTimeoutsTest` among the tests
  that stall under `forkCount=16`, but that mechanism (drain-path busy-spin) is unrelated to this one.
  Useful as a signature to **rule out**.
- `docs/inflight/test-load-tightness-flakes.md` - the family this was wrongly filed under, and the
  warning left for the members still in it.
- confluentinc#803 / astubbs#44 - a real production commit-lock timeout, maintainer-confirmed as a
  rebalance-vs-normal-commit race, with confluentinc#809 and confluentinc#833 likely the same defect.
  Related area, different bug. It is why this test is worth keeping sharp. **Its fix is claimed by the
  still-open astubbs#29**, described there as the same root cause - so anyone working this area should
  read that PR before assuming the production bug is unowned.

### What the prior-art search actually covered

Recorded so the next reader knows where the gaps are rather than assuming none.

- **Merged PRs**, by the files they touch (`TransactionTimeoutsTest`, `ProducerManager`,
  `AmbientProbeExtension`, `ProgressProbe`) - this is what surfaced astubbs#110, astubbs#86 and
  astubbs#98, none of which a symptom search would have found. Searching *open* PRs alone is a
  collision check, not prior art.
- **Issues, all states**, fork and upstream. Nothing matches this mechanism; the only commit-lock
  issues are the confluentinc#803 cluster above, which is a production rebalance race, not this.
- **Not covered:** the `highcpu` and `quarantine` CI lanes were not scanned for prior sightings of
  this test, only the main `CI` workflow (120 failed runs, plus first-attempt results across 400 runs
  - needed because a retried run reports `success`). One sighting is all that search found.
