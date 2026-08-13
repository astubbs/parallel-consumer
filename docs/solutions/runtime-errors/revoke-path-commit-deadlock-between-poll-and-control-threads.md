---
title: "Commit-path AB-BA deadlock between the poll and control threads on partition revoke"
date: 2026-08-07
category: runtime-errors
module: parallel-consumer-core/internal
problem_type: runtime_error
component: background_job
severity: critical
symptoms:
  - "Consumption stops entirely after a rebalance with multiple consumers in the group - no records processed, no exception, no error log"
  - "Poll thread parks in `onPartitionsRevoked` on `synchronized (commitCommand)` and never returns from the rebalance callback"
  - "Control thread holds that same monitor inside a blocking `commitAndWait()`, which can only complete once the poll thread services `maybeDoCommit()`"
  - "AB-BA cycle: neither thread can make progress, and the consumer group appears permanently paused"
  - "Only reachable in PERIODIC_CONSUMER_SYNC - the reproducer test runs a transactional mode where this cycle cannot occur"
root_cause: thread_violation
resolution_type: code_fix
related_components:
  - AbstractParallelEoSStreamProcessor
  - BrokerPollSystem
  - ConsumerOffsetCommitter
  - ConsumerManager
  - ThreadConfinedConsumer
  - RebalanceEoSDeadlockTest
tags:
  - deadlock
  - ab-ba-lock-ordering
  - rebalance
  - offset-commit
  - trylock
  - poll-thread
  - unverified-fix
  - issue-857
---

# The commit-path AB-BA deadlock behind confluentinc#857

> **Status: diagnosis verified, fix implemented, fix UNPROVEN and probably insufficient.** The lock
> cycle described below is confirmed by reading the code on both `master` and the branch. The
> `tryLock()` change has **never been observed working**. PR astubbs#29 is a **draft**, unmerged,
> with `Integration Tests`, `Performance Tests` and `PR Checklist` red, and the one test written to
> prove this fix - `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` - fails **5 of 5 repetitions** in
> CI. Do not read this document as a fix report. It is a diagnosis report with an unlanded candidate
> fix attached.
>
> **Read the mode caveat before anything else.** The cycle described here can only close in
> `PERIODIC_CONSUMER_SYNC`. The reproducer test runs `PERIODIC_TRANSACTIONAL_PRODUCER`, where the
> cycle **cannot occur at all** - and where a *different*, unbounded block on the revoke path
> (`:434-435`) is untouched by this fix. See "The mode caveat" below. This document therefore
> describes a real deadlock that its own headline test does not reproduce.

## Problem

Upstream confluentinc#857 ("paused consumption after rebalance with multiple consumers") is one
reported symptom sitting on top of **three independent defects**. Two have landed on this fork:

- **astubbs#100** - a mid-rebalance commit threw `RebalanceInProgressException`, nothing caught it,
  and it permanently killed the broker-poll thread.
- **astubbs#80** - a draining consumer stopped calling `consumer.poll()`, producing a ~10 kHz
  busy-spin and a rebalance-unresponsive member that zombie-held its assignment.

This document is the **third**: a genuine AB-BA deadlock between the two PC threads on the offset
commit path.

### The cycle, as it exists on `master` today

PC runs two long-lived threads. `BrokerPollSystem.controlLoop()` names itself `pc-broker-poll`
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/BrokerPollSystem.java:140`)
and, in a single loop body, does two things that matter here: it calls `handlePoll()` (`:150`),
which is where `consumer.poll()` fires the rebalance callbacks, and it calls `maybeDoCommit()`
(`:152`), which is the **only** producer of commit responses. The controller thread names itself
`pc-control`
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java:900-901`)
and calls `commitOffsetsThatAreReady()` from its own loop (`:955`) and from `innerDoClose` (`:786`).

On `master`, `commitOffsetsThatAreReady()` wraps its whole body in the monitor of the
`commitCommand` flag:

```java
// git show origin/master:.../AbstractParallelEoSStreamProcessor.java  (lines 1308-1310)
protected void commitOffsetsThatAreReady() throws TimeoutException, InterruptedException {
    log.trace("Synchronizing on commitCommand...");
    synchronized (commitCommand) {
```

and `onPartitionsRevoked` - which runs on `pc-broker-poll`, inside `poll()` - calls straight into it
(`origin/master:...:425`). So:

1. **`pc-control`** enters `commitOffsetsThatAreReady()`, takes the `commitCommand` monitor, and
   calls `committer.retrieveOffsetsAndCommit()`. In the consumer-commit modes the committer is the
   broker poll system (`AbstractParallelEoSStreamProcessor.java:323,326`), which routes to
   `ConsumerOffsetCommitter.commit()`
   (`.../internal/ConsumerOffsetCommitter.java:72-85`). `pc-control` is not the owning thread
   (`isOwner()`, `:120-122`; ownership is claimed by the poller at `BrokerPollSystem.java:147`), so
   in `PERIODIC_CONSUMER_SYNC` it takes the `commitAndWait()` branch (`:141-165`), which enqueues a
   request and then **blocks** on `commitResponseQueue.poll(...)` (`:155`).
2. The only thing that can satisfy that wait is `ConsumerOffsetCommitter.maybeDoCommit()`
   (`:174-188`), which is called exclusively from the poll loop (`BrokerPollSystem.java:152`).
3. Meanwhile **`pc-broker-poll`** is inside `poll()`, inside `onPartitionsRevoked`, blocked trying
   to enter `synchronized (commitCommand)` - held by `pc-control`.

`pc-control` waits on `pc-broker-poll` to service the commit queue. `pc-broker-poll` waits on
`pc-control` to release the monitor. Neither side has a timeout that resolves the cycle in a useful
way, and the member stops responding to the group coordinator while it is stuck. That is the
"paused consumption after rebalance" the users report.

The monitor is genuinely two-purpose on `master`: `commitCommand` is an `AtomicBoolean` flag
(`:198`) whose set/get/clear paths are also `synchronized` on the same object. Guarding the
*execution* of a commit with the monitor of a *flag* is what put a blocking, cross-thread,
round-trip operation inside a lock that a callback on the other thread must also take.

## Symptoms

- Consumption stops on some or all partitions after a rebalance. No exception, no crash, no error
  log. Lag grows without bound; only a restart clears it. This is the shape reported upstream in
  confluentinc#857 and mirrored on this fork as astubbs#119.
- Under a contended broker (thread-parallel integration tests on one shared Kafka),
  `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` fails with `Rebalance did not finished`. That test
  was written for confluentinc#541 and re-surfaced here: it forces the window open by delaying
  `pc-control` by ~3-4 s *before* it enters `commitOffsetsThatAreReady()` while a second consumer joins the
  group (`.../integrationTests/RebalanceEoSDeadlockTest.java:86-99,117`).
- Thread dumps during the stall show `pc-broker-poll` parked on a monitor inside
  `onPartitionsRevoked` and `pc-control` parked in `LinkedBlockingQueue.poll`.
- The group's rebalance never completes for the stuck member, so the *other* consumers in the group
  are held up too - which is why the symptom appears as "multiple consumers" rather than one.

## What Didn't Work

**Chasing it as one bug.** For a long stretch this was treated as a single defect with one root
cause. It is three, and each of the three produces the same user-visible sentence: "after a
rebalance, consumption stops". astubbs#100 (unhandled `RebalanceInProgressException` killing the
poll thread) and astubbs#80 (draining consumer that stops polling) both had to be found and landed
*separately* before this one could even be looked at cleanly, and both were briefly mistaken for
"the" fix. The verification that finally separated them was a control-arm experiment recorded in
`docs/inflight/bug-857-family.md`: astubbs#29 and astubbs#31 were shown *not* to fix the drain defect, and an
uber-branch showed the astubbs#80 stack composes with the others rather than duplicating them. A
symptom that decomposes is the normal case for concurrency bugs in a two-thread system, and
"one issue, one root cause" was the wrong prior.

**Also worth stating: the upstream fix that looked adjacent was not this.** confluentinc#882 fixed
stale work-container cleanup in `ProcessingShard.getWorkIfAvailable()`. It is correct and necessary,
and it addresses stale containers blocking new work after a *clean* rebalance. It does not touch
the lock cycle (`docs/BUG_857_INVESTIGATION.md`).

**Making the test suite green by isolating the tests.** astubbs#68 reworked integration testing to
fork one JVM (and one TestContainers broker) per fork, `-DforkCount=4`
(`.github/workflows/maven.yml:86`). The suite became fast and reliable. But
`RebalanceEoSDeadlockTest.noDeadlockOnRevoke` only fails when the broker is *contended* - the
deadlock needs the window where `pc-control` is slow inside a commit while a rebalance lands. Giving
each test an uncontended broker removes the window, so the test goes green **without the bug being
fixed**. The repo's own ledger states this plainly:

> "astubbs#68 made the integration suite reliable by *forking* per broker (`forkCount=4`), which
> sidesteps the deadlock rather than proving it gone" - `docs/inflight/bug-857-family.md`

Note the honest tension in the record: the CI config comment at `.github/workflows/maven.yml:82-85`
argues the opposite, "without masking anything (each test runs on an uncontended broker)". Both were
written in good faith. The reconciliation is that forking does not mask *test-tightness* flakes, but
it does remove the contention that this particular main-code bug needs to manifest - which is why
`docs/solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md:152`
records the deliberate decision **not** to loosen timeouts to go green, and defers a Step 2:
re-run `-Dparallel-tests=true` on a shared broker *after* astubbs#29 lands, as the real proof. That
Step 2 has not been run. Until it is, nothing has demonstrated the deadlock is gone.

**Reading a green suite as evidence.** The generalisable failure here is that "the test passes now"
was true for a reason that had nothing to do with the code under test.

## Solution

*(Implemented on branch `bugs/857-paused-consumption-multi-consumers-bug` / PR astubbs#29. Draft.
Not merged. Not verified - see the verification status at the end of this section, which is the part
that matters.)*

Split the two jobs the `commitCommand` monitor was doing. A dedicated `ReentrantLock` now guards
commit *execution*; the `commitCommand` monitor keeps guarding only the flag itself
(`AbstractParallelEoSStreamProcessor.java:1555,1589,1595`).

```java
// AbstractParallelEoSStreamProcessor.java:204-208
/**
 * Lock for offset commit operations. Replaces synchronized(commitCommand) for commit execution
 * to allow tryLock() semantics in rebalance callbacks, preventing the deadlock in confluentinc#857.
 */
private final java.util.concurrent.locks.ReentrantLock commitLock = new java.util.concurrent.locks.ReentrantLock();
```

The controller path takes it blockingly, exactly as before, just on the new lock
(`:1373-1383`). The revoke path - the one running on `pc-broker-poll` inside `poll()` - no longer
blocks. `onPartitionsRevoked` calls `tryCommitOffsetsOnRevoke()` (`:445`), which is:

```java
// AbstractParallelEoSStreamProcessor.java:473-489
private void tryCommitOffsetsOnRevoke() {
    if (commitLock.tryLock()) {
        try {
            log.debug("Acquired commitLock on revoke, committing offsets");
            committer.retrieveOffsetsAndCommit();
            clearCommitCommand();
            this.lastCommitTime = Instant.now();
        } catch (Exception e) {
            log.warn("Failed to commit offsets during revoke: {}", e.getMessage());
        } finally {
            commitLock.unlock();
        }
    } else {
        log.info("Skipping offset commit during partition revocation — control thread is mid-commit. " +
                "Uncommitted offsets will be re-delivered to the new assignee. See confluentinc#857.");
    }
}
```

The `else` branch is the whole fix: when the controller already holds the lock, the poll thread
**declines to commit and returns immediately**, so `poll()` completes, the rebalance completes, and
`maybeDoCommit()` gets serviced on the next loop iteration, releasing the controller. The
correctness argument for skipping is stated in the javadoc at `:462-472` and in the call-site
comment at `:440-444`: uncommitted offsets are not lost, Kafka redelivers them to the new assignee.
That trades a possible reprocessing of in-flight records for liveness, which is the right trade
under PC's at-least-once contract.

### The mode caveat - the cycle and the reproducer are in different modes

The second edge of the cycle lives entirely inside `ConsumerOffsetCommitter`, and that object is
**only constructed for the consumer-commit modes** (`BrokerPollSystem.java:95-100`). In
`PERIODIC_TRANSACTIONAL_PRODUCER` the committer is the producer manager
(`AbstractParallelEoSStreamProcessor.java:321`) and `BrokerPollSystem.committer` is
`Optional.empty()`: there is no request queue, no response queue, no `commitAndWait()`. **The cycle
cannot occur in that mode.**

`RebalanceEoSDeadlockTest` runs `PERIODIC_TRANSACTIONAL_PRODUCER` (`:76`). So the headline reproducer
does not exercise the deadlock this document describes. Two further consequences:

- Even among the consumer-commit modes the cycle only closes in `PERIODIC_CONSUMER_SYNC`. In
  `PERIODIC_CONSUMER_ASYNCHRONOUS`, `commit()` falls through to `requestCommitInternal()` and never
  blocks.
- **The fix does not remove the only poll-thread block on the revoke path in the test's mode.**
  Before `tryCommitOffsetsOnRevoke()` is reached at all, `onPartitionsRevoked` runs:

  ```java
  // AbstractParallelEoSStreamProcessor.java:434-435
          while (isTransactionCommittingInProgress())
              Thread.sleep(100); //wait for the transaction to finish committing
  ```

  `isTransactionCommittingInProgress()` (`:1562-1565`) is true exactly while the control thread is
  mid-transaction, which in transactional mode is the common case - `maybeAcquireCommitLock()`
  (`:942`) takes the producer write lock *before* `commitOffsetsThatAreReady()` (`:955`). So the
  revoke callback still spins unboundedly waiting on `pc-control`, on `master` **and** on this
  branch, and `tryLock()` never gets the chance to decline anything. Any claim that this branch
  makes the revoke path non-blocking is false for the mode the reproducer uses.

That is the sharpest open question on this work: the fix addresses a cycle the test cannot reach,
while the block the test *can* reach is untouched.

### Verification status - the part that matters

**Unproven.** Specifically:

- `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` fails **5/5** repetitions on this branch in CI
  (`Integration Tests`, run 31147427151), each with
  `org.opentest4j.AssertionFailedError: Rebalance did not finished`, thrown at
  `RebalanceEoSDeadlockTest.java:141` after a 30 s `CountDownLatch` await.
- `Integration Tests`, `Performance Tests` and `PR Checklist` are all red on astubbs#29.
- In the entire 741,161-line CI log for that run, the string
  `Skipping offset commit during partition revocation` appears **zero** times - and it is an INFO
  log under a root logger set to `info`
  (`parallel-consumer-core/src/test-integration/resources/logback-test.xml`). The contended arm of
  `tryLock()` - the arm that *is* the fix - never executed in that run. Whatever CI proved, it did
  not prove this.

There is also a **test/fix contract mismatch that alone accounts for the deterministic 5/5
failure**, independent of any other defect. `RebalanceEoSDeadlockTest` proves "the revoke path
committed" by subclassing PC and overriding `commitOffsetsThatAreReady()`, counting the latch down
when that override runs on a thread whose name contains `pc-broker-poll`
(`RebalanceEoSDeadlockTest.java:86-99`). On this branch the revoke path **no longer calls
`commitOffsetsThatAreReady()`** - it calls the private `tryCommitOffsetsOnRevoke()`, which inlines
the same work. The only remaining callers of `commitOffsetsThatAreReady()` are on `pc-control`
(`:786`, `:955`). The latch is therefore unreachable *by construction*, and the test would fail
5/5 even on a perfectly working fix. That is a structural defect in the PR, not evidence about the
deadlock either way, and it must be fixed before the test can say anything at all.

## Why This Works

*(The argument for why the change is correct in principle. It is an argument, not an observation.)*

A deadlock needs a cycle in the wait-for graph. There are exactly two edges here:

- `pc-control` waits for `pc-broker-poll`, because only the poll loop drains
  `commitRequestQueue` and posts to `commitResponseQueue`
  (`ConsumerOffsetCommitter.java:174-188`). This edge is structural. It cannot be removed without
  redesigning who owns the consumer.
- `pc-broker-poll` waits for `pc-control`, because the revoke callback wanted the commit lock.
  This edge is **discretionary**: the revoke-time commit is an optimisation, not a requirement.

Cutting the discretionary edge breaks the cycle. `tryLock()` is the minimal way to cut it: it
preserves the optimisation whenever the lock happens to be free (the common case - no contention,
commit still happens at revoke, offsets still get committed early) and drops it only in exactly the
window where taking it would deadlock.

Skipping is safe rather than merely tolerable because of Kafka's own contract: offsets that were not
committed before revocation are simply not committed, so the partition's new owner resumes from the
last committed position and the records are redelivered. PC is at-least-once; redelivery is a
supported outcome, and duplicate suppression is the user function's business. The failure mode of
skipping is bounded reprocessing. The failure mode of blocking is a permanently stalled consumer
group. Those are not comparable.

The commit is also not silently lost from PC's own bookkeeping: the offsets stay marked dirty, and
the deferral machinery in `ConsumerOffsetCommitter.commitDeferringOnRebalance()` (`:224-237`) is
built on exactly this principle - a commit that cannot happen now is *postponed, not dropped*, and
the success marking is deliberately not applied.

**What has not been shown:** that this reasoning survives contact with a real contended broker. The
experiment that would show it - the deferred "Step 2", `-Dparallel-tests=true` against a single
shared broker, with a control arm of the same run on `master` - has not been run.

## Two further open defects on this branch

Both are real and both are unfixed. They are recorded here because anyone picking up astubbs#29 will
hit them before they get anywhere near proving the deadlock fix.

### 1. `ThreadConfinedConsumer` ownership violation on the close path

The branch adds `ThreadConfinedConsumer`
(`.../internal/ThreadConfinedConsumer.java`), a `@Delegate` wrapper that records an owner thread
(`:39,49-52`) and throws `IllegalStateException` from `checkThread` (`:54-68`) if any non-`wakeup()`
consumer method is called from another thread. `close()` and `close(Duration)` are both guarded
(`:174-184`).

Ownership is claimed by the **poll** thread: `BrokerPollSystem.controlLoop()` calls
`consumerManager.claimConsumerOwnership()` at `:146`, which delegates to `claimOwnership()`
(`ConsumerManager.java:288-290`). But in transactional mode the **control** thread is the one that
closes the consumer. The two subsystems disagree about who is "responsible for commits":
`BrokerPollSystem.isResponsibleForCommits()` is `committer.isPresent()` (`:213-215`), which is false
in transactional mode, so the poller skips its close (`:205-211`); while
`AbstractParallelEoSStreamProcessor.isResponsibleForCommits()` is
`committer instanceof ProducerManager` (`:824-826`), which is true, so `maybeCloseConsumer()`
(`:818-822`) calls `consumerManager.close(...)` from `pc-control`, reaching `consumer.close(timeout)`
at `ConsumerManager.java:306`.

The guard throws. `innerDoClose` catches it and downgrades it to a warning
(`AbstractParallelEoSStreamProcessor.java:798-802`), so the consumer is **never closed**, the member
never sends its LeaveGroup, and the group waits out the session timeout before the next rebalance can
complete. In one CI run the ownership message appears **86 times** - every occurrence is
`Consumer.close()` from `pc-control` against an owner of `pc-broker-poll` - alongside 43
`failed to maybeCloseConsumer during close sequence` warnings.

**Correction to the working theory:** the owner thread is usually *not* dead at that point. Of the
43 distinct instances in one CI run, **33 report `alive:true` and 10 report `alive:false`**. So this
is predominantly a live-thread ownership conflict between two subsystems with contradictory notions
of commit responsibility - but the dead-owner arm is roughly a quarter of cases, and a fix that
assumes a live owner will be wrong that often. Both arms need handling.

The live case is also the mechanically expected one, independent of the counts: the poll loop runs
on a single-thread executor (`BrokerPollSystem.java:117`) and `closeAndWait()` waits on the `Future`
- task completion, not thread death - so the pooled thread keeps its `pc-broker-poll` name and stays
alive. `alive:false` appears only once the pool itself has been torn down.

The fix has to reconcile `isResponsibleForCommits()` across the two classes, or hand ownership over
at close time. Tolerating a dead owner addresses the minority case only.

**And be explicit about the inference:** "the deadlock fix is masked by this second defect" is
**analysis, not proof**. Given the test/fix contract mismatch documented above, this defect is not
even needed to explain the 5/5 failure. The deadlock fix has still never been observed working.

### 2. `numberRecordsOutForProcessing` double-decrement

The branch adds `WorkManager.adjustOutForProcessingOnRevoke()`
(`.../state/WorkManager.java:138-149`), called from `onPartitionsRevoked` (`:113`) and
`onPartitionsLost` (`:123`) *before* the partition state cleanup, to subtract in-flight work that
belonged to the revoked partitions. It clamps:

```java
// WorkManager.java:143-147
numberRecordsOutForProcessing -= (int) inflightForRemovedPartitions;
if (numberRecordsOutForProcessing < 0) {
    log.warn("numberRecordsOutForProcessing went negative ({}), resetting to 0", numberRecordsOutForProcessing);
    numberRecordsOutForProcessing = 0;
}
```

The containers it counts are selected with `.filter(WorkContainer::isInFlight)`
(`ShardManager.countInflightForPartitions()`, `:145-152`) - that is, work **still out with the
worker pool**. Those same containers later resolve and come back through the mailbox, where
`handleFutureResult()` decrements the *same* counter again. The stale branch is one such site:

```java
// WorkManager.java:309-313
        if (checkIfWorkIsStale(wc)) {
            // no op, partition has been revoked
            log.debug("Work result received, but from an old generation. Dropping work from revoked partition {}", wc);
            wc.endFlight();
            this.numberRecordsOutForProcessing--;
```

These are **not alternatives - they are a sequence**, which is exactly why the double-decrement
happens: the revoke-time subtraction runs first, the container completes normally afterwards, and
the mailbox path subtracts it again. And the stale branch is not the only second decrement. There
are **three** unclamped decrement sites in `handleFutureResult` - `onSuccessResult` (`:204`),
`onFailureResult` (`:222`) and the stale branch (`:313`) - so the container is counted off twice
whether or not it is judged stale. Only the revoke-time path defends its floor.

The counter feeds `isSufficientlyLoaded()` (`:263-278`), which gates the poller's pause/resume, so
drift distorts backpressure in either direction: a permanently inflated counter is the original
silent-stall signature documented at `:268-271`, and a negative one causes over-fetching. The
correct fix is for exactly one path to own the decrement - most plausibly marking the containers as
already-accounted-for at revoke so **all three** mailbox sites skip them - not adding more clamps.

## Prevention

**Establish and document a lock order between `pc-control` and `pc-broker-poll`, and never take a
lock inside a Kafka callback.** The root enabler was that a rebalance callback, which runs on the
poll thread inside `poll()` and blocks the entire consumer group while it runs, was allowed to
acquire a monitor that the controller thread holds across a blocking cross-thread round trip. The
rule that would have prevented this: **code running inside a `ConsumerRebalanceListener` callback
may only acquire locks with `tryLock()` or not at all.** It is a hot, group-blocking context by
definition. Anything it cannot get immediately, it must decline. This is enforceable - the repo
already runs ArchUnit (`.../ArchitectureTest.java`), and a rule that no method reachable from
`onPartitionsRevoked` / `onPartitionsAssigned` / `onPartitionsLost` calls a blocking `lock()` or
enters a `synchronized` block on shared state would catch the next instance mechanically.

**Do not overload a lock's identity.** `synchronized (commitCommand)` guarded both a boolean flag
and the execution of a multi-second network round trip. Those have completely different hold times
and completely different callers. When a monitor's hold time varies by four orders of magnitude
depending on which path took it, it is two locks wearing one name. Give the long operation its own
lock, which is exactly what the `commitLock` split does.

**...and then check the name you gave it is free.** The split introduced a `commitLock` field
(`:208`) into a class that already had `maybeAcquireCommitLock()` (`:1021`, called from `controlLoop`
at `:942`) - a method that acquires an entirely different lock, the producer transaction lock, via
`producerManager.get().preAcquireOffsetsToCommit()` and bounded by
`options.getCommitLockAcquisitionTimeout()`. Two unrelated locks now answer to "commit lock" three
hundred lines apart in one file. This is not cosmetic: a report of "commit lock timeout" is
ambiguous between them, and the two are contended by different threads for different reasons, so a
failure in one says nothing about the other. Renaming one of them is cheap now and gets steadily
more expensive.

**A test that goes green by isolation is not evidence the bug is gone.** This is the durable lesson.
`RebalanceEoSDeadlockTest` only fails under broker contention; astubbs#68 removed the contention by
giving every fork its own broker; the test went green and stayed green while the deadlock sat
untouched in `master`. Before accepting a green run as proof, ask **what changed in the test
environment** and whether the failure's precondition still exists. Concretely:

- When a test stops failing after an *infrastructure* change rather than a *code* change, record it
  as "no longer reproduced" rather than "fixed", and keep the reproducing configuration alive as a
  separate, deliberately-hostile lane. The deferred "Step 2" (`-Dparallel-tests=true` on a shared
  broker) is exactly that lane; it should be a scheduled job, not a note in a document.
- Keep the contended configuration as a first-class stress lane, not a historical footnote.
  "Contended brokers must not cause failures" is the real bar, and only the contended lane measures
  it.

**Check that the arm of the fix you care about actually executed.** The `tryLock()` failure branch
logs at INFO precisely so a run can be interrogated for it. Zero occurrences across the whole CI log
is the finding that turns "the tests failed" into "the fix was never exercised". Grep for your own
instrumentation before drawing any conclusion from a run.

**When you change a code path, check what observes it.** The revoke path stopped calling
`commitOffsetsThatAreReady()`, and the test that proves the revoke path works observes it by
overriding exactly that method. Any fix that moves work out of an overridable method silently
disconnects every subclass-based test harness pointed at it. Grep for overrides of a method before
routing around it.

**A symptom is not a bug count.** confluentinc#857 was one issue and three defects. When a fix
provably lands and the symptom persists, the default hypothesis should be "there is another defect
with the same symptom", not "the fix was wrong". The control-arm method that separated astubbs#100,
astubbs#80 and astubbs#29 - apply one candidate at a time, keep everything else identical, record
which symptom moves - is what makes that hypothesis testable instead of demoralising.

## Related

- `docs/BUG_857_INVESTIGATION.md` - the branch-local investigation log this document distils.
  **Read it with care:** it is a chronological record that argues with itself (three different
  sections each name a different "root cause"), it presents the `tryLock()` fix as validated at
  80-90% pass rates, and it lists `ThreadConfinedConsumer` under "what was fixed". This document
  contradicts all three of those. It also predates astubbs#80, astubbs#100 and astubbs#108, so parts
  of its Bug 1 and Bug 2 analysis are superseded.
- `docs/inflight/bug-857-family.md` - what is still open across the family.
- `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md` - astubbs#80, the
  sibling defect, and the authority on why the two are not duplicates.
- `docs/solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md` -
  where `RebalanceEoSDeadlockTest` was first mapped to this deadlock, and the source of the
  "diagnose before masking" rule.
- `docs/solutions/workflow-issues/keeping-both-sides-of-a-merge-conflict-resurrects-a-deleted-abstraction.md` -
  covers the `ThreadConfinedConsumer` close-ownership blocker from the merge-reconciliation angle.
- `docs/solutions/test-flakiness/unforceable-trigger-commit-lock-timeout-2026-08-07.md` -
  disambiguation: that one is a **different** commit lock (the producer transaction lock), not the
  `commitCommand` monitor described here.
- `RebalanceEoSDeadlockTest` itself is upstream's guard from confluentinc#548, *"Fix deadlock between
  pc-control and pc-broker-poll threads where partitions are revoked"* (merged 2023-04-03) - already carried by this fork. A
  deadlock between these two threads was fixed once before; this is a second one in the same pair.
