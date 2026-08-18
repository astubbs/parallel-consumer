---
title: "Two threads, one consumer: why the commit seam keeps producing deadlocks, and why mechanism changes never fix it"
date: 2026-08-18
category: architecture-patterns
module: parallel-consumer-core/internal
problem_type: architecture_pattern
component: background_job
severity: high
applies_when:
  - Asking why `pc-control` and `pc-broker-poll` split responsibilities the way they do
  - About to "reconcile" the two `isResponsibleForCommits()` methods, or otherwise assuming they contradict each other
  - Adding a thread-ownership guard or confinement wrapper around the Kafka consumer
  - Proposing to replace the commit request/response queues with actors, futures, or another IPC mechanism
  - Diagnosing a deadlock, stall or unclosed consumer anywhere on the revoke or close path
  - Reading a fix that adds a flag, a cache or a sleep near `onPartitionsRevoked`
related_components:
  - BrokerPollSystem
  - AbstractParallelEoSStreamProcessor
  - ConsumerManager
  - ConsumerOffsetCommitter
  - ProducerManager
tags:
  - thread-ownership
  - consumer-confinement
  - deadlock
  - ab-ba-lock-ordering
  - commit-path
  - shared-nothing
  - archaeology
  - issue-857
---

# Two threads, one consumer

Why Parallel Consumer's commit machinery is shaped as it is, reconstructed from the commit record on
2026-08-18. Written because the reasoning existed only in the original author's head, and three
separate 2026 investigations each re-derived part of it before acting.

## The one-paragraph answer

`KafkaConsumer` is not thread-safe. PC runs two long-lived threads. Which thread may touch the
consumer was never *decided* - it is residue from extracting the blocking poll loop into its own
thread in 2020. Every incident since has happened at the seam where that residue meets the fact that
**commit initiation lives on one thread and commit execution may live on the other, depending on
commit mode.** Four times now the seam has been patched with a cache, a flag or a sleep, and never
restructured.

## How it got this way

| When | Commit | What it decided |
|---|---|---|
| pre-2020-06 | - | **Single-threaded.** The control loop called `consumer.poll()` itself. Commits were transactional-producer only. No ownership question existed. |
| 2020-06-17 | `af1fa5de4` "Event based control, integration tests, back-pressure" | **`BrokerPollSystem` became its own thread.** The consumer object went with it. The commit body gives no reason for consumer ownership - one thread cannot block on both the work mailbox and a long broker poll, so each blocking source got a thread, and the consumer travelled with the poll. **Poll-thread ownership is a side effect, not a decision.** |
| 2020-11-23 | `60e398102` "Choose between Consumer commit or Producer transactional commits" (confluentinc#25) | **The mode split.** Created `ConsumerManager`, `ConsumerOffsetCommitter`, `AbstractOffsetCommitter`, `ProducerManager`, and *both* `isResponsibleForCommits()` methods, in one 16-file change. Commit *initiation* stayed on control (it owns `WorkManager` state and cadence). Commit *execution* went to whichever component owns the needed client. |
| 2020-12-03 | `9dc92e51c` "Move consumer back to PC wrapped for thread safety, so commits are in line with control" | **The redesign was attempted three weeks later and abandoned.** On branch `move-cons-to-pc`, it comments out `BrokerPollSystem`'s `committer`, `maybeCloseConsumer` and `isResponsibleForCommits` so control commits directly. Never merged. **The record does not say why.** |
| 2020-12-15 | `c033f367e` "Replace commit request/signal locking system with far simpler message passing" | Lock + `Condition` + `commitRequested` flag replaced by `commitRequestQueue` / `commitResponseQueue`. **Same wait-for edge, cleaner mechanism.** |
| 2021-07-23 | `a3378ed58` "AK 2.7 blocks concurrent access to group metadata, so cache it" | Control's `groupMetadata()` call collided with the poll thread. Patched with the `metaCache` still in `ConsumerManager`. |
| 2023-04-03 | `2738fb3d5` (confluentinc#548) | A deadlock between these same two threads on revoke. Patched with an `AtomicBoolean isRebalanceInProgress` and a `while (isTransactionCommittingInProgress()) Thread.sleep(100)` spin. **That spin is now the transactional revoke wait behind astubbs/parallel-consumer#44.** |
| 2026 | confluentinc#857 | The same pair, deadlocked again, on the same path. |

## The two `isResponsibleForCommits()` methods do NOT contradict each other

This is the most reliable way to misread the code, and a 2026 investigation got it wrong before
checking. Both methods were created in `60e398102`, by one author, on one day. Both carry the
**identical javadoc**, still present verbatim:

> To keep things simple, make sure the correct thread which can make a commit, is the one to close
> the consumer. This way, if partitions are revoked, the commit can be made inline.

They are an **XOR over commit mode**:

| Class (thread) | Predicate | True when |
|---|---|---|
| `BrokerPollSystem` (`pc-broker-poll`) | `committer.isPresent()` | consumer-commit modes |
| `AbstractParallelEoSStreamProcessor` (`pc-control`) | `committer instanceof ProducerManager` | transactional mode |

Exactly one is true. Exactly one thread closes the consumer. Neither predicate has been edited since
2020.

**The trap is that the relationship is implicit, not that the name is wrong.** Both methods ask the
*same* question - "am I the component that commits, and therefore the one that closes the consumer?" -
and the shared javadoc says so. Only the answer differs, per component, per mode. That is
polymorphism, and it is not expressed as such: there is no shared type, no stated invariant, and
nothing that says these two are halves of one decision. A reader who finds both concludes they
disagree.

**This is a correction to an earlier draft of this document**, which described them as two different
questions with a badly overloaded name. That was wrong in a way worth recording, because it is the
mistake the code invites: differing *implementations* were read as differing *questions*.

**The fix is an interface, not a rename.** Give the two a common type - one method, one javadoc, two
implementors - so the polymorphism is official. That also makes the XOR **statable and checkable**:
"exactly one implementor returns true for a given configuration" is an invariant nobody can express
today and which can be asserted at construction, rather than left for the next archaeologist to
rediscover. Renaming alone only documents the trap more loudly.

## Why transactional mode is where things break

The design carved out an exception from day one. The comment removed by `c033f367e` reads:

> if owning thread is asking, then perform the commit directly (this is the thread that controls the
> consumer) - this can happen when the system is closing ... and partitions are revoked and we want
> to commit

That carve-out exists **only on the consumer-commit side**. In transactional mode the revoke callback
runs on the poll thread, but the committer is the `ProducerManager` on the control thread - so there
is no direct path. **Both confluentinc#548 and confluentinc#857 live in exactly that gap.**

## What has been tried, and the one thing that survives everything

The fatal edge is: **the control thread blocks on something only the poll thread can produce.**

It survived:

1. the 2020 lock + `Condition` + flag scheme (`60e398102`),
2. its replacement by request/response queues (`c033f367e`),
3. the 2022 actor draft on `improvements/commit-command-actor`, which commented out `commitAndWait()`
   and both queues and replaced them with `Future ask = commitRequestSend(); if (isSync())
   ask.get(timeout)` - **the same wait-for edge with better plumbing.**

**Changing the mechanism has never removed the deadlock class.** Only changing *who owns the consumer*
would. The 2022 draft says so itself, in a TODO on `ConsumerOffsetCommitter.commit()`:

> todo this should be package private, and should not need to expose a thread safe interface - bubble
> up to broker system instead - see Controller refactor

alongside a second TODO on the same method - `todo why? when am i the owner? audit - i think never` -
suggesting the owner-asks-directly carve-out was already suspected dead.

`refactor/extract-controller` (`25db90e38`, 2022-11-30, "extract SubscriptionHandler interface, pull
Poll up") got as far as an `internal/Controller.java`, a `RebalanceHandler` and a
`SubscriptionHandler` interface. The actor family stalled with **two unreconciled actor bases**
(`d391398f1`: "split: actor base - needs unifying of the two actor classes"). Why it stopped, the
record does not say.

The canonical tracker is **confluentinc#200 / astubbs/parallel-consumer#142, "Refactor: Consider a
shared nothing architecture, to reduce thread complexity"** - both still open. *Shared nothing* means
no object is shared between threads at all; each owns its state and they communicate by messages.
Applied here it means eliminating the separate poller thread, so one thread owns the consumer and
does both the polling and the committing.

## Constraints any fix must respect

Derived from what the history punished, not from first principles:

- **The revoke callback must be able to complete a commit inline, before truncation**, on whatever
  thread `consumer.poll()` runs on. Set in `60e398102`, re-affirmed by confluentinc#548's fix. In
  transactional mode this needs the producer - a cross-thread dependency the current design cannot
  express except via flags and sleeps.
- **Whoever closes the consumer triggers revoke callbacks on the closing thread.** That is the whole
  reason for "the committer closes". Any confinement scheme therefore needs ownership *transfer* -
  `ThreadConfinedConsumer.claimOwnership()` is claim-only, with no release or handover - or close must
  be routed to the owning thread.
- **A confinement guard must test whether anyone is still *using* the consumer, not thread identity.**
  In `innerDoClose`, `brokerPollSubsystem.closeAndWait()` returns *before* `maybeCloseConsumer()` runs,
  so at the moment the guard fires the poll loop has already finished and there is no concurrency at
  all. The guard rejects the call because the pooled `pc-broker-poll` thread object still exists and
  still holds the claim - identity, not usage. This is also why roughly three quarters of observed
  cases report a *live* owner: a pooled thread outlives the task that claimed ownership. Close is a
  clean sequential handoff point; releasing ownership when the poll loop exits makes the guard assert
  what it actually means.
- **...but close is only sequential when it succeeds, and the failure path is a real race.**
  `closeAndWait()` throws on timeout; `innerDoClose` catches that, downgrades it to a warning, and
  calls `maybeCloseConsumer()` regardless. So a poll system that failed to shut down still has its
  consumer closed from another thread - genuine concurrent access to a non-thread-safe client, on
  `master`, today. Any ownership scheme must therefore release from **inside the poll task as its last
  act**, never at the closer's request: that way the happy path clears and the failure path still
  throws. A fix that drives the violation count to zero has disabled the guard rather than fixed the
  bug.
- **`consumer.wakeup()` is the only thread-safe consumer call, and it is aimed poorly.** The
  `pollingBroker` guard and the `WakeupException` retry loops in `ConsumerManager` exist because a
  wakeup meant for `poll()` can land on `commitSync()`. Any design keeping cross-thread wakeups keeps
  that race.
- **Control must not read `groupMetadata()` live** - AK 2.7+ blocks concurrent access. The `metaCache`
  is load-bearing.
- **Mode-conditional thread topology is the root hazard.** Every incident happened where transactional
  mode put a required action on the thread that does not own the needed client. A fix should unify who
  commits across modes, or unify who owns the consumer. **Asserting confinement on top of the split
  just makes the 2020 design's deliberate violations throw** - which is what
  astubbs/parallel-consumer#29's `ThreadConfinedConsumer` does, 88 times in one CI run.

## What astubbs/parallel-consumer#29 established, 2026-08-18

The branch's entire production diff was a single April 2026 commit bundling **four independent
changes**. Nobody had written the decomposition down, so every prior assessment judged the branch as
one thing - and the one thing was red, which made the only provable change look as doubtful as the
three around it.

| # | Change | Outcome |
|---|---|---|
| 1 | `commitLock` + `tryCommitOffsetsOnRevoke` - the deadlock fix | **Proven.** Kept |
| 2 | `ThreadConfinedConsumer` - runtime thread-confinement guard | **Kept, after fixing its predicate.** It rejected a legal handoff |
| 3 | `adjustOutForProcessingOnRevoke` + `countInflightForPartitions` | **Deleted.** The drift it targeted does not exist; it caused drift itself |
| 4 | `pausedForThrottling` reset on assignment | **Deleted and replaced.** It reintroduced this issue's own symptom |

### 1. The deadlock fix - measured twice

A purpose-built probe forces the revoke-during-commit overlap deterministically in
`PERIODIC_CONSUMER_SYNC`, byte-identical on both arms, against a shared broker (forking one broker
per fork removes the window - that is how the suite went green while the deadlock sat untouched):

| Arm | n | Failures | `Skipping offset commit during partition revocation` |
|---|---|---|---|
| `origin/master` | 20, then 60 | **20/20**, then **60/60** | 0 |
| fix head | 20, then 60 | **0/20**, then **0/60** | 21, then **63** |

The skip-log count is what makes the result mean anything: it is the INFO line on the contended
`tryLock` branch, so a nonzero count proves the fix path executed. **A clean fixed arm with a zero
skip-count is indistinguishable from a probe that never opened the window** - which is exactly how
this fix looked unproven for four months. The second run interleaved the arms (A,B,A,B,A,B) so
neither sat in different box conditions.

**The reproducer that shipped with the fix could not observe it**, on two independent counts: it runs
`PERIODIC_TRANSACTIONAL_PRODUCER`, two modes from the cycle, and it counts a latch by overriding
`commitOffsetsThatAreReady()`, which the fixed revoke path no longer calls. Measured: it passes 5/5
on the **defect** arm and fails 5/5 on the **fixed** arm. It reports the fix as a regression.

### 2. The confinement guard was testing thread identity, not usage

`ThreadConfinedConsumer` threw 88 times in one CI run - every one `Consumer.close()` from
`pc-control` against an owner of `pc-broker-poll` - and `innerDoClose` swallowed each to a warning,
so the consumer was never closed, no LeaveGroup was sent, and the group waited out its session
timeout. That cascaded into 16 failing integration tests.

Every one of those fired **after** `closeAndWait()` had returned, i.e. after the poll task had
provably completed. The guard was comparing `Thread` identity while a pooled thread outlives its
task, so it rejected a legal sequential handoff.

**The fix is not to relax it.** Ownership is now three-state - `UNCLAIMED` (pre-start, allow all, so
init-time `subscribe` works), `OWNED`, and `RELEASED` (admits no direct use; only a claim). The poll
loop releases **as its own last act inside the poll task**, and the closer takes over with a
non-stealing CAS. That ordering matters: `closeAndWait()` throws on timeout and `innerDoClose`
proceeds to close the consumer **anyway**, so there is one path where the control thread closes a
consumer the poll loop may still be using. Releasing at the *closer's* request would legalise exactly
that path; releasing from inside the poll task leaves it still throwing, correctly.

### 3. The counter drift was never an accounting bug

`WorkManager.numberRecordsOutForProcessing` feeds `isSufficientlyLoaded()`, which gates the poller's
pause/resume. Master's own comment there names the failure mode: *"A high outForProcessing with no
awaitingSelection and no real progress is the counter-drift signature."*

Measured with a deterministic probe (real engine on a `MockConsumer`, user function gated so records
are genuinely out with the pool, revoke driven through the engine's own listener, then quiesce and
compare against ground truth):

| | counter at quiesce, 5 revoke+reassign cycles | truth |
|---|---|---|
| master | **0, 0, 0, 0, 0** | 0 |
| with the April fix | **-8, -16, -20, -20, -20** | 0 |

So the fix corrupted the counter *downward* - the over-fetch direction - and by cycles 3-4 it read
**0 while 4 records were genuinely in flight**. It also made a single-threaded counter cross-thread:
every other mutation runs on the control thread, that one from `onPartitionsRevoked` on the poll
thread, on a plain `int`.

**Why the drift is not real:** every worker-side exit returns the container to the mailbox and every
mailbox branch decrements - including the stale branch, which has decremented since upstream
confluentinc#549 (`fdd245edc`, 2023-02-21), three years before the fix was written. The accounting
was already balanced. The most consistent reading of the original observation is that the counter was
**truthfully** high, because in-flight work never returned - user functions wedged behind the
commit-path deadlock. astubbs/parallel-consumer#100 removed that. **The "counter-drift signature"
describes a symptom whose only known cause was the deadlock**, which is why one April session saw
both.

`OutForProcessingCounterDriftProbeTest` is kept as the regression test - the only thing in the repo
that can tell the counter from reality.

### 4. Mirroring Kafka's pause state cannot work

The reset of `pausedForThrottling` on assignment was correct for the **eager** protocol, where
`assignFromSubscribed` replaces the assignment map and clears every partition's pause, and wrong for
**cooperative**, where partitions retained across a rebalance keep theirs. `resumeIfPaused()` was
gated on the flag, so the two disagreeing meant nothing ever resumed: **paused consumption after a
rebalance, this issue's own symptom, introduced by the code meant to fix it.**

Replaced by asking Kafka - `consumer.paused()` - which makes the protocol irrelevant and the logic
self-correcting, and needs no reset hook at all. The constraint that makes this non-trivial is the
one running through this whole document: `paused()` is a consumer call, so only the **poll** thread
may make it. The control thread reads a per-poll cache, which is safe only because its callers are
heuristics where a spurious wakeup costs nothing.

## The close path: warnings a user cannot act on

Seven WARN/ERROR lines fired during close. A WARN addresses the operator, but by then they have
already asked us to stop - so each was one of three things, and the split is the useful part:

- **A bug wearing a warning's clothes.** `failed to maybeCloseConsumer` fired 88 times and meant *"no
  LeaveGroup was sent, your group stalls for the session timeout"*. The right response was never to
  reword it. Fixing the handoff took it to zero.
- **A note to the developer, at the user's log level.** One asked *"test latch locks?"* in a
  production log; another said an interrupt *"may lead to issues"*. Both are DEBUG, and only useful
  with the state that would actually help - active count, queue depth, run state.
- **A real consequence stated as internals.** A close-time commit failure means *offsets were not
  committed and these records will be redelivered*, and close makes **exactly one attempt** by design
  (`commitSync`'s retry loop stops once the poll system is closing, because retrying stalls shutdown
  while nothing polls). The remedy is `closeDrainFirst()`, and the message now says so - but only
  when the user did not already drain.

**Logging a throwable is running its author's code.** `getMessage()` and `getCause()` are overridable,
and Logback's `ThrowableProxy` calls `getCause()` directly - so on a failure path the log call can
throw and the caller sees the logger's failure instead of the one being reported. `ThrowableUtils`
(from astubbs/parallel-consumer#267) exists for this: `describeWithRootCause` never throws, and
`logWithoutEscaping` guarantees the log call cannot become the failure.

<!-- issue-refs: exempt-begin - the bare #N below are row labels for this document's own sighting
     table, not issue references. Genuine issue refs inside this block are written qualified. -->

## The sightings: what was observed, and what can still be replayed

Six chaos sightings plus one unit-level failure, 2026-07-30 to 2026-08-18. **The ledger recorded a
commit mode for none of them**, which is how a transactional-mode failure came to be logged as "live
confirmation" of a cycle that cannot occur in that mode. Mode is the discriminator; it is listed here
because without it a sighting cannot be attributed at all.

| # | Date | Test / arm | Mode | Signature | Failing seed |
|---|---|---|---|---|---|
| 1 | 07-30 | `RebalanceEoSDeadlockTest`, 1 of 20, local stress | `TRANSACTIONAL_PRODUCER` | test latch timeout | none captured |
| 2 | 08-11 | `ChaosRevokeUnderWorkIT` (eager) | `CONSUMER_SYNC` | `CLASS2_STALL/LAG_STAGNATION`, 154s vs 150s bound | `4734674029169027864` |
| 3 | 08-12 | `ChaosRevokeUnderWorkCooperativeIT` | `CONSUMER_SYNC` | **no probe verdict**; shutdown timeout only | `3986919097693415295` |
| 4 | 08-12 | `ChaosChurnStormIT` | `CONSUMER_ASYNCHRONOUS` | `ZOMBIE_MEMBER/REBALANCE_BLOCKED`, dwell 15426ms | `7731567379755737438` |
| 5 | 08-18 | `ChaosChurnStormIT` | `CONSUMER_ASYNCHRONOUS` | `NO_PROGRESS`, fleet stuck 98150/100000 | `3086917415748208232` |
| 6 | 08-17 | `ChaosChurnStormIT` | `CONSUMER_ASYNCHRONOUS` | `NO_PROGRESS`, fleet stuck 95382/100000 | `8603691233664838594` |
| 7 | 08-18 | `ChaosRevokeUnderWorkIT` (eager) | `CONSUMER_SYNC` | same as #2: `CLASS2_STALL`, 154s vs 150s | **none** - console truncated |

Replay any seed with:

```bash
./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
  -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=<seed>
```

**None of these seeds has ever been replayed.** Each entry names replay as its own deciding
experiment, and it remains the family's cheapest open work.

**Do not replay these expecting a failure** - they are *passing control arms*, and one was circulated
as a failure by a handoff written from a truncated log: `4087023100803854645`, `6926127865194591503`,
`5980280513720170608`, `334227014609238766`, `642714983109785585`, `374908783265204320`.

**What the record deliberately does not claim.** Sightings 3-6 decline attribution outright - *"Do not
read this entry as identifying either. It records a signature and a replayable seed; which defect it
belongs to, if any, is what the replay is for."* Only #1 broke that discipline, and #1 is the one
since shown to be misattributed.

**Two things the sightings settle regardless of the deadlock:**

- **#2 and #7 are the same failure seven weeks apart** - same test, same eager arm, same signature,
  same 154s against a 150s bound, in the only mode where the AB-BA cycle can close. #7's control arm
  needs no replay: the same lane passed on the two preceding heads of that branch, and the diff to the
  failing head contains **zero non-comment Java lines**, so the bytecode is identical. The same
  executable passed twice and failed once.
- **#4, #5 and #6 are in `PERIODIC_CONSUMER_ASYNCHRONOUS`, where no known defect can operate** - the
  AB-BA cycle cannot close (`commit()` falls through to `requestCommitInternal()` and never blocks),
  the transactional revoke wait cannot run, and astubbs/parallel-consumer#100 and
  astubbs/parallel-consumer#80 have landed.
  Either a fourth defect or the chaos harness's own teardown races. Nobody had said this out loud,
  because modes were never recorded.

<!-- issue-refs: exempt-end -->

### Retrieving the evidence is itself a trap

Three times in one day, console logs destroyed the evidence:

- `gh run view --job <id> --log` **silently truncated** a 5948-line log to 1654 lines, mid-phase. A
  handoff written from it named the wrong test and circulated a passing arm's seed.
- GitHub truncated a log **stream** server-side, so the `=== AMBIENT PROBE AUTOPSY ===` block was in
  neither `--log` nor `--log-failed`.
- One run's console parsed as `tests=0 failures=0 errors=0` - **indistinguishable from a clean run** -
  while the uploaded artifact carried the violation. A false negative, which is worse than an absence.

Go to the **uploaded test-report artifact** first for any chaos or broker failure; the autopsy is
embedded in the failsafe XML's captured `system-out` and survives both truncation mechanisms. The
run-logs archive (`gh api repos/.../actions/runs/<id>/logs`) is the second route. And **the seed
should move into the autopsy block**, so the deciding experiment survives the log it is printed to -
the seventh sighting has no seed for exactly this reason.


## Related

- [`a-query-must-never-mutate-derive-thread-safety-from-callers.md`](a-query-must-never-mutate-derive-thread-safety-from-callers.md) -
  the sibling lesson: do not add a thread-ownership guard on the strength of a javadoc or a design
  assumption. This document is the worked case of that rule being broken.
- `docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md` -
  the confluentinc#857 AB-BA cycle itself, and why it closes only in `PERIODIC_CONSUMER_SYNC`.
- `docs/plans/2026-08-18-002-fix-857-revoke-path-cluster-decomposition-plan.md` - the four clusters in
  astubbs/parallel-consumer#29 and the order to take them.
- `docs/inflight/bug-857-transactional-revoke-wait.md` - the confluentinc#548 sleep-spin as it stands
  today, carrying astubbs/parallel-consumer#44.
- `docs/refactoring.md` - the Actor/IPC section and the `refactor-thread-model-god-class` entry.
