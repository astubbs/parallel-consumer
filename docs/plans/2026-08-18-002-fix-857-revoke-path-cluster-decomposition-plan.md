# The confluentinc#857 revoke path: four changes in one commit, and the order to take them

**Date:** 2026-08-18
**Status:** decomposition agreed; cluster 1 measured, clusters 2-4 undecided
**Subject:** PR astubbs/parallel-consumer#29, branch `bugs/857-paused-consumption-multi-consumers-bug`

## Why this document exists

PR astubbs/parallel-consumer#29's entire production diff is a **single April 2026 commit**
(`1479f73f`, squashed from 35 commits whose backup ref no longer exists). It bundles **four
independent changes**. They were written together because they all touch the partition-revoke path,
which is also why they look like one fix and are not.

Nothing in the repo stated the decomposition, so every prior assessment judged the branch as a unit -
and the unit is red, which made the one *provable* change look as doubtful as the three around it.
This document strings them together, states the relationships, and gives an order.

**The clusters share no symbols.** Verified directly: cluster 1 references only pre-existing
identifiers (`committer`, `clearCommitCommand`, `lastCommitTime`); clusters 2, 3 and 4 touch disjoint
files. They can move independently. The only shared file is
`AbstractParallelEoSStreamProcessor.java`, where the hunks do not overlap.

## The four clusters

| # | Cluster | Size | Files | State |
|---|---|---|---|---|
| 1 | Commit-path deadlock fix | ~20 lines | `AbstractParallelEoSStreamProcessor` | **Proven** |
| 2 | `ThreadConfinedConsumer` | ~290 lines | + `ConsumerManager`, `PCModule`, `BrokerPollSystem`, `ArchitectureTest` | **Net-negative** |
| 3 | Counter adjustment on revoke | ~42 lines | `WorkManager`, `ShardManager` | **Half a fix** |
| 4 | `pausedForThrottling` reset | ~15 lines | `BrokerPollSystem` | **Confirmed regression** under cooperative |

### How they relate

All four are responses to the same *symptom* - "consumption stops after a rebalance" - reached from
the same investigation. They are not responses to the same *defect*:

- **1 and 3 are both revoke-time bookkeeping**, and both are safe only under contention that the
  fork's per-fork-broker CI no longer produces.
- **2 exists to make 1 debuggable** - it was added to surface the thread-safety violations the
  investigation kept guessing at. It is diagnostic scaffolding that grew into production code.
- **4 is unrelated to the deadlock entirely** and rides along because it was found in the same
  session.

The practical consequence: **1 does not need any of the others**, and 2 and 4 actively prevent the
branch from going green, which is why 1 has never been evaluated on its own merits.

## Cluster 1 - the deadlock fix. Measured 2026-08-18.

`onPartitionsRevoked` blocked on `synchronized(commitCommand)` while the control thread held it
mid-commit. The poll thread cannot service the commit-response queue while blocked; the control
thread cannot finish without it. AB-BA. Replaced with a dedicated `ReentrantLock` whose revoke-side
call **declines** (`tryLock`) rather than blocks.

**A/B soak result.** Purpose-built probe forcing the revoke-during-commit overlap deterministically,
byte-identical on both arms, `-Dparallel-tests=true` against a shared broker (forking per broker
removes the window - see `docs/inflight/test-857-parallel-integration-proof.md`):

| Arm | Failures | `Skipping offset commit during partition revocation` |
|---|---|---|
| `origin/master` | **20 / 20** - control thread died with `Timeout waiting for commit response`; revoke commit blocked ~6.05s | 0 |
| astubbs#29 head | **0 / 20** - revoke commit 0-5ms | **21** (>=1 per iteration) |

Box load 2-8 of 32 cores throughout; the forced-window design is load-insensitive.

**Scope: `PERIODIC_CONSUMER_SYNC` only.** The cycle's second edge lives in `ConsumerOffsetCommitter`,
constructed only for the consumer-commit modes, and only the SYNC arm blocks - async falls through to
`requestCommitInternal()` and never blocks.

**Two findings that come with it:**

1. **The existing reproducer is inverted.** `RebalanceEoSDeadlockTest` passes **5/5 on the defect
   arm** and fails **5/5 on the fixed arm** - measured, not argued. It runs
   `PERIODIC_TRANSACTIONAL_PRODUCER` (two modes from the cycle) and counts a latch by overriding
   `commitOffsetsThatAreReady()`, which the fixed revoke path no longer calls. Any A/B run on it
   reports the fix as a regression.
2. **Why CI logged the fix as never executing.** The commit gate is `isTimeToCommitNow() &&
   wm.isDirty() && !isRebalanceInProgress`. With fast test processing everything is committed before a
   rebalance lands, so the control thread is never mid-commit and `tryLock` always succeeds. The zero
   skip-log count in CI means **CI's tests never open the window** - not that the arm is dead code.
   The first probe version reproduced this exactly: 10/10 false-green *on the defect arm*.

The instrument is committed alongside this document as
`Rebalance857CommitSyncDeadlockProbeIT`. It is a measurement instrument, not yet a production
regression test - it needs a tag, a runtime budget and a review before it belongs in a CI lane.

## Cluster 2 - `ThreadConfinedConsumer`. Do not land as-is.

Ownership is claimed by the poll thread (`BrokerPollSystem.claimConsumerOwnership()`), but in
transactional mode the **control** thread closes the consumer, because the two subsystems disagree
about who is responsible for commits: `BrokerPollSystem.isResponsibleForCommits()` is
`committer.isPresent()` (false in transactional mode) while
`AbstractParallelEoSStreamProcessor.isResponsibleForCommits()` is `committer instanceof
ProducerManager` (true).

The guard throws `IllegalStateException`, `innerDoClose` swallows it to a `log.warn`, the consumer is
never closed, no LeaveGroup is sent, and the group waits out the session timeout. **88 occurrences in
one CI run**, with a cascade of 5 failures and 11 errors in the integration lane
(`CloseAndOpenOffsetTest`: 10 errors of 14).

The guard is doing its job; what it guards is wrong. **The fix is to reconcile
`isResponsibleForCommits()` across the two classes, or hand ownership over at close time.** Roughly a
quarter of observed cases have a dead owner thread, so tolerating a dead owner addresses the minority
case only - both arms need handling.

`ArchitectureTest`, `getAssignmentSize()` and the `MultiInstanceRebalanceTest` state dump ride with
this cluster.

## Cluster 3 - the counter adjustment. Half a fix.

`WorkManager.adjustOutForProcessingOnRevoke()` subtracts in-flight work belonging to revoked
partitions. Those same containers later resolve through the mailbox, where `handleFutureResult`
decrements the **same** counter again. Three unclamped decrement sites (`onSuccessResult`,
`onFailureResult`, the stale branch); only the revoke path clamps.

This is not an edge case: results already sitting in the mailbox at revoke are still `isInFlight`, so
they are counted by the revoke adjustment *and* decremented on processing. A persistent negative makes
`hasWorkInFlight()` and `isNoRecordsOutForProcessing()` wrong (drain and close logic) and inflates
`calculateQuantityToRequest`.

**Landing this alone institutionalises the double-decrement.** The correct shape is for exactly one
path to own the decrement - most plausibly marking containers as already-accounted-for at revoke so
all three mailbox sites skip them - not adding more clamps.

## Cluster 4 - `pausedForThrottling` reset. Probably a regression.

`BrokerPollSystem.onPartitionsAssigned()` clears `pausedForThrottling` on every assignment. Its
justifying comment - "Kafka clears its internal pause state on reassignment" - holds only for the
**eager** protocol. Under `CooperativeStickyAssignor`, retained partitions are never revoked, keep
their consumer-side pause, and `onPartitionsAssigned` fires with only the *added* set.

`resumeIfPaused()` is gated entirely on that flag: with it cleared, it never calls
`consumerManager.resume()`. Those partitions never resume.

**That is this PR's own symptom - paused consumption after a rebalance - introduced by this PR**, and
it lands precisely where the branch advertises new cooperative support.

**Confirmed against Kafka's source, 2026-08-18** (kafka-clients 3.9.2), which settles the question the
comment gets wrong. `ConsumerCoordinator.onJoinPrepare`:

```java
case EAGER:
    revokedPartitions.addAll(subscriptions.assignedPartitions());
    invokePartitionsRevoked(revokedPartitions);
    subscriptions.assignFromSubscribed(Collections.emptySet());   // wipes the map

case COOPERATIVE:
    // only revoke those partitions that are not in the subscription anymore
    if (!revokedPartitions.isEmpty()) {
        ownedPartitions.removeAll(revokedPartitions);
        subscriptions.assignFromSubscribed(ownedPartitions);      // retained partitions passed through
    }
```

and `SubscriptionState.assignFromSubscribed` reuses the existing per-partition state object rather
than making a new one:

```java
TopicPartitionState state = this.assignment.stateValue(tp);
if (state == null)
    state = new TopicPartitionState();
assignedPartitionStates.put(tp, state);
```

`TopicPartitionState` holds `private boolean paused`. So:

- **Eager: the comment is right.** `assignFromSubscribed(emptySet())` destroys every state object, so
  every partition returns with `paused = false`. Clearing PC's flag matches reality.
- **Cooperative: the comment is wrong.** Retained partitions keep their state object and stay paused
  at the Kafka level. And when `revokedPartitions` is empty, `assignFromSubscribed` is not called at
  all, so nothing is touched.

PC pauses at the Kafka level (`doPause()` -> `consumerManager.pause(assignment)`), so this is exactly
the state that survives, while `resumeIfPaused()` - gated on the cleared flag - never resumes it.

**Verdict: drop it, or reset the flag only under the eager protocol.** As written it introduces
permanently paused consumption after a cooperative rebalance.

## Order of work

1. **Cluster 1, now.** It is measured, it is ~20 lines, and it is the only one with evidence. Ship it
   with a productionised version of the probe, since without a test that can observe it the fix
   regresses silently. While extracting: rename `commitLock` (the file already has an unrelated
   `maybeAcquireCommitLock()` for the producer transaction lock ~800 lines away), keep acquired-path
   commit failures loud rather than warn-swallowed, and restore the interrupt flag in the catch.
2. **Cluster 4, next** - the Kafka question is now settled from source (above): it is a regression
   under the cooperative protocol, not a fix. Drop it, or gate the reset on the eager protocol. No
   decision outstanding.
3. **Cluster 3** - finish it or drop it. Do not land it in its current shape.
4. **Cluster 2, last and separately** - it is the largest, it is currently the reason the branch is
   red, and its fix is a design decision about consumer ownership rather than a patch.

**Not carried forward:** the branch's `src/test-integration/resources/logback-test.xml` (collides with
master's `src/test/resources/logback-test.xml` - both copy to the same `target/test-classes/` path,
making which wins timestamp-dependent, and it raises six per-record loggers to DEBUG module-wide; the
chaos job log is 126 MB), `docs/BUG_857_INVESTIGATION.md` (superseded, at the wrong path, cites
`file:line`), `ManagedPCInstanceLifecycleTest` (sleep-driven, tests harness code that astubbs#292
already fixed better on master), and several dead methods
(`ConsumerManager.getConsumerClass()`, `ModelUtils.createWorkFor(long,long)`,
`BrokerIntegrationTest.resetKafkaContainer()`).

## What this does NOT resolve

The three `ChaosChurnStormIT` sightings in `PERIODIC_CONSUMER_ASYNCHRONOUS`
(`docs/inflight/test-857-churn-storm-async-stalls.md`) are untouched by every cluster here: the AB-BA
cycle cannot close in that mode, the transactional revoke wait cannot run, and astubbs#100 and
astubbs#80 are landed. They are unexplained by every known member of the family, and cluster 1 landing
does not change that.

Nor does any cluster address the transactional-mode unbounded revoke wait
(`docs/inflight/bug-857-transactional-revoke-wait.md`), which carries
astubbs/parallel-consumer#44 - the only issue upstream ever labelled a verified bug.
