# confluentinc#857 family: the unbounded revoke wait in transactional mode

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->

**Commit mode: `PERIODIC_TRANSACTIONAL_PRODUCER` only.** This is the discriminator - the defect below
<!-- post-merge: checked -->
and the AB-BA deadlock in astubbs#29 are in mutually exclusive modes and cannot be the same bug.

## The defect

`AbstractParallelEoSStreamProcessor.onPartitionsRevoked` waits with **no deadline** for an in-flight
<!-- post-merge: checked -->
transaction, on master, predating astubbs#29:

```java
// AbstractParallelEoSStreamProcessor.java:418-419 (master)
while (isTransactionCommittingInProgress())
    Thread.sleep(100); //wait for the transaction to finish committing
```

`isTransactionCommittingInProgress()` (`:1494-1496`) is gated on
`options.isUsingTransactionCommitMode()`, so this loop only runs in transactional mode - and there it
is the *common* case, not a rare race: the control thread takes the producer write lock in
`maybeAcquireCommitLock()` before committing.

The callback runs on the poll thread inside `poll()`, so it is bounded by `max.poll.interval.ms`.
Overrunning it evicts the member.

## Why this is not astubbs#29's deadlock <!-- post-merge: checked -->

The AB-BA cycle's second edge lives in `ConsumerOffsetCommitter`, which `BrokerPollSystem` constructs
**only** for the consumer-commit modes (`switch (options.getCommitMode())`, the
`PERIODIC_CONSUMER_SYNC, PERIODIC_CONSUMER_ASYNCHRONOUS` arm). In transactional mode there is no
request queue, no response queue and no `commitAndWait()` - **the cycle cannot occur here**.
<!-- post-merge: checked -->
astubbs#29's `tryLock()` change does not touch `:418-419` and cannot fix this.

Two different locks are both called "commit lock", which is part of why this was conflated: the
`commitCommand` monitor guarding consumer commit execution, and the producer transaction lock behind
`maybeAcquireCommitLock()` / `commitLockAcquisitionTimeout` (5 min default). This defect is the
latter.

## Sighting: `RebalanceEoSDeadlockTest`, 1 failure in 20, 2026-07-30

Local fork16 stress hunt on astubbs#80's branch (master-like code). Recorded in the original family
ledger as *"Live confirmation the deadlock is still present"* - see
`test-load-tightness-flakes.md`, where it is explicitly *not* a member.

**That attribution was wrong, and the correction is the point of this file.**
`RebalanceEoSDeadlockTest` runs `PERIODIC_TRANSACTIONAL_PRODUCER`
(`.commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)`), the mode in
<!-- post-merge: checked -->
which the AB-BA cycle cannot close. So the failure is **not** evidence for astubbs#29.

It is, however, a **real** failure and it is evidence for the block above. The run was on
master-family code, where the test's latch was still reachable - the latch-unreachable defect
(the revoke path calling the private `tryCommitOffsetsOnRevoke()` instead of the overridden
<!-- post-merge: checked -->
`commitOffsetsThatAreReady()`) only voids runs on **astubbs#29's branch**. So this sighting survives
the correction; only its attribution moves.

No seed was captured.

## Sighting: `ChaosRevokeUnderWorkTransactionalIT.revokeUnderWorkStaysProtocolHonestInTransactionalMode`, 1 failure in 2 runs, 2026-09-05

<!-- post-merge: checked - astubbs/parallel-consumer#448 is cited for its permanent diff content, which does not change after merge or if its branch is deleted -->
astubbs/parallel-consumer#448, a docs-and-data PR - entries added under `docs/data/`, markdown notes
edited, and one long-`@Disabled` sanity test deleted under `integrationTests/sanity/`
(`git diff --name-status <merge-base>..<head>` gives the shape). Failed in "Chaos Pain
Suite 2/4" on
[run 33938124400, job 101230384149](https://github.com/astubbs/parallel-consumer/actions/runs/33938124400/job/101230384149),
head `3ef5a009a`.

**Why the branch cannot have caused it**, on ground the deleted test file does not undermine - it is
a Java deletion, so "touches no Java" would be false and is not the argument. Nothing under
`src/main` changed and nothing in the `chaostests` package changed, so neither the product code under
test nor the scenario itself moved. Nor can removing a class reshuffle the shard: `.github/workflows/maven.yml`
gives each chaos shard a **hardcoded** `scenarios:` class list, passed through as `CHAOS_SCENARIOS`
(Suite 2/4 is `ChaosRevokeUnderWorkTransactionalIT,ChaosRevokeUnderWorkKeyOrderIT`), so shard
composition is fixed by that file rather than derived from what the tree contains. A class in the
`sanity` package, not `@Tag("chaos")`, was never selected by any shard and its removal changes none
of them.

**Failing condition:** `AbstractRevokeUnderWorkScenario.runRevokeUnderWorkScenario:283`, the
Awaitility condition aliased *"backlog drained after the storm settles (quiet phase)"* did not
complete within its 5-minute bound - `ConditionTimeout`, 366.8s elapsed. Seed
`7976335177229963841` (scenario `w4tx`, printed by `AbstractRevokeUnderWorkScenario`'s
`"=== CHAOS {} revoke-under-work (cooperative={}): seed={} (replay: {}) ==="` banner;
replay: `./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true -Dincluded.groups=chaos
-Dexcluded.groups= -Dchaos.seed=7976335177229963841`). The run's own `settleRun` summary reported
`probe violations=[]` - no gating probe fired; the failure is the drain-await itself timing out.
Several `CLASS2_STALL/LAG_STAGNATION` non-gating observations were logged in the same window
(per-partition lag stagnant ~154s against the 150s bound).

A rerun of the same job on the same commit
([job 101232514184](https://github.com/astubbs/parallel-consumer/actions/runs/33938124400/job/101232514184))
passed.

**This is this scenario's first recorded red.** Its own class javadoc records "Calibration status:
UNCALIBRATED" and, before this, only one prior run: GREEN in 144s on 2026-09-01 with
`probe violations=[]`, on the confluentinc#857 branch. The javadoc states the open question this
sighting bears on without resolving it - "It is not yet known whether it goes red on master, red
only under particular timing, or green because the revoke wait needs a sharper shape than this
family produces." **Not diagnosed here**, and no replay of this seed has been run - recorded only so
it is not lost with the run's logs.

## User-facing report

**astubbs#44 (confluentinc#803)** - *"Transactional Producer instance gets timeout getting commit lock
while second instance starts"* - matches this mechanism exactly: second instance joins, rebalance
fires, poll thread spins here, `max.poll.interval.ms` is breached, the group reports *"group is
already rebalancing"*, and the run ends on `commitLockAcquisitionTimeout`.

It carries upstream's *verified bug* label. **This note previously called it the ONLY such issue,
which is false** - a couple of dozen upstream issues carry that label, and this claim propagated
from here into a roadmap entry, a plan, several notes and a PR body before anyone ran
`gh issue list -R confluentinc/parallel-consumer --state all --label "verified bug"`. The label
still matters - it means a maintainer confirmed the report rather than merely triaging it - but
it does not make this issue unique. It was re-triaged off
<!-- post-merge: checked -->
astubbs#29 and onto this block on 2026-08-18; its `pr-available` label was removed, because no open
PR addresses it.

## Open decision - do not write code before settling it

The wait needs a deadline, and the obvious design is ruled out: the poll thread **cannot** abort the
transaction, because `ProducerManager` enforces single-writer from the control thread and throws
`ConcurrentModificationException` otherwise.

The candidate is to deadline the **holder** instead - bound the control thread, which owns the
transaction and can abort itself - rather than the revoke callback that merely notices the overrun.
Not agreed with the user.

Proceeding past the wait is separately unsafe until producer fencing is recoverable:
`ProducerFencedException` is wrapped in `InternalRuntimeException` and kills the instance. See
`next-recoverable-producer-fencing.md` and astubbs#225.

Branch `fix/bound-revoke-transaction-wait` exists with no code on it.
