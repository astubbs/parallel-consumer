# confluentinc#857 family: is the transactional revoke wait bounded at the right value?

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- post-merge: checked - a dated vetting record, past tense against 2026-09-08, naming no branch whose deletion falsifies it -->
<!-- inflight-vetted: 2026-09-08 - applied: re-premised on astubbs#466, which removed the unbounded spin; the open question is now the bound itself, held by astubbs#408, and the four stale citations are repaired (the "no open PR" claim, the dead `fix/bound-revoke-transaction-wait` branch, the renamed fencing note, and the two file:line citations); checked: `AbstractParallelEoSStreamProcessor.commitOnRevokeViaTheControlThread` is what `onPartitionsRevoked` reaches in transactional mode and its wait is `getCommitLockAcquisitionTimeout()`, the `while (isTransactionCommittingInProgress()) sleep` spin is gone from the source, astubbs#408 was the open draft holding it on that date, and `fix/bound-revoke-transaction-wait` no longer existed on origin -->

**Commit mode: `PERIODIC_TRANSACTIONAL_PRODUCER` only.** This is the discriminator - the defect below
<!-- post-merge: checked -->
and the AB-BA deadlock in astubbs#29 are in mutually exclusive modes and cannot be the same bug.

## What this note used to be, and what it is now

It was about an **unbounded** wait: `onPartitionsRevoked` spun on
`while (isTransactionCommittingInProgress()) Thread.sleep(100)`, on the poll thread inside `poll()`,
with no deadline, in the mode where the control thread routinely holds the producer write lock. That
spin is gone. astubbs#466 (merged 2026-09-07) routes transactional revocation through
`AbstractParallelEoSStreamProcessor.commitOnRevokeViaTheControlThread`: the callback posts a request,
the control thread runs its ordinary lock-flush-drain-commit-fence sequence, and the callback waits
**bounded by `commitLockAcquisitionTimeout`**. A commit already in flight queues behind rather than
being waited on or declined, and on timeout the commit is declined at WARN.

<!-- post-merge: checked-begin - names astubbs#408 (a PR reference, permanent and resolvable) and
     states the measurement as a thing that happened, with no claim about any branch's existence or
     any PR's open/draft state -->
**So the open question is the bound, not the absence of one.** The five-minute default is the same
bound the inline commit's own write-lock acquisition already had - no regression - but
confluentinc#803's complaint is precisely that such a bound burns `max.poll.interval.ms`, and a
callback that overruns it evicts the member. astubbs#408 measured exactly that overrun and left the
bound alone: see "The measurement, and what it does not settle" below. The collision on
`tryCommitOffsetsOnRevoke` is resolved - astubbs#466 landed first and astubbs#408 took its design.
<!-- post-merge: checked-end -->

Two different locks are both called "commit lock", which is part of why this was conflated: the
`commitCommand` monitor guarding consumer commit execution, and the producer transaction lock behind
`maybeAcquireCommitLock()` / `commitLockAcquisitionTimeout` (5 min default). This note is the latter.

## Why this is not astubbs#29's deadlock <!-- post-merge: checked -->

The AB-BA cycle's second edge lives in `ConsumerOffsetCommitter`, which `BrokerPollSystem` constructs
**only** for the consumer-commit modes (`switch (options.getCommitMode())`, the
`PERIODIC_CONSUMER_SYNC, PERIODIC_CONSUMER_ASYNCHRONOUS` arm). In transactional mode there is no
request queue, no response queue and no `commitAndWait()` - **the cycle cannot occur here**.
<!-- post-merge: checked -->
astubbs#29's `tryLock()` change does not touch the transactional revoke path and never could fix it;
`tryCommitOffsetsOnRevoke` and its `commitLock.tryLock()` remain for the consumer-commit modes, where
confluentinc#857's cycle is real and the inline commit is correct.

## Sighting: `RebalanceEoSDeadlockTest`, 1 failure in 20, 2026-07-30

Local fork16 stress hunt on astubbs#80's branch (master-like code, long predating astubbs#466).
Recorded in the original family ledger as *"Live confirmation the deadlock is still present"* - see
`test-load-tightness-flakes.md`, where it is explicitly *not* a member.

**That attribution was wrong, and the correction is why this section is kept.**
`RebalanceEoSDeadlockTest` runs `PERIODIC_TRANSACTIONAL_PRODUCER`
(`.commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)`), the mode in
<!-- post-merge: checked -->
which the AB-BA cycle cannot close. So the failure is **not** evidence for astubbs#29.

It was, however, a **real** failure and it was evidence for the unbounded wait. The run was on
master-family code, where the test's latch was still reachable - the latch-unreachable defect
(the revoke path calling the private `tryCommitOffsetsOnRevoke()` instead of the overridden
<!-- post-merge: checked -->
`commitOffsetsThatAreReady()`) only voids runs on **astubbs#29's branch**. So this sighting survives
the correction; only its attribution moves.

No seed was captured.

## Sighting: `ChaosRevokeUnderWorkTransactionalIT.revokeUnderWorkStaysProtocolHonestInTransactionalMode`, 1 failure in 2 runs, 2026-09-05

Predates astubbs#466, so it is a sighting against the unbounded spin rather than against today's
bound - kept because it is this scenario's only red and no replay of its seed has been run.

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

**Failing condition:** the `diagnosableWait` in `AbstractRevokeUnderWorkScenario.runRevokeUnderWorkScenario`
aliased *"backlog drained after the storm settles (quiet phase)"* (grep that alias) did not
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
while second instance starts"* - matched the original mechanism exactly: second instance joins,
rebalance fires, poll thread waits here, `max.poll.interval.ms` is breached, the group reports
*"group is already rebalancing"*, and the run ends on `commitLockAcquisitionTimeout`. Whether
astubbs#466's bounded wait is short enough to keep that from recurring is the open question above.

It carries upstream's *verified bug* label. **Earlier versions of this note called it the ONLY such
issue, which is false** - a couple of dozen upstream issues carry that label, and the claim
propagated from here into a roadmap entry, a plan, several notes and a PR body before anyone ran
`gh issue list -R confluentinc/parallel-consumer --state all --label "verified bug"`. The label
still matters - a maintainer confirmed the report rather than merely triaging it - but it does not
make this issue unique. It was re-triaged off
<!-- post-merge: checked -->
astubbs#29 and onto this block on 2026-08-18. Its `pr-available` label was removed at the time
<!-- post-merge: checked - a dated fact plus a PR citation, both permanent -->
because no open PR addressed it; astubbs#408 was opened against it afterwards.

## The measurement, and what it does not settle

<!-- post-merge: checked-begin - every sentence is a measurement or a decision recorded in the past
     tense against a PR number, which stays resolvable after that PR closes and its branch is deleted -->
`Revoke857TransactionalWaitProbeIT.revokeMustNotWaitOnATransactionPastTheMaxPollInterval` is the
instrument, built on astubbs#408 to make the overrun observable. Against astubbs#466's delegated
commit it reports **19.2s of callback time out of a 20s in-flight dwell, against a 10s
`max.poll.interval.ms` budget, 5/5** - so the callback still holds the poll thread for as long as the
transaction runs, and a transaction longer than `max.poll.interval.ms` still evicts the member. That
is confluentinc#803's complaint, reproduced against the current design.

**What astubbs#466 did fix, and the probe confirms:** the callback is no longer *starved across
successive* transactions. The spin the confluentinc#548 code used measured **79s of callback time
from the same 20s dwell**, because a 1s commit interval let the control thread re-take the lock as
fast as it dropped it. Bounded delegation removes that multiplication; it does not remove the wait.

**What is not settled, and is deliberately not decided here.** Bounding the delegated wait needs a
value, and PC cannot read the consumer's own `max.poll.interval.ms` - so the bound needs either a new
option or a derivation, and the timeout fallback astubbs#466 already has (decline at WARN, with the
deferred-duplicate cost named) is what it would fall back to. That is a design choice with a
user-visible option surface, and it belongs to the owner rather than to a reconciliation pass.

**Declining unconditionally is NOT the answer, and this is the trap worth not re-deriving.**
astubbs#408 originally declined the revoke commit on the poll thread. astubbs#466 refuted that by
experiment: a revoke that commits nothing leaves its produced output in the open transaction for the
*next* commit to publish without the matching offset - the same duplicate through a different door.
Declining is the deadline fallback, logged at WARN, never the fix. Two arms in `ProducerManagerTest`
carry that result.

**Held for the owner's call, on astubbs#408's branch:** the three `ProducerManager` revocation lock
helpers, their unit tests, and the `DeclineCountingProducerManager` instrument. They count a decline
the transactional path no longer makes, and they are the seam a bounded-wait-with-decline design
would use.
<!-- post-merge: checked-end -->

## The constraint any further bound has to respect

The poll thread **cannot** abort the transaction, because `ProducerManager` enforces single-writer
from the control thread and throws `ConcurrentModificationException` otherwise. That is why
astubbs#466 deadlines the *holder* - the control thread, which owns the transaction and can abort
itself - rather than the revoke callback that merely notices the overrun, and any sharpening of the
bound has to keep that shape.

Proceeding past the wait is separately unsafe until producer fencing is recoverable:
`ProducerFencedException` is wrapped in `InternalRuntimeException` and kills the instance. See
[`core-recoverable-producer-fencing.md`](core-recoverable-producer-fencing.md) and astubbs#225.

## Adjacent, and NOT this: the revoke commit that did not drain the mailbox

The same method this note bounds - `tryCommitOffsetsOnRevoke` - had a second, independent defect:
when it did commit, it committed without first draining the work mailbox, so a revoke-time
transaction could omit the offset of a record it already produced. Diagnosed in astubbs#436 and
fixed by astubbs#466, the same change that re-premised this note; the record is
[`docs/solutions/logic-errors/the-revoke-path-commit-did-not-drain-the-mailbox-2026-09-07.md`](../solutions/logic-errors/the-revoke-path-commit-did-not-drain-the-mailbox-2026-09-07.md).
