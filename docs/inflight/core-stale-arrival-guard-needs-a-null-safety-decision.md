# Rejecting a stale ARRIVAL at the shard - blocked on a null-safety decision

<!-- inflight-type: task -->
<!-- inflight-impact: reliability -->
<!-- inflight-state: blocked - needs a null-safety decision -->


**Parked, needing a judgement call rather than more investigation.** The work is understood and was
prototyped; it is not filed here because it is hard, but because shipping it means deciding
something only the maintainer should decide.

## What it would do

`ProcessingShard.addWorkContainer` checks whether the **resident** is stale (that is
astubbs/parallel-consumer#31, merged). It does **not** check whether the **arrival** is stale, so
old-epoch containers still enter shards whenever a rebalance lands after the once-per-batch
`epochIsStale()` guard. They are harmless today - `couldBeTakenAsWork` refuses them and
`getWorkIfAvailable` evicts them inline - but rejecting them at the insert would prevent the churn
rather than clean it up.

## Why this reads as astubbs#31's, and what astubbs#31 actually settled

**It surfaced during astubbs/parallel-consumer#31's review and was deliberately deferred out of it** -
which is why anyone remembering it reaches for that number. astubbs#31 is merged, and it settled the
**resident** half only: `addWorkContainer` now replaces a stale entry already sitting at the offset.
Verified against master, where `addWorkContainer` tests `isWorkContainerStale(existing)` and nothing
tests the arrival. The **arrival** half is this note, and it is untouched.

It is also the "why not just add a null check - no partition means the work is stale, so drop it?"
question, asked in review and never answered. The answer is not "no"; it is the decision below, which
that question runs straight into. This note is that conversation, written down.

## The two measured reasons it is not a one-liner

1. **It NPEs three existing tests.** The guard runs on *every* add, where the resident check runs
   only when an entry already exists - so it evaluates inputs the current code never reaches.
   `PartitionStateManager.getPartitionState` returns `partitionStates.get(tp)` **unguarded**, and
   `PartitionStateCommittedOffsetTest` registers polls against a `PartitionState` that was never
   installed in the manager. `compactedTopic`, `committedOffsetLower` and one more fail with an NPE.
2. **It makes the stale-resident branch unreachable from every public entry point.** Verified by
   experiment: with the arrival guard in place and the resident branch reverted, the other
   regression tests still pass. So the branch astubbs#31 added becomes defensive code with no
   reachable coverage unless it keeps a white-box test that plants a resident directly (that test
   exists, on master).

## The decision needed

- Treat an absent `PartitionState` as **not stale** (fail open, preserving today's behaviour), or
  make the absence an **error** (fail closed, and fix the fixtures)?
- Do those three fixtures encode a **real production shape** - a poll registering against a
  partition the manager does not know about - or are they only a test shortcut? That question
  decides the first one.

Until both are answered, this stays parked. The mechanism it guards is documented in
`PartitionState#epochIsStale`'s three-checkpoint javadoc, which also records why re-checking per
record or consulting the live epoch closes nothing.
