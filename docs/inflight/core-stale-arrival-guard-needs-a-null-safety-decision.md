# Rejecting a stale ARRIVAL at the shard - blocked on a null-safety decision

<!-- inflight-type: task -->
<!-- inflight-labels: concurrency -->
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
   **The third is not named, and recovering it means re-running the prototype** - the guard was not
   kept, so the list cannot be re-derived by reading. Name it in this note when you do, since
   identifying which fixtures encode a real production shape is exactly the decision below.
2. **It makes the stale-resident branch unreachable from every public entry point.** Verified by
   experiment: with the arrival guard in place and the resident branch reverted, the other
   regression tests still pass. So the branch astubbs#31 added becomes defensive code with no
   reachable coverage unless it keeps a white-box test that plants a resident directly (that test
   exists, on master).

## Infer's Pulse says this is many call sites, not one

Measured 2026-08-25 with every Infer checker enabled (see
[`docs/inflight/static-infer-findings.md`](./static-infer-findings.md);
Pulse is disabled in CI, so none of this is visible today). `getPartitionState`'s unguarded
`partitionStates.get(tp)` is dereferenced **without a null check at many call sites** across
`PartitionStateManager` itself, plus `ProcessingShard.isWorkContainerStale` and
`WorkManager.checkIfWorkIsStale`. Re-run `bin/infer-test.sh`, or read the `NULLPTR_DEREFERENCE`
entries for those classes in `config/infer-known-findings.txt`, for today's exact list - it moves as
findings are fixed, so a count written here would be stale the first time one is.

**This does not change the decision below; it changes its scope.** The question was framed as "should
the arrival guard add a null check", which invites a one-site answer. Many callers already assume
non-null, so whichever way it goes is a **policy about the method** - either it never returns null
(and something upstream guarantees that), or it is `@Nullable` and every call site needs to say what
it does about it. A null check added at the arrival guard alone would leave the rest reading as
correct while resting on the same unstated assumption.

It also explains why NullAway is silent on all of them: it reasons from annotations, and
`getPartitionState` carries none, so it is assumed non-null and never questioned. Pulse infers across
the program instead. That difference is the reason to read this list rather than trust the green
NullAway lane.

## The decision needed

- Treat an absent `PartitionState` as **not stale** (fail open, preserving today's behaviour), or
  make the absence an **error** (fail closed, and fix the fixtures)?
- Do those three fixtures encode a **real production shape** - a poll registering against a
  partition the manager does not know about - or are they only a test shortcut? That question
  decides the first one.

Until both are answered, this stays parked. The mechanism it guards is documented in
`PartitionState#epochIsStale`'s three-checkpoint javadoc, which also records why re-checking per
record or consulting the live epoch closes nothing.

## 2026-09-03: a fourth fixture, found by a harness rather than by the prototype

Added beside the text above rather than over it - the decision is unchanged, its evidence is not.

<!-- post-merge: checked-begin -->
`ShardManagerLincheckTest` reached `ProcessingShard.isWorkContainerStale`'s unguarded deref of
`PartitionStateManager.getPartitionState` and reported `NullPointerException` in CI. It is the same
shape as the three fixtures named above - a harness driving a `PCModuleTestEnv` whose partition was
never assigned - and it had simply never taken the branch before: the branch needs an arrival to find
a RESIDENT at its offset, and until the declining revoke sweep landed (astubbs#431) a sweep always
removed the resident and the empty shard with it. A sweep that DECLINES leaves the resident in place,
so the next add takes the branch.

Fixed as a fixture - the harness now assigns its partition, which is what its own constructor claims
to model - so this note's policy question is untouched. What it adds is that the unguarded call is
reachable from a *state the product deliberately creates*, not only from a test shortcut, which is a
datum for the second bullet under "The decision needed".
<!-- post-merge: checked-end -->
