---
artifact_contract: "ce-handoff/v1"
created_at: "2026-09-01T04:20:00Z"
title: "The rebalance-path blocking-call hunt, beyond the confluentinc#857 fix"
summary: "One member of the blocking-on-the-poll-thread defect class is fixed, a second is found and open on master, and the static gate that covers the class cannot see the shape of the original bug."
keywords: ["deadlock", "rebalance", "AB-BA", "RetryQueue", "ArchUnit", "confluentinc-857", "poll-thread", "defect-class-sweep", "transport-branch"]
cwd: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/pr29"
resume_focus: "Decide the fix shape for RetryQueue's unbounded write-lock acquire on the rebalance path - the one open member of the class - or close the static gate's blind spot to synchronized blocks."
repository: "astubbs/parallel-consumer"
repo_root_sha: "713c7468d5ecda55a2190d38e698cda017e91259"
branch: "bugs/857-paused-consumption-multi-consumers-bug"
head: "acaa437aa"   # updated 2026-09-01; was 605116004 when first written
worktree_path: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/pr29"
---

---

# ADDENDUM, 2026-09-01 - read this before the body above

The body is still accurate about the defect class. Six things happened after it was written, and two
of them change what the next agent should *not* bother doing.

## The one open member is unchanged, and is still the resume focus

`RetryQueue`'s unbounded `writeLock().lock()`, reachable from every rebalance callback, is still open
and unowned. Nothing below supersedes that.

## A seed replay was run, came back CLEAN, and settles nothing

Seed `1170511790377175835` (a `ChaosRevokeUnderWorkDrainIT` kill captured in the wild) was replayed
against master in a throwaway worktree. Classified from the failsafe report, not the exit code:
`tests="1" errors="0" skipped="0" failures="0"`, 160.2s, no probe observations. **It did not
reproduce.**

**Do not read that as evidence, and do not spend another run on it.** A chaos seed fixes the
conductor's schedule, not the poll-versus-control interleaving, so the instrument cannot force the
race this family turns on - which is exactly why the assignor question was settled on the
deterministic probe instead. One clean replay of a race the seed cannot pin is a sample, on a laptop,
where the original failure was on a loaded CI runner.

**The more useful artifact from that exercise is the false green.** The first attempt omitted
`-Dfailsafe.failIfNoSpecifiedTests=false`, so the `-Dit.test` filter matched nothing on the parent
module, **no failsafe report was written at all**, and the run still had to be classified from the
report to notice - the exit code alone read as a pass. Always classify from the report.

## The ledger gained a sixth Drain capture, and refuted three novelty claims

`docs/inflight/bug-857-family.md` now carries a 2026-09-01 entry. Its substance for a newcomer: the
capture was **not** a new class. The 2026-08-26 *fifth capture* is the same Drain test, the same
`AtomicBoolean` monitor and holder, and is already a not-PR-introduced control arm. A handoff
claiming novelty on scenario, signature and clean tree was wrong on all three - by a session that had
read the file's opening and its eleventh sighting but not the 2026-08-26 series.

**The concrete trap it fell into is one you can still fall into:** it "corrected" master's line 580 to
the control-thread side. At master, 580 is the `commitOffsetsThatAreReady()` call *inside*
`onPartitionsRevoked` - the POLL side - and 1775 is the `synchronized (commitCommand)` acquisition
where the control thread holds across the blocking `retrieveOffsetsAndCommit()`. The fifth capture's
own paragraph says it outright: **match captures by method and monitor, never by line number.**

## The performance-lane failure is a SEPARATE defect, and one suspect is already eliminated

Recorded in `docs/inflight/test-857-branch-red-lanes-cause-unestablished.md`. The prediction that it
shared the pause-cache cause is **refuted** - the lane still fails with that fix in. Measured, same
test and configuration: a test-scope branch off master passes at `recordsPerSecond=71387`;
astubbs/parallel-consumer#29 fails at `36184`, not completing.

astubbs/parallel-consumer#393's lane passes and its thread-confinement refactor is in #29's tree, so
that work is ruled out **without running an experiment** - a branch cut out of a suspect tree is a
control arm for whatever stayed behind. The cheapest untried step is ablating the revoke fork's
INFO-level logging.

## Three merge-time tasks now sit on astubbs/parallel-consumer#29

All in that PR's inflight note, and all silent failures if missed:

1. **Six scripts get duplicated, and git will not flag it.** astubbs/parallel-consumer#381 renames
   every experiment runner to an `exp-` prefix and lands first; #29 still carries the originals, so
   the merge sees six *new paths* rather than conflicts and master ends up with both copies.
2. **`getAssignmentSize` was deleted by a dependency and #29 still calls it** - a compile error, so
   the good case. The resolution is not to revert the deletion: the survivor is a diagnostic on a
   failure path.
3. **`getConsumerClass` must NOT come back the way it left.** It could never have worked
   (`Object.getClass()` on a wrapper), and restoring it during conflict resolution yields a check
   that always takes the not-a-known-driver path - defended on paper, undefended in fact.

## Two things in the wider hunt closed

The `grep -c ... || echo 0` two-line-capture defect class is **closed** - the last latent instance
merged as `0a97a5a80`. And two async-drain experiment runners were **retired** on
astubbs/parallel-consumer#381 because their question was answered; their method is written up under
`docs/solutions/test-flakiness/` (more firings beat more seeds; a stopping condition must be the
answer, never the engagement).


# The rebalance-path blocking-call hunt

## What the class is

**A rebalance callback runs inside `poll()` on the broker-poll thread. Anything it does that can wait
unboundedly can stall the consumer, and if the thing it waits for is held by a thread that is itself
waiting on the poll thread, the cycle closes and nothing moves again.**

This is the defect class. confluentinc#857 was one member of it. The hunt described here is for the
others.

Read `parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/ArchitectureTest.java` -
grep `rebalanceCallbacksMustNotBlock` - for the rule that encodes the class, and its
`KNOWN_BLOCKING_VIOLATIONS` set for every instance currently tolerated.

## Status of each member found so far

**FIXED - the `commitCommand` monitor cycle (confluentinc#857).** The revoke path now declines the
commit lock rather than blocking on it. Grep `tryCommitOffsetsOnRevoke` in
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java`;
it `tryLock()`s and gives up. The control thread's own path still takes `commitLock.lock()`
unconditionally, which is correct - that thread is allowed to wait. Verified on a four-cell control
(pre-fix arm red 20/20, fixed arm green 20/20, on both the eager and the cooperative assignor).
Shipped on PR astubbs/parallel-consumer#29, still open and in draft.

**CLEARED, with the reasoning recorded at the site - the three remaining `synchronized (commitCommand)`
blocks.** In the same file, grep `Cleared suspicion, 2026-08-31`. Three holders remain
(`requestCommitAsap`, `isCommandedToCommit`, `clearCommitCommand`). The discriminator is what a
holder *does*, not that it holds: each performs one `AtomicBoolean` get or set, so no hold spans a
wait and no edge exists for a cycle to close on. **The javadoc states what would reopen it** -
anything blocking added inside those three blocks, or a fourth holder that waits while holding.
Nothing gates this; the reasoning is the only protection.

**OPEN, live on master, unowned - `RetryQueue`'s write lock on the rebalance path.** This is the one
piece of unfinished work in the hunt.
`docs/inflight/bug-retry-queue-write-lock-on-the-rebalance-path.md` owns it and should be read first.
Summary of what is established: `RetryQueue.remove(WorkContainer)` takes
`lock.writeLock().lock()` - unbounded, blocking - and is reachable from **every** rebalance callback,
through `WorkManager` and `PartitionStateManager` as well as `AbstractParallelEoSStreamProcessor`
directly. Six root paths are listed as exemptions in `KNOWN_BLOCKING_VIOLATIONS`. It predates every
open PR; the sweep only surfaced it once the ArchUnit deny list learned to recognise
`ReentrantReadWriteLock`.

**TOLERATED, pre-existing - `Thread.sleep` inside `onPartitionsRevoked`.** First entry in
`KNOWN_BLOCKING_VIOLATIONS`. Not investigated during this hunt.

**DIFFERENT DEFECT, blocked on a decision that is the user's - the unbounded transactional revoke
wait.** `docs/inflight/bug-857-transactional-revoke-wait.md`, tracked as
astubbs/parallel-consumer#44. The cycle broken above cannot close in transactional mode at all,
because the second edge lives in a committer built only for the consumer-commit modes. **Its fix is
blocked on an open design decision, not on effort - do not start it without the user.**

## What was checked and found sound - do not re-derive these

- **`ConsumerOffsetCommitter`** waits with `commitResponseQueue.poll(commitTimeout, MILLISECONDS)`.
  Bounded. This is the safe shape.
- **`ExternalEngine`** uses `dispatchCeiling.tryAcquire(records, DISPATCH_CAPACITY_SHUTDOWN_POLL_MS,
  MILLISECONDS)`. Bounded. Also the safe shape.
- **Module scope.** Every rebalance callback in the tree lives in core main code -
  `AbstractParallelEoSStreamProcessor`, `PartitionStateManager`, `WorkManager`. Nothing in vertx,
  reactor or mutiny implements one, so nothing outside core is unswept today. The ArchUnit rule
  imports `bz.stub.parallelconsumer` whole (see its `@AnalyzeClasses`), so a new callback added in
  another module would still be covered.

## The gate's blind spot - the most important thing here

**`rebalanceCallbacksMustNotBlock` matches method CALLS. A `synchronized` block is a `MONITORENTER`
instruction, which ArchUnit cannot see. The rule that exists for this defect class would not have
caught confluentinc#857 itself.**

It does catch the `RetryQueue` shape, and would catch a newly added `Thread.sleep`, `lock()`,
`Future.get`, `BlockingQueue.take` and the rest of the widened deny list. It cannot catch the
original bug's shape. This is stated in the rule's own javadoc and again at `clearCommitCommand`.

There is a partial runtime counterpart: `PollThreadStallDiagnosis` asks the JVM for a cycle via
`ThreadMXBean`, preferring `findDeadlockedThreads()` (which covers `ReentrantLock` too) and falling
back to `findMonitorDeadlockedThreads()`. But it only speaks once a cycle has actually closed at
runtime, and it is currently reached only from `ConsumerOffsetCommitter` plus its own unit test - it
is a diagnosis aid, not a gate.

## The open question the next agent will hit immediately

**The `RetryQueue` fix is not a mechanical `tryLock` swap, and assuming it is will produce a wrong
patch.** Declining a `remove()` during a revoke leaves work for partitions the instance no longer
owns sitting in the retry queue. Whether that is tolerable depends on whether the epoch check filters
those containers before anything acts on them - which is not established, and is the first thing to
determine. Both possible answers lead somewhere real: if epoch filtering already covers it, declining
is safe and cheap; if it does not, the work has to move off the poll thread instead, which is a
larger change.

One detail the inflight note does not mention and which should be verified rather than trusted:
`RetryQueue` takes its write lock in **four** methods - `clear()`, `add()`, `remove()` and
`removeAll()` - not only `remove()`. The exemption list names the paths that reach it today; whether
a rebalance callback can reach `removeAll()` or `clear()` as well is worth confirming before
choosing a fix shape. Call sites are in `state/ProcessingShard.java` and `state/ShardManager.java`
(grep `retryQueue.`).

## Machine-local state, and a warning

- The worktree at `/Users/astubbs/github/parallel-consumer/.claude/worktrees/pr29` holds branch
  `bugs/857-paused-consumption-multi-consumers-bug`, which has **open PR
  astubbs/parallel-consumer#29** against it. Renaming or deleting that branch auto-closes the PR.
  The branch is pushed and clean as of HEAD `605116004`.
- `.worktree-owner` in that directory records a local `stash@{0}` holding superseded drafts. Nothing
  in this handoff depends on it.
- A hand-written squash message for #29 lives in this session's scratchpad, machine-local and
  untracked. It is deliberately NOT in the PR body, per `docs/merge-checklist.md`.
- **This handoff is orientation only. The durable record is in the repo** - the inflight notes named
  above, and the javadoc at each cleared site. If those and this document disagree, they win.

## Verification performed

- `bin/check-all.sh`: 15 ran, 15 passed, 0 failed, 5 skipped.
- PR #29 CI at HEAD: `Integration Tests` SUCCESS (red for weeks before this), `claude-review`
  SUCCESS, `review: human LGTM` SUCCESS, unit/static/mutation/hygiene SUCCESS. `Performance Tests`
  FAILURE, unattributed pending a master baseline from astubbs/parallel-consumer#381.
  `Check PR Dependencies` FAILURE by design while #381 and #393 are unmerged. `Chaos Pain Suite`
  was IN_PROGRESS at capture.

## Where to start reading

1. `docs/inflight/bug-retry-queue-write-lock-on-the-rebalance-path.md` - the open item, in full.
2. `ArchitectureTest.java`, grep `rebalanceCallbacksMustNotBlock` - the rule, its deny list, and its
   documented blind spot.
3. `AbstractParallelEoSStreamProcessor.java`, grep `Cleared suspicion, 2026-08-31` - why the
   remaining monitors are not a residual edge, and what would reopen them.
4. `docs/solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md`
   - the method lesson from this hunt: a detector that could not have fired proves nothing.
