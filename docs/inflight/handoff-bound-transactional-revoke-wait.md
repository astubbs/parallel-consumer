# Handoff: bound the unbounded transactional revoke wait (astubbs#44, confluentinc#857 family)

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-labels: concurrency -->

<!-- post-merge: checked-begin - written for an agent picking this up on a branch that carries no
     code, so every reference below is to something on master or to a named open PR, not to this
     branch's own state -->

For an agent starting with no context. **Read
[`bug-857-transactional-revoke-wait.md`](bug-857-transactional-revoke-wait.md) first and in full** -
it owns the defect, the mode discriminator, the user-facing report, and the open design decision.
This file exists to say where that work stands, what has been ruled out, and what must be settled
before any code is written. It does not repeat the note.

## The objective

`AbstractParallelEoSStreamProcessor.onPartitionsRevoked` waits with no deadline for an in-flight
transaction - the `while (isTransactionCommittingInProgress())` loop with its
`//wait for the transaction to finish committing` comment. It runs on the poll thread inside
`poll()`, so it is bounded only by `max.poll.interval.ms`, and overrunning it evicts the member.
Give that wait a deadline.

## The decision is settled - read it before changing the shape of the fix

It was open for weeks, and it is not open now. **Decline, do not deadline**, settled with Antony on
2026-09-01. [`bug-857-transactional-revoke-wait.md`](bug-857-transactional-revoke-wait.md) owns the
reasoning; what binds anyone editing this branch is the short form:

- **Do not reintroduce a wait of any length on the revoke path.** The measurement that killed the
  deadline-the-holder candidate is that 79s came out of a 20s dwell - the waiter is starved across
  *successive* transactions, so no per-transaction bound helps.
- **Do not make the poll thread abort the transaction.** `ProducerManager` enforces single-writer
  from the control thread and throws `ConcurrentModificationException` (grep `is not safe for
  multi-threaded access`).
- **astubbs#225 is not a blocker for this design, and was for the other one.** Declining performs no
  transaction operation at all, so recoverable producer fencing is not on its critical path. If you
  find yourself reaching for an abort, you have left the settled design.

## Current state

**The fix is written; nothing is pushed and no PR exists.** Settled 2026-09-01 with Antony: the
revoke path **declines** the producer commit lock rather than deadlining anyone. Both unbounded
waits are gone - the `Thread.sleep` spin is deleted, and the commit is attempted only once the lock
is already held, so `acquireCommitLock`'s five-minute wait is unreachable under contention. The
reasoning, the ruled-out alternatives and the Kafka Streams comparison are in
[`bug-857-transactional-revoke-wait.md`](bug-857-transactional-revoke-wait.md), which owns the
decision; this file only says where the work stands.

`Revoke857TransactionalWaitProbeIT` (added 2026-09-01) is now the **regression test** rather than a
one-way instrument: it grew a `revocationDeclines` counter, and its vacuity guard accepts either a
long wait (pre-fix) or a nonzero decline count (post-fix). Without that counter a fixed run and a run
where no commit was ever in flight look identical - the lesson the cluster-decomposition plan paid
for on the sibling defect.

- The branch is now **`fix/803-bound-transactional-revoke-wait`**, renamed from `fix/857-...` on
  2026-09-01: confluentinc#857 is the *other* defect, in the mutually exclusive commit mode, and the
  branch name was the last place still conflating them.
- `fix/bound-revoke-transaction-wait` is a separate **empty branch** cut from an old master - no
  commits, no PR. Delete it or reset it; it is not a starting point.

**Correction owed on `709e2a92b`.** Its commit message says prior art was checked including *"a git
grep of every local and remote ref"*. That sweep was `*.java` only, and it missed the cluster
decomposition plan, the commit-seam architecture write-up and a sibling defect note - all
branch-only. The claim overstates what was done. Not amendable without a history rewrite, so it is
recorded here instead; `node bin/prior-art.mjs` (astubbs#400) is the tool that closes the gap.

**No open PR addresses this**, verified 2026-08-31 by sweeping every open PR for an edit to the wait
loop, and re-checked 2026-09-01:

- astubbs/parallel-consumer#257 (transactional batches failing and reprocessing succeeded records)
  does not touch it - the keyword matches in its diff are context lines.
- astubbs/parallel-consumer#262 (prove or falsify every documented transactional guarantee) only
  *observes* `isTransactionCommittingInProgress()` in test assertions. It changes nothing here. It is
  merged into this branch as its base, and is now OPEN, non-draft and MERGEABLE - a `depends on
  astubbs/parallel-consumer#262` line belongs in this PR's body when one is opened.
- astubbs/parallel-consumer#317 on `feats/833-commit-failure-seam` adds a `tryCommitOffsetsOnRevoke`
  that declines - but for the **consumer** lane's commit-cycle monitor, leaving this loop untouched.
  Same rule, different lock. **This branch deliberately reuses that method name**, so if both land
  the collision is visible and they get unified rather than silently coexisting.
- astubbs/parallel-consumer#29 appears in a naive sweep, but the loop is textually identical to
  master there; it merely moved down the file when that PR wrapped the code below it in a try/catch.
  The mode discriminator in the note explains why astubbs#29 *cannot* fix this: the AB-BA cycle it
  fixes lives in a commit mode where this loop does not run.

## What is already known, so it is not rediscovered

- **astubbs#44 (confluentinc#803) matches this mechanism exactly** and is the only issue upstream
  ever labelled *verified bug*. It was re-triaged off astubbs#29 onto this defect on 2026-08-18 and
  its `pr-available` label removed. It is open.
- **`RebalanceEoSDeadlockTest` failing is evidence for THIS defect, not for astubbs#29.** It runs
  `PERIODIC_TRANSACTIONAL_PRODUCER`, the mode in which the AB-BA cycle cannot close. The note carries
  the sighting and the corrected attribution. **No seed was captured.**
- **Two different things are both called "the commit lock"** - the `commitCommand` monitor guarding
  consumer commit execution, and the producer transaction lock behind `maybeAcquireCommitLock()`.
  This defect is the latter. Conflating them is what produced the wrong attribution above.

## A verification method already exists - use it rather than inventing one

Do not try to reproduce this by replaying a captured chaos seed. The family ledger records, in bold,
that seed replay does not reproduce this class of defect: a chaos seed fixes the *conductor's*
schedule, not the thread interleaving these races turn on. That was re-derived the hard way on
2026-08-31 and cost an hour.

What works is a purpose-built probe that forces the window open, plus a control arm differing by one
term. `Rebalance857CommitSyncDeadlockProbeIT` on astubbs#29 is the worked example for the sibling
defect: it dwells in the revoke callback against a short commit interval, and its pre-fix control
fails every repetition while the fixed arm fails none. Copy the shape, not the file.

Two traps that voided runs of it, both cheap to avoid:

- **Confirm the arm actually engaged.** Log which configuration resolved and assert on it. A property
  that is silently unread produces two green arms that look like a passing fix.
- **Run with `-Dpc.log.level=info`.** The revoke path's log lines are filtered at the default test
  verbosity, and their absence is indistinguishable from the race never happening.

## Suggested first moves

Talk to the user about the holder-deadline design and about whether astubbs#262 should land first.
Only then write a probe that reproduces the overrun, and only then the bound. The bug has been
understood for weeks; what it lacks is an agreed design, not analysis.

<!-- post-merge: checked-end -->
