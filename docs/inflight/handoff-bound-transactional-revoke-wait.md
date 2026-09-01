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

## Read this before forming a plan - a decision is open and it is not yours to make alone

The note's "Open decision - do not write code before settling it" section is the reason this has no
code. The obvious design is already **ruled out**: the poll thread cannot abort the transaction,
because `ProducerManager` enforces single-writer from the control thread and throws
`ConcurrentModificationException` otherwise. The live candidate is to deadline the **holder** - bound
the control thread, which owns the transaction and can abort itself - rather than the revoke callback
that merely notices the overrun. **That candidate is recorded as not agreed with the user.** Settle
it with them before writing code.

There is also a blocker sitting underneath it: proceeding past the wait is unsafe until producer
fencing is recoverable, because `ProducerFencedException` is wrapped in `InternalRuntimeException`
and kills the instance. See
[`core-recoverable-producer-fencing.md`](core-recoverable-producer-fencing.md) and astubbs#225,
which is open.

## Current state

**A reproducer exists; the fix does not.** `Revoke857TransactionalWaitProbeIT` (added 2026-09-01)
forces the window open and measures the overrun - 5/5 fail on the defect arm at 79s against a 10s
`max.poll.interval.ms`, 5/5 pass on the control arm. It is an instrument, expected to fail, and is
not wired into any lane. No main-code change exists, and **the design decision below is still
open** - though the probe's 79s result now rules out bounding a single transaction as a fix.

- `fix/bound-revoke-transaction-wait` is an **empty branch** cut from an old master - no commits, no
  PR. It is held by the `.claude/worktrees/revoke-wait` worktree (machine-local). Either reset it
  onto master or delete it; do not assume it contains a starting point.
- This branch, `fix/857-bound-transactional-revoke-wait`, carries documentation only.

**No open PR addresses this**, verified 2026-08-31 by sweeping every open PR for an edit to the wait
loop:

- astubbs/parallel-consumer#257 (transactional batches failing and reprocessing succeeded records)
  does not touch it - the keyword matches in its diff are context lines.
- astubbs/parallel-consumer#262 (prove or falsify every documented transactional guarantee) only
  *observes* `isTransactionCommittingInProgress()` in test assertions. It changes nothing here.
- astubbs/parallel-consumer#29 appears in a naive sweep, but the loop is textually identical to
  master there; it merely moved down the file when that PR wrapped the code below it in a try/catch.
  The mode discriminator in the note explains why astubbs#29 *cannot* fix this: the AB-BA cycle it
  fixes lives in a commit mode where this loop does not run.

**The sequencing argument for waiting is real but is not coverage.** astubbs#262 sets out to prove or
falsify the documented transactional guarantees, so it may well establish what the correct bound is
and reframe the design. That is a reason to talk to it first - not a reason to believe it is being
handled.

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
