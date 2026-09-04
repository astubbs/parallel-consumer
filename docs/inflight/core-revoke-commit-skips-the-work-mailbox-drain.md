# The revoke-path commit does not drain the work mailbox first

<!-- inflight-type: bug -->
<!-- inflight-impact: data-loss -->
<!-- inflight-labels: concurrency -->
<!-- inflight-state: open - diagnosed and machine-checked; the FIX is a thread-ownership decision, not a line -->

**Pre-existing on master, not introduced by astubbs/parallel-consumer#408.** Found while disproving a
P0 raised against that PR, and recorded because the disproof turned up a real thing one step over.

The control loop commits like this: take the producer write lock, **drain the work mailbox**, then
commit. Draining first is what makes the transactional guarantee hold - a record already produced
into the open transaction has its success sitting in the mailbox, and only the drain marks its
partition dirty so `collectCommitDataForDirtyPartitions` includes its offset. Produce and offset then
commit atomically.

The revoke path takes the same lock and commits, but **never drains**. So a revoke-time commit
publishes a transaction whose offsets omit work whose success is still queued: the output is
committed, the input offset is not, and the next owner reprocesses that input and produces the output
again. Exactly-once degrades to at-least-once, silently.

## The open question is now settled: the window IS reachable

The earlier version of this note recorded reachability as **NOT established**, and offered a
plausible reason it might be unreachable:

> The produce read lock is held across the send and its acks, and a commit cannot start while any
> read lock is held, so the mailbox may in fact be empty of *produced* work whenever a commit can
> begin.

**That argument is backwards, and finishing it inverts the conclusion.** The ordering is
send -> `addToMailbox` -> release produce read lock, not send -> release -> mailbox.
`AbstractParallelEoSStreamProcessor#cleanUpContext` is the single release point, it runs in
`runUserFunction`'s `finally`, and its own javadoc states the contract: *"Only unlock our producing
lock once every WorkContainer of this context has been safely returned to the controller's inbound
queue ... until all produce locks have been returned, inbound queue processed, and thus their
representative offsets placed into the commit payload."*

So:

- produce read lock returned **=>** that record's success is **already in the mailbox**;
- commit write lock granted **=>** every produce read lock returned;
- therefore a commit that can begin at all **always has that work in front of it**, undrained.

The lock discipline does not close the window. It **guarantees the window is open**. What closes it
is the drain, and only the control loop performs one. The two halves of the contract are
`maybeAcquireCommitLock` and `processWorkCompleteMailBox`; `tryCommitOffsetsOnRevoke` has the first
and not the second.

The single-route claim that makes this bite: `PartitionState#onSuccess` is the only thing that marks
a partition dirty on a success, and in main it is reachable only from
`WorkManager#handleFutureResult`, whose only caller is `processWorkCompleteMailBox`. Grep
`processWorkCompleteMailBox` in `AbstractParallelEoSStreamProcessor` - control and close paths, never
the revoke one.

## The controlled experiment

Prediction stated before the run: with the partition dirty from a drained offset-0 success, offset 1
produced into the same still-open transaction and its success left undrained, and no produce lock
held, a revoke-time commit sends `{partition -> offset 1}` rather than `{partition -> offset 2}`.

| Arm | One term changed | Result |
|---|---|---|
| `aRevokeTimeCommitIncludesTheOffsetOfEveryRecordItAlreadyProduced` | revoke commits as it does today | **RED 5/5** - sends offset 1, omitting the offset of a record inside the transaction it just committed |
| `aRevokeTimeCommitIncludesThatOffsetWhenTheMailboxIsDrainedFirst` | `processWorkCompleteMailBox(ZERO)` inserted immediately before the revoke, nothing else | **GREEN** - sends offset 2 |

Same magnitude, different position: the drain is the only term that moves, so the outcome is
attributable to it and not to added latency or to anything else the revoke path does. The prediction
held exactly. Both arms live in `ProducerManagerTest`, beside the C9 proofs they extend; the red one
carries `@Quarantined` with this file as its `tracking`.

Reproduction rate: 5/5, deterministic, hand-driven control loop on a mocked producer - no broker, no
load, no timing. This is not a flake and must not be treated as one.

## What this does to the register

`TransactionalClaim.NO_PRODUCE_WITHOUT_ITS_OFFSET` (C9) moves `PROVED` -> `REFUTED`. The claim is
written as a property of the system - *"The system must prevent records from being produced to the
brokers whose source consumer record offsets has not been included in this transaction"* - and one
reachable commit path breaks it. It remains proved on the control-loop path, with its original
observed negative control; the register records both halves rather than replacing one with the other.

The documented sentence in `ParallelConsumerOptions` is deliberately **left alone**. `Status.REFUTED`
says the disposition - correct the docs or file the defect - is a triage decision, and this note is
the defect being filed. Softening the promise instead would be the wrong half of that choice to take
unilaterally.

`STRATEGY.md` and the README's machine-checked list are corrected in the same change, because
`STRATEGY.md` said in terms that no claim in the register is refuted.

## Why there is no fix here

**The one-line fix is a trap, and the repo has already paid for it once.** The revoke callback runs
on the **broker-poll thread**; `processWorkCompleteMailBox` mutates `WorkManager` and
`PartitionStateManager` state that every other mutation reaches from the **control thread**. Calling
the drain from `onPartitionsRevoked` is precisely the shape of change that corrupted
`numberRecordsOutForProcessing` in astubbs/parallel-consumer#29 - a revoke-path fix that made a
single-threaded counter cross-thread, measured at `-8, -16, -20, -20, -20` against a truth of 0.
[`docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`](../solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md)
owns that history and names this exact hazard: *"Mode-conditional thread topology is the root
hazard. Every incident happened where transactional mode put a required action on the thread that
does not own the needed client."*

Note also that the control loop already declines to commit during a rebalance -
`shouldTryCommitNow = isTimeToCommitNow() && wm.isDirty() && !isRebalanceInProgress.get()` - so the
two commit paths are mutually exclusive by design, but **the drain is not gated by that flag**: the
control thread keeps draining while a revoke is in progress. A poll-thread drain would race it.

Candidate dispositions, none of them free, in rough order of how well they fit the existing design:

- **Decline instead of committing.** `tryCommitOffsetsOnRevoke` already has a documented-safe decline
  branch for the contended case (*"Uncommitted offsets will be re-delivered to the new assignee"*).
  Extending it to "decline when undrained work is queued" trades a wrong offset map for no commit,
  which is the outcome the design already accepts. The catch: the check has to sit **after** the
  write lock is taken, because only then is the mailbox stable with respect to produced work -
  checking in `onPartitionsRevoked` before `retrieveOffsetsAndCommit` leaves a narrower version of
  the same race, and a fix that narrows a data-loss window without closing it is worse than none,
  because it reads as closed.
- **Move the drain inside the commit sequence**, so both commit initiators get it by construction
  rather than by each remembering. This is where it belongs, and it is a change to
  `AbstractOffsetCommitter`/`ProducerManager`, not to the revoke path - but it still leaves the poll
  thread executing it.
- **Fix the ownership**, which is the answer the archaeology keeps arriving at:
  confluentinc#200 / astubbs/parallel-consumer#142, *shared nothing*. Out of scope for anything
  smaller than its own piece of work.

Picking between these is a design decision about thread ownership at the commit seam, and the
precedent for splitting it out is this suite's own: *"a main-code correctness fix deserves its own
change and its own reviewer"* (astubbs/parallel-consumer#262's residuals commit).

## What is still not established

- **Field impact.** No broker-level reproduction was attempted; the proof is in-process against a
  mocked producer. What the unit arms establish is that the offset map is wrong, which is upstream of
  any observable duplicate - but the size of the practical window under a real rebalance, and how
  often a revoke lands with undrained produced work, is unmeasured.
- **Whether astubbs/parallel-consumer#408 narrows or widens it.** Declining more often means
  committing on revoke less often, which would make it rarer - still nobody measured.
- **Whether `onPartitionsLost` has the same gap.** It does not commit at all, so it looks unaffected,
  but it was not the subject of this work and was not tested.
- **Relationship to astubbs/parallel-consumer#173 (confluentinc#777),** *"Handling Partition
  Revocation in Parallel-Consumer Leading to Duplicate Event Processing"*, which is open and reports
  this symptom from the field. This mechanism is a candidate cause; attribution needs its own
  experiment, and the two must not be conflated on the strength of matching symptoms.
