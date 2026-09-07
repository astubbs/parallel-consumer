# The revoke-path commit does not drain the work mailbox first

<!-- inflight-type: bug -->
<!-- inflight-impact: data-loss -->
<!-- inflight-labels: concurrency -->

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
| `aRevokeTimeCommitIncludesTheOffsetOfEveryRecordItAlreadyProduced` | revoke commits as it does today | **RED** - sends offset 1, omitting the offset of a record inside the transaction it just committed |
| `aRevokeTimeCommitIncludesThatOffsetWhenTheMailboxIsDrainedFirst` | `processWorkCompleteMailBox(ZERO)` inserted immediately before the revoke, nothing else | **GREEN** - sends offset 2 |

Same magnitude, different position: the drain is the only term that moves, so the outcome is
attributable to it and not to added latency or to anything else the revoke path does. The prediction
held exactly. Both arms live in `ProducerManagerTest`, beside the C9 proofs they extend; the red one
carries `@Quarantined` with this file as its `tracking`.

**Deterministic, and it must not be treated as a flake.** The arm drives the control loop by hand
against a mocked producer - no broker, no load, no timing - so it is red on every run rather than
some of them, and the number of runs behind that is not the finding. Re-run it with
`bin/quarantined-test.sh` (the red arm is `@Quarantined`, so the ordinary unit lane skips it; its
control-arm sibling runs there).

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
  which is the outcome the design already accepts.

  **The catch is that a mailbox-emptiness test does not close the window, wherever it is placed, and
  an earlier draft of this note said it did once taken after the write lock.** Taking the write lock
  stabilises the mailbox against *new* produced work - no producer can take the read lock - but not
  against the control thread *emptying* it: `processWorkCompleteMailBox` does
  `workMailBox.drainTo(results, size)` into a local queue and only then loops calling
  `wm.handleFutureResult`, which is what reaches `PartitionState#onSuccess` and marks the partition
  dirty. A poll-thread check landing in that gap sees an empty mailbox and a partition not yet
  dirty, and commits the same incomplete offset map. Nothing gates that drain during a rebalance
  (above), so the gap is live exactly when the revoke path runs. So this candidate is only safe if
  it either declines unconditionally in transactional mode, or coordinates with the control-thread
  drain so the two cannot interleave - and a fix that narrows a data-loss window without closing it
  is worse than none, because it reads as closed.
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
- **astubbs/parallel-consumer#408 neither narrows nor widens the reachable case - checked, 2026-09-07.**
  That PR declines the commit only when the transaction lock is *contended*; its own amended
  `RebalanceEoSDeadlockTest` accepts "committed inline if the dwell had already ended" as a resolved
  outcome. The uncontended inline commit is exactly this defect's path, and astubbs#408 leaves it untouched.
  What astubbs#408 does establish is the machinery the fix below reuses: a decline branch that is
  documented safe, and a test contract that already counts "declined" as success.
- **Whether `onPartitionsLost` has the same gap.** It does not commit at all, so it looks unaffected,
  but it was not the subject of this work and was not tested.
- **Relationship to astubbs/parallel-consumer#173 (confluentinc#777),** *"Handling Partition
  Revocation in Parallel-Consumer Leading to Duplicate Event Processing"*, which is open and reports
  this symptom from the field. This mechanism is a candidate cause; attribution needs its own
  experiment, and the two must not be conflated on the strength of matching symptoms.

## What the fix should be, and the one question that decides whether it is correct

Recorded at merge prep so the next piece of work starts from a position rather than from the three
candidates above. **Take the first candidate, unconditionally, in transactional mode only.**

**Why unconditional.** The candidate list above already establishes that a mailbox-emptiness test
does not close the window wherever it is placed - `processWorkCompleteMailBox` drains into a local
deque and only then loops calling `handleFutureResult`, so a poll-thread check can land in the gap.
Declining unconditionally has no gap to land in. It also adds no cross-thread mutation at all, which is
the property the other two candidates both still have to work around: declining performs no
transactional action, so nothing new runs on the poll thread and the astubbs#29 hazard is sidestepped
rather than handled carefully.

**Why not anything that makes the poll thread wait on the control thread.**
[`docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md`](../solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md)
owns the AB-BA deadlock behind confluentinc#857 at this exact seam - the poll thread parked in
`onPartitionsRevoked`, the control thread holding the same monitor inside a blocking commit. It is
reachable only in `PERIODIC_CONSUMER_SYNC`, so it is not this defect, but it disqualifies a fix shape:
the second candidate above in its naive form, and the "coordinate with the control-thread drain"
variant of the first, both put the poll thread in exactly that position. That is a second, independent
argument for the unconditional decline, which waits on nothing.

**Why transactional mode only.** In consumer-commit mode the revoke commits offsets and nothing
else; an undrained success is simply not included and its record is redelivered, which is the
consumer lane's published at-least-once contract doing its job. Nothing was produced inside a
transaction, so there is no output to duplicate. The defect is specific to EoS.

**Where.** Its own pull request, cut from master - not on astubbs/parallel-consumer#408. That PR's
subject is the unbounded *wait* (confluentinc#803) and it is stacked two deep on astubbs#410 and astubbs#262;
changing its subject would delay it and blur its record. The two will collide on
`tryCommitOffsetsOnRevoke`, and that is resolved at merge rather than dodged by relocating either
change. This note and astubbs#408's note should each name the other. The precedent for the split is the one
this note already cites: a main-code correctness fix deserves its own change and its own reviewer.

**THE QUESTION THAT DECIDES CORRECTNESS, AND IT IS NOT YET ESTABLISHED.** When the revoke declines,
what becomes of the open transaction's *already-produced* output? Two outcomes, opposite in effect:

- **The transaction is aborted.** The output is never visible to a `read_committed` consumer; the
  next owner reprocesses from the last committed offset, produces again, and commits atomically.
  Exactly-once is *preserved*, not degraded, and the decline is the correct behaviour rather than a
  compromise.
- **The control thread commits it later, without the revoked partition's offset.** Output committed,
  input offset not - the same defect through a different door.

astubbs#408's body says declining "costs a replay: offsets stay dirty and travel to the new assignee" and is
silent on the produced output, so the design's own record does not settle it. **It must be settled by
running it, not by arguing it** - the diagnosis this note records was needed because a plausible
argument about reachability was backwards until someone measured. The instrument already exists:
`aRevokeTimeCommitIncludesTheOffsetOfEveryRecordItAlreadyProduced`, un-quarantined, goes green exactly
when the defect is gone, and a broker-level check that no duplicate reaches the output topic settles
which of the two outcomes the decline actually produces.

**The cost to measure before shipping.** How often a revoke lands with undrained produced work. That
is the entire price of going unconditional - each such revoke discards completed work for redelivery.
If it is rare the fix is free; if it is common the fix trades a correctness defect for a throughput
regression at every rebalance, and that should be known before the change lands rather than after.
State the prediction first.

**What the fix makes possible, and owes.** Under an unconditional decline the revoke path stops
touching control-thread state - so the confinement declaration the next section says cannot yet be
written truthfully *becomes* truthful, and the fix owes it in the same change, with its assertion.

**Acceptance.** The quarantined proof leaves the registry and gates; the confinement declaration and
its runtime assertion land with it; C9 and C4 return to `PROVED` on the strength of the observed
control, and the README caution comes out.

## The annotation this seam cannot truthfully carry yet - and what the fix owes

Asked at merge prep: can the concurrency annotations expose this better? **No, and declaring one
would make it worse.** The rule and its reasoning are not restated here -
[`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md`](../../parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md)
**owns them**, under "Declare thread confinement with `@ThreadConfined`, and assert it at the entry
point": the analyser consumes the declaration and never checks it, so an unenforced one silences
RacerD rather than informing it, and `RetryQueue.RetryQueueIterator` is the worked pattern.

**What is specific to this defect is that there is nothing truthful to declare, and why.** A
confinement claim over the state the revoke-path drain would mutate is exactly the claim this note
says is violated. That owner's own "check the premise before you write it" paragraph reaches the
same seam from the other side, citing
`AbstractParallelEoSStreamProcessor.lastCommitTime` - described as control-thread-confined by every
record that mentioned it, and written by `tryCommitOffsetsOnRevoke()` on the poll thread. Two
investigations, one starting from the mailbox drain and one from a field's writers, landed on the
same place without knowing about each other. That is corroboration, not coincidence.

**THE OBLIGATION THIS NOTE CARRIES, for whoever closes it.** The thread-ownership decision at this
seam is the open item. When it is taken, the declaration and its runtime assertion go in **the same
change as the fix** - the reason is the one the owner gives for `@GuardedBy`: the decision is obvious
while you are making it and archaeology a month later. Closing this note without them leaves the seam
free to rot back to exactly the state it is in now, and nothing would fail when it did.
