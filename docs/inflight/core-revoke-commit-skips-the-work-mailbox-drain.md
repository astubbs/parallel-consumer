# The revoke-path commit does not drain the work mailbox first

<!-- inflight-type: bug -->
<!-- inflight-impact: data-loss -->
<!-- inflight-labels: concurrency -->

<!-- post-merge: checked - names the PR that made the change, which reads the same once merged -->
**Pre-existing on master, not introduced by astubbs/parallel-consumer#408** - found while
disproving a P0 raised against that PR, and recorded because the disproof turned up a real thing one
step over.

The control loop commits like this: take the producer write lock, **drain the work mailbox**, then
commit. Draining first is what makes the transactional guarantee hold - a record already produced
into the open transaction has its success sitting in the mailbox, and only the drain marks its
partition dirty so `collectCommitDataForDirtyPartitions` includes its offset. Produce and offset then
commit atomically.

The revoke path takes the same lock and commits, but **never drains**. So a revoke-time commit can
publish a transaction whose offsets omit work whose success is still queued: the output is committed,
the input offset is not, and the next owner reprocesses that input and produces the output again.
Exactly-once degrades to at-least-once, silently.

## What is and is not established

- **Established:** the control loop drains (`processWorkCompleteMailBox` between
  `maybeAcquireCommitLock` and `commitOffsetsThatAreReady`); the revoke path has no equivalent call.
  Grep `processWorkCompleteMailBox` in `AbstractParallelEoSStreamProcessor` - it appears on the
  control and close paths, never the revoke one.
<!-- post-merge: checked - both are historical statements about when the gap existed -->
- **Established:** this predates astubbs/parallel-consumer#408 and astubbs/parallel-consumer#29 -
  master's `onPartitionsRevoked` called `commitOffsetsThatAreReady()` directly, with the same gap.
- **NOT established:** whether the window is reachable in practice. The produce read lock is held
  across the send and its acks, and a commit cannot start while any read lock is held, so the
  mailbox may in fact be empty of *produced* work whenever a commit can begin. That argument was not
  finished, and it is the whole question.
<!-- post-merge: checked -->
- **NOT established:** whether astubbs/parallel-consumer#408 narrows or widens it. Declining more
  often means committing on revoke less often, which would make it rarer - but nobody measured.

## Why it was easy to miss

The P0 that led here claimed the opposite thing - that *declining* orphans records. That does not
hold: declining performs no transactional action at all, and the control thread's in-flight
transaction commits its own collected offsets atomically. The hazard is on the **acquire** path, not
the decline path, and it is older than the change being reviewed.

Settling it wants the astubbs/parallel-consumer#262 claim harness rather than a fresh instrument:
that suite exists to prove or falsify each documented transactional guarantee, and this is one of
them.
