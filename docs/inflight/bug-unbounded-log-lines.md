# Log lines that interpolate a whole collection, so the diagnostic is truncated away

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

A log line that interpolates a record batch, a partition map or a state object grows with
`max.poll.records` and with the assignment, so log tooling truncates the line and takes with it the
part that identified the event. The line is still emitted, and still reads as if it reported
something - which is why this is filed as misdirection rather than as noise.

**The lines behind astubbs#169, astubbs#170 and astubbs#168 are fixed; the rest of the class is live
in `parallel-consumer-core`.** The one worth doing next is named below, and the **dismissals matter
as much as the hits** - several lines have this shape and are correct as they are. The search is
written down so it is not repeated; re-run it with
`grep -rnE 'log\.(warn|error)' --include=*.java parallel-consumer-core/src/main` and read what each
line interpolates.

<!-- post-merge: checked-begin -->
The fixed lines, and the shape a fix takes, are astubbs#203 (astubbs#169 / confluentinc#631 in
`RemovedPartitionState`, astubbs#170 / confluentinc#640 in `AbstractParallelEoSStreamProcessor`):
a bounded summary on the operator-facing line, the unabridged object one level down at `DEBUG`.

astubbs#168 / confluentinc#629 in `ConsumerOffsetCommitter` followed, on branch
`fix/168-commit-error-line-keeps-identifiers`, with the one variation the fix shape allows: **no
partition cap**.
A commit map holds one entry per partition and the partitions are exactly what astubbs#168 asked
for, so every one stays named and only the per-entry `metadata` is reduced - to its length, which is
itself the diagnostic when a commit is rejected for its size.
<!-- post-merge: checked-end -->

**`bz.stub.parallelconsumer.internal.utils.RecordBatchSummary` is the shared renderer - reuse it
rather than writing another format string.** Its own class javadoc **owns** what it keeps, what it
caps and why; what is here is only which lines still need it.

This is the concrete half of astubbs#238 (confluentinc#57, "Reduce debug log output"), which is the
umbrella and stays open. It is a different axis from
[`core-product-log-levels-at-info.md`](core-product-log-levels-at-info.md), which owns whether a
statement is at the right *level*; this note is about how much a statement renders once it fires.
Whether a hostile `toString()` can escape a log call is a third axis again, owned by
[`core-blanket-safe-logging.md`](core-blanket-safe-logging.md).

## The one worth doing next

It is strictly worse than the lines already fixed: it renders at `WARN`, where their replacement
detail sits at `DEBUG`, and what it renders is not only long but a disclosure.

- **`state/PartitionStateManager.java`**, anchor `or is this a race? Please file a GH issue` -
  renders a whole `PartitionState` at **WARN**. `PartitionState` is Lombok `@ToString` with no
  `@ToString.Exclude` (grep `@ToString` in `state/PartitionState.java`), so it prints
  `incompleteOffsets`: every tracked incomplete record, **keys and values included**, on a line
  that asks the operator to paste it into a public issue. That exposure, not the length, is the
  reason this one is first.

  **The fix belongs on the type, not on the call site** - `@ToString.Exclude` plus a size accessor -
  which also fixes the two other sites that render a `PartitionState`:
  `internal/AbstractParallelEoSStreamProcessor.java`, anchor `Partitions revoked {}, state: {}`, and
  `state/PartitionStateManager.java`, anchor `Reassignment of previously revoked partition`.

## Checked and dismissed - reasons, so the search is not repeated

- `state/PartitionState.java`, anchor `Offsets {} have been removed from partition {}` - the offset
  list **is** the diagnostic payload. Those offsets are non-contiguous by construction (they are the
  ones missing from the polled batch), so a range summary would destroy what the line exists to give.
- `state/PartitionState.java`, anchor `Polled an empty batch of records? {}` - guarded by
  `records.isEmpty()`, so bounded by construction.
- `internal/AbstractParallelEoSStreamProcessor.java`, anchor
  `Worker pool is shut down, not submitting work` - already bounded by hand, and converting it to
  `RecordBatchSummary` would change the message that
  `SubmitWorkToPoolShutdownRaceTest.REJECTED_SUBMISSION_MESSAGE` asserts on.
- **vertx, reactor and mutiny** render a single `WorkContainer` on their failure paths
  (`ExternalEngine`, anchor `Failed to record the user function failure against {}`, and
  `VertxParallelEoSStreamProcessor`, anchor `Failed to record the send failure against {}`).
  `WorkContainer.toString()` is hand-written and prints topic-partition, offset and key only - no
  value, and one record rather than a batch. Not this class.
- **`parallel-consumer-examples`** is the one qualification to that: `CoreApp`, anchors
  `Retry count {} exceeded max of {}` and `is circuitBroken, will retry message when server is up`,
  interpolates a raw `ConsumerRecord`, whose Kafka-authored `toString()` does print the value. It is
  one record, so it is bounded, and it is example code an operator can edit - which is why it is
  dismissed rather than fixed, but it is not the "no value" case the three library modules are.
