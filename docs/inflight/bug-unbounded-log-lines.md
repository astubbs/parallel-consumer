# Log lines that interpolate a whole collection, so the diagnostic is truncated away

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

A log line that interpolates a record batch, a partition map or a state object grows with
`max.poll.records` and with the assignment, so log tooling truncates the line and takes with it the
part that identified the event. The line is still emitted, and still reads as if it reported
something - which is why this is filed as misdirection rather than as noise.

**Two instances were fixed**; at least eight more are live in `parallel-consumer-core`. The search
that found them is written down here so it is not repeated, and the **dismissals matter as much as
the hits** - three lines look like instances and are not.

<!-- post-merge: checked-begin -->
The fixed pair, and the shape a fix takes, are astubbs#203 (astubbs#169 / confluentinc#631 in
`RemovedPartitionState`, astubbs#170 / confluentinc#640 in `AbstractParallelEoSStreamProcessor`):
a bounded summary on the operator-facing line, the unabridged object one level down at `DEBUG`.
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

## The two worth doing next, worst first

Both are strictly worse than the pair already fixed - one is at `WARN`, the other at `ERROR`,
where the fixed pair's replacement detail sits at `DEBUG`.

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

- **`internal/ConsumerOffsetCommitter.java`**, anchor `Error committing offsets: {}, exception: ` -
  renders `Map<TopicPartition, OffsetAndMetadata>` at **ERROR**. PC writes its encoded offset map
  into that metadata, capped per partition at `OffsetMapCodecManager.DefaultMaxMetadataSize`, so the
  line is partitions x up to 4KB of base64 on the occasion you most need it intact.

  **Do not summarise the map away.** astubbs#168 (confluentinc#629) asked for exactly the topic,
  partition and offset this line carries, and its fork-status note records the line as the
  implementation of that request - so a fix keeps every identifier and drops or caps only the
  `metadata` string that `OffsetAndMetadata.toString()` drags along. Re-read astubbs#168 before
  touching it.

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
