# Next: make producer fencing recoverable instead of fatal

<!-- inflight-type: feature -->
<!-- inflight-impact: reliability -->


> Extracted from `origin/docs/session-learnings-857-family` @94bb98a9d, `docs/inflight/core-recoverable-producer-fencing.md`.

A `ProducerFencedException` during a transactional commit currently kills the PC instance. It should
be treated as "our partitions moved, clean up and rejoin" - which is what Kafka Streams does.

## What happens today

`ProducerManager.commitOffsets()` catches `ProducerFencedException` from `sendOffsetsToTransaction`
and rethrows it wrapped in `InternalRuntimeException`. That propagates out of
`AbstractOffsetCommitter.retrieveOffsetsAndCommit()`, out of `commitOffsetsThatAreReady()`, out of
`controlLoop()`, and lands in the supervisor loop, which records it as `failureReason`, calls
`doClose()` and rethrows. The instance is gone.

Offset bookkeeping does survive this correctly - `onOffsetCommitSuccess()` is skipped, so offsets
stay dirty and the records are redelivered - and `postCommit()` runs in a `finally`, so the produce
lock is released. The data is safe. It is the liveness that is wrong.

## Why it matters

Under KIP-447 (`exactly_once_v2`), fencing by *consumer generation* is the normal mechanism by which
a rebalance takes partitions away from a producer. Being fenced is a routine consequence of a
rebalance you lost, not an error condition. Treating a routine event as fatal means any consumer
that is slow enough to lose a race during a rebalance dies rather than rejoining.

## The Kafka Streams model, which is the one to copy

Streams unwraps `ProducerFencedException` in `RecordCollectorImpl` specifically so it can be
converted into `TaskMigratedException` rather than triggering a shutdown. `TaskMigratedException` is
handled by closing out the assigned tasks and **rejoining the consumer group**. The thread survives.

Do not treat this as a solved problem upstream: `KAFKA-14567` ("Kafka Streams crashes after
ProducerFencedException") shows the same class of bug reaching Streams in EOS-v2. Copy the shape of
the design, not the assumption that it is airtight.

## Proposal

Introduce a distinct, recoverable exception - the PC equivalent of `TaskMigratedException` - raised
where fencing is detected, and handle it in the control loop by aborting the transaction, leaving
offsets dirty, and letting the consumer rejoin rather than closing.

The bounded revoke wait (see the branch that carries this note) reduces how often fencing is reached
on the revoke path, but does not remove it: fencing can still arrive from a slow commit that overran
its generation for reasons unrelated to a revoke. The two changes are complementary and independent.

## What is undecided

- Whether "rejoin" is even expressible in PC's current lifecycle. Streams owns its thread and can
  re-initialise tasks; PC's control loop has no equivalent re-entry point today, so this may need a
  state-machine addition rather than just an exception swap. **This is the part to investigate
  first** - it determines whether this is a small change or a structural one.
- Whether other fatal paths deserve the same treatment. `InternalRuntimeException` is used widely;
  this proposal deliberately does not widen beyond fencing.

Tracked as astubbs#225.
