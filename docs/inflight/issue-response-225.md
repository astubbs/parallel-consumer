# Draft response to astubbs#225 - posted by the pre-release sweep, not by this PR

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-state: deferred - until the pre-release sweep, or an explicit instruction to post -->
<!-- post-merge: exempt-file - a drafted issue reply, held until the sweep posts it. It deliberately
     outlives the PR that wrote it, so it cannot be written in post-merge terms. -->

Not posted. Post only on explicit instruction; delete this file when it is posted, not when its PR
merges.

---

Requirements are drafted:
`docs/plans/2026-09-02-001-feat-recoverable-producer-fencing-plan.md`. Scoping it overturned three
things this issue assumed, so they are worth stating here rather than leaving in the plan.

**Aborting the transaction is not available.** `KafkaProducer#abortTransaction` lists
`ProducerFencedException` as a fatal `@throws`, exactly as `commitTransaction` does. Kafka Streams
calls it anyway and swallows the throw, on the grounds that the broker has already aborted — which is
the shape to copy, but it is not the "abort, then rejoin" this issue described.

**"Rejoin" was not the hard part; the producer was.** Recovery needs a *new* producer, and PC cannot
build one: `ParallelConsumerOptions` holds a finished `Producer` instance, and a `KafkaProducer`'s
configuration cannot be read back out of it. So the change is an ownership change — PC takes producer
configuration and builds the producer itself, the shape of Kafka Streams' `KafkaClientSupplier` — not an
exception swap. The producer-instance option stays, without recovery.

**"Whether rejoin is expressible in PC's lifecycle" — the part this issue said to investigate first —
is answered, and no state-machine addition is needed.** The produce/commit lock pair already gives the
control thread exclusive access at the moment a condition is detected. Two review rounds then showed
that an explicit rejoin issued from there *deadlocks*: `onPartitionsRevoked` spins on
`isTransactionCommittingInProgress()`, which is that same write lock, and the consumer is confined to
the broker-poll thread anyway. The requirement now states the outcome — recovery ends with PC a member
on a live generation — and leaves the mechanism to planning, which may find no explicit rejoin is
needed at all, since the classic protocol rejoins on the next poll after a generation loss.

**There is field evidence, which this issue did not have.** astubbs#411 (`confluentinc#830`) reported
the produce-path version in production after a broker expired an idle producer id, and asked for
precisely this feature. That report also revealed a live defect of its own — the shutdown that
answered it upstream cannot fire for the case it was written for. Recorded in
`docs/inflight/bug-411-wrapped-send-failure-spins-forever.md`; not fixed by this work, and deliberately
excluded from its scope rather than silently absorbed.

**Scope note.** The issue asked not to widen beyond fencing, and the plan does not: the wider
transaction-failure taxonomy stays with astubbs#241. But the condition set is six exceptions rather
than one, collapsed into a single response the way Kafka Streams does, and the produce path is in
scope because that is where the only real report landed.
