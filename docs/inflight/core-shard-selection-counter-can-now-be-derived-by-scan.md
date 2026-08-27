# Core: the shard's selection counter no longer has a hot-path reader, so it can be derived

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->
<!-- inflight-labels: concurrency -->
<!-- inflight-state: deferred - wants a measurement of the scan at real buffer sizes before the counter goes -->

`ProcessingShard.workAwaitingSelectionCount` is maintained state that has to be adjusted correctly at every
site that changes whether a container is selectable. astubbs#373 made those adjustments *owned* - only the
winner of a compare-and-set on the container's claim moves the counter - rather than inferred, which is what
made the four drift defects it fixed possible. That is the right shape for a counter that has to exist.

<!-- post-merge: checked-begin -->
**It no longer has to exist**, and astubbs#373 said so, naming the load gate as the only reader that needed
an O(1) answer and astubbs#336's conservation-derived figure as what would remove it. That reader is gone:
`WorkManager.isSufficientlyLoaded()` reads `ShardManager.getWorkableRecords()`, which is derived from
`RecordPopulation` and never touches this counter.
<!-- post-merge: checked-end -->

Every reader left is off the steady-state path - verified by grepping
`getNumberOfWorkQueuedInShardsAwaitingSelection` and `getCountOfWorkAwaitingSelection` across
`parallel-consumer-core/src/main`:

- `AbstractParallelEoSStreamProcessor.drain()`, via `isRecordsAwaitingProcessing()` - shutdown cadence, and
  the one reader that is not a diagnostic. Reading low here closes the consumer early, so it is the reader
  any replacement has to satisfy.
- the `WAITING_RECORDS` gauge - once per metrics scrape.
- the under-served-retrieval diagnostic in `ShardManager.getWorkIfAvailable`, and the control loop's
  end-of-loop line - both behind `log.isDebugEnabled()` / `log.isTraceEnabled()`.

`ProcessingShard.countSelectionClaimedByScan()` already computes the same figure by scanning, and
`ShardAvailableCountOwnershipTest` holds the two against each other, so the replacement exists and is
already asserted correct.

## What is left to decide

astubbs#373 measured the scan and recorded the cost per entry and at two buffer sizes, and rejected it *on
the strength of the load gate calling it every control loop*. That premise is what changed, so the number to
re-derive is the cost at scrape and shutdown cadence rather than at control-loop cadence - re-take it rather
than reusing the old verdict, which was answering a different question.

Deleting the counter also deletes the claim protocol's reason to exist, including the two-atomics transient
its javadoc documents and the Lincheck candidate recorded against it in
`test-lincheck-lane-open-items.md` - so this is worth settling before anything is built on that transient.
