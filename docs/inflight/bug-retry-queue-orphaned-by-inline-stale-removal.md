# `getWorkIfAvailable`'s inline stale removal orphans the retry-queue entry

<!-- inflight-type: bug -->
<!-- inflight-impact: throughput -->


**Known defect in product code. Not data loss - it inflates the counter that gates the poller
throttle.** Surfaced while reviewing astubbs/parallel-consumer#31; pre-existing and independent of it.

`ShardManager.removeStaleContainers` cleans **both** structures and says so in its own comment
(`// remove stale containers from both processingShards and retryQueue`), mapping `retryQueue::remove`
over what the shard returned. But `ProcessingShard.getWorkIfAvailable`'s inline stale eviction -
anchor `there are still stale work container` - calls `iterator.remove()` **alone**.

So if the control thread's inline removal reaches a *failed* (retry-queue-resident) container that
has just gone stale before the poll thread's sweep does, that queue entry is orphaned permanently.
It inflates `getQueueSizeAndNumberReadyToBeRetried` and therefore
`getNumberOfWorkQueuedInShardsAwaitingSelection` - throttle-gate noise and a false "ready to retry"
signal, forever, for the life of the instance.

## Why it is not a one-line fix

`retryQueue` is already a parameter of `getWorkIfAvailable`, so the call itself is trivial. The
problem is what would be landing it into: **`RetryQueue` has three tests and all three cover
ordering only** ([[test-retry-queue-behaviour-untested]] has the full gap list - the two-map
`unique`/`sorted` invariant, last-write-wins re-add with a changed retry-due time, revoke behaviour,
and shard/queue consistency after a stale removal by either path). A one-line fix into an untested
class is how the next defect hides.

**Do the coverage first, then the fix, in one PR.** The test that would have caught this is exactly
the shard/queue-consistency case that does not exist.

## Traced, not reproduced

Confirmed by reading the two removal paths; no failing test or production sighting. The class of
consequence (a monotonically inflating gauge that never self-corrects) is what makes it worth fixing
rather than watching.
