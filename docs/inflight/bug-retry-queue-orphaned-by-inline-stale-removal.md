# `getWorkIfAvailable`'s inline stale removal orphans the retry-queue entry

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->


**Known defect in product code. Not data loss - no record is dropped and no offset is mis-committed -
but it can stop the poller permanently.** Surfaced while reviewing astubbs/parallel-consumer#31;
pre-existing and independent of it.

**Re-classified from `throughput` to `stall` on 2026-08-19**, after tracing where the inflated count
is actually consumed. It was filed as throttle-gate noise; the gate it feeds is the broker-poller
pause/resume, and the inflation does not wash out. Mechanism below - correct the impact, not the
history: the original reading was that an orphan inflates both terms of the count and therefore
cancels, which is true in only one of the two branches.

`ShardManager.removeStaleContainers` cleans **both** structures and says so in its own comment
(`// remove stale containers from both processingShards and retryQueue`), mapping `retryQueue::remove`
over what the shard returned. But `ProcessingShard.getWorkIfAvailable`'s inline stale eviction -
anchor `there are still stale work container` - calls `iterator.remove()` **alone**.

So if the control thread's inline removal reaches a *failed* (retry-queue-resident) container that
has just gone stale before the poll thread's sweep does, that queue entry is orphaned permanently.
It inflates `getQueueSizeAndNumberReadyToBeRetried` and therefore
`getNumberOfWorkQueuedInShardsAwaitingSelection` - a false "ready to retry" signal, forever, for the
life of the instance.

## Why this is `stall` and not `throughput`

The orphan is counted in **both** halves of the tuple - `sorted.size()`, and
`getNumberOfFailedWorkReadyToBeRetried()`, which counts anything whose `isDelayPassed()` is true (a
stale orphan's delay passed long ago and never un-passes). Grep
`diffBetweenShardsAndRetrySize` in `ShardManager`; the count reduces to

```
ready + max(0, -size + shards)
```

The two contributions cancel **only while `-size + shards` is positive**. When the pipeline is
drained - shards nearly empty, which is exactly the condition in which a stall matters - that term
goes negative and is **clamped to zero**, discarding the orphan's contribution to `size` while its
contribution to `ready` survives. `awaitingSelection` is then permanently inflated by the orphan
count.

That value gates `WorkManager.isSufficientlyLoaded()`, which gates the broker poller's pause/resume.
Its own comment states the consequence: *"If it stays true while no records are actually flowing, the
poller never resumes and the PC stalls"* - the confluentinc#857 silent-stall signature, reached here by a
different route. Once accumulated orphans exceed `targetAmountOfRecordsInFlight * loadingFactor`, the
condition is true regardless of real work, permanently.

`workIsWaitingToBeProcessed()` is the same count `> 0`, so an instance with a single orphan also
believes work is waiting forever - worth checking against the drain and shutdown paths when this is
fixed.

**Accumulation rate is the open question, and it decides urgency.** One orphan needs a *failed*
(retry-queue-resident) container to go stale and be reached by the control thread's inline eviction
before the poll thread's sweep. That is rare per rebalance, but it is monotonic and never cleaned up,
so the exposure grows with instance uptime and rebalance count. Nobody has measured it.

## Why it is not a one-line fix

`retryQueue` is already a parameter of `getWorkIfAvailable`, so the call itself is trivial. The
problem is what would be landing it into: **`RetryQueue` has three tests and all three cover
ordering only** ([`test-retry-queue-behaviour-untested.md`](test-retry-queue-behaviour-untested.md) has the full gap list - the two-map
`unique`/`sorted` invariant, last-write-wins re-add with a changed retry-due time, revoke behaviour,
and shard/queue consistency after a stale removal by either path). A one-line fix into an untested
class is how the next defect hides.

**Do the coverage first, then the fix, in one PR.** The test that would have caught this is exactly
the shard/queue-consistency case that does not exist.

## Traced, not reproduced

Confirmed by reading the two removal paths; no failing test or production sighting. The class of
consequence (a monotonically inflating gauge that never self-corrects) is what makes it worth fixing
rather than watching.
