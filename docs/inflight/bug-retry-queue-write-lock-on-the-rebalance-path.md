# `RetryQueue`'s write lock is taken on the poll thread, inside every rebalance callback

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->

<!-- post-merge: checked - the sentence below dates the find rather than describing an open PR, so it
     reads the same once that work has landed -->
**Live on master, and not introduced by any open PR.** Found on 2026-08-31 by the defect-class sweep
run at the merge prep for the confluentinc#857 deadlock fix, once
`ArchitectureTest.rebalanceCallbacksMustNotBlock` learned to recognise `ReentrantReadWriteLock`. It is the second member of the class that rule exists
for: a blocking acquire on the poll thread inside a rebalance callback.

## The shape

`RetryQueue.remove(WorkContainer)` takes `lock.writeLock().lock()` - an unbounded, blocking acquire.
It is reachable from **every** rebalance callback, through `WorkManager` and `PartitionStateManager`
as well as `AbstractParallelEoSStreamProcessor` directly. The exemption entries in
`ArchitectureTest`'s `KNOWN_BLOCKING_VIOLATIONS` name each root path, so `grep` them for the current
list rather than copying it here.

A rebalance callback runs on the poll thread inside `consumer.poll()`, and the whole group waits
while it runs. Anything it cannot get immediately, it must decline rather than wait for. This waits.

## Why it is not obviously benign, and what has NOT been established

Most `RetryQueue` operations hold the lock for a short map manipulation, which would make this
contention rather than a stall. **One does not.** `RetryQueue.iterator()` acquires the READ lock and
hands it to the caller, released only when the iterator is closed - its own javadoc says it is
"really important for it to be closed in timely fashion to release the lock". `ShardManager` is the
caller, in a try-with-resources, on the control thread.

So the poll thread's write-lock acquire waits for however long the control thread's scan takes. Two
things sharpen that:

- the lock is constructed **fair** (`new ReentrantReadWriteLock(true)`), so a waiting writer also
  blocks readers that arrive behind it - contention queues rather than interleaving
- the wait sits inside the `max.poll.interval.ms` budget, which is the same budget the
  astubbs/parallel-consumer#44 revoke wait overruns

**What has not been established, and must be before anyone calls this benign or serious:** how long
the control thread can hold that read lock in `ShardManager`, and whether any path holds it across
something slower than an in-memory scan. No cycle has been found - this is not a second AB-BA
deadlock as far as the sweep went - so the current claim is "an unbounded wait on the poll thread
whose worst case is unmeasured", not "a deadlock".

## Why it went unseen

Two independent reasons, both worth keeping because they are properties of the instrument rather
than of this defect:

1. **The rule's deny list did not name `ReentrantReadWriteLock`.** It listed only the primitives the
   two known defects happened to use, so it answered a narrower question than its description
   claimed.
2. **The exemption was keyed on the root method.** `AbstractParallelEoSStreamProcessor.onPartitionsRevoked`
   was exempted wholesale for its `Thread.sleep`, which silenced that callback for *every* blocking
   call - including this one, in the same method. Exemptions are now keyed on the
   `root => target` pair, and re-keying them is what surfaced the sixth path.

## Fixing it

Not started, and no design agreed. The rule's own advice is the starting point - decline rather than
wait (`tryLock`), or move the work off the poll thread - but `remove()` returning "I could not do
it" changes a caller contract, so it is a decision rather than a patch. The revoke path's own
requirements are the constraint: whatever it does must be correct when the removal does not happen.

Related, and worth reading first because it is the same class with an agreed-hard design problem:
[`bug-857-transactional-revoke-wait.md`](bug-857-transactional-revoke-wait.md).
