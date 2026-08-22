# Select retries from the RetryQueue, which already exists and is already the right shape

<!-- inflight-type: feature -->
<!-- inflight-impact: performance -->

**Antony's proposal, 2026-08-22.** Keep records awaiting retry in a separate queue ordered by the
time their retry falls due. A selector checks that queue's head first: if the soonest retry is due,
take from there; otherwise take from the main available work. Both O(1)-ish, and retries never
appear in the main scan.

## Most of it is already built

`RetryQueue` is exactly that structure:

```java
private final Map<WorkContainerKey, WorkContainerSortKey> unique = new HashMap<>();
private final NavigableMap<WorkContainerSortKey, WorkContainer<?, ?>> sorted;   // a TreeMap
private final Comparator<WorkContainerSortKey> comparator = Comparator
        .comparing(WorkContainerSortKey::getRetryDueAt)
        .thenComparing(WorkContainerKey::getTopic)
        .thenComparing(WorkContainerKey::getPartition)
        .thenComparing(WorkContainerSortKey::getOffset);
```

Time-ordered, with an **offset tiebreak** - which is the right secondary key, for the frontier reason
below - and a hash map alongside it giving O(1) removal by record. The proposal's data structure
needs no design work at all.

## What is missing: nothing selects from it

Its only consumers today are counting (`getQueueSizeAndNumberReadyToBeRetried`),
`getLowestRetryTime()` - which decides how long the control loop may sleep - and removal bookkeeping.
`ProcessingShard#getWorkIfAvailable(int, RetryQueue)` takes it as a parameter and uses it at exactly
one line, to remove what it has already taken by other means.

**So a retried record lives in two places**: the retry queue, and still in its shard's entry map,
where the scan finds it by testing `isDelayPassed()` on every candidate. The queue is a secondary
index that nothing reads for selection.

## Why wiring it in is worth more than the scan saving

**It advances the commit frontier directly.** A retried record is usually the *lowest incomplete
offset* - it is the thing pinning the frontier, and everything completing above it widens the encoded
incomplete set. Taking retries first is therefore not merely fair, it is the single most effective
thing a selector can do to shrink the commit payload and reduce replay on crash. Today the scan gets
this right only incidentally, by walking in offset order.

**And it removes retries from the scan path** - which matters more once in-flight records are no
longer walked, because the remaining scan cost is then dominated by whatever *is* left in the way.

## The two risks

**1. The lock is FAIR, and that is now on the hot path.**

```java
private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
```

A fair lock hands off in strict FIFO with no barging, which is materially slower under contention
than the default. Today that is fine because nothing hot touches it. Put N selectors on the head of
this queue for **every take** and the fair lock becomes the new bottleneck - the same shape of
mistake as the walk it replaces, arrived at from the other direction. **Measure it before wiring it
in**, and expect to need either a non-fair lock or a cheaper head read (a cached
`firstEntry` is O(log n) on a `TreeMap`, not O(1), which is small but not nothing at this call rate).

**2. Two homes for one record.** A retried record in both the queue and the shard can be found twice.
The atomic claim transition makes that *safe* - the loser simply fails the transition - but wasted
finds are exactly the cost being removed, and a record with one home is easier to reason about.
Removing it from the shard's selectable view while it waits is the cleaner end state, and it composes
with the `UNORDERED`-queue direction: the shard holds ordered work, the retry queue holds waiting
work, and neither contains the other's.

## Sequencing

This is cheap and independent - the structure exists, the comparator is right, and the change is at
the selection call site. **It does not depend on the `UNORDERED` queue, the shard queue, or the
manager thread**, and it makes each of them a little simpler if they land later.

See also: [`next-is-the-shard-required-under-unordered.md`](next-is-the-shard-required-under-unordered.md),
[`next-selectable-shard-queue.md`](next-selectable-shard-queue.md).
