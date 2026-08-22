# Select retries from the RetryQueue, which already exists and is already the right shape

<!-- inflight-type: feature -->
<!-- inflight-impact: performance -->

> **SCOPE: `UNORDERED` ONLY. Antony's ruling, 2026-08-22.** Do not extend any of this to `KEY` or
> `PARTITION`, and do not treat that as an unfinished half.
>
> **There is nothing to win there.** `OrderingModeDispatchParityTest` measures `KEY` at **exactly one
> entry examined per record dispatched** - the ordered break fires after the head, so the scan already
> costs the minimum a scan can cost. Every design in this family exists to remove a walk that
> `KEY` does not perform.
>
> **And there is real correctness risk.** Under the ordered modes a waiting record's *presence in the
> shard* is what blocks a later record for the same key. Moving it elsewhere means replacing an
> implicit guarantee with an explicit one, for a measured benefit of zero.
>
> So the ordered path stays exactly as it is. Everything below is the `UNORDERED` selection path.

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

## Why a retried record goes back at all - and where it does not have to

**Antony: "why do the retry entries need to go back into the original queue?"**

**Ordering, and only ordering.** Under `KEY`, if offset 5 for key K fails and is waiting, offset 9 for
K must not run. Today **the presence of record 5 in the shard is the blocking mechanism**: the scan
reaches it, `isAvailableToTakeAsWork()` is false because `isDelayPassed()` is false, and then
`if (isOrderRestricted()) break;` fires. Remove it from the shard and offset 9 becomes selectable -
a correctness break, not a slowdown.

So the record's return is not bookkeeping, it is the ordering guarantee, expressed as a side effect
of where the object lives. **That is worth naming**, because it is the same implicit-mechanism shape
that made the in-flight prefix walk necessary: a record stays put so that its presence means
something.

| Mode | Does it need to go back? |
|---|---|
| `UNORDERED` | **No.** Nothing is ordering it. One home - the retry queue - until it is taken |
| `KEY`, `PARTITION` | **Yes today**, because presence is what blocks the shard. **Not necessarily tomorrow**: if the shard's blocked state were explicit - the in-flight count extended to mean "has a record out OR waiting to retry" - the record could live only in the retry queue and the shard would still refuse a second taker |

That second row is the same trade as everywhere else in this family: make an implicit property
explicit, and the structure stops having to hold something purely so that its presence can be
observed.

## The degenerate case is NOT the same problem

**Antony's own objection: "in the worst case all records need retries, they all get moved to the
retry queue, and we have the same problem again."**

**It does not**, and the reason is the ordering key. The current problem is a **linear walk past
ineligible entries** - the scan steps over every in-flight record to reach a claimable one.
`RetryQueue` is sorted by **retry-due time**, so:

- If the head is not due, **nothing is due**. That is one comparison, whatever the size.
- If the head is due, take it - `firstEntry` on a `TreeMap` is O(log n).
- There are no ineligible entries *before* the head to step over, because "eligible" is exactly the
  sort key.

So a retry queue holding every record still answers "is there work" in one comparison and "give me
work" in O(log n). **The pathological case of the current design is the ordinary case of this one.**

The one thing that would reintroduce a walk is leaving *taken* records in the queue, so that a
selector steps past records other workers already hold. Removing on take - which the design does
anyway - avoids it.

## Sequencing

This is cheap and independent - the structure exists, the comparator is right, and the change is at
the selection call site. **It does not depend on the `UNORDERED` queue, the shard queue, or the
manager thread**, and it makes each of them a little simpler if they land later.

See also: [`perf-unordered-departure-on-take-measured.md`](perf-unordered-departure-on-take-measured.md),
[`next-selectable-shard-queue.md`](next-selectable-shard-queue.md).
