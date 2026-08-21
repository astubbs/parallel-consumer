# Bug: the shard's available-work counter drifts, and is corrected by a clamp

<!-- inflight-type: bug -->
<!-- inflight-impact: correctness -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-21, found while measuring the shard dispatch scan
([`parked-resume-shard-dispatch-scan.md`](parked-resume-shard-dispatch-scan.md)). **No prior art**:
searched `docs/inflight/`, `docs/solutions/`, `docs/refactoring.md` and both trackers for
`availableWorkContainerCnt` and for work-counter drift. Nothing.

## The code

`ProcessingShard`:

```java
private void dcrAvailableWorkContainerCntByDelta(int ByNum) {
    availableWorkContainerCnt.getAndAdd(-1 * ByNum);
    // in case of possible race condition
    if (availableWorkContainerCnt.get() < 0L) {
        availableWorkContainerCnt.set(0L);
    }
}
```

**A counter that needs a clamp is a bug that has been observed and papered over rather than found.** The
comment says as much: *"in case of possible race condition"* - not a described race, a suspected one.

## Why it matters more than it looks

This is not a display counter. `getCountOfWorkAwaitingSelection()` reads it, and
`ShardManager.getNumberOfWorkQueuedInShardsAwaitingSelection()` sums it across every shard - which
feeds `WorkManager.isSufficientlyLoaded()`, **which is what pauses and resumes the broker poller**.

So the value gates record intake. Drift low and the poller resumes when it should throttle; drift high
and it throttles when it should fetch. **The clamp only catches drift in one direction** - a counter
that has drifted *high* is silently wrong and nothing corrects it.

**And there is a known defect in exactly this area.** The silent-stall investigation (`confluentinc#857`)
left a diagnostic comment in `WorkManager.isSufficientlyLoaded()` naming
*"the numberRecordsOutForProcessing counter-drift signature"* as a stall cause - a different counter, the
same class of fault, in the same calculation. **Two drifting counters feeding one throttle decision is
the parallel-state shape**, and it is worth treating as one problem rather than two.

## Why it is not just "make it volatile" or "use a lock"

It is already an `AtomicLong`; the individual operations are atomic. The drift is not a torn read, it
is that the counter and the collection it describes are updated at different moments by different
threads, so the pair is never consistent. **The fix is to stop having a second source of truth**, not
to synchronise two.

## The shape of a fix

**Derive the number instead of maintaining it.** The obvious version - `entries.size()` - is not
available: `entries` is a `ConcurrentSkipListMap` and `size()` on one is O(n), and this is read on the
broker-poll path.

The design that solves both this and the dispatch scan at once is recorded in
[`parked-resume-shard-dispatch-scan.md`](parked-resume-shard-dispatch-scan.md): **an index of
not-in-flight records over the single ordered collection.** Membership of that index cannot disagree
with itself the way a counter drifts. **That is the real prize in that note - not the microseconds.**
It needs a structure whose size is O(1), which rules out `ConcurrentSkipListSet` and points at a
`ConcurrentHashMap` plus an ordered view, or a maintained size on a purpose-built holder.

**And if a counter survives at all, keep it as a `LongAdder`, not an `AtomicLong`.** The owner's
framing was exactly right - *a counter that needs no lock on write, only a reduce on read*. That is
`LongAdder`: increments are spread across striped cells so writers rarely touch the same memory, and
`sum()` reduces them on read. It ships in the JDK and needs no dependency.

**It does not fix the drift on its own**, and the distinction matters. `LongAdder` removes *write
contention*; the drift here is that the counter and the collection it describes are updated at
different moments by different threads. **A contention-free wrong number is still wrong.** Use it if a
counter survives the redesign; do not reach for it as the fix.

**And if a counter is kept, keep it as a `LongAdder`, not an `AtomicLong`.** The owner's framing was
exactly right - *a counter that does not need a lock on write, only a reduce on read*. That is
`LongAdder`: it spreads increments across striped cells so writers rarely touch the same memory, and
`sum()` reduces them on read. It ships in the JDK, needs no dependency, and is the correct tool for a
counter written by many threads and read by one.

**It does not fix the drift on its own**, and that distinction matters: `LongAdder` removes *write
contention*, while the drift here is that the counter and the collection it describes are updated at
different moments. A contention-free wrong number is still wrong. **Use it if a counter survives the
redesign; do not reach for it as the fix.**

## What to do first

1. **Reproduce the drift.** Nobody has: the clamp implies it was seen, but no test asserts it and no
   note records the conditions. An invariant check in a stress or chaos run - counter versus actual
   selectable count - would say whether it still happens and how far it goes.
2. **Check the high-side case**, which the clamp does not cover, because that is the direction that
   throttles intake and looks like a stall rather than a wrong number.
3. **Only then design.** Replacing state that turns out never to drift is a refactor; replacing state
   that drifts is a fix, and the two deserve different amounts of risk.

**Do not simply delete the clamp.** It is load-bearing until the underlying drift is understood -
removing it converts a papered-over fault into a negative count reaching the throttle calculation.
