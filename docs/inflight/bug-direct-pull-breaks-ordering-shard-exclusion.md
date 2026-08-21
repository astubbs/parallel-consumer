# Bug: direct pull enforces ordering per-record, but the guarantee is per-shard

<!-- inflight-type: bug -->
<!-- inflight-impact: correctness -->
<!-- inflight-labels: needs-measurement -->

Found 2026-08-22 by the owner, reading the merged `DirectPullWorkerPool` for correctness rather than
speed. **Not yet reproduced by a test** - `test/direct-pull-coverage` was dispatched to attack exactly
this invariant. Filed now because the reasoning is sound on inspection and the code is on
`research/market-analysis-recut`.

## The claim

**Under `KEY` and `PARTITION`, a shard must hand out at most one record at a time. Direct pull has no
mechanism that guarantees that.**

## The evidence

`DirectPullWorkerPool` holds exactly one `ReentrantLock`, and its own javadoc says what it is for:

> *Bumped every time work may have become selectable. Read before a scan and re-read under the lock
> before parking, **which is what closes the lost-wakeup window without holding the lock across a shard
> scan.***

**The lock guards the parking protocol. The scan is deliberately outside it** - which is the right
call for throughput and fatal for the ordered invariant.

Meanwhile the ordered guarantee is still enforced the way it always was, in `ProcessingShard`:

```java
if (isOrderRestricted()) {
    // can't take any more work from this shard, due to ordering restrictions
    break;
}
```

**That `break` means "I have taken one, stop" - and it was sufficient only because a single-threaded
control loop was the only thing scanning.**

## Why the per-record CAS does not save it

`WorkContainer.onQueueingForExecution()` now does `inFlight.compareAndSet(false, true)`, so two workers
cannot claim **the same** record. **That is not the invariant.** Two workers scanning the same `KEY`
shard concurrently claim **different** records - A takes offset 5, B takes offset 6, both CAS succeed,
both run. Same key, two records in flight, **ordering silently gone.**

**The atomicity is per-record. The guarantee is per-shard.** Nothing bridges them.

## This is the second time today the same mistake surfaced

The split-shard experiment removed in-flight records from the map being scanned, and ten tests failed
because **in-flight records staying in the shard is how ordering is enforced** - see
[`parked-resume-shard-dispatch-scan.md`](parked-resume-shard-dispatch-scan.md). Direct pull keeps them
in the map but removes the *other* half of the same mechanism: the single scanner.

**The pattern worth naming: PC's ordering guarantee is enforced by a structural accident rather than by
an explicit exclusion**, and two independent refactors have now removed the accident without noticing.
Any future engine change must be checked against it deliberately, because nothing in the code says
"this is the ordering lock" - there isn't one.

## The proposed fix, which may also be the performance fix

**Owner's proposal, 2026-08-22: lock the shard for selection - and possibly for `UNORDERED` too, on the
grounds that it may be just as fast and much simpler.**

**The correctness argument is straightforward:** a per-shard lock (or a shard-level claimed flag) makes
selection exclusive, which is what the guarantee requires. One mechanism for every ordering mode.

**The performance argument is the interesting one, and it is not obvious.** Direct pull collapsed at
high concurrency - **-87% at 1,000 workers, -95% at 5,000**
([`perf-direct-pull-measured.md`](perf-direct-pull-measured.md)) - because shard selection walks the
in-flight prefix from the head, and **N workers each pay that walk per record** where the shipped engine
pays it once per batch.

**A shard lock turns N wasteful walks into one walk plus N-1 short waits.** It serialises *selection*,
which is microseconds, while the *work* stays fully parallel - and the walks it eliminates are mostly
workers rediscovering records another worker already claimed. **So it may be the fix for the scaling
collapse, arriving from a different direction than the O(1)-selection idea the measurement note
proposes.**

**And it would make three of direct pull's forced changes unnecessary:** `WorkContainer.inFlight` need
not be an `AtomicBoolean`, `iterationResumePoint` need not be `volatile`, and the per-record CAS goes
away. **A change that removes concurrent-access machinery rather than adding it is the opposite shape
from everything else tried today**, which is a point in its favour.

## What to do

1. **Reproduce it.** Two workers, one `KEY` shard, assert at most one record in flight. `test/direct-pull-coverage` has this as its named highest-risk target.
2. **Then implement the shard lock**, and measure at the points where direct pull currently collapses -
   1,000 and 5,000 concurrent - **before** assuming it costs throughput. The prediction is that it
   *gains*.
3. **Do not ship direct pull in any form until 1 is settled.** It is behind `-Dpc.directPull=true` and
   nothing in CI exercises it
   ([`test-opt-in-engine-paths-are-unexercised.md`](test-opt-in-engine-paths-are-unexercised.md)), so
   the exposure today is zero - but that is an accident of it being unfinished, not a safeguard.
