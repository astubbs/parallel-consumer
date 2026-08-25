# Parked: the 2022 central-queue / direct-pull / per-thread-queue rework

<!-- inflight-type: parked -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

Recorded 2026-08-21. **This is the most valuable piece of prior art in the performance work, and it
was found last rather than first.** Everything a full day of experiments concluded, this branch family
had already established in 2022 - and it went further, because it built the alternative rather than
measuring the existing one.

Branches: `origin/refactor/gpt3-central-queue-direct-pull` and
`origin/refactor/gpt3-queue-management-with-msg-push`. Classes `PCWorker` and `QueuedShardManager`
exist **only** there - this was a rework of the engine, not a patch.

## What it did, read from its own commits

| Commit | Message |
|---|---|
| `7b0eb7c0f` | runs extremely slow |
| `4e78c4b68` | START: Central distribution via actor msg, batch 100 from central |
| `3bdcb3630` | runs correctly, fast, but not as fast |
| `58826c349` | **START: Central queue facade over Shards, filled when entries fail or succeed** |
| `5dcd39bb3` | **START: ThreadLocal attempt for not sharing a queue** |
| `2041e4e82` | works |
| `e12ba8a8b` | **START: cache counts of work awaiting selection - the slowest stage of controller loop and broker poller loop - because it's O(n) shards** |
| `0df84c9e5` | **Inefficient routines in control loop fixed, still only 1/3~ a fast as ThreadPoolExecutor version** |
| `7e775a111` | **identified possible issue with poller throttling, doesn't seem to help which is also interesting** |

**`58826c349` is the "queue that is not a queue"**: a facade over the shards, so workers pull directly
and the intermediate buffer disappears.

**`5dcd39bb3` is the per-thread queue**, and it is exactly the design proposed again today - a map of
queues keyed by thread id:

```java
Map<Long, BlockingQueue<ProcessingShard<K, V>>> shardQueueMap = new ConcurrentHashMap<>();
var shardQueue = shardQueueMap.computeIfAbsent(Thread.currentThread().getId(),
        aLong -> new LinkedBlockingQueue<>());
```

## RETRACTED: the "1/3 as fast" verdict does not measure the design. The blocking wait was never wired up.

**Read the code, 2026-08-21, after the owner objected to this note taking his own four-year-old
verdict at face value.** He was right to object. `QueuedShardManager.inner()`, with the comments
stripped, is the entire live path a worker takes to get its next record:

```java
private List<WorkContainer<K, V>> inner() throws InterruptedException {
    List<WorkContainer<K, V>> element = null;
    while (element == null) {
        element = tryOneIterationQueueRemoval();
    }
    return element;
}
```

**That is a busy-spin.** When a worker finds no work it loops immediately and tries again. The
blocking wait sits directly beneath it, commented out, with an unresolved race condition noted in a
`todo`:

```java
// monitor.wait(1000); // NOSONAR
// boolean wasSignaled = newWorkEvent.await(1, SECONDS);
// todo move out of sync, use Lock? - fix race condition between adding, and notifying
```

**So N workers with no work available burn N cores.** The sibling commit is titled `WIP!`; this is
what the WIP was.

**And this session measured that exact pathology independently, four years later.** Replacing the
worker pool's `LinkedBlockingQueue` with a lock-free `LinkedTransferQueue` - which **spins before
parking** - cost **69% of throughput** on a machine with far more threads than cores
([`parked-worker-pool-queue-lock-is-not-the-cost.md`](parked-worker-pool-queue-lock-is-not-the-cost.md)).
Same failure mode, same class of machine. A design whose idle path spins is not being measured on its
design.

**Therefore: "~1/3 as fast as the ThreadPoolExecutor version" is evidence the branch was unfinished,
not evidence the architecture is slow.** Everything below that treats it as a verdict on the design is
withdrawn. What remains true is that the branch was *not proven* - which is a different and much
weaker statement than *disproven*.

**The lesson worth keeping, because it cost real time here:** a register entry that records an outcome
without a cause invites exactly this. `docs/refactoring.md` said "mostly dead-ends", this note repeated
"1/3 as fast", and the recommendation to leave it alone was made three times before anyone opened the
file. **Read the code before repeating a verdict on it.**

## What still stands against direct pull, and is untested

One objection survives the retraction, and it is architectural rather than incidental: **direct pull
turns shard access from single-consumer into N-way concurrent.** Today the control loop is the only
thread selecting work; under direct pull every worker contends on shared shard state per record. The
branch's own commits record fighting an O(n) count in exactly that path (`e12ba8a8b`). **That cost
grows with concurrency, which is the direction that matters.**

**It has now been measured**, on `perf/direct-pull-measured`, by building a finished direct-pull path
on current code with a real blocking wait:
[`perf-direct-pull-measured.md`](perf-direct-pull-measured.md). **The objection holds, and by a wide
margin.** That note also corrects two things below: which commit the busy-spin was live on (an
earlier one than this note assumes - by `0df84c9e5` it was dead code and the worker was taking from a
`LinkedBlockingQueue`), and the claim further down that direct pull makes `DynamicLoadFactor`
disappear (it does not; it removes the `ThreadPoolExecutor.getQueue().size()` reading, which is the
part the virtual-threads argument actually needs).

## The three results, and what each one settles

**1. ~~The rework was three times SLOWER.~~ RETRACTED - see above.** The measurement is real; what it
measured is a busy-spin, not the architecture. Kept here only so the retraction has something to point
at.

**2. The O(n) shard count was suspected then, and is refuted now.** *"cache counts of work awaiting
selection - the slowest stage of controller loop and broker poller loop - because it's O(n) shards."*
The same suspicion arose today. **It is measurably wrong**: `KEY` ordering puts ~500,000 shards through
that code path against `UNORDERED`'s ten and is *faster*
([`perf-hypothesis-register.md`](perf-hypothesis-register.md), hypothesis 6). A four-year-old suspicion, finally
tested.

**3. Poller throttling was already the named suspect.** *"identified possible issue with poller
throttling, doesn't seem to help which is also interesting."* **That is today's conclusion** - the
Kafka client is the limit, in-flight plateaus near 2,750 for PC and 2,848 for a bare consumer with a
thread pool, and nothing inside the engine moves it.

**Two investigations, four years apart, different methods, same answer.** That is the strongest
corroboration available for it.

## What this costs, stated plainly

**The prior-art rule exists for exactly this and was not followed.** The branches were registered in
[`docs/refactoring.md`](../refactoring.md) the whole time, under a heading that names them. A day of
experiments re-derived, more weakly, what one branch already showed - and the register's "mostly
dead-ends" summary hid it, because a verdict without a reason is not prior art.

**The fix is the reason, not the verdict.** This note exists so the next person reads *"three times
slower, and poller throttling was the suspect"* rather than *"dead end"*.

## The argument for direct pull that has nothing to do with speed

**Owner's point, 2026-08-21, and it reframes the whole branch:** *even if it is not faster, is it not
still better to talk directly to the WorkManager than to maintain another buffer?*

**Yes - and it is worth more than the throughput it failed to deliver.** The intermediate buffer is not
one thing, it is a system that exists only to keep that buffer at the right depth:

- `DynamicLoadFactor`, its warm-up, its cool-down, its step-up, and its cap at 100 - **which has its own
  open defect**, astubbs#155 *"Max loading factor steps reached: 100/100"*.
- `checkPipelinePressure()`, `isPoolQueueLow()`, `getQueueTargetLoaded()`, `calculateQuantityToRequest()`.
- `getNumberOfUserFunctionsQueued()`, which reaches into `ThreadPoolExecutor.getQueue().size()`.
- The buffer's own memory cost, and the offsets it holds beyond the committable one - which
  `DynamicLoadFactor`'s javadoc warns "could cause much larger replays than necessary".

**If workers pull straight from the shards, every one of those disappears.** There is no buffer to size,
so there is no load factor, no pressure check, and no queue depth to read.

**And that last one is the unlock.** The blocker on virtual threads
([`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md)) is that the
pressure system reads `getQueue().size()` and `getActiveCount()` off the `ThreadPoolExecutor`, which a
virtual-thread executor does not expose - the exact problem PR astubbs#51's author reported. **Direct pull
removes the question rather than answering it.**

**So the 2022 branch was not merely a failed optimisation.** It was a simplification that happened to be
measured only on speed, and judged only on speed. Re-read with that lens, "1/3 as fast" is a reason to
find out *why it was slow*, not a reason to discard the design - especially now that the thing making
everything slow has been identified and is not in that code.

## What it does not settle

- **Whether it would still be 3x slower.** It predates six years of change and its own commits say the
  control loop had inefficiencies being fixed as it went. It was not a finished comparison.
- **Whether per-thread queues would help *today*** - it was `START:` on a branch that was already
  losing, so the idea never got a clean test of its own.
- **Anything about virtual threads**, which did not exist for this codebase then and remain the one
  untried structural change.

## Actions

- **Assess this family in astubbs#327**, the open branch-and-issue accounting PR, and record reasons
  rather than verdicts for all six engine/queue branches.
- **Amend [`docs/refactoring.md`](../refactoring.md)** so the entry carries "3x slower than
  ThreadPoolExecutor; poller throttling suspected" instead of "mostly dead-ends".
