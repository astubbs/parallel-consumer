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

## The three results, and what each one settles

**1. The rework was three times SLOWER.** *"Inefficient routines in control loop fixed, still only
1/3~ a fast as ThreadPoolExecutor version."* Direct pull, per-thread queues and a central facade,
built and working, and still a third of the speed of the plain `ThreadPoolExecutor`.

**That is a much stronger result than anything measured today.** Today's experiments made the existing
design cheaper and found it did not matter. This one replaced the design and found the replacement
worse.

**2. The O(n) shard count was suspected then, and is refuted now.** *"cache counts of work awaiting
selection - the slowest stage of controller loop and broker poller loop - because it's O(n) shards."*
The same suspicion arose today. **It is measurably wrong**: `KEY` ordering puts ~500,000 shards through
that code path against `UNORDERED`'s ten and is *faster*
([`perf-hypothesis-register.md`](perf-hypothesis-register.md) #6). A four-year-old suspicion, finally
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
