# Next: the mailbox is the largest park site, and it is our own code

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-21, from a flight recording read **properly** - `jfr print` truncates stacks to five
frames by default, and raising it changes what the profile says.

## What the stacks show

Five seconds, `core` engine, zero-cost handler, maxConcurrency 1,000, real broker. Grouped by full
stack rather than by top frame:

| Parks | Where |
|---:|---|
| **17,785** | `addToMailbox` -> `AbstractQueue.add` -> **`LinkedBlockingQueue.offer`** -> `ReentrantLock.lock` |
| 12,107 | `ThreadPoolExecutor.getTask` -> `LinkedBlockingQueue.take` |
| 7,615 | `ThreadPoolExecutor.getTask` -> `LinkedBlockingQueue.take` (condition await) |
| ~1,200 | pool shutdown paths |

**The largest single site is not the pool's work queue. It is PC's own mailbox** - the queue every
worker pushes its completed result into, drained by the control loop in `processWorkCompleteMailBox`.
The two `getTask` groups below it are workers **idle, waiting for work**, which is the pool operating
correctly and not a cost.

## The structure

```java
private final BlockingQueue<ControllerEventMessage<K, V>> workMailBox = new LinkedBlockingQueue<>();
// Thread safe, highly performant, non blocking
```

**The comment is wrong.** `LinkedBlockingQueue.offer` takes a `putLock`. It is thread safe; it is not
non-blocking, and 17,785 parks land on that lock.

**And the shape is the textbook case for something better.** Many producers - every worker thread -
and exactly one consumer, the control loop. **MPSC.** A `LinkedBlockingQueue` is a general-purpose
structure being used for the one access pattern that has well-known specialised alternatives.

## Prior art in this repository - do not start from scratch

**This was tried in 2020 and the branches still exist on origin:**

- `9473ab395` **"START: Disrupter engine experiments"** - LMAX Disruptor.
- `9d3d18af0` **"WIP! Direct ring buffer approach for work publishing - avoid intermediate buffer which
  must be managed"**, on `origin/direct-ringbuffer` and `origin/ringbuffer-batch`. Its message: *"A new
  Thread blocks putting work into the buffer, which feeds directly into the ExecutorService."*

**Read those before writing anything.** They are the same idea, by the same author, and whatever
stopped them is information this note does not have.

## The obvious candidates

- **A ring buffer / MPSC queue.** JCTools' `MpscArrayQueue` or the Disruptor - bounded, lock-free,
  designed for exactly this producer/consumer shape. A new dependency, which is a real cost for a
  library.
- **A sharded counter, for the related problem.** `workMailBox.size()` is read on the drain path, and
  `ProcessingShard.availableWorkContainerCnt` has a drift clamp
  ([`bug-available-work-counter-needs-a-clamp.md`](bug-available-work-counter-needs-a-clamp.md)). The
  JDK already ships the sharded-counter idea as **`LongAdder`** - striped cells, contention-free
  increment, `sum()` on read. It is the right tool for a counter written by many threads and read by
  one, and it needs no dependency.

## State this as a hypothesis, because the register says to

**This is a profile reading, and profile readings have gone 0 for 2 in this investigation.** The queue
lock looked exactly this convincing and removing it cost 69%. **The mailbox is where threads wait; that
is not yet evidence it is where throughput is lost.**

**The test, before any redesign:** replace the mailbox with an MPSC structure behind the same interface
and measure at 0ms, 2ms and 100ms with repeats. If throughput does not move, it joins the register.
**And note the trap the last experiment fell into** - do not change the structure and its `size()`
behaviour at the same time, or the result is uninterpretable.

## Why it might genuinely be different this time

Two reasons to think this is not another zero, stated so they can be checked rather than believed:

- **It is the only park site that is PC's own code.** The others are workers idling.
- **It scales with concurrency in the wrong direction.** Every worker completing a record touches one
  lock, so the contention rises with exactly the setting users are told to raise.
