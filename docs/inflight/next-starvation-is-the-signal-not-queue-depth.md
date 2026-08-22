# The buffer controller reads queue depth; starvation is the signal that actually generalises

<!-- inflight-type: feature -->
<!-- inflight-impact: architecture -->

**Antony's question, 2026-08-22: "if a virtual-thread executor has no queue to read, why would a
normal thread need one?"** Following it through says the queue was never the right input.

## Why a pooled executor has a queue at all

A fixed pool is a **scarcity**: `poolSize` threads, more tasks than that, so the surplus waits
somewhere - the `LinkedBlockingQueue` in `setupWorkerPool`. A virtual-thread executor creates a
thread per task, so there is no scarcity and nothing waits. **The queue is an artefact of pooling,
not of concurrency.**

What Parallel Consumer actually wants to know is *how much work is outstanding*. Queue depth was a
convenient proxy that a `ThreadPoolExecutor` happened to expose. `UserFunctionTaskAccounting` already
fixed that half - it derives `queued` and `active` by conservation from PC's own counters, so the
figure no longer depends on an executor detail and works for both kinds.

## The half that is still wrong

**Under virtual threads the controller's input is a constant.** `queued = submitted - started -
neverStarted`, and a virtual thread starts essentially the instant it is submitted, so `queued` sits
at approximately zero permanently. `isPoolQueueLow()` is therefore always true, and
`DynamicLoadFactor` steps up unconditionally until it saturates.

That is not obviously harmful - with no thread scarcity, "pull more" is a reasonable default, and a
sweep on 2026-08-22 showed the virtual-thread arm reaching exactly its configured 5,000 in flight
where the platform arm topped out at 3,526. But it is **a control loop whose input never varies**,
which is dead machinery rather than control, and it is the second time this signal has needed special
handling for a new executor.

## PC already has the better signal, in exactly one of three paths

Direct pull does not read a queue. It uses a **starvation signal**: a worker that was allowed to work
and found nothing, which `AbstractParallelEoSStreamProcessor` consumes to step the load factor
(`DirectPullWorkerPool#starvedSinceLastCheck`).

**That measures the condition rather than a proxy for it** - are workers idle for want of work - and
it is executor-agnostic by construction. It works for a pooled platform executor, for
thread-per-task, and for pullers, because it is a property of the *workers*, not of the *plumbing
between the controller and the workers*.

| Path | Signal | Generalises? |
|---|---|---|
| Platform pool | derived queue depth vs target | Needed `UserFunctionTaskAccounting` to survive the executor change |
| Virtual threads | same computation, structurally ~0 | Degenerate - the controller cannot observe anything |
| Direct pull | worker found no work | **Yes** - and it is the only one that does |

## What to do

**Try starvation as the single signal for every path.** A pooled worker that finishes its task and
finds the queue empty is starved, in exactly the sense direct pull already means. If that works, the
queue-depth reading, its target, and the per-executor special cases all go, and `DynamicLoadFactor`
gets one input that means the same thing everywhere.

**Measure before believing it.** Starvation is a lagging signal - it fires once a worker has already
gone idle - where queue depth is leading, and a controller driven by a lagging signal can oscillate.
That is the real objection and it is empirical, not architectural. Operating points that discriminate:
0ms and 2ms at high concurrency, where the buffer decision is the only thing that matters.

**Blocked on nothing**, except that the direct-pull arm cannot currently complete a run at high
concurrency - see the contention investigation. The signal itself is independent of that.

See also: [`next-pre-rendered-work-order-list.md`](next-pre-rendered-work-order-list.md) - if the
work-order list ever lands, the buffering decision moves into PC's own structure entirely and this
question changes shape again;
[`perf-virtual-threads-measured.md`](perf-virtual-threads-measured.md).
