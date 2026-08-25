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

## CORRECTION, 2026-08-22: starvation is the ERROR term, not the state term

**Antony, on reading the above: "lags vs leads was the whole idea. Threads waiting for work is wasted
time. So I was trying to stay ahead of demand."**

That is decisive, and it makes the proposal above wrong as written. A controller that waits for
starvation has already lost the time it was built to protect: the worker was idle, and no signal
arriving afterwards gets that back. `DynamicLoadFactor` is **predictive on purpose** - its whole
function is to pull work forward so a worker never has to ask. Replacing a leading indicator with a
lagging one is not a simplification, it is a regression to the condition the machinery exists to
prevent.

**And virtual threads do not rescue it either**, because they are opt-in and JDK 21+. Whatever
replaces this has to work on the platform path, unassisted. "It is degenerate under virtual threads"
is an argument that the *input* is wrong, not that the *controller* is unnecessary.

### What is actually wrong, restated

The controller's shape is right. Its **state input** is wrong: executor queue depth is an
implementation detail of one executor kind, which is why it needed `UserFunctionTaskAccounting` to
survive thread-per-task and is degenerate there anyway.

**The leading signal that generalises is how much SELECTABLE WORK is buffered** - how much could be
handed out right now if a worker came free this instant. That is a property of PC's own shards, not
of the plumbing, so it exists identically for a pooled executor, thread-per-task, and pullers.

**PC can now measure it exactly.** `ShardManager#getUpperBoundOnSelectableWork()` used to return
`min(awaitingSelection, shardCount)` - an estimate, and named as an upper bound because the per-shard
truth was unavailable. The per-shard in-flight count (commit `b73f8b97e`) made that figure exact.
**The better input became available before anyone asked for it.**

### The corrected proposal

| Term | Now | Should be |
|---|---|---|
| **State** (leading) | executor queue depth | selectable work buffered in the shards |
| **Error** (lagging) | none on the pooled path | a worker found no work - what direct pull already does |

Starvation keeps a job, just not the one proposed above: it is the **error term**, the evidence that
the prediction was too low. That is precisely how direct pull already uses it - `consumeStarvationSignal()`
into `maybeStepUp()`. A controller wants both: a state it steers on and an error that tells it when
it steered wrong. Queue depth was a poor state term; starvation is a fine error term and a terrible
state term.

### Why this is worth doing even if nothing else lands

It removes the executor dependency without weakening the prediction, it needs neither virtual threads
nor direct pull, and it deletes a special case rather than adding one. If the work-order list ever
lands, the state term becomes trivially exact - the length of the list - so this change is a step
toward that design rather than a competing one.

**Still to measure**: whether selectable-work-buffered actually predicts as well as queue depth did.
It is a different quantity, not a renaming, and the load factor's step thresholds were tuned against
the old one.

See also: [`next-pre-rendered-work-order-list.md`](next-pre-rendered-work-order-list.md) - if the
work-order list ever lands, the buffering decision moves into PC's own structure entirely and this
question changes shape again;
[`perf-virtual-threads-measured.md`](perf-virtual-threads-measured.md).
