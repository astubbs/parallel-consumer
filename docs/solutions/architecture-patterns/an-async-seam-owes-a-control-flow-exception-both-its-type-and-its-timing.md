---
title: "An async seam owes a control-flow exception both its type and its timing - fixing one leaves the defect"
date: 2026-09-01
category: architecture-patterns
module: parallel-consumer-streams
problem_type: architecture_pattern
component: service_object
severity: critical
applies_when:
  - Moving work off the thread a framework expects to throw on, and that framework dispatches recovery on the exception's TYPE
  - A wrapper is added "for context" around exceptions crossing a thread boundary
  - A test asserts assertThrows(X.class, () -> oneFrameworkIteration()) and the work now completes asynchronously
  - Deciding whether an unwrap-only fix is worth taking when no test can show it working
  - A commit, checkpoint or flush can run between an asynchronous failure and its delivery
tags:
  - exception-handling
  - control-flow-exception
  - asynchronous-dispatch
  - kafka-streams
  - task-corrupted
  - recovery
  - commit-fence
  - seam
related_components:
  - PcTaskDispatcher
  - StreamTask
  - kafka-streams
---

# An async seam owes a control-flow exception both its type and its timing

## Problem

`parallel-consumer-streams` runs a Kafka Streams processor chain on Parallel Consumer's worker pool
instead of on the StreamThread. A worker's exception is therefore caught on a worker, held, and
handed back to the StreamThread at the top of a later pump.

Some exceptions in Kafka Streams are not errors at all. `TaskCorruptedException` and
`TaskMigratedException` are *control flow*: a task raising one is telling its `TaskManager` to close
it dirty, revive it, re-initialise its state and carry on. `StreamThread` catches them **by type**,
around the processing loop, and dispatches recovery on that type.

Two things went wrong at the seam, and they are independent:

- **The type was lost.** Every worker failure came back out of `process()` wrapped in a
  `StreamsException`. Recovery never fired, and an application stock Streams would have recovered
  shut its client down instead.
- **The delivery was late.** Even unwrapped, the exception arrived one or more pumps after the record
  failed - possibly in a later `runOnce` entirely. Kafka's own
  `StreamThreadTest.shouldReinitializeRevivedTasksInAnyState` asserts the stock shape directly:
  `assertThrows(TaskCorruptedException.class, () -> runOnce(...))`, one iteration, one throw.

The trap is that **each half looks like the whole defect from where you are standing**, and fixing
either alone leaves an application that still shuts down when it should have recovered.

## What made the cheap fix untestable

The unwrap is three lines and obviously right on its own terms: an exception should not change type
merely because it travelled through a mailbox. But with the timing untouched, the upstream case stays
red, so **there is no test that can distinguish the unwrap from its absence** - the exception still
does not arrive inside the `runOnce` the assertion wraps.

That is why it was declined once and recorded rather than taken: a change no test can hold in place
is a change the next refactor removes for free. The lesson generalises past this codebase - when a
defect has two independent causes, a fix for one of them is not a partial fix, it is an
unverifiable one.

## The fix, both halves

**Type.** Mirror the framework's own catch ladder rather than inventing one. Kafka's
`ProcessorNode.process` already rethrows `TaskCorruptedException`, `TaskMigratedException` and
`FailedProcessingException` unchanged instead of handing them to the processing-exception handler, so
they reach the seam with their type intact; both control-flow types extend `StreamsException`, so
passing a `StreamsException` straight through is the whole of it. What must NOT be mirrored is
stock's raw `TimeoutException` rethrow - see the trap below.

**Timing.** A pump that has dispatched work and then finds nothing left to hand out is not idle: it
waits, bounded, for the outcome of what it already has in flight, and re-checks for a failure before
returning. That is enough, because the framework's own loop calls `process()` again while it reports
progress - the pump that dispatched the record returns "progress", the loop comes back, and the
second pump waits and throws. One `runOnce`, one throw, with the type.

## The structural argument for the wait's cost was wrong, and a control arm caught it

The wait looked free: it is reached only when the pump hands out nothing, which is exactly the state
in which the framework's loop breaks out and returns to `poll()`, and it ends at the first outcome.

**"Handed out nothing" turned out to conflate two states.** Either the work queue had nothing to give
- the idle case, where waiting is right - or **the worker pool was full**, which is the normal steady
state under load: plenty of work, nowhere to put it, and the next act would have been to fetch more.
Waiting in the second case throttles *intake*. Measured with the seam's own backpressure switched off
so that nothing else bounded inflow: peak occupancy over a 600-record backlog was **36 with an
unconditional wait and 596 without it**, one term changed on the same machine, broker, topology and
data. Sixteen-fold.

**The throughput was not the worst of it.** The wait had become a second, undesigned memory bound, and
that bound was in the *control arm* of the memory-bound proof - so the control looked almost bounded
and the proof's separation collapsed from 596-against-30 to 36-against-30, while every assertion still
passed. A contaminated control arm turns a measurement into a reassurance.

So the wait is gated on which of the two states the pump stopped in, which is knowable where the pump
loop breaks and nowhere else. **Two lessons, and the second is the general one:** a condition that
reads as "there is nothing to do" often names two states with opposite costs, so name them separately
before building on either; and **an integration arm that measures a resource, not a verdict, is what
caught it** - every functional test passed under the unconditional wait, including the two-arm proof
whose control it had quietly disarmed.

## The third thing, which only shows up once the timing is understood

A failure that is delivered late is also a failure that something can run **past**. Kafka's loop is
`process` then `punctuate` then `maybeCommit`, so a failure landing in that window let a scheduled
commit make *another key's* completed offsets durable for a task that was about to be closed dirty
and rewound. For a `TaskCorruptedException` that is worse than a duplicate: recovery wipes the task's
state stores and re-reads from the committed position, so the committed offsets mark records covered
whose state changes are then thrown away.

So a failure **fences commit-data collection**. Not committing is the safe direction - the frontier
simply does not advance and whoever owns the partition next re-reads.

**Fence the collection, never the "is there work outstanding" query.** They are opposite questions.
The second is what `StreamTask.validateClean` turns into a `TaskMigratedException` so the
`TaskManager` closes the task dirty; fencing that would make a failed task look *clean to close*,
which is the reverse of what a failure should mean. The two are pinned by two tests that would both
have to be deleted to reintroduce either mistake.

## The trap: matching the framework's exception TYPE can break its exception CONTRACT

Stock Streams rethrows a `TimeoutException` from `process()` unchanged, and its `TaskExecutor` reads
that as RETRIABLE - it initialises a task timeout, logs "will move to next task and retry later", and
keeps the task RUNNING. That is safe there only because the record is still in the `RecordQueue` and
genuinely will be re-selected.

An asynchronous seam with retries disabled cannot honour it. The record is permanently failed, the
dispatch bar has closed dispatch, so `process()` returns false for ever, no second exception is
produced, `task.timeout.ms` never trips, and the partition stays paused. Zero throughput, no
exception, nothing logged - reachable from an ordinary broker timeout.

**Faithfulness to a framework is faithfulness to what a caller does with the value, not to the value
itself.** Wrapping tells the truth (this task is dead) and the framework's fatal branch kills it
loudly, which is what it does for any failure it cannot retry.

## How to know it worked

Run the framework's own suite against the patched classes with the seam ON, before and after,
changing nothing else, and read the per-class numbers out of the report directory rather than the
console - **deleting the report directories first**, because a run that dies before that execution
leaves the previous run's XML behind and it reads as a clean pass.

The unit-level proof of the timing half is a preparer that blocks on a latch: dispatch, assert
`pollFailure()` is null (which is what the pump would have seen and acted on), release the latch,
then assert the wait returns *because there was something to see* rather than because it expired, and
that the failure is now available. That separates "the wait works" from "the machine was fast".

## See also

- [`a-query-must-never-mutate-derive-thread-safety-from-callers.md`](a-query-must-never-mutate-derive-thread-safety-from-callers.md) -
  the same seam, the rule that keeps its query surface safe to reach from a second thread
- [`docs/investigating.md`](../../investigating.md) - a fix that works is not evidence of the cause
