# `closeDrainFirst()` on a never-started instance throws `NoSuchElementException: No value present`

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

Build a `ParallelEoSStreamProcessor` and close it without ever calling a `poll*` method, and the
close reports an internal-looking failure that names nothing about what actually went wrong:

```
java.util.NoSuchElementException: No value present
	at bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.waitForClose(...)
```

## Why

Two methods in `parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java`
disagree about the `UNUSED` state:

- `transitionToClosing` guards it - `if (state == State.UNUSED)` sets `CLOSED` directly.
- `transitionToDraining` has no such guard; it sets `DRAINING` unconditionally.

`waitForClose` then loops `while (!state.equals(CLOSED))` and resolves the control thread's future
with a bare `this.controlThreadFuture.get()` - an unguarded `Optional::get`, unlike the
`isPresent()`/`isEmpty()`-checked reads elsewhere in the same class. `controlThreadFuture` is only
populated by `supervisorLoop`, so on a never-started instance it is empty and the `get()` throws.

So the two close modes behave differently, and only one of them is broken:

| Call | Never-started behaviour |
|---|---|
| `close()` / `closeDontDrainFirst()` | fine - the `UNUSED` guard sets `CLOSED`, so the wait loop is skipped entirely |
| `closeDrainFirst()` | throws `NoSuchElementException`, and the state is left at `DRAINING`, so a retried close fails the same way |

`closeWithoutRunningShouldBeEventBasedFast` in
`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/ParallelEoSStreamProcessorTest.java`
covers only the working half - it calls `closeDontDrainFirst()`. Nothing exercises the drain mode on
an unused instance.

## Scope

Diagnostic quality, not a leak. The worker pool is built eagerly in the constructor, but
`ThreadPoolExecutor` creates its threads lazily on first submission, so a never-started instance has
no worker threads to strand. What it costs is a confusing exception: it reads as an internal null
bug rather than "this instance was never started".

## Not fixed here deliberately

Found while fixing the neighbouring defect on `fix/close-shuts-down-worker-pool` (a drain that threw
skipped the worker pool shutdown, leaking non-daemon threads). That fix is about the close sequence's
exception safety and is worth keeping reviewable on its own; this one is a state-machine
inconsistency between the two transition methods, and wants its own decision about whether the
remedy is an `UNUSED` guard on `transitionToDraining`, a guarded read in `waitForClose`, or both.
