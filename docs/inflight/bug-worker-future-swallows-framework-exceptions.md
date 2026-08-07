# Nothing reads the worker thread's `Future`, so framework exceptions vanish

`AbstractParallelEoSStreamProcessor.submitWorkToPoolInner` submits each batch to the worker pool and
parks the result on the work container:

```java
Future outputRecordFuture = workerThreadPool.get().submit(() -> runUserFunction(...));
for (final WorkContainer<K, V> workContainer : batch) {
    workContainer.setFuture(outputRecordFuture);
}
```

`WorkContainer#future` has a getter and is written here - and is **read by nothing in `src/main`**.
Anything thrown out of `runUserFunction` therefore goes into a `Future` that no code ever calls
`get()` on, and disappears without a log line.

That is not the same as user-function failures, which *are* handled: `runUserFunction`'s `catch` logs
at ERROR and marks the record failed. What is lost is everything thrown from the framework's own code
*around* that catch - from inside the failure handler itself, and from the `finally`.

## Why this matters

This is what hid the produce-lock double release for its whole life. That lock was released twice on
every transactional produce; the second release threw `IllegalMonitorStateException` from `cleanUpContext`
in the `finally`, and it went into this future and nowhere else. An investigation that counted
acquires against releases in the debug log read a clean 1:1 and concluded no double release was
happening - see the resolution note appended to
[`docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md`](../plans/2026-08-03-001-investigate-transactional-commit-flake.md)
§11.

The two other `submit()` sites are supervised and do not have this problem:
`BrokerPollSystem#start` keeps its future in `pollControlThreadFuture` and checks it in `supervise()`;
`AbstractParallelEoSStreamProcessor#supervisorLoop` keeps its future in `controlThreadFuture` and
reads it in `isClosedOrFailed`, `waitForClose` and shutdown. The worker future is the only unsupervised
one.

## What would close it

Either read the future somewhere that can log a framework failure loudly, or stop pretending to
return one - drop `WorkContainer#future` and have `runUserFunction` report its own escapes. Deciding
which is the work; the current state is the worst of both, since the field exists and looks like it is
someone's responsibility.

Not urgent on its own - it is a diagnostic hole rather than a live defect - but it is a hole that has
already cost one long investigation, and the next exception thrown from that `finally` will be just as
invisible.
