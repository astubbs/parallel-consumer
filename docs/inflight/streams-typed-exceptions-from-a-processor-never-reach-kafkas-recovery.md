# A `TaskCorruptedException` raised inside a processor is wrapped, so Kafka never recovers the task

<!-- inflight-type: bug -->
<!-- inflight-impact: reliability -->

`parallel-consumer-streams` (astubbs#255). **This is what the dispatch seam's default-off is currently
waiting on**, and it is the third reason in a row to be found by measuring rather than by review.

## What happens

`TaskCorruptedException` and `TaskMigratedException` are not processing errors. They are Kafka Streams'
*control-flow* signals from a task to its `TaskManager`: close this task dirty, revive it, re-initialise
its state, carry on. Stock Streams lets them out of `StreamTask.process()` with their type intact, and
`StreamThread` dispatches recovery on that type.

On the PC path a worker's exception is caught, held, and surfaced at the top of the **next** pump - and
surfaced wrapped in a `StreamsException`. So two things go wrong at once, and only one of them is about
timing:

- **The type is lost.** Recovery never fires, and an application stock Streams would have recovered shuts
  its client down instead.
- **The delivery is late.** Even with the type preserved, the exception arrives one or more pump cycles
  after the record failed, so a caller that expects `process()` itself to throw does not see it.

## The evidence

`StreamThreadTest.shouldReinitializeRevivedTasksInAnyState`, seam on, fails on its first
`assertThrows(TaskCorruptedException.class, ...)` on every parameter combination. Its topology throws
`TaskCorruptedException` from a processor deliberately, which is precisely the shape above.

It fails **identically before and after** the task lifecycle unit, so that work neither caused nor fixed
it - which is the control arm that separates this from the revival defect the same test class was also
reporting. Revival itself is fixed:
`shouldRecoverFromInvalidOffsetExceptionOnRestoreAndFinishRestore` passes on every parameter, and nothing
leaves a StreamThread uncaught any more. The remaining failure is a different mechanism reaching the same
consequence.

Reproduce by flipping the oracle execution's `pc.streams.dispatch.enabled` pin in
`parallel-consumer-streams/pom.xml` to `true`, running the module's whole `test` phase, and reading
`target/surefire-reports-kafka-upstream/`. Restore the pin afterwards - it is the seam-off
behaviour-preservation claim and must not travel.

## Why it is not refusable

Every other unsupported construct is refused by naming a **topology shape** or a **config key** - a DSL
call, a store type, `processing.guarantee`. This is a property of an *exception object*, thrown from user
code at run time. There is nothing to inspect at build time or at task construction, so the refusal
envelope cannot reach it however far it is extended.

## Two candidate fixes, and neither is free

1. **Rethrow the control-flow types unwrapped.** Cheap and clearly right on its own terms: a
   `TaskCorruptedException` should not become a `StreamsException` merely because it travelled through a
   mailbox. It does not fix the timing, so the upstream case above stays red - which makes it an
   improvement that no test can currently prove.
2. **Surface a worker failure before the pump returns.** Fixes both halves and changes the dispatch
   loop's shape, so it needs its own evidence.

Both belong to the error-surfacing work, alongside astubbs/parallel-consumer#271's open review thread on a
worker's failure being committed past - the same asynchrony, seen from the commit side rather than the
recovery side.

## Delete when

A corruption signal raised inside a topology reaches Kafka's recovery under the seam, with the upstream
case green, and the dispatch default has been re-decided against a fresh seam-on measurement.
