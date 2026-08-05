# MDC (diagnostic context) propagation into the processing threads

Branch `feat/mdc-context-propagation`. Raised as a non-blocking finding on #197 (mirror of `upstream #907`,
mirrored here as #195): PC never carried the caller's SLF4J MDC across into the worker pool, so a `trace_id` /
`request_id` / tenant established by the caller was invisible in the logs their user function wrote.

## What landed

`MdcPropagation` (core `internal`) - capture on the thread that has the context, `enter(..)` as a
try-with-resources scope on the thread that runs the work. Wired through `PCModule.mdcPropagation()` and gated by
`ParallelConsumerOptions.propagateMdc`, **default true**.

Boundaries covered: core worker pool, vert.x event loop, Reactor scheduler, Mutiny executor. The controller and
broker-poller threads `adopt(..)` the context outright (they serve one instance for life).

## Decisions a future session should not relitigate without new information

- **PC's own keys win a collision.** `pcId` / `offset` are applied *after* the caller's map at every call site, so
  a caller key of the same name cannot shadow them. PC's own log lines are read by those keys.
- **Capture is at `poll*()` time, not per record.** None of PC's threads exist yet at that point and the MDC is not
  inheritable, so that is the only moment the caller's context is reachable. Consequence, and it is documented on
  the option: a *request-scoped* value in the MDC when `poll*()` is called gets pinned for the life of the consumer.
  PC logs the captured **keys** (not values) at INFO precisely so that mistake is discoverable.
- **Default on.** Not propagating fails silently for everyone; propagating fails visibly (an extra key in a log
  line) and has an off switch. `propagateMdc=false` restores the old behaviour *exactly*, including the pre-existing
  leak of the user function's own `MDC.put` calls onto the pooled thread - that is deliberate, so the flag is a true
  kill switch rather than a half-revert.

## Known gap (deliberate, not forgotten)

Reactor/Mutiny propagation covers **the invocation of the user's function and PC's terminal signal handling**. It
does not follow the operators of the `Publisher`/`Uni` the user returns onto further schedulers - that needs
Reactor's own `io.micrometer:context-propagation` and is the user's call, not PC's. Say so if anyone reports
"context missing deep in my reactive chain".

## Testing note

The leak-on-reuse test is the one that matters and it is **verified to detect the defect**: flipping
`setupWithSingleWorkerThread(false)` in `MdcContextPropagationTest#contextDoesNotLeakToTheNextTaskOnTheSamePooledThread`
reproduces four poisoned keys carried across five records on one pooled thread. The three engine tests were verified
the same way (temporarily overriding `getDefaultOptions()` with `propagateMdc(false)`). Redo that flip if you change
the isolation logic - a green test here is worth nothing if it cannot go red.
