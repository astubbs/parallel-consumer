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
  kill switch rather than a half-revert. **Default-on is the one decision here the maintainer may want to overrule**
  - the residual risk (a request-scoped value pinned for the consumer's life) is real, and flipping the default to
  off is a one-line change if that trade is judged the wrong way round.
- **The startup INFO line fires exactly once per instance.** `captureCallersDiagnosticContext()` is called only from
  `supervisorLoop`, which throws `IllegalStateException` if `poll*()` is called more than once - so it is structurally
  impossible for it to land on a per-poll or per-record path. Keep it that way; an INFO line on the hot path would be
  a real regression.

## Measured cost (not just reasoned about)

`ThreadMXBean.getThreadAllocatedBytes` around a million `capture()`/`enter()`/`close()` cycles, logback 1.6.1:

| Case | Bytes allocated per batch |
|---|--:|
| Caller never touches the MDC (the default) | **0** |
| Propagation disabled | **0** |
| Caller has a 2-key MDC - `capture()` on the controller thread | 240 |
| Caller has a 2-key MDC - `enter()`+`close()` on a clean worker thread | 184 |

So the "nil hot-path cost for users who never touch the MDC" claim is measured, not assumed: `getCopyOfContextMap()`
returns `null` without allocating on an empty context, and `enter(null)` returns the shared `CLEAR_ON_EXIT` singleton.
For a caller who *does* use the MDC it is ~424 bytes per batch, against the `FutureTask`, two `ArrayList`s and
`PollContextInternal` that `runUserFunction` already allocates per batch.

Caveat worth knowing: the zero is a property of logback's `MDCAdapter`, which returns `null` for an empty context.
Some other SLF4J bindings return an empty map instead, which would make `capture()` allocate one small map per batch.
Behaviour is correct either way - only the zero-allocation claim is binding-specific.

## Known gap (deliberate, not forgotten)

Reactor/Mutiny propagation covers **the invocation of the user's function and PC's terminal signal handling**. It
does not follow the operators of the `Publisher`/`Uni` the user returns onto further schedulers - that needs
Reactor's own `io.micrometer:context-propagation` and is the user's call, not PC's. Say so if anyone reports
"context missing deep in my reactive chain".

## Testing note

The leak-on-reuse test is the one that matters and it is **verified to detect the defect**: flipping
`setupWithSingleWorkerThread(false)` in `MdcContextPropagationTest#contextDoesNotLeakToTheNextTaskOnTheSamePooledThread`
reproduces four poisoned keys carried across five records on one pooled thread. The three engine tests were verified
the same way (temporarily overriding `initAsyncConsumer(..)` with `propagateMdc(false)`); each fails on its own
"context must be visible on the &lt;engine&gt;" assertion, showing `values seen: [null, ...]`. Redo that flip if you
change the isolation logic - a green test here is worth nothing if it cannot go red.

The three engine tests share `MdcBoundaryProbe` (core test-jar) by *composition* - each already extends its own
module's unit-test base, so a common superclass is not available. The probe holds the bookkeeping and the two
assertions that are identical across engines; the per-engine bits stay in the tests, because they are what stops a
test quietly degrading into a re-run of the core worker-pool case: Reactor asserts its scheduler thread *positively*
(`boundedElastic`), vert.x and Mutiny can only assert "not a `pc-` thread" since their executor is caller-supplied.
