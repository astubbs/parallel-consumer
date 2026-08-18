# Blanket-safe logging: costed, and DECLINED for now - keep the guards

astubbs#267 established that handing a user-supplied throwable to a logger runs that user's code,
and that the logging framework does **not** protect you. Measured, not assumed - Logback's
`ThrowableProxy` constructor calls `getCause()` directly and catches nothing:

```
java.lang.UnsupportedOperationException: rendering me fails
	at ...ParallelEoSStreamProcessorTest$1.getCause
	at ch.qos.logback.classic.spi.ThrowableProxy.<init>
	at ch.qos.logback.classic.Logger.error
```

Google Truth's `StackTraceCleaner` has the identical hole, so this is not a Logback quirk. And the
binding is the *user's* choice, so no version of "use a safer logger" is available to a library.

## What astubbs#267 shipped, and the objection to it

`ThrowableUtils.logWithoutEscaping(reported, logCall)` wraps the log statement so a throwing render
cannot escape, attaching the failure to `reported` as suppressed. It is **defensive**: it protects
the code after the log by letting the log line be lost. Eight call sites carry it, and each reads as
ceremony around an ordinary log statement.

Two costs. The log line is exactly what a user needs when a hostile throwable is involved, and that
is the case where it is dropped. And a guard whose value is invisible at the call site is one a
future reader tidies away.

**Both are real and both were judged acceptable** - see the decision below. They are recorded here
because they are the argument that would be made again, not because they were dismissed.

## The proposal that was costed

Never hand the raw throwable to the logger. Walk it **once**, under the guards astubbs#267 already
built, and hand the logger a plain `Throwable` chain we constructed:

```java
log.error(msg, ThrowableUtils.safeToLog(e));
```

Each link carries the original's type name, its safely-read message, and its `getStackTrace()`. The
logger then walks an object with no overridable behaviour, so the render cannot throw, cannot cycle,
and cannot allocate without bound.

What it buys over the current mechanism:

- **The stack trace survives.** Today a throwing render loses the whole line.
- **Call sites read normally again** - the guard collapses from eight places into one adapter.
- **It is positive rather than defensive**, so its purpose is legible and it will not be tidied away.

Costs, honestly: one allocation per failure log (failure path only), and `safeToLog` must be
genuinely total - it is the thing standing between a hostile throwable and the logger. The walk
guards and their hostile-input tests already exist in astubbs#267, which is what makes this cheap;
`safeToLog` is a new consumer of machinery that is already written and tested.

## It would not have been a revert of astubbs#267

It replaces **one** of that PR's several mechanisms. `logWithoutEscaping` and its eight call sites
would go; `describeWithRootCause`, the guarded walk it depends on, the retriable classification, the
concurrent collections and the record-before-you-log orderings all stand on their own. The orderings
in particular remain correct regardless - recording state before doing optional work is right even
when the optional work cannot fail.

## DECISION (2026-08-18): do not do this yet. Keep the eight guards.

Costed during astubbs#267 and declined, on two findings that only appeared once the numbers were
counted rather than estimated.

**The exposure is a handful of render sites, not ninety.** A throwable is only dangerous to render
if its author is the *user*. User code enters through several doors:

| Where a user-authored throwable originates | |
|---|---|
| `UserFunctions.carefullyRun` wrap sites | 3 |
| the user's rebalance listener (`AbstractParallelEoSStreamProcessor.onPartitionsRevoked`) | 1 |
| async `onError` (reactor, mutiny) | 2 |
| vert.x send failure (`send.onFailure`) | 1 |
| a user **serializer**, wrapped at `ParallelEoSStreamProcessor`, grep `Error while waiting for produce results` | 1 |

**Count the render sites, not the doors** - the doors converge. The serializer case is the worked
example: it is wrapped in an `InternalRuntimeException` and thrown from inside the user function's
own execution, so it arrives at `runUserFunction`'s catch, which astubbs#267 guards. Enumerating
sources is how you end up believing there are more unguarded sites than there are.

Every other one of the ~90 raw-throwable log calls in main renders a Kafka or PC-internal
exception - library classes with ordinary cause chains and no overridden `getCause`. Not
*guaranteed* safe in the abstract, but not adversarial either, which is the property that matters.
They were never at risk, so they are not "sites needing an exemption". astubbs#267 guards the sites
these doors actually reach, plus the control loop and the close path.

**One known instance of the shape, deliberately left**: `ParallelEoSStreamProcessor` logs
`Closing parallel Consumer due to InvalidPidMappingException` and only then calls
`closeOnException(...)` - log before the thing that must happen. The throwable is Kafka-authored, so
by the reasoning above it does not qualify. Recorded so it is a decision rather than an oversight.

**The evidence does not support the pathological case.** There is no known real hostile throwable in
this codebase - every demonstration in astubbs#267 was a synthetic test written for the purpose. What
is genuinely observed, in descending order: cyclic cause chains (real - `initCause` allows
`A -> B -> A`, and a deserialized chain carries no guard at all; the symptom is a **hang**, which is
why `describeWithRootCause`'s cycle and depth guards do earn their place); null messages (very
common, and the original complaint); a `getMessage`/`getCause` that throws (rare and adversarial).

So the guards on the walk are evidence-led. Blanket coverage of the render is insurance against the
rarest case, and rebuilding the logging stack for it is out of proportion.

**Revisit when** a real hostile throwable is observed, or the number of doors grows - a new
integration module is one more `onError`, and that is the tell.

## If it is ever revived, swap the logger, do not touch the call sites

The original proposal above - `safeToLog(e)` at every call site - is the **worse** of the two shapes,
and is kept only because the sanitising walk it describes is the reusable part.

The cheaper form is a drop-in logger. Counted:

| Approach | Edits |
|---|---|
| `safeToLog(e)` at each call site | **90** |
| replace `@Slf4j` with a wrapper field per class | **34** |

Every `log.error(msg, e)` then stays byte-for-byte identical while the wrapper sanitises on the way
through - blanket coverage, no call-site churn, and nothing at a call site for a future reader to
tidy away.

Its real costs, so the next reader does not rediscover them: it gives up the `@Slf4j` convention that
34 classes and every contributor expect; the wrapper must implement enough of SLF4J's surface to be a
genuine drop-in, or it constrains what callers may write; and the sanitised copy must report the
**original** type name, or every stack trace header names the wrapper instead.

## Sequencing

Superseded by the decision above. The one thing still worth watching: if the guard pattern is
copied to a ninth site, that is the signal the door count is growing and the trade-off should be
re-costed.

Related: [`bug-close-path-warns-cannot-be-acted-on.md`](bug-close-path-warns-cannot-be-acted-on.md)
records three sites deliberately left unguarded - that note's classification question stands on its
own and is unaffected by the decision here.
[`test-archunit-does-not-cover-main-code.md`](test-archunit-does-not-cover-main-code.md) lists the
"no raw Throwable to a Logger" rule as a candidate and points back here for why it is not worth
writing.
