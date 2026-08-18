# Make a throwable safe to log, instead of guarding every site that logs one

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

## What astubbs#267 shipped, and why it is the wrong shape long-term

`ThrowableUtils.logWithoutEscaping(reported, logCall)` wraps the log statement so a throwing render
cannot escape, attaching the failure to `reported` as suppressed. It is **defensive**: it protects
the code after the log by letting the log line be lost. Eight call sites carry it, and each reads as
ceremony around an ordinary log statement.

Two costs. The log line is exactly what a user needs when a hostile throwable is involved, and that
is the case where it is dropped. And a guard whose value is invisible at the call site is one a
future reader tidies away.

## The proposal

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

## Why this is not a revert of astubbs#267

It replaces **one** of that PR's several mechanisms. `logWithoutEscaping` and its eight call sites
would go; `describeWithRootCause`, the guarded walk it depends on, the retriable classification, the
concurrent collections and the record-before-you-log orderings all stand on their own. The orderings
in particular remain correct regardless - recording state before doing optional work is right even
when the optional work cannot fail.

## Sequencing

Not urgent. astubbs#267's guards make the current state safe; this makes it *simple*, and recovers
the stack traces. Worth doing before the pattern is copied to a ninth site.

Related: [`bug-close-path-warns-cannot-be-acted-on.md`](bug-close-path-warns-cannot-be-acted-on.md)
records three sites deliberately left unguarded - this proposal would cover them without touching
those lines, but their own classification question has to be answered first, and it is gated on
astubbs#29 (see that note).
