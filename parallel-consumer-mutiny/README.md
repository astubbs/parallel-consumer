<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The Mutiny engine - return a Uni, and the record completes when it resolves

An adapter that lets your record handler return a SmallRye Mutiny `Uni` (or a `Multi`), completing
and committing the record only when that asynchronous work resolves. Parallel Consumer still owns
shard order, retries and commits; Mutiny owns the threading. Aimed squarely at applications that are
already Mutiny-shaped - Quarkus, most often.

## What is in it

One class, `MutinyProcessor`, extending core's `ExternalEngine`. Its entry point is
`onRecord(Function<PollContext, Uni<T>>)`. Subscription runs on an `Executor` you can supply to the
constructor; the default is `Infrastructure.getDefaultWorkerPool()`. A returned `Multi` is
subscribed as-is, a single item is wrapped, and `null` is treated as an empty result - all three
count as success.

## The one thing that makes this module different: it needs Java 17

Every other module targets Java 8 bytecode via Jabel. This one does not, and it says so in its own
`pom.xml` via `release.target` - **that comment is the full reasoning and this file will not
duplicate it.** In one line: Mutiny's `Multi` implements `java.util.concurrent.Flow.Publisher`, and
SmallRye Mutiny 2.x is itself compiled for Java 17, which the release level cannot detect.

The user-facing consequence - a Java 8 or 11 application builds cleanly and then dies with
`UnsupportedClassVersionError` - is stated in the root [`README.adoc`](../README.adoc) at the
`[[java-version-per-module]]` anchor, which **owns that warning**. Adding this module to a project is
the entire opt-in; nothing else in the build pulls Mutiny in.

## Other constraints, before you write against it

- **No exactly-once.** `ExternalEngine` rejects `PERIODIC_TRANSACTIONAL_PRODUCER` in its
  constructor. Core's rule, shared with `vertx` and `reactor`.
- **Do not block inside the function that builds the `Uni`.**
  `ExternalEngine.setupWorkerPool` pins this module to a single dispatch thread; that thread exists
  only to subscribe and return.
- **A failure signal is a failure.** `MutinyProcessor.onError` calls `onUserFunctionFailure` for
  every record in the context, so a failed `Uni` retries as a thrown exception would; a
  `PCRetriableException` carried on the failure is recognised there and logged at debug rather than
  error.
- **`onRecord` takes one `PollContext`**, which is a batch when `batchSize` is set - the whole batch
  succeeds or fails together.

## Where the topics are owned

- The capability record - maturity, setup coordinates, boundaries, and the validation this module
  must pass: [`docs/features/mutiny-integration.yaml`](../docs/features/mutiny-integration.yaml).
- The Java floor, from the user's side:
  root [`README.adoc`](../README.adoc), `[[java-version-per-module]]`; from the build's side, the
  `release.target` comment in this module's `pom.xml`.
- Ordering, retries and commit behaviour: unchanged from
  [`parallel-consumer-core`](../parallel-consumer-core) - this module changes *where the work runs*,
  nothing else.
- Testing mechanics: [`docs/testing.md`](../docs/testing.md). This module's tests are unit-only
  (`MutinyPCTest`, `MutinyBatchTest`) and need no Docker.
