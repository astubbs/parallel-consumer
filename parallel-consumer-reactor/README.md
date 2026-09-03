<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The Reactor engine - return a Publisher, and the record completes when it does

An adapter that lets your record handler return any Reactor `Publisher` - a `Mono`, a `Flux`, a
WebClient call - and marks the record succeeded only when that publisher completes. Parallel
Consumer still owns shard order, retries and commits; Reactor owns the threading.

## What is in it

One class, `ReactorProcessor`, extending core's `ExternalEngine`. Its single entry point is
`react(Function<PollContext, Publisher<?>>)`. Subscription runs on a `Scheduler` you can supply to
the constructor; the default is `Schedulers::boundedElastic`. There is no separate interface and no
`Jstream` variant - the publisher *is* the result stream.

## Pick this over core when

Your handler already speaks Reactor, or your work is non-blocking IO. Core costs one pool thread per
in-flight record; here the record's thread is released as soon as the publisher is returned, and
concurrency is bounded by `ParallelConsumerOptions#getTargetAmountOfRecordsInFlight` rather than by
a pool size. **If your work is CPU-bound, or blocking anyway, core is the simpler choice and gives
up nothing.**

[`docs/features/reactor-integration.yaml`](../docs/features/reactor-integration.yaml) **owns the
capability record** - maturity, setup coordinates and the boundaries.

## Constraints, before you write against it

- **No exactly-once.** `ExternalEngine` rejects `PERIODIC_TRANSACTIONAL_PRODUCER` in its
  constructor, naming Reactor in the message. Core's rule, shared with `vertx` and `mutiny`.
- **Do not block inside the function that builds the publisher.**
  `ExternalEngine.setupWorkerPool` pins this module to a single dispatch thread; that thread exists
  only to subscribe and return.
- **Completion, not return, is success - and an error *signal* is a failure.** `ReactorProcessor`'s
  `onError` calls `onUserFunctionFailure` for every record in the context, so a publisher that
  errors retries exactly as a thrown exception would; a `PCRetriableException` carried on the error
  signal is recognised there and logged at debug rather than error, the same as in core.
- **`react` takes one `PollContext`**, which is a batch when `batchSize` is set - the whole batch
  succeeds or fails together.

## Where the topics are owned

- Usage, with the worked snippet: the root [`README.adoc`](../README.adoc), anchor
  `[[project-reactor]]`.
- Ordering, retries and commit behaviour: unchanged from
  [`parallel-consumer-core`](../parallel-consumer-core) - this module changes *where the work runs*,
  nothing else.
- Testing mechanics: [`docs/testing.md`](../docs/testing.md). This module's tests are unit-only
  (`ReactorPCTest`, `ReactorBatchTest`) and need no Docker.
