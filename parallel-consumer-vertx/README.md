<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The Vert.x engine - hand each record to Vert.x and let it own the concurrency

An adapter that lets your record handler return a Vert.x `Future` - typically a `WebClient` HTTP
call - and completes the record only when that future resolves. Parallel Consumer still decides
shard order, retries and commits; Vert.x decides how many requests are in flight and on which
threads.

## What is in it

Four types, and no engine of its own:

- `VertxParallelStreamProcessor` - the interface. `vertxHttpReqInfo` (return a `RequestInfo` and the
  module builds and sends the request), `vertxHttpRequest` (build the `HttpRequest` yourself),
  `vertxHttpWebClient` (send it yourself, hand back the `Future`), `vertxFuture` (any Vert.x future
  at all, not just HTTP), and `batchVertxFuture` for the batched form.
- `VertxParallelEoSStreamProcessor` - the implementation, extending core's `ExternalEngine`.
- `JStreamVertxParallelStreamProcessor` / `JStreamVertxParallelEoSStreamProcessor` - the same thing
  exposed as a Java `Stream` of results (`vertxHttpReqInfoStream`) rather than callbacks.

## Pick this over core when

Your work is **non-blocking IO you would otherwise block a worker thread on**. Core gives you one
pool thread per in-flight record, so concurrency costs threads; here the record's thread is released
as soon as the future is handed back, and concurrency is bounded by
`ParallelConsumerOptions#getTargetAmountOfRecordsInFlight` rather than by a pool size.
`docs/features/vertx-integration.yaml` **owns the capability record** - maturity, setup coordinates,
and the boundaries, including the one that matters most: *this does not make a blocking handler
non-blocking*.

## Constraints, before you write against it

- **No exactly-once.** `ExternalEngine` rejects `PERIODIC_TRANSACTIONAL_PRODUCER` in its
  constructor, with a message naming Vert.x explicitly. This is core's rule, not this module's, and
  it applies to `reactor` and `mutiny` equally.
- **The dispatch thread must never block.** `ExternalEngine.setupWorkerPool` forces the pool to a
  single thread here, because that thread exists only to hand work to Vert.x. Blocking inside the
  function that *constructs* the future serialises the whole module.
- **A record is not done when your function returns - it is done when the future completes.**
  `ExternalEngine.onUserFunctionSuccess` deliberately does not mark success for async work, and the
  mailbox entry is deferred to the future's callback.
- **Vert.x stays on 4.x.** `vertx.version` in this pom is a 4.5 line; the 5.x move is a tracked
  deferred major in [`docs/inflight/deps-deferred-majors.md`](../docs/inflight/deps-deferred-majors.md).

## Where the topics are owned

- Usage, with the worked snippet: the root [`README.adoc`](../README.adoc), anchor
  `[[http-with-vertx]]`.
- The capability record: [`docs/features/vertx-integration.yaml`](../docs/features/vertx-integration.yaml).
- Ordering, retries and commit behaviour: unchanged from
  [`parallel-consumer-core`](../parallel-consumer-core) - this module changes *where the work runs*,
  nothing else.
- Testing mechanics: [`docs/testing.md`](../docs/testing.md). Worth knowing before you run anything
  here: the unit tests stand up WireMock, and `VertxConcurrencyIT` under `src/test-integration/java`
  needs Docker.
