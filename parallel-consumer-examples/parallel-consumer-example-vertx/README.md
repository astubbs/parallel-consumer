<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Vert.x example - one HTTP call per record, non-blocking

`VertxApp` builds a `JStreamVertxParallelStreamProcessor` and, for every record, returns a
`RequestInfo` describing an HTTP request. The Vert.x module sends it, and the record completes when
the response does.

## What to look at

- The options block is unchanged from the core example - `ordering(KEY)`, a consumer, a producer.
  **Switching engines is a change of processor type, not of configuration.**
- `parallelConsumer.vertxHttpReqInfoStream(context -> new RequestInfo(host, port, "/api", params))`
  - the tagged region the root `README.adoc` renders at its `[[http-with-vertx]]` anchor. You return
  a description of the request; you never touch the `WebClient`.
- `resultStream.forEach(...)` - the `JStream` variant hands results back as a Java `Stream` instead
  of a callback. `VertxParallelStreamProcessor` in the module has the callback forms, and
  `vertxFuture` for work that is not HTTP.

## Running it

`./mvnw test -pl :parallel-consumer-example-vertx -am` (from the repo root). `VertxAppTest` is a
**unit test - no Docker**: it stands up WireMock via core's shared `WireMockUtils`, subclasses
`VertxApp` to point at the WireMock port and to swap in `LongPollingMockConsumer`, feeds three
records and waits for the commit to reach offset 3.

For what the module itself constrains - the single dispatch thread, no exactly-once, and the fact
that this does not make blocking work non-blocking - see
[`parallel-consumer-vertx`](../../parallel-consumer-vertx/README.md), which owns those.
