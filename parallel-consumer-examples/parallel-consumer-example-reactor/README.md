<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Reactor example - return a Publisher per record

`ReactorApp` builds a `ReactorProcessor` and calls `react(...)`, returning a `Mono` for each record.
The record completes when the publisher does.

## What to look at

- The options block is unchanged from the core example - `ordering(KEY)`, a consumer, a producer.
  **Switching engines is a change of processor type, not of configuration**: `new
  ReactorProcessor<>(options)` in place of `ParallelStreamProcessor.createEosStreamProcessor`.
- `parallelConsumer.react(context -> Mono.just("something todo"))` - the tagged region the root
  `README.adoc` renders at its `[[project-reactor]]` anchor. `Mono.just` is a placeholder for your
  real publisher; the shape of the callback is the lesson, not its body.
- `closeDrainFirst()` on the way out - finish what is in flight before shutting down.

## Running it

`./mvnw test -pl :parallel-consumer-example-reactor -am` (from the repo root). `ReactorAppTest` is a
**unit test - no Docker**: it subclasses `ReactorApp` to swap in core's `LongPollingMockConsumer`
and a `MockProducer`, feeds three records, and waits for the commit to reach offset 3.

For what the module itself constrains - the single dispatch thread, no exactly-once, and error
signals counting as failures - see [`parallel-consumer-reactor`](../../parallel-consumer-reactor/README.md),
which owns those.
