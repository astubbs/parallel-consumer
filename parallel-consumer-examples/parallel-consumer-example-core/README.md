<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Core example - the setup you copy first, plus the recipes around it

One class, `CoreApp`, holding the minimal working setup and then a series of small self-contained
patterns. Start here if you are copying anything.

## What to look at

- `setupParallelConsumer()` - the whole setup: build a `KafkaConsumer` and `KafkaProducer`, choose
  `ordering(KEY)` and `maxConcurrency(1000)`, and `subscribe`. This is the block the root
  `README.adoc` renders at its `[[common_preparation]]` anchor.
- `run()` - `poll(record -> ...)`, the simplest form.
- `runPollAndProduce()` - `pollAndProduce`, returning a `ProducerRecord` and taking a callback with
  the resulting `RecordMetadata`.
- Then, in decreasing order of how often you will want them: `batching()`, `customRetryDelay()`
  (exponential backoff via `retryDelayProvider`), `maxRetries()`, `circuitBreaker()`, `closeModes()`
  (`drainTimeout` / `shutdownTimeout` / `closeDrainFirst`).

## Two things to know

- **Most of these methods are never called.** `maxRetries`, `circuitBreaker` and `closeModes` exist
  so that their `// tag::` regions can be included into the documentation and so that an API change
  breaks compilation instead of leaving the README wrong. `closeModes`'s javadoc states this;
  [the examples parent README](../README.md) covers what that means before you edit one.
- **`RandomUtils.nextInt()` in the topic names is a test convenience**, not a pattern to copy - it
  keeps repeated test runs from colliding on one broker.

## Running it

`./mvnw test -pl :parallel-consumer-example-core -am` (from the repo root). `CoreAppTest` subclasses
`CoreApp` to swap in core's `LongPollingMockConsumer` and a `MockProducer`, feeds three records, and
waits for the commit to reach offset 3 - so it is a **unit test and needs no Docker**. Those mock
helpers come from `parallel-consumer-core`'s `tests`-classifier artifact;
[`docs/testing.md`](../../docs/testing.md) owns where the shared harness lives.
