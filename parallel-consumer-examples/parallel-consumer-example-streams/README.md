<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Streams example - preprocess in Kafka Streams, then fan out concurrently

`StreamsApp` runs a two-stage pipeline in one process: a Kafka Streams topology transforms records
from an input topic onto an output topic, and a Parallel Consumer subscribes to *that* output topic
and processes it concurrently. This is the answer to "my Streams topology has one slow stage" that
does not involve adding partitions.

## What to look at

- `run()` - two calls, `preprocess()` then `concurrentProcess()`, and the whole pattern is in that
  ordering. The root `README.adoc` renders this tagged region at its `[[streams-usage-code]]`
  anchor.
- `preprocess()` - an ordinary `StreamsBuilder` topology, `mapValues` then `.to(outputTopicName)`.
  Nothing about it is Parallel-Consumer-aware.
- `setupParallelConsumer()` - subscribes to `outputTopicName`, not to the input topic. That is the
  join between the two halves.
- `getKafkaConsumer()` - note `ENABLE_AUTO_COMMIT_CONFIG` set to `false`, with the comment
  explaining it: Parallel Consumer manages commits itself.

## Running it

`./mvnw verify -pl :parallel-consumer-example-streams -am` (from the repo root). **This one needs
Docker.** `StreamsAppTest` lives in an `integrationTests` package, which is how failsafe selects it,
and it extends core's `BrokerIntegrationTest` to get a real broker, produces three records to the
input topic, and waits for the app's counter to reach three - proving the records made it through
both stages.

Unlike the other examples, `StreamsApp` takes `bootstrapServers` through its constructor, so you can
also drive it from your own code against a real cluster. There is still no `main` method.

## Where the topic is owned

[`docs/features/staging/kafka-streams-integration.yaml`](../../docs/features/staging/kafka-streams-integration.yaml)
holds the capability record - **staged, not published**, because part of its evidence is a release
that has not been cut; that directory's README owns the rule for moving it up.

## One trap in this module's pom

The `jackson-bom` import pinned to `2.18.9` is a CVE floor, not a routine bump: `kafka-streams`
drags in a vulnerable 2.16.2, and 2.18.0-2.18.8 would trade one vulnerability for another. The pin's
own comment carries the full reasoning, including why it must stay module-local - promoting it to
the root pom breaks WireMock in `parallel-consumer-vertx`.
