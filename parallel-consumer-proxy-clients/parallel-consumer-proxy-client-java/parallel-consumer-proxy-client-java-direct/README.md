<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - Java direct transport

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

## What it is

The [shared client surface](../parallel-consumer-proxy-client-java-api/README.md) bound straight to
`parallel-consumer-core`, **in the same process**. There is no sidecar, no wire and no protocol
underneath: your `RecordProcessor` runs on core's own worker threads, and its `Outcome` maps onto
core's success and retry paths, so ordering and commit semantics are core's, untouched.

**It is the one client that never speaks the proxy protocol**, and that is what makes it useful
beyond convenience: it is the control arm for the shared API. A conformance scenario that passes for
`pc-java-grpc` and fails here is a claim about the API rather than about a stream. Its pom bans
protobuf, gRPC and the protocol module from the classpath in every scope, so transport detail
leaking into the shared surface fails this module's build.

## What it can do today

Connect, configure, receive records, run the user function, apply per-record outcomes, produce
records back on success, observe the session's end through `sessionEnd()`, and close cleanly.

What it cannot do is not a half-feature but an absence: leases and heartbeats, the manifest
reconnect, worker-death reporting, terminal outcomes and the shutdown drain have no meaning one
layer below the wire, and the sibling transports do not implement them either.

```java
var client = DirectParallelConsumerClient.builder()
        .options(ClientOptions.builder()
                .topics(List.of("orders"))
                .kafkaProperties(Map.of("bootstrap.servers", "localhost:9092", "group.id", "orders"))
                .build())
        .build();                       // or .consumer(...)/.producer(...) with ready-made clients

client.poll(record -> Outcome.success());   // keys and values are byte[]; deserialization is yours
client.sessionEnd().toCompletableFuture().join();
client.close();
```

## Running it

There is no demo and no published artifact. It runs from this checkout, as a library on the
classpath of a test or an application you build here.

## Testing it

```bash
./mvnw test -pl :parallel-consumer-proxy-client-pc-java-direct -am
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=pc-java-direct
```

The first is this module's own suite - `DirectSpikeConformanceTest`, the shared spike suite from the
api module's test-jar, over core's mock Kafka clients, with no broker and no Docker. The second is
the shared cross-language suite, which drives this transport as an in-JVM **binding** rather than a
spawned runner (there is no process boundary to cross when the wire is a function call) and always
runs the `core` control arm beside it.

`-am` also builds and tests the modules above them in the reactor.

## Depth

[`client-authoring-guide.md`](../../../parallel-consumer-proxy/docs/client-authoring-guide.md),
[`protocol-specification.md`](../../../parallel-consumer-proxy/docs/protocol-specification.md), and
the conformance module's
[README](../../parallel-consumer-proxy-conformance/README.md) for why a JVM client is a binding.
