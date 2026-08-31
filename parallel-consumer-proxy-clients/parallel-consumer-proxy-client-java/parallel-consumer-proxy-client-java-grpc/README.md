<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - Java gRPC transport

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs/parallel-consumer#242.

## What it is

The [shared client surface](../parallel-consumer-proxy-client-java-api/README.md) implemented over
the v1 proxy protocol: one bidirectional gRPC stream to a sidecar running the engine. It is the JVM
reference implementation of the wire, and the Kotlin and Scala clients are thin shapes over it
rather than second and third session implementations.

**It owns no spawn, deliberately.** `GrpcParallelConsumerClient` connects to a port it is given;
starting and reaping the sidecar belongs to the lifecycle unit, not to the transport. That is why
this module has no `sidecarPath` option, and why the shared conformance suite drives it as an
in-JVM binding rather than as a child process - a subprocess runner would have had to write the
spawn the library does not have and then test that.

It depends on the sidecar in **no scope**: anything needing a running one lives in the sibling
[`java-harness`](../parallel-consumer-proxy-client-java-harness/README.md) module, so a client
wrapping this one does not drag `parallel-consumer-proxy` into its reactor.

**What it can be held to on this branch.** The sidecar that exists today hosts no engine and refuses
every session, so the furthest a real end-to-end run reaches is the handshake - which
`java-harness` asserts, and which is worth asserting on its own: it is the one claim a client library
makes that no in-JVM fake can make for it. Everything past the handshake is exercised against a
scripted proxy here.

## What it can do today

Connect and negotiate (`connect()` returns the `NegotiatedSession` before polling starts), receive
dispatch waves, hand records to executors, run the user function, report per-record outcomes with
the delivery token echoed verbatim, produce records back on success, observe the session's end
through `sessionEnd()` - including a mid-session stream error, which ends the session with its
cause and releases the executors - and shut down cleanly by half-closing the stream.

It declares exactly the `dispatch` capability, so the proxy sends nothing else. Not implemented, and
therefore not declared: the liveness lease and heartbeats, the manifest reconnect and `Drop`,
worker-death reporting, terminal outcomes, and the proxy-initiated shutdown drain.

The one protocol violation it raises at you is `ProxyProtocolViolation`: a dispatch past the
in-flight ceiling the proxy declared itself. The call is cancelled and the session fails with it -
no record is dropped and the queue never grows to absorb it.

```java
var client = GrpcParallelConsumerClient.builder()
        .port(port)                       // the sidecar's port - spawning is the caller's job
        .options(ClientOptions.builder()
                .topics(List.of("orders"))
                .kafkaProperties(Map.of("bootstrap.servers", "localhost:9092", "group.id", "orders"))
                .build())
        .build();

client.connect().toCompletableFuture().join();
client.poll(record -> Outcome.success());   // keys and values are byte[]; deserialization is yours
client.sessionEnd().toCompletableFuture().join();
client.close();
```

## Running it

There is no demo and no published artifact. It runs from this checkout, against a sidecar something
else started - `java-harness` starts one through the sidecar's own entry point.

## Testing it

```bash
./mvnw test -pl :parallel-consumer-proxy-client-java-grpc -am
./mvnw test -pl :parallel-consumer-proxy-client-java-harness -am
```

The first is the tests that need no sidecar - `WireMappingTest`, and `SessionEndTest`, which pins
that a mid-session stream error ends the session with its cause and that closing mid-processing does
not invent a verdict. The second is the handshake against a real sidecar. Neither needs Docker or a
broker.

`-am` also builds and tests the modules above them in the reactor.

## Depth

[`client-authoring-guide.md`](../../../parallel-consumer-proxy-protocol/docs/client-authoring-guide.md)
and
[`protocol-specification.md`](../../../parallel-consumer-proxy-protocol/docs/protocol-specification.md).
