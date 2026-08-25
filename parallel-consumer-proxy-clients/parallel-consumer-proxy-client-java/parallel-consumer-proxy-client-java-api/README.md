<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - the shared Java client surface

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

## What it is

**Not a client - the surface every client wears.** It holds the interfaces and value types both
Java transports implement (`java-direct` in-process, `java-grpc` over the sidecar) and the nine
other languages mirror in their own idiom: `ParallelConsumerClient`, `ClientOptions`,
`RecordProcessor`/`AsyncRecordProcessor`, `InboundRecord`, `OutboundRecord`, `Outcome`/`Outcomes`,
`ProcessingOrder`.

It is **dependency-free on purpose**: no engine, no protocol module, no protobuf, no gRPC. A
transport type reaching this surface would stop the shape being expressible in a language that has
no such type, so `ClientSurfaceArchTest` fails the build on the bytecode and the `java-direct`
sibling's `bannedDependencies` fails it on the classpath.

## What it can do today

Define the contract, and nothing else - it has no runtime of its own. To actually consume records,
pick a transport: [`java-direct`](../parallel-consumer-proxy-client-java-direct/README.md) or
[`java-grpc`](../parallel-consumer-proxy-client-java-grpc/README.md).

The contract it defines today: connect-time configuration, `poll`/`pollAsync` with a user function,
per-record success or failure outcomes, records produced back on success, `sessionEnd()` for
learning that the session ended and why, and `close()` for a clean shutdown. What it deliberately
says nothing about - because no client implements it and none declares the capability - is the
liveness lease and heartbeats, the manifest reconnect, worker-death reporting, terminal outcomes
and the proxy-initiated shutdown drain.

Its test tree also carries `SpikeConformanceTest`, the transport-parameterised suite both transports
subclass, shipped in this module's `tests` classifier.

## Running it

There is nothing to run. It is a jar of interfaces; the transports are the runnable things.

## Testing it

```bash
./mvnw test -pl :parallel-consumer-proxy-client-java-api -am
```

That is the surface's own tests - the ArchUnit rules on the surface and `Outcomes.asAsync`. The
shared cross-language conformance suite drives the *transports*, under their own names
(`-Dpc.conformance.language=java-direct,java-grpc`), never this module.

`-am` also builds and tests the modules above it in the reactor.

## Depth

[`client-authoring-guide.md`](../../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../../parallel-consumer-proxy/docs/protocol-specification.md).
