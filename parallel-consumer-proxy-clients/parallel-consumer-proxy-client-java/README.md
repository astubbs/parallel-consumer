<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - the Java proxy client

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

## What this is

An aggregator, not a client: **one shared surface, two transports behind it, and a lane that holds
the engine dependency for all of them.** The interesting property is that the same conformance suite
drives both transports with no transport-specific branch, so a behavioural difference between the
two runs is a transport's bug rather than the suite's ambiguity.

| Module | What it is |
|---|---|
| [`java-api`](parallel-consumer-proxy-client-java-api/README.md) | The user-facing surface both transports implement and the nine other languages mirror. Dependency-free: no engine, no protocol module, no protobuf, no gRPC |
| [`pc-java-direct`](parallel-consumer-proxy-client-pc-java-direct/README.md) | The engine bound in-process. **Never speaks the protocol** - so it is the control arm for the shared API |
| [`pc-java-grpc`](parallel-consumer-proxy-client-pc-java-grpc/README.md) | The surface over the v1 wire to a sidecar. The JVM reference implementation, which Kotlin and Scala wrap |
| [`java-harness`](parallel-consumer-proxy-client-java-harness/README.md) | **Not a product module**: the one place on the JVM side that depends on the engine, so a client wrapping a transport does not drag it into the reactor |

## What it can do today, and what it cannot

Both transports carry the same slice: connect and configure, receive records, run the user's
function (synchronously or through a `CompletionStage`), report per-record outcomes, produce records
back on success, observe the session's end through `sessionEnd()`, and close cleanly.

Neither implements leases and heartbeats, the manifest reconnect, worker-death reporting, terminal
outcomes or the shutdown drain - **un-negotiated capabilities rather than half-built features**;
`pc-java-grpc` declares `["dispatch"]` and nothing else, and those concepts have no meaning at all one
layer below the wire where `pc-java-direct` lives.

## Building and testing

```bash
./mvnw test -pl :parallel-consumer-proxy-client-java-api -am
./mvnw test -pl :parallel-consumer-proxy-client-pc-java-direct -am
./mvnw test -pl :parallel-consumer-proxy-client-pc-java-grpc -am
./mvnw test -pl :parallel-consumer-proxy-client-java-harness -am
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=pc-java-direct,pc-java-grpc
```

No Docker, no broker, and no `-Dpc.foreignClients` - these are JVM modules and the ordinary lane
runs them, because this is the Java client's primary evidence and a test that does not run is not a
passing test. `-am` also builds and tests the modules above them in the reactor.

**SpotBugs gates this whole tree** at `process-classes`, bound in this pom so every module beneath
inherits it - `check` rather than `spotbugs`, so a finding fails the build instead of writing a
report nobody reads. Dismiss one in the module's own `spotbugs-exclude.xml`, named class by class
with the reason beside it, never module-wide.

## Depth

[`client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md); what
this wave settled is in [`docs/inflight/clients/java.md`](../../docs/inflight/clients/java.md).
