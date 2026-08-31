<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - the Java proxy client

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs/parallel-consumer#242.

## What this is

An aggregator, not a client: **one shared surface, two transports behind it, and a lane that holds
the sidecar dependency for both.** The interesting property is that the surface is transport-free
by construction - two gates from opposite directions say so, one reading the bytecode and one
reading the dependency tree - so a language mirroring it is mirroring a shape it can express.

| Module | What it is |
|---|---|
| [`java-api`](parallel-consumer-proxy-client-java-api/README.md) | The user-facing surface both transports implement and the other languages mirror. Dependency-free: no engine, no protocol module, no protobuf, no gRPC |
| [`java-direct`](parallel-consumer-proxy-client-java-direct/README.md) | `parallel-consumer-core` bound in-process. **Never speaks the protocol** - so it is the control arm for the shared API |
| [`java-grpc`](parallel-consumer-proxy-client-java-grpc/README.md) | The surface over the frozen v1 wire to a sidecar. The JVM reference implementation, which Kotlin and Scala will wrap |
| [`java-harness`](parallel-consumer-proxy-client-java-harness/README.md) | **Not a product module**: the one place on the JVM side that depends on the sidecar, so a client wrapping a transport does not drag it into the reactor |

## What it can do today, and what it cannot

**`java-direct` carries a record end to end**: connect and configure, receive records, run the
user's function (synchronously or through a `CompletionStage`), report per-record outcomes, produce
records back on success, observe the session's end through `sessionEnd()`, and close cleanly. It
does that against `parallel-consumer-core`, which is an ordinary dependency - the sidecar's dispatch
engine is a different thing and is not on this branch.

**`java-grpc` speaks the wire and completes the handshake.** Against the sidecar that exists today
it gets as far as being told, in the protocol's own words, that the sidecar hosts no engine -
because [that is what the sidecar does](../../parallel-consumer-proxy/README.md). Its dispatch,
report and shutdown paths are implemented and are exercised against a scripted proxy in
`SessionEndTest`; what is missing is a real engine to point them at, which arrives with a later rung.

Neither transport implements leases and heartbeats, the manifest reconnect, worker-death reporting,
terminal outcomes or the shutdown drain - **un-negotiated capabilities rather than half-built
features**; `java-grpc` declares `["dispatch"]` and nothing else, and those concepts have no meaning
at all one layer below the wire where `java-direct` lives.

## Building and testing

```bash
./mvnw test -pl :parallel-consumer-proxy-client-java-api -am
./mvnw test -pl :parallel-consumer-proxy-client-java-direct -am
./mvnw test -pl :parallel-consumer-proxy-client-java-grpc -am
./mvnw test -pl :parallel-consumer-proxy-client-java-harness -am
```

No Docker, no broker, and no `-Dpc.foreignClients` - these are JVM modules and the ordinary lane
runs them, because this is the Java client's primary evidence and a test that does not run is not a
passing test. `-am` also builds and tests the modules above them in the reactor; **naming the
aggregator instead of the leaves builds nothing**, because Maven does not walk into a
packaging-`pom` project's modules for a `-pl` selection.

**SpotBugs gates this whole tree** at `process-test-classes`, bound in this pom so every module
beneath inherits it - `check` rather than `spotbugs`, so a finding fails the build instead of
writing a report nobody reads. Dismiss one in the module's own `spotbugs-exclude.xml`, named class
by class with the reason beside it, never module-wide.
[`docs/client-static-analysis.md`](../../docs/client-static-analysis.md) owns that policy.

## Depth

[`client-authoring-guide.md`](../../parallel-consumer-proxy-protocol/docs/client-authoring-guide.md)
says what an implementation must do, and
[`protocol-specification.md`](../../parallel-consumer-proxy-protocol/docs/protocol-specification.md)
says what the messages mean.
