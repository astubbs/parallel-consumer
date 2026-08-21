<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - Java harness lane

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

## What it is

**Not a client, and not a product module at all** - a build-graph module, and the only place on the
JVM side that depends on the engine. It has no main sources and publishes nothing.

It exists because a test-scope dependency is not transitive on the classpath but *is* an edge in the
Maven reactor. While `pc-java-grpc` test-depended on `parallel-consumer-proxy`, `-pl <anything wrapping
it> -am` built the engine, and `bin/build.sh` opens with `clean` - so the routine build of a wrapper
deleted the sidecar jar every other language's conformance test spawns. Confining that edge to a
leaf nothing depends on is this module's whole job.

Two things live here, and they are the same thing from two sides:

- **Its compile-scope dependencies are the harness classpath** - the engine, the proxy test-jar that
  holds `ProxyHarness` and `TestModeMain`, and core's test-jar. A JVM client that needs to spawn the
  sidecar declares this one module at test scope in its own opt-in profile instead of restating
  three dependencies and drifting from them.
- **Its test tree runs the gRPC transport's harness-backed conformance suite** - the shared spike
  suite from the api module's test-jar, driven against a real gRPC server over mock Kafka clients.
  Its evidence is recorded against `pc-java-grpc`, where a reader looking for that transport's evidence
  will look.

## What it can do today

Nothing an application would call. What it *proves* is `pc-java-grpc`'s: dispatch, the user function,
per-record outcomes, the produce payload, FIFO hand-out through one executor, the asynchronous
processor answering off-thread, the session-end stage, and the records-out-for-processing leak
check. Everything un-negotiated in the fan-out is un-negotiated here too - leases, heartbeats,
reconnect, worker death, terminal outcomes and the drain.

## Running it

There is nothing to run. Before adding any dependency here or to a client module, run

```bash
./mvnw -pl :parallel-consumer-proxy-client-kotlin -am validate
```

which must **not** print `parallel-consumer-proxy`. That reactor list, not an assertion, is the
measurement that says the hazard above is still gone.

## Testing it

```bash
./mvnw test -pl :parallel-consumer-proxy-client-java-harness -am
```

No Docker and no broker. It runs in the ordinary lane rather than behind `-Dpc.foreignClients`,
because this is the Java client's primary evidence and a test that does not run is not a passing
test.

## Depth

[`client-authoring-guide.md`](../../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../../parallel-consumer-proxy/docs/protocol-specification.md); this
module's `pom.xml` carries the full reasoning for its existence.
