<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - Java harness lane

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs/parallel-consumer#242.

## What it is

**Not a client, and not a product module at all** - a build-graph module, and the only place on the
JVM side that depends on the sidecar. It has no main sources and publishes nothing.

It exists because a test-scope dependency is not transitive on the classpath but *is* an edge in the
Maven reactor. If [`java-grpc`](../parallel-consumer-proxy-client-java-grpc/README.md) test-depended
on `parallel-consumer-proxy`, `-pl <anything wrapping it> -am` would build the sidecar, and
`bin/build.sh` opens with `clean` - so the routine build of a wrapper would delete the sidecar jar
other languages' tests spawn. Confining that edge to a leaf nothing depends on is this module's
whole job.

## What it can do today

One thing, and it is the one thing no in-JVM fake can do for the Java client: **drive the real gRPC
transport against a real sidecar, started through the sidecar's own entry point.** Because that
sidecar hosts no engine, the run ends where the sidecar's honesty ends - the client is told
`UNIMPLEMENTED`, in the protocol's own words, and reports that to its caller both from `connect()`
and from `sessionEnd()`.

That is a smaller claim than the module's name promises, and deliberately so. The **status code** is
the assertion rather than "it failed": the authority allowlist answers `PERMISSION_DENIED` and the
admission slot `RESOURCE_EXHAUSTED`, both before the service method runs, so only `UNIMPLEMENTED`
proves the handshake was actually delivered to the service. A second test points the same client at
a port nothing is listening on and asserts the failure is *not* that one, so the first cannot be
passing on any error at all.

The engine-backed conformance run, and the compile-scope harness classpath a JVM client declares to
spawn a sidecar as a child process, arrive with the conformance rung stacked on this one.

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
because this is the Java client's only evidence against a real server and a test that does not run
is not a passing test.

## Depth

[`client-authoring-guide.md`](../../../parallel-consumer-proxy-protocol/docs/client-authoring-guide.md)
and
[`protocol-specification.md`](../../../parallel-consumer-proxy-protocol/docs/protocol-specification.md);
this module's `pom.xml` carries the full reasoning for its existence.
