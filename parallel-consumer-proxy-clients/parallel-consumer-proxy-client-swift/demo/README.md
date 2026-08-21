# The Swift demo

```bash
# from anywhere in the repo
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-swift/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker and nothing else - no Swift, no JDK, no Kafka. The demo runs in a container, the
broker is a compose sibling, and the sidecar is spawned as a child process inside the demo's own
container.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first: the flags, the defaults, the environment variables, the two tables and the
effective-configuration fingerprint are all defined there. This file records only what is specific
to Swift.

## The two arms

| arm | what it is |
|---|---|
| `AK core` | [`swift-kafka-client`](https://github.com/swift-server/swift-kafka-client), one record at a time. No engine, no client library, no sidecar |
| `swift-grpc` | this module's client library: it spawns the sidecar, receives records over a socket, runs the same function on them, and reports outcomes back |

On the `swift-grpc` path the application does **no Kafka I/O at all** - the sidecar owns the
consumer, the producer, the group membership and the offsets. That the same process also seeds the
topic and runs the AK core arm with an ordinary Kafka client is a statement about the *path*, not
about the process: a comparison needs both sides.

**One thing to know before reading the AK core row.** `swift-kafka-client` feeds its message
`AsyncSequence` from a poll loop whose `pollInterval` defaults to 100 ms, and at small volumes that
loop, not the simulated work, is what the arm's wall clock measures - 30 records at 2 ms took 1.8 s
in the run that proved this demo works. The demo does not change the default, because the arm's
claim is "what this language's users already run". Whether it amortises at the contract's own
volumes has not been measured.

Java's demo carries four more arms (`pc-core`, `java-direct`, `java-grpc-uds`, `java-raw-grpc`)
because one JVM can hold every engine at once and each pair changes exactly one term. Swift's only
Kafka client is its own, so there is nothing to compare a wrapper or a raw wire against - **two arms
is the whole contract here**, as it is everywhere but Java.

## There is no native mode, and that is not a gap

Swift.org publishes Linux toolchains for Ubuntu, Amazon Linux and RHEL only, and this project's
development box is Debian 13, so **there is no `swift` on a developer machine here and there will
not be** ([`docs/inflight/parked-containerised-toolchains-and-runtime.md`](../../../docs/inflight/parked-containerised-toolchains-and-runtime.md)).
The client library beside this demo is built in a container for the same reason. `run.sh --native`
therefore says so and exits rather than pretending.

Two consequences follow from that, and both are visible in the demo's behaviour:

- **The demo never starts its own broker.** The Java seed uses Testcontainers when no `--bootstrap`
  is supplied; a demo container is never granted the host Docker socket (plan unit U35), so this one
  is *given* an address by `docker-compose.yml` instead. Run the binary with no broker and it says
  exactly that.
- **The image is the largest of the eleven.** It carries the Swift runtime (its own base), a JRE and
  the proxy's jars (the sidecar is a JVM program), and librdkafka compiled from source. If image
  size ever matters, the first lever is a slim runtime base plus a statically linked binary - the
  client module's `Dockerfile` already does the static half for its extracted artifacts.

## The three divergences, and why each one exists

### 1. The simulated work is `Task.sleep`, not a blocking sleep

The contract says a blocking sleep is fine in Swift. **It is not fine on the sidecar arm here**, and
the reason is this client library's own design rather than anything about the demo.

`ParallelConsumerClient.poll` starts `executorCount` **Swift concurrency tasks**, which share the
cooperative thread pool - a pool whose width is the machine's core count. A blocking sleep inside
the user function occupies one of those threads for its whole duration, so an arm asking for 100
in-flight records would only ever have as many running as the machine has cores, and the table would
report a ceiling of the *pool* while appearing to report the engine. This module has already been
bitten by the same mechanism once, in the conformance runner's ceiling barrier, which is an actor
with a continuation per waiter precisely because "a waiter that blocked its thread would take one of
those threads out of the pool for the whole hold"
([`docs/inflight/clients/swift.md`](../../../docs/inflight/clients/swift.md)).

`Task.sleep` is Swift's non-occupying wait, so it is what both arms use - both, because the arms must
differ by transport and nothing else. The AK core arm is serial and would be unaffected either way.

**This has not been measured.** The claim above is about the mechanism, not about an observed gap;
nobody has run a blocking-sleep control arm here.

### 2. `--partitions` reaches the broker, not an admin client

`swift-kafka-client` has **no admin client** - no `CreateTopics`, no `DescribeTopics`, no metadata
API at all on its public surface. The Java seed creates its topic with an `AdminClient` and *asserts*
the partition count it got, refusing to run if an existing topic disagrees with `--partitions`.

Swift cannot do either. The demo's topic is created by its first produce, by the broker, with the
broker's own `num.partitions` - which `docker-compose.yml` sets from `PC_DEMO_PARTITIONS`, and which
`run.sh` forwards `--partitions N` into. So the flag works on the supported path and the number in
the fingerprint is the number the topic got.

**What it cannot do is verify.** Two consequences worth knowing:

- With `--bootstrap` pointing at a cluster of your own, `--partitions` has no effect: that broker's
  `num.partitions` decides, and auto-creation may be off entirely. Pass `--topic` naming a topic you
  created yourself.
- The fingerprint's `partitions` line is what was *asked for*. On the compose path it is also what
  was got, because the same value configured the broker.

### 3. No `Package.resolved` is committed for the demo

The client module commits one, so its image resolves what the checkout pins. The demo's cannot be
generated without a toolchain, and there is none here - so the image resolves the graph the exact
version pins in `Package.swift` admit, and copies the resulting `Package.resolved` to
`/app/Package.resolved` inside the image so a reader can see what was actually built.

## How the image is put together

`demo/Dockerfile` has four jobs in one build, and its header comment explains each:

1. **the sidecar** - a Maven stage builds `parallel-consumer-proxy` and copies its runtime jars to
   `/opt/parallel-consumer/lib`. `demo/sidecar.sh` becomes `/opt/parallel-consumer/sidecar`, the
   absolute path the client library is given;
2. **codegen** - the two protoc plugins are compiled from source (neither project ships a Linux
   binary) and the frozen `proxy.proto` becomes the client's protocol target. **These stages are
   byte-identical to the client module's Dockerfile so that BuildKit's cache is shared** - a
   Dockerfile cannot inherit a stage from another Dockerfile, and `docker compose up` cannot build
   one image before another;
3. **the demo binary** - `swift build` over `demo/`, a separate SwiftPM package with a path
   dependency on the client. It has its own scratch-path cache mount, for the reason the module's
   Dockerfile gives at length: SwiftPM does not partition its build database by configuration, so
   two builds sharing a path invalidate each other's whole dependency graph;
4. **the runtime** - the Swift base again, plus a JRE, plus the jars and the binary.

The build context is the **repository**, because the sidecar stage builds a Maven reactor module
that cannot be built without its parents and siblings. The repository-root `.dockerignore` is what
keeps that from uploading every `target/` directory in the tree.

## What is not here

- **There is no Maven module.** The client module is `packaging: pom` and drives its container
  through `bin/build-client.sh`; the demo is not in the reactor at all, and `docker compose` is its
  only entry point.
- **The demo is not in `bin/ci-demo-test.sh`.** That script runs the *Java* demo through both of its
  entry points on every pull request, which is part of the contract a per-language demo inherits.
  Wiring this one in is owed work, recorded in
  [`docs/inflight/clients/swift.md`](../../../docs/inflight/clients/swift.md).
- **No latency figures, in either table.** The backlog is pre-produced, so the workload is
  closed-loop and a per-record timing would be flattered by however far an arm had fallen behind.
  Throughput is the only honest number this shape can produce, and that is the contract's rule
  rather than this demo's choice.
