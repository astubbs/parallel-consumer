<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer proxy client - Swift

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

## What it is

Key-ordered concurrent Kafka processing from Swift, with the Parallel Consumer engine running as a
sidecar process this library spawns and owns. It speaks the frozen v1 protocol over one gRPC stream
and reads none of the proxy's Java.

**It builds in a container, and that is not an oversight**: Swift.org publishes Linux toolchains for
Ubuntu, Amazon Linux and RHEL only, and this project's box is Debian 13 - so Swift is one of the two
languages (with C++) that mise cannot serve. The base is the official `swift:6.1` image.

**Swift 6 language mode is this module's static analysis.** `swift format lint` is a formatter and
finds no defects; the compiler's strict concurrency checking is where the answer lives, so every
isolation boundary here is checked rather than asserted and a data race is a compile error.

## What it can do today

Connect, `Configure`, dispatch waves, the user's function, per-record reports with the delivery token
echoed verbatim, records produced back on success, and a clean client-initiated shutdown - all end to
end over real gRPC, and all five shared conformance scenarios.

Not implemented, and therefore **not declared**: the liveness lease and heartbeats, the manifest
reconnect, worker-death reporting, terminal outcomes and the proxy-initiated drain.
`Configure.capabilities` carries exactly `["dispatch"]`, because an empty list would mean "the whole
v1 baseline" on the wire and invite duties this client does not perform. They are un-negotiated
capabilities, not half-built features.

```swift
import ParallelConsumerProxyClient

var options = ClientOptions(sidecarPath: "/opt/parallel-consumer/proxy")  // ABSOLUTE, never PATH
options.topics = ["orders"]
options.kafkaProperties = ["bootstrap.servers": "localhost:9092"]

let client = try await ParallelConsumerClient.connect(options: options)

try client.poll { record in
    // PLACE SERDE SETUP IN YOUR LANGUAGE HERE - key and value are bytes
    try await handle(record.key, record.value)
    return .success
}

try await client.sessionEnd()   // or get on with other work
try await client.shutdown()
```

- **`poll` returns immediately**; `sessionEnd()` is how the caller learns the session ended and why -
  it throws the cause when the session died rather than ended. It is a method rather than `poll`'s
  return value, because a client that only connected still has an end to observe.
- **A thrown error is a failure outcome**, translated in one place, so one bad record cannot tear the
  stream down. Assert on `client.session`, never on the options: what was asked for and what is
  running are different things.
- **Logging is `swift-log`**, the ecosystem's facade, with no handler configured by default - unlike
  the C++ and TypeScript clients, whose injectable closure is the right answer only where the
  ecosystem has no facade. No record key, value or Kafka property appears in any log line or error at
  any level.

## Running it

```bash
bin/build-client.sh swift          # build in the container, extract the artifacts
bin/build-client.sh swift --test   # ... and run the extracted artifacts on this host
```

Through Maven it is
`./mvnw -Dpc.foreignClients -pl :parallel-consumer-proxy-client-swift -am package`; **without
`-Dpc.foreignClients` no container starts at all**, so an ordinary scoped build of this module is
inert. A missing Docker daemon exits `2` - "cannot run", never a pass.

Extracted into `target/container/`:

| Artifact | What it is |
|---|---|
| `pc-swift-selftest` | Statically linked; run on the host, it proves the extracted build is portable |
| `pc-swift-selftest-dynamic` | The **control** - the same source dynamically linked, expected to FAIL on a host with no Swift runtime. A run where both work proves nothing |
| `pc-swift-conformance-runner` | This language's half of the shared cross-language conformance suite |
| `link-report.txt` | `ldd` of each, and the toolchain versions - read it before believing either portability claim |

There is no published package. There **is** a demo - see below.

### In the Maven build

This module is `packaging: pom`, and its four `pc.foreign.*` properties name `bin/build-client.sh
swift` and the same script with the test flag. The `foreign-clients` profile in the clients
aggregator ([`../pom.xml`](../pom.xml)) binds them to `compile` and `test`, and decides whether the
module is in the reactor at all:

```bash
./mvnw compile -P foreign-clients -pl :parallel-consumer-proxy-client-swift -am   # build in the image, extract
./mvnw test    -P foreign-clients -pl :parallel-consumer-proxy-client-swift -am   # ...and run the artifacts on the host
```

`-P foreign-clients` and `-Dpc.foreignClients` are interchangeable here, unlike the clients that
spawn a JVM sidecar: this module declares no harness profile, because it needs nothing from the
engine's reactor. `-am` is not optional for `compile` or `test` - `-pl` alone fails the enforcer's
`ReactorModuleConvergence` with a message about parent modules that reads as a broken pom
([`docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md`](../../docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md)
owns that). `./mvnw clean -P foreign-clients -pl :parallel-consumer-proxy-client-swift` still needs
the profile - without it the module is not in the reactor at all - but needs no `-am`, the clean
lifecycle never reaching `validate` where the enforcer is bound.

**What a Java engineer will find surprising:**

- **`compile` never runs a container**, though it builds one. BuildKit builds the image and exports
  its final artifact stage straight to `target/container` with `--output type=local`; nothing is
  `docker run`, nothing is `docker cp`, no volume permissions are involved.
- **`swift test` and `swift format lint --strict` happen inside `compile`**, as layers of that image
  build - so a failing unit test or a lint finding fails the *compile* phase. Maven's `test` phase
  is the different assertion: it runs the extracted binaries **on this host**, and the dynamically
  linked control is expected to fail there. A run where both work proves nothing and the script
  says so.
- **`clean` needs no configuration and gets none.** The container route writes exactly one host-side
  path, `target/container`, which is inside `${project.build.directory}` - so Maven's default clean
  already removes the binaries, `Package.resolved` and `link-report.txt`. The pom records that
  measurement so the empty configuration does not read as an oversight.
- **`clean` is close to free, and that is the caching working.** Everything expensive lives outside
  this directory: the `swift:6.1` base image (3.4GB), the BuildKit layer cache, and SwiftPM's three
  **named cache mounts** (`pc-swift-scratch-static`, `-dynamic`, `-test` - grep them in the
  `Dockerfile`), one per build configuration because two sharing a path invalidate each other's
  dependency graph. A rebuild after a clean re-exports from that cache in seconds.
- **Never prune the BuildKit cache to tidy up after this module.** It is shared with every worktree
  and agent on the machine - 17GB of it here, nearly all reclaimable, which is exactly what makes
  `docker builder prune` look tempting and cost a full rebuild of SwiftNIO and grpc-swift. It is
  this language's `~/.m2`, and `mvn clean` does not empty `~/.m2`. There is deliberately no `.build`
  directory in this checkout for the same reason: the scratch paths are cache mounts inside the
  image.
- **The extracted artifacts carry the timestamps of the layer that made them**, not of your build,
  so `ls -l` is no evidence that a compile actually ran. Read `link-report.txt` for what was built
  and with which toolchain.

## The demo

```bash
demo/run.sh                       # from anywhere in the repo
cd demo && docker compose up      # or the plain container path
```

Two arms - Swift's own Kafka client one record at a time, and Swift over the sidecar through the
client library above - over one pre-produced backlog, reporting throughput. It needs Docker and
nothing else: the broker is a compose sibling and the sidecar is a child process, never a service.

[`demo/README.md`](demo/README.md) records what is specific to Swift, including the three
divergences from the shared contract ([`parallel-consumer-proxy/demo/README.md`](../../parallel-consumer-proxy/demo/README.md)):
the simulated work is `Task.sleep` rather than a blocking sleep, `--partitions` reaches the broker
rather than an admin client swift-kafka-client does not have, and there is no native mode.

**The demo is not part of the Maven build.** It is its own SwiftPM package with a path dependency on
this one, built by its own `Dockerfile`, so nothing in the reactor builds or runs it.

## Testing it

Two layers, both load-bearing:

- **`Tests/`**, run by `swift test` inside the image: the in-flight ceiling counted as *unresolved*
  records (including the authoring guide's own worked example), FIFO hand-out, the overflow protocol
  violation, discard-at-shutdown, credential and payload redaction, the declared capability set, and
  the port-line scan. The image also runs `swift format lint --strict`.
- **The shared conformance suite**, which drives `pc-swift-conformance-runner` through the same
  scenarios as every other language and asserts engine state this process cannot see:

```bash
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=swift
```

That entry needs Docker, and it says so by failing rather than skipping.

## Depth

Pins, traps and what this wave settled are in
[`docs/inflight/clients/swift.md`](../../docs/inflight/clients/swift.md). The protocol itself is
[`client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md).
