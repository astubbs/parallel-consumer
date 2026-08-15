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
end over real gRPC, and all four shared conformance scenarios.

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

There is no published package and no demo.

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
