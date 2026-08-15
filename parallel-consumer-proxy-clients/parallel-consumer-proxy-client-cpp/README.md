<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer proxy client - C++

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

Key-ordered concurrent Kafka processing from C++, with the Parallel Consumer engine running as a
sidecar process. Upstream: confluentinc#154.

**Wave one.** Connect, `Configure`, dispatch waves, the user's function, per-record reports, records
produced back on success, and a clean client-initiated shutdown all work end to end over real gRPC.
The liveness lease, heartbeats, the manifest reconnect, worker-death reporting, terminal outcomes and
the proxy-initiated drain are **not implemented and therefore not declared** - un-negotiated
capabilities rather than half-built features; see [Capabilities](#what-this-client-declares).

## Building it

There is no host build, and that is not an oversight: gRPC and protobuf arrive as **system dev
packages** rather than as a versioned toolchain, which makes C++ one of the two languages this
project's mise-based toolchain management cannot serve.

```bash
bin/build-client.sh cpp          # build in the container, extract the artifacts
bin/build-client.sh cpp --test   # ... and run the extracted artifacts on this host
```

Through Maven it is `./mvnw -Dpc.foreignClients -pl :parallel-consumer-proxy-client-cpp -am package`;
**without `-Dpc.foreignClients` no container starts at all**, so an ordinary scoped build of this
module is inert. The image also runs this module's own tests (`ctest`) and its static analysis
(`scripts/analyse.sh`), so a red one fails the build rather than waiting for CI.

Extracted into `target/container/`:

| Artifact | What it is |
|---|---|
| `pc-cpp-selftest` | Statically linked; run on the host, it proves the extracted build is portable |
| `pc-cpp-selftest-dynamic` | The **control** - the same source dynamically linked, expected to FAIL on a host with no `libgrpc++.so`. A run where both work proves nothing |
| `pc-cpp-conformance-runner` | This language's half of the shared cross-language conformance suite |
| `link-report.txt` | `ldd` of each, and the toolchain versions - read it before believing either portability claim |

## Using it

```cpp
#include "parallel_consumer_proxy_client.h"

namespace pcp = parallelconsumer::proxy;

pcp::ClientOptions options;
options.sidecar_path = "/opt/parallel-consumer/proxy";   // ABSOLUTE, never resolved through PATH
options.topics = {"orders"};
options.kafka_properties = {{"bootstrap.servers", "localhost:9092"}};

auto client = pcp::Client::connect(std::move(options));

client->poll([](const pcp::InboundRecord& record) {
    // PLACE SERDE SETUP IN YOUR LANGUAGE HERE - key and value are bytes
    std::cout << record.describe() << '\n';
    return pcp::Outcome::success();
});

client->session_end().wait();   // or carry on doing other work
client->shutdown();
```

### Does `poll` block? No - and here is how you observe the end

`poll(processor)` **returns as soon as processing is running**, and the session's end is observed
through **`session_end()`, a `std::shared_future<void>`**. That is the C++ spelling of the JVM
reference's `CompletionStage<Void> sessionEnd()` and of the .NET client's `Task`: wait on it, poll it
with `wait_for`, or ignore it. It becomes ready when the session ends for any reason - the proxy
completing the stream, a mid-session stream error, a cancelled call, the sidecar exiting, or this
client shutting down - and `get()` **rethrows the cause** when it ended in a fault.

It is an accessor rather than `poll`'s return value because a session can die before or without a
poll: a client that only connected still has an end to observe.

Errors are **exceptions**, all deriving from `pcp::ClientError`: `OptionsError`, `SidecarError`,
`TransportError`, `ProtocolError`, `TimeoutError`. `shutdown()` throws the session's first fault if
it had one, including a fault recorded while the application was doing something else.

### The user's function

`RecordProcessor` is `std::function<Outcome(const InboundRecord&)>`. Return
`Outcome::success()`, `Outcome::success(produce)` or `Outcome::failure(reason)` - **or throw**: a
thrown exception is translated into a failure outcome carrying `what()`, in exactly one place, so a
worker that falls over produces a failure report rather than tearing down the session.

Keys and values are **bytes** (`std::optional<std::string>`, where absent is a null key or a
tombstone and is not the same as empty). Deserialization is your code.

### The protocol violation this client can raise

`ProtocolError` naming the counts - the unresolved count, the negotiated `max_concurrency`, and the
overflowing record's token - when the proxy dispatches past the in-flight ceiling **it declared
itself**. That is a protocol violation and not a load condition, so no record is ever dropped to make
room and the queue never grows unbounded. The call is **cancelled** rather than answered with a
status, because only the server side of a gRPC call sets one.

## What this client declares

`["dispatch"]`, explicitly. **Declaring nothing would be worse than declaring a subset**: an empty
list means "the whole v1 baseline" on the wire, which would entitle the proxy to send heartbeat,
manifest, worker-death and shutdown traffic this client does not answer - and un-answered heartbeats
arm a lease-expiry redelivery loop. The wave that implements a duty adds its token in
`implemented_capabilities()`, beside the code.

Assert on `client->session()`, never on the options: what was asked for and what is running are
different things.

## Logging

**This library says nothing until you ask it to.** C++ has no logging facade, so - as with the
TypeScript client, where the ecosystem's absence of one is the answer rather than a gap - the sink is
injected: set `ClientOptions::logger` to any
`std::function<void(pcp::LogLevel, const std::string&)>`. Left empty, the library emits nothing at
all, at any level.

A healthy session is about four INFO lines. **Record keys and values appear in no log line at any
level**, and neither does any Kafka property: `ClientOptions::describe()`, `InboundRecord::describe()`
and `OutboundRecord::describe()` are hand-written so that they *cannot* print payload or credentials,
which is safe by construction rather than by call-site discipline.

The **sidecar's** stderr is inherited by this process by default, so a misconfigured broker explains
itself instead of becoming an unexplained hang; its stdout is drained for the child's whole life (a
pipe nobody reads fills up and stops the writer mid-log-line), and the last 40 lines are kept for the
error raised when it dies or never announces a port.

## Tests

Two layers, both load-bearing:

- **`tests/`**, run by `ctest` inside the image: the in-flight ceiling (including the authoring
  guide's own worked example, which is the only shape that tells a correct client from the common
  defect), credential and payload redaction, the declared capability set, option validation, the
  `Configured` fields whose absence is a violation, and the port-line scan.
- **The shared conformance suite**, which drives `pc-cpp-conformance-runner` through the same
  scenarios as every other language and asserts engine state this process cannot see:

```bash
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=cpp
```

That entry needs Docker, and it says so by failing: a missing daemon exits `2`, which fails the
build rather than skipping the language.

## Depth

[`client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md) own the
protocol; this file does not restate them.
