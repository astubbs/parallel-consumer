# The Java demo

```bash
# from anywhere in the repo - picks native or container for you
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker. A JDK is optional: with one, the demo runs natively and starts its broker in a
container; without one, the demo runs in a container too and the broker is a compose sibling. It
announces which it chose, and why, on its first line.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file only records what is specific to Java.

## What is specific to Java

Java is the one language that can run every arm in a single JVM against a single broker, so its
demo carries four arms no other language's demo has or needs: `pc-core`, `java-direct`,
`java-grpc-uds` and `java-raw-grpc`. They exist to price the client library and the wire hop
separately. Everywhere else the two contract arms - that language's own Kafka client, and that
language over the sidecar - are the whole demo.

### Which client each arm names

The contract requires every row to name the library that produced it, because "AK core" is a
category rather than a client. Java's answers:

| arm | client | what it is |
|---|---|---|
| `AK core` | `KafkaConsumer` | Apache Kafka's own consumer, one record at a time |
| `pc-core` | `ParallelEoSStreamProcessor` | the engine directly, no client library |
| `java-direct` | `this client, in process` | the client library, engine bound in process |
| `java-grpc` | `this client` | **the contract arm**: the client library over a spawned sidecar |
| `java-grpc-uds` | `this client, over UDS` | the same, over a Unix domain socket |
| `java-raw-grpc` | `no client library` | the protocol written by hand, as a control |

**Java has one serious Kafka client**, so unlike Go or Ruby there is no second `AK core` arm worth
running: `KafkaConsumer` is what a reader asking "is this fast in my language" already uses.
`java-grpc` keeps the bare `this client` spelling every language uses for that row; the four extra
arms qualify themselves against it.

`java-grpc-uds` needs an epoll domain-socket transport, so it is absent on macOS natively and
present in the container. The demo asks the runtime rather than guessing, and says so when it
cannot run.

### The keys column is checkable here

The contract asks every arm for `records` and `keys` beside its rate, because those two are
deterministic where a throughput figure never is. In this demo the backlog is seeded cyclically over
a fixed key space (`DemoBroker.KEY_SPACE`, 1,000), so the expected count is exactly
`min(records, 1000)` - `DemoBroker.expectedUniqueKeys` is that arithmetic, and `ReferenceDemoIT`
asserts every arm hit it. A `keys` figure that collapses towards 1 means the backlog was never
spread, however good the rate looks.

### Noise this demo has not yet removed

Roughly thirty lines of logback's own configuration status print **before** the banner, on **both**
entry points. They come from `logback-test.xml` inside `parallel-consumer-core`'s test jar, which
the demo's classpath carries for the sidecar spawn: its `scan="true"` cannot watch a file inside a
jar, logback warns, and a warning makes it dump its whole status. A `logback.xml` here would not
win - a `logback-test.xml` anywhere on the classpath outranks it - so the fix belongs wherever that
test jar's logging config does, not in this module.

## Where the code lives

The code lives in `../parallel-consumer-proxy-client-java-demo`, beside the client library it
exercises rather than beside the sidecar. An earlier version lived in the sidecar module and spoke
the protocol by hand; it demonstrated that the engine works and said nothing about the client,
which is the artifact users actually touch. That arm survives as `java-raw-grpc`, as a control.
