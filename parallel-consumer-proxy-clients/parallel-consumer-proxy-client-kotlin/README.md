<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - Kotlin proxy client

A Kotlin client for the Parallel Consumer language proxy: key-ordered concurrent Kafka processing
from Kotlin, with the Java engine running as a sidecar child process and your function running as an
ordinary **suspending lambda**.

**Wave one. Not for application use** - see [Status](#status).

## The shape

```
your process
├── your function (a suspend lambda - the proxy never learns what it is)
├── this library
│   ├── admin      - spawns the sidecar, holds the ONE gRPC stream, owns the dispatch queue
│   └── executors  - coroutines, each: take record → run your function → report the outcome
└── sidecar proxy (child process) - runs Parallel Consumer, owns Kafka entirely
```

Coroutines are Kotlin's whole contribution to that shape. They are **not** here to multiply
concurrency - the proxy's `max_concurrency` is the ceiling and the handshake decides the executor
count. They are here so *your* code may suspend: a Ktor call, a coroutine-native driver, a `delay`,
with no thread-blocking wrapper and no hidden thread pool.

## Using it

```kotlin
val client = ParallelConsumerClient.open(
    options = ClientOptions(
        topics = listOf("orders"),
        kafkaProperties = mapOf("bootstrap.servers" to "localhost:9092"),
    ),
    sidecar = SidecarCommand(Path.of("/absolute/path/to/parallel-consumer-proxy")),
)

client.use {
    it.poll { record ->
        // PLACE SERDE SETUP IN YOUR LANGUAGE HERE - keys and values are bytes; deserialization is yours.
        when (val result = handle(record.key, record.value)) {
            is Handled -> Outcome.Success()
            else -> Outcome.Failure("could not handle $result")
        }
    }
}
```

Four things worth knowing before reading the API:

- **`poll` suspends for the life of the session; it does not block a thread and it does not return
  once processing has started.** That is deliberate, and it is Kotlin's answer to a question the
  shared specification leaves open. A function that starts background work and returns leaves
  coroutines nobody owns - exactly what structured concurrency exists to prevent. Suspending makes
  the session a child of your scope: cancel the scope and the session ends, wrap it in `withTimeout`
  and it is bounded, and a protocol violation is thrown to you rather than delivered to a callback.
- **`close()` is the clean shutdown, cancellation is the abrupt one.** `close` stops hand-out, lets
  executing records finish and report, half-closes the stream and reaps the sidecar - and `poll`
  then returns. Cancelling the polling coroutine instead cancels executing records; the proxy
  redelivers them.
- **A throw is a failure outcome**, translated once, in one place - so one bad record cannot tear
  down the stream. `CancellationException` is the exception to that: it is re-thrown, never turned
  into a verdict for a record whose processing was cancelled.
- **The sidecar path must be absolute.** The library never resolves it through `PATH` or a relative
  lookup: this process hands that binary your Kafka credentials, so which binary runs is
  security-relevant. It is launched directly and never through a shell, because a shell wrapper
  holds the lifecycle pipe open and defeats the proxy's parent-death signal.

`kafkaProperties` is credential-bearing, and nothing here logs it, prints it, or puts it in a
`toString()` - at any level.

## Building and testing

Kotlin is the one non-Java client with **no foreign-toolchain wrapper**: it is a JVM language and
Maven builds it directly with `kotlin-maven-plugin`. There is no `Makefile`, no `kotlinc` invocation
and nothing to install beyond the repo's own JDK 17.

```bash
# compile and run the unit tests (no sidecar involved)
bin/build.sh -pl :parallel-consumer-proxy-client-kotlin -am

# the end-to-end test against the REAL test-mode sidecar; -Dpc.foreignClients is what supplies
# the harness classpath (see the kotlin-e2e-harness profile in pom.xml)
./mvnw --batch-mode test -pl :parallel-consumer-proxy-client-kotlin -am -Dpc.foreignClients
```

The end-to-end test spawns `TestModeMain --mock` as an ordinary child process, so it exercises the
whole lifecycle contract - launch, port line, loopback connect, handshake, dispatch, report,
half-close, reap - rather than an in-process shortcut. It **fails** rather than skips when its
classpath file is missing, and names the command that produces it.

## Static analysis

[detekt](https://detekt.dev) - bug and bad-pattern detection, not only style:

```bash
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-kotlin/detekt.sh
```

Same version (1.23.7), same jar (sha256-pinned), same flags as this module's CI row, so a local
green is evidence about the row. It runs detekt's **default** ruleset with no config file, exactly
as CI does - where a default rule is wrong for a piece of code, that code carries an `@Suppress`
with its reason rather than a config file quietly disabling the rule everywhere.

## Status

Wave one of the Kotlin client (astubbs#242): connect, `Configure`, one `Dispatch` wave, the user's
function, the report with the token echoed verbatim, and a clean client-initiated shutdown - proven
end to end against the test-mode sidecar.

This client declares exactly the `dispatch` capability, so the proxy expects nothing of it that it
does not do. Deferred to later waves, and **not** implemented here: the liveness lease and
heartbeats, the manifest reconnect and `Drop`, worker-death reporting, terminal outcomes, the
`Shutdown` drain and `Released`, the demo and its container, publishing, and the rest of the
conformance suite.
