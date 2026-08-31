<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - Scala proxy client

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

A Scala client for the Parallel Consumer language proxy: key-ordered concurrent Kafka processing
from Scala, with the Java engine running as a sidecar child process and your function running as an
ordinary `InboundRecord => Future[Outcome]`. See [Status](#status).

## The shape

```
your process
├── your function (a lambda - the proxy never learns what it is)
├── this library
│   ├── admin      - spawns the sidecar, holds the ONE gRPC stream, owns the dispatch queue
│   └── executors  - each: take record → run your function → report the outcome
└── sidecar proxy (child process) - runs Parallel Consumer, owns Kafka entirely
```

The session itself belongs to `parallel-consumer-proxy-client-java-grpc`, which every JVM client
shares. This module is the Scala shape over it - and that is the design, not a shortcut: three JVM
session implementations would mean fixing every session defect three times.

## Using it

```scala
implicit val ec: ExecutionContext = ExecutionContext.global

val session = for {
  client <- ParallelConsumerClient.open(
    ClientOptions(
      topics = Seq("orders"),
      kafkaProperties = Map("bootstrap.servers" -> "localhost:9092")),
    SidecarCommand(Paths.get("/absolute/path/to/parallel-consumer-proxy")))
  ended <- client.poll { record =>
    // PLACE SERDE SETUP IN YOUR LANGUAGE HERE - keys and values are bytes; deserialization is yours.
    handle(record.key, record.value).map {
      case Handled => Outcome.succeeded
      case other   => Outcome.failed(s"could not handle $other")
    }
  }
} yield ended
```

Four things worth knowing before reading the API:

- **`poll` returns a `Future[Unit]` for the session, and that future is how you learn it ended and
  why.** It completes when the session has ended and the sidecar has been reaped, and it *fails with
  the cause* when the session died rather than ended - a broken stream, a protocol violation, a
  sidecar that went away. Nothing blocks, and you never have to close the client to find out what
  happened to it. `client.ended` is the same future for a caller that has not polled.
- **`Future`, not `IO` or `Task`, and that is about who this client is for.** cats-effect and ZIO
  both wrap a `Future` in one call (`IO.fromFuture`, `ZIO.fromFuture`), so this surface excludes
  neither; either alternative would exclude the other's users and add a large dependency to a client
  whose whole argument is thinness.
- **Scala 2.13.** Kafka-adjacent Scala still overwhelmingly runs 2.13, and a 2.13 artefact is usable
  from Scala 3 while the reverse is not - so 2.13 serves both populations. The pom carries the full
  argument.
- **`close()` is the clean shutdown.** It stops hand-out, lets executing records finish and report,
  half-closes the stream and reaps the sidecar; the session's future then completes.

## Saying "no verdict"

**A failed `Future` is a failure *report on the wire*** - the right answer for a function that threw,
and the wrong one for a record your client never actually ran. The only way to say "I have no verdict
for this record" is a future that never completes, which is what
`ParallelConsumerClient.noVerdict` is:

```scala
client.poll { record =>
  if (draining) ParallelConsumerClient.noVerdict   // never reported; the engine redelivers it
  else process(record)
}
```

Use it for that and nothing else - an ordinary record whose future never completes is a stall, and it
will look like one. `NoVerdictIsInventedTest` holds this rule, with controls, because a fabricated
verdict looks exactly like a real one from every side.

## What it does today

The `dispatch` capability and nothing else: connect, `Configure`, receive a dispatch wave, run your
function, report with the token echoed verbatim, shut down cleanly. Leases and heartbeats, the
manifest reconnect, worker-death reporting, terminal outcomes and the `Shutdown` drain are later
waves, and this client declares none of them - so the proxy does not expect them of it.

The in-flight ceiling and the queue that enforces it, overflow as a protocol violation rather than a
load condition, FIFO hand-out, the verbatim token echo and the shutdown order are all **inherited**
from the shared transport rather than implemented here.

## Building and testing

```bash
./mvnw test -pl :parallel-consumer-proxy-client-scala -am                     # unit tests
./mvnw test -pl :parallel-consumer-proxy-client-scala -am -Dpc.foreignClients # + the sidecar tests
scripts/analyse.sh                                                            # static analysis
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=scala
```

Static analysis is the Scala compiler: `-Xlint -Wunused -Werror`, declared once in the pom and run by
`scripts/analyse.sh`. Scala's standalone analysers were weighed and rejected - `scripts/analyse.sh`
carries the reasoning.

The last command is the shared cross-language suite. Like Kotlin, this client is driven as a
**spawned runner** (`scripts/conformance-runner`) rather than as an in-JVM binding, because it owns
a sidecar spawn; its registry entry carries no build command, because its toolchain is the Maven
build already running.

## Status

Experimental, unpublished, and built from a checkout. It carries records end to end over the real
protocol and nothing beyond that; there is no reliability claim. Its module testing-evidence record
predates its registration in the shared suite and still describes that conformance claim as
untested. Tracking: astubbs#242.

## Depth

[`client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md) own the
protocol; this file does not restate them.
