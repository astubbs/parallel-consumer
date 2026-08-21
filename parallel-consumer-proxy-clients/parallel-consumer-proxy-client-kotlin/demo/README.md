# The Kotlin demo

```bash
# from anywhere in the repo - picks native or container for you
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-kotlin/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker. A JDK is optional: with one, the demo runs natively and starts its broker in a
container; without one, the demo runs in a container too and the broker is a compose sibling. It
announces which it chose, and why, on its first line.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).** Read
that first. This file records only what is specific to Kotlin.

## The two arms

| arm | what it is |
|---|---|
| **AK core** | a plain `KafkaConsumer` driven from Kotlin, one record at a time |
| **kotlin-sidecar** | this module's `ParallelConsumerClient`: it spawns the sidecar as a child process, receives records over a socket, runs a **suspending** function on them and reports outcomes back |

Two arms is the whole contract everywhere except Java. Java can hold the engine, the client library
in process and the raw wire in one JVM, so each of its extra pairs changes exactly one term; Kotlin
has no second Kafka client to compare a wrapper against, so a third arm here would compare two
things at once and mean nothing.

**The sidecar arm goes through the client library, not the protocol.** An earlier version of the
Java demo spoke gRPC by hand: it proved the engine worked and said nothing about the client library,
which is the artifact users actually touch. Everything the sidecar arm does here - the spawn, the
handshake, the dispatch, the report, the reap - is code in `../src/main/kotlin`, called the way an
application calls it.

## What is specific to Kotlin

### The simulated work is `delay`, not `Thread.sleep` - and that is a divergence, stated

The shared contract says the simulated work "must use that language's non-occupying wait", and then
lists Kotlin among the languages where "a blocking sleep is fine". **This demo takes the rule and
not the list**, because the list is written for a thread-per-record client and this client is not
one.

The user's function here is `suspend (InboundRecord) -> Outcome`, and each record runs as a
coroutine on `Dispatchers.IO`. `Dispatchers.IO` has a default parallelism of 64. So a blocking sleep
inside that coroutine occupies one of 64 threads, and **caps in-flight records at 64 however high
`--concurrency` is set** - while the effective-configuration block goes on printing the number the
reader asked for. That is precisely the failure the fingerprint exists to prevent: a throughput
figure reported against settings that did not apply.

Observed rather than assumed. At `--records 4000 --delay-ms 50 --concurrency 200 --replay-factor 1`,
the sidecar arm finished in **2.7s**. 4000 records at 50ms each is 200 record-seconds of work, so
the mean number of records in flight was about **74** - above the 64 a blocking sleep could ever
have reached. The suspending wait demonstrably ran wider than a blocking one can. (That is an
inference from one arm rather than a two-arm controlled experiment; a loaded machine can only push
the in-flight figure *down*, never above the thread ceiling, so the direction of the conclusion is
safe even though the absolute rate is not.)

The AK core arm still uses `Thread.sleep`, and that is not an inconsistency: it is a serial loop
with nothing to interleave, so blocking and suspending cost it the same wall clock. The distinction
only bites where records run concurrently.

### The demo is not a Maven module

The Java demo is. This one lives inside the client module, under `demo/`, compiled by the
`kotlin-demo` profile in [`../pom.xml`](../pom.xml). A new module needs a line in the clients
aggregator pom, and that file is shared by every language wave at once.

Two consequences worth knowing before you edit anything here:

- **The demo compiles into the module's test output tree** (`target/test-classes`), because the
  module's published surface is guarded by `-Xexplicit-api=strict` and a demo is not part of it.
  Its sources live under `demo/src`, not `src/test`, so nothing reads it as a test. The module
  already puts a program-that-uses-the-client there - `scripts/conformance-runner` launches one.
- **The engine dependency is behind `-Dpc.kotlinDemo`.** The sidecar arm hands the spawned child
  this JVM's classpath, so the demo needs `parallel-consumer-proxy` on it - but declaring that
  unconditionally would give the module a permanent reactor edge to the engine, and `bin/build.sh`
  opens with `clean`, so an ordinary build of this module would delete the sidecar jar every other
  language's conformance test spawns. The invariant still holds and is measurable:

  ```bash
  ./mvnw -pl :parallel-consumer-proxy-client-kotlin -am validate   # must not print parallel-consumer-proxy
  ```

### The logging configuration has a deliberately strange name

`pc-kotlin-demo-logback.xml`, named explicitly in `-Dlogback.configurationFile` by both `run.sh` and
the `Dockerfile`. `parallel-consumer-core`'s test output is on this demo's classpath and carries a
`logback-test.xml`, which logback prefers over any `logback.xml` whatever the classpath order - so
named the obvious thing, the demo's own configuration was silently ignored and the first run printed
core's INFO levels over the tables. The demo's own output goes through `println`, not the logger, so
it survives any level a reader or a container imposes.

## What has not been done

- **No CI entry-point test.** `bin/ci-demo-test.sh` runs the *Java* demo through both entry points
  on every pull request. Nothing yet does that for this one, and the contract's "both entry points
  are tested" clause is therefore unmet here. Tracked in
  [`docs/inflight/clients/kotlin.md`](../../../docs/inflight/clients/kotlin.md).
