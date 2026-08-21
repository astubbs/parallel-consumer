# The Scala demo

```bash
# from anywhere in the repo - picks native or container for you
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-scala/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker. A JDK is optional: with one, the demo runs natively and starts its broker in a
container; without one, the demo runs in a container too and the broker is a compose sibling. It
announces which it chose, and why, on its first line.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file only records what is specific to Scala.

## The two arms

| arm | what it is |
|---|---|
| `AK core` | a plain `KafkaConsumer`, driven from Scala, one record at a time |
| `scala-grpc` | this module's `ParallelConsumerClient` over a sidecar the client library spawns |

`scala-grpc` goes through **the client library**, not the protocol. Nothing in
`demo/src/main/scala` names a protobuf message, a channel or a token - the demo opens a
`ParallelConsumerClient`, hands it a `ClientOptions`, and returns a `Future[Outcome]` per record,
which is the whole of what a Scala user writes. An earlier version of the Java seed spoke the wire
by hand and had to be rewritten, because it proved the *engine* worked and said nothing about the
artifact users actually touch.

**Scala runs two arms, not six, and that is deliberate rather than unfinished.** The Java seed also
runs `pc-core`, `java-direct`, `java-grpc-uds` and `java-raw-grpc`, because one JVM can hold all of
them against one broker so each *pair* changes exactly one term. Scala is a JVM language and could
technically do the same - which is exactly the temptation the contract forecloses. A reader who has
run one language's demo has run them all, and a six-row Scala table beside a two-row Ruby table
would not be the same demo.

## Divergences from the contract

**None in the interface.** Same flags, same `PC_DEMO_*` environment variables, same precedence
(flags beat environment beats defaults), same defaults, same two tables in the same order, the
effective configuration printed first and never carrying the bootstrap address, and no latency
reported anywhere.

Three things are Scala's own, and none of them changes the shape:

- **The simulated work is a blocking `Thread.sleep`**, which the contract permits in Scala. It is
  paid for explicitly: the sidecar arm runs the user function on a fixed pool sized to
  `--concurrency`, so a record occupying a thread for `--delay-ms` cannot make the arm measure the
  pool instead of the engine. The client library's own plumbing - the spawn's wait, the handshake,
  each record's completion - runs on a *different* execution context, because every thread in the
  work pool is asleep at peak and the library's continuations must not queue behind the work they
  exist to complete.
- **The big replay has exactly one row.** It excludes any arm that does not go parallel, which in
  Scala means AK core, and Scala has no third arm to keep it company. That is the contract's own
  arithmetic rather than a degenerate table: the row says what the engine sustains once start-up
  stops dominating, and the small replay above it is where the comparison lives.
- **The sidecar "binary" is this JVM's own `java` launcher with a classpath argument.**
  `SidecarCommand` requires an absolute path to an executable, and the sidecar ships as a jar. This
  is the same shape the module's end-to-end test uses and the same one the Java seed's
  `SidecarProcess` uses; it is a fact about a JVM sidecar, not a divergence in what the demo shows.

And one thing this demo has that the Java seed does not: **`logback.xml`, pointed at by
`-Dlogback.configurationFile` rather than shipped as a resource.** With no logging configuration
anywhere on the classpath, logback falls back to root at `DEBUG` - a fifty-record run buried both
tables under four thousand lines of Netty frames and docker-java headers. One of the levels it sets
is a rule rather than a preference: every Kafka client logs its full effective configuration at
`INFO` when constructed, `bootstrap.servers` included, which would print the address the demo's own
fingerprint deliberately omits. The file says so at the point of use.

## How it is built, and why the demo is behind a profile

The demo's sources are a **test source root added only by the `scala-demo` Maven profile**
(`-Dpc.scalaDemo`, which `run.sh` and the Dockerfile both pass). Outside that profile they are not
compiled and the demo's dependencies do not exist.

That is not tidiness. The sidecar arm has to hand its child process a classpath carrying
`parallel-consumer-proxy`, and an unconditional dependency on it would put the engine in this
module's ordinary `-am` reactor - where `bin/build.sh`, which opens with `clean`, would delete the
sidecar jar every other language's conformance test spawns. The module's standing check still
passes:

```bash
./mvnw -pl :parallel-consumer-proxy-client-scala -am validate   # must not print parallel-consumer-proxy
```

The `scala-e2e-harness` profile above it exists for the same reason, and this is the same
arrangement.

## The container

`Dockerfile` and `docker-compose.yml`, with **the broker as a compose sibling and no host Docker
socket ever mounted** - a documented socket mount is root-equivalent host access taught as the
normal way to run the product. **The sidecar is not a compose service either**: the client library
spawns it as a child process inside the demo's own container, exactly as it does natively, because a
compose service would show a deployment the product does not ask for.
