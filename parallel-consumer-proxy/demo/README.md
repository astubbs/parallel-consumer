# The reference demo, and the contract every language's copy keeps

```bash
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/demo/run.sh
```

Needs Docker. A JDK is optional: with one the demo runs natively and starts its broker in a
container; without one the demo runs in a container too, and the broker is a compose sibling.
Nothing to install, configure or deploy - and the sidecar is never one of those things, because the
client library spawns it as a child process.

**The reference implementation is Java, and it lives with the Java client**, at
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/`. This contract lives here,
with the sidecar, because it binds all eleven languages rather than any one of them. An earlier
version put the demo here too and spoke the protocol by hand; it proved the *engine* worked and
said nothing about the *client library*, which is the artifact users actually touch.

## What it shows

The same records through two arms:

- **AK core** - that language's own Kafka client, one record at a time. In Java that is a plain
  `KafkaConsumer`. Always spelled "AK core", never bare "core", which reads as
  `parallel-consumer-core` ([`CONCEPTS.md`](../../CONCEPTS.md)).
- **The sidecar arm** - the application as a *foreign client*. Its client library spawns the sidecar,
  receives records over a socket, runs its own function on them, and reports outcomes back. **The
  application does no Kafka I/O on this path**: the sidecar owns the consumer, the producer, the
  group membership and the offsets. In a genuinely foreign language that is the whole story - the
  application needs no Kafka client library at all. In *this* demo it is a statement about the path,
  not about the process: the same JVM creates the topic, produces the backlog, and runs the AK core
  and pc-core arms with ordinary Kafka clients, because a comparison needs both sides.

**Java is the seed because it is the only place the sidecar hop can be priced honestly.** In every
other language the two arms are different client libraries as well as different engines. Here every
arm runs in one JVM, against one broker, with the same sleep as the user function - so the gap is
what crossing a process boundary costs, and nothing else.

### Java carries four extra arms. Do not mirror them.

Because one JVM can hold all of them at once, the Java demo also runs `pc-core` (the engine
directly), `pc-java-direct` (the client library with the engine in process), `pc-java-grpc-uds` (the
client library over a **Unix domain socket**) and `pc-java-raw-grpc` (the protocol by hand). Each
*pair* changes exactly one term, which is the only way a difference means anything:

| pair | what it isolates |
|---|---|
| `pc-core` vs `pc-java-direct` | what reaching the engine through the client library costs |
| `pc-java-direct` vs `pc-java-grpc` | going out of process at all |
| `pc-java-grpc` vs `pc-java-grpc-uds` | the TCP/IP stack, with everything else held identical |
| `pc-java-grpc` vs `pc-java-raw-grpc` | what the client library itself costs on the wire |

A language whose only Kafka client is its own has nothing to compare a wrapper or a raw wire
against, so **two arms is the whole contract everywhere else.**

**`pc-java-grpc-uds` is absent where it cannot run, and never silently.** It needs an epoll
domain-socket transport, which means Linux - including inside this demo's own container on any
host, so `demo/run.sh --docker` gets it on macOS too. The demo asks the runtime whether it can open
a domain socket rather than guessing from the operating system's name, and when it cannot it says
so and names the container as the way to include it. **An arm is additive**: where it is absent
every other arm reports exactly what it always reports, so such a platform is one row short rather
than running a different comparison. That is why availability did not have to be settled before the
arm was worth adding.

## Two replays, because one volume cannot answer both questions

| replay | arms | question |
|---|---|---|
| small | every arm, identical records | how do they compare on the same work? |
| big | every arm that goes **parallel** | what does the engine sustain once start-up stops dominating? |

At a volume the serial arm can finish in a sane wall-clock, the parallel arms are already done - so
a single-volume demo can only ever report one of the two honestly. **The big replay excludes any arm
that does not go parallel**, which in every language except Java means the AK core arm alone: it
would need minutes to hours for a backlog the sidecar clears in seconds, and waiting that long to
learn nothing new is not worth the wall clock.

## The output a reader actually sees

These are contract because the demo's whole job is to be watched. A demo that is correct and
unreadable has failed at the only thing it does.

### It opens by saying what it is

The **first thing printed** names the product. Not the module, not the arm, not a configuration
line - a reader who runs this and sees `pc-ruby-grpc: the proxy granted 100 executor threads` has been
told nothing about what they are looking at. Every language prints the same banner, differing only
in its own name:

```
================================================================
  PARALLEL CONSUMER  -  <language> demo
  The same records, twice: one at a time, then all at once.
================================================================
```

Then the effective configuration, then the run.

### The broker is quiet

A KRaft broker at INFO emits hundreds of lines of controller elections and log-segment chatter and
buries the tables. Every `docker-compose.yml` sets `KAFKA_LOG4J_ROOT_LOGLEVEL: WARN` and
`KAFKA_TOOLS_LOG4J_LOGLEVEL: WARN`. Kafka's own warnings and errors still come through - this
quietens the routine, not the diagnostics.

### Every arm names the client it actually ran

**"AK core" is a category, not a client.** A reader cannot judge a comparison without knowing what
produced it, and the answer differs everywhere: `rdkafka` in Ruby, `franz-go` in Go, `kafkajs` in
TypeScript, `confluent-kafka` in Python, `Confluent.Kafka` in .NET, `swift-kafka-client` in Swift.
So the arm is labelled with both - the role and the library:

```
AK core (franz-go)          rather than   AK core
pc-go-grpc (this client)       rather than   pc-go-grpc
```

**Where a language has more than one serious Kafka client, RUN THEM ALL, each as its own arm.** Not
"consider" - run them. A reader asking "is this fast in my language" is really asking about the
client they already use, and answering for one of three leaves two thirds of them guessing. Go has
franz-go, confluent-kafka-go and sarama; Python has confluent-kafka, kafka-python and aiokafka;
TypeScript has kafkajs and confluentinc-kafka-javascript. Each is somebody's production choice.

Name the clients you found in the demo's own README, including any you deliberately did **not** run,
with the reason. A client excluded for a real constraint - it needs cgo and the image is
`CGO_ENABLED=0`, it is archived and unmaintained - is a finding worth writing down. A client excluded
because one arm felt tidier is not.

Extra arms are a **legitimate addition, not drift**. `bin/ci-demo-conformance.sh` compares the arms
every language must have and permits additional ones, reporting them rather than failing them. That
had to be said out loud: its first version required every language's output to match exactly, so a
language adding a second client would have been failed for doing what this section asks.

## Idiomatic inside, identical outside

**The Java demo is a seed, not a template.** What must match across eleven languages is everything
this document specifies: the flags and their precedence, the banner, the tables, the arms, the
container rules, the exit codes. **Nothing about the internals must match, and trying to make them
match makes every demo worse.**

Write each demo the way someone fluent in that language would write it. Use its idioms, its
concurrency primitives, its error handling, its project layout, its test framework. If the Java
version uses a class where your language wants a function, use a function. If it threads a value
through three objects and your language has a better way, take the better way. A Java program
transliterated into Ruby is not a Ruby demo - it is a Java demo that happens to run under Ruby, and
it teaches a Ruby reader nothing about using this client in Ruby.

This is not licence to diverge on behaviour. The contract above is exactly the part that is not
yours to reinterpret; everything below the output is.

### Every arm reports what it did, not just how fast

Throughput alone cannot show the work happened. Each arm also reports **records processed** and
**unique keys seen**, so the table demonstrates the run rather than asserting it:

| | |
|---|---|
| **records** | must equal the target; a short arm is a failed arm, not a fast one |
| **keys** | the distinct keys observed, which shows the backlog was really spread rather than one key repeated |

These two are **deterministic** - every language processing the same records reports the same
figures - which is what makes them comparable across languages when elapsed and msg/s never can be.
`bin/ci-demo-conformance.sh` compares their **values** across languages for that reason, rather than
masking them the way it masks a rate.

**The column order is fixed, and it is:**

```
arm | records | keys | elapsed | msg/s | vs AK core
```

Evidence before rate: what the arm *did* comes before how fast it did it.

This is written down because it was once left unstated, and eleven implementations then returned
**three different orders** from the one document: six beside `arm`, four appended after
`vs AK core`, one in the middle. Every one of the eleven was defensible, and the two that wrote down
a reason gave a *good* one - appending kept the original four-column header a prefix of the new one,
which mattered while the conformance skeleton matched headers loosely. The lesson is not that anyone
chose badly. It is that a contract which leaves something unstated does not get consistency, it gets
a vote, and nothing anywhere goes red to report the result.

**Assert your column order in your own test suite.** Three of the eleven already did - the ones that
did were not the problem, and their tests are what made the divergence cheap to find and safe to fix.
A language whose table shape nothing asserts will drift again the next time someone adds a column,
and the cross-language check cannot be the only thing watching: it compares languages to each other,
so eleven demos that drift *together* still pass it.

Column *widths* are still not contract: a longer arm name may widen a column, and a check that
enforced alignment would put every language with a long client name in permanent violation.

## The contract a per-language demo must keep

Mirror this, so a reader who has run one has run them all:

| | |
|---|---|
| **entry point** | `<client-module>/demo/run.sh`, no arguments needed |
| **flags** | `--records --delay-ms --concurrency --partitions --replay-factor --bootstrap --topic`, same defaults |
| **environment** | every flag has one, `PC_DEMO_` + the flag in upper snake case. Flags beat the environment beats the defaults |
| **arms** | that language's own Kafka client, and that language over the sidecar |
| **replays** | small over every arm; big over the parallel arms only |
| **output** | the two tables above, same columns, same order |
| **fingerprint** | print the effective configuration before running - a number without its settings is not reproducible. Never print the bootstrap address: own-cluster mode puts a user's real broker there |
| **container** | `<client-module>/demo/Dockerfile` and `docker-compose.yml`, so a reader with only Docker can run it. Java is included, not exempted (R72) |
| **latency** | do not report any. The backlog is pre-produced, so the workload is closed-loop and per-record timings are flattered by however far an arm fell behind. Reporting throughput only is the honest option available here |

### Pick a free port, do not reserve one

A demo that publishes a fixed host port cannot run beside another demo that chose the same number,
and two of eleven collided on 29092 the first time all eleven ran. **Do not solve this by allocating
eleven distinct numbers** - that is a registry to maintain, and it is wrong the moment somebody runs
two copies of the same demo.

**Try a port; if the bind fails, try the next one.** Report the port actually used. The demo already
reports the topic it created for the same reason: a value chosen at run time is fine as long as it is
announced.

### The container rule that is not negotiable

**A demo container is never granted the host Docker socket.** Broker mode inside a container reaches
a broker started as a compose sibling on the demo's own network, never one the container starts
itself. A documented socket mount is root-equivalent host access taught as the normal way to run the
product. This is U35's rule, and it is why the demo takes a `--bootstrap` address rather than
starting Testcontainers when it is containerised.

**The sidecar is not a compose service**, either. The client library spawns and supervises it
(KTD41), so the user never installs, deploys or operates a process. A compose service would show a
deployment the product does not ask for.

### The one thing that genuinely differs per language

**The simulated work must not occupy a slot the engine is counting as available.** The rule used to
name languages, and that was wrong - it listed nine as safe for a blocking sleep, and four of them
turned out not to be. The predicate is a property of the CLIENT, not of the language:

> **Is the client thread-per-record?** If each record gets a thread that can block harmlessly, a
> blocking sleep is honest. If records share an event loop, a coroutine dispatcher, an async
> runtime, a fixed cooperative pool, or worker processes, a blocking sleep caps in-flight work at
> something *other than* the concurrency the fingerprint printed - and the table then reports the
> runtime's ceiling while appearing to report the engine's.

Measured, not reasoned: in Rust the same workload gave **10,341 msg/s** through the client's
blocking adapter and **3,518 msg/s** with a raw thread sleep - roughly core-count-over-delay, which
is the ceiling the runtime imposed rather than anything about Parallel Consumer.

By that predicate: **Python** (worker processes), **TypeScript** (one event loop), **Rust** (async
runtime), **Kotlin** (coroutines on a bounded dispatcher), **Swift** (cooperative pool) and **C#**
(thread-pool tasks) all need their language's non-occupying wait. **Ruby** does not - its executors
are threads and MRI releases the GVL around `sleep`, which was checked rather than assumed. Apply
the predicate to your client; do not trust a list.

Everything else in the contract is identical by design. Where a language must diverge, say so in its
own README rather than quietly changing the shape.

## Both entry points are tested, and that is part of the contract

A demo with one tested entry point has an untested entry point. `bin/ci-demo-test.sh` runs this one
through **both** - native and container - on every pull request, at a volume chosen to prove the
machinery rather than to measure anything.

It is deliberately separate from the module's `ReferenceDemoIT`. That test calls the demo's own
entry method and proves the *arms* work; it says nothing about how a reader actually starts the
thing - the classpath step, the forked JVM the spawned sidecar depends on, the image build, the
compose broker, or the exit code a scripted caller sees. **Every failure this demo has actually had
lived in that gap**, and not one of them was a logic error that a unit test could have caught.

A per-language demo inherits this: mirroring the flags and the tables is not enough if nobody ever
runs the container you shipped.

## What this demo is not

This is the **comparison** demo: two arms, one workload, throughput out. It is not the *reading*
demo of plan unit U35's second half, which consumes and displays - three modes (own-cluster,
broker, mock), a TTY prompt with a documented non-TTY fallback, a marked
`PLACE SERDE SETUP IN YOUR LANGUAGE HERE` extension point, and a rate-limited sample of message
content.

Those belong there and deliberately not here, because they answer a different question and some of
them are meaningless against a comparison. Reporting a rate-limited sample of message *content* is
the opposite of a demo whose entire output is two throughput tables, and a non-TTY default of
*mock* would mean an automated run measured throughput against a `MockConsumer`. R39 does not govern
either demo: R39 is about how configuration reaches the proxy, and a demo is an application.
