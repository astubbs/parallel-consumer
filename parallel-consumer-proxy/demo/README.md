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
- **The sidecar arm** - the application as a *foreign client*: it never touches Kafka. Its client
  library spawns the sidecar, receives records over a socket, runs its own function on them, and
  reports outcomes back.

**Java is the seed because it is the only place the sidecar hop can be priced honestly.** In every
other language the two arms are different client libraries as well as different engines. Here every
arm runs in one JVM, against one broker, with the same sleep as the user function - so the gap is
what crossing a process boundary costs, and nothing else.

### Java carries three extra arms. Do not mirror them.

Because one JVM can hold all of them at once, the Java demo also runs `pc-core` (the engine
directly), `java-direct` (the client library with the engine in process) and `java-raw-grpc` (the
protocol by hand). Each pair isolates one cost: the client library, and going out of process. A language
whose only Kafka client is its own has nothing to compare a wrapper or a raw wire against, so
**two arms is the whole contract everywhere else.**

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

**The simulated work must use that language's non-occupying wait.** A blocking sleep is fine in
Java, Kotlin, Scala, Go, Ruby, Rust, Swift, C# and C++. It is **not** fine in two places:

- **Python** - the client runs worker *processes*; a hundred sleeping processes is not the free
  thing a hundred sleeping threads is.
- **TypeScript** - a single event loop; a blocking sleep there stops everything, so it must be an
  awaited timer.

Everything else in the contract is identical by design. Where a language must diverge, say so in its
own README rather than quietly changing the shape.

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
