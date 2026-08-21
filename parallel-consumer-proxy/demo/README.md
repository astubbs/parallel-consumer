# The reference demo, and the contract every language's copy keeps

```bash
parallel-consumer-proxy/demo/run.sh
```

Needs Docker and a JDK 17 toolchain. Nothing to install, configure or deploy: the broker is a
container the demo starts, and the sidecar is a binary the demo spawns as a child process.

## What it shows

The same records through two arms:

- **AK core** - a plain `KafkaConsumer`, one record at a time. Always spelled "AK core", never bare
  "core", which reads as `parallel-consumer-core` ([`CONCEPTS.md`](../../CONCEPTS.md)).
- **Sidecar** - the application as a *foreign client*: it never touches Kafka. It spawns the sidecar,
  receives records over a socket, runs its own function on them, and reports outcomes back.

**Java is the seed because it is the only place the sidecar hop can be priced honestly.** In every
other language the two arms are different client libraries as well as different engines. Here both
arms run in one JVM, against one broker, with the same sleep as the user function - so the gap is
what crossing a process boundary costs, and nothing else.

## Two replays, because one volume cannot answer both questions

| replay | arms | question |
|---|---|---|
| small | both, identical records | how do they compare on the same work? |
| big | sidecar only | what does the engine sustain once start-up stops dominating? |

At a volume the serial arm can finish in a sane wall-clock, the parallel arm is already done - so a
single-volume demo can only ever report one of the two honestly. The serial arm is dropped from the
big replay because it would need minutes to hours for a backlog the sidecar clears in seconds.

## The contract a per-language demo must keep

Mirror this, so a reader who has run one has run them all:

| | |
|---|---|
| **entry point** | `<module>/demo/run.sh`, no arguments needed |
| **flags** | `--records --delay-ms --concurrency --partitions --replay-factor`, same defaults |
| **arms** | that language's own Kafka client, and that language over the sidecar |
| **output** | the two tables above, same columns, same order |
| **fingerprint** | print the effective configuration before running - a number without its settings is not reproducible |
| **latency** | do not report any. The backlog is pre-produced, so the workload is closed-loop and per-record timings are flattered by however far an arm fell behind. Reporting throughput only is the honest option available here |

### The one thing that genuinely differs per language

**The simulated work must use that language's non-occupying wait** (KTD40). A blocking sleep is fine
in Java, Kotlin, Scala, Go, Ruby, Rust, Swift, C# and C++. It is **not** fine in two places:

- **Python** - the client runs worker *processes*; a hundred sleeping processes is not the free thing
  a hundred sleeping threads is.
- **TypeScript** - a single event loop; a blocking sleep there stops everything, so it must be an
  awaited timer.

Everything else in the contract is identical by design. Where a language must diverge, say so in its
own README rather than quietly changing the shape.
