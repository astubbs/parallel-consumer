<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The .NET demo

```bash
# from anywhere in the repo - picks native or container for you
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-dotnet/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker. A .NET SDK and a JDK are optional: with both, the demo runs natively and starts its
broker in a container; without either, the demo runs in a container too and the broker is a compose
sibling. `run.sh` announces which it chose, and why, before it builds anything.

The demo itself opens with the banner every language's demo opens with - the product's name and
what is about to happen - then the effective configuration, then the run:

```
================================================================
  PARALLEL CONSUMER  -  .NET demo
  The same records, twice: one at a time, then all at once.
================================================================
```

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).** Read
that first. This file only records what is specific to .NET.

## The two arms

| arm, as the tables label it | what it is |
|---|---|
| `AK core (Confluent.Kafka)` | `Confluent.Kafka`, one record at a time, in this process |
| `dotnet-grpc (this client)` | this module's client library, over a sidecar it spawns itself |

**Each label carries the role and the client**, because "AK core" is a category rather than a
client and a reader cannot judge a comparison without knowing what produced it. In .NET the answer
is `Confluent.Kafka`, the `librdkafka` binding Confluent publishes, and the contract asks a
language with a second serious client to say so here and consider running both. **.NET does not
appear to have one.** The packages a reader is likely to meet - KafkaFlow, Streamiz - are
frameworks built **on top of** `Confluent.Kafka` rather than protocol implementations of their own,
so a second arm would price a wrapper over the same client and not a different client. If that is
wrong, the fix is a third arm here rather than a footnote.

On the `dotnet-grpc` path **the application does no Kafka I/O**: the sidecar owns the consumer, the
producer, the group membership and the offsets. That is a statement about the *path*, not about the
process - the same process creates the topic, produces the backlog and runs the `AK core` arm with
an ordinary Kafka client, because a comparison needs both sides. A genuinely foreign application
carries no Kafka client library at all, which is the property this arm stands in for.

Two arms is the whole contract outside Java. Java carries four more (`pc-core`, `java-direct`,
`java-grpc-uds`, `java-raw-grpc`) because one JVM can hold every one of them at once and each pair
changes exactly one term; here the two arms are two different client libraries as well as two
different engines, so a difference between them is not a wire cost and must not be read as one.

## What the tables print

Five columns, the same five in both tables and in the same order:

```
  arm                           elapsed          msg/s     vs AK core    records     keys
  AK core (Confluent.Kafka)        4.9s              4           1.0x         20       20
  dotnet-grpc (this client)        1.7s             11           2.9x         20       20
```

`records` and `keys` are the **deterministic** pair. Elapsed, msg/s and the ratio describe one
machine on one run and travel nowhere; every language over the same backlog must print the same
record count and the same distinct-key count, so those two are what `bin/ci-demo-conformance.sh`
can compare across languages. They also make the table demonstrate the run rather than assert it -
a throughput figure cannot show the work happened, and an arm that is **short** is a failed arm
rather than a fast one.

Keys are counted over the record key's **bytes** - base64, because a Kafka key is not text - and a
null key counts as one distinct key of its own. There is no latency column, in either table, and
that is contract: the backlog is pre-produced, so the workload is closed-loop and a per-record
timing would be flattered by however far an arm had fallen behind.

## What is specific to .NET

### The simulated work is an awaited timer, not a blocking sleep - and it is no longer a divergence

This module's demo diverged from the contract here, and **the contract has since moved to meet it**.
The rule used to name languages and listed C# as safe for a blocking sleep; it now asks a question
about the *client* instead - is it thread-per-record? - and C# is one of the six languages the
question rules out.

It rules this one out because the library's executors are `Task`s on the thread pool: a hundred of
them sitting in `Thread.Sleep` is a hundred pool threads occupied, and the pool injects
replacements at roughly one per second once its core count is used up. The sidecar arm would then
report the thread pool's injection rate rather than the engine's throughput - a number that looks
like a measurement and is not one.

So the wait is `await Task.Delay(...)`, which is what "non-occupying" means in a language whose
concurrency is tasks rather than threads. **Both arms use it.** The `AK core` arm holds one record
at a time and so could not starve anything, but it is the denominator of every ratio in both tables,
and an arm must not differ from its numerator by the wait primitive as well as by the transport.

### The sidecar is a JVM, so "the binary" is `java` and the proxy is a classpath

The client library takes an absolute path to a binary and launches it directly, never through a
shell - a wrapper process would inherit the write end of the lifecycle pipe and defeat the sidecar's
parent-death signal. In this repository the sidecar is not a shipped executable but
`parallel-consumer-proxy`'s `Main`, so the demo hands the library a `java` launcher with `-cp` and
the main class. That is scaffolding for a repository that builds its sidecar from source; the
product model is unchanged, and the sidecar is **not** a compose service in either mode.

`run.sh` builds that classpath with Maven before it starts the demo; the container bakes it in at
image build time and passes it as `PC_DEMO_SIDECAR_CLASSPATH`.

### The broker address is normalised before it travels

Testcontainers for .NET returns its address as a URI - measured here as
`plaintext://127.0.0.1:62347/`, trailing slash included. librdkafka accepts that string; the Java
client behind the sidecar rejects it, so the address is reduced to `host:port` before it goes
anywhere. It cost the sidecar arm a whole run to find, because R48 deliberately withholds the reason
from the proxy's error - a Kafka `ConfigException` embeds property values, and those may be
credentials.

### Three extra environment variables, and none is part of the contract

| variable | what it does |
|---|---|
| `PC_DEMO_SIDECAR_CLASSPATH` | the sidecar's classpath, instead of discovering it from the checkout. The container sets it |
| `PC_DEMO_SIDECAR_LOG` | any value sends the sidecar's standard error to this demo's, which is the first thing to try when the sidecar arm will not start |
| `PC_DEMO_JAVA` | the JVM to launch the sidecar with, ahead of `JAVA_HOME` and `PATH` |

The contract's own variables - `PC_DEMO_RECORDS`, `PC_DEMO_DELAY_MS`, `PC_DEMO_CONCURRENCY`,
`PC_DEMO_PARTITIONS`, `PC_DEMO_REPLAY_FACTOR`, `PC_DEMO_BOOTSTRAP`, `PC_DEMO_TOPIC` - are exactly
the flags, and flags beat the environment beats the defaults.

### The demo project is in the module's one solution

`dotnet build`, `dotnet test` and the CI row's `dotnet format analyzers` all run at the module root
with no project argument, and error out if they find more than one candidate there - so the demo
joined `Bz.Stub.ParallelConsumer.Proxy.Client.sln` rather than adding a second solution. It also
means the module's ordinary build keeps the demo compiling, under the same analyzers-as-errors lint
as the library.

## What has been run, and what has not

Both entry points, by hand, at `--records 20` - the native one with no arguments at all (the case
that has broken before, scaled down by `PC_DEMO_*` variables) and with explicit flags and a big
replay; the container one through `run.sh --docker`. Both arms completed in every run and both
exited 0, and the deterministic columns agree with what the seeding predicts: 20 records over 20
keys in the small replay, 40 over 40 in the big one.

**No run at the contract's defaults has ever happened**, and nothing here should be read as a
measurement: at twenty records both arms are dominated by consumer-group join time, and the runs
above were taken on a machine running ten agents at once. The `msg/s` and `vs AK core` columns in
this file's example are shape, not a figure anybody should quote.

`bin/ci-demo-test.sh` runs the **Java** demo through both of its entry points on every pull request.
Nothing runs this one in CI yet - which is exactly the gap that script exists to close. Open work,
with the rest, in [`docs/inflight/clients/dotnet.md`](../../../docs/inflight/clients/dotnet.md).
