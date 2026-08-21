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
sibling. It announces which it chose, and why, on its first line.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).** Read
that first. This file only records what is specific to .NET.

## The two arms

| arm | what it is |
|---|---|
| `AK core` | `Confluent.Kafka`, one record at a time, in this process |
| `dotnet-grpc` | this module's client library, over a sidecar it spawns itself |

On the `dotnet-grpc` path **the application does no Kafka I/O**: the sidecar owns the consumer, the
producer, the group membership and the offsets. That is a statement about the *path*, not about the
process - the same process creates the topic, produces the backlog and runs the `AK core` arm with
an ordinary Kafka client, because a comparison needs both sides. A genuinely foreign application
carries no Kafka client library at all, which is the property this arm stands in for.

Two arms is the whole contract outside Java. Java carries four more (`pc-core`, `java-direct`,
`java-grpc-uds`, `java-raw-grpc`) because one JVM can hold every one of them at once and each pair
changes exactly one term; here the two arms are two different client libraries as well as two
different engines, so a difference between them is not a wire cost and must not be read as one.

## What is specific to .NET

### The simulated work is an awaited timer, not a blocking sleep - and that is a divergence

The contract says a blocking sleep is fine in C#, and names Python and TypeScript as the two
exceptions. **It is not fine here**, and the reason is this client's shape rather than the
language's: the library's executors are `Task`s on the thread pool, so a hundred of them sitting in
`Thread.Sleep` is a hundred pool threads occupied, and the pool injects replacements at roughly one
per second once its core count is used up. The sidecar arm would report the thread pool's injection
rate rather than the engine's throughput - a number that looks like a measurement and is not one.

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

### Two extra environment variables, and neither is part of the contract

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

## What is NOT here

`bin/ci-demo-test.sh` runs the **Java** demo through both of its entry points on every pull request.
Nothing runs this one in CI yet. Both entry points have been run by hand; the automated equivalent
is open work, recorded in
[`docs/inflight/clients/dotnet.md`](../../../docs/inflight/clients/dotnet.md).
