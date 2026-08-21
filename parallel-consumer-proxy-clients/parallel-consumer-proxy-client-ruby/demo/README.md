<!--
Copyright (C) 2026 Antony Stubbs and contributors
-->

# The Ruby demo

```bash
# from anywhere in the repo - picks native or container for you
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-ruby/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker. Ruby 3.2+ and a JDK are optional: with both, the demo runs natively and starts its
broker in a container; without either, the demo runs in a container too and the broker is a compose
sibling. It announces which it chose, and why, on its first line.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file records only what is specific to Ruby.

## The two arms

| arm | what it is |
|---|---|
| `AK core` | librdkafka through the [`rdkafka`](https://github.com/karafka/rdkafka-ruby) gem, one record at a time |
| `ruby-grpc` | this module's client library: it spawns the sidecar, receives records over a socket, runs the demo's block on its executor threads and reports outcomes back |

On the second arm **the application does no Kafka I/O**: the sidecar owns the consumer, the
producer, the group membership and the offsets. That is a statement about the path rather than
about the process - this same process creates the topic, produces the backlog and runs the AK core
arm with an ordinary Kafka client, because a comparison needs both sides.

**It is not an isolated price for the sidecar hop, and no per-language demo's is.** The two arms
differ in client library as well as in engine - librdkafka's C consumer against the JVM consumer
inside the sidecar. That is exactly the choice a Ruby user faces, which is why the demo is worth
running; the isolated cost of crossing the process boundary is a thing only the Java seed can
measure, and it does.

## What is specific to Ruby

### A blocking `sleep` is the right simulated work here, not merely the allowed one

The contract names Python and TypeScript as the two languages where a blocking sleep would be a
lie, and lists Ruby among the languages where it is fine. It is worth saying *why*, because the
reason is the same one that decided this client's concurrency model: **MRI releases the global VM
lock around `sleep`**, and this client's executors are **threads**, not processes
([`docs/inflight/clients/ruby.md`](../../../docs/inflight/clients/ruby.md)). N executors sleeping
are N records in flight. Had the client forked worker processes, as Python's does, a hundred
sleeping processes would have been as misleading here as it is there.

### `rdkafka`, and why not `ruby-kafka`

`rdkafka` binds librdkafka, and is what a Ruby application consumes Kafka with today - Karafka is
built on it. The pure-Ruby alternative, `ruby-kafka`, was rejected on a fact rather than a taste:
its authors archived it in 2023, and a comparison whose serial arm is an unmaintained gem would
flatter the sidecar for a reason that has nothing to do with Parallel Consumer.

It ships precompiled for Linux, so the demo container installs it without compiling anything. On
other platforms - a native run on macOS, for one - `bundle install` builds librdkafka from source
and needs a C toolchain.

### The demo does not start its own broker; its entry point does

The Java seed starts a Testcontainers broker when no `--bootstrap` is supplied. Ruby has no
equivalent this demo would rather depend on, so `demo/run.sh` starts the compose broker on the host
and hands the address in - the same address compose hands in inside the container. The promise the
contract makes, "omit `--bootstrap` and a broker is started for you", is kept one process out.

One consequence a reader should know about: the native path publishes the compose broker's **host
listener on port 29092**, not 9092, so a broker you already have on the usual port is left alone.

### Two environment variables that are not part of the flag contract

`PC_DEMO_SIDECAR_CLASSPATH` and `PC_DEMO_SIDECAR_JAVA` tell the demo where the sidecar is. **They
have no flags, and that is deliberate**: they are plumbing between the entry point and the demo -
`run.sh` computes them with Maven, the image bakes them in - rather than dials a reader turns. The
sidecar is a JVM program and Ruby cannot build one, which is why this demo needs a JDK toolchain (or
its container) at all.

### The environment reaches the container, which in the seed it does not

The contract says every flag has a `PC_DEMO_` variable and that the environment beats the defaults.
Compose forwards nothing it is not told to, so `docker-compose.yml` here forwards **every**
`PC_DEMO_` variable explicitly, and the demo reads a blank one as "not supplied". Without that,
`PC_DEMO_RECORDS=20 demo/run.sh` would silently do nothing on the container path - which is the
path a reader without Ruby always takes.

## Running one small thing

```bash
demo/run.sh --records 20 --delay-ms 1 --concurrency 4 --partitions 2 --replay-factor 1
```

Twenty records proves the machinery and measures nothing; `--replay-factor 1` skips the big replay.
Throughput at that volume is start-up cost and should not be read as a result.
