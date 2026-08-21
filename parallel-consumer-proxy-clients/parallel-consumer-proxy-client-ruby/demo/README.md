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
| `AK core (rdkafka)` | librdkafka through the [`rdkafka`](https://github.com/karafka/rdkafka-ruby) gem, one record at a time |
| `pc-ruby-grpc (this client)` | this module's client library: it spawns the sidecar, receives records over a socket, runs the demo's block on its executor threads and reports outcomes back |

Each arm carries the **library it actually ran**, not only its role. "AK core" is a category and
every language fills it with a different client - `rdkafka` here, `franz-go` in Go, `kafkajs` in
TypeScript - and a reader asking "is this fast in my language" is really asking about the client
they already use.

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

The contract's rule is a predicate about the **client**, not a list of languages: *is it
thread-per-record?* If each record gets a thread that can block harmlessly, a blocking sleep is
honest; if records share an event loop, a coroutine dispatcher, an async runtime, a cooperative pool
or worker processes, a blocking sleep caps in-flight work at something other than the concurrency
the fingerprint printed, and the table then reports the runtime's ceiling while appearing to report
the engine's.

Applied here: this client's executors are **threads**, not processes, and **MRI releases the global
VM lock around `sleep`** ([`docs/inflight/clients/ruby.md`](../../../docs/inflight/clients/ruby.md)),
so N executors sleeping are N records in flight. That was **checked against this client's design
rather than assumed** - and it is worth knowing that the earlier version of the rule, which did name
languages, named nine as safe and was wrong about four of them. Had this client forked worker
processes, as Python's does, a hundred sleeping processes would have been as misleading here as it
is there.

### Ruby has more than one serious Kafka client, and this demo runs one of them

The contract asks a language with more than one to say so here, and to consider running both as
separate arms. Ruby has two, and **the second one is not a live option**:

| gem | what it is | why the demo does or does not run it |
|---|---|---|
| [`rdkafka`](https://github.com/karafka/rdkafka-ruby) | librdkafka behind FFI | **the serial arm.** What a Ruby application consumes Kafka with today - Karafka is built on it |
| [`ruby-kafka`](https://github.com/zendesk/ruby-kafka) | a pure-Ruby protocol implementation | **not run.** Its authors archived it in 2023 |

The second row is the whole argument for having only one serial arm. Adding a second arm is only
worth the reader's wall clock if the number it produces is one a reader might act on, and a
comparison whose serial arm is an **unmaintained** gem flatters the sidecar for a reason that has
nothing to do with Parallel Consumer - it would price a library nobody should adopt, and invite the
conclusion that Parallel Consumer is fast because `ruby-kafka` is slow. If `ruby-kafka` is ever
un-archived, this is the decision to revisit; nothing else about it would need to change.

`rdkafka` ships precompiled for Linux, so the demo container installs it without compiling
anything. On other platforms - a native run on macOS, for one - `bundle install` builds librdkafka
from source and needs a C toolchain.

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
