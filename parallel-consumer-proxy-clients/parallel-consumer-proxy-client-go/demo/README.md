# The Go demo

```bash
# from anywhere in the repo - picks native or container for you
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker. A Go toolchain and a JDK are optional: with both, the demo runs natively and starts
its broker in a container; without either, the demo runs in a container too and the broker is a
compose sibling. `run.sh` announces which it chose, and why, before it starts building.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file only records what is specific to Go.

## The two arms

| arm | what runs |
|---|---|
| **AK core (franz-go)** | [franz-go](https://github.com/twmb/franz-go), one record at a time, in this process |
| **pc-go-grpc (this client)** | this process as a **foreign client**, through the Go client library, over a real sidecar the library spawns as a child process |

Both rows name the client that produced them, because **"AK core" is a category and not a client**:
the answer is franz-go here, `rdkafka` in Ruby, `kafkajs` in TypeScript, and a reader cannot judge
the comparison without knowing which one ran.

On the `pc-go-grpc` path the application does no Kafka I/O: the sidecar owns the consumer, the
producer, the group membership and the offsets. That is a claim about the *path*, not about the
process - the same binary creates the topic, seeds the backlog and runs the AK core arm with
franz-go, because a comparison needs both sides.

Two arms is the whole contract outside Java, which is the seed precisely because one JVM can hold
every engine at once and so price the sidecar hop with everything else held identical. Here the two
arms are different client libraries as well as different engines, so the gap between them is not
attributable to the wire.

**The arm goes through the client library, never through hand-written gRPC.** An earlier version of
the Java seed spoke the protocol by hand; it proved the *engine* worked and said nothing about the
*client library*, which is the artifact users actually touch.

## What is specific to Go

### Go has three serious Kafka clients, and this demo runs one of them

The contract asks a language with more than one serious client to say so, and to consider running
more than one as separate arms - because the choice materially changes the number, and a reader
asking "is this fast in my language" is really asking about the client they already use. Go has
three:

| client | what it is | why it is not a second arm here |
|---|---|---|
| [franz-go](https://github.com/twmb/franz-go) | pure Go, the one this demo runs | - |
| [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) | a cgo binding to librdkafka | needs cgo. `demo/Dockerfile` builds with `CGO_ENABLED=0` precisely so the binary produced in the `golang` stage can *run* in the `eclipse-temurin` stage the spawned JVM sidecar needs; a dynamically linked one would be looking for the build image's libc |
| [sarama](https://github.com/IBM/sarama) | pure Go, the long-established one | cheap to add - one more serial arm costs `records x delay-ms` - but see below |

**The judgement: name them here, run one.** The blocker is not cost, it is the drift check.
[`bin/ci-demo-conformance.sh`](../../../bin/ci-demo-conformance.sh) proves the eleven demos still
behave alike by requiring their output skeletons to be *identical*, arm rows included, and it
exempts exactly one language - Java, which is documented as carrying extra diagnostic arms. A third
row in Go and nowhere else is permanent drift, so the harness would fail on a demo that is doing
what the contract invited.

A reader who wants the sarama number is one arm away, and adding it is the right change once the
harness can express "this language legitimately carries an extra arm". Until then a divergence that
breaks the check for the other ten languages is the wrong trade, and this table is the honest
version of the same information: **the AK core row is franz-go's number, not Go's**.

### The demo binary never starts a broker - `run.sh` does

This is the one place the Go demo's *shape* differs from the Java reference, and the integrator
should know it is deliberate.

Java's `DemoBroker` starts a Testcontainers broker when no `--bootstrap` is supplied. Go has no
comparable dependency here, and adding one would put a Docker client library and its transitive
tree into a demo whose point is that the application needs no infrastructure. So the responsibility
moves one level out:

- **natively**, `run.sh` starts a `cp-kafka` container on a free port and removes it on exit;
- **in the container**, the compose sibling is the broker and `PC_DEMO_BOOTSTRAP` names it;
- **the binary always receives an address**, and says so plainly if it does not.

The user-facing contract is unchanged - omit `--bootstrap` and a broker appears - and the rule that
produced the container half is the same either way: **a demo container is never granted the host
Docker socket**, so a containerised demo reaches a broker it did not start.

### The sidecar is a JVM application, so its "binary" is the JVM launcher

The client library asks for an absolute path to a binary and spawns it directly, with no shell in
between, because the pipe it holds is the sidecar's parent-death signal. The proxy has no native
launcher yet, so the demo passes the JVM launcher as that binary and the proxy as a `-cp` argument.
`run.sh` computes that classpath once with Maven and hands it over in a file named by
`PC_DEMO_SIDECAR_CLASSPATH`; the Dockerfile bakes both in at image build time.

**`PC_DEMO_SIDECAR_CLASSPATH` is plumbing, not an eighth flag.** It has no `--flag`, no default a
user would set, and it disappears the day the sidecar ships as a binary. Every variable the contract
*does* specify - one per flag, `PC_DEMO_` plus the flag in upper snake case - is implemented exactly
as written, and `options_test.go` derives the names from the flag list rather than trusting seven
hand-written bindings.

### A blocking sleep is the right simulated work here

The contract singles out Python (worker *processes*) and TypeScript (one event loop) as the two
languages that must not use a blocking sleep. Go is not one of them: a sleeping goroutine is as
cheap as a sleeping thread, so both arms use `time.Sleep` and differ by transport and engine only.

### The demo is a nested Go module

`demo/go.mod` is separate from the client library's, and the library is reached through a `replace`
directive. Go's module graph propagates requirements to every consumer, so franz-go in the
library's `go.mod` would hand a Kafka client library to applications whose whole reason for using
the proxy is not needing one.

The consequence worth knowing: **`go build ./...` and `go test ./...` in the parent module do not
descend into this directory**, so the module's Maven-driven build never compiles the demo. `run.sh`
and the Dockerfile do, and both entry points are meant to be exercised - a demo with one tested
entry point has an untested entry point.

Run the demo's own tests directly:

```bash
cd parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/demo && go test ./...
```

## What it prints

**A banner first**, naming the product before anything else does - identical in all eleven
languages bar the language's own name. Then the effective configuration, because a number without
its settings is not reproducible, and then one table per replay:

```
================================================================
  PARALLEL CONSUMER  -  Go demo
  The same records, twice: one at a time, then all at once.
================================================================

Small replay - every arm over the same 2000 records (the comparison)
  arm                      records     keys    elapsed        msg/s   vs AK core
  AK core (franz-go)         2,000    1,000       4.1s          487         1.0x
  pc-go-grpc (this client)      2,000    1,000       0.3s        6,250        12.8x
```

*(the shape of the output, not a measurement: elapsed and msg/s depend entirely on the machine.)*

The banner is printed by the demo **binary**, so it is the first line of the demo's own output in
both modes. Natively, `run.sh` has already said which mode it chose and built the sidecar above it;
that is build progress rather than the demo introducing itself as something else.

**`records` and `keys` come before the speed columns**, and are the only two figures here that are
deterministic. Throughput alone cannot show the work happened: a short arm reads as a fast one
rather than a failed one, and a backlog that collapsed onto a single key would report the same rate
as one properly spread. Every language over the same backlog reports the same pair - this demo
seeds `key-{n % 1000}`, so `keys` is `min(records, 1000)` everywhere - which is what lets
`bin/ci-demo-conformance.sh` compare languages that can never be compared on elapsed or msg/s.

**The bootstrap address is never printed**: own-cluster mode puts a user's real broker there.

**No latency is reported, in any arm.** The backlog is pre-produced, so the workload is closed-loop
and per-record timings would be flattered by however far an arm had fallen behind. Throughput is the
only honest *speed* number this shape can produce - which is exactly why `records` and `keys` sit
beside it.

The big replay runs **only the arms that go parallel**, which here is `pc-go-grpc` alone: AK core would
need `records x replay-factor x delay-ms` milliseconds to finish a backlog the sidecar arm clears in
seconds, and waiting that long to learn nothing new is not worth the wall clock.

## Depth

The client library itself is [`../README.md`](../README.md); the protocol is owned by
[`client-authoring-guide.md`](../../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../../parallel-consumer-proxy/docs/protocol-specification.md).
Findings from this wave, including anything the contract left open, are in
[`docs/inflight/clients/go.md`](../../../docs/inflight/clients/go.md).
