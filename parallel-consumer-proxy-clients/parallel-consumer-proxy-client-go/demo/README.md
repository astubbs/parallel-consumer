# The Go demo

```bash
# from anywhere in the repo - picks native or container for you
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/demo/run.sh

# or, from this directory, the plain container path with nothing else needed
docker compose up
```

Needs Docker. A Go toolchain and a JDK are optional: with both, the demo runs natively and starts
its broker in a container; without either, the demo runs in a container too and the broker is a
compose sibling. It announces which it chose, and why, on its first line.

**The contract this keeps - and that every other language's demo keeps - is
[`parallel-consumer-proxy/demo/README.md`](../../../parallel-consumer-proxy/demo/README.md).**
Read that first. This file only records what is specific to Go.

## The two arms

| arm | what runs |
|---|---|
| **AK core** | [franz-go](https://github.com/twmb/franz-go), one record at a time, in this process |
| **go-grpc** | this process as a **foreign client**, through the Go client library, over a real sidecar the library spawns as a child process |

On the `go-grpc` path the application does no Kafka I/O: the sidecar owns the consumer, the
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

The effective configuration first - a number without its settings is not reproducible - and then
one table per replay, same columns and same order as every other language's demo. **The bootstrap
address is never printed**: own-cluster mode puts a user's real broker there.

**No latency is reported, in any arm.** The backlog is pre-produced, so the workload is closed-loop
and per-record timings would be flattered by however far an arm had fallen behind. Throughput is the
only honest number this shape can produce.

The big replay runs **only the arms that go parallel**, which here is `go-grpc` alone: AK core would
need `records x replay-factor x delay-ms` milliseconds to finish a backlog the sidecar arm clears in
seconds, and waiting that long to learn nothing new is not worth the wall clock.

## Depth

The client library itself is [`../README.md`](../README.md); the protocol is owned by
[`client-authoring-guide.md`](../../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../../parallel-consumer-proxy/docs/protocol-specification.md).
Findings from this wave, including anything the contract left open, are in
[`docs/inflight/clients/go.md`](../../../docs/inflight/clients/go.md).
