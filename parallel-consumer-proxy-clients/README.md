<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer proxy clients

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

## What this is

Key-ordered concurrent Kafka processing, from any language. The engine runs as a **sidecar process**
owning Kafka entirely; a client library in your language holds one gRPC stream to it, runs your
function on its own workers, and reports each record's outcome. The one exception is `java-direct`,
which binds the engine in-process and never speaks the protocol at all.

This aggregator holds every client, plus the shared cross-language conformance suite that drives
them all through the same scenarios.

## Each client is a miniature of the engine's own controller

Deliberately, part for part:

| Engine (core) | Client |
|---|---|
| Broker poller thread | Transport thread reading the stream |
| Control loop, never blocked | Transport thread, never blocked |
| Work manager holding in-flight records | Dispatch queue holding unresolved records |
| Worker pool | Executors |
| In-flight ceiling (`maxConcurrency × batchSize`) | Queue depth (negotiated `max_concurrency`) |
| Mailbox for completions | Reports back over the stream |

**The one thing the client deliberately doesn't mirror is ordering.** All shard selection, retry
scheduling and offset tracking stay in the engine. The client gets records and gives back verdicts -
which is what keeps it a facade rather than a second implementation of Parallel Consumer in every
language.

Both ends share one invariant: **the thread that moves work must never be the thread that waits on
work.** A client whose processor blocks instead of awaiting deadlocks its whole session, because the
single thread reading the wire is stuck inside user code. It is also why the ceiling counts
*unresolved* records rather than queued ones - it is the client's copy of the engine's in-flight
accounting, and two ends disagreeing about what "in flight" means make the check decorative rather
than able to catch a misbehaving proxy. Three clients had that subtly wrong.

## What every client does today

Connect and negotiate, configure, receive dispatch waves, run the user's function, report per-record
success or failure with the delivery token echoed verbatim, produce records back on success, and
shut down cleanly. Keys and values are bytes everywhere - the proxy never deserializes.

## What no client does yet

Leases and heartbeats, the manifest reconnect and `Drop`, worker-death reporting, terminal outcomes,
and the proxy-initiated shutdown drain.

**These are un-negotiated capabilities, not half-built features.** Every client declares exactly
`["dispatch"]` in its handshake, so the proxy never sends traffic the client cannot answer - which
is what keeps a wave-one client safe against an engine that already supports more. Declaring nothing
would be worse than declaring a subset: an empty list means "the whole v1 baseline" on the wire.

Also absent everywhere: published artifacts, demos and their containers.

## The clients

| Module | Runs your function as | In the shared suite | What is different about it |
|---|---|---|---|
| `java-api` | - | - | **Not a client**: the surface both Java transports implement and every other language mirrors. Dependency-free by design |
| `java-direct` | core's worker threads | `java-direct`, in-JVM binding | **Never speaks the protocol** - the engine in-process. Its wire is a function call, which makes it the control arm for the shared API |
| `java-grpc` | executor threads | `java-grpc`, in-JVM binding | **Owns no sidecar spawn** - connecting to a given port is the transport's job, starting the process is the lifecycle unit's |
| `java-harness` | - | - | **Not a client**: a build-graph module, the one JVM place that depends on the engine |
| `kotlin` | suspending lambdas | `kotlin`, spawned runner | Wraps `java-grpc` rather than reimplementing the session; owns the spawn, which is what keeps that path covered |
| `scala` | `Future`s (Scala 2.13) | `scala`, spawned runner | Wraps `java-grpc`, same as Kotlin. `noVerdict` is its way of saying "I did not run this record" |
| `go` | goroutines | `go` | Commits its generated stubs, because `go get` has no codegen step |
| `python` | worker **processes** | `python` | The GIL is not the ceiling. The pool is forked before any gRPC channel exists |
| `typescript` | concurrent async invocations | `typescript` | One event loop, not `worker_threads` - so synchronous CPU work blocks the transport |
| `rust` | tokio tasks | `rust` | Its suite runs on a current-thread runtime, so blocking instead of awaiting deadlocks rather than hiding |
| `ruby` | threads | `ruby` | On MRI a CPU-bound block gets concurrency but not parallelism |
| `dotnet` | tasks | `dotnet` | Stubs are generated at build time by `Grpc.Tools`, not committed |
| `cpp` | `std::thread`s | `cpp` | **Builds in a container** - gRPC arrives as system dev packages, so there is no host toolchain |
| `swift` | child tasks of a task group | `swift` | **Builds in a container** too - Swift.org publishes no Debian toolchain. Swift 6 strict concurrency is its static analysis, so a data race is a compile error |

Every module has its own README with the detail behind its row.

**Your language is not in that table?** Open an issue on
[the tracker](https://github.com/astubbs/parallel-consumer/issues) and we will give it a go. The set
above was chosen by judgement, and one person asking is better evidence of demand than any amount of
reasoning about who might want it.

## The shared conformance suite

[`parallel-consumer-proxy-conformance`](parallel-consumer-proxy-conformance/README.md) drives each
language's runner through one scenario set, written once in Java, and asserts engine-side truth the
client cannot see. **Thirteen bindings answer the same four scenarios today**: the engine itself
driven by a plain Java function, plus every client in the table above.

The engine binding is the **control arm** and runs in every selection: a scenario red against a
plain Java function is a wrong scenario, not a broken client.

Four scenarios rather than the eleven the authoring guide names, because the mock harness serves
four - the rest are un-negotiated, not failing.

```bash
./mvnw test -pl :parallel-consumer-proxy-conformance -am                          # every binding
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=core   # control arm alone
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=go     # one language + the control arm
```

A language name that is not registered **fails** rather than selecting nothing, and an absent or
broken runner fails rather than skipping.

## Building

```bash
./mvnw test -pl :parallel-consumer-proxy-client-go -am -Dpc.foreignClients   # one client, its own toolchain
bin/build-client.sh --list                                                   # every language this script knows
bin/build-client.sh cpp --test                                               # the container route
```

**`-Dpc.foreignClients` is the switch**, and its absence is not a silent skip of a client's tests -
it is the reason an ordinary reactor build works on a machine with no Go, Node, Ruby, Rust, Python
or .NET at all. Without it, the non-JVM modules build their pom and start no foreign toolchain.

The JVM clients (`java-*`, `kotlin`, `scala`) need no foreign toolchain, and Maven builds them
directly; the same flag still activates Kotlin's and Scala's harness profile, which is what supplies
the classpath their sidecar tests spawn. The two container languages (`cpp`, `swift`) need Docker,
and say so by exiting `2` rather than passing.

Nothing in this tree is installed or deployed: `maven.install.skip` and `maven.deploy.skip` are true
for the whole aggregator.

## Depth

[`client-authoring-guide.md`](../parallel-consumer-proxy/docs/client-authoring-guide.md) is what a
new client is written from, and
[`protocol-specification.md`](../parallel-consumer-proxy/docs/protocol-specification.md) is the
frozen contract. Per-language findings and divergences live in `docs/inflight/clients/`.
