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
./mvnw test -P foreign-clients                      # everything, including the eight foreign clients
./mvnw test                                         # JVM only - the eight are not in the reactor at all
bin/build-client.sh --list                          # every language that script knows
bin/build-client.sh cpp --test                      # the container route
```

**One profile id is the switch**, and every way of turning it on turns all of it on:

- `-P foreign-clients`
- `-Dpc.foreignClients` (the profile's own activation)
- a `<activeProfiles>` entry in `~/.m2/settings.xml`, for a machine that always wants them

Without it the eight foreign modules are **excluded from the reactor entirely** - not present-and-idle.
That is deliberate: an ordinary Java build must not require Go, Node, Ruby, Rust, Python, .NET or
Docker, and a module that appears in the reactor while doing nothing reports `SUCCESS` for work that
never ran. The root pom prints one notice naming what is missing and how to include it.

The JVM clients (`java-*`, `kotlin`, `scala`) are always in the build - Maven compiles them directly
and they need no foreign toolchain. The two container languages (`cpp`, `swift`) need Docker, and say
so by exiting `2` rather than passing.

Nothing here is installed or deployed: `maven.install.skip` and `maven.deploy.skip` are true for the
whole aggregator.

### What a Maven lifecycle phase means in each language

The thing most likely to surprise someone arriving from Java: **`compile` does not mean "produce a
jar-shaped artifact" here.** It means "whatever that toolchain does to establish the sources are
valid", and the answer differs by language. Each module's pom names its own command in
`pc.foreign.build.args`; each module's README owns the detail.

| Language | `compile` runs | Where output lands |
|---|---|---|
| java, kotlin, scala | javac / kotlinc / scalac | `target/classes` |
| go | `go build ./...` | **nowhere in the module** - a shared content-addressed cache |
| rust | `cargo clippy --all-targets` | `target/` |
| python | editable install, then `python -m compileall` | `.venv/` plus caches |
| ruby | `bundle install`, then `ruby -c` per file | `vendor/` |
| typescript | `npm run build` (`tsc`) | `dist/` |
| dotnet | `dotnet build` | `bin/`, `obj/` |
| cpp, swift | a container build via `bin/build-client.sh` | `target/container/` |

**All eight fail `compile` on a syntax error.** Python and Ruby did not until their parse checks were
added - their "build" installs dependencies, which proves nothing about the source, and a phase that
reports success without having checked anything is the failure class
[`docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`](../docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md)
exists for.

### What `clean` removes, and what it must not

**Build output, never fetched dependencies.** `mvn clean` empties `target/`; it does not empty
`~/.m2`. The same line holds per language, and it is the mistake most easily made here: `node_modules`,
`.venv`, `vendor/bundle` and the cargo registry are that language's local repository.

**It is `maven-clean-plugin` filesets in each module's own pom, outside any profile.** Not a shell-out
to `go clean` or `npm run clean`, because **no shell-out can be bound to the `clean` phase**: both
`exec-maven-plugin:exec` and `maven-antrun-plugin:run` declare `requiresDependencyResolution=test`,
so binding either makes *cleaning* demand a resolvable dependency tree. That was measured, not assumed.

Four modules need no fileset, and each pom says why at length, because "nothing to clean" and "nobody
wired it" are indistinguishable otherwise:

- **cpp, swift** - export exactly one host path, `target/container`, which the default clean already
  removes. The container image and BuildKit layer cache are **shared with every worktree and agent on
  the machine** and must never be touched.
- **ruby** - emits nothing at all.
- **go** - its compiled output lives in a shared content-addressed cache outside the repo, so `clean`
  does **not** return Go to a from-scratch state. That is a real asymmetry with Java and it costs
  nothing:
  [`docs/inflight/bug-mvn-clean-does-not-clean-go-output.md`](../docs/inflight/bug-mvn-clean-does-not-clean-go-output.md)
  **owns the reasoning**, including why closing it is not worth the shared-cache cost.

### Scoping a build to one module

`-pl` alone fails on enforcer's `ReactorModuleConvergence`, because it builds a module without its
parents. Add `-am`, or `-Denforcer.skip=true`. The message names the parents rather than the fix -
[`docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md`](../docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md).

## Depth

[`client-authoring-guide.md`](../parallel-consumer-proxy/docs/client-authoring-guide.md) is what a
new client is written from, and
[`protocol-specification.md`](../parallel-consumer-proxy/docs/protocol-specification.md) is the
frozen contract. Per-language findings and divergences live in `docs/inflight/clients/`.
