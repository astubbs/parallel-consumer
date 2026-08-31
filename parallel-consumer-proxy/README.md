<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The proxy sidecar - Parallel Consumer for languages that are not the JVM

> **⚠️ EXPERIMENTAL - not for production use.** Nothing here is published to any package registry,
> the API may change without notice, and none of it has carried production traffic. Build it from
> this checkout, read it, test it - do not depend on it. Tracking: astubbs#242, upstream
> confluentinc#154.

The intended end state is a process that runs Parallel Consumer, holds the Kafka connection, and
hands records to a worker in another language over the frozen
[v1 protocol](../parallel-consumer-proxy-protocol). The worker returns verdicts; the engine decides
everything else - shard selection, retry scheduling and offset tracking all stay here, which is what
keeps a client a facade rather than a second implementation of Parallel Consumer in every language.

## What is actually in this module today

**The packaging and runtime boundary, and nothing above it.** The sidecar builds, starts, binds,
admits or rejects a connection under its own rules, and shuts down cleanly when its parent dies.
There is no engine: a client that opens a session is answered `UNIMPLEMENTED` with a description
saying which piece is missing. That is the honest state of this build rather than a stub pretending
to work, and it is asserted by a test rather than left to be discovered.

- `Main` - the executable. It binds an ephemeral loopback port, prints `port: <n>` as the first line
  of stdout, and serves until its parent dies. **It parses no configuration and refuses arguments**:
  bootstrap servers, credentials, ordering, concurrency and subscription all arrive connect-time in
  `Configure` over the protocol, so a flag here would be a flag that did nothing.
- `lifecycle/ParentDeathWatchdog` - an orphaned sidecar holds a group membership on behalf of an
  application that no longer exists, so parent-death detection is correctness rather than
  housekeeping. EOF on the inherited pipe is the primary signal; a pid poll backs it up for the case
  where a wrapper process holds the write end open. **Launch the sidecar directly, never through a
  shell.**
- `transport/` - `ProxyServer` and the two things that make loopback-only safe:
  `AuthorityAllowlistInterceptor` and `SingleConnectionGuard`. **The connection is the session**; one
  worker per sidecar is a design constraint, not a limitation waiting to be lifted.

The transport's contract with what it hosts is `io.grpc.BindableService` and nothing more, so it
compiles against no engine type at all. `NoEngineSessionService` is what fills that slot until the
engine arrives, and `Main#sessionServiceFactory` is the one call site that changes when it does.

## Building it as a native executable

`bin/native-image-sidecar.sh` builds this module's `Main` into a GraalVM native executable and then
drives that executable through the whole lifecycle above - the port line, a real gRPC session
answered `UNIMPLEMENTED`, a clean exit on parent death, the socket released. It needs a GraalVM JDK;
without one it prints a banner saying so and exits 0, unless `PC_NATIVE_IMAGE_STRICT` is set, which
is what the `Native Image` workflow does on rows that install the toolchain themselves.

**Nothing in the ordinary build runs it.** There is no Maven profile and no plugin, so `./mvnw
install` and every other CI lane are unchanged on a machine that has never heard of GraalVM.

Why it matters here rather than as a packaging detail: **the proposition is that a Go, Python or Ruby
team is handed an executable, not a JVM dependency**. The engine's guarantees stop being a JVM
feature only if the artifact stops being a JVM artifact. What the script proves today is that the
*shell* survives closed-world compilation - the transport, the generated stubs and protobuf all still
work in the image. What it cannot yet prove is anything about an engine that is not in this module.

The recipe is one flag, `--no-fallback`, and the script's header says why that one is load-bearing
and why there is deliberately no reachability metadata here.

## Security posture

Loopback-only, and that is what removes the need for authentication: only the spawning process can
reach it, and one application per sidecar means no multi-tenancy. Those constraints are load-bearing
- a shared multi-tenant server would discard all of them at once and is a different product. A
non-loopback bind refuses to start unless the opt-in named in the refusal is set, and when it is set
the server warns with the full surface it is exposing.

## Not here yet

Connect-time configuration and the options mapping, the dispatch engine and its waves, the in-flight
registry, epochs, leases and heartbeats, reconnect with manifest reconciliation, the produce path,
the shutdown drain that waits on records held in a foreign process, capability negotiation, and the
client libraries. Those are reviewed as their own changes; astubbs#242 tracks the whole.

Also not here: the `--shared` C-ABI build that embeds the engine in the host process with no gRPC at
all. That is a different artifact from the executable above and is proven separately in
astubbs#340.

## Related

- [`parallel-consumer-proxy-protocol`](../parallel-consumer-proxy-protocol) - the wire contract, its
  specification, the client-authoring guide and the freeze gate.
- [`parallel-consumer-proxy-clients`](../parallel-consumer-proxy-clients) - the client module
  scaffolding.
