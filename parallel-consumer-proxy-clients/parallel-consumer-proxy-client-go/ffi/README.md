# Vendoring Parallel Consumer inside the Go client

An experiment, not a shipping path. The Go client normally spawns the **sidecar** and talks to it
over gRPC. This asks whether the same engine can be linked *into* the Go process instead, as a
GraalVM `--shared` library, so a Go team gets one binary rather than a binary plus a JVM.

## What is already proven

`probe/main.go` is Probe 0. A Go process creates a GraalVM isolate, attaches a thread, and calls
into a shared library built from Parallel Consumer's own classes:

```
ok   isolate created and thread attached from Go
ok   pc_sum(3,4) = 7 - the C ABI works
     ProcessingOrder constants: reflective=-1 static=3
```

That last line is a finding, not decoration. Reading the enum **statically** works, so Parallel
Consumer really is linked in. Reading it **reflectively** returns -1, so the `--shared` build did
not inherit the reflection registration the executable build got. Anything the engine resolves by
reflection - and the Kafka client resolves its serialisers by reflection, from configuration
strings - needs explicit reachability metadata or it fails at *runtime*, not at build time.

## Why the C surface is a pull model

Core's entry point is `void poll(Consumer<PollContext<K,V>>)`. It takes a Java lambda, and a lambda
cannot cross a C ABI. Wrapping it would also run the caller's function while holding a core thread,
which is the one shape an FFI must avoid.

So the surface mirrors the **protocol** instead, which is already the right shape.
`rpc Session(stream ClientMessage) returns (stream ProxyMessage)` is bidirectional, and as a C ABI
that is two queues: the host pulls work frames and pushes verdict frames, on its own threads, at its
own pace. The bytes are the same protobuf frames the gRPC transport carries, so eleven clients'
encoding logic is reused rather than reinvented.

It also avoids the documented hole: nothing here calls back into Java from a foreign thread, which
GraalVM does not fully support ([oracle/graal#730](https://github.com/oracle/graal/issues/730)).

## Building

```bash
./build-shared-library.sh probe      # the Probe 0 library
./build-shared-library.sh session    # the pc_session_* surface
```

Needs GraalVM with `native-image` (`sdk install java 23-graal`, or set `GRAALVM_HOME`) and a JDK 17
for the Maven build (`PC_JDK17_HOME`). The script sets neither globally.

Then:

```bash
cd probe && go run .
```

## Prior art

- [`docs/inflight/perf-native-image-sidecar-works.md`](../../../docs/inflight/perf-native-image-sidecar-works.md) -
  the sidecar as a native **executable**, and the five-attempt build log. Every entry in this
  script's `--initialize-at-build-time` list is there because a build actually failed without it.
- [`docs/inflight/parked-a-c-client-and-the-ffi-question.md`](../../../docs/inflight/parked-a-c-client-and-the-ffi-question.md) -
  why an FFI client was parked, and the ladder of probes this is the bottom rung of.
