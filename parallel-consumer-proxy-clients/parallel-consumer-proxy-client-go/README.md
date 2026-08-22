<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - Go proxy client

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

A Go client for the Parallel Consumer language proxy: key-ordered concurrent Kafka processing from
Go, with the Java engine running as a sidecar child process and the user's function running in
ordinary goroutines. It speaks the frozen v1 protocol and nothing else - it never reads the proxy's
Java.

**Wave one.** Connect, configure, receive dispatch waves, run the user function, report per-record
outcomes, produce records back on success, shut down cleanly. See [Status](#status) for what is
absent.

## The shape

```
your process
├── your function (an ordinary Go func - the proxy never learns what it is)
├── this library
│   ├── admin      - spawns the sidecar, holds the ONE gRPC stream, owns the dispatch queue
│   └── executors  - goroutines, each: take record → run your function → report the outcome
└── sidecar proxy (child process) - runs Parallel Consumer, owns Kafka entirely
```

Goroutines are the whole of Go's part in that. There are no worker processes, so none of the
fork-safety machinery other languages need exists here - which is the point of the protocol
specifying the *shape* and leaving the *mechanism* to each language.

## Using it

```go
client, err := parallelconsumer.Open(ctx, parallelconsumer.Options{
    SidecarPath:     "/absolute/path/to/parallel-consumer-proxy",   // absolute, always
    Topics:          []string{"orders"},
    KafkaProperties: map[string]string{"bootstrap.servers": "localhost:9092"},
})
if err != nil {
    return err
}
defer client.Close()

err = client.Poll(ctx, func(ctx context.Context, r parallelconsumer.InboundRecord) (parallelconsumer.Outcome, error) {
    // PLACE SERDE SETUP IN YOUR LANGUAGE HERE - keys and values are bytes; deserialization is yours.
    if err := handle(r.Key, r.Value); err != nil {
        return parallelconsumer.Outcome{}, err // a returned error IS the failure outcome
    }
    return parallelconsumer.Succeed(), nil
})
```

Three things worth knowing before reading the API:

- **Errors are returned, not thrown.** A non-nil error from your function is a failure outcome, and
  its text rides the redelivery as the previous failure's reason. A panic is recovered and reported
  as a failure too, so one bad record cannot tear the stream down.
- **`Poll` returns immediately.** It starts the executors and the admin loop; `Done` is closed when
  the session **ends by any route** - you closed the client, the proxy completed the stream, the
  sidecar went away, or the stream faulted mid-session - and `Err` then says why, `nil` for a clean
  end. Observing the end never requires ending the client, so `select { case <-client.Done(): }` is
  the whole idiom. `Close` performs the client-initiated shutdown: stop handing records out, let
  executing records report, half-close the stream, reap the sidecar; call it after `Done` fires too,
  since that is what reaps the sidecar.
- **`SidecarPath` must be absolute.** The library never resolves the sidecar through `PATH` or a
  relative lookup: this process hands that binary your Kafka credentials, so which binary runs is
  security-relevant. It is launched directly and never through a shell, because a shell wrapper
  would hold the lifecycle pipe open and defeat the proxy's parent-death signal.

**The one error the protocol can raise at you** is the in-flight overflow: the proxy dispatching a
record while `MaxConcurrency` of them are already unresolved - queued plus executing - which is the
proxy exceeding the ceiling it declared itself. The library cancels the call and ends the session
with an error whose text starts `parallelconsumer: the proxy dispatched a record while N were
already unresolved`, naming the count, the declared `max_concurrency` and the offending record's
token. You read it from `Err()` after `Done()` fires. It is a protocol violation and never a load
condition, so the library never drops a record or grows the queue to absorb it.

`KafkaProperties` carries credentials. The library never logs the map, never echoes an entry of it
in an error, and never writes it to argv, an environment variable or a temp file - it travels the
stream and nowhere else. Hold your own code to the same rule.

## Status

Wave one implements exactly one path: connect → `Configure` → receive a `Dispatch` wave → run the
function → report success or failure → clean shutdown. It declares only the `dispatch` capability,
so a session negotiates nothing else and the proxy sends nothing else.

Not implemented, and not silently half-implemented - the capability is simply not declared:
heartbeats and the liveness lease, the `Manifest` reconnect and `Drop`, `WorkerDied`, `Terminal`
outcomes, the proxy-initiated `Shutdown` drain and the `Released` outcome. Also absent: the demo
and its container, packaging, and the rest of the conformance suite.

The current findings, including the ones the frozen documents could not answer, are in
[`docs/inflight/clients/go.md`](../../docs/inflight/clients/go.md).

## Generated code

The protobuf and gRPC stubs under `gen/` are **committed**, because Go has no codegen step at
`go get` time - a consumer must find them already there. Regenerate with:

```bash
bin/build.sh -pl :parallel-consumer-proxy-protocol -am -DskipTests   # once: downloads protoc
scripts/generate-proto.sh
```

Regenerating from an unchanged `.proto` must leave `git status` clean. The script's header explains
where each tool comes from and why it is assembled rather than assumed - notably that the frozen
`.proto` carries no `go_package` option, so the Go import path is supplied on the command line.

## Building and testing

```bash
./mvnw compile -Dpc.foreignClients -pl :parallel-consumer-proxy-client-go -am   # runs: go build ./...
./mvnw test    -Dpc.foreignClients -pl :parallel-consumer-proxy-client-go -am   # runs: go test ./...
go test ./...                                                                   # the shorter loop, once Maven has run
```

Those two phases are the whole of what Maven does here. This module is `packaging: pom` with four
`pc.foreign.*` properties naming the Go commands, and the `foreign-clients` profile in the clients
aggregator ([`../pom.xml`](../pom.xml)) binds them to `compile` and `test`; the profile also owns
whether this module is in the reactor at all, so an ordinary `bin/build.sh -am` runs no Go
whatsoever. Nothing binds to `clean` - see below.

- **`-am` is not optional for `compile` or `test`.** `-pl` alone fails the enforcer's
  `ReactorModuleConvergence` with a message about parent modules, which reads as a broken pom;
  [`docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md`](../../docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md)
  owns that. `./mvnw clean -P foreign-clients -pl :parallel-consumer-proxy-client-go` still needs
  the profile - without it the module is not in the reactor at all - but needs no `-am`, the clean
  lifecycle never reaching `validate` where the enforcer is bound.
- **Reaching the test phase needs `-Dpc.foreignClients`, not `-P foreign-clients`.** Both activate
  the module, but the `go-e2e-harness` profile - which is what pulls the proxy module into the
  reactor and writes `target/sidecar-classpath.txt` for the harness to read - activates on the
  *property*. Under `-P` alone the Go test fails hunting for the proxy's test jar. `-am` then builds
  the engine for you, so the older instruction to build the proxy by hand first is no longer
  required; it remains the fastest loop when you are only re-running `go test`. The flip side is
  worth knowing: `-P` leaves the engine out of the reactor - three modules instead of six, and no
  JDK 17 needed - which makes it the quicker loop when all you want is this module compiled.

### What a Java engineer will find surprising here

- **`compile` writes nothing into this directory.** `go build ./...` matches several packages, and
  for that form the toolchain compiles as a check and discards the objects. Measured: a build leaves
  `git status --ignored` on this module unchanged. The compiled output goes to Go's shared,
  content-addressed build cache (`go env GOCACHE`; 725MB on this machine) and the fetched modules to
  `GOMODCACHE`.
- **So `clean` does not put Go back to a from-scratch state** - a rebuild reuses cached compilation
  of our own code, which is not how `target/classes` behaves.
  [`docs/inflight/bug-mvn-clean-does-not-clean-go-output.md`](../../docs/inflight/bug-mvn-clean-does-not-clean-go-output.md)
  **owns that gap**: what is left behind, why neither route to a fix is worth taking, and the change
  that should make someone revisit it. What `clean` does remove is `target/`, holding
  `conformance-runner` and `sidecar-classpath.txt`, by Maven's default fileset with nothing added.
- **Never `go clean -cache` or `-modcache` to tidy up after a build.** Both are shared with every
  worktree, agent and unrelated Go project on the machine, which makes them this language's `~/.m2`
  - and `mvn clean` does not empty `~/.m2`.
- **A green test phase may have run nothing.** `go test` caches passing results, so a second
  `./mvnw test ...` over unchanged sources prints `ok ... (cached)` and executes no test. Surefire
  has no equivalent. `go test -count=1 ./...` forces the re-run when you need to see it happen.

The shared cross-language conformance suite drives this client's runner
(`cmd/conformance-runner`) through the same scenarios as every other language, asserting engine
state Go cannot see:

```bash
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=go
```

## Depth

[`client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md) own the
protocol; this file does not restate them.
