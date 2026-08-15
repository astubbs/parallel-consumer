<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - Go proxy client

A Go client for the Parallel Consumer language proxy: key-ordered concurrent Kafka processing from
Go, with the Java engine running as a sidecar child process and the user's function running in
ordinary goroutines.

**Wave one. Not for application use** - see [Status](#status). Its purpose is
[falsification](#why-this-module-exists) as much as function.

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
bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests   # the test spawns this module's harness
go test ./...                                               # or: ./mvnw -pl :parallel-consumer-proxy-client-go test -Dpc.foreignClients
```

The Maven wrapper runs the Go toolchain **only** under `-Dpc.foreignClients`; an ordinary
`bin/build.sh -am` builds this module's pom and runs no Go at all. The proxy module must be built
first either way: the test spawns the JVM conformance harness, and this module deliberately has no
Maven dependency on the engine, so nothing can order that build for you.
