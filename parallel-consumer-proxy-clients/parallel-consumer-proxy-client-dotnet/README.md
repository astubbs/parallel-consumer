<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Parallel Consumer - .NET proxy client

> **⚠️ EXPERIMENTAL - not for production use.** Everything in this module is new, unreleased and
> unproven: nothing is published to any package registry, the API may change without notice, and
> the v1 proxy protocol is frozen but has never carried production traffic. Build it from this
> checkout, read it, test it - do not depend on it. Tracking: astubbs#242.

A C# client for the Parallel Consumer language proxy: key-ordered concurrent Kafka processing from
.NET, with the Java engine running as a sidecar child process and the user's function running as an
ordinary `async` delegate. See [Status](#status). Its purpose is falsification as much as function:
whether an author with no access to the proxy's Java can build a working client from the frozen
documents alone.

## The shape

```
your process
├── your function (an ordinary delegate - the proxy never learns what it is)
├── this library
│   ├── admin      - spawns the sidecar, holds the ONE gRPC stream, owns the dispatch queue
│   └── executors  - tasks, each: take record → run your function → report the outcome
└── sidecar proxy (child process) - runs Parallel Consumer, owns Kafka entirely
```

Executors are tasks on the thread pool, so there are no worker processes and none of the
fork-safety machinery other languages need - which is the point of the protocol specifying the
*shape* and leaving the *mechanism* to each language.

## Using it

```csharp
await using var client = await ParallelConsumerClient.ConnectAsync(new ClientOptions
{
    SidecarPath = "/absolute/path/to/parallel-consumer-proxy",   // absolute, always
    Topics = new[] { "orders" },
    KafkaProperties = new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" },
});

await client.PollAsync(async (record, cancellationToken) =>
{
    // PLACE SERDE SETUP IN YOUR LANGUAGE HERE - keys and values are bytes; deserialization is yours.
    await HandleAsync(record.Key, record.Value, cancellationToken);
    return Outcome.Succeed();
}, stoppingToken);
```

Three things worth knowing before reading the API:

- **`PollAsync` returns the session, as a `Task`** - the shape this client picked for the
  guide's open question. That `Task` completes when the session ends **however it ends** (the proxy
  completed the stream, your token was cancelled, the client was disposed, the stream died) and
  faults with the session's first fatal error, so `await`ing it waits for the session and *not*
  `await`ing it runs the session in the background while still leaving you a handle to observe its
  end. There is no separate "is it finished" surface, because in C# the `Task` already is one. A
  proxy that breaks the protocol - a dispatch past the negotiated `max_concurrency` above all -
  cancels the call and faults that `Task` with `ProxyProtocolViolationException`, naming the count.
- **Failure has two spellings and one meaning.** Return `Outcome.Fail("why")` when failure is an
  expected result; throw when it is exceptional. Both are reported as a `Failure` outcome, and the
  text rides the redelivery as `InboundRecord.LastFailureReason`. One thrown exception cannot tear
  the stream down.
- **`SidecarPath` must be absolute.** The library never resolves the sidecar through `PATH` or a
  relative lookup: this process hands that binary your Kafka credentials, so which binary runs is
  security-relevant. It is launched directly and never through a shell, because a shell wrapper
  would hold the lifecycle pipe open and defeat the proxy's parent-death signal.

`KafkaProperties` carries credentials. The library never logs the map, never echoes an entry of it
in an exception message, and never writes it to argv, an environment variable or a temp file - it
travels the stream and nowhere else. Hold your own code to the same rule.

## Status

Wave one implements exactly one path: connect → `Configure` → receive `Dispatch` waves → run the
function → report success or failure, with records produced back on success → clean shutdown. It
declares only the `dispatch` capability, so a session negotiates nothing else and the proxy sends
nothing else.

Not implemented, and not silently half-implemented - the capability is simply not declared:
heartbeats and the liveness lease, the `Manifest` reconnect and `Drop`, `WorkerDied`, `Terminal`
outcomes, the proxy-initiated `Shutdown` drain and the `Released` outcome. Also absent: NuGet
packaging.

The current findings, including the ones the frozen documents could not answer, are in
[`docs/inflight/clients/dotnet.md`](../../docs/inflight/clients/dotnet.md).

## The demo

```bash
demo/run.sh
```

The same records through `Confluent.Kafka` one at a time, and through this library over a sidecar it
spawns - two throughput tables out, Docker the only requirement. Its own
[`demo/README.md`](demo/README.md) records what is specific to .NET, above all why the simulated work
is an awaited timer rather than the blocking sleep the shared contract permits in C#. The contract
every language's demo keeps is
[`parallel-consumer-proxy/demo/README.md`](../../parallel-consumer-proxy/demo/README.md).

## Generated code

The protobuf and gRPC stubs are **generated at build time and not committed**: `Grpc.Tools` runs
`protoc` and the C# gRPC plugin as an ordinary MSBuild step and carries its own `protoc`, so there
is nothing to install and nothing to regenerate by hand. The stubs land in `obj/` and their
namespace comes from the frozen `.proto`'s own `csharp_namespace` option, never from a command line
here.

That is the opposite of the Go client, which commits its stubs because `go get` has no codegen step.
Both are right for their ecosystem; the shared rule is that the one authority on where generated
code lands is the `.proto`.

## Building, testing and linting

Everything below runs from this directory. All three are local commands - none of them needs CI to
tell you the answer.

```bash
dotnet build                                                # build, WITH the lint (see below)
dotnet test                                                 # the end-to-end conformance test
dotnet format --verify-no-changes                           # formatting and style, no build needed
```

Through Maven, which is what the CI row does:

```bash
./mvnw -pl :parallel-consumer-proxy-client-dotnet -am package -Dpc.foreignClients
```

The Maven wrapper runs the .NET toolchain **only** under `-Dpc.foreignClients`; an ordinary
`bin/build.sh -am` builds this module's pom and runs no `dotnet` at all.

### In the Maven build

This module is `packaging: pom` with four `pc.foreign.*` properties naming `dotnet build` and
`dotnet test`, and the `foreign-clients` profile in the clients aggregator
([`../pom.xml`](../pom.xml)) binds them to `compile` and `test` and decides whether the module is in
the reactor at all. `clean` is `maven-clean-plugin` filesets in this module's own pom instead - no
shell-out can be bound to `clean`, and the aggregator's pom carries that argument.

- **The bare `build` and `test` arguments work because this directory holds exactly one solution
  file.** A second solution, or a stray project at this level, makes every `dotnet` command here
  ambiguous - including the CI row's `dotnet format analyzers`, which also takes no argument.
- **`package`, not `test`**: `ConformanceHarness.cs` looks for the proxy module's test jar as a
  *file*, and `test` stops one phase short of producing one. The `dotnet-e2e-harness` profile is
  what puts the proxy in the reactor, so the older instruction to build it by hand first is no
  longer a prerequisite - `bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests` is now just
  the fastest local loop. That profile activates on `-Dpc.foreignClients` and **not** on
  `-P foreign-clients`, which activates the module without the engine behind it. That has its uses:
  `-P` leaves the engine out of the reactor - three modules instead of six, and no JDK 17 needed -
  which makes it the quicker loop when all you want is `dotnet build`.
- **`-am` is not optional for `compile` or `test`.** `-pl` alone fails the enforcer's
  `ReactorModuleConvergence` with a message about parent modules, which reads as a broken pom;
  [`docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md`](../../docs/inflight/bug-scoping-a-build-to-one-client-module-fails.md)
  owns that. `./mvnw clean -P foreign-clients -pl :parallel-consumer-proxy-client-dotnet` still
  needs the profile - without it the module is not in the reactor at all - but needs no `-am`, the
  clean lifecycle never reaching `validate` where the enforcer is bound.

**What a Java engineer will find surprising:**

- **None of the output is under `target/`.** MSBuild writes `bin/` (the assemblies) and `obj/` (the
  intermediates, including the protobuf and gRPC stubs `Grpc.Tools` regenerates on every build)
  beside each of the three `.csproj` files. Measured: a compile leaves
  `src/…/bin/Debug/net8.0/*.dll` and `src/…/obj/Debug/net8.0/parallelconsumer/proxy/v1/Proxy.cs`.
- **`clean` therefore has to be told**, and it is - by directory *pattern* rather than by path, so a
  fourth project added to the solution is covered without a second edit. Verified: after
  `./mvnw clean`, no `bin/` or `obj/` remains anywhere in this module.
- **`~/.nuget/packages` is not touched**, being fetched dependencies rather than build output -
  this language's `~/.m2`, which `mvn clean` does not empty either. `dotnet clean` is not used
  because it needs the SDK present to delete files, and the point of the opt-in profile is that the
  SDK is usually absent.

### The lint is the build

There is no separate lint step to remember and no third-party analyzer package. The SDK's own
Roslyn analyzers run on every `dotnet build`, at `AnalysisLevel latest-recommended`, with
`TreatWarningsAsErrors` and nullable reference types on - so a real defect is a build failure on the
developer's own machine, not a CI comment. `Directory.Build.props` carries the settings and the
reasoning, including why `EnforceCodeStyleInBuild` is deliberately off.

Proven to fail, not assumed to: a dereference of a possibly-null reference (`CS8602`) and a
culture-dependent `string.StartsWith` (`CA1310`) were each introduced deliberately, failed the
build, and were reverted.

`dotnet format --verify-no-changes` is the stricter of the two formatting commands and passes
today; the CI row runs its `analyzers` subset.

## Requirements

- **.NET SDK 8.0 or newer.** The projects target `net8.0`, which is the CI row's pinned SDK; a
  machine with only a newer runtime still runs the tests (the test project rolls forward).
- **JDK 17**, for the sidecar the tests spawn. `JAVA_HOME` or `PC_PROXY_TEST_JAVA` names it.

## The shared conformance suite

It drives this client's runner (`tests/ConformanceRunner`) through the same scenarios as every other
language, asserting engine state this process cannot see:

```bash
./mvnw test -pl :parallel-consumer-proxy-conformance -am -Dpc.conformance.language=dotnet
```

## Depth

[`client-authoring-guide.md`](../../parallel-consumer-proxy/docs/client-authoring-guide.md) and
[`protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md) own the
protocol; this file does not restate them.
