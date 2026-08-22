# Client: .NET (astubbs#242)

Per-language working note for the .NET client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the .NET wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave one landed.** Connect, `Configure`, one `Dispatch` wave, the user's function, the
report, and a clean client-initiated shutdown, proven by one end-to-end test against the test-mode
sidecar. The module is at
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-dotnet/`; its maturity and
testing-evidence deferrals are lifted. Later waves: leases and heartbeats, the manifest reconnect,
worker death, terminal outcomes, the shutdown drain, the demo and its container, NuGet packaging,
and the rest of the conformance suite.

## The falsification result

**The premise held.** The client was written from `protocol-specification.md`,
`client-authoring-guide.md` and the frozen `.proto` alone. **No proxy Java source was read at any
point** - the only JVM-side artifact consulted was `TestModeMain`'s own usage text, which the guide
names as authoritative for its flags. The one-record test passed on its first run against the real
wire, and a negative control (returning `Outcome.Fail` instead of succeeding) produced the
documented redelivery with `attempt` incremented and the reason verbatim.

Three known specification defects were handed to this wave rather than rediscovered, so they cost
nothing here. Two of them - the queue-overflow status and the queue at close without `shutdown` -
were settled in the guide's wave-sync ledger while this wave was running, both the way this client
had already implemented them; the third, whether `poll` blocks, the ledger recorded as **open, for
wave two to answer in five languages at once**. That answer is the next section - and the ledger has
since settled it, on 2026-08-15, as *each language's own shape* with one binding property, which is
what the next section argues for.

## The decision the fan-out needs: does `poll(processor)` block?

The guide gives `ParallelConsumerClient.poll(processor)` and never says whether it returns once
processing has started or runs for the session's life. **.NET's answer is that the question does not
have to be settled the way the other languages settle it, because a `Task` is both answers at
once**, and that is the idiom rather than a dodge:

```csharp
Task PollAsync(RecordProcessor processor, CancellationToken cancellationToken = default);
```

The returned `Task` **is the session**: it completes when the session ends - the proxy completed the
stream, the token was cancelled, or the client was disposed - and it faults with the session's first
fatal error. So `await client.PollAsync(...)` is the blocking reading, and `var session =
client.PollAsync(...)` without an `await` is the background reading, chosen by the caller at the
call site. Nothing else is needed: no `Done` channel, no `Err()`, no `IsRunning` - in C# those are
what a `Task` already is, and adding them would be the un-idiomatic option.

**What the resolver should take from this:** the reference surface should say what `poll` *means*
(it owns the session until it ends), not what it *does with the calling thread*. A language whose
concurrency primitive can represent "this operation, still running" - `Task`, `Future`, `Promise`,
`async` anything - renders that as a value and lets the caller decide; a language without one picks
one behaviour and documents it. Go chose return-immediately with `Done()`/`Err()`; that is the same
meaning with a hand-built awaitable, so the two clients agree at the level that matters and differ
only in spelling.

**One thing the meaning does have to settle, and the documents do not:** whether the client owes the
"always read the stream" duty *before* the user calls `poll`. This surface connects and then polls,
so between `Configured` arriving and `PollAsync` being called, nothing reads the stream - and the
proxy may dispatch as soon as it has answered the handshake. Wave one documents "call `PollAsync`
promptly" and leaves the records in the transport's own buffer, which is inference, not
specification. Every language with a two-step surface has this window; the guide should say whether
the admin loop starts at handshake time or at poll time.

## Evidence on the defects handed to this wave

- **A gRPC client cannot fail a stream with `FAILED_PRECONDITION` - confirmed a third time, in C#.**
  `Grpc.Net.Client` offers no way for a client to set a status; the call can be cancelled, which the
  peer observes as `CANCELLED`. On a queue overflow this client cancels the call and faults the
  session with a `ProxyProtocolViolationException` naming the depth, the negotiated
  `max_concurrency` and the overflowing token, as the ledger's settled form requires.
- **`Released` on a session without `shutdown`: this client discards its queue**, which is the
  ledger's settled answer. The negotiated set is `["dispatch"]`, so sending `Released` would be the
  ordinary un-negotiated-message violation; the proxy reclaims the records as unheld, attempt counts
  unchanged.
- **The `csharp_namespace` option added at the freeze did its job.** The generated code landed in
  `Bz.Stub.ParallelConsumer.Proxy.Protocol.V1` with no command-line override anywhere in this
  module, which is exactly the outcome the option was added for - and the guide's new §0 now
  forbids the override outright. Nothing to fix; recorded because the Go note predicted the cost of
  *not* having the options for nine languages.

## Language quirks (local, not for the guide)

- **.NET needs neither route the guide's §0 offers for sourcing `protoc`.** `Grpc.Tools` is an
  ordinary NuGet package carrying its own `protoc`, its own C# plugin and the well-known-type
  descriptors, and it runs them as an MSBuild step - so neither the mise `protoc` nor the
  Maven-plus-unzip fallback is used here, and there is no generation script to keep. The module
  points at the frozen `.proto` by path and nothing else. §0's rule that placement comes from the
  `.proto` is obeyed as written; it is only the tooling half that does not apply.
- **The stubs are therefore NOT committed**, which is the opposite of the Go client's choice and
  right for both: `go get` has no codegen step, so a Go consumer must find the stubs already there,
  while a .NET consumer's own build generates them. Consequence: there is no
  "regenerating produces no diff" check to wire up here, because there is nothing committed to
  diff - the equivalent guarantee is that the build fails if the `.proto` cannot be found or read.
- **Failure has two spellings and one meaning.** `Outcome.Fail(reason)` for an expected failure and a
  thrown exception for an exceptional one, translated to the same `Failure` report in exactly one
  place. Go has one spelling because Go has one; C# would be un-idiomatic with either alone.
- **The dispatch queue is a bounded `Channel<T>` written with `TryWrite`.** The transport must never
  block on it, so `WriteAsync` is never called; a `false` return is the protocol violation, never
  backpressure. Executors take one record per pass rather than draining the channel, so several
  executors actually share a wave.

## Module-layout constraints the plan does not mention

- **Exactly one solution file at the module root, and no project file beside it.** The seeded pom
  runs `dotnet build` and `dotnet test` with no argument, and the CI row runs
  `dotnet format analyzers --verify-no-changes` with no argument; all three search the working
  directory and **error out if they find more than one candidate**. So the layout is
  `Bz.Stub.ParallelConsumer.Proxy.Client.sln` at the root with `src/` and `tests/` beneath it, and
  the seeded `ParallelConsumer.Proxy.Client.csproj` moved into `src/`. A later wave adding a demo
  project must add it to that solution rather than beside it.
- **Classic `.sln`, not `.slnx`.** SDK 10 creates the XML `.slnx` format by default and the CI row's
  pinned SDK 8.0.404 cannot read it; `dotnet new sln -f sln` is the flag that keeps both working.
- **The CI row pins `8.0.404` and the projects target `net8.0` to match.** A machine carrying only a
  newer runtime still runs the tests - `RollForward: Major` in the test project - so "build what CI
  builds" and "run it locally" do not conflict. The pin belongs to the row's owner, not to this
  wave; if it moves to .NET 10 the `TargetFramework` here can move with it.
- **A module-scoped `.editorconfig` was needed.** The repository's root file sets
  `insert_final_newline = false` and a two-space indent in its `[*]` block - chosen for Java and XML,
  and wrong for every C# tool. The module's own file overrides just those for `[*.cs]` and marks
  `obj/**` generated; it deliberately does not set `root = true`.

## The lint, and that it can fail

**C#'s mature analysis is the SDK's own**, so this module adds no third-party analyzer package. The
settings are in `Directory.Build.props`: `AnalysisLevel latest-recommended`,
`TreatWarningsAsErrors`, nullable reference types enabled. They run on an ordinary build, which is
the point - the lint is the build, and a defect is red on the developer's machine before it is red
anywhere else.

```bash
cd parallel-consumer-proxy-clients/parallel-consumer-proxy-client-dotnet
dotnet build                        # the lint: analyzers + nullability, as build errors
dotnet format --verify-no-changes   # formatting and style, no build needed
```

Both are green. **Proven to fail, not assumed to:** a `string?` dereference (`CS8602`) and a
culture-dependent `string.StartsWith` (`CA1310`) were each introduced deliberately, each failed the
build with the rule named, and were reverted.

`EnforceCodeStyleInBuild` is deliberately **off**. It promotes the `IDExxxx` formatting rules to
build diagnostics, and that set differs between SDK 8 and SDK 10 in both directions - which with
`TreatWarningsAsErrors` would make the CI row red for formatting on code the local build accepts.
Formatting is checked by `dotnet format`, correctness by the build.

**The CI row runs the `analyzers` subset** (`dotnet format analyzers --verify-no-changes`), which is
green here. `dotnet format --verify-no-changes` - whitespace, style and analyzers together - is
green too, and is the stricter command a local check should use.

## Plan defects (same as Go's, restated because they are still open)

- **The unit's verification command cannot work as written.**
  `./mvnw -pl :parallel-consumer-proxy-client-dotnet -am test` cannot build the conformance harness:
  it lives in `parallel-consumer-proxy`'s test jar, and this module deliberately has **no** Maven
  dependency on the engine, so `-am` does not pull it into the reactor. Something must build the
  proxy first (`bin/build.sh -pl :parallel-consumer-proxy -am -DskipTests`). Documented in this
  module's pom and README; the test fails with that command in its message rather than skipping.
- **The mock harness ignores the subscription**, so a wrong-topic run still passes and the obvious
  negative control does not exist. Confirmed here, exactly as the Go note describes it.

## Effort

Recorded honestly rather than backfilled: no budget was set for this module before it started, so
this falsifies nothing against ASM1 - it is a second data point for whoever records the real
distribution.

One agent session, roughly two hours wall clock. **1,478 lines of hand-written C#** across ten files
(1,182 library, 296 test), plus about 60 lines of MSBuild and 8,237 lines of generated stubs that
are never committed and never read.
