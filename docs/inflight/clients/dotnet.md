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
worker death, terminal outcomes, the shutdown drain, NuGet packaging, and the rest of the
conformance suite.

**The demo has landed** at `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-dotnet/demo/`
- two arms, both entry points, container included. What it found, what it diverges on, and what it
left open are in [The demo](#the-demo) below.

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

## The demo

`demo/run.sh` in this module, mirroring the contract in `parallel-consumer-proxy/demo/README.md`:
the same flags and defaults, the same `PC_DEMO_*` variables with the same precedence, the banner
first, then the fingerprint without the bootstrap address, then the same two tables, no latency
anywhere. Two arms - `AK core (Confluent.Kafka)`, serial, and `dotnet-grpc (this client)`, this
module's client library over a sidecar it spawns. The demo project joined the module's one
solution, so the ordinary `dotnet build` and the clients CI row keep it compiling under the same
analyzers-as-errors lint as the library.

### The divergence that stopped being one: an awaited timer, not a blocking sleep

The contract used to list C# among the languages where a blocking sleep is fine, naming Python and
TypeScript as the only exceptions. **For this client that list was wrong, and the demo diverged.**
The library's executors are `Task`s on the thread pool, so `Thread.Sleep` in the user's function
occupies a pool thread; at the contract's default `--concurrency 100` the pool would be injecting
replacement threads at roughly one a second and the sidecar arm would report the injection rate
rather than the engine's throughput. Both arms use `await Task.Delay(...)`, because the serial arm
is the denominator of every ratio and must not differ from its numerator by the wait primitive as
well as by the transport.

**Recorded here rather than edited into the contract, and the contract has since absorbed it.** The
argument this note made - that the predicate is the client's concurrency *shape* and not the
language's name - is now the rule as written: *is the client thread-per-record?* Six languages fail
it, C# among them for exactly this reason, and the note's two guesses at the neighbours (Kotlin's
coroutines, Swift's cooperative pool) are both on that list. So this module no longer diverges from
anything; the demo's code and README say so, and the reasoning is kept here because it is the
evidence behind a rule eleven languages now follow. Rust supplied the measurement that made it
undeniable, not this module - 10,341 msg/s through its blocking adapter against 3,518 with a raw
thread sleep.

### The reader-experience pass (the contract's "output a reader actually sees")

Applied here after the contract gained the section. Three changes, all in the demo, none of them
touching what is measured:

- **The banner is the first thing the demo prints**, before the broker is resolved and before the
  fingerprint, in the contract's exact shape with `.NET` as the language. It is printed before the
  argument parsing too, so a reader who mistypes a flag is still told what they were trying to run.
  **What still precedes it on the native path is `run.sh`'s own scaffolding** - the mode line, the
  Maven sidecar build, the `dotnet build`. That is the script, not the demo, and the demo is what
  the contract binds and what the conformance harness captures from the container; a script that
  has to build a JVM sidecar and a .NET project cannot make the banner byte one. The `dotnet build`
  summary block was silenced (`-consoleLoggerParameters:NoSummary`) so the banner is at least the
  line after the build, and build errors are unaffected by that flag.
- **Both arms are labelled with the role and the client**: `AK core (Confluent.Kafka)` and
  `dotnet-grpc (this client)`. The bare `dotnet-grpc` survives as a separate constant for the
  consumer group id - a label with spaces and brackets has no business in a Kafka group name or the
  metric names derived from it.
- **Two deterministic columns, `records` and `keys`**, appended after `vs AK core`. Appended rather
  than inserted for a concrete reason, below. Keys are counted over the key BYTES as base64 (a
  Kafka key is not text) with a `<null>` sentinel, in a `ConcurrentDictionary` in both arms - the
  serial arm does not need concurrency but must not reach a compared figure by a different route
  than the arm it is the denominator of. Measured at `--records 20 --partitions 2`: both arms
  report 20 records and 20 keys in the small replay, and the sidecar arm 40 and 40 in the big one,
  which is what the seeding (`key-{ordinal % 1000}`) predicts.

### The new output silently blinds `bin/ci-demo-conformance.sh`, and `bin/**` was not mine to fix

**Reproduced, not inferred**: the script's own `skeleton()` awk, run verbatim over this demo's real
capture, emits `DIAL`s, both `TITLE`s and both `HEADER`s - and **not one `ROW` line**. Before this
change the same capture produced three.

Two independent breakages, and only one of them is about the labels:

- **The `ROW` matcher is end-anchored on the ratio**: `... [0-9,-]+ [0-9.x-]+[[:space:]]*$`. Two more
  columns after `vs AK core` leave nothing for `$` to match, so the row is dropped **whatever the
  arm is called**. Every language adding the contract's `records` and `keys` hits this, including
  one that keeps `AK core` bare.
- **The arm-name class is `[A-Za-z][A-Za-z0-9 _-]*`**, which admits the space in `AK core` but not
  the brackets or the dot in `AK core (Confluent.Kafka)`. And `normalise_arms` maps
  `^ROW [a-z0-9]+-grpc$`, anchored, so `dotnet-grpc (this client)` would not normalise either.

**The failure mode is the dangerous one: silent.** Unmatched lines are dropped rather than flagged,
so every language emits a skeleton with no `ROW` lines, the skeletons still agree with each other,
and the drift check passes green while having stopped comparing the arms entirely - which is most of
what it was there to compare.

The `HEADER` matcher is *not* anchored at its end, which is why the two new columns went **after**
`vs AK core` rather than before: that ordering is the one that keeps the header recognised, and
every language should use it. Whoever owns `bin/**` needs to extend the `ROW` pattern by two columns,
widen its arm-name class, and widen `normalise_arms`, in the change that lands these labels. The
absolute assertions - dials echoed, no bootstrap address, no latency - are unaffected and still pass.

### Two findings against the product, both from running it

- **The sidecar's own log is unreachable from the client, in every language.** The proxy's `Main`
  prints `port: <n>` on stdout and that channel belongs to the client library, which drains and
  discards everything else on it. `logback-classic` is on the sidecar's runtime classpath with no
  configuration file anywhere in `parallel-consumer-proxy/src/main`, so its default
  `ConsoleAppender` writes to **stdout** - into the channel that is discarded. `ClientOptions`
  offers `SidecarErrorLog`, and it carries stderr, which is empty. Measured: a failing sidecar arm
  with `PC_DEMO_SIDECAR_LOG` set produced not one line. Either the sidecar should log to stderr, or
  the lifecycle channel's non-port lines should be offered to the client; today a sidecar that
  refuses a session can only be debugged by rebuilding it.
- **R48's reason-free rejection is correct and still costs an hour.** `Configure` failed with
  "constructing the session's Kafka clients and engine failed with
  `org.apache.kafka.common.KafkaException`" and nothing else, deliberately, because a Kafka
  `ConfigException` embeds property values and those may be credentials. The actual cause was the
  bootstrap address (below). **Naming the offending property KEY without its value would have
  settled it immediately** and gives nothing away - the keys are the client's own and travel in
  `Configure`; it is the values R48 protects. Worth putting to the protocol documents.

### The trap the next .NET reader will hit

Testcontainers for .NET returns its address as a URI: measured here,
`plaintext://127.0.0.1:62347/` - lower-cased scheme and **trailing slash**. librdkafka accepts that
string verbatim, so the AK core arm worked with it untouched; the Java client behind the sidecar
rejects it, and the trailing slash survives naive scheme-stripping. `DemoBroker.NormaliseBootstrap`
reduces each comma-separated entry to `host:port`. Two clients, two parsers, one address that
travels between them - the same shape will bite any language whose Kafka client is librdkafka-based
(Python, Ruby, C++, Go's confluent-kafka-go) if it starts its broker with that language's
Testcontainers.

### The container could not be built on an arm64 host, and that was a module bug

`Grpc.Tools` **2.71.0** ships a bundled `linux_arm64` `protoc` that segfaults - MSB6006, "exited
with code 139" - when MSBuild spawns it inside a container on an Apple Silicon machine. The whole
module, not just the demo, was unbuildable there.

Established with a control arm rather than by upgrading and hoping. In one container, one four-line
`.proto`, one project, **only the Grpc.Tools version changed**: 2.71.0 died at MSB6006, 2.83.0
generated the stubs. Two facts make it an environment bug rather than a bad argument: the same
`protoc`, run **by hand with MSBuild's exact command line** (captured with
`-consoleLoggerParameters:ShowCommandLine`), succeeded at *both* versions; and the same minimal
project built under `--platform linux/amd64` at 2.71.0. So it is specific to protoc-spawned-by-
MSBuild on linux/arm64 - which means **CI's amd64 runners never saw it**, and only a developer on
Apple Silicon would.

`GrpcVersion` is therefore 2.83.0 with the control written beside it in `Directory.Build.props`.
The bump is otherwise inert: `ProtobufVersion` stayed at 3.31.1, the library builds warning-free
under the same analyzers-as-errors lint, `dotnet format --verify-no-changes` is clean, and the
module's conformance test still passes against the real wire. **If it is ever lowered, re-run the
control.**

### What is open

- **No CI runs this demo.** `bin/ci-demo-test.sh` drives the Java demo through both entry points and
  is Java-only; extending it - or adding a sibling - was out of this session's ownership
  (`bin/**` was off limits). Until then the .NET demo has exactly the untested entry points that
  script exists to prevent, and the contract says both entry points being tested is part of it.
- **`bin/ci-demo-conformance.sh` needs its `ROW` and `normalise_arms` patterns widened** for the new
  arm labels, or its drift check silently stops comparing arms - see the section above for the two
  exact regexes. Same ownership boundary: `bin/**` is not this branch's.
- **No default-scale run has ever happened.** Every run was at `--records 20`, chosen to prove the
  machinery on a machine running ten agents at once. At that volume both arms are dominated by
  consumer-group join time and the ratios in the tables are meaningless; nothing in this branch
  should be read as a measurement. A run at the contract's defaults on an unloaded machine is
  outstanding.
- **The image is large and rebuilt from scratch on any source change.** It carries a .NET SDK, a
  JDK, a populated `/root/.m2` and the built reactor, because the sidecar classpath baked in at
  build time has to point at files the running container actually has - the same trade the Java
  demo's image documents. Nothing here has been optimised; a reader on a cold cache waits several
  minutes. Measured while verifying the reader-experience pass: the `COPY . .` layer is invalidated
  by **any** change in the repository, including one that touches no .NET file at all - so making a
  commit mid-build costs the whole Maven layer again, which was 480-520s on its own here, plus
  around 400s to export and load a 2.3GB image. **Do not edit the tree while `--docker` is
  building**, and budget well past a ten-minute timeout for a cold container run.
- **A killed `docker compose up` strands the network and the NEXT run fails, not the killed one.**
  Observed here: `network pc-dotnet-demo_default was found but has incorrect label
  com.docker.compose.network set to "default" (expected: "")`. It reads like a compose-file defect
  and is not one; `docker compose -f <demo>/docker-compose.yml down --remove-orphans` clears it.
  Worth knowing before anyone "fixes" the compose file in response.
- **`shared/JvmToolchain.cs` is compiled into two projects by `Compile Include` link** - the test
  harness and the demo both have to find a JVM, and neither is a library anyone references. If a
  third consumer appears, that is the moment to reconsider an assembly.

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
