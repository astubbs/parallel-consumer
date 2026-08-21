# Client: Kotlin (astubbs#242)

Per-language working note for the Kotlin client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the Kotlin wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave one landed, and has since been reworked to wrap the Java client.** Connect,
`Configure`, one `Dispatch` wave, the user's function, the report with the token echoed verbatim,
and a clean client-initiated shutdown - proven by one end-to-end test against the real test-mode
sidecar. The module is at `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-kotlin/`;
its maturity and testing-evidence deferrals are lifted. **The demo and its container have since
landed** - see "The demo" below for what is open about them. Later waves: leases and heartbeats, the
manifest reconnect, worker death, terminal outcomes, the `Shutdown` drain, publishing, and the rest
of the conformance suite - **most of which this client now inherits rather than implements.**

## It wraps `java-grpc`, and the two reasons it did not are fixed at source

Wave one implemented its own gRPC session and gave three reasons. Two were real, and the answer to
both was to fix the thing that made wrapping unattractive rather than to keep a second JVM session
implementation. The third was wrong.

- **The `-am` reach (real, fixed in the build).** `java-grpc` test-depended on the engine, so a
  module depending on it put `parallel-consumer-proxy` in the reactor of `-pl <that module> -am` -
  and `bin/build.sh` opens with `clean`, so the routine build of a wrapper deleted the sidecar jar
  every other language's conformance test spawns. The engine dependency now lives in
  `parallel-consumer-proxy-client-java-harness`, a leaf module nothing wraps, which also runs the
  gRPC transport's harness-backed conformance suite. Measure rather than assume:
  `./mvnw -pl :parallel-consumer-proxy-client-kotlin -am validate` prints the reactor, and
  `parallel-consumer-proxy` is not in it.
- **A `runBlocking` per record (real, fixed in the API).** `java-api`'s processor returned an
  `Outcome` synchronously, so a coroutine surface over it would have parked a thread per record.
  `java-api` now also has `AsyncRecordProcessor`, returning a `CompletionStage<Outcome>`, and
  `ParallelConsumerClient.pollAsync`. A coroutine completes the stage and no thread waits - so
  `poll` is still a `suspend fun` that suspends for the life of the session, and is now backed by
  the bridge rather than by a blocked thread.
- **"A wrapper cannot answer the two specification defects" (void).** `java-grpc` answers both, and
  inheriting its answers is the entire point.

**The compounding argument is the reason this matters more than the module's own size.** The JVM was
heading for three session implementations - java-grpc, Kotlin, Scala - so every session defect would
have needed fixing three times. It is one.

### What that cost, honestly

- **The module got smaller, but by less than the deletions suggest.** Main sources went from 927
  lines to 690 (**-26%**); ignoring comments and blanks, 513 code lines to 323 (**-37%**). Two files
  are gone outright - `ProxyStream.kt` (the session) and `Wire.kt` (the protobuf mapping, whose
  tests were a second copy of `WireMappingTest`) - and `ParallelConsumerClient.kt` grew, because
  what is left in it is the part that needed explaining.
- **`Sidecar.kt` is 152 of the remaining 690 lines and has no Java counterpart.** Spawning the
  sidecar is the lifecycle unit's job in the plan, and `java-grpc` still connects to a port it is
  given. When that unit lands, this module deletes another ~150 lines and Scala never writes them.
- **`Bridge.kt` is the standing cost of being a Kotlin client**, and it is one file on purpose so
  the cost stays countable. It is the only translation the module performs.
- **One genuine loss of purity:** `Session` is now `java-grpc`'s `NegotiatedSession`, so its
  accessors read `session.capabilities()` rather than `session.capabilities`. A Kotlin respelling
  would have bought the property syntax and nothing else.

## The idiomatic decisions, for the wave sync

All of wave one's calls survive the rework; two of them are now *enforced* rather than described.

- **`poll` suspends for the life of the session.** It does not block a thread and does not return
  when processing starts. Go chose "returns immediately, with `Done`/`Err`"; Kotlin's structured
  concurrency makes the opposite choice the idiomatic one - a function that starts background work
  and returns leaves coroutines nobody owns, and suspending puts the session in the caller's scope
  where cancellation, `withTimeout` and exception propagation all work normally. **Both answers are
  consistent with the same reference surface, which is the evidence that the specification has to
  settle the question rather than leave it.**
- **Nullability replaces `Optional`**, per the plan's own note about Kotlin and Swift, and one
  further step: `last_failure_at` and `last_failure_reason` become a single nullable
  `PreviousFailure`, because two independent nullables let a caller ask a question the wire cannot
  answer (a reason with no time).
- **`Outcome` is a sealed interface**, so `when` over it is exhaustive - the reference surface's
  "closed two-armed value" expressed as a closed type rather than a boolean field.
- **No builder.** Default and named arguments say "unset means take the engine's default" once, at
  the declaration.
- **Cancellation is not a verdict**, and this is where wrapping made the rule *sharper*. The
  transport turns an exceptionally-completed stage into a failure report, so a cancelled record must
  be answered with a stage that never completes - which `AsyncRecordProcessor` defines as "no
  verdict for this record". `NoVerdictIsInventedTest` asserts both silent paths and one control,
  because a fabricated verdict is indistinguishable from a real one from every side.
- **`ProcessingOrder` is imported, not respelled.** A three-constant enum is already the same thing
  in both languages. The rule for `Bridge.kt`: respell only where Kotlin genuinely says it better.
- **The dispatcher is injected** (defaulted to `Dispatchers.IO`), so the library's work lands on the
  application's own dispatcher rather than one it never mentioned.

## What it now inherits, including the P0 that was parked

This is the compounding argument working as designed, so it is recorded rather than worked around:

- **A mid-session stream error used to park every executor with no way for a caller to learn the
  session died** - the P0 the spike+freeze review parked. The transport half is fixed by `061324e20`:
  `java-grpc` now stops hand-out on a stream error and reports the end, with its
  cause, through `ParallelConsumerClient.sessionEnd()`. **The Kotlin half is open**: `poll` still
  returns on `close` or cancellation only, because it awaits its own `ended` and nothing joins that
  to the transport's `sessionEnd()`. Wiring the two together is this client's remaining work, and it
  is now a wiring job rather than a defect with nowhere to report to.
- **The two known specification defects** - the unimplementable `FAILED_PRECONDITION` overflow
  response, and `Released` on shutdown being gated by an un-negotiated `shutdown` capability -
  are answered in `java-grpc` and by `startRecord`'s "hand-out has stopped" path respectively.
- **A strictness improvement flowed the other way.** Wave one found that a `Configured` missing
  `max_concurrency` or `executor_count` is a violation and never an "unlimited"; `java-grpc` had
  been defaulting it to 1, which silently serialises a client that asked for concurrency. That check
  now lives in `WireMapping.toNegotiatedSession`, so every language wrapping the transport gets it.

## Static analysis: detekt, and how it is run locally

```bash
parallel-consumer-proxy-clients/parallel-consumer-proxy-client-kotlin/detekt.sh
```

detekt **1.23.7**, fetched from Maven Central and sha256-verified, run as
`--input src --build-upon-default-config` from the module directory - byte-for-byte the version and
flags the module's CI row uses, so local and CI cannot disagree. There is deliberately **no
`detekt.yml`**: the CI row passes no `--config`, so a local config file would make local green mean
nothing. Rules that are genuinely wrong for a piece of code are answered with an `@Suppress` and a
reason at the site (there is one, `LongParameterList` on `InboundRecord`).

**Proven able to fail**, not assumed: adding an unused private function to
`ParallelConsumerClient.kt` turned it red (`UnusedPrivateMember`, plus `MagicNumber`) and reverting
turned it green. It also earned its place during the rework - `RethrowCaughtException` on a
catch-and-rethrow of `CancellationException` was the prompt to replace a comment with
`invokeOnCompletion` and a test.

## For whoever owns the CI row

Nothing here needs the workflow edited - the row's gate reads the maturity fragment, which this wave
lifted. Two facts the row's owner may want:

- The row's command (`test -pl :<module> -am -Dpc.foreignClients`) **needs the harness-lane stanza
  in this module's pom**: in a reactor run, a `test-jar` dependency on a module that has not reached
  the package phase resolves to a directory that is not its test output, so core's JUnit
  `TestExecutionListener` service file arrives without the listener it names and the surefire fork
  dies with `ServiceConfigurationError: Provider ... MyRunListener not found` before any test runs.
  The `kotlin-e2e-harness` profile excludes the engine artifacts from the *test* classpath (they are
  only ever handed to the sidecar child process). Verified red before, green after. **Any other JVM
  client that borrows this harness-profile pattern will hit the same thing** - and it now reaches
  those three jars through the single `parallel-consumer-proxy-client-java-harness` dependency.
- The detekt version and hash above match the row exactly; a bump has to move both copies.

## The conformance runner, and the two facts that made it cheap

**Done.** Kotlin answers all four shared scenarios, as a spawned child process like every other language.
The predictions in this section's earlier form both held: it was cheap *because* the client wraps
`java-grpc` - the runner is a `main()` over the same client an application holds, not a session
implementation - and the registry did want a resolved classpath rather than a two-line wrapper.

- **Where it lives.** `src/test/kotlin/.../coroutines/conformance/ConformanceRunner.kt`, launched by
  `scripts/conformance-runner`. The test tree, not `src/main`: it is a program that uses the client, and
  `-Xexplicit-api=strict` guards what the published surface is.
- **The classpath file is written by the DEFAULT build**, not the harness profile: a
  `conformance-classpath` execution of `dependency:build-classpath` in this module's pom. The runner needs
  no engine at all - it spawns the sidecar *shim* the suite hands it on the sidecar flag, and the engine
  lives in the suite's own JVM - so the harness lane's three jars are beside the point here.
- **Its registry entry carries no build command**, and that is the one real difference from every other
  language. Kotlin's toolchain is the Maven build already running, so the conformance module test-depends
  on this module and the reactor compiles the runner before a scenario starts. A nested `mvn` would rewrite
  the class directories of the JVM executing the suite while it ran. **Scala will want exactly this
  arrangement**, and it is one pom stanza plus one wrapper.
- **The wrapper prefers `$JAVA_HOME/bin/java`** over `PATH`, because this repository's JDK 17 comes from a
  version manager and is deliberately not on `PATH`; the surefire fork inherits `JAVA_HOME`, so the JVM that
  ran Maven is the one that runs the runner.
- **Proven red before green**, per scenario: a success reported as a failure, silence reported as a success,
  a failure reason that is not the contract's literal, and a mutex around the whole processor. Each turned
  exactly its own scenario's row red and left the rest green.

**`java-direct` and `java-grpc` did NOT get runners, and that is deliberate** - they are driven as client
objects by `JvmClientBindings` in the conformance module, whose README section *A JVM client is a binding,
not a subprocess* owns the reasoning. Kotlin is the JVM client that keeps the spawn path covered, because
`Sidecar.kt` is the only JVM spawn there is.

## The demo, and the three things still open about it

`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-kotlin/demo/` - `run.sh`,
`Dockerfile`, `docker-compose.yml`, `README.md` and Kotlin sources under `demo/src`. Two arms, per
the shared contract: AK core, and Kotlin over the sidecar through this module's own client. It
keeps the contract's flags, `PC_DEMO_*` variables, precedence, defaults, fingerprint and two tables.

**It is not a Maven module, and that was forced.** A new module needs a line in the clients
aggregator pom, which the parallel language waves all share. So the demo lives inside this module,
compiled by a `kotlin-demo` profile activated by `-Dpc.kotlinDemo`, into `target/test-classes` -
the module's published surface is guarded by `-Xexplicit-api=strict` and a demo is not part of it.
**Scala should copy this arrangement rather than re-deciding it**: it is one profile, one extra
Kotlin/Scala compile execution, and one `build-classpath` execution.

- **No CI entry-point test.** `bin/ci-demo-test.sh` runs the *Java* demo through both entry points
  on every pull request; nothing does that for Kotlin, so the contract's "both entry points are
  tested" clause is unmet here. That script is `bin/**` and shared, so extending it belongs to
  whoever owns the fan-out's CI, not to this module. **This is the largest open item.**
- **The engine reactor edge is now reachable, on purpose.** `-Dpc.kotlinDemo` puts
  `parallel-consumer-proxy` in this module's reactor, because the demo hands the spawned sidecar
  its own classpath. The module's standing invariant - `./mvnw -pl
  :parallel-consumer-proxy-client-kotlin -am validate` must not print `parallel-consumer-proxy` -
  still holds for the DEFAULT lane, and was re-measured after the profile was added. Any future
  change here must re-measure it rather than assume it.
- **The demo profile also widens `conformance-classpath.txt`**, because that execution takes test
  scope and the profile adds the harness to it. Harmless - the conformance runner never names the
  engine artifacts, exactly as in the harness lane - but worth knowing before wondering why the
  file changes size under `-Dpc.kotlinDemo`.

### The one divergence from the shared contract, and why

**The simulated work is `delay`, not `Thread.sleep`.** The contract's rule is "the simulated work
must use that language's non-occupying wait", and its list then says a blocking sleep is fine in
Kotlin. The list is written for a thread-per-record client; this one runs each record as a coroutine
on `Dispatchers.IO`, whose default parallelism is 64. A blocking sleep there caps in-flight records
at 64 however high `--concurrency` is set, while the fingerprint keeps printing the number the
reader asked for - a throughput figure reported against settings that did not apply, which is the
exact failure the fingerprint exists to prevent.

Measured, on a heavily loaded machine (load average 23 on 12 cores, ten agents): at
`--records 4000 --delay-ms 50 --concurrency 200 --replay-factor 1` the sidecar arm finished in
**2.7s**, which is 200 record-seconds of work in 2.7s, so about **74 records in flight** - above the
64 ceiling a blocking sleep could ever reach. It is an inference from one arm rather than a two-arm
controlled experiment, and the control arm was not run; contention can only push the in-flight
figure *down*, never above the thread ceiling, so the direction survives the load even though the
absolute rate does not.

**This may be a defect in the shared contract rather than a Kotlin exception.** The contract names
only Python and TypeScript as languages where a blocking sleep is not the non-occupying wait; the
real predicate is not the language but whether the client's execution model is thread-per-record.
Any coroutine-, fiber- or async-native client - Kotlin here, and plausibly Swift's structured
concurrency and C#'s `async` - is in the same position. Recorded here rather than edited into
`parallel-consumer-proxy/demo/README.md`, which is the fan-out's shared contract file and not this
wave's to change.

### What was actually run, and under what load

All on the loaded box described above. **Absolute throughput here is contended and should be
re-measured on an idle machine before it is quoted anywhere.**

- Native, `--records 20 --delay-ms 1 --concurrency 4 --partitions 2 --replay-factor 1`: both arms
  completed, tables printed, exit 0.
- Native, **no arguments at all** - the double-click case, and the one that has broken before: both
  replays ran and exit was 0. AK core 2000 records in 6.7s; kotlin-sidecar 2000 in 1.3s, then 40000
  in 3.5s.
- Container, `demo/run.sh --docker --records 20 ...`: image built from the repository context
  (9.17MB uploaded, so `.dockerignore` is doing its job), broker came up as a compose sibling, the
  sidecar was spawned inside the demo container with no host Docker socket anywhere, both arms
  completed, exit 0.
- Container, **`docker compose up` with no arguments and no environment** - the second documented
  entry point, and the one a reader who has never seen this repository types: both replays ran,
  demo container exited 0. AK core 2000 in 7.6s; kotlin-sidecar 2000 in 1.1s, then 40000 in 5.0s.
- Argument handling, without a broker: an unknown flag exits **2** with the usage text rather than
  reporting numbers for settings nobody asked for; and `PC_DEMO_RECORDS=7 PC_DEMO_DELAY_MS=9
  --records 11` printed `records = 11, delayMs = 9`, which is the contract's precedence - flags beat
  the environment beats the defaults - and no bootstrap address anywhere in the fingerprint.

### A sighting that is not the Kotlin client's, recorded here only because of file ownership

Running this module's default lane (`./mvnw -pl :parallel-consumer-proxy-client-kotlin -am test`)
during the demo wave, on a machine at **load average 83 of 12 cores**, failed one test in
`parallel-consumer-core`:

```
BlockedThreadAsserterTest.functionThatReturnsOnItsOwnScheduleIsRejected
  BlockedThreadAsserter accepted a function that returns on a timer rather than on the unblocker,
  but it must have rejected it
```

**Contention, established by control rather than asserted**: the same class re-run on its own
passed 7/7 immediately afterwards, and the Kotlin module's own tests are green (20/20). This is the
helper's *self-test* - it feeds the asserter a function that returns on its own timer and expects
rejection - so under enough load the timer-returning function is late enough to look like a genuine
unblock.

`docs/inflight/test-untracked-ci-flakes.md` already carries `BlockedThreadAsserter` prior art, but
for a **different** test and a different signature (`assertUnblocksAfter` measuring a window two
milliseconds short, owned by astubbs#262). This one is a new signature on the same helper.
**It belongs in that file, not this one** - it is recorded here because the Kotlin demo wave owns
only `clients/kotlin.md`, and the fan-out's integrator should move it.
