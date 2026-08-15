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
its maturity and testing-evidence deferrals are lifted. Later waves: leases and heartbeats, the
manifest reconnect, worker death, terminal outcomes, the `Shutdown` drain, the demo and its
container, publishing, and the rest of the conformance suite - **most of which this client now
inherits rather than implements.**

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

## What it now inherits, including the parked P0

This is the compounding argument working as designed, so it is recorded rather than worked around:

- **A mid-session stream error used to park every executor with no way for a caller to learn the
  session died** - the P0 that was in `docs/inflight/parked-proxy-review-findings.md`. The transport
  half is fixed: `java-grpc` now stops hand-out on a stream error and reports the end, with its
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

## Not done, and owed to whoever picks the module up

- `src/docs/development/upstream-map.yaml` has no entry for this work - outside the wave's file
  scope.
- The demo, its container, and the `PLACE SERDE SETUP IN YOUR LANGUAGE HERE` extension point exist
  only as a comment in the README's example; wave (g) owns the real one.
- The sidecar spawn (`Sidecar.kt`) is still this module's own. It belongs in the Java lifecycle unit,
  and until it moves every JVM client writes it again.
