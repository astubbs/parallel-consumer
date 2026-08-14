# Client: Kotlin (astubbs#242)

Per-language working note for the Kotlin client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the Kotlin wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave one landed.** Connect, `Configure`, one `Dispatch` wave, the user's function, the
report with the token echoed verbatim, and a clean client-initiated shutdown - proven by one
end-to-end test against the real test-mode sidecar, which passed on its first run. The module is at
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-kotlin/`; its maturity and
testing-evidence deferrals are lifted. Later waves: leases and heartbeats, the manifest reconnect,
worker death, terminal outcomes, the `Shutdown` drain, the demo and its container, publishing, and
the rest of the conformance suite.

## What it wraps, and why it wraps neither Java transport

Kotlin is a JVM language, so the obvious wave-one shortcut was to wrap
`parallel-consumer-proxy-client-java-grpc` behind an idiomatic surface. **It does not**, on one
measurable ground and two design ones:

- **`-am` reach.** `java-grpc` test-depends on the engine, so a Kotlin module depending on it puts
  `parallel-consumer-proxy` in the reactor of `-pl :parallel-consumer-proxy-client-kotlin -am` - and
  `bin/build.sh` starts with `clean`, so the routine Kotlin build would delete the sidecar jar every
  other language's conformance test spawns. Reproduce with
  `./mvnw -pl :parallel-consumer-proxy-client-java-grpc -am validate`, which prints the reactor.
  Depending on `parallel-consumer-proxy-protocol` instead reaches core and the protocol module only.
- **The executors would be Java's.** Wrapping the Java client means its fixed thread pool runs the
  user's function, and a coroutine surface over it is a `runBlocking` per record. Here the executors
  *are* coroutines, so a suspending user function is native rather than bridged.
- **The two known specification defects are the client's to answer**, and a wrapper cannot answer
  them - the overflow response and the shutdown-time treatment of queued records are decided in the
  session code, which would have been Java's.

It does not depend on `parallel-consumer-proxy-client-java-api` either: that module defines the
reference *shape*, which this client mirrors, but importing its types would drag `Optional`,
builders and a boolean-flagged outcome into a surface whose whole purpose is to be Kotlin.

## The idiomatic decisions, for the wave sync

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
- **Cancellation is not a verdict.** `CancellationException` is re-thrown out of the
  function-to-outcome translation rather than becoming a `Failure`. This is a Kotlin-specific hazard
  with no counterpart in the reference implementation, and it is the kind of thing a language wave
  is for: swallowing it would fabricate an outcome for a record whose processing was cancelled and
  break structured concurrency for every caller above.
- **The dispatcher is injected** (defaulted to `Dispatchers.IO`), so the library's work lands on the
  application's own dispatcher rather than one it never mentioned.

## The two known specification defects, as implemented here

Both were handed to this wave already diagnosed (Python and Go hit them independently), so this is a
third data point rather than a discovery:

- **Queue overflow.** A gRPC *client* cannot set a status, so the guide's "fail the stream with
  `FAILED_PRECONDITION`" is unimplementable as written. This client cancels the call and raises
  `ProxyProtocolViolation` naming the ceiling that was exceeded.
- **`Released` on shutdown.** It is gated by the `shutdown` capability, which the harness does not
  negotiate, so queued records are discarded and left for the proxy to reclaim - and the code says
  where to send `Released` instead once the token is in the negotiated set.

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
turned it green. Three real findings were fixed rather than suppressed on the way in
(`UseCheckOrError` twice, `LongParameterList` justified).

## For whoever owns the CI row

Nothing here needs the workflow edited - the row's gate reads the maturity fragment, which this wave
lifted. Two facts the row's owner may want:

- The row's command (`test -pl :<module> -am -Dpc.foreignClients`) **needed a fix in this module's
  pom to work at all**, and it is in place: in a reactor run, a `test-jar` dependency on a module
  that has not reached the package phase resolves to a directory that is not its test output, so
  core's JUnit `TestExecutionListener` service file arrives without the listener it names and the
  surefire fork dies with `ServiceConfigurationError: Provider ... MyRunListener not found` before
  any test runs. The `kotlin-e2e-harness` profile now excludes the engine artifacts from the *test*
  classpath (they are only ever handed to the sidecar child process). Verified red before, green
  after. **Any other JVM client that borrows the Python module's harness-profile pattern will hit
  the same thing.**
- The detekt version and hash above match the row exactly; a bump has to move both copies.

## Not done, and owed to whoever picks the module up

- `src/docs/development/upstream-map.yaml` has no entry for this work - outside the wave's file
  scope.
- The demo, its container, and the `PLACE SERDE SETUP IN YOUR LANGUAGE HERE` extension point exist
  only as a comment in the README's example; wave (g) owns the real one.
- `poll` currently returns only on `close`, on the proxy completing the stream, or on a violation.
  There is no session-end signal for the case where the sidecar dies without completing the stream;
  the reconnect wave owns that, because that is where the distinction starts to matter.
