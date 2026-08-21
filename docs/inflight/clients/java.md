# Client: Java (astubbs#242)

Per-language working note for the Java client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the Java wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave not started.** The module skeleton is seeded at
`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/ (api, direct and grpc modules)`; its maturity and testing-evidence deferrals live in
`docs/data/module-maturity.d/` and `docs/data/testing-evidence.d/` under the module's artifact
name, and the wave that starts this client lifts them from its own fragment files.

## Two changes the Kotlin wave made here, which the Java wave inherits

Kotlin was reworked to *wrap* `java-grpc` rather than implement a second JVM session, and the two
things that had made wrapping unattractive were fixed at source rather than routed around. Both are
in place already; the Java wave should build on them rather than rediscover the reasoning.

- **A fourth module, `parallel-consumer-proxy-client-java-harness`.** A test-scope dependency is not
  transitive on the classpath but it *is* a reactor edge, so while `java-grpc` test-depended on
  `parallel-consumer-proxy`, `-pl <anything wrapping it> -am` built the engine - and `bin/build.sh`
  opens with `clean`, which deleted the sidecar jar other languages' tests spawn. The engine
  dependency and the gRPC transport's harness-backed conformance run now live in that leaf module,
  which nothing wraps. Its pom carries the reasoning and the reactor-list measurement. **Do not put
  an engine dependency back into a transport module**, in any scope; check what a new dependency
  does to `./mvnw -pl :parallel-consumer-proxy-client-kotlin -am validate` first. Its compile-scope
  dependencies are also the one place the harness classpath is declared, so a JVM client needing to
  spawn the sidecar declares that one module rather than restating three.
- **An asynchronous processor on the shared surface.** `AsyncRecordProcessor` returns
  `CompletionStage<Outcome>` and `ParallelConsumerClient.pollAsync` takes it; `Outcomes.asAsync`
  expresses the synchronous form in terms of it, so each transport has one executor loop and not
  two. Three parts of the contract are new and are stated on `AsyncRecordProcessor`: return
  promptly, concurrency is bounded by the engine's in-flight ceiling rather than by the executor
  count, and **a stage that never completes reports nothing** - which is how a client says it has no
  verdict for a record it did not run, instead of inventing one. `java-grpc` also gained `connect()`
  returning the `NegotiatedSession` before polling starts, because that is what a wrapper reports to
  its own user.

The direct transport implements `pollAsync` by joining the stage on core's worker, and says so: core
has no asynchronous user function underneath, so there is no thread to save there. That is a
property of the degenerate transport, not of the form.

## Both Java transports are now in the shared conformance suite, as bindings rather than runners

`java-direct` and `java-grpc` answer all four of the shared cross-language scenarios, registered in
`JvmClientBindings.java` in the conformance module rather than in `LanguageRunners.java`. The
conformance module's README section *A JVM client is a binding, not a subprocess* **owns the
reasoning**; what a later Java wave needs to know is the shape and what it constrains.

- **The prescription is one implementation, not three.** `PrescribedRun.java` carries the runner
  contract in-JVM - the four behaviour tokens, the fixed failure literal, one observation per
  delivery, an exit status as the verdict - and the control arm, `java-direct` and `java-grpc` all
  execute it. Do not add a per-transport branch to it; a binding that could take a shortcut there
  would be conforming to a contract nobody else implements.
- **The harness grew a third lane for `java-direct`, `ProxyHarness.startEmbeddedClient`.** Its other
  two lanes both own the engine; a client that constructs Parallel Consumer for itself fits neither,
  and what it needs is the mock consumer whose commit history the assertions read. A test that built
  its own mocks instead would be asserting about a fixture the harness cannot see, so every scenario
  would have to be written a second time. Any future in-process JVM client uses that lane.
- **Neither transport got a subprocess runner, because neither has a sidecar spawn to exercise** -
  `GrpcParallelConsumerClient` connects to a port it is given, by design. **If the lifecycle unit
  gives the Java client a spawn, that decision is worth revisiting**: at that point a subprocess
  runner would test something the binding cannot.
- **The conformance module now test-depends on `java-direct` and `java-grpc`**, which is a new
  reactor edge. It is safe in this direction - that module is a leaf nothing depends on - but the
  rule from the harness module's pom is unchanged and still binds: do not put an engine dependency
  back into a transport module, in any scope.
- `DirectSpikeConformanceTest` and `GrpcSpikeConformanceTest` **stay**. They overlap the shared suite
  on the success path and the committed offset, and they cover things it does not - the produce
  payload, FIFO hand-out through one executor, the asynchronous processor answering off-thread, the
  session-end stage, and the records-out-for-processing leak check. Redundancy between two suites is
  cheaper than a scenario deleted by whoever knew least about what it was protecting.

## The reader-experience pass on the demo output (astubbs#242)

The demo contract (`parallel-consumer-proxy/demo/README.md`) grew three output rules after someone
watched a run and found it unimpressive. Java, as the reference the other ten transcribe, took them
first. What changed here, and what the pass could not close.

### What the Java demo now prints

- **A banner, before anything the demo controls.** `ReferenceDemo.BANNER`, printed from `announce`
  in `main` **before** `DemoBroker.resolve` - so it precedes the "starting a broker in a container"
  paragraph rather than following it. The effective-configuration block moved with it, for the same
  reason. `runFor` no longer prints the fingerprint, which means `ReferenceDemoIT` no longer sees it;
  that is deliberate - the contract binds the demo's output, and the test asserts on returned
  results.
- **Every arm names its client**, as `arm (client)`. `ArmResult` now carries the two separately and
  renders `label()`; everything that has to *match* - `baselineOf`, `ReferenceDemoIT`'s expected
  arms, `bin/ci-demo-test.sh`'s `REQUIRED_ARMS` - still keys off `arm()` alone, which is what kept
  the change from rippling.
- **Two evidence columns, `records` and `keys`.** `ArmTally` accumulates both in one call, so an arm
  cannot count a record without noting its key. The arm column is sized to the widest label in the
  table rather than to a constant, because `pc-core (ParallelEoSStreamProcessor)` does not fit one.

### THE TWO `bin/` SCRIPTS THAT NOW NEED UPDATING - one fails loudly, one fails silently

Both read the tables by regex, and both were written against the old row shape. **Neither was
touched by this pass** (the fan-out that produced it owns `bin/`), and every language changing its
labels breaks them identically, so this is a shared item rather than a Java one.

- **`bin/ci-demo-test.sh` FAILS.** Its per-arm assertion is
  `grep -qE "^[[:space:]]*${arm}[[:space:]]+[0-9]"` - the arm name followed by its elapsed figure.
  A row now reads `AK core (KafkaConsumer)    1.9s ...`, so what follows the name is `(`, not a
  digit, and every required arm reports "no row". The `java-grpc-uds` assertion further down has the
  same shape and the same fate.
- **`bin/ci-demo-conformance.sh` PASSES, having stopped checking the arms.** Its `HEADER` pattern
  survives (it is anchored only at the start, and the two new columns were appended *after*
  `vs AK core` partly for that reason). Its `ROW` pattern does not: it forbids `(` and `,` in the
  name and requires the line to *end* at the ratio. So no `ROW` lines reach the skeleton, the
  skeletons still diff clean on `DIAL`/`TITLE`/`HEADER` alone, and the drift check silently covers
  less than it did. `normalise_arms` is moot for the same reason. **A green run of this script after
  the label change is not evidence the arms agree.**

### Column order, and why `records`/`keys` went last

The contract names the two columns but not their position. They are appended after `vs AK core`
rather than inserted after `arm`, which keeps `ci-demo-conformance.sh`'s `HEADER` pattern matching
and reads as "the old table, plus its evidence". **The other ten demos have to match this**, and
nothing enforces the order - the conformance script compares column identity through a pattern that
would accept either arrangement.

### The contract clause this could not honour: the banner is not the first line

Roughly thirty lines of logback's own configuration status print before the banner, and **both**
entry points have it - checked, not assumed. `parallel-consumer-core`'s test jar is on the demo
classpath (the sidecar spawn needs it) and carries a `logback-test.xml` with `scan="true"`; logback
cannot watch a file inside a jar, warns, and a warning makes it dump its whole status. **A
`logback.xml` in the demo module would not fix it** - a `logback-test.xml` anywhere on the classpath
outranks it - so the fix belongs wherever that test jar's logging config does, not here. Recorded
rather than worked around; the demo's own README says so too.

Everything the demo itself controls is in contract order: banner, then the effective configuration,
then the run - the broker paragraph now comes after both rather than between them.

### What was actually run

`demo/run.sh --records 20 --concurrency 4 --partitions 2 --replay-factor 2`, natively and with
`--docker`; both exited 0 and printed both tables. Natively, five arms (`java-grpc-uds` correctly
absent on macOS, with the message naming the container); in the container, all six. `records` and
`keys` read 20/20 in the small replay and 40/40 in the big one on both paths - the big replay's arms
re-read from the beginning, so they see the whole seeded backlog, which is why 40 is right there.
`DemoOptionsTest`, `ConfigureParityTest` and `ArmCompletionTest` pass (29 tests), and
`ReferenceDemoIT` passes in the integration lane - so the new `uniqueKeys` equality assertion has
been executed against a real broker and all five arms that platform runs, not merely compiled.

**No throughput figure from these runs is worth anything and none is recorded here.** Eleven agents
were doing this simultaneously on one machine - load average was over 80 on twelve cores, the first
container attempt had its broker killed with exit 137, and the image build took a quarter of an
hour. The runs prove the output SHAPE.
