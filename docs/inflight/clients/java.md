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
