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
