<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The examples - copyable code that the build keeps honest

An aggregator pom over five small applications, one per way of using the library. They exist to be
copied from, and to stop the documentation lying: the root `README.adoc` does not contain pasted
snippets, it `include::`s tagged regions of *these compiled files*.

## The five

| Module | Shows |
|---|---|
| [`parallel-consumer-example-core`](parallel-consumer-example-core) | The plain `poll` / `pollAndProduce` setup, plus retry, batching, circuit-breaker and shutdown recipes |
| [`parallel-consumer-example-metrics`](parallel-consumer-example-metrics) | Wiring a Micrometer `PrometheusMeterRegistry` in and scraping it |
| [`parallel-consumer-example-vertx`](parallel-consumer-example-vertx) | An HTTP call per record through the Vert.x module |
| [`parallel-consumer-example-reactor`](parallel-consumer-example-reactor) | Returning a Reactor `Publisher` per record |
| [`parallel-consumer-example-streams`](parallel-consumer-example-streams) | Preprocess in a Kafka Streams topology, then consume the output topic concurrently |

## Two things that decide how you read them

- **The tagged regions are the README's source code.** Blocks fenced by `// tag::example[]` and
  friends are pulled into `src/docs/README_TEMPLATE.adoc` by the asciidoc-template plugin and
  rendered into the repo-root `README.adoc`. `CoreApp.closeModes` carries the reasoning in its own
  javadoc: the method is never called, and exists so that an API change breaks the *build* rather
  than quietly leaving the README wrong. **So renaming a tag, or deleting a method that looks
  unused, edits the published documentation.** Grep `README_TEMPLATE.adoc` for the file's path
  before touching one.
- **None of these has a `main` method.** They are wired up and driven by their own tests, and the
  streams example additionally takes its bootstrap servers via the constructor so you can drive it
  from your own code. Each module's README says which test runs it and what it needs.

## Running them

From the repo root, `./mvnw verify -pl :parallel-consumer-examples -am` builds and exercises all
five. Two of them need Docker - failsafe selects on the **package name** `integrationTest*`, not the
source root, so `example-metrics` and `example-streams` run under failsafe from `src/test/java`
while the other three are ordinary surefire unit tests. [`docs/testing.md`](../docs/testing.md)
**owns the suite split**, and `AGENTS.md` owns the repo-wide build scripts; the package-name detail
is the one that catches people here.

## Not published, deliberately

This pom sets `maven.deploy.skip`, `maven.install.skip` and `gpg.skip`, and turns
`skipPublishing` on for the central-publishing plugin: examples are sample code, not library
artifacts. Its comment records the version trap behind that (a plugin version where skipping the
last module in reactor order skipped the entire bundle upload).

That non-publication is also why two of these modules carry **module-local dependency pins** -
`example-metrics` on `jackson-databind`, `example-streams` on the `jackson-bom` - which would break
`parallel-consumer-vertx`'s WireMock tests if promoted to the root pom. Each pin's own comment
carries the reasoning, and
[`docs/inflight/deps-deferred-majors.md`](../docs/inflight/deps-deferred-majors.md) tracks them.
