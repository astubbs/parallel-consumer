# Package rename: the legacy `io.confluent.csid.asyncconsumer` reference the sweep will hit

Context for whoever runs the package rename. Nothing here blocks anything; it exists so a finding
already made is not re-derived, or worse, mis-repaired.

## The reference

`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/KafkaSanityTests.java`,
on the javadoc of `pausedConsumerStillLongPollsForNothing`:

```java
/**
 * @link io.confluent.csid.asyncconsumer.BrokerPollSystem#pollBrokerForRecords
 */
```

`io.confluent.csid.asyncconsumer` is the project's pre-parallel-consumer package name and has no
tracked source file, so the rename script's completeness sweep will flag this on the legacy token.
It is the **only** surviving occurrence of that token: astubbs/parallel-consumer#289 deleted the
rest, which were all inert logback logger entries, and deliberately left this one alone rather than
repair a reference into a package that the rename is about to move again.

## What was established about it

- **It is live, not dead.** The class survives as
  `io.confluent.parallelconsumer.internal.BrokerPollSystem`, and `pollBrokerForRecords` is still a
  real method on it. Only the package in the reference is stale. So the fix is to re-point it, not
  to delete it - the opposite of the treatment the logger entries got.
- **The method is `private`.** A javadoc `@see` or `{@link}` at *member* granularity will not
  resolve to it from another class, and doclint reports that as a reference-not-found error. Link the
  class and name the method in `{@code}`, or keep the pointer as prose.
- **`@link` here is not a javadoc tag.** It is being used as a *block* tag; the inline form
  `{@link ...}` is the only valid one. Whatever the sweep rewrites it to, the surrounding form needs
  fixing too, or a doclint-enabled build will complain about an unknown block tag rather than about
  the package.
- Test sources are not javadoc'd today, which is why none of this has ever surfaced as a build
  failure.

## Do not assume the asciidoc tags in the example poms are dead

`exampleRepo` was unreferenced, which astubbs#289 removed - but that is **not** true of the tagging
apparatus generally, and the tempting generalisation is wrong. `src/docs/README_TEMPLATE.adoc`
carries `include::...[tag=exampleDep]` for the core, reactor and vertx example poms, and the
extracted `<dependency>` blocks are visibly present in the generated `README.adoc`. Deleting those
markers would break the published install instructions.

The asymmetry is in the history: both tags arrived in `7b4f5a5dd`, but the `exampleRepo` include was
deleted in `e7146adf0` (2020, "post release of 0.1 to repo1") while the `exampleDep` includes were
never removed.

One genuine loose end, not acted on: `parallel-consumer-example-metrics/pom.xml` has an
`exampleDep` region that no include names - the template documents the metrics module's `CoreApp`
but never its dependency snippet. That is a missing include or a stray tag; decide which rather than
assuming.
