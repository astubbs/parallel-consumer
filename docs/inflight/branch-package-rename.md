# Package rename `io.confluent.parallelconsumer.*` → `bz.stub.parallelconsumer.*`

The package-rename project's entry. This copy holds one recorded finding, carried over from
astubbs/parallel-consumer#289 - the change that cleared the legacy-token residue ahead of the rename.
Other branches working the rename keep their own account of it at this same path, so when they
converge git raises a conflict and whoever resolves it reads both and combines them into the single
entry for the project. That is deliberate, and more reliable than a cross-reference someone has to
remember to follow.

Nothing below blocks anything; it exists so a finding already made is not re-derived, or worse,
mis-repaired.

## The one surviving legacy-token reference

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

The one region that genuinely was orphaned - `parallel-consumer-example-metrics/pom.xml`, whose
`exampleDep` markers no include named - has had its markers removed by astubbs#289. The template
documents the metrics module's `CoreApp` but never its dependency snippet; removal was chosen over
adding the missing include, so if a metrics snippet is ever wanted in the README the markers come
back with it.
