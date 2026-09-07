# Draft response to astubbs#180 - posted by the pre-release sweep, not by this PR

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- post-merge: exempt-file - a drafted issue reply, held until the pre-release sweep posts it.
     It deliberately outlives the PR that wrote it, so it cannot be written in post-merge terms. -->

Written while the context is here, per `docs/inflight/AGENTS.md`. **Not posted, and it survives this
PR** - it is deleted when it is posted and not before; the sweep in
[`docs/releasing.md`](../releasing.md) is what consumes it. astubbs#180 mirrors
confluentinc#861, so this reply is the fork's answer to that report.

---

Diagnosed: nothing is wrong with the repository or with your dependencies, and there is no missing
release. `ManagedTruth` and its `*Subject` family are **generated, not source** -
`truth-generator-maven-plugin` writes them into
`parallel-consumer-core/target/generated-test-sources` at the `generate-test-sources` phase, and
`target/` is git-ignored. Any invocation that does not reach that phase leaves them absent, and every
test that statically imports `ManagedTruth.assertThat` then fails to compile. The same phase runs
`build-helper`'s `add-test-source`, which is why the same mistake also produces
`package ...parallelconsumer.integrationTests does not exist`.

So the fix is a command, not a dependency:

```bash
./mvnw clean install -DskipTests     # the first command in a fresh clone
```

Three invocations spring the trap, all documented now in
[`docs/building.md`](https://github.com/astubbs/parallel-consumer/blob/master/docs/building.md):

- `./mvnw compile` - stops before `generate-test-sources`, so it never generates anything. On a fresh
  clone it does not even get that far: it dies at `parallel-consumer-vertx` unable to resolve
  `parallel-consumer-core:jar:tests`, which reads like a broken repository or a network outage but is
  the same cause, and is confluentinc#162 filed from the other end.
- `./mvnw -pl <module> ...` without `-am` - fails the enforcer at `validate`. That rule's default
  message named no fix; it now points at `-am` and at the generated assertions.
- Running a test straight from an IDE after import - the IDE compiles test sources with its own
  compiler and never runs Maven's `generate-test-sources`. Run the install once on the command line,
  then reimport.

`bin/build.sh`'s own usage example was the broken form (`-pl <module>` with no `-am`); that is fixed
too.

What is **not** fixed, and is recorded rather than closed: the IDE path has no guard (an enforcer
rule cannot reach it), and the underlying test-jar coupling behind confluentinc#162 is untouched -
removing it means extracting the shared test fixtures into their own module.
