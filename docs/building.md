# Building: the fresh-clone recipe, and the traps around it

Why the Truth assertion classes are not in source control, which Maven invocations produce them and
which do not, and what the resulting errors look like. The root `AGENTS.md` owns the script list and
the one rule that binds every session; everything below is what you need once a build has already
gone wrong.

## The first command in a fresh clone

```bash
./mvnw clean install -DskipTests
```

Then run the tests however you like - `bin/build.sh` and the `bin/ci-*.sh` scripts (listed in
`AGENTS.md` -> "How to Build") wrap the usual combinations. Narrowing any of them to one module
needs `-am` as well as `-pl` (`bin/build.sh -pl parallel-consumer-core -am`); see the table below.

**Do not start with `./mvnw compile`, and do not start with `./mvnw -pl <module> ...` without
`-am`.** Both skip the code generation the test sources need, and both fail in ways that read like a
broken repository rather than a wrong command.

## Why: the Truth assertion classes are generated, not source

The tests assert through `bz.stub.parallelconsumer.ManagedTruth` and a large family of `*Subject`
classes. **None of these are in source control.** They are produced by `truth-generator-maven-plugin`
(see the `truth-generator-maven-plugin` execution in `parallel-consumer-core/pom.xml`), bound to the
**`generate-test-sources`** phase, and written to:

```
parallel-consumer-core/target/generated-test-sources/truth-assertions-managed/
parallel-consumer-core/target/generated-test-sources/truth-assertions-templates/
```

That is ~158 generated `.java` files under `target/`, which is git-ignored. So they exist only after
a build that actually reaches `generate-test-sources`. Anything earlier in the lifecycle - or any
invocation that excludes core from the reactor - leaves them absent, and the test sources that
`import static bz.stub.parallelconsumer.ManagedTruth.assertThat;` then fail to compile.

The same phase also runs `build-helper`'s `add-test-source`, which is what puts
`src/test-integration/java` on the test compile path - so the same mistake also produces
`package bz.stub.parallelconsumer.integrationTests does not exist`.

## The error, and what actually causes it

If you are here because you searched for this
([astubbs#180](https://github.com/astubbs/parallel-consumer/issues/180),
[confluentinc#861](https://github.com/confluentinc/parallel-consumer/issues/861)):

```
[ERROR] .../ProducerManagerTest.java: cannot find symbol
  symbol:   class ManagedTruth
  location: package bz.stub.parallelconsumer
[ERROR] .../ProducerManagerTest.java: static import only from classes and interfaces
```

or, in an IDE, `ManagedTruth.assertThat cannot be found`.

**Nothing is wrong with the repository or with your dependencies.** The generator has not run.

| Invocation | Result |
|---|---|
| `./mvnw clean install -DskipTests` (root) | **Works.** The recommended first command - also installs core's `tests` classifier jar, which the vertx/reactor/mutiny/examples modules depend on |
| `./mvnw test-compile` (root) | **Works**, from an empty local repository too: BUILD SUCCESS across all 11 modules |
| `./mvnw -pl <module> -am test-compile` | **Works.** `-am` ("also make") pulls the parent and core into the reactor |
| `./mvnw -pl <module> test-compile` | **Fails** at `validate`, before compiling anything: enforcer `ReactorModuleConvergence`, *"Module parents have been found which could not be found in the reactor."* The rule carries a custom message pointing at `-am`; this is the guard working |
| `./mvnw compile` (root) | **Fails, and misleadingly** - see below. `compile` is before `generate-test-sources`, so nothing is ever generated either way |
| Running a test straight from the IDE after import | **Fails** with the `ManagedTruth` error, for the same reason - see below |

Measured on JDK 17 against a hard-linked copy of `~/.m2/repository` with the project's own snapshots
deleted from it, so "fresh clone" means what it says.

## `./mvnw compile` looks like a broken repo, and is not

`compile` is the most tempting "just check it builds" command, and it is the one whose failure tells
you least. What it does depends on what is already in your local `~/.m2`:

- **Fresh clone, no prior `install`.** It dies on module 3/11:

  ```
  [ERROR] Failed to execute goal on project parallel-consumer-vertx: Could not resolve dependencies
  for project bz.stub.parallelconsumer:parallel-consumer-vertx:jar:0.6.0.0-SNAPSHOT: Could not find
  artifact bz.stub.parallelconsumer:parallel-consumer-core:jar:tests:0.6.0.0-SNAPSHOT in central
  (https://repo1.maven.org/maven2/)
  ```

  That reads like a network or repository problem. It is neither, and the artifact is not supposed to
  be on Central: `compile` stops before `test-compile`, so core's `tests` classifier jar is never
  produced inside this reactor run, and vertx's test-scoped dependency on it has nothing to resolve
  against (see *Related*, below). Offline (`-o`) the same failure prints different text - "Cannot
  access central ... in offline mode" - which is why the isolated-repository run above is the one to
  trust.

- **After any earlier `install`.** That test-jar is now cached, so the same command exits
  **BUILD SUCCESS having generated nothing** - the quieter trap; the truth-generator goal does not
  appear in core's goal list at all. The failure resurfaces later as the `ManagedTruth` error, from
  whatever runs the tests.

Either way the fix is the same: use `./mvnw clean install -DskipTests`, or `./mvnw test-compile`.

## In the IDE

An IDE compiles test sources with its own compiler and does not run Maven's `generate-test-sources`
for you. After importing the project:

1. Run `./mvnw clean install -DskipTests` (or at minimum `./mvnw generate-test-sources`) **once, on
   the command line**, so the generated sources exist on disk.
2. Reimport / reload the Maven project. The generator registers its output as a test source root, so
   the IDE picks up `truth-assertions-managed` and `truth-assertions-templates` once they are there.
   If it does not, mark those two directories as *Test Sources Root* by hand.

Repeat step 1 after anything that wipes `target/` (including the IDE's own "Rebuild Project" in some
configurations).

## If generation half-happens, `clean`

`truth-generator-maven-plugin` is configured with `cleanTargetDir=false`, so it does **not** repair a
partially-deleted output directory. Deleting `target/generated-test-sources` by hand and re-running
`./mvnw install -DskipTests` reproducibly regenerates only part of the set (137 of 158 when last
measured) and then fails with ~190
`cannot find symbol` errors *inside the generated code itself* (`ManagedTruth.java` referencing
`ParallelConsumerOptionsSubject`, `CommitModeSubject`, ...). Do not hand-delete parts of `target/` -
run `./mvnw clean ...` and let it regenerate the whole set.

## Related

The cross-module coupling behind this is the same one as
[astubbs#132](https://github.com/astubbs/parallel-consumer/issues/132) /
[confluentinc#162](https://github.com/confluentinc/parallel-consumer/issues/162) - whose title,
*"mvn compile fails if test-jar of parallel-consumer-core was not previously installed"*, is the
fresh-clone failure above, reported from the other end: the vertx, reactor, mutiny and example
modules depend on `bz.stub.parallelconsumer:parallel-consumer-core:jar:tests`, which does not exist
until core is packaged and installed. The release pipeline's instance of this is already handled
(`preparationGoals` in the `maven-release-plugin` configuration in `pom.xml`); the contributor-facing
path is what this document covers.

What documenting it did **not** fix - the unguarded IDE path, `cleanTargetDir`, and the test-jar
coupling itself - is
[`inflight/ci-180-generated-truth-build-traps.md`](inflight/ci-180-generated-truth-build-traps.md).
