# The core tests jar was configuring every downstream module's JUnit runner

Found while running Apache Kafka's own test suite against a patched build (astubbs#255): 159 of 188
upstream tests errored with "Unable to initialize state, this task has already been initialized" and
"same state directory". Kafka's tests are written for a serial runner. Nothing in that module asked for
a parallel one.

## What was happening

`parallel-consumer-core/src/test/resources/junit-platform.properties` is packaged at the **root** of the
core **tests** jar. Eight modules depend on that jar - the four integrations plus every example - and
JUnit reads the first `junit-platform.properties` it finds on the classpath. So all eight silently ran
their tests with:

    junit.jupiter.execution.parallel.enabled=<whatever core baked>
    junit.jupiter.execution.parallel.config.dynamic.factor=20

The value is Maven-filtered from `${parallel-tests}` **at core's build time**, so a consuming module's
own setting was never consulted. That is the part that bites: there was no way for a module to opt out,
and nothing anywhere said it had opted in.

It also inverted the root pom's stated intent. The `ci` profile sets `parallel-tests=false` deliberately,
because JUnit thread parallelism contends on static state and CI parallelises by forking JVMs instead
(see the comment on `surefire.forkCount`). A locally-built jar bakes `true`, so a downstream module
could inherit `enabled=true, factor=20` precisely where the project had decided it did not want it.

Severity depends on how core was built. `bin/ci-*.sh` all pass `-Pci`, so a CI-built jar bakes `false`
and downstream inherits that. A plain local build bakes `true`. So this is mostly a local-dev and
mixed-build hazard rather than a live CI fire - but the consuming module's own setting is ignored either
way, which is the real defect.

## The fix

Moved into `parallel-consumer-core/pom.xml` as surefire and failsafe `configurationParameters`, and the
resource deleted. As build configuration the setting is per-module by construction: each module
evaluates its own `${parallel-tests}`, nothing is packaged, nothing leaks, and `configurationParameters`
outrank a properties file if one is ever reintroduced.

Core's own behaviour is unchanged. The eight downstream modules revert to the JUnit default (serial) -
which is what they would always have had if the leak had never existed.

## Why two branches will care

- **`ci/reenable-parallel-tests`** - that branch exists to *measure* parallel execution's speed and
  reliability. Any measurement taken while downstream modules were already running at `factor=20`
  regardless of the flag has a contaminated control arm: the "off" case was not off everywhere. Worth
  re-taking any numbers gathered before this lands.
- **`optimize/unit-gate`** - fork-tail packing interacts directly with JUnit thread parallelism. Same
  caution about pre-existing measurements.

Neither had an open PR when this was written, which is why the note is here rather than in a review
comment.

## Delete this file when

The fix has landed on master and both branches above have either merged master or confirmed their
measurements are unaffected.
