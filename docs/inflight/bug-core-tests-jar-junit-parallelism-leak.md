# `parallel-consumer-core`'s tests jar leaks JUnit parallelism into every consuming module

`parallel-consumer-core/src/test/resources/junit-platform.properties` is packaged at the **root** of the
`parallel-consumer-core` **tests** jar. JUnit Platform reads `junit-platform.properties` from the root of
the classpath, so **every module that depends on that jar silently inherits it** - which is every module
with integration tests:

```
junit.jupiter.execution.parallel.enabled=${parallel-tests}
junit.jupiter.execution.parallel.mode.default=concurrent
junit.jupiter.execution.parallel.config.dynamic.factor=20
```

The consuming module gets 20x dynamic parallelism, concurrent by default, that it never asked for and
that its own configuration does not mention. It is invisible until something is order- or
isolation-sensitive, and then it looks like a flake in the consuming module.

**How it surfaced.** Apache Kafka's own `StreamTaskTest`, run against the patched classes in
`parallel-consumer-streams-spike`, failed **159 tests on state-directory lock contention** before anyone
had looked at the patch. Kafka's tests are written for a serial runner - `StreamTaskTest` pins a fixed
state-directory name and shares `MockProducer`/`MockConsumer` per instance. Nothing in that module
requested parallelism; it arrived with the core tests jar.

**Worked around, not fixed.** `parallel-consumer-streams-spike/pom.xml` pins it off for the Kafka
execution via surefire `configurationParameters`, which outrank the properties file:

```xml
<configurationParameters>junit.jupiter.execution.parallel.enabled = false</configurationParameters>
```

That is a per-execution patch on one module. The leak itself is untouched, and the next module to hit it
will spend the same time diagnosing it.

**The fix is a decision, not a puzzle:** the file configures *core's own* test run, so it should not be
in the published tests artifact - either exclude it from the tests jar, or move core's setting somewhere
that does not travel (surefire `configurationParameters` on core itself). Either way, any module that
genuinely wants parallelism should then ask for it explicitly.

Also written up as §10.1 of
[`docs/plans/2026-08-08-002-ks-on-pc-spike-result.md`](../plans/2026-08-08-002-ks-on-pc-spike-result.md).
