# astubbs#181 - Java 24: what actually broke, and why this tree no longer hits it

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

[astubbs#181](https://github.com/astubbs/parallel-consumer/issues/181), mirroring
[confluentinc issue #862](https://github.com/confluentinc/parallel-consumer/issues/862). **Verified
on a workstation, never in CI** - that gap is the whole of the remaining work.

## The mechanism, confirmed rather than assumed

The break was never in Parallel Consumer's code. kafka-clients up to 3.9.0 calls
`Subject.getSubject(AccessController.getContext())` from its SASL callback handlers
(`SaslClientCallbackHandler`, `OAuthBearerSaslClientCallbackHandler`). JDK 23 already makes that
throw `UnsupportedOperationException` - "supported only if a security manager is allowed", so
`-Djava.security.manager=allow` was still an escape hatch - and JDK 24 removes the hatch, which is
why the reporter drew the line at 24 rather than 23. kafka-clients 3.9.1 (KAFKA-19024) routes the
same calls through `org.apache.kafka.common.internals.SecurityManagerCompatibility`, which selects
`Subject.current()` / `callAs` when the JDK has them.

Two predictions were refuted and are worth recording, because both are believable and both are wrong:

- **`Subject.doAs` is not the failing call.** It still returns normally on JDK 26. Only `getSubject`
  throws, so the broken surface is the authentication callback path, not every Kafka client. A
  PLAINTEXT client on kafka-clients 3.7.1 processed records to completion on JDK 26 in the same
  harness.
- **confluentinc PR #908 cannot resolve confluentinc#862**, contrary to the ranking in
  `src/docs/development/upstream-pr-analysis.adoc` (grep `Virtual Threads support`). That PR migrates
  `synchronized` to `ReentrantLock` for virtual-thread pinning, and JDK 24 removed pinning anyway.
  Different mechanism. Fork PR astubbs#51 is a copy of it and neither blocks nor closes this issue.

## Where this tree stands

`pom.xml` carries `<kafka.version>3.9.2</kafka.version>`, past the fix. No main-code reference to
`SecurityManager`, `AccessController`, `doPrivileged` or `sun.misc.Unsafe` exists in any module. The
reflective access that does exist -
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java`
(grep `getDeclaredField("delegate")`) and
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/ProducerWrapper.java` (grep
`transactionManager`) - targets kafka-clients classes on the classpath, not JDK-internal modules, so
strong encapsulation never applies to it.

Evidence, run locally: the built core jar is class file version 52 (Java 8 bytecode), and an
end-to-end `ParallelStreamProcessor` run over a `MockConsumer` (KEY ordering,
`PERIODIC_CONSUMER_SYNC`, 50 records) completed identically on JDK 17 and JDK 26. JDK 26 is strictly
harsher than 24. **The caveat matters: a `MockConsumer` cannot exercise SASL, which is the one path
that ever broke.** A real SASL client on a modern JDK remains untested here.

## What is left, and the decision that is not mine

Nothing in CI runs above `java-version: '17'`, and the Kafka-version axis is switched off in
`.github/workflows/maven.yml` (grep `test-kafka-compat`). That gap is astubbs#128
(mirroring confluentinc#103), and until a lane exists "runs on Java 24" is a local observation, not
a claim the project can publish.

**Maintainer's call:** close astubbs#181 on the rationale above - the concrete ask (stop being pinned
to kafka-clients 3.7.1) is satisfied by 3.9.2, and the mechanism it was pinned on is gone - and let
astubbs#128 carry the CI proof; or hold it open until that lane exists. The reporter should be told
which kafka-clients version settles it, not just that it is fixed.

This is **not** 0.7.x work, so do not fold it into `docs/inflight/pr-53-java-baseline-kafka4.md`:
that note is about the *build and compile* baseline needed for Kafka 4, a different question from
what the shipped Java 8 jar can run on. The two only look alike because both say "Java".
