---
title: Apache Kafka 4.x support
type: feat
status: draft
date: 2026-04-23
---

# Apache Kafka 4.x support for Parallel Consumer

## Problem

PC is pinned to `kafka-clients 3.9.1` and compiled to Java 8 bytecode via Jabel. An experimental `[3.9.1,5)` version-range job exists in CI (`test-kafka-compat`) but it fails to compile against Kafka 4.x. The broader Kafka ecosystem has moved: Kafka 4.0 GA'd in early 2025, 4.2 brought KIP-932 Share Groups to GA. Spring Kafka 4.0 (released November 2025) tracks `kafka-clients 4.1.1`. PC needs a credible story on 4.x or it becomes irrelevant for new projects.

This brainstorm captures the direction for adding 4.x support. Implementation is experimental for now — the work will branch off `origin/master`, be explored and proven, but not merged until the fundamentals in flight (857 silent-stall investigation, snapshot/release pipeline stabilization) complete.

## Research findings

### Actual API-surface breakage between 3.9 and 4.x

Nearly all `kafka-clients` types PC uses are structurally unchanged: `Consumer<K,V>`, `Producer<K,V>`, `ConsumerRecord`, `ConsumerRecords`, `TopicPartition`, `OffsetAndMetadata`, `Header`, `ProducerRecord`, `Callback`, `ConsumerRebalanceListener`. The "producer typing" concern that motivated this brainstorm turned out to be a red herring.

What did break at 4.0 (via Kafka 4.0 upgrade guide + release notes):

| Removed in 4.0 | Replacement (exists in 3.9 already) |
|---|---|
| `Consumer#poll(long)` | `Consumer#poll(Duration)` |
| `Consumer#committed(TopicPartition)` / `(TP, Duration)` | `Consumer#committed(Set<TP>)` / `(Set<TP>, Duration)` |
| `Producer#sendOffsetsToTransaction(Map, String groupId)` | `Producer#sendOffsetsToTransaction(Map, ConsumerGroupMetadata)` |
| `MockConsumer(OffsetResetStrategy)` / `#setException()` | `MockConsumer(String)` / `#setPollException()` |
| `Admin#alterConfigs()` | `Admin#incrementalAlterConfigs()` |
| `RecordMetadata(... Integer checksum ...)` ctor | ctor without checksum |
| `ConsumerGroupState` enum | `GroupState` |
| `NotLeaderForPartitionException` | `NotLeaderOrFollowerException` |
| `DefaultPartitioner` / `UniformStickyPartitioner` classes | none (sticky is built-in) |
| `Partitioner#onNewBatch()` | removed from interface |

Every replacement above **already exists in Kafka 3.9**. Code written against the forward-compatible API runs unchanged on both 3.9 and 4.x.

### Runtime / build-level constraints

- **`kafka-clients 4.x` requires Java 11 minimum** at runtime. 3.x supports Java 8-17. Once we depend on 4.x being possible, the PC baseline must move to Java 11.
- Maven coordinate `org.apache.kafka:kafka-clients` unchanged.
- No separate client-only artifact split.

### Actual CI failure on the current experimental 4.x job

The existing `test-kafka-compat` job fails at compile time with ~25 errors, all on PC's own exception classes (`InternalRuntimeException`, `ExceptionInUserFunctionException`, `ParallelConsumerException`, etc.). Example:

```
InternalRuntimeException.java:29: error: recursive constructor invocation
ProducerManager.java:247: error: incompatible types:
  ProducerFencedException cannot be converted to java.lang.String
```

**Root cause, confirmed by local reproduction:** it's the Jabel cross-compilation (Java 17 source → Java 8 bytecode) colliding with Kafka 4.x's Java 11 minimum bytecode on the classpath. Same code, same Lombok version, same kafka-clients 4.x:

| Compile config | Result |
|---|---|
| `release.target=8` (Jabel default) + kafka-clients 4.x | ❌ 25 errors on PC's exception classes |
| `release.target=11` + kafka-clients 4.x | ✅ BUILD SUCCESS |

Mechanism: when Jabel's javac hook encounters Java 11 class files on the classpath during annotation processing, Lombok's `@StandardException` silently fails to generate the `(Throwable)` and `(String, Throwable)` constructors it would normally produce. Call sites like `new InternalRuntimeException(someThrowable)` then fail to resolve to any visible constructor, and the custom varargs constructor `(String, Throwable, Object...)` becomes self-recursive on its internal `this(msg, e)` call (which expected the missing `(String, Throwable)` target).

**The compile never reaches PC's actual Kafka API call sites**, so we don't yet know whether those also need changes beyond the known list. Once Jabel is removed, the 25 "errors" vanish with zero source changes, and then any real Kafka 4.x API migrations become visible.

## Goals

- **G1.** PC compiles and runs against `kafka-clients 4.x` (at least 4.2).
- **G2.** PC continues to work against `kafka-clients 3.9.x` (the existing release line's Kafka version), from a single JAR if practical.
- **G3.** Don't degrade PC's public API to accommodate 3.x — if a 4.x-only convenience is worth exposing, expose it; don't lowest-common-denominator.
- **G4.** Bump PC's minimum Java version to 11 (forced by 4.x). Keep source on Java 17, drop Jabel for the 0.7.x line.
- **G5.** Experiment on a branch; don't merge until PC's base (857 investigation, release pipeline) has stabilized.

## Non-goals

- Maintaining the 0.6.x line after 0.7.x ships. No backport window. Legacy users freeze at the last 0.6.x release.
- Dual `core-3` / `core-4` modules with separate artifacts. Explored and rejected — single module is expected to work.
- Shipping KIP-932 Share Groups integration in 0.7.x. See Deferred.

## Key decisions

### D1. Single-module, version-agnostic JAR (not dual-module, not abstraction layer)

Target: one `parallel-consumer-core` artifact that compiles clean against both `kafka-clients 3.9.x` and `4.x`. Achieved by coding exclusively against the forward-compatible API subset (which is the 4.x API surface, all pieces of which also exist in 3.9).

No `KafkaClientApi` wrapper interface. No conditional compilation. No dual modules.

**Rationale.** Every 4.x API change is a replacement of a deprecated-in-3.x method. Using the forward-compatible subset eliminates the need for an abstraction layer entirely — the "abstraction" is just "use the new method names". Dual modules would double the maintenance surface for a problem that doesn't actually need the split.

### D2. Cut a new major line: 0.7.x = Java 11 + kafka-clients 4.x (compatible with 3.9)

`0.7.x` branch and artifact line starts from this work. Legacy `0.5.x` / `0.6.x` are frozen — no new releases, no backports, no support window. Users on Kafka 3.x with Java 8 stay on the last 0.6.x release.

**Rationale.** Kafka 4.x requires Java 11 at runtime. Matching industry pattern (Spring Kafka 3.x ↔ Kafka 3.x, 4.x ↔ 4.x). Single-maintainer reality means a parallel maintenance line isn't sustainable.

### D3. Experimental branch; don't merge until fundamentals land

Branch `feat/kafka-4-support` (or similar) forks from `origin/master`. Work happens there. Merge blocked by: (a) #857 investigation concluded to a landable state, (b) release pipeline proven on a real 0.6.x cut.

### D4. Fix Lombok/@StandardException compile failure first

Before touching any Kafka API call site, resolve the exception-class compilation errors. Evidence strongly suggests this is fixable by adjusting the custom varargs constructor pattern — not a Kafka problem at all. Without this fix, we can't see what else breaks.

### D5. `parallel-consumer-share` (KIP-932) is a separate future module, out of scope here

Share Groups ship in kafka-clients 4.2+. `ShareConsumer` is a new API, not a replacement for `Consumer<K,V>`. Any PC-flavored share-groups integration would be an additive module depending on `kafka-clients ≥ 4.2`, not part of the 0.7.x core.

Deferred to a separate brainstorm when the core 0.7.x is stable.

## Requirements

- **R1.** `kafka.version` default in `pom.xml` advances to a current 4.x release (TBD during implementation — 4.1.x or 4.2.x).
- **R2.** All code builds and tests green against both `kafka-clients 3.9.1` and `kafka-clients 4.x` (the CI matrix proves this).
- **R3.** Source-level Java baseline moves to 11. Jabel removed from the parallel-consumer-core compile path. Source can stay at Java 17.
- **R4.** The Lombok `@StandardException` compile failure is resolved — ideally by simplifying the custom constructor pattern on PC's exception classes.
- **R5.** All removed-in-4.0 API call sites in PC source are replaced with their forward-compatible equivalents (the list in Research findings above).
- **R6.** `MockConsumer` uses in test code migrate to the 4.0 constructor / method signatures (the new forms already exist in 3.9).
- **R7.** CI's `test-kafka-compat` job flips from `continue-on-error: true` to a blocking tier on this branch.
- **R8.** Release notes / CHANGELOG call out Java 11 minimum and the 0.6.x freeze.

## Success criteria

- `bin/ci-build.sh` passes on default Kafka version.
- `bin/ci-build.sh '[3.9.1,5)'` passes against Kafka 4.x.
- Integration tests run against a Kafka 4.x broker (TestContainers image bump) and pass.
- No regression in `parallel-consumer-vertx`, `parallel-consumer-reactor`, `parallel-consumer-mutiny`, `parallel-consumer-examples`.
- End-to-end sanity: a toy consumer using PC 0.7.x + `kafka-clients 4.2` + broker 4.2 processes messages correctly.

## Open questions (defer to planning)

- **Q1.** Exact 4.x version to target: 4.1.x (stable, conservative) vs 4.2.x (latest GA, includes Share Groups foundations). *Planning decision.*
- **Q2.** TestContainers `confluentinc/cp-kafka` image tag bump — which version pairs with kafka-clients 4.x for integration testing. *Planning / research during implementation.*
- **Q3.** Does any PC module have a deeper dependency on Jabel (e.g., `parallel-consumer-mutiny` already overrides to release 9)? Confirm Jabel-removal doesn't cascade into hidden regressions. *Planning audit.*
- **Q4.** Are any of PC's public API signatures Java-8-specific in ways that break when moving source baseline? (Very unlikely — Java 8 → 11 public API is backward-compatible at source level.) *Planning audit.*

## Deferred to separate tasks

- **KIP-932 Share Groups integration** (new `parallel-consumer-share` module). Separate brainstorm when 0.7.x core is stable.
- **0.6.x bugfix backports.** No plan to do these; explicitly not in scope.
- **Formal 0.6.x "last release" tag cut** — cosmetic closure for the 0.6.x line. Out of scope for the 4.x work; can be its own small task.

## Sources

- [Kafka 4.0 Official Upgrade Guide](https://kafka.apache.org/40/getting-started/upgrade/)
- [Kafka 4.0.0 Release Notes](https://archive.apache.org/dist/kafka/4.0.0/RELEASE_NOTES.html)
- [Kafka 4.2.0 Release Notes](https://archive.apache.org/dist/kafka/4.2.0/RELEASE_NOTES.html)
- [KIP-1124: Clear Kafka Client upgrade path for 4.x](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1124:+Providing+a+clear+Kafka+Client+upgrade+path+for+4.x)
- [Spring for Apache Kafka 4.0 GA](https://spring.io/blog/2025/11/18/spring-kafka-4/)
- Failing CI run: `https://github.com/astubbs/parallel-consumer/actions/runs/24811153191` (Kafka Compat experimental 4.x job log, 2026-04-23)
- PC's current exception base: `parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/InternalRuntimeException.java`
