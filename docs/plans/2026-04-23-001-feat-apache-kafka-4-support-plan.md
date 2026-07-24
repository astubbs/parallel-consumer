---
title: "feat: Apache Kafka 4.x support (PC 0.7.x, Java 11 baseline)"
type: feat
status: active
date: 2026-04-23
origin: docs/brainstorms/2026-04-23-apache-kafka-4-support-requirements.md
---

# feat: Apache Kafka 4.x support (PC 0.7.x, Java 11 baseline)

## Overview

Cut PC's new 0.7.x major line with Kafka 4.x as the default client version and Java 11 as the minimum. Code remains source-compatible with kafka-clients 3.9.x at runtime — the same JAR runs against either broker generation. Legacy 0.6.x on Kafka 3.x + Java 8 is frozen.

This plan implements the direction set by the origin brainstorm. Work lives on a feature branch (`feat/kafka-4-support` off `origin/master`) and is not merged until the 857 investigation lands and the release pipeline proves on a real 0.6.x cut (see origin: `docs/brainstorms/2026-04-23-apache-kafka-4-support-requirements.md`, Goal G5).

## Problem Frame

PC is pinned to `kafka-clients 3.9.1` and cross-compiles Java 17 source to Java 8 bytecode via Jabel. An experimental CI job builds against Kafka 4.x with `continue-on-error: true` and fails at compile time. Confirmed root cause (local reproduction): **Jabel's Java-8 target collides with kafka-clients 4.x's Java 11 minimum bytecode — the interaction silently breaks Lombok's `@StandardException` constructor generation on PC's own exception classes, producing 25 cascading compile errors.** The Kafka 4.x API changes themselves are a small list and mostly already handled.

Switching the target to Java 11 (i.e., dropping Jabel) makes the same code compile green against Kafka 4.x with zero source changes. That's the keystone of this plan.

## Requirements Trace

- **R1.** PC compiles and runs against `kafka-clients 4.x` (4.2.x targeted). *(origin G1)*
- **R2.** PC continues to work against `kafka-clients 3.9.1` from a single JAR. *(origin G2)*
- **R3.** PC's public API is not narrowed to accommodate 3.x. *(origin G3)*
- **R4.** Minimum Java version bumped to 11; Jabel removed. *(origin G4)*
- **R5.** 0.6.x line is frozen. No backports; no maintenance branch. *(origin decision D2)*
- **R6.** CI runs against both Kafka 4.x (primary) and Kafka 3.9.1 (regression) and gates on both. *(origin R7 refined)*
- **R7.** All deprecated-in-3.x / removed-in-4.0 API call sites migrate to the forward-compatible subset. *(origin R5)*
- **R8.** MockConsumer usages migrate to the 4.0 constructor / method forms. *(origin R6)*
- **R9.** Release notes / CHANGELOG / AGENTS.md / README reflect Java 11 minimum and the 0.6.x freeze. *(origin R8)*

## Scope Boundaries

- **Not maintaining a 0.6.x line post-0.7.x.** No backports, no extended support window. Legacy users stay on the last 0.6.x release.
- **No dual-module (`core-3` / `core-4`) split.** Single `parallel-consumer-core` artifact, API-subset strategy.
- **No KIP-932 Share Groups integration in 0.7.x.** Separate future module (deferred below).
- **Not merging this branch yet.** Experimental until 857 resolves and release pipeline stabilizes (origin G5).

### Deferred to Separate Tasks

- **`parallel-consumer-share` module (KIP-932 integration):** new additive module depending on `kafka-clients ≥ 4.2`. Separate brainstorm + plan after 0.7.x core stabilizes.
- **Formal 0.6.x final release cut:** cosmetic closure for the frozen line. Small standalone task, not part of this plan.
- **Exception-class custom-ctor simplification** (old origin R4): the Lombok compile failure was the Jabel cascade; with Jabel removed, the exception classes compile clean as-is. No refactor needed here, but a follow-up housekeeping pass to simplify the `(String, Throwable, Object... args)` pattern across `InternalRuntimeException` et al. would be healthy — filed as a separate task.

## Context & Research

### Relevant code and patterns

- **`pom.xml`** (root): compiler plugin config — lines 679-694 contain the Jabel annotation-processor path and the `<release>${release.target}</release>` setting. Properties `source.version=17` and `release.target=8` at lines 63-64. Jabel dep lines 276-281. Jabel version at 118. `intellij-idea-only` profile at line ~130 contains a "disable Jabel" variant that already compiles at Java 17 target — useful reference for what the Jabel-free config looks like.
- **`parallel-consumer-mutiny/pom.xml`** line 20: `<release.target>9</release.target>` override. Becomes unnecessary once parent moves to 11.
- **`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ProducerWrapper.java`** lines 94-101: deprecated `sendOffsetsToTransaction(Map, String)` overload that internally calls `new ConsumerGroupMetadata(String)`. Both the overload and the inner constructor are deprecated for removal in Kafka 4.x.
- **`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ConsumerManager.java`** line 95 and **`…/ThreadConfinedConsumer.java`** line 75: `poll()` call sites. Audit confirms both already use `Duration`, so no migration needed.
- **`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/OffsetMapCodecManager.java`** line 142: `committed(new HashSet<>(assignment))` — already Set-based, no migration needed.
- **`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/BrokerIntegrationTest.java`** lines 65 / 82: TestContainers image defaults to `confluentinc/cp-kafka:7.9.0` (CP 7.9 = Kafka 3.9.x). Needs bump to a CP release that ships Kafka 4.2.x. Dynamic computation at line 82 accepts `cpMajor`/`cpMinor` inputs.
- **`.github/workflows/maven.yml`** lines 115-143: `test-kafka-compat` job, `continue-on-error: true`. Repurpose for the new role.
- **~18 test files** that construct `MockConsumer<>(OffsetResetStrategy.EARLIEST)` — needs migration to the String form.
- **`parallel-consumer-core/src/test/java/io/confluent/csid/utils/LongPollingMockConsumer.java`** line 40: custom test helper whose ctor takes `OffsetResetStrategy`. Signature update needed.
- **~4 test files** that call `new ConsumerGroupMetadata(String)` — the string form is deprecated; tests should use `consumer.groupMetadata()` or a test fixture.

### Institutional Learnings

- **`docs/solutions/workflow-issues/copyright-header-rules-for-fork-2026-04-21.md`** — `-Dlicense.skip` is mandatory for local Maven commands (fork's copyright-header policy). Applies to any `./mvnw` invocation during verification.

### External references

- **Kafka 4.0 Upgrade Guide** — [kafka.apache.org/40/getting-started/upgrade](https://kafka.apache.org/40/getting-started/upgrade/). Complete list of removed APIs.
- **Spring for Apache Kafka 4.0 GA announcement** (Nov 2025) — [spring.io/blog/2025/11/18/spring-kafka-4](https://spring.io/blog/2025/11/18/spring-kafka-4/). Industry precedent for major-version tracking.
- **KIP-1124: Clear Kafka Client upgrade path for 4.x** — [cwiki](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1124:+Providing+a+clear+Kafka+Client+upgrade+path+for+4.x). Context for forward-compatibility strategy.

## Key Technical Decisions

- **D1. Remove Jabel; move to `--release 11` compile.** Single change unblocks everything. No abstraction layer, no dual-module.
- **D2. Keep `source.version=17`; change `release.target=8` → `release.target=11`.** Source-level Java 17 stays (PC's code uses Java 17 features). Bytecode target moves 8 → 11.
- **D3. Drop Mutiny module's `release.target=9` override.** Becomes redundant (11 ≥ 9).
- **D4. Target `kafka-clients 4.2.0` as the new default.** Latest GA, includes KIP-932 foundations if we later build the share module. Conservative pick (4.1.x) is also acceptable if 4.2 reveals surprise issues during implementation — noted as a fallback, not a blocker.
- **D5. `test-kafka-compat` CI job inverts roles.** Main build uses Kafka 4.2 (the new default). Compat job becomes the Kafka 3.9.1 regression check (proving R2). Both blocking.
- **D6. Version bump: 0.6.0.0-SNAPSHOT → 0.7.0.0-SNAPSHOT.** Marks the new major line. Happens in the same PR as the Jabel removal since the two are logically the same change (user-visible breaking: Java 11 required).
- **D7. TestContainers CP image target: determined during implementation.** CP 8.x ships Kafka 4.x per Confluent release track. Exact tag needs verification when the work starts — deferred to implementation.

## Open Questions

### Resolved during planning

- **Exact 4.x target (Q1 from origin)**: **4.2.0**. Rationale in D4.
- **Java baseline bump**: confirmed Java 11 (forced by kafka-clients 4.x minimum; cross-referenced with research).
- **Single-module vs dual-module**: single, locked in D1 and origin brainstorm.
- **Which API migrations are real vs hypothetical**: audit shows only `ProducerWrapper.sendOffsetsToTransaction(Map, String)` deprecated overload needs removal + `MockConsumer(OffsetResetStrategy)` + `ConsumerGroupMetadata(String)` test migrations. `poll()` and `committed()` already use forward-compatible forms.
- **Should 0.6.x get a maintenance branch (Q from origin)**: **no** — dropped entirely (origin decision D2).
- **Lombok/exception class refactor needed**: **no** — Jabel removal alone fixes the compile failure. Exception-class simplification deferred to separate housekeeping task.

### Deferred to implementation

- **Exact TestContainers `cp-kafka` tag for Kafka 4.2.x** (Q2 from origin) — requires checking TestContainers' supported tags and Confluent Platform release mapping at the time of implementation.
- **Whether downstream modules (`parallel-consumer-vertx`, `parallel-consumer-reactor`, `parallel-consumer-mutiny`, `parallel-consumer-examples`) reveal additional compile errors once Jabel is out and Kafka is at 4.2** (Q3 from origin) — only knowable by building. Mutiny in particular had its own `release.target=9` override; removing that plus parent moving to 11 should be clean but needs verification.
- **Whether any PC source unintentionally uses Java 8-specific API shape that breaks at 11+** (Q4 from origin) — vanishingly unlikely (Java 8 → 11 is source-compatible) but a compile will confirm.
- **Whether the 3.9.1 regression compile reveals any forward-compatibility regression** — pure mechanical check during implementation; behavior expected but not proven until CI matrix runs.

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

**Dependency graph of the six units:**

```
Unit 1: Jabel removal + version bump + Java 11 baseline
    │  (unblocks compile against kafka-clients 4.x)
    ▼
Unit 2: Bump kafka.version to 4.2, update TestContainers CP image
    │  (reveals remaining compile/test errors)
    ▼
Unit 3: Migrate deprecated API call sites (ProducerWrapper, MockConsumer uses, ConsumerGroupMetadata(String) uses)
    │  (main + test code green against 4.x)
    ▼
Unit 4: Downstream module audit (vertx, reactor, mutiny, examples)
    │  (verify compile + test green everywhere)
    ▼
Unit 5: CI: flip test-kafka-compat to 3.9.1 regression check; main build is now 4.2
    │  (gates both surfaces)
    ▼
Unit 6: Docs (AGENTS.md, README, CHANGELOG) — Java 11 minimum, Kafka 4.2 default, 0.6.x freeze
```

Unit 1 is the keystone — it single-handedly turns 25 compile errors into 0. Units 2-4 work on top of it. Units 5-6 are configuration and narrative.

## Implementation Units

- [ ] **Unit 1: Remove Jabel, bump Java baseline to 11, version bump to 0.7.0.0-SNAPSHOT**

**Goal:** Unblock Kafka 4.x compile by dropping the Jabel cross-compilation hack and moving the bytecode target to Java 11. Same commit bumps the project version to the new 0.7.x major line to signal the breaking Java-baseline change.

**Requirements:** R1, R4, R6 (CI Java setup)

**Dependencies:** None — this is the keystone change.

**Files:**
- Modify: `pom.xml` (root)
- Modify: `parallel-consumer-mutiny/pom.xml` (remove redundant release.target=9 override)
- Modify: `parallel-consumer-core/pom.xml`, `parallel-consumer-vertx/pom.xml`, `parallel-consumer-reactor/pom.xml`, `parallel-consumer-examples/**/pom.xml` (version bump only)

**Approach:**
- Root pom: remove the Jabel property (`jabel.version`), the Jabel `<dependency>` block, the Jabel annotation-processor path in the compiler plugin config, and the `intellij-idea-only` profile's "disable Jabel" workaround (no longer needed).
- Change `<release.target>8</release.target>` → `<release.target>11</release.target>`. Source version stays at 17.
- Remove mutiny's `<release.target>9</release.target>` property.
- Bump `<version>` to `0.7.0.0-SNAPSHOT` in the parent pom. All child modules inherit the parent version.
- Leave Lombok version untouched.

**Technical design:** *(directional — not implementation spec)*

Before/after shape of the compiler-plugin config:

```
-- Before (Jabel: Java 17 source → Java 8 bytecode via annotation processor) --
<source>${source.version}</source>        <!-- 17 -->
<target>${release.target}</target>        <!-- 8  -->
<release>${release.target}</release>      <!-- 8  -->
<annotationProcessorPaths>
  <path>projectlombok</path>
  <path>bsideup.jabel</path>              <!-- removed -->
</annotationProcessorPaths>

-- After (plain Java 17 source → Java 11 bytecode) --
<source>${source.version}</source>        <!-- 17 -->
<target>${release.target}</target>        <!-- 11 -->
<release>${release.target}</release>      <!-- 11 -->
<annotationProcessorPaths>
  <path>projectlombok</path>
</annotationProcessorPaths>
```

**Patterns to follow:**
- `intellij-idea-only` profile in root `pom.xml` already shows what a Jabel-free config looks like (it overrides source/target/release to `${source.version}` = 17). Use that as structural reference, but target Java 11 not 17.

**Test expectation:** none — build config change, verified via compile-green on the default Kafka version. Unit tests and integration tests provide the behavioral signal, but they don't directly test this unit's content.

**Verification:**
- `./mvnw -pl :parallel-consumer-core -am clean compile -Dlicense.skip` succeeds against the existing (Kafka 3.9.1) configuration.
- `./mvnw -pl :parallel-consumer-core -am clean compile -Dkafka.version='[3.9.1,5)' -Dlicense.skip` succeeds against Kafka 4.x (currently fails with 25 errors; should be 0).
- No references to `jabel` remain in any `pom.xml` under grep.
- Project version is `0.7.0.0-SNAPSHOT` in the parent pom.

---

- [ ] **Unit 2: Bump `kafka.version` default to 4.2.0, update TestContainers CP image**

**Goal:** Make Kafka 4.2 the default client version for the 0.7.x line. Ensure integration tests spin up a matching broker.

**Requirements:** R1, R6

**Dependencies:** Unit 1 (compile must work against 4.x).

**Files:**
- Modify: `pom.xml` (root) — `<kafka.version>` property
- Modify: `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/BrokerIntegrationTest.java` — `FALLBACK_CP_IMAGE` constant and any hardcoded CP version assumptions

**Approach:**
- Change `<kafka.version>3.9.1</kafka.version>` → `<kafka.version>4.2.0</kafka.version>` (exact patch version may be 4.2.x — pick latest at implementation time).
- Update `FALLBACK_CP_IMAGE` from `confluentinc/cp-kafka:7.9.0` to the Confluent Platform tag that ships Kafka 4.2.x. **Deferred to implementation** — requires verifying the CP-to-Kafka mapping at impl time. If no CP tag is available, fall back to `apache/kafka:4.2.0` image if TestContainers supports it.
- Any hardcoded CP major/minor assumptions in the dynamic image logic (`BrokerIntegrationTest.java` line 82) should be sanity-checked.

**Execution note:** Run the integration test suite against the new broker image locally before committing.

**Test expectation:** existing integration tests green against the new broker. No new tests added here.

**Verification:**
- `./mvnw -pl :parallel-consumer-core -am verify -Dlicense.skip` succeeds end-to-end (surefire + failsafe) against the new default Kafka version.
- Integration tests spawn a broker that reports a 4.2.x version.

---

- [ ] **Unit 3: Migrate deprecated Kafka API call sites**

**Goal:** Replace the three classes of deprecated-in-3.x / removed-in-4.0 usages the audit surfaced. All replacements already exist in Kafka 3.9, so the code stays source-compatible with both client versions.

**Requirements:** R2, R3, R7, R8

**Dependencies:** Units 1 and 2 (can't see the real errors until the build runs against 4.x).

**Files:**
- Modify: `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ProducerWrapper.java` — remove the deprecated `sendOffsetsToTransaction(Map, String)` overload (lines ~94-101); callers already use the `(Map, ConsumerGroupMetadata)` overload.
- Modify: ~18 test files using `new MockConsumer<>(OffsetResetStrategy.EARLIEST)` — switch to the String-arg constructor (`new MockConsumer<>("earliest")` or equivalent). Full file list is the audit output:
  - `parallel-consumer-core/src/test/java/io/confluent/csid/utils/LongPollingMockConsumer.java` (ctor signature change: accept `String` instead of `OffsetResetStrategy`)
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/MockConsumerTest.java`
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/MockConsumerTestWithCommitTimeoutException.java`
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/MockConsumerTestWithEarlyClose.java`
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/MockConsumerTestWithSaslAuthenticationException.java`
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/ParallelConsumerOptionsTest.java`
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/AbstractParallelEoSStreamProcessorTestBase.java`
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/AbstractParallelEoSStreamProcessorConfigurationTest.java`
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/state/WorkManagerTest.java`
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/offsets/WorkManagerOffsetMapCodecManagerTest.java`
- Modify: test files that construct `new ConsumerGroupMetadata(CONSUMER_GROUP_ID)` — replace with `consumer.groupMetadata()` or a shared fixture:
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/internal/ProducerManagerTest.java` (lines 235, 258)
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/state/ModelUtils.java` (line 86)
  - `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/AbstractParallelEoSStreamProcessorTestBase.java` (line 123 `DEFAULT_GROUP_METADATA`)

**Approach:**
- `ProducerWrapper`: delete the string-arg overload entirely. Callers (`ProducerManager.java:243`) already pass `ConsumerGroupMetadata`, so no consumer-site changes needed.
- `MockConsumer` ctors: the simple replacement is `new MockConsumer<>("earliest")` / `"latest"` / etc. `OffsetResetStrategy.EARLIEST.toString().toLowerCase()` would have worked but the enum itself is deprecated for removal — drop the enum import.
- `LongPollingMockConsumer`: change its ctor to take `String offsetResetStrategy`, pass through to `super(String)`. Existing callers (listed above) update at the same time.
- `ConsumerGroupMetadata(String)` deprecation: prefer `consumer.groupMetadata()` when a live consumer exists; otherwise use a pre-built test fixture. The `DEFAULT_GROUP_METADATA` constant in `AbstractParallelEoSStreamProcessorTestBase` is a reasonable anchor for the test-fixture pattern.

**Execution note:** Characterization-first on the `ProducerWrapper` change — run the existing `ProducerManagerTest` suite before and after removal to confirm no caller relied on the string-arg wrapper.

**Patterns to follow:**
- `ProducerManager.java:243` already uses the modern form — match it.
- `LongPollingMockConsumer` is the canonical MockConsumer extension — follow its shape for test-helper signature updates.

**Test scenarios:**
- Happy path: all existing MockConsumer-based unit tests pass after the ctor migration (`./mvnw test -pl parallel-consumer-core`).
- Integration: `ProducerManagerTest` full suite green, demonstrating the `sendOffsetsToTransaction(Map, ConsumerGroupMetadata)` path is exercised.
- Edge case: any test that previously asserted on `OffsetResetStrategy` enum values (search for `.EARLIEST` / `.LATEST` in test assertions, not just ctor args) migrates to the equivalent String-valued comparison.

**Verification:**
- Zero references to `OffsetResetStrategy` remain in `parallel-consumer-core/src/test/**` after the unit.
- Zero references to `new ConsumerGroupMetadata(` remain; lookups go through `consumer.groupMetadata()` or the fixture.
- `ProducerWrapper` no longer has the string-arg overload.
- Full module build + test suite green on both Kafka 3.9.1 (via `-Dkafka.version=3.9.1`) and Kafka 4.2.

---

- [ ] **Unit 4: Downstream module audit (vertx, reactor, mutiny, examples)**

**Goal:** Verify that dropping Jabel and bumping kafka-clients doesn't break the downstream modules. Mutiny is the highest-risk module because it had its own Java version override.

**Requirements:** R1, R2

**Dependencies:** Units 1, 2, 3.

**Files:**
- Audit (no edits expected, but fixable if needed): `parallel-consumer-vertx/**`, `parallel-consumer-reactor/**`, `parallel-consumer-mutiny/**`, `parallel-consumer-examples/**`
- Modify only if compile errors surface.

**Approach:**
- Run `./mvnw clean verify -Dlicense.skip` at the reactor root with both Kafka 3.9.1 and Kafka 4.2 as `-Dkafka.version` arguments.
- For any compile/test failures surfaced, file as per-module sub-fixes inside this unit (don't blow up the plan — adjust in place).
- Mutiny module: confirm its Multi/Flow.Publisher code still compiles at release 11 (previously needed release 9; 11 is compatible).
- Examples: confirm example apps still build (they rarely surface unique issues).

**Test scenarios:**
- Happy path: reactor build green, all module tests green, both Kafka versions.
- Edge case: mutiny-specific tests still pass; no Flow.Publisher regressions.
- Integration: example modules' integration tests (if any) still run.

**Verification:**
- `./mvnw clean verify -Dlicense.skip` green on default Kafka version.
- `./mvnw clean verify -Dkafka.version=3.9.1 -Dlicense.skip` green (forward-compat check).
- Each module listed in parent reactor shows `SUCCESS` in the build summary.

---

- [ ] **Unit 5: Repurpose `test-kafka-compat` CI job as Kafka 3.9.1 regression check**

**Goal:** CI's main `Build and Test` tiers now run on Kafka 4.2 (via the new default). The old `test-kafka-compat` job inverts role: it becomes the regression check that Kafka 3.9.1 still works. Both are blocking.

**Requirements:** R2, R6

**Dependencies:** Units 1, 2, 3, 4 (CI needs the code to compile).

**Files:**
- Modify: `.github/workflows/maven.yml` (lines ~115-143, the `test-kafka-compat` job)

**Approach:**
- Rename the job to something like "Kafka 3.9.1 regression" to reflect new intent.
- Remove `continue-on-error: true` — the job is now a hard gate.
- Change the `-Dkafka.version` argument from `'[3.9.1,5)'` (range resolving to latest 4.x) to the fixed `3.9.1`.
- Update the step name and Codecov flag accordingly.
- Verify the job still uses the same cache/restore pattern as other jobs (no setup-java `cache: 'maven'`).

**Test expectation:** none — CI config change. Verification is that the workflow runs both tiers successfully on a subsequent push.

**Verification:**
- On push to the feature branch, CI runs two tiers: main `Build and Test` against Kafka 4.2 (default), plus `Kafka 3.9.1 regression` against 3.9.1.
- Both jobs green (or failing for legitimate reasons, not `continue-on-error` noise).

---

- [ ] **Unit 6: Documentation — AGENTS.md, README, CHANGELOG**

**Goal:** Call out the breaking changes for the 0.7.x line clearly. Users scanning the README should know immediately that 0.7.x requires Java 11 and targets Kafka 4.x.

**Requirements:** R9

**Dependencies:** Units 1-5 done — docs reflect landed reality.

**Files:**
- Modify: `AGENTS.md` — update Build Requirements (Java 11 minimum), Testing section (Kafka 4.x), remove Jabel mentions.
- Modify: `src/docs/README_TEMPLATE.adoc` — add a 0.7.x compatibility note near the top; regenerate `README.adoc` via `mvn process-sources`.
- Modify: `README.adoc` — regenerated from template (do not hand-edit).
- Modify: `CHANGELOG.adoc` — new `== 0.7.0.0` section listing: Java 11 minimum, Kafka 4.2 default, 3.9.1 compat, Jabel removed, 0.6.x frozen, pom `<developer><id>` now `astubbs` (carried forward from earlier audit).

**Approach:**
- AGENTS.md: adjust "JDK 17" line to mention Java 11 as the library's minimum target. Drop Jabel architecture bullet. Mention "Kafka 4.2 default, 3.9.1 compat-tested".
- README template: add a `NOTE::` or `IMPORTANT::` block near the top stating "0.7.x requires Java 11+ and defaults to Kafka 4.2. For Java 8 / Kafka 3.x, stay on 0.6.x (no longer maintained)." Regenerate the rendered README via the project's existing `mvn process-sources` step — do not hand-edit `README.adoc`.
- CHANGELOG: new top-of-file section with breaking-change callouts. Preserve the existing 0.6.0.0 entries below.

**Test expectation:** none — docs-only. Regeneration parity (template vs rendered README) verified by re-running the generator and confirming zero drift.

**Verification:**
- `grep -r "Jabel" AGENTS.md README.adoc` returns no matches.
- `grep -i "Java 11" AGENTS.md README.adoc` finds the new minimum-version notes.
- `CHANGELOG.adoc` has a `== 0.7.0.0` section above the existing 0.6.0.0 entry.
- `./mvnw -N process-sources -Dlicense.skip` produces no change to the committed `README.adoc`.

## System-Wide Impact

- **Interaction graph:** Compile-time-only for most units. CI matrix gains a new regression tier. No runtime wire-format changes.
- **Error propagation:** Jabel removal is pure build-tooling; no runtime behavior change. API migrations use already-available replacements — semantically identical.
- **State lifecycle risks:** None.
- **API surface parity:**
  - **Breaking for consumers on Java 8:** cannot use PC 0.7.x.
  - **Source-compatible for consumers on Java 11+ with Kafka 3.9.x:** same JAR works.
  - **Source-compatible for consumers on Java 11+ with Kafka 4.x:** same JAR works.
  - `ProducerWrapper.sendOffsetsToTransaction(Map, String)` is removed — but this was an internal wrapper method (deprecated), not public API. If any external caller relied on it, they'd need to switch to `(Map, ConsumerGroupMetadata)`.
- **Integration coverage:** The key cross-layer scenario is "PC running against a Kafka 3.9.1 broker from Java 11". Unit 4's matrix build covers this via CI.
- **Unchanged invariants:**
  - Public `ParallelConsumer`, `ParallelEoSStreamProcessor`, and options APIs — signature-stable.
  - Vertx, Reactor, Mutiny module public entry points — unchanged.
  - Example apps' demo code — unchanged.
  - Offset-encoding wire format and metadata shape — unchanged (PC's own format, not Kafka's).

## Risks & Dependencies

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Mutiny module breaks under Java 11 target for a reason unrelated to Jabel | Low | Medium | Unit 4 builds Mutiny explicitly; any issue surfaces before merge. Fallback: retain a `release.target=11` pin on mutiny as a no-op guard. |
| TestContainers `cp-kafka` tag for Kafka 4.2 not yet available in a stable TestContainers release | Medium | Medium | Unit 2 flags this; fallback to `apache/kafka:4.2.0` image (TestContainers supports it). Worst case: pin integration tests to the highest available CP-based Kafka until tagged image is live. |
| Downstream module compile surfaces real Kafka 4.x API calls not seen in core audit | Low | Medium | Unit 4 is deliberate; fix in-place if any appear. |
| PC on Java 11 surfaces an accidental Java 8-specific source pattern (e.g., a String.format locale default) | Very low | Low | Compile is the test; full test matrix in Unit 4 flushes out any behavioral surprises. |
| `test-kafka-compat` rename + config change breaks the existing CI artifact linkage (Codecov flag collisions, cache key misses) | Low | Low | Unit 5 verifies first run post-change; rollback is a one-line git revert. |
| Implementation reveals more MockConsumer / `OffsetResetStrategy` references in modules beyond core | Medium | Low | Unit 3 lists the known set from the core audit; downstream modules get the same treatment when Unit 4 surfaces them. |
| Kafka 4.2 behavioral deltas (`linger.ms` default = 5ms, `flush()` in send callback throws) cause subtle test flakes | Low | Medium | Behavioral changes documented in the brainstorm's Research section; watch for timing-sensitive test failures during Unit 2/4. |

## Phased Delivery

### Phase 1 — Unblock (Units 1, 2)

The keystone. After these two units the build compiles against Kafka 4.2. Everything else is incremental cleanup.

### Phase 2 — Migrate (Units 3, 4)

Fix the deprecated-API call sites across main + test + downstream modules. End of phase: full test suite green on both Kafka versions.

### Phase 3 — Land the new shape (Units 5, 6)

CI reflects the new reality. Docs tell the story. Branch is ready for user review (but still unmerged per origin G5 — waits for 857 resolution and release-pipeline stabilization).

## Documentation / Operational Notes

- The feature branch (`feat/kafka-4-support` off `origin/master`) holds all work until merge approval.
- No consumer-facing rollout steps — this is a library version change, consumers pull in 0.7.x when they're ready.
- Post-merge (much later): cut the last 0.6.x release as a maintenance snapshot for the frozen line. Not part of this plan.
- `docs/solutions/` candidate: a short write-up on "Jabel cross-compile vs Java 11 dependencies" would be useful institutional memory after this lands. Filed as a follow-up.

## Sources & References

- **Origin document:** [docs/brainstorms/2026-04-23-apache-kafka-4-support-requirements.md](../brainstorms/2026-04-23-apache-kafka-4-support-requirements.md)
- **Kafka 4.0 Upgrade Guide:** [kafka.apache.org/40/getting-started/upgrade](https://kafka.apache.org/40/getting-started/upgrade/)
- **Spring Kafka 4.0 GA (industry precedent):** [spring.io/blog/2025/11/18/spring-kafka-4](https://spring.io/blog/2025/11/18/spring-kafka-4/)
- **KIP-1124 Clear Kafka Client upgrade path:** [cwiki.apache.org/confluence/display/KAFKA/KIP-1124](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1124:+Providing+a+clear+Kafka+Client+upgrade+path+for+4.x)
- **Failing experimental CI job:** [astubbs/parallel-consumer run 24811153191 — Kafka Compat (experimental 4.x)](https://github.com/astubbs/parallel-consumer/actions/runs/24811153191)
- **Related code anchors:** `pom.xml` (compiler plugin config, Jabel setup), `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ProducerWrapper.java` (deprecated overload), `.github/workflows/maven.yml` (`test-kafka-compat` job)
