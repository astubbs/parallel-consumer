# In-flight & parked work

> Shared, cross-branch working notes (not an issue tracker), kept on `master` so any branch or session
> can see them. Records work that is parked, in progress on other branches, or otherwise not obvious
> from `git log`. Keep it current when context-switching. Last updated: 2026-07-28.
>
> **The durable fork↔upstream mapping now lives in a machine-readable cache:**
> [`src/docs/development/upstream-map.yaml`](../src/docs/development/upstream-map.yaml) is the
> source of truth for which fork branch/PR maps to which upstream issue/PR and its status
> (editorial analysis in `src/docs/development/upstream-pr-analysis.adoc`). This file stays for
> *transient* working notes -- what's parked/in-flight right now. When adding a branch that maps
> to an upstream issue/PR, record the mapping in `upstream-map.yaml`, not (only) here.

## 0.6.0 — first fork release (off `master`)

The fork's debut is the **rebrand already on `master`** (`bz.stub.parallelconsumer`, Java 8,
Jabel intact). It builds green and **already snapshot-published** (5 successful publish runs on
2026-04-22; the Central snapshot has likely aged out of the ~90-day retention since). No Java-baseline
change. Release = strip `-SNAPSHOT` → `0.6.0.0` and merge to `master`. Release blockers are tracked by
Antony separately (not in this doc).

**Release mechanics:** no `maven-release-plugin`; the pom `<version>` is the single source of truth.
Merge to `master` → `.github/workflows/publish.yml` runs after "Build and Test" succeeds and deploys via
the `maven-central` profile (`central-publishing-maven-plugin`, GPG signed). A non-`-SNAPSHOT` version
also tags `v<version>` and cuts a GitHub release. See `AGENTS.md` "Releasing".

## 0.7.x — Java baseline + Kafka 4 (branch `feat/java-17-baseline`, PR #53 — draft)

WIP toward Kafka 4 support. **The only reason to move off Java 8 is Kafka 4.** kafka-clients 4.x require
**Java 11** (brokers/tools require 17, but PC only depends on the client lib); the target baseline is
Java 11 — "support at least what Kafka supports; don't be stricter than them."

Approaches still open (decided when the work actually starts):
- **Remove Jabel + rewrite** the code's Java 14+ switch-expressions/text-block to Java 11 syntax — real
  refactor, ~9 core files incl. the offset-encoding hot path (`BitSetEncoder`, `RunLengthEncoder`,
  `EncodedOffsetPair`, `OffsetBitSet`) + 2 test files + 1 text block.
- **Keep Jabel at `--release 11`** — currently breaks Lombok `@StandardException` constructor generation
  (25 errors); unproven whether a Lombok bump / processor-order tweak fixes it. If so → Java 11 with
  **zero source refactor**. Worth trying first.
- **Native Java 17** — zero source changes but stricter than Kafka's clients (drops Java 11-16 users).
  Dispreferred.

Jabel is what lets `javac` accept Java 17 syntax while emitting older-release bytecode — that's how the
code "does Java 8" today; keep it at `release 8` and no change is needed at all. PR #53 currently holds a
provisional state (Jabel removed, `release=17`) plus the Kafka 4 research docs (which live on that branch).

**Kafka 4 units still to do** (plan: `docs/plans/2026-04-23-001-feat-apache-kafka-4-support-plan.md`, on
the PR #53 branch):
- **Unit 2** — bump `kafka.version` `3.9.1` → `4.2.x`; update the TestContainers CP image
  (`BrokerIntegrationTest.FALLBACK_CP_IMAGE`, currently `confluentinc/cp-kafka:7.9.0`).
- **Unit 3** — migrate deprecated/removed-in-4.0 APIs: `ProducerWrapper.sendOffsetsToTransaction(Map,String)`;
  `MockConsumer(OffsetResetStrategy)` → String ctor; `new ConsumerGroupMetadata(String)` → `groupMetadata()`.
- **Unit 4** — downstream module audit under Kafka 4.2.
- **Unit 5** — CI: flip `test-kafka-compat` to a blocking Kafka 3.9.1 regression check; main build on 4.2.
- **Unit 6** — docs for Kafka 4.2 default + 3.9.1 compat.
- **Deferred further:** `parallel-consumer-share` module (KIP-932 Share Groups); exception-class
  custom-ctor simplification.

## In-flight on other branches / worktrees

- **`fix/859-metrics-leak-plus-cherrypicks`** (**PR #57, open**; worktree `.claude/worktrees/dev-cc`) —
  5 commits ahead of master, rebased clean, targets `master`. Fixes PCMetrics memory leak (#859):
  duplicate Micrometer meter re-registration on partition assignment/revocation. Owns `PCMetrics.java`,
  `PCMetricsDef.java`, `PartitionState.java`, `PartitionStateManager.java`, `ShardManager.java`;
  regression test `PCMetricsTest859.java` + a P1-review-hardening pass. **Now bundles** cherry-picks
  #893 (offset accuracy on assignment) and #905 (max-queued-records-per-shard metric) into the one PR
  rather than a stack. Supersedes the old 3-deep cherry-pick stack (closed **#42** → open-then-closed
  **#43** → closed **#45**); the old `bugs/859-pcmetrics-leak-v2` branch still exists on origin.
- **`bugs/857-paused-consumption-multi-consumers-bug`** — silent-stall-after-rebalance. **Root cause
  found + fixed:** `synchronized(commitCommand)` deadlock between poll thread (`onPartitionsRevoked`)
  and control thread (`commitOffsetsThatAreReady`), replaced with `ReentrantLock.tryLock()`. Chaos test
  ~20%→~80%; residual failures are a test-harness `ConcurrentModificationException`, not production.
  5 fixes, "ready for PR." (Memory note is old — re-verify before relying on it.) Relates to upstream
  PR #548 (same deadlock) and issues #326/#541/#546. User weighing a larger single-thread refactor
  (merge poll + control threads) to kill this bug class.
- **`bugs/912-vertx-stream-memory-leak`** — clear JStream deque on close (#912). Fix + regression test
  (`JStreamMemoryLeakTest912`) committed and pushed to origin; **no PR yet.** Touches only the vertx
  module (`JStream*Processor.java`) — isolated from core. Production memory leak (issue #912, Jan 2026).
  **Ready to resume:** rebase onto master → open PR.
- **`fix/909-stale-container-replacement`** — regression test for stale container at same offset.
- **`astubbs/orca`** (worktree `/Users/astubbs/orca/workspaces/parallel-consumer/orca`) — 9 ahead / 9
  behind master, diverged, clean. CI/tooling: Claude Code Review + PR Assistant workflows, PR-dependency
  -check workflow, CI matrix tweaks. Stale-ish; needs rebase onto master.
- **Upstream-PR isolation branches:** `upstream-pr-893`, `upstream-pr-905`, `pr-909-temp`,
  `cherry-pick/893-offset-reset`, `cherry-pick/905-max-shard-metric`, `refactor/test-hardening`.
- **Stale/backup:** `backup/*` (pre-rebase snapshots), `dev-cc` & `master-confluent` (pinned at
  pre-rebrand `7f290122`), `dev/self-hosted-runner`, 4× `dependabot/maven/*`.

## Parallel-safe work while PR #57 is in flight

PR #57 owns the metrics/state core: `metrics/PCMetrics.java`, `metrics/PCMetricsDef.java`,
`state/PartitionState.java`, `state/PartitionStateManager.java`, `state/ShardManager.java`. The verdicts
below are purely about **file collision with #57** — pick parallel-safe work to avoid rebase churn; run
the sequenced items after #57 lands. (Backlog source: `src/docs/development/upstream-pr-analysis.adoc`.)

**Collides with #57 → sequence after it merges (do NOT run in parallel):**
- **#857** deadlock fix (`bugs/857-...`) — touches `PartitionState`/`PartitionStateManager`. Fixed &
  "ready for PR" but shares files. Pair with #909 as one post-#57 rebalance-correctness stream.
- **#909** stale-container replacement (`fix/909-stale-container-replacement`) — touches `ShardManager`.
- **#51** virtual threads (`features/enable-virtual-threads`) — also edits `PCMetrics.java`.

**Parallel-safe (no overlap with #57), ranked by readiness:**
- **#912 vertx memory leak** — *ready*, branch done & pushed, just needs rebase + PR (see above). Best
  immediate parallel pick.
- **0.6.0.0 release** (`release/0.6.0.0`, PR #56, worktree `.claude/worktrees/pc-release`) — pom/docs
  only. Already running in parallel.
- **Logging-verbosity cleanup** — batch issues #629/#631/#640 into one PR (`ConsumerOffsetCommitter`,
  `RemovedPartitionState`, `AbstractParallelEoSStreamProcessor`). Low-effort, high-ROI.
- **Security dep bumps** — #851 (postgres), #913 (assertj); pom-only. Logback already in flight on
  `ci/tag-triggered-release` (bumped 1.5.19→1.6.0).
- **Contributor-friction build fixes** — #162 (mvn compile without test-jar), #861 (`ManagedTruth` not
  found), #906 (pom version mismatch). Small, unblocks external contributors.
- **Issue #40** — dedup `MockConsumer*` test classes (test-only; duplication bot keeps flagging these).
- **#915 batch construction strategy** (cherry-pick upstream, closes 4-yr issue #266) — medium effort.
- **DLQ** (#310 / revive #366) — most-demanded missing feature; large, idea-bank spec not a live branch.

## Deferred dependency upgrades (branch `deps/cap-non-major-upgrades`, 2026-07-29)

Ahead of the 0.6.0.0 patch release we bumped **every dependency + build plugin to its newest
*non-major* version** and deliberately capped anything whose only newer release is a major (fork /
patch-release risk aversion). Enforced with `versions-maven-plugin` `-DallowMajorUpdates=false` plus a
ruleset (`bin/deps-version-rules.xml`) that also ignores pre-releases (alpha/beta/`-Mn`/RC/snapshot)
and Confluent `-ce`/`-ccs` Kafka builds — without the `-ce` filter, kafka "latest" mis-resolves to
`8.3.0-ce` (a Confluent build), not Apache. Build is green (`mvn -Dlicense.skip -DskipTests verify`, all
11 modules). **The following were intentionally NOT taken and still need updating later:**

**Majors — need a deliberate migration (not for a patch release):**
- **kafka-clients / kafka-streams / kafka-streams-test-utils** `3.9.1 → 4.3.1` — Apache Kafka 4;
  requires the Java 11 baseline. Already tracked above under **0.7.x — Java baseline + Kafka 4** (Unit 2).
- **junit-jupiter** `5.14.4 → 6.1.2` + **junit-platform** `1.14.4 → 6.1.2` — JUnit 6 requires Java 17;
  blocked by the same Java-baseline move. Do it with the Kafka 4 / Java-baseline work.
- **org.testcontainers:testcontainers** `1.21.4 → 2.0.5` — Testcontainers 2.x (core artifact only; the
  `kafka`/`postgresql`/`junit-jupiter` TC modules already moved to 1.21.4 in this pass).
- **io.vertx** vertx-junit5 / vertx-web-client `4.5.31 → 5.1.5` — Vert.x 5.
- **io.smallrye.reactive:mutiny** `2.9.5 → 3.3.0` — Mutiny 3.
- **com.github.tomakehurst:wiremock-jre8** `2.35.2 → 3.0.1` — WireMock 3 (artifact renamed to
  `org.wiremock:wiremock`; test-only).

**Micrometer family — source-incompatible, held back even though it's NOT a major:**
- **micrometer-core** (`1.13.0`) + **micrometer-registry-prometheus** (`1.12.2`) → latest `1.17.x`.
  Micrometer 1.13 renamed the Prometheus registry package `io.micrometer.prometheus` →
  `io.micrometer.prometheusmetrics` (and reworked the artifact), so `example-metrics/CoreApp.java` fails
  to compile against 1.17. Both are pinned with in-pom comments. **To upgrade:** migrate the `CoreApp`
  imports + registry construction, then bump the whole micrometer family together (keep the two aligned).

**Build plugins — only pre-releases available, held by the risk policy:**
- Maven-4-era plugins offered only as betas/milestones: `maven-clean/deploy/install/jar/resources/source/
  compiler` `4.0.0-beta-*`; `maven-surefire`/`maven-failsafe` `3.6.0-M1`; `maven-site-plugin` `4.0.0-M16`
  (left at `4.0.0-M15`). Revisit once these reach GA.

## CI reliability / gate issues (follow-up work)

Surfaced while diagnosing PR #56 (docs-only) showing 4 red checks. **None were caused by the docs** —
all are pre-existing job/gate problems. Only three checks actually gate merge (ruleset on `master`):
**Unit Tests, Integration Tests, Performance Tests**. Everything else is advisory. Track and fix:

- **Parallel integration flakiness — RESOLVED via forked per-broker mode (PR #68, "Step 1", 2026-07-28).**
  Enabling JUnit thread-parallelism (`-Dparallel-tests=true`) made integration flaky (~2/104 per run,
  rotating) because all ~104 tests contend ONE shared TestContainers broker. **Fix:** run failsafe
  **forked** (`-DforkCount=4 -DreuseForks=true`) so each JVM fork gets its own broker — reliable (5/5 Mac;
  green GitHub-hosted 6:16 vs ~11:38 sequential) and faster, and it masks nothing (each test runs on an
  uncontended broker). Wired into `maven.yml` (GitHub-hosted) + `self-hosted-tests.yml`. **Key finding:**
  the contended `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` failure maps to the REAL **#857** deadlock
  (`synchronized(commitCommand)` in `onPartitionsRevoked`), *not* test flakiness — contention was exposing
  a real bug (hence AGENTS.md's new "be extremely careful modifying tests under stress" rule). Full
  write-up: `docs/solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`.
  **"Step 2" DEFERRED:** retry full thread-parallel on a shared broker to *validate* the #857 deadlock is
  gone (not merely avoided) — only **after** #857/#29 finishes and merges on its own merits (it's a
  ~454-line WIP concurrency refactor, "root cause still open"). Reproducer for then: `-Dparallel-tests=true`.
- **NEXT (follow-up to the above): unit tests are now the CI long pole (~8.5 min) — fork them too.** Step 1
  forked the *integration* suite (failsafe); *unit* tests (surefire) still run **sequential** (`ci` profile
  `parallel-tests=false` applies to both), so they became the slowest required check — **8m31s** vs forked
  integration 6m16s (run `30352770661`, 2026-07-28). The unit job runs surefire across **all** modules
  (core/vertx/reactor/mutiny/examples) serially, and a few heavy classes dominate:
  **`RunLengthEncoderTest` 81.7s** (pure-CPU encoder — an outlier, ~16% of the run), then
  `VertxBatchTest`/`ParallelEoSStreamProcessorTest`/`CoreBatchTest` 41-56s, and reactor/mutiny/streams
  batch+app tests 30-35s each. **Two follow-ups:**
  1. **Fork the unit tests** (surefire `forkCount>1`) — same proven per-fork-isolation pattern as the
     integration fix; unit tests already carry `@Isolated`/`@ResourceLock` for static-state races, which
     per-fork isolation handles cleanly → should drop unit toward ~1.5-2 min, but **floored at
     `RunLengthEncoderTest`'s 81s** (forking distributes *classes*, so the slowest single class is the limit).
  2. **`RunLengthEncoderTest` (81s)** — trim / parameterize-down / parallelise its methods so it stops being
     the floor.
  Apply the same rigor (AGENTS.md): any new failure under parallel = establish **contention-artifact vs real
  bug** before masking. Not started — the harness (`scratchpad/fork-harness.sh`) and forked-mode invocation
  are ready to reuse.
- **`Integration Tests` was flaky (required → blocked merges) — FIX IN FLIGHT (PR #63).** Failed
  intermittently on `BrokerIntegrationTest.ensureTopic` → `TimeoutException` (e.g. #56, #61). Root cause:
  `ensureTopic` was a drifted duplicate of `KafkaClientUtils.createTopics` that waited only
  `.get(1, TimeUnit.SECONDS)`, so topic creation on a cold/loaded CI broker timed out and hard-failed.
  **Fix (PR #63):** consolidate onto one blocking `KafkaClientUtils.createTopic` helper (generous 60s
  bound with a clear timeout message, classifies `TopicExistsException`); `ensureTopic` delegates. Not a `rerunFailingTestsCount` band-aid —
  removes the cause. Remove this note once #63 merges.
- **`ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` is flaky (unit →
  hits the *required* Unit Tests gate).** A Mockito `verify()` interaction race: the assertion at
  `ProducerWrapper.sendOffsetsToTransaction` (via `ProducerManager.initProducer`) intermittently reports
  the interaction as not-as-expected (`MockitoAssertionError`, ~22s elapsed → looks timing-related).
  Non-deterministic — failed 1/245 in a local `-pl core test` run, then passed 3/3 on isolated rerun.
  **Distinct** from the Integration TestContainers flake (this is a pure unit/Mockito test, no broker)
  and from the `@StandardException` compile flake (this is runtime interaction, not compilation).
  Uncovered 2026-07-28 while validating the IRE fix. **Fix:** investigate the transaction/offset
  interaction timing in the test (likely an Awaitility/async ordering assumption); consider
  `rerunFailingTestsCount` as a stopgap since it's a *required* gate.
- **`@StandardException` compile flake (intermittent → hits the *required* Unit Tests gate).** A
  main-source annotation-processing race: Lombok's generated exception constructors are sometimes not
  visible when their callers are type-checked, giving `error: constructor ... cannot be applied to given
  types` / `recursive constructor invocation`. Non-deterministic — the *same code* passes or fails
  between runs (the `logback 1.5.19 → 1.6.0` bump build `30311773374` failed on it; logback is a red
  herring, `test`-scope). **Distinct** from the Integration flake above (compile-time, not broker
  startup) and also caught the release pipeline's maiden publish run. **Fix:** simplify/harden the
  exception-class constructors — drop the custom `(String, Throwable, Object... args)` ctor, or stop
  applying `@StandardException` to classes that declare their own ctor (the deferred "exception-class
  custom-ctor simplification" in the 0.7.x plan). Do this before trusting the release pipeline.
  **FIXED (`fix/standardexception-race-internalruntimeexception`, PR #65):** all 12 `@StandardException`
  exception classes now hand-write their constructors, so *no* exception constructor depends on
  annotation processing and the race is structurally impossible. Rollout was two-stage: (1) minimal fix
  of `InternalRuntimeException` (the one class mixing `@StandardException` with a hand-written ctor
  delegating via `this(...)` - the demonstrated fatal trigger), then (2) escalation to the remaining 11
  pure classes after PR #65's own **PIT** run flaked *fatally* on them (`OffsetDecodingError`,
  `NoEncodingPossibleException`, `ParallelConsumerException`, `ExceptionInUserFunctionException`, the
  `*EncodingNotSupported*` family, etc.) - proving the "transient diagnostics recover" assumption wrong
  and meeting the pre-set escalation criterion. To keep the near-identical boilerplate under the
  duplication/file-similarity detectors, each class is **trimmed to only the constructors it (or a
  subclass's `super(...)`) actually uses** rather than the full four: e.g. `OffsetDecodingError` keeps only
  `(String, Throwable)`, the `RunLength*`/`NoEncodingPossible` classes only `(String)`,
  `KafkaStreamsEncodingNotSupported` only its custom no-arg. `PCRetriableException` (public user-throwable)
  keeps all four; `InternalRuntimeException` keeps its four + `(String, Throwable, Object...)` varargs
  (now `super(...)`) + `msg()` factory. `ExceptionConstructorsTest` reflectively verifies whatever ctors
  each class exposes. Public API unchanged for the classes users construct.
- **`Duplicate Code (jscpd)` absolute cap is below baseline → fails on EVERY PR.** jscpd cap is 4% but
  the codebase baseline is already 4.22% (85 clones), so the absolute-limit rule red-flags all PRs even
  when "max increase vs base" is +0.00% (as in #56). PMD CPD is fine (3.60% < 5%). **Fix:** raise
  `INPUT_JSCPD_MAX_PCT` above baseline (e.g. 5%, matching PMD) and rely on the max-increase-vs-base gate,
  which is the real safety net. Not a merge blocker (advisory), but noisy on every PR.
- **`Kafka Compat (experimental 4.x)` is known-broken until the Kafka 4 migration.** Compile failure
  under kafka-clients 4.x (`MockProducer<>` type inference in `AbstractParallelEoSStreamProcessorTestBase`,
  and the removed-API set in 0.7.x "Unit 3" above). **Fix:** mark the job `continue-on-error` /
  non-blocking until 0.7.x lands so it stops red-flagging unrelated PRs. Advisory, not a blocker.
- **`Mutation Testing (PIT)` cascades from any flaky test.** PIT requires a fully green suite, so a
  single flaky/timeout test aborts the whole run ("1 test did not pass without mutation"). **Fix:**
  depends on the Integration flake fix; consider not gating PIT on the integration suite. Advisory.
- **Path-filter inconsistency.** `Build and Test` (and `SpotBugs Baseline`) are *skipped* on docs-only
  PRs, but Integration / PIT / Kafka-Compat are *not* filtered the same way, so they run and fail on
  changes that touch no code. **Fix:** align the `paths`/`paths-ignore` filters across these jobs so a
  docs-only change runs a consistent (or fully skipped) set.
