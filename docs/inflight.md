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
  blocked by the same Java-baseline move. Do it with the Kafka 4 / Java-baseline work. **Second blocker:**
  `archunit-junit5` will not run on JUnit 6 and there is no `archunit-junit6` engine yet (TNG/ArchUnit
  [#1556](https://github.com/TNG/ArchUnit/issues/1556)) — the ArchUnit tests must be migrated/re-wired
  before the JUnit 6 bump can land.
- **org.testcontainers:testcontainers** `1.21.4 → 2.0.5` — Testcontainers 2.x (core artifact only; the
  `kafka`/`postgresql`/`junit-jupiter` TC modules already moved to 1.21.4 in this pass).
- **io.vertx** vertx-junit5 / vertx-web-client `4.5.31 → 5.1.5` — Vert.x 5.
- **io.smallrye.reactive:mutiny** `2.9.5 → 3.3.0` — Mutiny 3.
- **com.github.tomakehurst:wiremock-jre8** `2.35.2 → 3.0.1` — WireMock 3 (artifact renamed to
  `org.wiremock:wiremock`; test-only). **Side effect while it stays on 2.x:** wiremock-jre8 2.35.2 drags
  in an ancient `net.bytebuddy:byte-buddy 1.12.18` that wins the version conflict and lacks the `JAVA_V21`
  field mockito 5.23 needs (`MockitoInitializationException` → every Mockito unit test errors). Worked
  around by pinning `byte-buddy`/`byte-buddy-agent` to `1.17.7` (mockito's version) in
  `dependencyManagement` (`byte-buddy.version` property). **Remove that pin when wiremock moves to 3.x.**

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

### SpotBugs 4.10 surfaced 11 pre-existing concurrency findings (follow-up, not this PR)

The `spotbugs 4.8.6 → 4.10.3` bump in this PR expanded the multithreading (`AT_*`) detectors, so the
`SpotBugs` PR job reports **11 "new" bugs**. They are **not introduced here** — this PR changes no
`src/main/**/*.java`; the CI baseline was just generated with the *old* 4.8.6, so 4.10.3's new detectors
fire on **existing** code. All are in `parallel-consumer-core`, all real-looking thread-visibility/atomicity
observations worth a proper look as their own task (several sit in the poll/control-thread coordination
that the **#857** single-thread refactor is already reworking — fixing piecemeal now may conflict/be moot):

- **`AT_NONATOMIC_OPERATIONS_ON_SHARED_VARIABLE`** (8) — non-atomic read-modify-write (e.g. `count++`) on a
  shared field:
  - `AbstractParallelEoSStreamProcessor.numberOfAssignedPartitions` (lines 420, 448, 463)
  - `ConsumerManager` counters `noWakeups` (143, 226), `erroneousWakups` (201, 233), `correctPollWakeups` (111)
- **`AT_STALE_THREAD_WRITE_OF_PRIMITIVE`** (3) — primitive written in one thread may not be visible to another
  (missing `volatile`/sync):
  - `AbstractParallelEoSStreamProcessor.lastWorkRequestWasFulfilled` (979)
  - `ConsumerManager.commitRequested` (287)
  - `RetryQueue.closed` (287)

**Action:** don't block this deps PR. After merge, master's push build regenerates the SpotBugs baseline
with 4.10.3, so these drop out of "new". Track the actual fixes (make the counters `AtomicInteger`/`Atomic
Long`, mark the flags `volatile`, or fold into the #857 threading rework) as a follow-up. Note `ConsumerManager
.erroneousWakups` is also a pre-existing typo ("Wakups") worth fixing while there.

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
- **DONE (PR #69): unit suite parallelised by FORKING (surefire `forkCount=1C`), not threading.** The `ci`
  profile now forks the unit suite one-JVM-per-core (`forkCount=1C`, `reuseForks=true`), keeping
  `parallel-tests=false`. Core unit dropped **5:14 → 1:39** (259 tests, 0 failures) on a 12-core box, and it
  auto-scales (`1C` = 2 forks on GitHub's 2-core gate, = core count on the self-hosted box). Forking is faster
  AND reliable where thread-parallel (2:32) is flaky — separate processes don't share the static state that
  threads race on. Unblocked by the RunLengthEncoderTest fix (removed the ~85s single-class floor) + the arch
  rule (keeps container tests out of the forked unit suite). Write-up:
  `docs/solutions/test-flakiness/unit-tests-parallelise-by-forking-not-threading-2026-07-29.md`.
  - **Follow-up (jacoco under forking):** `prepare-agent` writes ONE `jacoco.exec` in append mode; N forks
    appending concurrently can corrupt/undercount coverage. If CI coverage looks wrong, give each fork its own
    exec file (`destFile` with `${surefire.forkNumber}`) + `jacoco:merge` before the report.
  - **`RunLengthEncoderTest` ~59s floor** (the `testSimultaneousWithOverflowErrors` INT case genuinely walks
    ~2.1B offsets in `OffsetSimultaneousEncoder.invoke()`) — needs a delta-aware `invoke()` (main-code
    optimisation), not urgent.
- **DISABLED (PR #69): the experimental "Kafka Compat (experimental 4.x)" CI job** (`test-kafka-compat` in
  `.github/workflows/maven.yml`) is turned off via `if: false`. It currently fails and adds a red X of noise
  to every PR (it is `continue-on-error`, so it never gated merges). **Re-enable when the Kafka 4.x migration
  work starts** by restoring `if: github.event_name == 'pull_request'` — that work will make it pass.
- **pitest (Mutation Testing) was pre-existing RED — root cause found + fixed (PR #69): coverage-minion OOM
  at `-Xmx1g`.** The single coverage-generation minion (runs all target tests once to map coverage) crashed
  with `UNKNOWN_ERROR`; confirmed locally that 1g crashes and 4g completes → raised to `-Xmx2g`. Also switched
  `-Dthreads` 1→2: PIT runs mutation analysis in separate **minion JVMs** (process-parallel, so — like our
  forked unit suite — it is SAFE from the shared-static-state races that break JUnit thread parallelism), so
  threads is the right lever; kept `threads*heap` (2×2g) within runner RAM. **Further speedup still on the
  table (deferred — user said try parallel first):** scope PR mutation to only the classes changed vs base
  (pitest SCM / `+GIT`), keeping the full `internal.*` sweep for push/nightly — biggest win, and it also
  shrinks the coverage minion's load. Fine-tune threads (2 vs 3) with a multi-class benchmark once green.
  - **UPDATE (measured on CI): heap fix CONFIRMED working** — coverage gen now passes ("Calculated coverage
    in 425s", no OOM). **But the job then TIMES OUT at the 300-min cap** in the mutation phase: the history
    cache key includes `hashFiles('**/src/main/**/*.java')`, so any main-code change makes it cold → full
    22-unit sweep of `internal.*`, and each mutant re-runs `targetTests=parallelconsumer.*` which includes the
    SLOW integration tests → ~5h. `threads=2` on a 2-core runner can't fix that. **The real fix is the scope
    restriction (now clearly needed, not optional):** (a) restrict `targetTests` to the fast unit tests only
    (huge — stops re-running Docker integration tests per mutant), and/or (b) mutate only classes changed vs
    base on PRs (pitest SCM / `+GIT`), keeping the full sweep for push/nightly. `-Dthreads=$(nproc)` is moot
    for speed here (nproc=2 on the gate = current 2).
  - **DONE (PR #69): implemented (a).** Added `-DexcludedTestClasses="io.confluent.parallelconsumer.integrationTests.*"`
    to the PIT step, so per-mutant runs no longer re-run the slow TestContainers integration tests (they live in
    `integrationTests` packages, enforced by #69's ArchUnit rule). Mutations on `internal.*` are exercised by the
    fast unit tests. Trade-off: a mutation only an integration test could kill now shows as *survived* — acceptable
    for an advisory/non-gating signal. Option (b) (changed-classes-only via pitest SCM) still available if needed.
  - **INCREMENTAL HISTORY REMOVED (PR #69) — pitest 1.25.x needs a paid plugin for it.** #73 bumped pitest
    1.17.4 → 1.25.8, which dropped the built-in file-based history: `-DwithHistory` *and* the explicit
    `-DhistoryInputFile`/`-DhistoryOutputFile` now both error with "no history plugin has been installed"
    (history moved entirely to the commercial **arcmutate** plugin). We removed all history flags + the history
    cache step; PIT runs a full `internal.*` sweep each run, which is tractable now that (a) drops the slow
    integration tests. So each PR re-mutates the whole engine rather than only changed code.
  - **SHELVED PLAN — restore incremental (and changed-classes-only) via arcmutate's free OSS licence.**
    arcmutate (https://www.arcmutate.com, by the pitest author) is **free for open-source projects** and provides
    the history plugin (incremental) + git plugin (mutate only classes changed vs base — the biggest win, = option
    (b) above). **Why shelved for now:** (1) needs the maintainer to manually sign up for the OSS licence at
    subscribe.arcmutate.com — I can't do that; (2) the licence is a file `arcmutate-licence.txt` at the repo root,
    which on a *public* repo means either committing a licence key or wiring it in as a CI secret — friction +
    a terms check; (3) it adds a commercial-plugin dependency to a FOSS build; (4) the current no-history +
    `excludedTestClasses` approach should stay under the 300-min cap, so incremental is a nice-to-have, not a need.
    **Revisit when:** PIT's full-sweep time creeps toward the cap. Then: get the free OSS licence → add the arcmutate
    history + git plugins → switch PRs to changed-classes-only, keep the full sweep for push/nightly.
  - **DONE (PR #69): moved PIT off GitHub's 2-core runner onto the self-hosted Mac (core-scaled parallelism).**
    On GitHub-hosted, a full `internal.*` sweep was impractically slow — `-Dthreads=2` maxed the 2 cores and it
    ran 17+ min without finishing. PIT is CPU-bound and process-parallel across minion JVMs, so it scales with
    cores. Removed the GitHub-hosted `mutation-testing` job and added a `Mutation (PIT)` entry to the
    `pr-mac-fast-feedback.yml` matrix, driven by `bin/ci-mutation-test.sh` which defaults `-Dthreads` to the
    box's core count (~12 on the Mac ⇒ ~5-6× faster). **Caveats:** (i) advisory only, and the Mac may be offline,
    so there's now no PIT signal when the laptop's off (acceptable — it never gated); (ii) RAM = threads × 2g
    (~24g at 12 threads), lower `PIT_THREADS` if the box is constrained. This is the "more cores" answer;
    changed-classes-only (arcmutate, above) is still the way to make it *cheap* rather than just *parallel*.
- **FLAKY (tracked, PR #69): `MultiInstanceMetricsTest.sameRegistryCanBeReusedAfterPcInstanceClosed`** (core
  `integrationTests`). Fails intermittently under the forked-per-broker integration run on loaded CI with
  `TimeoutException: Timeout while waiting to get produce lock (PT2S)` / commit lock (PT1S) — ~1/104, passes on
  re-run. **Hypothesis:** the intentionally-tight 1–2s lock timeouts fire under CI CPU/IO contention, not a real
  lock bug — the same "test-tightness under load" class the #68 investigation catalogued (distinct from the #857
  deadlock, which was a *revoke* path). **To confirm before touching:** reproduce under artificial CPU load and
  check whether it's contention (raise this test's lock-acquisition timeouts / mark heavier) vs a real
  produce/commit-lock stall in the multi-instance-shared-registry path. Do NOT just bump the timeout to go green
  without establishing which (AGENTS.md rule). Not yet reproduced deterministically.
- **DONE (PR #69): moved the "unit" tests that were actually INTEGRATION tests out of surefire, and now
  ENFORCED so no more can hide.** Two container-based tests were landing in surefire only because they
  weren't in an `integrationTest*` package: `examples…streams.StreamsAppTest` and
  `examples…metrics.CoreAppMetricsIntegrationTest` (+ its `PrometheusContainer` helper). Both relocated into
  their module's `integrationTests` package (so **failsafe** runs them, with Docker), each with the DI fixed
  (constructor injection instead of a package-private subclass-and-override seam) so they still run and assert
  the same thing. The metrics one also needed a real `jackson-databind` test dep (it had been parsing with the
  shaded Jackson). **The audit is now permanent, not a one-off:** the `TestConventionRules` ArchUnit rule fails
  the build if any test using Testcontainers / extending `BrokerIntegrationTest` sits in a surefire package -
  and it is green across every module, which *proves* no other container test is hiding in the unit suite.
- **DONE (PR #69): `RunLengthEncoderTest` no longer a ~1.5-min beast.** `vTwoIntegerOverflow` used to loop
  `Range.range(11, overflowedValue)` = **~2.1 billion `encodeCompletedOffset` calls** to reach the integer
  overflow. But the encoder accumulates run-length from the **delta** between offsets (`encodeRunLength:
  delta = relativeOffset - previousRangeIndex`), not from the call count, so a single completed offset a huge
  distance past the previous one overflows in ONE step via the same `Math.toIntExact` path. Replaced the loop
  with a two-call delta jump: the v2 case went **~85s → 0.087s**, overflow assertion untouched. Both overflow
  tests also got proper javadoc explaining the what/why. Class total now ~59s (was ~120s+).
  - **Follow-up (main-code optimisation, NOT urgent — user: "not our big win"):** the *other* overflow test
    `testSimultaneousWithOverflowErrors` INT case is still ~59s and **can't** use the delta shortcut —
    `OffsetSimultaneousEncoder.invoke()` walks *every* offset in the range (`range(length).forEach(...)`), so
    an int overflow genuinely iterates ~2.1B. Speeding it up needs a delta-aware `invoke()` (the run-length
    optimisation TODO already in `OffsetSimultaneousEncoder`) — a real main-code change, left for later.
- **BUG-OR-FLAKE to triage: `ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown`
  fails under thread-parallel unit tests.** The full thread-parallel unit run (`-Dparallel-tests=true`, 2:32)
  went red only on this one — an `AssertionError` in `assertCommits`
  (`AbstractParallelEoSStreamProcessorTestBase.java:382`); a mock shutdown-timing/commit-assertion test.
  Per AGENTS.md: establish **static-state/timing artifact vs real concurrency bug** before masking — do NOT
  just `@Isolated`/serialise it to go green. (Surefire mis-attributed it to `PartitionStateCommittedOffsetTest`
  in the per-class report — report cross-contamination under parallelism; the real failing class is
  `ParallelEoSStreamProcessorTest`.) **Revisit alongside the #857 locking work** — it's a shutdown/commit
  assertion, so it may be the same commit-lock timing family; don't investigate/mask it in isolation, look
  at it when we're back in the locking code.
- **DONE (PR #69): unified Awaitility + Hamcrest onto the real libraries.** Swapped all 11 shaded usages
  (`org.testcontainers.shaded.org.awaitility` in 3 `MockConsumerTest*` + 8 integration tests; also discovered
  `org.testcontainers.shaded.org.hamcrest` in 3 of them) to the real `org.awaitility` / `org.hamcrest` (both
  already on the classpath). New ArchUnit rule `tests_must_not_use_shaded_libraries` bans any
  `org.testcontainers.shaded..` dependency so it can't regress. The arch rules were also made DRY: defined
  once in `TestConventionRules` (core test-jar), each module pulls them in via `ArchTests.in(...)` (replacing
  the duplicated per-module `IntegrationTestPlacementArchTest` copies).
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
- **`PartitionStateCommittedOffsetIT.committedOffsetRemoved` — NOT a benign timeout; it's a real silent
  stall in the #857 family. DO NOT MASK.** Surfaces as `ConditionTimeoutException` after 10s (`expected not
  to be empty within 10 seconds`, `runPcUntilOffset` → `committedOffsetRemoved` first poll). Not
  runner-specific (reproduced locally). **Diagnosis (2026-07-29):** under real contention (`forkCount=16` on
  ≤12 cores) a PC instance polls **zero** records for the whole window — **still zero with a 120s bound**, no
  exception, consumer alive — so it's a stall, not slowness. The stall **roams** across timeout-sensitive
  ITs (`TransactionTimeoutsTest`, `KafkaSanityTests`, `TransactionMarkersTest`). Chasing it **root-caused a
  drain-path defect**: `drain()` fires `ConsumerManager.signalStop()`, whose `shutdownRequested` flag makes
  `poll()` short-circuit without ever calling `consumer.poll()` — defeating the intended "paused 2s long
  poll = drain-loop sleep" (comment in `handlePoll()`). Measured **~10k poll-loop iterations/s** while
  draining; and with `consumer.poll()` never invoked, the draining consumer can't join rebalances while
  background heartbeats keep it a live member — a **zombie holding its partitions** (up to
  `max.poll.interval.ms`, 5 min) that starves same-group siblings and burns a core. Likely a third
  mechanism behind **#857**'s "paused consumption after rebalance". **Bumping the test await would hide a
  real product bug.** Full write-up + drain design review:
  `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md`. **FIX LANDED on PR #80
  (2026-07-30):** the duplicated `ConsumerManager.shutdownRequested` flag is deleted (state collapse —
  abort now derives from `BrokerPollSystem.runState` via an injected signal), so a draining consumer keeps
  polling (paused, long-poll cadence) and stays rebalance-responsive; guarded by `BrokerPollSystemDrainTest`
  (characterisation-first, flipped RED→GREEN). **Verified: neither PR #29 (#857) nor PR #31 (#909) fixed
  this** — they fix sibling mechanisms (assign-time throttle-pause reset + CME; stale-container
  replacement) of the same "alive but not progressing" symptom. **UBER EXPERIMENT RUN + CONCLUDED
  (2026-07-30)** — full results:
  `docs/solutions/test-flakiness/uber-stall-experiment-results-2026-07-30.md` (archive refs
  `experiment/stall-uber-{nofix,fix}`). Headlines: (1) drain fix validated by direct RED→GREEN guards
  (`DrainingMemberRebalanceIT` + unit guard), statistical axes uninformative; (2) `committedOffsetRemoved`
  stall **still fires with all three fixes combined** — distinct broker-side mechanism, still open;
  (3) **#29 does NOT compose with current master** (deterministic: close→reopen redelivery 9/14 ERROR,
  `RebalanceEoSDeadlockTest` 5/5 FAIL) — attribution arms prove **#29 owns 100% of the breakage, #31 is
  exonerated**. Merge order: #75 → #80 → #31 (trivial rebase) → #29 rebased + split into slices, with the
  two deterministic regressions as rebase acceptance criteria (conflict cheat-sheet: uber-fix merge commit
  `340bf75f`). Chaos Pain Suite design (branch `docs/chaos-pain-suite-design`) targets the remaining open
  mechanisms; revisit the `committedOffsetRemoved` await only after its stall is actually traced and
  fixed.
- **`VertxTest.failingHttpCall` + `testVertxFunctionFail` — DNS-coupled, brittle on any runner with a local
  resolver. FIXED on #75.** They drove an HTTP call at the *dotless* bogus host `"xxxxxxxxx"` (port 1, via the
  shared `getBadRequest()`) and asserted the failure cause was a **DNS resolution** failure
  (`["failed","resolve"]`). On a network with a local resolver + search domain the name **resolves** to a LAN
  IP, so the failure mode became *connection refused* and the assertion failed; on GitHub's public DNS it
  passed. **Fixed** by pointing `getBadRequest()` at a **closed local port** (`127.0.0.1:1`) → deterministic
  "connection refused" everywhere, no DNS. (Both sibling tests share the helper, so both assertions were
  updated.) Found bringing up the self-hosted high-CPU runner.
