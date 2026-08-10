# Parallel Consumer - Agent Context

Project context for AI coding agents (Claude Code, Copilot, Cursor, etc.).

## IN FLIGHT: the package rename - rename your branch BEFORE you merge master

The fork is moving `io.confluent.*` to `bz.stub.*` with `bin/rename-packages.sh`. Until every open
branch has run it, this binds **any** agent merging **any** branch. Delete this section once no open
branch predates the rename. Reasoning and measurements:
`docs/plans/2026-08-11-001-refactor-package-rename-plan.md` (until it merges:
`git show refactor/package-rename:docs/plans/2026-08-11-001-refactor-package-rename-plan.md`).

- **Run `bin/rename-packages.sh` on your branch first, then merge master.** Both sides then agree on
  where every file lives and what it is called, so the merge is ordinary.
- **This is mandatory, not tidiness, and a clean merge is not evidence that you skipped it safely.**
  Merging renamed master into a branch that had NOT been renamed reported **zero conflicts** and
  silently applied the streams module's ArchUnit test edit into the *mutiny* module's file: git
  paired the five near-identical `TestConventionsArchTest.java` files across modules. Measured, not
  predicted. Renamed on both sides, the same case surfaces as a rename/rename conflict on the right
  file, for a human to resolve.
- **Sweep with `grep -rnE 'io[\\./]*conflu'`, never `grep -rn "io\.confluent"`.** Three files encode
  the package as an escaped regex (`io\.confluent\.parallelconsumer\.`) and one as a misspelling
  (`parallalconsumer`); the habitual sweep reports success without `bin/lib/quarantine-common.sh`
  even appearing in its output.
- **Assert the renames git RECORDED, and their pairing - a bare R-count is not enough.**
  `git show --raw -M <rev>` must show one R per moved file *and* each old path must map to its new
  path under the same transformation: squashing the rename into one commit invented four
  cross-module renames and dropped a fifth file to an add/delete pair. `bin/rename-packages.sh`
  asserts both; if you moved anything by hand, assert both by hand.
- **Confirm the mutation lane scored mutants instead of trusting the tick.** `bin/ci-mutation-test.sh`
  exits **0** printing "nothing to mutate, skipping" when its package regex is stale, which is
  indistinguishable from a pass in the job summary. Read the summary for a mutation score and a
  survivor list.

## Where things live (read this before concluding something isn't tracked)

Documentation is split by *purpose*, and the split is enforced by convention rather than tooling - so
the commonest mistake is not misreading a doc, it is **never opening it**. Before you conclude that
some category of work is untracked, check this table. (Real example: a whole triage doc was once
written because only `docs/inflight/` was grepped, duplicating `docs/refactoring.md`, which had
owned that content all along.)

| Document | Owns | Explicitly NOT for |
|---|---|---|
| **`AGENTS.md`** (this file) | Conventions, build/test commands, and the rules agents must follow | Work items of any kind |
| **`STRATEGY.md`** (repo root) | What the product is and why: the target problem, the guiding choice to solve it client-side, who it is for, the metrics that would show the approach working, and the tracks under investment | A roadmap, a schedule, or a feature list. It is a *claims* document that nothing tests, so work which falsifies one of its claims has to update it - the open branches that will are named in `docs/inflight/pr-strategy-doc-merge-triggers.md` |
| **`docs/inflight/`** | *Transient* cross-branch state, **one file per item**, named `<category>-<slug>.md` (`bug-`, `test-`, `ci-`, `deps-`, `pr-`, `branch-`, `release-`, `parked-`, `next-`). Rules in [`docs/inflight/AGENTS.md`](docs/inflight/AGENTS.md) | A backlog. A file is deleted when its work lands - and **never** a committed index file, which every PR would edit |
| **`docs/refactoring.md`** | The deferred-work backlog: internal refactors grouped by file, **breaking changes queued for the next major** in their own release-gated section, and the **triage of `TODO`/`FIXME`/`XXX` markers** | In-flight work; anything already started |
| **`docs/TODO_INDEX.md`** | Generated inventory of every marker in the tree (`bin/todo-index.sh`, `--check` fails when stale) | Priorities - it is deliberately unsorted; triage goes in `refactoring.md` |
| **`docs/QUARANTINED_TESTS.md`** | CI-enforced registry of quarantined tests and their owning fix PR | Tests that merely flake - quarantine requires a diagnosis |
| **`CONCEPTS.md`** (repo root) | Shared domain vocabulary: entities, named processes and status concepts whose meaning here is project-specific (the produce/commit lock pair, *dirty*, shard, in-flight work). Each entry stands alone - no file paths, class names or current config values. Relevant when orienting to the codebase or writing about it | A spec, an architecture doc, or general programming vocabulary |
| **`docs/solutions/`** | Write-ups of problems already **solved**, by category, with frontmatter for searching | Open problems |
| **`docs/plans/`** | Dated plan and investigation documents for a specific piece of work | Durable reference - a plan goes stale once its work lands |
| **`docs/SELF_HOSTED_RUNNER.md`** | Setup and operation of the self-hosted highcpu runner | CI policy, which lives in the workflows |
| **`src/docs/development/upstream-map.yaml`** | **Source of truth** for fork↔upstream mapping: fork branch/PR → upstream **PR**, with status | Editorial opinion (that is the `.adoc` beside it), and **upstream issues** - those live in the fork mirrors, `upstream-mirror` label |
| **`src/docs/development/upstream-pr-analysis.adoc`** | Editorial analysis of upstream PRs: rankings, verdicts, merge order | Facts - when they disagree, the manifest wins |
| **`CHANGELOG.adoc`** | Release notes. Sections for **shipped** releases are frozen; the section for the release being cut - **`== 0.6.0.0` included** - is regenerated at release time from the commit log, so what is under it today is working text, not the notes v6 will publish. `README.adoc` links to it and no longer embeds it | Anything invisible to users or operators - and **not** a per-PR chore: a PR never *adds* an entry. Correcting a factual error in an existing one is the only edit it may make |

Rule of thumb: **is it happening now** → `docs/inflight/`; **should happen later** → `refactoring.md`;
**already happened** → `CHANGELOG.adoc` or `docs/solutions/`.

## Before you investigate anything

Do all five checks below **before** forming a hypothesis, and say in your write-up what they returned
- including "nothing". Prior art does not only tell you the answer; it tells you the *method* that
settled the last question of this shape, and the traps that voided someone's earlier experiment.

| Check | Command | What it catches |
|---|---|---|
| Prior investigations | `ls docs/plans/`, then grep them | The same question already answered, and how it was proved |
| Solved problems | `grep -rl <mechanism> docs/solutions/` | A documented root cause with a signature you can rule in or out |
| In-flight state | `ls docs/inflight/`, `grep -rl <mechanism> docs/inflight/` | A known-open defect you are about to rediscover |
| Open PRs | `gh pr list -R astubbs/parallel-consumer`, then `gh pr diff <n> --name-only` | A fix already in flight, and files your change would collide with |
| **Merged** PRs, by file | `gh pr list --state merged --limit 100 --json number,title,files --jq '.[] \| select(.files[]?.path \| test("<ClassName>")) \| "\(.number) \(.title)"'` | The PR that last fixed something in this exact file - the richest prior art there is, and invisible to a search on the *open* list |
| Existing issues | `gh issue list -R astubbs/parallel-consumer --state all --limit 300` and filter by title - fork issues *and* the `upstream-mirror` ones | An upstream bug already triaged; read the upstream issue itself, not the mirror's summary |

**Grep the mechanism, not the symptom.** The name of the failing test is the weakest search term
available. Search the class, the lock, the option, the exception, the log line - whatever the failure
actually turns on.

**`--state open` is a collision check, not a prior-art search.** The PR that already solved something
in your file is, by definition, merged. Searching only the open list feels like due diligence and
finds nothing, which is worse than not looking - it produces false confidence. Same for issues:
`--state all`, because the useful ones are usually closed.

### Settling it: a fix that works is not evidence of the cause

Promoted here from `docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md` §11, because a
dated plan goes stale once its work lands and this method must not go with it.

- **Confirm a cause with a control arm, not with a fix that appears to work.** Change the one term you
  believe is responsible, hold everything else identical, and show the outcome flips. Same-magnitude,
  different-position beats bigger-hammer. The worked example: an identical 400ms delay injected on
  either side of a lock release - *after* it (opening the window) failed 8/8; *before* it (same added
  latency, inside the lock) passed 8/8, against a ~1-in-6 baseline. The control arm is what ruled out
  "it is just slower under load", which every previous look at that flake had concluded.
- **State the prediction before running it, and report the refuted ones.** A prediction that fails is
  the cheapest result you will get. If a fix works but its prediction was wrong, you have a symptom.
- **Verify your instrumentation actually reached the run** - the failure mode is a silent false
  negative that reads as "no effect":
  - `./mvnw -pl <module>` **without `-am`** fails the `ReactorModuleConvergence` enforcer, so the test
    never recompiles and both arms run the stale class.
  - `surefire:test` alone **does not reprocess test resources**, so an edited `logback-test.xml` never
    reaches `target/test-classes` and your new logging silently does not exist.
  - Use `./mvnw -pl parallel-consumer-core -am verify` (what `bin/soak-test.sh` runs) and confirm
    `BUILD SUCCESS` on the compile step. Better, assert the setting in the run's own output - PC logs
    its full options at INFO on init, so the arm proves itself.
- **Report the rate and the conditions, never a bare verdict.** "0 failures" is meaningless without N
  and the load. `bin/soak-test.sh <Class#method> <runs>` at a low `SOAK_FREE_CORES` is the house
  reproducer; its own closing line says it - no failures is not proof the flake is gone.
- **A guard added with a fix must be verified by negative control.** Break the thing it guards and
  confirm it fails deterministically. An assertion nobody has seen fail is decoration.

Real example, 2026-08-07: the `TransactionTimeoutsTest.commitTimeout` handoff searched for the test's
own name, found nothing, and classified the failure by analogy. Grepping the *mechanism*
(`producerTransactionLock` / `commitLockAcquisitionTimeout`) finds
`docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md`, the only prior investigation
into that exact lock - which already documented the lock's ordering invariant, the controlled-experiment
method for settling contention-vs-bug, and a build trap that had silently voided one earlier
experiment. All of it applied; none of it was used.

## Overview

Parallel Consumer is a Java library that enables concurrent message processing from Apache Kafka with a single consumer, avoiding the need to increase partition counts. It maintains ordering guarantees (by partition or key) while processing messages in parallel.

This is a community-maintained fork of `confluentinc/parallel-consumer` (the upstream is no longer actively maintained), published to Maven Central as `bz.stub.parallelconsumer`.

## Build Requirements

- **JDK 17** (required - the project uses Jabel to compile Java 17 source to Java 8 bytecode)
- **Docker** (required for integration tests - TestContainers spins up Kafka brokers)
- **Maven** via wrapper (`./mvnw`) - do not use system Maven

## How to Build

```bash
# Quick local build (compile + unit tests)
bin/build.sh

# Unit tests only (no Docker needed)
bin/ci-unit-test.sh

# Integration tests only (requires Docker for TestContainers)
bin/ci-integration-test.sh

# Full CI build with all tests against a Kafka version matrix (used by push-to-master CI)
bin/ci-build.sh

# Full CI build against a specific Kafka version
bin/ci-build.sh 3.9.1

# Performance tests only (requires substantial hardware)
bin/performance-test.sh
```

## Module Structure

| Module | Purpose |
|--------|---------|
| `parallel-consumer-core` | Core library - consumer, producer, offset management, sharding |
| `parallel-consumer-vertx` | Vert.x integration for async HTTP |
| `parallel-consumer-reactor` | Project Reactor integration |
| `parallel-consumer-mutiny` | SmallRye Mutiny integration (Quarkus) |
| `parallel-consumer-examples` | Example implementations for each module |

## Key Architecture Decisions

- **Jabel cross-compilation**: Source is Java 17, bytecode targets Java 8 via Jabel annotation processor. This means `--release 8` is set in the compiler plugin, which restricts available APIs to Java 8 surface. The Mutiny module overrides this to 17, which is that module's real runtime floor: `Multi` needs `java.util.concurrent.Flow` (9+), and SmallRye Mutiny 2.8+ is itself compiled for Java 17. Its pom carries the full reasoning, including why the build cannot detect the second constraint.
- **Offset encoding**: Custom offset map encoding (run-length, bitset) stored in Kafka commit metadata for tracking in-flight messages.
- **Sharding**: Messages are distributed to processing shards by key or partition for ordering guarantees.

## Testing

- **⚠️ Be EXTREMELY careful modifying tests to make them pass — especially under parallelism/stress.** We do
  **not** work from a position of 100% confidence in the main code. A test that fails under concurrent load
  or when the broker is contended may be exposing a **real main-code bug that only manifests under stress**,
  not a flaky test. So **never** loosen a timeout, weaken/remove an assertion, add a retry, or serialize a
  test just to get green until you have first determined *why* it fails: is it a **test-infra contention
  artifact** (e.g. one shared TestContainers broker overloaded by many parallel tests) or a **genuine
  concurrency bug** in the library? Prefer diagnostics that *separate* those (e.g. giving a test an
  uncontended/own broker: if it then passes it was contention; if it still fails, investigate the code — do
  not mask it). Loosening deadlines to go green can hide exactly the bugs this library exists to prevent.
  When you do change a test, say in the commit/PR *which* of the two causes you established and how.
- **Check the ambient probe autopsy first when a broker IT fails.** Every broker integration test failure
  log includes an `=== AMBIENT PROBE AUTOPSY ===` block (grep for it) with rebalance-dwell / lag-stagnation
  violations and per-partition frozen-committed detail — it answers exactly the "contention artifact vs
  genuine bug" question above before you start manual diagnosis. `probe clean` means the fault is likely in
  the test itself, not consumer-group progress. Disable via `-Dambient.probe=off` or `@NoAmbientProbe` only
  when the probe itself is the problem (see `AmbientProbeExtension` javadoc).
  **`probe clean` is only informative when the probe's detectors could have fired.** Lag stagnation needs
  `LAG_STAGNATION_MIN_LAG` (50) of real lag sustained past `LAG_STAGNATION_BOUND` (150s), and rebalance
  dwell needs `REBALANCE_DWELL_BOUND` (15s). A test with a handful of records, or one that fails inside a
  window shorter than those bounds, cannot trip either - so its autopsy prints `probe clean` and the
  sentence "the fault is likely in the test itself" carries no evidence at all. Check the test's record
  count and failure window against those constants before treating a clean probe as a finding. (This is
  not hypothetical: the `commitTimeout` autopsy of 2026-08-07 read `probe clean` on a 15-record test that
  failed in 35s, where the thresholds are 50 records and 150s.)
- **Unit tests**: `mvn test` / surefire plugin. Source in `src/test/java/`.
- **Integration tests**: `mvn verify` / failsafe plugin. Source in `src/test-integration/java/`. Uses TestContainers with `confluentinc/cp-kafka` Docker image.
- **Test exclusion patterns**: `**/integrationTest*/**/*.java` and `**/*IT.java` are excluded from surefire, included in failsafe.
- **Kafka version matrix**: CI tests against multiple Kafka versions via `-Dkafka.version=X.Y.Z`.
- **Quarantine lane for known-failing-on-master tests (`@Quarantined`).** When a test is red on master's
  *gating* CI and its fix lives in another (open) PR, do NOT leave it red (ambiguous checks, error-prone
  merge decisions) and do NOT `@Disabled` it (loses the signal — a "known flake" can be a real product
  bug - see the drain-zombie write-up, `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md`, which lands with PR astubbs#80). Instead annotate it
  `@Quarantined(reason, tracking, fixedBy)` (in core's shared test sources): it leaves the gating suites
  (green means mergeable) but keeps running on every PR push and after every merge to master (workflow_dispatch on
  demand) in the non-gating "Quarantine Lane / tests" CI job, whose summary carries pass/fail + the audit of every
  quarantined test and its owner; the seconds-fast "Quarantine Audit" job enforces the rules on every
  PR (registry drift / broken owner claims fail fast - no tests are run there). The live registry
  / task list is `docs/QUARANTINED_TESTS.md` - CI-enforced (`bin/check-quarantine-registry.sh`) to match
  the annotations in both directions, so it can't drift; `bin/check-quarantine-owners.sh` additionally
  verifies each entry's owner claim (owning PR exists + is open + eventually removes the quarantine). Rules: **(1) no
  quarantine without diagnosis** — undiagnosed red stays red and blocks, on purpose; **(2) quarantine is
  master-state, not PR-state** — a test red on only one PR is that PR's problem; **(3) the owning fix PR
  deletes the annotation AND its registry entry in the same commit** after merging master, atomically
  restoring the test to the gating lane. Releases are blocked
  while the lane is non-empty (`release.yml` guard; snapshots still publish). Run
  the lane locally with `bin/quarantined-test.sh`.
- **Reuse test utilities — search before you add (DRY).** Shared client/broker helpers live in `KafkaClientUtils` (topic creation, producers, consumers, PC builders) and `BrokerIntegrationTest` (the base class most integration tests extend). Before writing a new helper or a raw `admin`/producer/consumer call in a test, search these two first and extend them. Duplicating an existing helper is how bugs get reintroduced — e.g. a copy of topic-creation logic drifted to a 1-second timeout and became a flaky-CI source (see `docs/solutions/test-issues/`). When you must add a helper, put it in the shared util, not the test. Also check `docs/solutions/` for prior art before solving a problem that feels familiar.

### Chaos Pain Suite (on-demand bug detector — never gates)

A seeded, calibrated chaos suite (`integrationTests.chaostests`: `ChaosConductor`, `ProgressProbe`,
`ChaosScenarioBase` + scenarios `ChaosChurnStormIT` W1, `ChaosRevokeUnderWorkIT` W4) that hunts the
"alive but not progressing" bug class: rebalance-dwell zombies, protocol-invisible per-partition lag
stagnation (Class 2, W4's prey), drain overruns, and record loss/duplication. Tagged
`@Tag("chaos")` and excluded from all default/gating suites via `pom.xml`'s `excluded.groups` default.

- **Run locally** (requires Docker; ~5-6 min):
  `./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups=`
- **Replay a schedule**: every run logs its seed and the full replay command; add `-Dchaos.seed=<seed>`.
- **CI**: per same-repo PR commit via the highcpu fast-feedback lane (check `highcpu / Chaos Pain
  Suite` - not optional: a chaos RED shows red); on-demand seeded hunts via
  `.github/workflows/chaos-pain.yml` (`workflow_dispatch`, inputs `seed`/`reps`), e.g.
  `gh workflow run chaos-pain.yml -f seed=42 -f reps=3`. Both call `bin/chaos-test.sh`. NB unlike the
  local recipe above, CI runs EXCLUDE `@Quarantined` chaos scenarios (the Quarantine Lane owns those) -
  while `ChaosChurnStormIT` is quarantined under PR astubbs#80 they therefore select zero tests, and the job
  summary flags that loudly.
- **Probe a fix PR** (the suite's primary purpose): on the fix PR's branch (merge master in first if
  the branch predates the suite landing there), run the suite at a commit before the fix (expect RED —
  the violation names the mechanism) and at the fix (expect GREEN). The local recipe above includes
  `@Quarantined` scenarios (`-Dexcluded.groups=` is empty), so known-RED detectors still fire locally.
  See `ChaosChurnStormIT`'s class javadoc for the full recipe.
- A RED run is investigation food, not flake noise — the probes are calibrated against the real historical drain-zombie defect (RED on pre-fix compositions, GREEN on fixed; thresholds sit in measured gaps). Never loosen a probe to go green; tune the workload/conductor instead.

## Code Style

- **Lombok**: Used extensively (builders, getters, logging). IntelliJ Lombok plugin required.
- **EditorConfig**: Enforced via `.editorconfig` - 4-space indent for Java, 120 char line length.
- **License headers**: Enforced by `bin/check-copyright-headers.sh`, which also runs in the build
  itself (`validate` phase, via exec-maven-plugin), so a plain `mvn` catches violations - not only the
  `Copyright Headers` workflow. Skip it with `-Dcopyright.skip=true`.
  There is **no header-applying tool**: the scanner checks, it does not write. New files get their
  header written by hand, per the provenance rules below. The mycila `license-maven-plugin` used to
  fill that role and was removed - it knew only the Confluent header template, so its `format` goal
  stamped the wrong attribution onto fork-original files, and its git-year resolver auto-bumped years
  and broke in worktrees. `-Dlicense.skip` no longer exists as a property; drop it from any command
  you copy from an older doc or script.
- **Copyright rules for this fork**:
  - Do not change copyright headers on existing files unless the file has substantive code changes in the same commit
  - Do not bump copyright years as an incidental or standalone change
  - The `NOTICE` file at repo root contains the legal attribution structure for the fork
  - New files written entirely for the fork use `Copyright (C) <year> Antony Stubbs and contributors` -
    never the Confluent header
  - Upstream-derived files MODIFIED on the fork retain the Confluent notice and ADD
    `Modifications Copyright (C) <year> Antony Stubbs and contributors` beneath it (Apache 2.0
    4(b) retain-notices + 4(c) change-notice - the convention used by e.g. Amazon Corretto and
    MariaDB for derived files). The scanner detects modification against the fork point
    automatically, so forgetting the line fails CI
  - Files renamed or extracted from upstream keep the Confluent header - register renames in
    `RENAMED_FROM_UPSTREAM` (`newpath|oldpath` lines) and extractions in `EXTRACTED_FROM_UPSTREAM`
    inside `bin/check-copyright-headers.sh`. Renames with content changes, and all extractions,
    also require the modifications line
  - A whole-package MOVE is a rule, not ~200 rename entries: `PACKAGE_MOVES` in the same script maps
    a current path back to its fork-point path before every lookup, so provenance survives
    `io.confluent.*` → `bz.stub.*`. Without it the verdict *inverts* - every upstream file misses the
    fork-point lookup, is judged fork-original, and its required Confluent header becomes a violation
    (measured: 0 → 197, in maven's `validate` phase, so every `./mvnw` dies before it starts)
- **Google Truth**: Used for test assertions alongside JUnit 5 and Mockito.

## CI

**Reading a failed job's log.** `gh run view --log` refuses while *any* job in the run is still going
("logs will be available when it is complete"), and `--log-failed` is often empty for a Maven job,
because the failure text is ordinary stdout rather than an `::error::` annotation. Neither means the
log is unavailable. Fetch the job directly - this works as soon as **that job** finishes, regardless
of the rest of the run:

```bash
jid=$(gh run view <run-id> --json jobs --jq '.jobs[] | select(.name=="Integration Tests") | .databaseId')
gh api "repos/astubbs/parallel-consumer/actions/jobs/$jid/logs" > /tmp/job.log
```

Then grep it - `Tests run:`, `<<< FAILURE`, and for broker ITs the
`=== AMBIENT PROBE AUTOPSY ===` block, which classifies contention-vs-bug before you start reading
stack traces (see Testing).


- **`.github/workflows/maven.yml`** — Build and test on every push/PR. PRs run two tiers in parallel: (1) split suites on default Kafka 3.9.1 for fast feedback (`bin/ci-unit-test.sh`, `bin/ci-integration-test.sh`, `bin/performance-test.sh`), and (2) an experimental Kafka 4.x compatibility check (`bin/ci-build.sh`). A seconds-fast "Quarantine Audit" job enforces the quarantine registry on every PR; the `@Quarantined` lane itself runs non-gating on every PR push and every push to master (+ dispatch) in its own workflow (`quarantine-lane.yml`) — see Testing. Push to master runs a single full build on default Kafka version via `bin/ci-build.sh` to gate SNAPSHOT publishing. All jobs use explicit `cache/restore` with rotating keys from the `prepare-deps` job - never `setup-java cache: 'maven'`. Includes SpotBugs, duplicate detection, mutation testing (PIT), and dependency vulnerability scanning on PRs.
- **`.github/workflows/publish.yml`** — Publishes to Maven Central on every push to `master`. The pom.xml version is the source of truth: `-SNAPSHOT` versions deploy as snapshots, non-snapshot versions deploy as full releases (and create a git tag + GitHub release).
- **`.github/workflows/copyright.yml`** — Copyright-header conformance via `bin/check-copyright-headers.sh` (runs its self-test `bin/test-check-copyright-headers.sh` first, then the real scan) on every push/PR. GitHub-hosted; needs `fetch-depth: 0` so the fork-point commit is in history.
- **`.github/workflows/shell-hygiene.yml`** — static checks on the repo's own shell scripts, one job per concern (`shell: sigpipe` today). `bin/check-shell-sigpipe.sh` fails any `bin/*.sh` that pipes into `grep -q` under `pipefail`, which reports failure exactly when it *matches* once >64 KiB follows; `shellcheck` does not detect this. Its self-test runs first. Kept out of `copyright.yml` so neither workflow's name outlives its contents.
- **`.github/workflows/claude-code-review.yml`** — Automated PR review. The job ends with a gate,
  `bin/check-review-posted.sh` (self-tested by `bin/test-check-review-posted.sh`, which runs first),
  asserting that a review from *this* run actually landed on the PR. Without it the check reports
  success when the action reviews nothing, which is indistinguishable from "reviewed, no findings" -
  it has happened twice here. **The gate fails on any PR that edits `claude-code-review.yml` itself**:
  the action refuses to run unless that file matches the default branch, so a PR cannot rewrite its
  own reviewer. That is the guard working. Get a real review with a `@claude review this` PR comment
  (which runs from `claude.yml`, unmodified, so it validates), or split the workflow edit into its
  own PR. Do not disable the gate to get a green check.
- **`.github/workflows/repo-hygiene.yml`** — "Repo Hygiene", on every push/PR plus dispatch. Independent jobs, one per concern. `sigpipe` runs `bin/check-shell-sigpipe.sh` (its own self-test first) to catch a `bin/*.sh` piping into `grep -q` under `pipefail`, which silently inverts the script's answer. `rename` runs `bin/test-rename-packages.sh`, the self-test for the `io.confluent.*` → `bz.stub.*` package-rename tool — a tool run by hand once per branch, which is exactly the shape that rots unnoticed between the day it is written and the day the whole rename depends on it. `actions` runs `bin/check-action-versions.sh`, keeping every GitHub Action pinned to a single version across all workflows. None gates the build - they exist because the failures they catch are invisible rather than loud.
- **`.github/workflows/check-dependencies.yml`** — "PR Dependency Check". Reads `depends on astubbs/parallel-consumer#N` lines from the PR body and blocks the child until every parent has merged. Produces the **required** check `Check PR Dependencies`, so a stacked PR cannot merge out of order. See [PR Discipline](#pr-discipline) for the syntax.
- **`.github/workflows/cancel-closed-pr-runs.yml`** — Cancels a PR's in-flight runs when it closes, so a withdrawn PR stops occupying runners. Housekeeping only; gates nothing.
- **Self-hosted lanes** (see [`docs/SELF_HOSTED_RUNNER.md`](docs/SELF_HOSTED_RUNNER.md)). None of these gate merging - they are for speed and for work too heavy for a 2-core hosted runner. All are **skipped for PRs from forks** (`head.repo.full_name == github.repository`), because a fork PR must never run on our own hardware.
  **`highcpu` is the only self-hosted label** - six runners, all online. Declare labels in [`.github/actionlint.yaml`](.github/actionlint.yaml) or actionlint flags them.

  - `pr-highcpu-fast-feedback.yml` ("highcpu") — on every in-repo PR plus dispatch. The lane that earns the hardware.
  - `mutation-full-sweep.yml` — dispatch only: the whole-project PIT sweep (`bin/ci-mutation-test.sh -Dverbose=true -Dthreads=N`). The PR-scoped mutation job in `maven.yml` only covers classes changed against the base; this is its exhaustive counterpart.

  **There is no scheduled build, deliberately.** Every suite worth re-running is already a required check on each PR and runs again on every push to master, so a cron lane would only repeat covered work. **Do not add a lane for suites the gate already covers.**

  **Before pinning a job to a self-hosted label, confirm a runner serves it** - `gh api repos/astubbs/parallel-consumer/actions/runners` lists each runner's labels and online status. A job pinned to a label nothing advertises does not fail; it queues until GitHub cancels it, so the lane reports nothing at all and looks merely quiet.

  **Beware: `performance` names two unrelated things.** It is the *test suite* (`bin/performance-test.sh`, the required **Performance Tests** check, on every PR from `maven.yml`, `ubuntu-latest`). It is **not** a runner label - the only self-hosted label is `highcpu`.
- **`.semaphore/`** — Legacy Confluent internal CI/release pipelines, retained but inactive on the fork.

## Changelog

`CHANGELOG.adoc` holds the release notes. **Nothing about it is a per-PR chore.**

**Release-time generation is in effect now, and it covers `0.6.0.0` itself.** "Frozen" below is a
statement about *text already written in the file* - leave it alone - and never a claim that some
release's published notes are settled. What state a section is in follows from whether its release
has **shipped**:

| Section | State |
|---|---|
| `== 0.5.x` and below | Hand-written legacy from before the fork, and shipped. **Frozen.** |
| `== 0.6.0.0` - the release being cut | **Not shipped, so not settled.** Whatever sits under this heading now is working text. It will be **regenerated at release time from `git log <last-tag>..HEAD`**, replacing what is there, and frozen only once 0.6.0.0 ships. |
| Every release after it | Same treatment: generated when that release is cut, frozen once it ships. |

Two readings this is written to rule out. **`0.6.0.0` is not on the hand-written side of the line** -
generation does not start at some later release. And **the current contents of `== 0.6.0.0` are not
what v6 will publish** - do not cite them as the release notes, and do not treat the section as
appendable just because the release has not gone out. It is not yours to add to *or* to trust.

**In a PR the changelog is never added to.** No new entries, and no `== Unreleased` section - a
shipped section is finished, and the in-flight section belongs to the generator. There is no window
in which a PR contributes an entry.

**The one edit a PR may make is correcting a factual error in text that is already there.** astubbs#198 did
exactly this: a Dependencies entry said the Kafka client stayed on `3.9.1` when the pom had moved to
`3.9.2`. A wrong statement in a published artefact does not get better by waiting for the next
generation pass, so fix it. The test is whether you are *changing an existing claim to be true*
(allowed) or *adding information about a change* (the generator's job, not yours).

The policy removes the file that every PR used to touch - it appeared in 30 of the last 30 master commits,
dragging the generated `README.adoc` with it - and removes the ordering problem where an entry had to
cite a PR number that did not exist when the entry was written.

### What this asks of a commit

Nothing extra. The commit log is the raw material, so write it as you already should: a subject that
says what changed and, where it matters to a user, what it changed *for them*; the diagnosis, the
experiment and the rejected alternatives in the body. A good commit message is now doing double duty,
which is a reason to keep writing them properly rather than a new process.

### At release time

An agent reads `git log <last-tag>..HEAD` - full messages, not just subjects - and drafts the release
section. The judgement it applies, and that a human should re-apply before freezing:

- **The entry test.** Can a *user or operator* observe this without reading our repo - API, behaviour,
  performance, logs and metrics, or the published artifact? If not, it gets no entry. Most CI,
  tooling, refactor and docs commits produce nothing, and that is correct: the changelog answers one
  question, "should I upgrade, and will anything change for me?"
- **One sentence, about 25 words, then the link.** Name what a reader would have *seen*, and who it
  hits when that is not everyone - not how the bug worked. An entry that runs to a paragraph is
  written for its author; an entry too short to tell you whether you are affected (`fix: Paused
  consumption across multiple consumers`) is no better.
- **Assemble as a set, not one commit at a time.** Merge related commits into a single entry, drop
  what turned out not to matter, and rewrite for someone who was not there. This is the part a per-PR
  entry could never do.
- **One `=== Build & CI` entry for the whole release** - a short bullet list of the big hitters
  (quarantine lane, chaos suite, mutation testing) that tells a reader how carefully the library is
  tested, with the detail left to the log.
- **Sections:** `=== Breaking`, `=== Improvements`, `=== Fixes`, `=== Dependencies`, `=== Examples`,
  `=== Build & CI`. **Reference convention:** a bare `#NN` is this fork, `upstream #NN` is
  confluentinc; make issue links explicit (`.../issues/NN[#NN]`), since GitHub numbers issues and PRs
  from one sequence.

### The `PR Checklist` changelog gate is a different, narrower check

`.github/scripts/changelog-ref-gate.js` fails a human PR that adds a `CHANGELOG.adoc` bullet under
`Breaking`, `Improvements`, `Fixes` or `Examples` without an explicit `/issues/NN` link. **Do not read
a green gate as compliance with the rule above** - it is neither a subset nor a superset of it, and it
is not dormant:

- It **passes** entries the policy forbids. The gate only cares about the *citation*, so an added
  entry that links an issue sails through. astubbs#57's entries all cite issues.
- It **fires** on the one edit the policy allows. The gate cannot tell an edit from an addition - its
  own header explains that matching removed bullets against added ones was tried and abandoned as the
  subtlest code in the file - so a correction like astubbs#198's looks like a new entry. That is what
  `changelog-ref: N/A - <reason>` on its own line in the PR body is for; the workflow names this case
  explicitly.
- PRs **do** still touch the file. astubbs#51, astubbs#57, astubbs#105 and astubbs#106 were all open before this policy landed and
  all modify `CHANGELOG.adoc`; every PR predating the policy is in the same position.

So the gate enforces the *citation convention* on entries, and a human author and reviewer enforce
"no entries in a PR". Tightening it to reject *every* addition was considered and rejected: "adds an
entry" and "corrects an existing one" are both `+*` lines and are not mechanically distinguishable -
the gate's own header records that matching removed bullets against added ones was built and then
abandoned as the subtlest code in the file. A blanket rule's only escape hatch would be a
self-declared `changelog-ref: N/A - <reason>` in the PR body, which legitimises a violating addition
exactly as easily as a legitimate correction: no enforcement the written rule does not already have,
at the cost of an opt-out on every correction.

## Issue references

Every upstream issue now has a **fork mirror** (astubbs#44, astubbs#117-astubbs#195, label `upstream-mirror`), so a
reference has a fork-local number a reader can click. Find one with
`gh issue list -R astubbs/parallel-consumer --label upstream-mirror --search "confluentinc#NNN"`.

**Working a mirrored issue? Read the upstream original too - body and comments.**
`gh issue view <N> -R confluentinc/parallel-consumer --json body,comments`. Every mirror says
"Summarised, not copied", which makes it one agent's reading of the issue rather than the issue.
Verify the summary against the original and against the code before fixing or documenting anything.
This is not hypothetical: astubbs#194's summary said the Mutiny dependency "requires a higher
bytecode level", while confluentinc#906's reporter had written *"I think the compiler target for
that dependency is 17"* - the detail that actually mattered. A fix followed the summary, set
`release.target=9`, compiled green, and shipped a jar that died with `UnsupportedClassVersionError`
on Java 8 and 11. astubbs#171 shows the same failure in its **Fork status** notes rather than its
summary - "`shutdownTimeout` and `drainTimeout` (default 30s)" reads as one shared default, where the
code has two (10s and 30s) - so check the mirror's added commentary as sceptically as its summary.
When the mirror turns out to be wrong, say so in the PR **and correct the mirror**, or the next
reader inherits the same error.

**The convention: below the threshold, say which repo you mean.** `astubbs#119` for this fork,
`confluentinc#857` for the original - or a hyperlink, which qualifies it just as well. Add `PR` or
`issue` where the distinction matters (`confluentinc PR #548`); both forms pass the gate.

> **The threshold was #1000 when this was written, but `QUALIFY_BELOW` in
> [`.github/scripts/issue-ref-gate.js`](.github/scripts/issue-ref-gate.js) is the source of truth.**
> It is expected to move - confluentinc's numbering still creeps, as the headroom note below says -
> and prose cannot read a constant, so every `1000` in this file is a snapshot. If they disagree, the
> constant is right and this document is stale. Change it there, then sweep the prose.

**A hyperlink satisfies the gate, but not the reader - so name the repo anyway in link text.** The
gate can see the target and stops asking; a human reading `issue #12` cannot, and this fork has its
own `#12`. Write `[confluentinc issue #12]`, not `[issue #12]`, wherever the number is prose someone
reads - `README.adoc` above all, since it is the published artefact and its audience is *on the
fork*. Leave a quoted upstream title intact and append the number instead
(`[Enhanced retry epic confluentinc#65]`), rather than editing the quotation. This is style, not
enforcement: the gate will not flag a bare number beside a URL, which is exactly why it is written
down here.

**Name the owner, not the role.** `confluentinc#857`, not `upstream #857` - "upstream" describes a
relationship rather than a repository, and it is not stable: this fork is itself upstream to anyone
who forks it. The gate accepted `upstream #NN` while the tree still used it; the tree-wide sweep
removed the last use and the tolerance went with it, so the form is now **flagged like any bare
number**. It was dropped rather than merely discouraged because a tolerated form comes back the
moment someone copies older text.
Same reasoning that rules out "fork" as a qualifier. In anything
**posted to GitHub**, use the fully qualified `confluentinc/parallel-consumer#857`: upstream prose
does not auto-link there, and a bare `#NN` in a comment silently resolves against whichever repo it
is posted in.

**Closing keywords are the exception, and getting this one wrong fails silently.** GitHub honours only
`Fixes #167` or `Fixes astubbs/parallel-consumer#167` - the `owner#NN` short form this section
otherwise prefers is **not** cross-reference syntax, so `Fixes astubbs#167` renders as plain text and
closes nothing. A bare number is what the convention above forbids, so in a PR body write the fully
qualified form: `Fixes astubbs/parallel-consumer#167` - the one form that closes the issue, names the
repo and auto-links. The gate reads PR bodies, so the other two spellings now fail it rather than
failing silently.

At or above #1000 a bare number is unambiguous, because only this fork can have one.

**Check it before you push: `bin/check-issue-refs.sh`.** The `PR Checklist` workflow fails a PR that
adds an unqualified reference, and finding that out from CI costs a push cycle for a one-character
fix. The script applies the same rule as the gate, because it calls the same
`.github/scripts/issue-ref-gate.js` module rather than a second copy of it - so the rule cannot drift
from CI. It judges the working tree, like `bin/check-copyright-headers.sh`, so uncommitted edits are
caught too. Only lines you *add* are scanned; pre-existing bare refs in a file you touch are fine.

The *inputs* differ in two narrow ways. CI reads patches from GitHub's `pulls.listFiles`, which omits
`patch` for a very large diff, and the gate skips a file it cannot see - while the local script
builds its own patch with `git diff` and still checks it. And CI additionally scans the **PR body**,
which does not exist when you run the script. So a green local run promises neither that CI looked at
every file nor that the description passes; a red one is always real.
That holds only while confluentinc's numbering stays below 1000 - it is dormant rather than
archived, so it still creeps. Measure the headroom rather than trusting a figure written here:
`gh api 'repos/confluentinc/parallel-consumer/issues?state=all&per_page=1&sort=created&direction=desc' --jq '.[0].number'` -
`state=all` matters, since the highest number is usually a merged PR.
`upstream-sweep.sh` warns when it thins.

**Why not "a bare `#NN` is this fork", which is what this used to say:** the fork's numbers sit
*entirely inside* upstream's range, so a bare number is a coin flip. Of the 51 numbers cited across
one PR's files, **48 existed in both repos meaning different things** - `#29` is this fork's
rebalance fix and upstream's async-sending request; `#114` is a docs PR here and a GPG-key issue
there. "fork" is not used as the qualifier either: this repo *is* a fork, so the word names nothing.

**Cite both numbers, fork first.** `(astubbs#119, confluentinc#857)`. The fork number is what `Fixes` acts on
and what a reader of this repo can open; the upstream number is what four months of commits, branch
names (`bugs/857-...`) and the upstream threads all use. Dropping either breaks a trail.

**Fix references in any file a PR touches.** Not a bulk rewrite - opportunistic, as files are
touched. In a file being changed anyway:

- every unqualified `#NNN` below the threshold gains its repo - `astubbs#NNN` or `confluentinc#NNN` -
  **hyperlinked** where the format allows (markdown link, javadoc `<a href>`; a raw URL in a `//`
  comment, which every IDE linkifies)
- resolve the number in **both** repos before choosing the prefix; it very likely exists in each
- add the fork mirror number alongside an upstream one where a mirror exists
- there is no backlog left to work through: the tree-wide sweep qualified every remaining reference
  and converted the last `upstream #NNN` uses, so anything you find now is drift, not leftovers

**PR titles carry both**, e.g. `fix(core) astubbs#119: paused consumption after rebalance (confluentinc#857)`. The
title becomes the squash commit subject, so it is the reference most people will ever see.

**A CI gate enforces this.** The `PR Checklist` job fails a PR whose *added* lines contain an
unqualified `#NN` below 1000. The check is purely textual - no API calls, so it cannot race issue
creation. An earlier version instead asked "does this number resolve here?", and so passed `#200` - a
real fork issue about ManagedTruth - while the author meant confluentinc#200, the shared-nothing
architecture. **A wrong reference that resolves is worse than a broken one**, because nothing looks
amiss.

What it checks is that a reference *names* a repo, not that it names the right one: `astubbs#857`
passes the gate and is still wrong. Resolve the number in both repos before you write it.

**The PR body is in scope too, and it is the one place the fully qualified form is mandatory.** The
body is the surface people actually read on GitHub, and a bare `#200` renders there as a *working*
link to the wrong issue - the exact failure the gate exists to prevent, on its most visible page. So
write `astubbs/parallel-consumer#NN` or `confluentinc/parallel-consumer#NN` in a description: the
short `astubbs#NN` satisfies the gate but is not cross-reference syntax, so GitHub renders it as
plain text and the body loses the link it would otherwise have had. Same for closing keywords -
`Fixes astubbs#167` closes nothing. This is not a second rule: the body is fed to the same
`suspectRefs` as a synthetic entry (`prBodyEntry`), attributed as `<PR body>` in the failure. Fenced
code blocks in the body are skipped, because GitHub does not auto-link inside one either, so a
pasted log or a quoted gate failure is not a violation. Editing the body re-runs the job, so a fix
there needs no push.

The files listed in `EXEMPT_PATHS` are exempt, because a bare number legitimately means upstream in them: `CHANGELOG.adoc`,
`upstream-map.yaml`, `upstream-pr-analysis.adoc`, and the gate's own test fixtures. If a flagged
reference really is fork-local, put `issue-refs: N/A - <reason>` on its own line in the PR body -
which skips the body's own references along with everything else.
Logic and tests live in `.github/scripts/issue-ref-gate.js` and `issue-ref-gate.test.js`.

**`Fixes #NNN` only closes on PRs targeting the default branch.** Discovered on astubbs#29, which targeted
`master-confluent`: the keyword was in the body and GitHub ignored it entirely. Check
`gh pr view N --json closingIssuesReferences` rather than assuming. And never use `Fixes` for a
*partial* fix - see the mirrors for confluentinc#233, confluentinc#326 and confluentinc#857, none of which their linked PRs
actually resolve.

## Commits

**`.gitmessage` is the template for everything below.** Turn on the editor prompt once per checkout - `git config commit.template .gitmessage` - and it walks you through the subject format and the trailers. It is not wired up automatically, and nothing lints commit messages, so all of this is on you.

- **Subject is `type(scope) #NNN: subject`, and the trailing `(#N)` slot belongs to the PR number - never put an issue there.** GitHub appends `(#123)` to the subject on squash-merge, so a title ending `... (#41)` merges as `... (#41) (#123)`: two bare numbers, and no way to tell the issue from the PR. Cite the issue at the FRONT instead, as `type(scope) #41: subject` - which merges cleanly to `docs(mutation) #41: subject (#123)`, and matches both Apache Kafka (`KAFKA-14561: ... (#13114)`) and this repo's own pre-fork history (`GH-725: ... (#727)`). For an *upstream* issue, word it - `fix(core) confluentinc#909: subject` - because a bare `#909` autolinks to *fork* issue 909, which is not the one you meant. Prefer the fork mirror's own number when one exists (`#119` mirrors confluentinc#857), and put the upstream number in the [commit trailers](#commit-trailers), where a tool can read it. **The same rule governs PR titles**, because on a squash-merge the merged subject *is* the PR title.
- **`(scope)` is optional and only earns its place when it narrows things usefully** - `(core)`, `(producer)`, `(changelog)`, `(mutation)`. A directory name is not a scope. Plain `docs #208: ...` beats a scope that adds nothing.

See also [What this asks of a commit](#what-this-asks-of-a-commit) under Changelog, for what the body has to carry so release notes can be generated from the log.

### Branch naming

Branches encode the upstream number: `bugs/857-...`, `fix/909-...`, `cherry-pick/893-...`, `upstream-pr-905`. Keep this — it makes the mapping greppable and matches the manifest's `fork.branches`.

### Commit trailers

Commits that relate to upstream carry DEP-3-style trailers so provenance lives in the commit itself:

```
Upstream-Issue: confluentinc/parallel-consumer#857
Upstream-PR: confluentinc/parallel-consumer#548
Forwarded: <upstream comment URL | no | not-needed>
Applied-Upstream: <no | commit:SHA | VERSION>
```

Keep the existing subject convention for *upstream* references (`... (#893)`, `cherry-pick Confluent #905`), governed by the subject rule above. **Trailers are not enforced** — they only fit upstream-related commits, not fork-only work (rebrand, release, dependabot, formatting). Use judgement.

## PR Discipline

- **Before merging a fix, look for other instances of the same defect - and say what you found,
  including "none".** A fix that removes today's instance invites tomorrow's. Once you can name the
  defect *class* (not the symptom), grep for it: the pattern, the API being misused, the shape of the
  mistake. State which candidates you checked and dismissed, not just the hits - "I found none" is only
  worth reading if it says where you looked. Do this at merge prep, when the class is understood;
  doing it while still diagnosing just widens the investigation.
  Worked example, astubbs#220: the class was *a test awaiting a consequence whose trigger it cannot
  force*. The greppable proxy was sleep-as-synchronisation in integration tests plus awaits on a
  failure outcome. That surfaced `DrainCloseTest` and `RetriesTest` as relatives, and - just as
  usefully - confirmed the sibling `TransactionTimeoutsTest.produceTimeout` is **not** an instance,
  because it latches its trigger with a real margin. Ruling one out is a result.
- **Before merging, recommend a merge strategy - and say why.** A long-lived PR accumulates
  fix-ups, review responses and course corrections that nobody wants in the permanent log, but it
  usually also contains two or three genuinely separate pieces of work. Do not default; look at the
  actual commits and recommend one of:
  - **Re-cutting the commits.** `git reset --mixed <merge-base>`, then restage into a handful of
    atomic commits and rebase-merge, so each lands on master on its own. Right when the branch holds
    distinct workstreams someone will later want to bisect to or revert independently. The test for
    "atomic" is whether the commit message needs an "and also". Verify the re-cut with
    `git diff <old-tip> HEAD` - it must be empty, proving history changed and content did not.
    **`git fetch origin master` first, every time**, then reset to the **merge-base**, not to
    `origin/master`: a stale ref or the wrong base silently reverts whatever master gained meanwhile.
    That failed here - the tell was files appearing in the staged set that the branch never touched.
  - **Squash-merge.** Right when the branch is one idea and the intermediate commits are noise. If
    you recommend this, **write the suggested squash commit message out in full** - it becomes the
    permanent record, and the default (a concatenation of every commit subject) is unreadable.
    Remember the subject becomes the PR title with `(#N)` appended.
  - **Rebase-merge as-is.** Right only when the existing commits are already clean and atomic.
  Releases after 0.6.0.0 generate their notes from the commit log, so this choice decides what a
  future changelog has to work with.
- **Closing something as superseded: link both directions, and link a durable anchor.** Name the
  successor from the closed PR *and* the predecessor from the successor - a reader arrives from
  whichever side they happen to know about, and the one-way link strands the other half. If the
  successor does not exist yet, cite the tracking issue rather than a branch: a branch name is not a
  link, tells you nothing about whether the work landed, and nobody comes back to upgrade it once the
  PR opens. The issue exists at both moments; the branch is only meaningful at one. Real example:
  astubbs#30 said "will land as a fresh PR" in July and was still saying it in August, while
  astubbs#57 - the PR in question - never mentioned its predecessor at all, so the earlier round of
  review was invisible from the work that carried it.
- **Keep the PR title and body in sync with what the PR actually covers.** As a PR grows, its description drifts - re-check it before requesting review and before merge. Update it only on *material* drift: whole changes/workstreams missing, wrong specifics (core counts, flags, forkCounts, file/label names), or scope that has outgrown the title. Do NOT churn the description for cosmetic wording - if it still accurately reflects the content, leave it.
- **Open PRs from the template and complete its checklist honestly.** `.github/PULL_REQUEST_TEMPLATE.md` is NOT auto-applied when a PR is created non-interactively (e.g. `gh pr create --body-file`), so base the PR body on it and resolve every box: check it `[x]`, or mark it `N/A - <reason>`. For human-authored PRs the `PR Checklist` CI gate (`.github/workflows/pr-checklist.yml`) fails when the checklist is missing entirely *or* when any box is left unchecked without an `N/A` - so dropping the template is not a bypass. Only real bot authors (GitHub user type `Bot`, e.g. Dependabot/Renovate) are exempt.
- **Respond to review comments IN-THREAD and resolve the thread when addressed.** Reply to the specific review comment (its own thread), NOT as a separate top-level PR comment - a summary comment leaves the original conversation unresolved and blocks merge on "unresolved conversations." When a finding is fixed, reply in-thread with the fix + commit SHA and mark the thread resolved (`gh api graphql ... resolveReviewThread`). Leave a thread open only when it genuinely needs the author's decision, and say so in the reply.
- **After opening a PR, follow up on the duplication reports.** The duplicate-code and file-similarity checks post comments flagging new clones/similarity. Read them, remove duplication introduced by *this* PR before it merges; ignore clones that already existed on the base branch (out of scope for this PR).
- **Stacked PRs: put `depends on astubbs/parallel-consumer#N` in the description** (one line per parent). The PR-dependency gate blocks the child from merging until the parent does; keep the list current if the chain changes. Write the **owner/repo** form, not the bare `depends on #N` the action also accepts: the issue-reference gate reads the body too, and a bare number below the threshold fails it. Both forms are equally understood by `dependencies-action` (`partialLinkRegex`), so nothing is lost.

## Releasing

**Tag-as-truth, dispatch-triggered.** `master` is **always** a `-SNAPSHOT`. A dispatch runs
`maven-release-plugin`'s `release:prepare` (which tags the release commit) and then deploys **that exact
tag** — nothing scans git history (an earlier history-scanning version re-released an ancient upstream
commit; see `docs/plans/2026-07-28-release-pipeline-hardening.md`).

**Cut a release:**
1. Run the **Release** workflow (Actions → *Release* → *Run workflow*) with the release version (e.g.
   `0.6.0.0`) and next dev version (e.g. `0.6.0.1-SNAPSHOT`). Tick **Dry run** first to rehearse with no
   commits/tags/deploy.
2. It runs `release:prepare` (rewrites poms, makes the two release commits, tags `v<version>`, **pushes
   to `master`** via `RELEASE_PAT`), refuses if master's latest *CI* workflow run isn't green, then checks
   out that tag and deploys it to Maven Central, then cuts a GitHub release. `master` ends on the next
   `-SNAPSHOT`.

Snapshots publish automatically on every push to `master` (`publish.yml`). Workflows: `release.yml`
(release), `publish.yml` (snapshot-only).

**Required GitHub repo secrets:**
- `RELEASE_PAT` — fine-grained PAT (repo **Contents: write**) owned by a repo admin, so `release:prepare`
  can push to `master`; the **"Repository admin" role must be in the master ruleset's bypass list**.
- `MAVEN_CENTRAL_USERNAME` — Sonatype Central Portal token username
- `MAVEN_CENTRAL_PASSWORD` — Sonatype Central Portal token password
- `MAVEN_GPG_PRIVATE_KEY` — Armored GPG private key for signing artifacts
- `MAVEN_GPG_PASSPHRASE` — Passphrase for the GPG key

## Worktree ownership

**Never do any work in the main checkout. Every task gets a worktree.** The main clone at the repo
root is shared mutable state - several agent sessions run against it at once, so its HEAD can move
between two of *your own* commands. Work only under `.claude/worktrees/<name>`, and reach a task by
`cd`-ing into its worktree. `git worktree list` tells you which one holds a branch; create one if
none does.

**Reaching for `git checkout <branch>` is the tell that you are in the wrong directory** - and it is
how the rule gets broken silently. Git refuses to check out a branch another worktree already holds,
so the command *fails*; if you piped it into `tail`/`head`, the pipeline still exits 0 and a
following `&& git rebase …` runs against whatever branch you were really on. On 2026-08-06 that
rebased an unrelated PR's branch by accident. Two habits prevent it: change directory rather than
branch, and never pipe a git command whose failure must stop an `&&` chain (or test
`${PIPESTATUS[0]}`).

Multiple agents/sessions often work in parallel git worktrees (kept under `.claude/worktrees/`). Neither git nor the Claude UI records **which agent is using which worktree**, so this repo uses a convention:

- **`.worktree-owner` marker** — each worktree holds a `.worktree-owner` file at its root describing `owner`, `status`, `branch`, `pr`, and a brief `work:` line. It is **local-only** (git-ignored via `.gitignore`, so it is never committed). When you claim, hand off, or finish a worktree, write/update this file.
- **`bin/worktree-status.sh`** — prints every worktree with its marker fields plus live process holders (via `lsof`), giving the "who's on what" view the UI lacks. Run it before starting parallel work: `bash bin/worktree-status.sh`.
- **Before deleting a worktree**, verify it is safe: no live `lsof` holder, no uncommitted changes, and its branch content is merged or preserved. A marker `status: merged — SAFE TO DELETE` records that verification. For stronger protection, `git worktree lock --reason "..."` makes git refuse removal.
- The higher-level map of what each branch/worktree is for lives in `docs/inflight/` (the `branch-` and `pr-` files).

## Documented Solutions

`docs/solutions/` - documented solutions to past problems and workflow patterns, organized by category with YAML frontmatter (`module`, `tags`, `problem_type`). Relevant when implementing or debugging in documented areas.

## Refactoring backlog

Deferred internal refactors (too big/risky to fold into the change at hand) live in [`docs/refactoring.md`](docs/refactoring.md) - a versioned markdown list, grouped by file, **not** GitHub issues (overkill for a solo maintainer). When you notice one, drop a `// TODO(refactor): <one line>` marker at the spot (`grep -rn "TODO(refactor)" --include=*.java` lists them) and, if it warrants context, add an entry to the doc. **`docs/refactoring.md` also owns the triage of plain `TODO`/`FIXME`/`XXX` markers** - there are ~90 of those versus a handful using the `TODO(refactor):` convention, and they are inventoried in the generated [`docs/TODO_INDEX.md`](docs/TODO_INDEX.md) (`bin/todo-index.sh`, `--check` fails when stale). It already covers the breaking-change queue, static-state removal, offset-encoder cleanups and per-file backlogs - so write triage up here, and **do not start a parallel list** (see *Where things live* at the top). Promote an item to a branch/PR only when you actually start it; if it maps to an upstream issue, link it rather than duplicate. The doc also tracks **breaking changes queued for the next major version** in a separate, release-gated section, kept apart from the non-breaking internal refactors (those are batched for a major bump, not folded in ad hoc). This is distinct from `docs/inflight/` (in-flight), `upstream-map.yaml` (fork↔upstream), and PR review feedback (raise on the PR).

## Upstream tracking

This is a maintained hard fork of the effectively-archived `confluentinc/parallel-consumer`. We keep a durable, machine-readable cache of the fork↔upstream relationship so it never has to be re-derived from scratch:

- **`src/docs/development/upstream-map.yaml`** — the **source of truth** for the *facts*: which fork branch/PR maps to which upstream issue/PR, its work group, and current status. Its header documents the schema. Validate/render with `scripts/upstream-map.py {validate,table,refs}`.
- **`src/docs/development/upstream-pr-analysis.adoc`** — the *editorial* analysis (rankings, verdicts, recommended merge order). When prose and manifest disagree, **the manifest wins for facts**. Manifest entries link back to `.adoc` section anchors via `adoc_anchor`.
- **`docs/inflight/`** — *transient* cross-branch working notes only, one file per item.

**When you start work that maps to an upstream PR, add or update its entry in `upstream-map.yaml`** (don't just note it in prose). Design follows Debian DEP-3, Yocto `Upstream-Status:`, and OpenShift's `UPSTREAM:` fork conventions.

**If the work maps to an upstream *issue*, the fork mirror is where status goes** - diagnosis, labels, and closing all belong on the mirror, because this manifest tracks upstream **PRs** only (every upstream issue has one: astubbs#44, astubbs#117-astubbs#195, label `upstream-mirror`). Find it with `gh issue list -R astubbs/parallel-consumer --label upstream-mirror --search "confluentinc#NNN"`, and cite both numbers, fork first - see [Issue references](#issue-references).

**Keeping it in sync is the agent's job, and it does not stop at "start work".** Nothing automated checks the *fork* side: `upstream-map.py validate` only checks the schema, and `upstream-sweep.sh` only watches upstream — so a manifest that says `prs: []` while a fork PR is open still passes every check, and the mapping quietly rots (a 2026-08-04 audit found five such entries). Update the entry **at every lifecycle transition of your own work**, in the same commit that causes it: opening a PR (`prs:` + `status: pr-open`), finishing on a branch without a PR (`status: ready`), merging (`merged`), releasing (`released`), abandoning (`superseded`/`wontfix`). Loose ends do **not** go in this manifest - it has no `todo:` field. Anything a command can answer ("how far behind is PR #N?" - `git rev-list --left-right --count`) should be asked of the command rather than cached here, where it rots. Record what no command knows in `docs/inflight/`; keep this manifest to the mapping itself.

**The `upstream:` half needs the same discipline, and nothing else maintains it.** `upstream-sweep.sh` reports drift; it never writes. When it flags an entry, correct `upstream.status` and bump `last_checked` in the same pass - an item left unfixed is re-reported every run, which teaches you to skim past the report, and then it stops working at all. Verify against GitHub rather than the entry: `confluentinc#548` sat recorded `open` while it had been merged since 2023, and a header note in the same file asserted a third answer.

Branch naming and commit trailers are git conventions rather than upstream-mapping facts, so they live under [Commits](#commits).

### Mirror format

Every open upstream issue has a mirror here. When you create one, or edit one:

- **Title `confluentinc#NNN: <description>`.** The `confluentinc#NNN:` prefix is the join to
  upstream and never changes. It uses the owner form for the same reason everything else does - see
  [Issue references](#issue-references). It read `upstream #NNN:` until astubbs#196: the bulk import
  deviated from its own plan, which had specified the owner form, and the deviation was written up
  afterwards as if it were the intent. Neither form auto-links in a title, so nothing was gained by
  the role word; all 78 mirrors were retitled. The description half started as upstream's own title,
  but it is **ours to rewrite** -
  many upstream titles name only where a failure surfaced ("Error in onPartitionsAssigned") and
  contain no term anyone would search for. Retitle once the cause is actually known.
- **Always record the upstream title verbatim in the body header**, whether or not the mirror's title
  still matches it: `> Upstream title: *"..."*`. Unconditional on purpose. The obvious rule - "record
  it when you retitle" - is the error-prone one: it needs whoever retitles to remember, and it leaves
  a reader unable to tell whether any given mirror's title is upstream's words or ours without opening
  upstream to compare. Recording it always makes the mirror self-describing and the mapping lossless.
- **The body is a summary that captures the original**, not a verbatim copy - a landing page that
  preserves the substance and links out. **No `@mentions` in mirrored content**, or the import
  notifies people who never opted in.
- **Labels:** `upstream-mirror`, one area label, one type label.
- **Cross-repo references in the body are fully qualified** - `confluentinc/parallel-consumer#NN`.
  This is the one place the house prose form does not apply: `confluentinc#NN` does not auto-link on
  GitHub - only `owner/repo#NN` does - and a bare `#NN` resolves against the fork's own numbering.
  Titles are different again: nothing auto-links there, which is why they use the short owner form.
  See [Issue references](#issue-references).
- **Read the upstream original before acting on a mirror** - see [Issue references](#issue-references)
  for why the summary is not a substitute, and correct the mirror when it turns out to be wrong.

The header block:

```markdown
> **Mirror of [confluentinc/parallel-consumer#NNN](https://github.com/confluentinc/parallel-consumer/issues/NNN)**
> Upstream title: *"<upstream's own title, verbatim>"*
> Opened by [<author>](https://github.com/<author>), <YYYY-MM-DD> ·
> <N> comments upstream · last upstream activity <YYYY-MM-DD>
> Summarised, not copied. Discussion belongs here - upstream is unmaintained and may be archived.

## Summary
<2-6 sentences: symptom, conditions, what the thread established>

## Fork status
<fixed-in / investigating / not started, with links to fork PRs and docs/solutions/ entries>
```

Mirrors created before this convention may lack the upstream title line. Add it when you touch one;
there is no value in a bulk backfill pass.

`docs/plans/2026-08-04-001-chore-mirror-upstream-issues-plan.md` carries the original bulk-import plan
and what the run taught. Read it for *why*, not *how* - it is a dated record, so its own copy of this
format has since drifted. Its title prefix was right and the run was wrong: it specified
`confluentinc#NNN:`, the import shipped `upstream #NNN:`, and that deviation stood until astubbs#196
retitled all 78 back to the planned form. This section is the live one.

### Backlinking upstream

**Done, and there is no tooling for it.** All 78 open upstream issues are mirrored here (astubbs#44,
astubbs#117-astubbs#195, label `upstream-mirror`) and every one carries a backlink comment pointing at its mirror.
Seven that are fixed in a *released* version carry a second comment naming the version. This is a
finished, one-off job, not a recurring chore - `upstream-map.yaml` tracks upstream **PRs** only,
since issues live in the mirror.

**If you need to comment upstream again**, do it directly with `gh`, and:

- Put a hidden marker in the body (`<!-- pc-mirror:issue-NNN -->` or similar) and check for it before
  posting. That is the idempotency record - not a field in a manifest, which goes stale the moment
  anyone comments and, when the field was removed, silently failed *open*.
- Use **plain cross-repo references, never `Fixes`/`Closes`** - they do not auto-close cross-repo and
  we are not closing anyone's issue.
- Comment a **second** time only when there is something to act on: a fix in a published version, or
  a question answered. One meaningful notification beats a stream of empty ones.
- Fully qualify references in anything posted to GitHub - `confluentinc/parallel-consumer#NN` - since
  `upstream #NN` does not auto-link and a bare `#NN` resolves against whichever repo it lands in.

The reasoning, and what the bulk run taught, is in
[`docs/plans/2026-08-04-001-chore-mirror-upstream-issues-plan.md`](docs/plans/2026-08-04-001-chore-mirror-upstream-issues-plan.md).

### Checking upstream for new activity

`scripts/upstream-sweep.sh` (read-only) lists upstream issues/PRs with activity since the manifest's `last_swept` and flags drift on tracked refs (recorded `open` but now closed/merged upstream). `--since <date>` overrides the window; `--publish` updates a single fork tracking issue (guarded, never spams). Run it periodically to catch new reports from users who don't know the fork exists - that is its whole purpose, since upstream is otherwise static.

**It ignores our own comments, and must.** Every mirrored issue carries a backlink comment from us, and posting one bumps that issue's `updated` timestamp - so a plain `updated:>=` search returns all 78 and the signal disappears entirely. An item is reported only when it was opened inside the window, or someone who is not us commented inside it. Keep that filter if you touch the script; without it the report is the tracker read back to you.

`last_swept` lives at the top of `upstream-map.yaml`. Bump it after acting on a sweep, or every run re-reports the same items.
