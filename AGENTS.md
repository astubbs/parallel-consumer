# Parallel Consumer - Agent Context

Project context for AI coding agents (Claude Code, Copilot, Cursor, etc.).

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

- **Jabel cross-compilation**: Source is Java 17, bytecode targets Java 8 via Jabel annotation processor. This means `--release 8` is set in the compiler plugin, which restricts available APIs to Java 8 surface. The Mutiny module overrides this to `--release 9` because Mutiny uses `java.util.concurrent.Flow` (Java 9+).
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
- **Unit tests**: `mvn test` / surefire plugin. Source in `src/test/java/`.
- **Integration tests**: `mvn verify` / failsafe plugin. Source in `src/test-integration/java/`. Uses TestContainers with `confluentinc/cp-kafka` Docker image.
- **Test exclusion patterns**: `**/integrationTest*/**/*.java` and `**/*IT.java` are excluded from surefire, included in failsafe.
- **Kafka version matrix**: CI tests against multiple Kafka versions via `-Dkafka.version=X.Y.Z`.
- **Quarantine lane for known-failing-on-master tests (`@Quarantined`).** When a test is red on master's
  *gating* CI and its fix lives in another (open) PR, do NOT leave it red (ambiguous checks, error-prone
  merge decisions) and do NOT `@Disabled` it (loses the signal — a "known flake" can be a real product
  bug - see the drain-zombie write-up, `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md`, which lands with PR #80). Instead annotate it
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
  `./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true -Dlicense.skip -Dincluded.groups=chaos -Dexcluded.groups=`
- **Replay a schedule**: every run logs its seed and the full replay command; add `-Dchaos.seed=<seed>`.
- **CI**: per same-repo PR commit via the highcpu fast-feedback lane (check `highcpu / Chaos Pain
  Suite` - not optional: a chaos RED shows red); on-demand seeded hunts via
  `.github/workflows/chaos-pain.yml` (`workflow_dispatch`, inputs `seed`/`reps`), e.g.
  `gh workflow run chaos-pain.yml -f seed=42 -f reps=3`. Both call `bin/chaos-test.sh`. NB unlike the
  local recipe above, CI runs EXCLUDE `@Quarantined` chaos scenarios (the Quarantine Lane owns those) -
  while `ChaosChurnStormIT` is quarantined under PR #80 they therefore select zero tests, and the job
  summary flags that loudly.
- **Probe a fix PR** (the suite's primary purpose): on the fix PR's branch (merge master in first if
  the branch predates the suite landing there), run the suite at a commit before the fix (expect RED —
  the violation names the mechanism) and at the fix (expect GREEN). The local recipe above includes
  `@Quarantined` scenarios (`-Dexcluded.groups=` is empty), so known-RED detectors still fire locally.
  See `ChaosChurnStormIT`'s class javadoc for the full recipe.
- A RED run is investigation food, not flake noise — the probes are calibrated against the real historical drain-zombie defect (RED on pre-fix compositions, GREEN on fixed; thresholds sit in measured gaps). Never loosen a probe to go green; tune the workload/conductor instead.

## Known Issues

- **Mutiny module**: Has a `release.target=9` override in its pom.xml because Mutiny's `Multi` implements `java.util.concurrent.Flow.Publisher` which is not available with `--release 8`.

## Code Style

- **Lombok**: Used extensively (builders, getters, logging). IntelliJ Lombok plugin required.
- **EditorConfig**: Enforced via `.editorconfig` - 4-space indent for Java, 120 char line length.
- **License headers**: Enforced by `bin/check-copyright-headers.sh` (runs in CI via the
  `Copyright Headers` workflow; run it locally before pushing header-related changes). The mycila
  `license-maven-plugin` is skipped by default in the root pom - it knows only the Confluent header
  template, so its `format` goal used to stamp the wrong attribution onto fork-original files and its
  git-year resolver auto-bumped years and broke in worktrees. `-Dlicense.skip` on the command line is
  no longer needed (harmless if still passed).
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
- **Google Truth**: Used for test assertions alongside JUnit 5 and Mockito.

## CI

- **`.github/workflows/maven.yml`** — Build and test on every push/PR. PRs run two tiers in parallel: (1) split suites on default Kafka 3.9.1 for fast feedback (`bin/ci-unit-test.sh`, `bin/ci-integration-test.sh`, `bin/performance-test.sh`), and (2) an experimental Kafka 4.x compatibility check (`bin/ci-build.sh`). A seconds-fast "Quarantine Audit" job enforces the quarantine registry on every PR; the `@Quarantined` lane itself runs non-gating on every PR push and every push to master (+ dispatch) in its own workflow (`quarantine-lane.yml`) — see Testing. Push to master runs a single full build on default Kafka version via `bin/ci-build.sh` to gate SNAPSHOT publishing. All jobs use explicit `cache/restore` with rotating keys from the `prepare-deps` job - never `setup-java cache: 'maven'`. Includes SpotBugs, duplicate detection, mutation testing (PIT), and dependency vulnerability scanning on PRs.
- **`.github/workflows/publish.yml`** — Publishes to Maven Central on every push to `master`. The pom.xml version is the source of truth: `-SNAPSHOT` versions deploy as snapshots, non-snapshot versions deploy as full releases (and create a git tag + GitHub release).
- **`.github/workflows/copyright.yml`** — Copyright-header conformance via `bin/check-copyright-headers.sh` (runs its self-test `bin/test-check-copyright-headers.sh` first, then the real scan) on every push/PR. GitHub-hosted; needs `fetch-depth: 0` so the fork-point commit is in history.
- **`.semaphore/`** — Legacy Confluent internal CI/release pipelines, retained but inactive on the fork.

## Changelog

`CHANGELOG.adoc` (repo root) is the source of truth for release notes; `README.adoc` regenerates from it at build/release time (never hand-edit `README.adoc` - see Code Style / the generated-README rule). **When you make a user- or operator-visible change, add a `CHANGELOG.adoc` entry in the same PR**, under `== Unreleased` (create that heading if it's missing, above the latest version), in the right subsection: `=== Breaking`, `=== Improvements`, `=== Fixes`, `=== Dependencies`, or `=== Build & CI`.

- **Do add:** behavioural/API changes, new features or modules, user-affecting bug fixes, and *notable or coordinated* dependency refreshes or any change to a user-facing runtime dependency (especially the Kafka client) - for a library these affect the transitive dependencies and compatibility that consumers inherit.
- **Do add (`=== Build & CI`):** notable build, CI, tooling and test-infrastructure changes. This is a deeply technical library and its own contributors/agents are a primary audience - a new CI capability, a runner/workflow, a mutation/quarantine mechanism, or a build-enforcement change is worth recording, not just buried in git history.
- **Don't add:** routine/automated single dependency bumps (Dependabot), no-op internal refactors, and pure formatting - genuinely invisible churn. (Everything with a real effect on how the project builds, tests, releases, or behaves is fair game.)
- **Reference convention:** a bare `#NN` refers to this fork; write `upstream #NN` for upstream references, and link the PR/issue.

Keep it a changelog people actually read, not a commit log: merge related entries, drop vanity items, and write for a future reader scanning for what changed.

## PR Discipline

- **Keep the PR title and body in sync with what the PR actually covers.** As a PR grows, its description drifts - re-check it before requesting review and before merge. Update it only on *material* drift: whole changes/workstreams missing, wrong specifics (core counts, flags, forkCounts, file/label names), or scope that has outgrown the title. Do NOT churn the description for cosmetic wording - if it still accurately reflects the content, leave it.
- **Open PRs from the template and complete its checklist honestly.** `.github/PULL_REQUEST_TEMPLATE.md` is NOT auto-applied when a PR is created non-interactively (e.g. `gh pr create --body-file`), so base the PR body on it and resolve every box: check it `[x]`, or mark it `N/A - <reason>`. For human-authored PRs the `PR Checklist` CI gate (`.github/workflows/pr-checklist.yml`) fails when the checklist is missing entirely *or* when any box is left unchecked without an `N/A` - so dropping the template is not a bypass. Only real bot authors (GitHub user type `Bot`, e.g. Dependabot/Renovate) are exempt.
- **Respond to review comments IN-THREAD and resolve the thread when addressed.** Reply to the specific review comment (its own thread), NOT as a separate top-level PR comment - a summary comment leaves the original conversation unresolved and blocks merge on "unresolved conversations." When a finding is fixed, reply in-thread with the fix + commit SHA and mark the thread resolved (`gh api graphql ... resolveReviewThread`). Leave a thread open only when it genuinely needs the author's decision, and say so in the reply.
- **After opening a PR, follow up on the duplication reports.** The duplicate-code and file-similarity checks post comments flagging new clones/similarity. Read them, remove duplication introduced by *this* PR before it merges; ignore clones that already existed on the base branch (out of scope for this PR).
- **Stacked PRs: put `depends on #N` in the description** (one line per parent). The PR-dependency gate blocks the child from merging until the parent does; keep the list current if the chain changes.

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

Multiple agents/sessions often work in parallel git worktrees (kept under `.claude/worktrees/`). Neither git nor the Claude UI records **which agent is using which worktree**, so this repo uses a convention:

- **`.worktree-owner` marker** — each worktree holds a `.worktree-owner` file at its root describing `owner`, `status`, `branch`, `pr`, and a brief `work:` line. It is **local-only** (git-ignored via `.gitignore`, so it is never committed). When you claim, hand off, or finish a worktree, write/update this file.
- **`bin/worktree-status.sh`** — prints every worktree with its marker fields plus live process holders (via `lsof`), giving the "who's on what" view the UI lacks. Run it before starting parallel work: `bash bin/worktree-status.sh`.
- **Before deleting a worktree**, verify it is safe: no live `lsof` holder, no uncommitted changes, and its branch content is merged or preserved. A marker `status: merged — SAFE TO DELETE` records that verification. For stronger protection, `git worktree lock --reason "..."` makes git refuse removal.
- The higher-level map of what each branch/worktree is for lives in `docs/inflight.md`.

## Documented Solutions

`docs/solutions/` - documented solutions to past problems and workflow patterns, organized by category with YAML frontmatter (`module`, `tags`, `problem_type`). Relevant when implementing or debugging in documented areas.

## Refactoring backlog

Deferred internal refactors (too big/risky to fold into the change at hand) live in [`docs/refactoring.md`](docs/refactoring.md) - a versioned markdown list, grouped by file, **not** GitHub issues (overkill for a solo maintainer). When you notice one, drop a `// TODO(refactor): <one line>` marker at the spot (`grep -rn "TODO(refactor)" --include=*.java` lists them) and, if it warrants context, add an entry to the doc. Promote an item to a branch/PR only when you actually start it; if it maps to an upstream issue, link it rather than duplicate. The doc also tracks **breaking changes queued for the next major version** in a separate, release-gated section, kept apart from the non-breaking internal refactors (those are batched for a major bump, not folded in ad hoc). This is distinct from `docs/inflight.md` (in-flight), `upstream-map.yaml` (fork↔upstream), and PR review feedback (raise on the PR).

## Upstream tracking

This is a maintained hard fork of the effectively-archived `confluentinc/parallel-consumer`. We keep a durable, machine-readable cache of the fork↔upstream relationship so it never has to be re-derived from scratch:

- **`src/docs/development/upstream-map.yaml`** — the **source of truth** for the *facts*: which fork branch/PR maps to which upstream issue/PR, its work group, and current status. Its header documents the schema. Validate/render with `scripts/upstream-map.py {validate,table,refs}`.
- **`src/docs/development/upstream-pr-analysis.adoc`** — the *editorial* analysis (rankings, verdicts, recommended merge order). When prose and manifest disagree, **the manifest wins for facts**. Manifest entries link back to `.adoc` section anchors via `adoc_anchor`.
- **`docs/inflight.md`** — *transient* cross-branch working notes only.

**When you start work that maps to an upstream issue/PR, add or update its entry in `upstream-map.yaml`** (don't just note it in prose). Design follows Debian DEP-3, Yocto `Upstream-Status:`, and OpenShift's `UPSTREAM:` fork conventions.

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

Enable the editor prompt once per checkout: `git config commit.template .gitmessage`. Keep the existing subject convention (`... (#893)`, `cherry-pick Confluent #905`). **Trailers are not enforced** — they only fit upstream-related commits, not fork-only work (rebrand, release, dependabot, formatting). Use judgement.

### Backlinking upstream

When we fix something downstream, we comment on the matching upstream issue/PR so users (who mostly don't know the fork exists) can find the fix. Driven by the manifest:

```
scripts/upstream-backlink.sh <entry-id>            # DRY-RUN (default): prints target + comment, posts nothing
scripts/upstream-backlink.sh --post <entry-id>     # actually comment (prompts; needs gh auth)
```

Two generic templates in `scripts/backlink-templates/`: `fix-backlink` (a fix is available in the fork) and `fork-awareness` (this is now maintained in a fork). For anything needing a tailored explanation, set a per-entry **`backlink`** field in `upstream-map.yaml` (it supports `{{FORK_REPO}}` / `{{FORK_REF}}` / `{{SUMMARY}}` / `{{ID}}`) - it overrides the template so the public wording lives in the source of truth, not a separate file (see the `bug-859-pcmetrics-leak` entry). Use **plain cross-repo references, never `Fixes/Closes`** (they don't auto-close cross-repo and we're not closing anyone's issue) — one respectful comment per item. After posting, paste the printed `forwarded:` snippet back into the entry in `upstream-map.yaml` (this is also what makes future runs skip the target). The helper is anti-spam by design: idempotent skip of already-forwarded targets, per-run cap (`--max`), inter-post delay, and a status guard so unfinished work can't be announced as fixed.

### Checking upstream for new activity

`scripts/upstream-sweep.sh` (read-only) lists upstream issues/PRs updated since the manifest's `last_swept` and flags drift on tracked refs (recorded `open` but now closed/merged). `--since <date>` overrides the window; `--publish` updates a single fork tracking issue (guarded, never spams). Run it periodically to catch new reports from users who don't know the fork exists.

The full per-item backlink plan, anti-spam details, and the sweep design live in [`src/docs/development/upstream-backlink-plan.md`](src/docs/development/upstream-backlink-plan.md).
