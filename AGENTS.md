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
- **Chaos Pain Suite** (`chaostests/` under `src/test-integration/`): opt-in stress scenarios hunting the #857 stall families (W1 churn storm, W4 revoke-under-work). Excluded from default suites via `@Tag("chaos")`; run with `./mvnw -pl parallel-consumer-core -Dincluded.groups=chaos -Dexcluded.groups= -Dlicense.skip verify`. Every run logs its seed; replay a schedule with `-Dchaos.seed=<long>`. Probe violations are self-diagnosing (they name the stall class, the measured value, and the bound). CI entry point: `.github/workflows/chaos-pain.yml` (workflow_dispatch, seed/reps inputs). Design and calibration history: `docs/plans/2026-07-30-*chaos-pain-suite*` plans and the Phase 2+ roster in `docs/inflight.md`.
- **Reuse test utilities — search before you add (DRY).** Shared client/broker helpers live in `KafkaClientUtils` (topic creation, producers, consumers, PC builders) and `BrokerIntegrationTest` (the base class most integration tests extend). Before writing a new helper or a raw `admin`/producer/consumer call in a test, search these two first and extend them. Duplicating an existing helper is how bugs get reintroduced — e.g. a copy of topic-creation logic drifted to a 1-second timeout and became a flaky-CI source (see `docs/solutions/test-issues/`). When you must add a helper, put it in the shared util, not the test. Also check `docs/solutions/` for prior art before solving a problem that feels familiar.

## Known Issues

- **Mutiny module**: Has a `release.target=9` override in its pom.xml because Mutiny's `Multi` implements `java.util.concurrent.Flow.Publisher` which is not available with `--release 8`.

## Code Style

- **Lombok**: Used extensively (builders, getters, logging). IntelliJ Lombok plugin required.
- **EditorConfig**: Enforced via `.editorconfig` - 4-space indent for Java, 120 char line length.
- **License headers**: Managed by `license-maven-plugin` (Mycila). Use `-Dlicense.skip` locally to skip checks.
- **Copyright rules for this fork**:
  - Do not change copyright headers on existing files unless the file has substantive code changes in the same commit
  - Do not bump copyright years as an incidental or standalone change
  - The `NOTICE` file at repo root contains the legal attribution structure for the fork
  - New files written entirely for the fork should not claim Confluent copyright
  - Always pass `-Dlicense.skip` to Maven to prevent the license plugin from auto-bumping years
- **Google Truth**: Used for test assertions alongside JUnit 5 and Mockito.

## CI

- **`.github/workflows/maven.yml`** — Build and test on every push/PR. PRs run two tiers in parallel: (1) split suites on default Kafka 3.9.1 for fast feedback (`bin/ci-unit-test.sh`, `bin/ci-integration-test.sh`, `bin/performance-test.sh`), and (2) an experimental Kafka 4.x compatibility check (`bin/ci-build.sh`). Push to master runs a single full build on default Kafka version via `bin/ci-build.sh` to gate SNAPSHOT publishing. All jobs use explicit `cache/restore` with rotating keys from the `prepare-deps` job - never `setup-java cache: 'maven'`. Includes SpotBugs, duplicate detection, mutation testing (PIT), and dependency vulnerability scanning on PRs.
- **`.github/workflows/publish.yml`** — Publishes to Maven Central on every push to `master`. The pom.xml version is the source of truth: `-SNAPSHOT` versions deploy as snapshots, non-snapshot versions deploy as full releases (and create a git tag + GitHub release).
- **`.semaphore/`** — Legacy Confluent internal CI/release pipelines, retained but inactive on the fork.

## Changelog

`CHANGELOG.adoc` (repo root) is the source of truth for release notes; `README.adoc` regenerates from it at build/release time (never hand-edit `README.adoc` - see Code Style / the generated-README rule). **When you make a user- or operator-visible change, add a `CHANGELOG.adoc` entry in the same PR**, under `== Unreleased` (create that heading if it's missing, above the latest version), in the right subsection: `=== Breaking`, `=== Improvements`, `=== Fixes`, `=== Dependencies`, or `=== Build & CI`.

- **Do add:** behavioural/API changes, new features or modules, user-affecting bug fixes, and *notable or coordinated* dependency refreshes or any change to a user-facing runtime dependency (especially the Kafka client) - for a library these affect the transitive dependencies and compatibility that consumers inherit.
- **Don't add:** routine/automated single dependency bumps (Dependabot), internal refactors, test-only changes, CI/tooling, docs, or formatting - those are visible in git history and just add noise.
- **Reference convention:** a bare `#NN` refers to this fork; write `upstream #NN` for upstream references, and link the PR/issue.

Keep it a changelog people actually read, not a commit log: merge related entries, drop vanity items, and write for a future reader scanning for what changed.

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
   to `master`** via `RELEASE_PAT`), refuses if master's latest *Build and Test* isn't green, then checks
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
