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

- **Unit tests**: `mvn test` / surefire plugin. Source in `src/test/java/`.
- **Integration tests**: `mvn verify` / failsafe plugin. Source in `src/test-integration/java/`. Uses TestContainers with `confluentinc/cp-kafka` Docker image.
- **Test exclusion patterns**: `**/integrationTest*/**/*.java` and `**/*IT.java` are excluded from surefire, included in failsafe.
- **Kafka version matrix**: CI tests against multiple Kafka versions via `-Dkafka.version=X.Y.Z`.

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

## Documented Solutions

`docs/solutions/` - documented solutions to past problems and workflow patterns, organized by category with YAML frontmatter (`module`, `tags`, `problem_type`). Relevant when implementing or debugging in documented areas.
