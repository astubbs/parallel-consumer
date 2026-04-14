# Parallel Consumer - Agent Context

Project context for AI coding agents (Claude Code, Copilot, Cursor, etc.).

## Overview

Parallel Consumer is a Java library that enables concurrent message processing from Apache Kafka with a single consumer, avoiding the need to increase partition counts. It maintains ordering guarantees (by partition or key) while processing messages in parallel.

This is a community-maintained fork of `confluentinc/parallel-consumer` (the upstream is no longer actively maintained), published to Maven Central as `io.github.astubbs.parallelconsumer`.

## Build Requirements

- **JDK 17** (required - the project uses Jabel to compile Java 17 source to Java 8 bytecode)
- **Docker** (required for integration tests - TestContainers spins up Kafka brokers)
- **Maven** via wrapper (`./mvnw`) - do not use system Maven

## How to Build

```bash
# Quick local build (compile + unit tests)
bin/build.sh

# Full CI build with all tests (unit + integration)
bin/ci-build.sh

# Full CI build against a specific Kafka version
bin/ci-build.sh 3.9.1
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
- **Google Truth**: Used for test assertions alongside JUnit 5 and Mockito.

## CI

- **`.github/workflows/maven.yml`** — Build and test on every push/PR. Push uses default Kafka version, PRs run the full version matrix. Includes concurrency cancellation.
- **`.github/workflows/publish.yml`** — Publishes to Maven Central on every push to `master`. The pom.xml version is the source of truth: `-SNAPSHOT` versions deploy as snapshots, non-snapshot versions deploy as full releases (and create a git tag + GitHub release).
- **`.semaphore/`** — Legacy Confluent internal CI/release pipelines, retained but inactive on the fork.

## Releasing

The pom.xml version drives publishing — there is no `maven-release-plugin` dance.

**Cut a release:**
1. Open a PR removing `-SNAPSHOT` from `<version>` in the parent pom (e.g. `0.6.0.0-SNAPSHOT` → `0.6.0.0`)
2. Merge it to master → CI publishes to Maven Central, tags `v0.6.0.0`, creates a GitHub release
3. Open another PR bumping to the next snapshot (e.g. `0.6.0.1-SNAPSHOT`) and merge

**Required GitHub repo secrets** for `publish.yml`:
- `MAVEN_CENTRAL_USERNAME` — Sonatype Central Portal token username
- `MAVEN_CENTRAL_PASSWORD` — Sonatype Central Portal token password
- `MAVEN_GPG_PRIVATE_KEY` — Armored GPG private key for signing artifacts
- `MAVEN_GPG_PASSPHRASE` — Passphrase for the GPG key
