# Parallel Consumer - Agent Context

Project context for AI coding agents (Claude Code, Copilot, Cursor, etc.).

## Overview

Confluent Parallel Consumer is a Java library that enables concurrent message processing from Apache Kafka with a single consumer, avoiding the need to increase partition counts. It maintains ordering guarantees (by partition or key) while processing messages in parallel.

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
- **Performance tests**: Tagged `@Tag("performance")` and excluded from regular CI by default. They run on a self-hosted runner via `.github/workflows/performance.yml` (see `docs/SELF_HOSTED_RUNNER.md`). Run locally with `bin/performance-test.sh` (or `bin/performance-test.cmd` on Windows). Override the test group selection with Maven properties: `-Dincluded.groups=performance` to run only perf, `-Dexcluded.groups=` to run everything.

## Known Issues

- **Mutiny module**: Has a `release.target=9` override in its pom.xml because Mutiny's `Multi` implements `java.util.concurrent.Flow.Publisher` which is not available with `--release 8`.

## Code Style

- **Lombok**: Used extensively (builders, getters, logging). IntelliJ Lombok plugin required.
- **EditorConfig**: Enforced via `.editorconfig` - 4-space indent for Java, 120 char line length.
- **License headers**: Managed by `license-maven-plugin` (Mycila). See "License headers" section below.
- **Google Truth**: Used for test assertions alongside JUnit 5 and Mockito.

## License headers

The Mycila `license-maven-plugin` enforces a Confluent copyright header on all source files. It uses git-derived years via `${license.git.copyrightYears}`.

**Skipping the check** (for any Maven goal):
```
./mvnw <goal> -Dlicense.skip
```

**When to skip:**
- Running builds inside a git worktree — the git-years lookup fails with `Bare Repository has neither a working tree, nor an index`
- Local iteration where you don't want years auto-bumped on touched files
- Any command other than the canonical `mvn install` flow when copyright drift would create noise in `git status`

The default behavior on macOS dev machines is `format` mode (auto-fixes headers) via the `license-format` profile (auto-activated). The `ci` profile flips this to `check` mode (fails the build on drift). Both `bin/build.sh` and `bin/ci-build.sh` already pass `-Dlicense.skip` for this reason.

**When NOT to skip:**
- You're intentionally running `mvn license:format` to update headers
- You want to verify CI's check would pass before pushing

## CI

- **GitHub Actions**: `.github/workflows/maven.yml` - runs on push/PR to master with Kafka version matrix.
- **Semaphore** (Confluent internal): `.semaphore/semaphore.yml` - primary CI for upstream.
