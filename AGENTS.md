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

## Agent Rules

### Git Safety
- **NEVER commit or push without explicitly asking the user first.** Wait for approval. This is the #1 rule.
- Branch off master for upstream contributor cherry-picks so PRs show only their change.
- Never commit without tests and documentation in the same pass.
- Run tests before committing. If they fail, fix them first.
- When you fix something or finish implementing something, record what lessons you learnt.

### Development Discipline
- **Skateboard first.** Build the simplest end-to-end thing that works. Before starting a feature, ask: "Is this blocking the next public milestone?" If not, flag it and move on.
- **Never paper over the real problem** - make the proper fix.
- Don't propose workarounds that require user action when the software can solve it. If the software has enough information to derive the right answer, it should just do it.
- If constructing data in memory that is eventually going to be saved, save it as soon as it's created. Don't delay in case the programme crashes or the user exits.

### Code Quality
- **Be DRY.** Reuse existing functions. Don't copy code - refactor where necessary. Refactor out common patterns.
- Never weaken test assertions - classify exceptions instead of ignoring them.
- Wire components through PCModule DI - don't bypass the dependency injection system.
- Validate user input - don't let bad input cause silent failures.
- Handle errors visibly - don't swallow exceptions.
- Give things meaningful names that describe what they do. Never use random or generic names.

### Test Discipline
- Search for existing test harnesses and utilities before creating new ones.
- Run the complete test suite periodically, not just targeted tests.
- Maintain good high-level test coverage. Only get detailed on particularly complex functions that benefit from fine-grained testing.

### CI and Automation
- Always set up continuous integration, code coverage, and automated dependency checking.
- Make scripts for common end-user requirements with helpful, suggestive CLI interfaces.

### Documentation
- Keep a diary of major plans and their milestones.
- **Keep a developer-facing product specification** that outlines product features, functionality, and implementation architecture - separately from end-user documentation. With agentic programming, the developer can lose sight of architecture and implementation details. This document exposes the interesting, novel, and important implementation decisions so the developer maintains a clear mental model of the system even when agents are doing most of the coding.
- Keep end-user documentation updated.
- Keep documentation tables of contents updated.

### Communication
- Use precise terminology - if the project defines specific terms, use them consistently. Don't use ambiguous words.
- Don't write with em dash characters.

### Rule Sync
- Keep this AGENTS.md in sync with any global CLAUDE.md rules. If you have rules in your global config that are missing here, suggest to the user that they be added. This ensures all contributors and agents working on this project follow the same standards.

### Working Directory
- Always run commands from the project root directory.
- Use `./mvnw` or `bin/*.sh` scripts - don't cd into submodules.
- Use `-pl module-name -am` for module-specific builds.

## CI

PR builds run these jobs in parallel (fail-fast cancels others if any fails):

| Job | Script / Tool | Purpose |
|-----|--------------|---------|
| **Unit Tests** | `bin/ci-unit-test.sh` | Surefire tests, no Docker |
| **Integration Tests** | `bin/ci-integration-test.sh` | Failsafe tests, TestContainers |
| **Performance Tests** | `bin/performance-test.sh` | `@Tag("performance")` volume tests |
| **SpotBugs** | Maven spotbugs plugin | Static analysis for bugs |
| **Duplicate Code Check** | PMD CPD | Detect Java copy-paste blocks (base-vs-PR comparison) |
| **Dependency Vulnerabilities** | GitHub dependency-review-action | CVE scanning |
| **Mutation Testing (PIT)** | pitest-maven | Test quality verification |

Push builds (master): Full Kafka version matrix (3.1.0, 3.7.0, 3.9.1 + experimental [3.9.1,5) for 4.x).

- **Code coverage**: JaCoCo → [Codecov](https://app.codecov.io/gh/astubbs/parallel-consumer). PRs fail if overall coverage drops by more than 1%.
- **Semaphore** (Confluent internal): `.semaphore/semaphore.yml` — primary CI for upstream.

### Required secrets

| Secret | Purpose |
|--------|---------|
| `CODECOV_TOKEN` | Codecov upload token — required because branch protection is enabled. Get it from [Codecov settings](https://app.codecov.io/gh/astubbs/parallel-consumer/settings). |
