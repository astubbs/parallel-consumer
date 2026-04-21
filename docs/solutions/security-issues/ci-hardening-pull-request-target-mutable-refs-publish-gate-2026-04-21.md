---
title: "CI Security Hardening: pull_request_target, Mutable Action Refs, Missing Test Gates, and Fragile Static Parsers"
date: 2026-04-21
category: security-issues
module: build-system
problem_type: security_issue
component: development_workflow
root_cause: config_error
resolution_type: config_change
severity: high
applies_when:
  - Setting up GitHub Actions CI for a fork that accepts external PRs
  - Using custom or third-party actions with write permissions
  - Publishing artifacts to Maven Central from CI
  - Parsing version strings in test infrastructure static initializers
tags:
  - ci
  - github-actions
  - supply-chain
  - action-pinning
  - publish-gate
  - static-initializer
  - pull-request-target
---

# CI Security Hardening: pull_request_target, Mutable Action Refs, Missing Test Gates, and Fragile Static Parsers

## Context

During code review of the `dev/rebrand-fork` branch (parallel-consumer fork rebranding), four CI security and robustness issues were identified. These ranged from a critical supply-chain vulnerability (`pull_request_target` + mutable action ref) to a test-killing `ExceptionInInitializerError` from unparseable version strings. None had been exploited, but all represented real attack surface or reliability gaps.

The fixes establish patterns applicable to any project using GitHub Actions with fork PRs, reusable actions, and Maven publishing.

## Guidance

### 1. Never use `pull_request_target` with mutable action refs

`pull_request_target` runs workflows in the context of the *base* repository, with access to base-repo secrets and elevated permissions. When combined with a mutable action ref (branch name), an attacker controlling that action branch can execute arbitrary code with write permissions on every fork PR.

**Rule**: If you need `pull_request_target`, pin every action to a commit SHA. Prefer switching to `pull_request` where the elevated capability is not actually needed.

### 2. Pin all action refs to commit SHAs

Branch and tag refs are mutable pointers. A force-push upstream silently changes what code runs in your CI, with whatever permissions `GITHUB_TOKEN` grants. SHA pins are immutable.

**Rule**: Every `uses:` entry in a workflow that has any write permission must reference a commit SHA. Keep the original ref as a trailing comment for human readability.

### 3. Gate publishing on a successful test run

Publishing on raw `push` with `-DskipTests` means any test-failing commit produces a published SNAPSHOT artifact. Downstream consumers inherit broken code.

**Rule**: Use `workflow_run` to trigger publishing only after the build-and-test workflow succeeds. Add an explicit `if:` guard on the conclusion. Keep `workflow_dispatch` so maintainers can manually re-run.

### 4. Harden version string parsing for pre-release suffixes

When parsing version strings at runtime, `Integer.parseInt()` on a string like `"4.0.0-SNAPSHOT"` throws `NumberFormatException`. If this code runs in a static initializer, the resulting `ExceptionInInitializerError` kills every test in every class that inherits the initializer - a confusing failure mode that appears unrelated to the actual cause.

**Rule**: Strip pre-release suffixes before parsing. Wrap in a try/catch with a safe fallback. Never let version detection run in a static initializer without a fallback.

## Why This Matters

**Supply chain risk (Issues 1 & 2).** `pull_request_target` with mutable action refs is a well-documented GitHub Actions attack vector. An attacker who can push to the referenced action branch - including via a compromised maintainer account on a third-party repo - silently executes code with `GITHUB_TOKEN` write access on every incoming PR. This can exfiltrate secrets, poison releases, or modify repository state.

**Artifact integrity (Issue 3).** Publishing broken SNAPSHOTs trains downstream developers to distrust the artifact feed. In a library project, a bad SNAPSHOT propagates into dependent projects before anyone notices.

**Test infrastructure fragility (Issue 4).** Static initializer failures produce `ExceptionInInitializerError` which JUnit reports as a class-level error, not a test failure. The root cause is buried in the stack trace with no obvious connection to the test being run.

## When to Apply

- **SHA pinning**: Any GitHub Actions workflow with write permissions, secrets access, or `pull_request_target` trigger
- **`pull_request` vs `pull_request_target`**: Use `pull_request_target` only when you explicitly need base-repo secrets for fork PRs (e.g., posting a comment after a permission check). For read-only checks, use `pull_request`
- **Publish gating**: Whenever publishing is triggered by a push and the build/test workflow lives in a separate workflow file. If tests and publishing are in the same file, a `needs:` dependency is simpler
- **Version string hardening**: Whenever parsing version strings from dependency metadata, environment variables, or external configuration - especially in static initializers or `@BeforeAll`

## Examples

### check-dependencies.yml (pull_request_target + mutable ref)

Before (vulnerable):
```yaml
on:
  pull_request_target:
    types: [opened, edited, closed, reopened]
permissions:
  checks: write
jobs:
  check_dependencies:
    steps:
    - uses: astubbs/dependencies-action@feat/auto-unblock-children-on-merge
```

After (hardened):
```yaml
on:
  pull_request:
    types: [opened, edited, closed, reopened]
permissions:
  checks: write
jobs:
  check_dependencies:
    steps:
    - uses: astubbs/dependencies-action@a09974c # feat/auto-unblock-children-on-merge
```

### maven.yml (mutable action refs)

Before:
```yaml
- uses: astubbs/duplicate-code-cross-check@v1
- uses: astubbs/duplicate-code-detection-tool@feat/base-vs-pr-comparison
```

After:
```yaml
- uses: astubbs/duplicate-code-cross-check@d3140ef # v1
- uses: astubbs/duplicate-code-detection-tool@4e302e7 # feat/base-vs-pr-comparison
```

### publish.yml (missing test gate)

Before:
```yaml
on:
  push:
    branches: [ master ]
jobs:
  publish:
    steps:
      - run: ./mvnw deploy -DskipTests
```

After:
```yaml
on:
  workflow_run:
    workflows: ["Build and Test"]
    branches: [ master ]
    types: [completed]
  workflow_dispatch:
jobs:
  publish:
    if: github.event_name == 'workflow_dispatch' || github.event.workflow_run.conclusion == 'success'
    steps:
      - run: ./mvnw deploy -DskipTests
```

### BrokerIntegrationTest.java (fragile version parsing)

Before:
```java
static String deriveCpKafkaImage() {
    String akVersion = AppInfoParser.getVersion();
    String[] parts = akVersion.split("\\.");
    int akMajor = Integer.parseInt(parts[0]);  // throws on "4.0.0-SNAPSHOT"
    int akMinor = Integer.parseInt(parts[1]);
    return "confluentinc/cp-kafka:" + (akMajor + 4) + "." + akMinor + ".0";
}
```

After:
```java
private static final String FALLBACK_CP_IMAGE = "confluentinc/cp-kafka:7.9.0";

static String deriveCpKafkaImage() {
    String akVersion = AppInfoParser.getVersion();
    try {
        String cleanVersion = akVersion.split("-")[0]; // strip -SNAPSHOT, -rc1
        String[] parts = cleanVersion.split("\\.");
        int akMajor = Integer.parseInt(parts[0]);
        int akMinor = Integer.parseInt(parts[1]);
        return "confluentinc/cp-kafka:" + (akMajor + 4) + "." + akMinor + ".0";
    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
        log.warn("Could not parse Kafka version '{}', falling back to {}",
                 akVersion, FALLBACK_CP_IMAGE, e);
        return FALLBACK_CP_IMAGE;
    }
}
```

## Related

- `docs/solutions/workflow-issues/copyright-header-rules-for-fork-2026-04-21.md` - companion fork workflow guidance (copyright headers)
- `.github/workflows/check-dependencies.yml` - primary subject of Issue 1
- `.github/workflows/maven.yml` - primary subject of Issue 2
- `.github/workflows/publish.yml` - primary subject of Issue 3
- `parallel-consumer-core/src/test-integration/.../BrokerIntegrationTest.java` - primary subject of Issue 4
