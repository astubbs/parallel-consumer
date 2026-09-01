---
title: Maven Central download timeouts on GitHub Actions runners in Azure West US regions
date: 2026-04-21
category: build-errors
module: build-system
problem_type: build_error
component: development_workflow
symptoms:
  - "Could not transfer artifact io.vertx:vertx-web-client:pom:4.5.7 from/to central: Read timed out"
  - Build fails consistently on vertx module after core module succeeds
  - Exactly 240-second (4-minute) hang per artifact download attempt from Maven Central
  - AK 3.9.1 matrix entry passes while AK 3.7.0 and 3.1.0 fail in the same workflow run
root_cause: config_error
resolution_type: config_change
severity: high
tags:
  - maven-central
  - github-actions
  - azure-region
  - vertx
  - timeout
  - cache-warming
  - ci
---

# Maven Central download timeouts on GitHub Actions runners in Azure West US regions

## Problem

GitHub Actions CI builds consistently fail downloading Maven dependencies (appearing as vertx timeouts), but only on some matrix entries while others pass in the same workflow run. The failures are not random - they correlate with which Azure data center the runner is assigned to.

## Symptoms

- `Could not transfer artifact io.vertx:vertx-web-client:pom:4.5.7 from/to central (https://repo1.maven.org/maven2/): Read timed out`
- Each download attempt hangs for exactly 240 seconds before falling through to the next repository
- The vertx module always fails; core module always passes (because core's deps are already cached)
- Re-running the failed jobs produces the same failure
- The same artifact downloads in under 200ms locally and in under 200ms from East US runners

## What Didn't Work

- **Re-running failed jobs** - same runners in the same regions, same timeout
- **`retryHandler.count=3` in `.mvn/maven.config`** - retries don't help when the CDN route itself is broken. Each retry adds another 120s timeout, totalling 240s+ per artifact per repository
- **Assuming it was a vertx-specific issue** - vertx appeared to be the problem because it's the first module with uncached dependencies. The real issue is the network route from certain Azure regions to Maven Central's CDN

## Solution

The root cause is that Maven Central's CDN has degraded connectivity from Azure's western US data centers (westcentralus, westus3). Since you can't control which region GitHub assigns your runner to, the fix is to pre-warm the Maven cache so no module needs to download from Central during the actual build.

The `prepare-deps` cache warming job downloads all dependencies once (including vertx), then the matrix jobs restore from that cache:

```yaml
# .github/workflows/maven.yml

prepare-deps:
  # Removed: if: github.event_name == 'pull_request'
  # Now runs on BOTH PR and push builds
  name: "Prepare Maven Cache"
  runs-on: ubuntu-latest
  timeout-minutes: 15
  steps:
    - uses: actions/checkout@v6
    - uses: actions/setup-java@v5
      with:
        distribution: 'temurin'
        java-version: '17'
    - name: Restore Maven cache
      uses: actions/cache/restore@v4
      with:
        path: ~/.m2/repository
        key: setup-java-Linux-x64-maven-${{ hashFiles('**/pom.xml') }}
        restore-keys: |
          setup-java-Linux-x64-maven-
    - name: Download all dependencies
      run: ./mvnw --batch-mode -Pci dependency:go-offline -DincludeScope=test -U
    - name: Save Maven cache (rotating key)
      if: success()
      uses: actions/cache/save@v4
      with:
        path: ~/.m2/repository
        key: setup-java-Linux-x64-maven-${{ hashFiles('**/pom.xml') }}-${{ github.run_id }}

build:
  if: github.event_name == 'push'
  needs: prepare-deps  # <-- added: wait for cache warming
  # ... matrix config ...
```

The rotating `...-${{ github.run_id }}` save key ensures every successful cache-warming run can update the cache, unlike `setup-java`'s built-in caching which uses `actions/cache` (won't save when the primary key hits).

## Why This Works

**The root cause is Azure region routing, not Maven Central or vertx.**

Evidence from the same workflow run (24700111718):

| Job | Azure Region | vertx-web-client download | Result |
|-----|-------------|--------------------------|--------|
| AK 3.9.1 | **eastus** | 160ms | Passed |
| AK 3.7.0 | **westcentralus** | 240s timeout | Failed |
| AK 3.1.0 | **westus3** | 30min timeout | Failed |

All three jobs started from the same cache, ran the same code, and tried to download the same 2.9KB POM file from the same Maven Central URL. The only difference was which Azure data center the runner was in.

Vertx appeared to be the culprit because:
1. The reactor build order puts vertx as the **first module after core** that needs non-core dependencies
2. Core module dependencies (kafka-clients, slf4j, lombok) are either already cached or hosted on faster CDN paths
3. When vertx fails, reactor and mutiny are SKIPPED - so vertx always appears to be the only failing module

With cache warming, all artifacts (including vertx) are downloaded by a single `prepare-deps` job. If that job lands on a bad region, it may take longer but will eventually succeed within its 15-minute timeout and 3 retries. The subsequent matrix jobs then read everything from the local cache regardless of their runner's region.

## Prevention

- **Always run `prepare-deps` before jobs that need Maven dependencies** - don't rely on `setup-java`'s built-in `cache: 'maven'` alone for push builds. The built-in cache uses `actions/cache` which won't overwrite an existing (possibly incomplete) cache key.
- **Use rotating cache keys** (`...-${{ github.run_id }}`) so each successful run can update the cache. Static keys based only on `hashFiles('**/pom.xml')` get stuck if the first cache entry was incomplete.
- **Don't chase the symptom** - when a specific dependency consistently times out in CI, check the Azure region of passing vs failing runners before assuming the dependency or its repository is the problem.

## Related Issues

- PR astubbs#48 (`fix/prepare-deps-push-builds`) - the fix that extends cache warming to push builds
- `docs/solutions/security-issues/ci-hardening-pull-request-target-mutable-refs-publish-gate-2026-04-21.md` - companion CI hardening doc
- `.mvn/maven.config` - connection timeout settings (10s connect, 120s read, 3 retries)
