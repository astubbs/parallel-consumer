---
title: Copyright header management rules for Apache 2.0 fork
date: 2026-04-21
category: workflow-issues
module: build-system
problem_type: workflow_issue
component: development_workflow
severity: medium
applies_when:
  - Rebranding an Apache 2.0 fork under new Maven coordinates
  - Creating new files in a forked repository
  - Running Maven builds that trigger the license plugin
  - Code review of changes to files with copyright headers
tags:
  - copyright
  - license-headers
  - fork
  - maven
  - mycila
  - apache-2
  - agents-md
---

# Copyright header management rules for Apache 2.0 fork

## Context

The `astubbs/parallel-consumer` repository is an Apache 2.0 fork of `confluentinc/parallel-consumer`, rebranded under `io.github.astubbs.parallelconsumer` Maven coordinates. During the `dev/rebrand-fork` branch review, three copyright header problems surfaced:

1. **License plugin auto-bumps years**: The `license-maven-plugin` (Mycila) auto-bumps copyright year ranges when run without `-Dlicense.skip`. A routine pom.xml property addition caused `parallel-consumer-mutiny/pom.xml` to silently gain a year bump from `2020-2025` to `2020-2026` as an unintended side effect.

2. **New fork files wrongly attributed**: Six new `bin/` scripts (build.sh, ci-build.sh, ci-integration-test.sh, ci-unit-test.sh, performance-test.cmd, performance-test.sh) were written entirely for the fork but carried `Copyright (C) 2020-2026 Confluent, Inc.` headers. Confluent didn't write them and they didn't exist before 2026.

3. **Review fix changed copyright incorrectly**: During code review autofix, the reviewer changed `EpochAndRecordsMap.java`'s copyright from `2020-2022 Confluent, Inc.` to `2020-2026 Confluent, Inc. and contributors`. The original PR commit only added code (a null-epoch guard), not a copyright change - the reviewer introduced a spurious copyright modification that had to be reverted.

None of this was caught early because AGENTS.md had no guidance on copyright header management.

## Guidance

Rules added to AGENTS.md under "Code Style":

```
- **Copyright rules for this fork**:
  - Do not change copyright headers on existing files unless the file has
    substantive code changes in the same commit
  - Do not bump copyright years as an incidental or standalone change
  - The `NOTICE` file at repo root contains the legal attribution structure
  - New files written entirely for the fork should not claim Confluent copyright
  - Always pass `-Dlicense.skip` to Maven to prevent the license plugin from
    auto-bumping years
```

The NOTICE file at the repository root is the authoritative attribution record:

```
Parallel Consumer
Copyright 2020-2026 Confluent, Inc.
Copyright 2026 Antony Stubbs and contributors
```

The pom.xml license template header was changed from:

```
Copyright (C) ${license.git.copyrightYears} ${project.organization.name}
```

to:

```
Copyright (C) 2020-${license.git.copyrightYears} Confluent, Inc. and contributors
```

## Why This Matters

**Legal accuracy.** Copyright headers are legal statements. Attributing work to Confluent that Confluent did not write, or claiming a file dates from 2020 when it was created in 2026, produces inaccurate legal records.

**Diff noise.** Spurious year bumps pollute commits and PRs with meaningless diffs. A reviewer looking at a pom.xml change to add `<release.target>9</release.target>` should not have to mentally filter out an unrelated copyright line change. This also makes git blame less useful.

**Review trust.** When an automated reviewer or agent changes copyright headers as a "fix," it undermines confidence in the review. The `EpochAndRecordsMap.java` incident changed a historically accurate range (`2020-2022`) to a wrong one (`2020-2026`).

**Plugin footgun.** The Mycila license plugin runs as part of the normal build cycle. Without `-Dlicense.skip`, any Maven command that triggers `license:check` or `license:format` will rewrite headers silently. The plugin also breaks in git worktrees.

## When to Apply

- Any time you run a Maven command: always include `-Dlicense.skip`
- Any time you create a new file that did not exist in upstream: do not add Confluent copyright
- Any time a code review suggests changing a copyright header: only accept if the same commit has substantive code changes
- Any time you see a standalone "bump copyright year" commit: reject it
- Any time you add a property, dependency, or config-only change to a pom.xml: the copyright header must not change

## Examples

**Bad - license plugin auto-bump as side effect:**

```xml
<!-- parallel-consumer-mutiny/pom.xml - only change was adding a property -->
<!-- Before (correct): Copyright (C) 2020-2025 Confluent, Inc. -->
<!-- After running Maven without -Dlicense.skip (wrong): Copyright (C) 2020-2026 Confluent, Inc. -->
```

Fix: run Maven with `-Dlicense.skip` and revert the header change.

**Bad - new fork-only file claiming upstream copyright:**

```bash
#!/bin/bash
# Copyright (C) 2020-2026 Confluent, Inc.   <-- WRONG
# bin/build.sh - created entirely for the astubbs fork in 2026
```

**Bad - reviewer adding spurious copyright change alongside a code fix:**

```java
// EpochAndRecordsMap.java
// Before (historically accurate): Copyright (C) 2020-2022 Confluent, Inc.
// After reviewer autofix (wrong): Copyright (C) 2020-2026 Confluent, Inc. and contributors
```

The commit only added a null-epoch guard. The range `2020-2022` was correct. The reviewer's change was reverted.

**Correct - substantive code change warrants a header update:**

```java
// Before: Copyright (C) 2020-2022 Confluent, Inc.
// After a real 2026 code change: Copyright (C) 2020-2026 Confluent, Inc. and contributors
```

## Related

- `AGENTS.md` lines 70-76 - the codified rules (authoritative source)
- `NOTICE` file at repo root - legal attribution structure
- `pom.xml` line ~613 - license template `inlineHeader` configuration
