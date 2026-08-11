---
title: "Declared is not resolvable - four Maven reactor traps in this repo"
date: 2026-08-11
category: build-errors
module: build-system
problem_type: build_error
component: development_workflow
severity: high
symptoms:
  - "`'build.plugins.plugin.version' for org.sonatype.central:central-publishing-maven-plugin is missing`, once per examples module plus the aggregator, on every ordinary build"
  - "`Could not resolve dependencies ... parallel-consumer-core:jar:tests:0.6.0.0-SNAPSHOT` from a bare `mvn validate` on a clean machine"
  - GitHub's managed Automatic Dependency Submission workflow failed on every run it made and reported this project as the problem
  - A published snapshot is served with HTTP 200 and Maven still cannot resolve it
root_cause: config_error
resolution_type: config_change
tags:
  - maven
  - multi-module
  - reactor
  - plugin-versions
  - dependency-resolution
  - snapshots
  - enforcer
---

# Declared is not resolvable - four Maven reactor traps in this repo

## Problem

Four independent failures in this multi-module reactor turn out to be the same misunderstanding:
**something being declared is not the same as it being resolvable at the moment something asks for
it.** Three of the four announce themselves only as a warning or as somebody else's error message.

## Symptoms

- Every ordinary `mvn` run printed six malformed-model warnings, ending in Maven's *"these problems
  threaten the stability of your build"*.
- A bare `mvn validate` on a machine without the project's snapshots installed dies at the second
  module with `Could not resolve dependencies ... parallel-consumer-core:jar:tests`.
- GitHub's managed *Automatic Dependency Submission (Maven)* failed on every run it ever made - the
  same resolution error on `master` and on every branch, so repo-wide and not any one PR's doing - and
  reported it as a problem with this project. The whole history is one query, and worth running before
  believing any claim about which branches a managed workflow broke on:
  `gh api "repos/astubbs/parallel-consumer/actions/workflows/261174653/runs?per_page=100" --jq '.workflow_runs[] | "\(.head_branch) | \(.conclusion)"'`
- `parallel-consumer-core:0.6.0.0-SNAPSHOT` is served with HTTP 200 from
  `central.sonatype.com/repository/maven-snapshots`, and Maven still will not resolve it.

## What Didn't Work

- **Reading the warning as cosmetic.** Six identical warnings per build train you to skim them. The
  model really was malformed; Maven simply declines to fail on it.
- **Assuming the version was inherited.** It was declared - just inside a profile that was not active.
- **Treating the dependency-submission failure as our misconfiguration.** It is a limitation of the
  probe, and the probe cannot be edited: the managed workflow is not in `.github/workflows`.
- **Reaching for `<repositories>` to fix the snapshot resolution.** Considered and rejected - see
  below. It would have worked, and cost more than it fixed.

## Solution

### 1. A plugin version declared only inside a profile, consumed version-less by a child module

`central-publishing-maven-plugin` had its version in the root pom's `maven-central` profile.
`parallel-consumer-examples` declared the same plugin **version-less**, purely to set
`skipPublishing=true`. Outside that profile there was nothing to inherit, so the model built
malformed - and Maven degrades that to a `[WARNING]`.

Move the version to `pluginManagement`, so it resolves in every profile including none:

```xml
<pluginManagement>
    <plugins>
        <plugin>
            <groupId>org.sonatype.central</groupId>
            <artifactId>central-publishing-maven-plugin</artifactId>
            <version>${central-publishing-maven-plugin.version}</version>
        </plugin>
    </plugins>
</pluginManagement>
```

Fixed in astubbs/parallel-consumer#259, with `requirePluginVersions` added to the enforcer rules so
the warning class becomes a failure. The rule was confirmed to **fail on the unfixed tree** before
being accepted, so it guards the fix rather than rubber-stamping it.

**The trap needs both halves, which is why it is rarer than it looks.** Two other version-less plugin
declarations exist here and neither is reachable by it: `maven-compiler-plugin` in the
`intellij-idea-only` profile and `maven-enforcer-plugin` in the `ci` profile are both **overlays onto
a versioned declaration in the same pom**, so they merge and resolve. The bug needs the version to
live in a profile *and* the consumer to be a child module.

### 2. `validate` builds nothing, so a mojo needing a sibling's jar must go to the network

`ossindex-maven-plugin`'s audit mojo declares `requiresDependencyResolution=test`. Several modules
depend on a sibling's jar, including `parallel-consumer-core:jar:tests` - `parallel-consumer-vertx`
(second in the reactor, and so the first to fail), `parallel-consumer-mutiny`, and four example
modules. A phase that compiles nothing has produced no such jar in the reactor, so Maven falls back
to the remote repositories, and **the repository that has it is not declared for reading** - see trap
4. Note the failure is not test-jar-specific: the log fails on plain `parallel-consumer-core:jar` too.

`test-compile` is the working invocation:

```bash
./mvnw --batch-mode test-compile -Dossindex.skip=false -Dossindex.authId=ossindex
```

Two things that look like they should help and do not:

- **`<scope>runtime</scope>` in the plugin config** filters what gets *audited*, not what Maven must
  *resolve* to run the mojo. Resolution scope is a property of the mojo, set in its descriptor; the
  configuration parameter cannot narrow it.
- **`ossindex.authId` is a plugin user property**, so `-Dossindex.authId=ossindex` works whether or
  not the pom declares `<authId>` - useful in a checkout that has not got it, and no reason to edit a
  pom to run a scan.

The execution stays **bound to `validate`**, deliberately: the binding is where the mojo should fire,
and it is only the *invocation* that has to reach `test-compile`. Note what `test-compile` does and
does not do: it builds no jar - both jar goals run in `package` - but it does compile every earlier
module, and Maven's reactor then answers a sibling dependency from that module's compiled output
directory instead of going to the network. That is all the mojo's resolution needs. Moving the binding
would not fix anything and would change when the audit runs.

### 3. The same trap breaks a tool that then blames your project

GitHub's managed dependency submission probes with a bare `mvn validate` at the repository root. Same
phase, same reactor, same failure - and the workflow is **not editable in-repo**, so there is no place
to add a build step before the probe. This is a design limitation of the managed workflow for
multi-module projects, not a misconfiguration here; its own log points at the escape hatch, the
submission action you drive from your own workflow.

Rather than leave a permanently red check on every PR, the managed setting was **switched back off on
2026-08-11**, to be reconsidered after 0.6.0.0 ships. That is the resolution: not a fix, a decision -
and it is recorded with the condition for revisiting it, so it does not have to be rediscovered.

The generalisable half: **a tool's probe can be the broken thing while it reports your project as the
problem.** Before changing a project to satisfy an external checker, find out what command the checker
actually runs.

### 4. Publishing an artifact and being able to consume it are two separate configurations

Only one of them announces its absence. We *do* publish snapshots - `parallel-consumer-core:0.6.0.0-SNAPSHOT`
returns HTTP 200 from `central.sonatype.com/repository/maven-snapshots`, test-jar included - and the
root pom's `<repositories>` declares `central`, `confluent` and `astubbs-truth-generator`, and **not**
the Central snapshots repository. (`astubbs-truth-generator` does enable snapshots, but it is a
different host serving a different artifact, so it cannot answer for these.) Publishing succeeded
loudly; consuming was never configured, and nothing said so.

**Declaring it was considered and rejected**, and the reasoning matters more than the conclusion:

- it changes the whole project's dependency resolution to satisfy one auxiliary probe;
- it lets a partial build (`-pl` without `-am`) silently resolve a **stale published snapshot**
  instead of failing loudly - trading a clear error for a wrong build;
- and it commits every future `-SNAPSHOT` to being published *and* declared, in perpetuity.

Waiting for a release does not fix it either, for the same reason.

## Why This Works

All four collapse into one sentence: **the reactor makes "declared" and "resolvable" different
things, and both Maven and the tools around it assume they are the same.**

| Declared | Resolvable at the moment of asking |
|---|---|
| A plugin version, inside a profile | Only when that profile is active - a child module consuming it version-less gets nothing |
| A module, in the reactor | Only once a phase has actually *compiled* it, so the reactor can answer from its output directory; `validate` compiles nothing |
| An artifact, published to a repository | Only if that repository is also declared for *reading* |

Maven reports the first as a warning, the second as somebody else's error, and the third not at all.

## Prevention

- **`requirePluginVersions` in the enforcer rules.** It converts the entire warning class into a
  build failure, project-wide. Worth stating plainly: this is a policy change, not a point fix - any
  future version-less plugin declaration anywhere in the reactor now fails.
- **Put the invocation in the file, next to the thing that needs it.** The `test-compile`-not-`validate`
  requirement is recorded in a comment beside the ossindex plugin config in the root pom, and in the
  audit workflow's own run step, because that is where somebody about to get it wrong is looking.
- **Verify a guard by control arm.** Every rule above was checked against the unfixed tree first. See
  [`../../investigating.md`](../../investigating.md).
- **A version-predicated suppression needs a guard against that version moving.** A CVE excluded from
  the audit *because* a module resolves a version outside the affected range is only correct while
  that stays true - an automated bump into the range makes the finding real while the exclusion
  silently hides it. Pin the assumption where the bump would come from. Concretely (merged in
  astubbs/parallel-consumer#281): the exclusion holds only because `parallel-consumer-example-streams`
  imports `jackson-bom` 2.18.9, so a `dependabot.yml` ignore is keyed to
  `com.fasterxml.jackson:jackson-bom`. The pre-existing ignore was keyed to
  `com.fasterxml.jackson.core:jackson-databind` - a **different coordinate**, which did not cover the
  new pin. Match the coordinate you actually depend on, not the one you were thinking of.

## Related Issues

- astubbs/parallel-consumer#259 (merged) - `pluginManagement` fix and the `requirePluginVersions`
  guard.
- astubbs/parallel-consumer#279 (open) - the audit job that runs `test-compile` for the reason in
  trap 2; `docs/inflight/ci-ossindex-lane-reassessment.md` on branch `ci/ossindex-audit-job` records
  the dependency-submission analysis in full.
- astubbs/parallel-consumer#281 (merged) - the CVE triage and the `jackson-bom` dependabot ignore.
- [`../workflow-issues/a-check-that-reports-success-without-having-run.md`](../workflow-issues/a-check-that-reports-success-without-having-run.md)
  - traps 1 and 3 are also instances of that class: Maven's warning and the unnoticed failing
  workflow both reported nothing wrong while something was.
- [`../workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md`](../workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md)
  - trap 3 in its general form: the probe was broken, not the thing it was probing.
- [`maven-central-timeout-azure-west-regions-2026-04-21.md`](maven-central-timeout-azure-west-regions-2026-04-21.md)
  - the other resolution failure documented here, and a different cause entirely (network, not model).
