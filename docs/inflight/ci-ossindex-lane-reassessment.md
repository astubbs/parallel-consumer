# OSS Index lane - does it still earn its keep?

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->


The `deps: whole-tree CVE scan` lane (added by astubbs/parallel-consumer#279) was justified on Dependabot
missing **transitive** components. That premise has partly expired, so the lane needs one deliberate
re-look rather than an indefinite assumption.

GitHub has shipped automatic Maven transitive dependency submission since
[2024-07-29](https://github.blog/changelog/2024-07-29-automatically-submit-your-maven-transitive-dependencies-to-the-dependency-graph/)
/ [2025-03-26](https://github.blog/changelog/2025-03-26-transitive-dependencies-are-now-available-for-maven/).
It was simply never switched on here. **Enabled 2026-08-11.**

## What was measured before the flip

Measured, not reasoned from the docs, via
`gh api repos/astubbs/parallel-consumer/dependency-graph/sbom`:

| Component | In the dependency graph | Dependabot alerted |
|---|---|---|
| `jackson-databind` | yes | yes |
| `micrometer-core` | yes | yes |
| `lz4-java` | **no** | no |
| `HdrHistogram` | **no** | no |
| `netty-codec-http` | **no** | no |

The overlap is exact: the graph held only what the poms declare, and the three "Dependabot blind
spots" are precisely the components absent from it. The transitive-closure gap was real. Note the
consequence was smaller than first claimed, though - only `lz4-java` had an advisory to alert on.

**Baseline at the moment of the flip**, so the re-measurement means something: 121 packages in the
graph, zero occurrences of `lz4-java` / `HdrHistogram` / `netty-codec-http`, 6 open Dependabot
alerts. Automatic submission runs on a push to the default branch, so it had not repopulated yet.

## What dependency submission actually is

Worth stating, because the name suggests more than it does and the question came up twice.

The dependency graph normally builds itself by **statically parsing `pom.xml`** - so it holds what
you *declare*, not what Maven *resolves*. That is the whole reason the transitive components are
missing from it, and therefore why Dependabot never alerted on `lz4-java` despite
`GHSA-xx22-p4ch-683r` existing: **Dependabot alerts read from the graph**, and the graph never had
the package.

Dependency submission closes that by running Maven and POSTing the *resolved* dependency list -
coordinates and versions - to the repository's own graph via GitHub's Dependency Submission API. It
does **not** send source anywhere and involves no code scanning; the payload is a list of library
coordinates the poms already imply, just resolved rather than declared.

Two ways to drive it, and they submit to the **same API and the same graph** - the only difference
is who runs Maven:

| | Managed (the repo setting) | Repo-owned workflow |
|---|---|---|
| Maven invocation | GitHub's fixed bare `mvn validate` probe | ours |
| Editable | no - the workflow is not in `.github/workflows` | yes |
| Submits to | Dependency Submission API | the same API |

So a repo-owned workflow buys exactly one thing: the ability to insert a build step before
submitting. The action is not smarter - **the same `validate`-builds-nothing trap bites it too** if
it is pointed at the repo root without building the reactor first.

## Blocker: automatic submission is currently failing

**Fix this first, or the trigger below silently never fires.** Since the flip, GitHub's managed
`Automatic Dependency Submission (Maven)` workflow (`submit-maven`) has failed on every run - the
same failure on `ci/ossindex-audit-job`, `docs/agents-gh-base-repo` and `build/enforce-plugin-versions`
alike, so it is repo-wide and not any one PR's doing.

Its `validate-project` step cannot resolve the reactor's own modules:

```
Failed to execute goal on project parallel-consumer-vertx: Could not resolve dependencies
  bz.stub.parallelconsumer:parallel-consumer-core:jar:0.6.0.0-SNAPSHOT       -> not in central
  bz.stub.parallelconsumer:parallel-consumer-core:jar:tests:0.6.0.0-SNAPSHOT -> not in central
```

This is the failure mode `.github/workflows/dependency-audit.yml` already documents for its own
Maven step: a bare `validate` on a clean runner has no `parallel-consumer-core` (or its test-jar)
installed, so the second module dies. That job works around it by running `test-compile`; the
managed workflow has no such step and cannot be edited in-repo, so it needs configuring (or
replacing with a repo-owned submission workflow that builds the reactor first).

**Until it succeeds at least once on `master`, the dependency graph will not repopulate**, and the
re-measurement below would read as "no change" for the wrong reason.

### Turned back OFF on 2026-08-11 - and it stays off

Rather than leave a permanently red check on every PR, the *Automatic dependency submission* setting
was switched off again. **Do not simply switch it back on**, at the v6 release or any other point:
the root cause below is a property of the managed workflow, not of anything a release changes, so
re-enabling it restores a permanently failing check and nothing else. The first draft of this note
did say "re-enable after 0.6.0.0 ships", which was written before the root cause was understood and
is contradicted by the next two paragraphs; the trigger at the bottom is the version that survived.

Root cause, and it is neither "we have never published" nor anything about repositories: **GitHub's
managed submission workflow probes with a bare `mvn validate` at the repo root.** `validate` builds
no artifacts, so any reactor module depending on a sibling's **jar** cannot resolve it - from the
reactor (nothing built) or remotely. It is a design limitation of the managed workflow for
multi-module projects, not a misconfiguration here, and the job's own log points at the escape
hatch: "for submitting Maven dependencies from your own workflow, refer to the submission action".

Snapshots are a red herring, recorded so nobody re-runs this reasoning. We *do* publish them -
`parallel-consumer-core:0.6.0.0-SNAPSHOT` is served with HTTP 200 from
`central.sonatype.com/repository/maven-snapshots` - and the root pom does not declare that
repository. **Do not fix it by declaring one.** That would change the whole project's dependency
resolution to satisfy one auxiliary probe, let a partial build (`-pl` without `-am`) silently
resolve a stale published snapshot instead of failing loudly, and still require every future
`-SNAPSHOT` to be published *and* declared in perpetuity. Waiting for the v6 release does not fix it
either, for the same reason.

**Current decision: leave the managed submission OFF.** The two real options, if it is ever wanted:

1. A repo-owned workflow running `advanced-security/maven-dependency-submission-action`, where the
   Maven invocation is ours and can build the reactor first (`mvn install -DskipTests`, or
   `test-compile` as `dependency-audit.yml` does for exactly this reason).
2. Nothing - because the coverage gap it was meant to close is the transitive one, and the OSS Index
   lane in astubbs/parallel-consumer#279 already covers transitive components. That is the argument
   *for* keeping the lane, and it is stronger now than when this note was written: the obvious
   managed alternative does not work on a project of this shape.

## The trigger

**If a repo-owned submission workflow is ever added and succeeds on `master`**, re-run the SBOM query and check two things:

1. do `lz4-java`, `HdrHistogram` and `netty-codec-http` now appear in the graph;
2. does a Dependabot alert fire for `lz4-java` (GHSA-xx22-p4ch-683r exists for it).

## What the answer would mean

If the graph picks up the full closure and Dependabot starts covering it, the **transitive-coverage**
justification for this lane is gone, and only two remain:

- **proving the scan actually ran** - Dependabot has no equivalent, and it is the entire reason
  `bin/check-ossindex-audit.sh` exists;
- **a whole-tree scheduled scan that can fail a build.**

That is a genuine question to ask at that point, not a foregone conclusion either way. The honest
answer might be that the lane collapses to just the scheduled scan.

Weigh in the maintenance cost the dependency graph does not carry: the exclusion list in
astubbs/parallel-consumer#281 has to be kept honest as advisories are published, corrected and
retired, and OSS Index has already produced two false positives and one phantom CVE id in a single
scan.

**Decision (2026-08-11): keep the scanner.** Nothing was removed or weakened; this note exists so the
reassessment actually happens instead of being rediscovered.
