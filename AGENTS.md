# Parallel Consumer - Agent Context

Project context for AI coding agents (Claude Code, Copilot, Cursor, etc.).

## Where things live (read this before concluding something isn't tracked)

Documentation is split by *purpose*, and the split is enforced by convention rather than tooling - so
the commonest mistake is not misreading a doc, it is **never opening it**. Before you conclude that
some category of work is untracked, check this table. (Real example: a whole triage doc was once
written because only `docs/inflight/` was grepped, duplicating `docs/refactoring.md`, which had
owned that content all along.)

| Document | Owns | Explicitly NOT for |
|---|---|---|
| **`AGENTS.md`** (this file) | Conventions, build/test commands, and the rules agents must follow | Work items of any kind |
| **`docs/inflight/`** | *Transient* cross-branch state, **one file per item**, named `<category>-<slug>.md` (`bug-`, `test-`, `ci-`, `deps-`, `pr-`, `branch-`, `release-`, `parked-`, `next-`). Rules in [`docs/inflight/AGENTS.md`](docs/inflight/AGENTS.md) | A backlog. A file is deleted when its work lands - and **never** a committed index file, which every PR would edit |
| **`docs/refactoring.md`** | The deferred-work backlog: internal refactors grouped by file, **breaking changes queued for the next major** in their own release-gated section, and the **triage of `TODO`/`FIXME`/`XXX` markers** | In-flight work; anything already started |
| **`docs/TODO_INDEX.md`** | Generated inventory of every marker in the tree (`bin/todo-index.sh`, `--check` fails when stale) | Priorities - it is deliberately unsorted; triage goes in `refactoring.md` |
| **`docs/QUARANTINED_TESTS.md`** | CI-enforced registry of quarantined tests and their owning fix PR | Tests that merely flake - quarantine requires a diagnosis |
| **`docs/solutions/`** | Write-ups of problems already **solved**, by category, with frontmatter for searching | Open problems |
| **`docs/plans/`** | Dated plan and investigation documents for a specific piece of work | Durable reference - a plan goes stale once its work lands |
| **`docs/SELF_HOSTED_RUNNER.md`** | Setup and operation of the self-hosted highcpu runner | CI policy, which lives in the workflows |
| **`src/docs/development/upstream-map.yaml`** | **Source of truth** for fork↔upstream mapping: fork branch/PR → upstream **PR**, with status | Editorial opinion (that is the `.adoc` beside it), and **upstream issues** - those live in the fork mirrors, `upstream-mirror` label |
| **`src/docs/development/upstream-pr-analysis.adoc`** | Editorial analysis of upstream PRs: rankings, verdicts, merge order | Facts - when they disagree, the manifest wins |
| **`CHANGELOG.adoc`** | Release notes. Frozen up to `== 0.6.0.0`; later sections are generated at release time from the commit log. `README.adoc` links to it and no longer embeds it | Anything invisible to users or operators - and **not** a per-PR chore: do not add entries in a feature PR |

Rule of thumb: **is it happening now** → `docs/inflight/`; **should happen later** → `refactoring.md`;
**already happened** → `CHANGELOG.adoc` or `docs/solutions/`.

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
- **Check the ambient probe autopsy first when a broker IT fails.** Every broker integration test failure
  log includes an `=== AMBIENT PROBE AUTOPSY ===` block (grep for it) with rebalance-dwell / lag-stagnation
  violations and per-partition frozen-committed detail — it answers exactly the "contention artifact vs
  genuine bug" question above before you start manual diagnosis. `probe clean` means the fault is likely in
  the test itself, not consumer-group progress. Disable via `-Dambient.probe=off` or `@NoAmbientProbe` only
  when the probe itself is the problem (see `AmbientProbeExtension` javadoc).
- **Unit tests**: `mvn test` / surefire plugin. Source in `src/test/java/`.
- **Integration tests**: `mvn verify` / failsafe plugin. Source in `src/test-integration/java/`. Uses TestContainers with `confluentinc/cp-kafka` Docker image.
- **Test exclusion patterns**: `**/integrationTest*/**/*.java` and `**/*IT.java` are excluded from surefire, included in failsafe.
- **Kafka version matrix**: CI tests against multiple Kafka versions via `-Dkafka.version=X.Y.Z`.
- **Quarantine lane for known-failing-on-master tests (`@Quarantined`).** When a test is red on master's
  *gating* CI and its fix lives in another (open) PR, do NOT leave it red (ambiguous checks, error-prone
  merge decisions) and do NOT `@Disabled` it (loses the signal — a "known flake" can be a real product
  bug - see the drain-zombie write-up, `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md`, which lands with PR astubbs#80). Instead annotate it
  `@Quarantined(reason, tracking, fixedBy)` (in core's shared test sources): it leaves the gating suites
  (green means mergeable) but keeps running on every PR push and after every merge to master (workflow_dispatch on
  demand) in the non-gating "Quarantine Lane / tests" CI job, whose summary carries pass/fail + the audit of every
  quarantined test and its owner; the seconds-fast "Quarantine Audit" job enforces the rules on every
  PR (registry drift / broken owner claims fail fast - no tests are run there). The live registry
  / task list is `docs/QUARANTINED_TESTS.md` - CI-enforced (`bin/check-quarantine-registry.sh`) to match
  the annotations in both directions, so it can't drift; `bin/check-quarantine-owners.sh` additionally
  verifies each entry's owner claim (owning PR exists + is open + eventually removes the quarantine). Rules: **(1) no
  quarantine without diagnosis** — undiagnosed red stays red and blocks, on purpose; **(2) quarantine is
  master-state, not PR-state** — a test red on only one PR is that PR's problem; **(3) the owning fix PR
  deletes the annotation AND its registry entry in the same commit** after merging master, atomically
  restoring the test to the gating lane. Releases are blocked
  while the lane is non-empty (`release.yml` guard; snapshots still publish). Run
  the lane locally with `bin/quarantined-test.sh`.
- **Reuse test utilities — search before you add (DRY).** Shared client/broker helpers live in `KafkaClientUtils` (topic creation, producers, consumers, PC builders) and `BrokerIntegrationTest` (the base class most integration tests extend). Before writing a new helper or a raw `admin`/producer/consumer call in a test, search these two first and extend them. Duplicating an existing helper is how bugs get reintroduced — e.g. a copy of topic-creation logic drifted to a 1-second timeout and became a flaky-CI source (see `docs/solutions/test-issues/`). When you must add a helper, put it in the shared util, not the test. Also check `docs/solutions/` for prior art before solving a problem that feels familiar.

### Chaos Pain Suite (on-demand bug detector — never gates)

A seeded, calibrated chaos suite (`integrationTests.chaostests`: `ChaosConductor`, `ProgressProbe`,
`ChaosScenarioBase` + scenarios `ChaosChurnStormIT` W1, `ChaosRevokeUnderWorkIT` W4) that hunts the
"alive but not progressing" bug class: rebalance-dwell zombies, protocol-invisible per-partition lag
stagnation (Class 2, W4's prey), drain overruns, and record loss/duplication. Tagged
`@Tag("chaos")` and excluded from all default/gating suites via `pom.xml`'s `excluded.groups` default.

- **Run locally** (requires Docker; ~5-6 min):
  `./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups=`
- **Replay a schedule**: every run logs its seed and the full replay command; add `-Dchaos.seed=<seed>`.
- **CI**: per same-repo PR commit via the highcpu fast-feedback lane (check `highcpu / Chaos Pain
  Suite` - not optional: a chaos RED shows red); on-demand seeded hunts via
  `.github/workflows/chaos-pain.yml` (`workflow_dispatch`, inputs `seed`/`reps`), e.g.
  `gh workflow run chaos-pain.yml -f seed=42 -f reps=3`. Both call `bin/chaos-test.sh`. NB unlike the
  local recipe above, CI runs EXCLUDE `@Quarantined` chaos scenarios (the Quarantine Lane owns those) -
  while `ChaosChurnStormIT` is quarantined under PR astubbs#80 they therefore select zero tests, and the job
  summary flags that loudly.
- **Probe a fix PR** (the suite's primary purpose): on the fix PR's branch (merge master in first if
  the branch predates the suite landing there), run the suite at a commit before the fix (expect RED —
  the violation names the mechanism) and at the fix (expect GREEN). The local recipe above includes
  `@Quarantined` scenarios (`-Dexcluded.groups=` is empty), so known-RED detectors still fire locally.
  See `ChaosChurnStormIT`'s class javadoc for the full recipe.
- A RED run is investigation food, not flake noise — the probes are calibrated against the real historical drain-zombie defect (RED on pre-fix compositions, GREEN on fixed; thresholds sit in measured gaps). Never loosen a probe to go green; tune the workload/conductor instead.

## Known Issues

- **Mutiny module**: Has a `release.target=9` override in its pom.xml because Mutiny's `Multi` implements `java.util.concurrent.Flow.Publisher` which is not available with `--release 8`.

## Code Style

- **Lombok**: Used extensively (builders, getters, logging). IntelliJ Lombok plugin required.
- **EditorConfig**: Enforced via `.editorconfig` - 4-space indent for Java, 120 char line length.
- **License headers**: Enforced by `bin/check-copyright-headers.sh`, which also runs in the build
  itself (`validate` phase, via exec-maven-plugin), so a plain `mvn` catches violations - not only the
  `Copyright Headers` workflow. Skip it with `-Dcopyright.skip=true`.
  There is **no header-applying tool**: the scanner checks, it does not write. New files get their
  header written by hand, per the provenance rules below. The mycila `license-maven-plugin` used to
  fill that role and was removed - it knew only the Confluent header template, so its `format` goal
  stamped the wrong attribution onto fork-original files, and its git-year resolver auto-bumped years
  and broke in worktrees. `-Dlicense.skip` no longer exists as a property; drop it from any command
  you copy from an older doc or script.
- **Copyright rules for this fork**:
  - Do not change copyright headers on existing files unless the file has substantive code changes in the same commit
  - Do not bump copyright years as an incidental or standalone change
  - The `NOTICE` file at repo root contains the legal attribution structure for the fork
  - New files written entirely for the fork use `Copyright (C) <year> Antony Stubbs and contributors` -
    never the Confluent header
  - Upstream-derived files MODIFIED on the fork retain the Confluent notice and ADD
    `Modifications Copyright (C) <year> Antony Stubbs and contributors` beneath it (Apache 2.0
    4(b) retain-notices + 4(c) change-notice - the convention used by e.g. Amazon Corretto and
    MariaDB for derived files). The scanner detects modification against the fork point
    automatically, so forgetting the line fails CI
  - Files renamed or extracted from upstream keep the Confluent header - register renames in
    `RENAMED_FROM_UPSTREAM` (`newpath|oldpath` lines) and extractions in `EXTRACTED_FROM_UPSTREAM`
    inside `bin/check-copyright-headers.sh`. Renames with content changes, and all extractions,
    also require the modifications line
- **Google Truth**: Used for test assertions alongside JUnit 5 and Mockito.

## CI

**Reading a failed job's log.** `gh run view --log` refuses while *any* job in the run is still going
("logs will be available when it is complete"), and `--log-failed` is often empty for a Maven job,
because the failure text is ordinary stdout rather than an `::error::` annotation. Neither means the
log is unavailable. Fetch the job directly - this works as soon as **that job** finishes, regardless
of the rest of the run:

```bash
jid=$(gh run view <run-id> --json jobs --jq '.jobs[] | select(.name=="Integration Tests") | .databaseId')
gh api "repos/astubbs/parallel-consumer/actions/jobs/$jid/logs" > /tmp/job.log
```

Then grep it - `Tests run:`, `<<< FAILURE`, and for broker ITs the
`=== AMBIENT PROBE AUTOPSY ===` block, which classifies contention-vs-bug before you start reading
stack traces (see Testing).


- **`.github/workflows/maven.yml`** — Build and test on every push/PR. PRs run two tiers in parallel: (1) split suites on default Kafka 3.9.1 for fast feedback (`bin/ci-unit-test.sh`, `bin/ci-integration-test.sh`, `bin/performance-test.sh`), and (2) an experimental Kafka 4.x compatibility check (`bin/ci-build.sh`). A seconds-fast "Quarantine Audit" job enforces the quarantine registry on every PR; the `@Quarantined` lane itself runs non-gating on every PR push and every push to master (+ dispatch) in its own workflow (`quarantine-lane.yml`) — see Testing. Push to master runs a single full build on default Kafka version via `bin/ci-build.sh` to gate SNAPSHOT publishing. All jobs use explicit `cache/restore` with rotating keys from the `prepare-deps` job - never `setup-java cache: 'maven'`. Includes SpotBugs, duplicate detection, mutation testing (PIT), and dependency vulnerability scanning on PRs.
- **`.github/workflows/publish.yml`** — Publishes to Maven Central on every push to `master`. The pom.xml version is the source of truth: `-SNAPSHOT` versions deploy as snapshots, non-snapshot versions deploy as full releases (and create a git tag + GitHub release).
- **`.github/workflows/copyright.yml`** — Copyright-header conformance via `bin/check-copyright-headers.sh` (runs its self-test `bin/test-check-copyright-headers.sh` first, then the real scan) on every push/PR. GitHub-hosted; needs `fetch-depth: 0` so the fork-point commit is in history.
- **`.github/workflows/shell-hygiene.yml`** — static checks on the repo's own shell scripts, one job per concern (`shell: sigpipe` today). `bin/check-shell-sigpipe.sh` fails any `bin/*.sh` that pipes into `grep -q` under `pipefail`, which reports failure exactly when it *matches* once >64 KiB follows; `shellcheck` does not detect this. Its self-test runs first. Kept out of `copyright.yml` so neither workflow's name outlives its contents.
- **`.github/workflows/claude-code-review.yml`** — Automated PR review. The job ends with a gate,
  `bin/check-review-posted.sh` (self-tested by `bin/test-check-review-posted.sh`, which runs first),
  asserting that a review from *this* run actually landed on the PR. Without it the check reports
  success when the action reviews nothing, which is indistinguishable from "reviewed, no findings" -
  it has happened twice here. **The gate fails on any PR that edits `claude-code-review.yml` itself**:
  the action refuses to run unless that file matches the default branch, so a PR cannot rewrite its
  own reviewer. That is the guard working. Get a real review with a `@claude review this` PR comment
  (which runs from `claude.yml`, unmodified, so it validates), or split the workflow edit into its
  own PR. Do not disable the gate to get a green check.
- **`.github/workflows/repo-hygiene.yml`** — "Repo Hygiene", on every push/PR plus dispatch. Two independent jobs. `sigpipe` runs `bin/check-shell-sigpipe.sh` (its own self-test first) to catch a `bin/*.sh` piping into `grep -q` under `pipefail`, which silently inverts the script's answer. `actions` runs `bin/check-action-versions.sh`, keeping every GitHub Action pinned to a single version across all workflows. Neither gates the build - they exist because the failures they catch are invisible rather than loud.
- **`.github/workflows/check-dependencies.yml`** — "PR Dependency Check". Reads `depends on #N` lines from the PR body and blocks the child until every parent has merged. Produces the **required** check `Check PR Dependencies`, so a stacked PR cannot merge out of order. See [PR Discipline](#pr-discipline) for the syntax.
- **`.github/workflows/cancel-closed-pr-runs.yml`** — Cancels a PR's in-flight runs when it closes, so a withdrawn PR stops occupying runners. Housekeeping only; gates nothing.
- **Self-hosted lanes** (see [`docs/SELF_HOSTED_RUNNER.md`](docs/SELF_HOSTED_RUNNER.md)). None of these gate merging - they are for speed and for work too heavy for a 2-core hosted runner. All are **skipped for PRs from forks** (`head.repo.full_name == github.repository`), because a fork PR must never run on our own hardware.
  **`highcpu` is the only self-hosted label** - six runners, all online. Declare labels in [`.github/actionlint.yaml`](.github/actionlint.yaml) or actionlint flags them.

  - `pr-highcpu-fast-feedback.yml` ("highcpu") — on every in-repo PR plus dispatch. The lane that earns the hardware.
  - `mutation-full-sweep.yml` — dispatch only: the whole-project PIT sweep (`bin/ci-mutation-test.sh -Dverbose=true -Dthreads=N`). The PR-scoped mutation job in `maven.yml` only covers classes changed against the base; this is its exhaustive counterpart.

  **There is no scheduled build, deliberately.** Every suite worth re-running is already a required check on each PR and runs again on every push to master, so a cron lane would only repeat covered work. **Do not add a lane for suites the gate already covers.**

  **Before pinning a job to a self-hosted label, confirm a runner serves it** - `gh api repos/astubbs/parallel-consumer/actions/runners` lists each runner's labels and online status. A job pinned to a label nothing advertises does not fail; it queues until GitHub cancels it, so the lane reports nothing at all and looks merely quiet.

  **Beware: `performance` names two unrelated things.** It is the *test suite* (`bin/performance-test.sh`, the required **Performance Tests** check, on every PR from `maven.yml`, `ubuntu-latest`). It is **not** a runner label - the only self-hosted label is `highcpu`.
- **`.semaphore/`** — Legacy Confluent internal CI/release pipelines, retained but inactive on the fork.

## Changelog

`CHANGELOG.adoc` holds the release notes. **Nothing about it is a per-PR chore.** Do not add entries
in a feature PR, and do not maintain an `== Unreleased` section. Everything up to and including
`== 0.6.0.0` is hand-written and now frozen; from the next release on, each section is **generated at
release time from the commit log** and then frozen in turn.

This removes the file that every PR used to touch - it appeared in 30 of the last 30 master commits,
dragging the generated `README.adoc` with it - and removes the ordering problem where an entry had to
cite a PR number that did not exist when the entry was written.

### What this asks of a commit

Nothing extra. The commit log is the raw material, so write it as you already should: a subject that
says what changed and, where it matters to a user, what it changed *for them*; the diagnosis, the
experiment and the rejected alternatives in the body. A good commit message is now doing double duty,
which is a reason to keep writing them properly rather than a new process.

### At release time

An agent reads `git log <last-tag>..HEAD` - full messages, not just subjects - and drafts the release
section. The judgement it applies, and that a human should re-apply before freezing:

- **The entry test.** Can a *user or operator* observe this without reading our repo - API, behaviour,
  performance, logs and metrics, or the published artifact? If not, it gets no entry. Most CI,
  tooling, refactor and docs commits produce nothing, and that is correct: the changelog answers one
  question, "should I upgrade, and will anything change for me?"
- **One sentence, about 25 words, then the link.** Name what a reader would have *seen*, and who it
  hits when that is not everyone - not how the bug worked. An entry that runs to a paragraph is
  written for its author; an entry too short to tell you whether you are affected (`fix: Paused
  consumption across multiple consumers`) is no better.
- **Assemble as a set, not one commit at a time.** Merge related commits into a single entry, drop
  what turned out not to matter, and rewrite for someone who was not there. This is the part a per-PR
  entry could never do.
- **One `=== Build & CI` entry for the whole release** - a short bullet list of the big hitters
  (quarantine lane, chaos suite, mutation testing) that tells a reader how carefully the library is
  tested, with the detail left to the log.
- **Sections:** `=== Breaking`, `=== Improvements`, `=== Fixes`, `=== Dependencies`, `=== Examples`,
  `=== Build & CI`. **Reference convention:** a bare `#NN` is this fork, `upstream #NN` is
  confluentinc; make issue links explicit (`.../issues/NN[#NN]`), since GitHub numbers issues and PRs
  from one sequence.

**Still live, and now inert:** the `PR Checklist` gate's changelog rule
(`.github/scripts/changelog-ref-gate.js`) fails a human PR that *adds* a `CHANGELOG.adoc` entry citing
no issue. Since PRs no longer touch the file, it should never fire - it remains a guard for anyone
hand-editing the frozen sections.

## Issue references

Every upstream issue now has a **fork mirror** (astubbs#44, astubbs#117-astubbs#195, label `upstream-mirror`), so a
reference has a fork-local number a reader can click. Find one with
`gh issue list -R astubbs/parallel-consumer --label upstream-mirror --search "upstream #NNN"`.

**The convention: below #1000, say which repo you mean.** `astubbs#119` for this fork,
`confluentinc#857` for the original - or a hyperlink, which qualifies it just as well. Add `PR` or
`issue` where the distinction matters (`confluentinc PR #548`); both forms pass the gate.

**Name the owner, not the role.** `confluentinc#857`, not `upstream #857` - "upstream" describes a
relationship rather than a repository, and it is not stable: this fork is itself upstream to anyone
who forks it. `upstream #NN` still passes the gate so older text is not broken, but new writing uses
the owner. Same reasoning that rules out "fork" as a qualifier. In anything
**posted to GitHub**, use the fully qualified `confluentinc/parallel-consumer#857`: upstream prose
does not auto-link there, and a bare `#NN` in a comment silently resolves against whichever repo it
is posted in.

At or above #1000 a bare number is unambiguous, because only this fork can have one.
That holds only while confluentinc's numbering stays below 1000 - it is dormant rather than
archived, so it still creeps. Measure the headroom rather than trusting a figure written here:
`gh api 'repos/confluentinc/parallel-consumer/issues?state=all&per_page=1&sort=created&direction=desc' --jq '.[0].number'` -
`state=all` matters, since the highest number is usually a merged PR.
`upstream-sweep.sh` warns when it thins.

**Why not "a bare `#NN` is this fork", which is what this used to say:** the fork's numbers sit
*entirely inside* upstream's range, so a bare number is a coin flip. Of the 51 numbers cited across
one PR's files, **48 existed in both repos meaning different things** - `#29` is this fork's
rebalance fix and upstream's async-sending request; `#114` is a docs PR here and a GPG-key issue
there. "fork" is not used as the qualifier either: this repo *is* a fork, so the word names nothing.

**Cite both numbers, fork first.** `(astubbs#119, confluentinc#857)`. The fork number is what `Fixes` acts on
and what a reader of this repo can open; the upstream number is what four months of commits, branch
names (`bugs/857-...`) and the upstream threads all use. Dropping either breaks a trail.

**Fix references in any file a PR touches.** Not a bulk rewrite - opportunistic, as files are
touched. In a file being changed anyway:

- every unqualified `#NNN` below 1000 gains its repo - `astubbs#NNN` or `upstream #NNN` -
  **hyperlinked** where the format allows (markdown link, javadoc `<a href>`; a raw URL in a `//`
  comment, which every IDE linkifies)
- resolve the number in **both** repos before choosing the prefix; it very likely exists in each
- add the fork mirror number alongside an upstream one where a mirror exists
- the tree-wide remainder is tracked in [`docs/inflight/next-qualify-remaining-refs.md`](docs/inflight/next-qualify-remaining-refs.md)

**PR titles carry both**, e.g. `fix(core) astubbs#119: paused consumption after rebalance (confluentinc#857)`. The
title becomes the squash commit subject, so it is the reference most people will ever see.

**A CI gate enforces this.** The `PR Checklist` job fails a PR whose *added* lines contain an
unqualified `#NN` below 1000. The check is purely textual - no API calls, so it cannot race issue
creation. An earlier version instead asked "does this number resolve here?", and so passed `#200` - a
real fork issue about ManagedTruth - while the author meant confluentinc#200, the shared-nothing
architecture. **A wrong reference that resolves is worse than a broken one**, because nothing looks
amiss.

What it checks is that a reference *names* a repo, not that it names the right one: `astubbs#857`
passes the gate and is still wrong. Resolve the number in both repos before you write it.

The files listed in `EXEMPT_PATHS` are exempt, because a bare number legitimately means upstream in them: `CHANGELOG.adoc`,
`upstream-map.yaml`, `upstream-pr-analysis.adoc`, and the gate's own test fixtures. If a flagged
reference really is fork-local, put `issue-refs: N/A - <reason>` on its own line in the PR body.
Logic and tests live in `.github/scripts/issue-ref-gate.js` and `issue-ref-gate.test.js`.

**`Fixes #NNN` only closes on PRs targeting the default branch.** Discovered on astubbs#29, which targeted
`master-confluent`: the keyword was in the body and GitHub ignored it entirely. Check
`gh pr view N --json closingIssuesReferences` rather than assuming. And never use `Fixes` for a
*partial* fix - see the mirrors for confluentinc#233, confluentinc#326 and confluentinc#857, none of which their linked PRs
actually resolve.

## Commits

**`.gitmessage` is the template for everything below.** Turn on the editor prompt once per checkout - `git config commit.template .gitmessage` - and it walks you through the subject format and the trailers. It is not wired up automatically, and nothing lints commit messages, so all of this is on you.

- **Subject is `type(scope) #NNN: subject`, and the trailing `(#N)` slot belongs to the PR number - never put an issue there.** GitHub appends `(#123)` to the subject on squash-merge, so a title ending `... (#41)` merges as `... (#41) (#123)`: two bare numbers, and no way to tell the issue from the PR. Cite the issue at the FRONT instead, as `type(scope) #41: subject` - which merges cleanly to `docs(mutation) #41: subject (#123)`, and matches both Apache Kafka (`KAFKA-14561: ... (#13114)`) and this repo's own pre-fork history (`GH-725: ... (#727)`). For an *upstream* issue, word it - `fix(core) confluentinc#909: subject` - because a bare `#909` autolinks to *fork* issue 909, which is not the one you meant. Prefer the fork mirror's own number when one exists (`#119` mirrors confluentinc#857), and put the upstream number in the [commit trailers](#commit-trailers), where a tool can read it. **The same rule governs PR titles**, because on a squash-merge the merged subject *is* the PR title.
- **`(scope)` is optional and only earns its place when it narrows things usefully** - `(core)`, `(producer)`, `(changelog)`, `(mutation)`. A directory name is not a scope. Plain `docs #208: ...` beats a scope that adds nothing.

See also [What this asks of a commit](#what-this-asks-of-a-commit) under Changelog, for what the body has to carry so release notes can be generated from the log.

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

Keep the existing subject convention for *upstream* references (`... (#893)`, `cherry-pick Confluent #905`), governed by the subject rule above. **Trailers are not enforced** — they only fit upstream-related commits, not fork-only work (rebrand, release, dependabot, formatting). Use judgement.

## PR Discipline

- **Closing something as superseded: link both directions, and link a durable anchor.** Name the
  successor from the closed PR *and* the predecessor from the successor - a reader arrives from
  whichever side they happen to know about, and the one-way link strands the other half. If the
  successor does not exist yet, cite the tracking issue rather than a branch: a branch name is not a
  link, tells you nothing about whether the work landed, and nobody comes back to upgrade it once the
  PR opens. The issue exists at both moments; the branch is only meaningful at one. Real example:
  astubbs#30 said "will land as a fresh PR" in July and was still saying it in August, while
  astubbs#57 - the PR in question - never mentioned its predecessor at all, so the earlier round of
  review was invisible from the work that carried it.
- **Keep the PR title and body in sync with what the PR actually covers.** As a PR grows, its description drifts - re-check it before requesting review and before merge. Update it only on *material* drift: whole changes/workstreams missing, wrong specifics (core counts, flags, forkCounts, file/label names), or scope that has outgrown the title. Do NOT churn the description for cosmetic wording - if it still accurately reflects the content, leave it.
- **Open PRs from the template and complete its checklist honestly.** `.github/PULL_REQUEST_TEMPLATE.md` is NOT auto-applied when a PR is created non-interactively (e.g. `gh pr create --body-file`), so base the PR body on it and resolve every box: check it `[x]`, or mark it `N/A - <reason>`. For human-authored PRs the `PR Checklist` CI gate (`.github/workflows/pr-checklist.yml`) fails when the checklist is missing entirely *or* when any box is left unchecked without an `N/A` - so dropping the template is not a bypass. Only real bot authors (GitHub user type `Bot`, e.g. Dependabot/Renovate) are exempt.
- **Respond to review comments IN-THREAD and resolve the thread when addressed.** Reply to the specific review comment (its own thread), NOT as a separate top-level PR comment - a summary comment leaves the original conversation unresolved and blocks merge on "unresolved conversations." When a finding is fixed, reply in-thread with the fix + commit SHA and mark the thread resolved (`gh api graphql ... resolveReviewThread`). Leave a thread open only when it genuinely needs the author's decision, and say so in the reply.
- **After opening a PR, follow up on the duplication reports.** The duplicate-code and file-similarity checks post comments flagging new clones/similarity. Read them, remove duplication introduced by *this* PR before it merges; ignore clones that already existed on the base branch (out of scope for this PR).
- **Stacked PRs: put `depends on #N` in the description** (one line per parent). The PR-dependency gate blocks the child from merging until the parent does; keep the list current if the chain changes.

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
   to `master`** via `RELEASE_PAT`), refuses if master's latest *CI* workflow run isn't green, then checks
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
- The higher-level map of what each branch/worktree is for lives in `docs/inflight/` (the `branch-` and `pr-` files).

## Documented Solutions

`docs/solutions/` - documented solutions to past problems and workflow patterns, organized by category with YAML frontmatter (`module`, `tags`, `problem_type`). Relevant when implementing or debugging in documented areas.

## Refactoring backlog

Deferred internal refactors (too big/risky to fold into the change at hand) live in [`docs/refactoring.md`](docs/refactoring.md) - a versioned markdown list, grouped by file, **not** GitHub issues (overkill for a solo maintainer). When you notice one, drop a `// TODO(refactor): <one line>` marker at the spot (`grep -rn "TODO(refactor)" --include=*.java` lists them) and, if it warrants context, add an entry to the doc. **`docs/refactoring.md` also owns the triage of plain `TODO`/`FIXME`/`XXX` markers** - there are ~90 of those versus a handful using the `TODO(refactor):` convention, and they are inventoried in the generated [`docs/TODO_INDEX.md`](docs/TODO_INDEX.md) (`bin/todo-index.sh`, `--check` fails when stale). It already covers the breaking-change queue, static-state removal, offset-encoder cleanups and per-file backlogs - so write triage up here, and **do not start a parallel list** (see *Where things live* at the top). Promote an item to a branch/PR only when you actually start it; if it maps to an upstream issue, link it rather than duplicate. The doc also tracks **breaking changes queued for the next major version** in a separate, release-gated section, kept apart from the non-breaking internal refactors (those are batched for a major bump, not folded in ad hoc). This is distinct from `docs/inflight/` (in-flight), `upstream-map.yaml` (fork↔upstream), and PR review feedback (raise on the PR).

## Upstream tracking

This is a maintained hard fork of the effectively-archived `confluentinc/parallel-consumer`. We keep a durable, machine-readable cache of the fork↔upstream relationship so it never has to be re-derived from scratch:

- **`src/docs/development/upstream-map.yaml`** — the **source of truth** for the *facts*: which fork branch/PR maps to which upstream issue/PR, its work group, and current status. Its header documents the schema. Validate/render with `scripts/upstream-map.py {validate,table,refs}`.
- **`src/docs/development/upstream-pr-analysis.adoc`** — the *editorial* analysis (rankings, verdicts, recommended merge order). When prose and manifest disagree, **the manifest wins for facts**. Manifest entries link back to `.adoc` section anchors via `adoc_anchor`.
- **`docs/inflight/`** — *transient* cross-branch working notes only, one file per item.

**When you start work that maps to an upstream PR, add or update its entry in `upstream-map.yaml`** (don't just note it in prose). Design follows Debian DEP-3, Yocto `Upstream-Status:`, and OpenShift's `UPSTREAM:` fork conventions.

**If the work maps to an upstream *issue*, the fork mirror is where status goes** - diagnosis, labels, and closing all belong on the mirror, because this manifest tracks upstream **PRs** only (every upstream issue has one: astubbs#44, astubbs#117-astubbs#195, label `upstream-mirror`). Find it with `gh issue list -R astubbs/parallel-consumer --label upstream-mirror --search "upstream #NNN"`, and cite both numbers, fork first - see [Issue references](#issue-references).

**Keeping it in sync is the agent's job, and it does not stop at "start work".** Nothing automated checks the *fork* side: `upstream-map.py validate` only checks the schema, and `upstream-sweep.sh` only watches upstream — so a manifest that says `prs: []` while a fork PR is open still passes every check, and the mapping quietly rots (a 2026-08-04 audit found five such entries). Update the entry **at every lifecycle transition of your own work**, in the same commit that causes it: opening a PR (`prs:` + `status: pr-open`), finishing on a branch without a PR (`status: ready`), merging (`merged`), releasing (`released`), abandoning (`superseded`/`wontfix`). Loose ends do **not** go in this manifest - it has no `todo:` field. Anything a command can answer ("how far behind is PR #N?" - `git rev-list --left-right --count`) should be asked of the command rather than cached here, where it rots. Record what no command knows in `docs/inflight/`; keep this manifest to the mapping itself.

**The `upstream:` half needs the same discipline, and nothing else maintains it.** `upstream-sweep.sh` reports drift; it never writes. When it flags an entry, correct `upstream.status` and bump `last_checked` in the same pass - an item left unfixed is re-reported every run, which teaches you to skim past the report, and then it stops working at all. Verify against GitHub rather than the entry: `confluentinc#548` sat recorded `open` while it had been merged since 2023, and a header note in the same file asserted a third answer.

Branch naming and commit trailers are git conventions rather than upstream-mapping facts, so they live under [Commits](#commits).

### Backlinking upstream

**Done, and there is no tooling for it.** All 78 open upstream issues are mirrored here (astubbs#44,
astubbs#117-astubbs#195, label `upstream-mirror`) and every one carries a backlink comment pointing at its mirror.
Seven that are fixed in a *released* version carry a second comment naming the version. This is a
finished, one-off job, not a recurring chore - `upstream-map.yaml` tracks upstream **PRs** only,
since issues live in the mirror.

**If you need to comment upstream again**, do it directly with `gh`, and:

- Put a hidden marker in the body (`<!-- pc-mirror:issue-NNN -->` or similar) and check for it before
  posting. That is the idempotency record - not a field in a manifest, which goes stale the moment
  anyone comments and, when the field was removed, silently failed *open*.
- Use **plain cross-repo references, never `Fixes`/`Closes`** - they do not auto-close cross-repo and
  we are not closing anyone's issue.
- Comment a **second** time only when there is something to act on: a fix in a published version, or
  a question answered. One meaningful notification beats a stream of empty ones.
- Fully qualify references in anything posted to GitHub - `confluentinc/parallel-consumer#NN` - since
  `upstream #NN` does not auto-link and a bare `#NN` resolves against whichever repo it lands in.

The reasoning, and what the bulk run taught, is in
[`docs/plans/2026-08-04-001-chore-mirror-upstream-issues-plan.md`](docs/plans/2026-08-04-001-chore-mirror-upstream-issues-plan.md).

### Checking upstream for new activity

`scripts/upstream-sweep.sh` (read-only) lists upstream issues/PRs with activity since the manifest's `last_swept` and flags drift on tracked refs (recorded `open` but now closed/merged upstream). `--since <date>` overrides the window; `--publish` updates a single fork tracking issue (guarded, never spams). Run it periodically to catch new reports from users who don't know the fork exists - that is its whole purpose, since upstream is otherwise static.

**It ignores our own comments, and must.** Every mirrored issue carries a backlink comment from us, and posting one bumps that issue's `updated` timestamp - so a plain `updated:>=` search returns all 78 and the signal disappears entirely. An item is reported only when it was opened inside the window, or someone who is not us commented inside it. Keep that filter if you touch the script; without it the report is the tracker read back to you.

`last_swept` lives at the top of `upstream-map.yaml`. Bump it after acting on a sweep, or every run re-reports the same items.
