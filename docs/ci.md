# CI: workflows, lanes, and reading a failed run

What each workflow does, which ones gate merging, and how to get at a failed job's log. Read this
when CI goes red or when you are adding or changing a workflow. The rules that apply to every
agent regardless of CI live in AGENTS.md; this is the reference behind them.

## Reading a failed job's log

The `--log` flag on `gh run view` refuses while *any* job in the run is still going ("logs will be
available when it is complete"), and `--log-failed` is often empty for a Maven job, because the
failure text is ordinary stdout rather than an `::error::` annotation. Neither means the log is
unavailable. Fetch the job directly - this works as soon as **that job** finishes, regardless of the
rest of the run:

```bash
jid=$(gh run view <run-id> -R astubbs/parallel-consumer --json jobs --jq '.jobs[] | select(.name=="Integration Tests") | .databaseId')
gh api "repos/astubbs/parallel-consumer/actions/jobs/$jid/logs" > /tmp/job.log
```

Then grep it: `Tests run:`, `<<< FAILURE`, and for broker integration tests the
`=== AMBIENT PROBE AUTOPSY ===` block, which classifies contention-vs-bug before you start reading
stack traces (see [`docs/testing.md`](testing.md)).

## Workflows

- **`maven.yml`** - build and test on every push/PR. PRs run two tiers in parallel: split suites on
  the pom's default Kafka version (`bin/ci-unit-test.sh`, `bin/ci-integration-test.sh`,
  `bin/performance-test.sh`) for fast feedback, and an experimental Kafka 4.x compatibility check
  (`bin/ci-build.sh`). Also carries the seconds-fast Quarantine Audit job, SpotBugs, duplicate
  detection, PR-scoped mutation testing (PIT), and dependency vulnerability scanning. Push to
  master runs a single full `bin/ci-build.sh` on the default Kafka version to gate SNAPSHOT
  publishing. All jobs use explicit `cache/restore` with rotating keys from the `prepare-deps`
  job - never `setup-java cache: 'maven'`.
- **`publish.yml`** - publishes to Maven Central on every push to `master`. The pom version is the
  source of truth: `-SNAPSHOT` versions deploy as snapshots, non-snapshot versions deploy as full
  releases (and create a git tag + GitHub release). See [`docs/releasing.md`](releasing.md).
- **`copyright.yml`** - header conformance via `bin/check-copyright-headers.sh` (its self-test
  `bin/test-check-copyright-headers.sh` runs first, then the real scan) on every push/PR.
  GitHub-hosted; needs `fetch-depth: 0` so the fork-point commit is in history. Rules:
  [`docs/copyright.md`](copyright.md).
- **`quarantine-lane.yml`** - runs the `@Quarantined` tests on every PR push, every push to master,
  and on dispatch. Its job is the **required** check `tests`, so the job name is an API here too -
  but the test-running step is `continue-on-error`, so red quarantined tests cannot block a merge.
  See [`docs/testing.md`](testing.md).
- **`pr-checklist.yml`** - hosts the PR-body gates: the template checklist (rule in AGENTS.md, PR
  Discipline), the changelog-citation gate (`changelog-ref-gate.js`, see
  [`docs/releasing.md`](releasing.md)) and the issue-reference gate (`issue-ref-gate.js`, see
  [`docs/issue-references.md`](issue-references.md)).
- **`check-dependencies.yml`** - "PR Dependency Check". Reads `depends on
  astubbs/parallel-consumer#N` lines from the PR body and blocks the child until every parent has
  merged. Produces the **required** check `Check PR Dependencies`, so a stacked PR cannot merge out
  of order.
- **`repo-hygiene.yml`** - cheap repo-wide static checks needing no JDK, no broker and no build.
  **One job per concern**, named `<area>: <check>` to match the master ruleset's context
  convention - a job renamed here silently stops satisfying that ruleset, so treat the names as an
  API. `sigpipe` runs `bin/check-shell-sigpipe.sh`, which fails any `bin/*.sh` piping into
  `grep -q` under `pipefail` - that construct reports failure exactly when it *matches*, and
  shellcheck does not detect it. `actions` runs `bin/check-action-versions.sh`, keeping every
  GitHub Action pinned to one version across all workflows. Self-tests run first. **Both are
  required status checks** (`shell: sigpipe`, `workflows: action versions`) - which is exactly why
  the job names are an API. They exist because the failures they catch are invisible rather than
  loud, and they gate precisely so those failures cannot be skimmed past.
  - `cve-exclusions` runs `bin/check-cve-exclusions.sh`, which **expires temporary CVE
    exclusions**. Entries in the root pom's `excludeVulnerabilityIds` come in two kinds: *standing*
    (retiring them needs someone else to act, on no timetable we control) and *temporary* (the
    upstream fix is merged, and the entry exists only because no release carries it yet). A
    temporary entry is marked `TEMPORARY-SINCE: YYYY-MM-DD` in the comment above it, and the check
    fails once it is more than **90 days** old - one quarter, anchored on jackson-databind 2.18.x's
    observed patch cadence (median gap ~65 days), since the upstream patch line, not our own
    release cycle, is what actually retires these entries. Undated, unparseable and future-dated
    markers **fail** rather than reading as "not expired yet", and so does an id with no rationale
    comment, because an unclassified entry silently means standing-forever. **Exit 3**, deliberately
    not 1 or 2: those belong to `bin/check-ossindex-audit.sh` (broken lane / real finding) and this
    is neither - the scan and the tree are fine, the bookkeeping has rotted. The red is always
    clearable in one reviewable line: retire the entry if the fix shipped, or re-date / reclassify
    it having re-checked upstream. It lives here rather than in the audit job because that job is
    skipped for fork PRs and dies early on a token expiry, so the list would go unwatched exactly
    when it matters most. **`deps: CVE exclusion expiry` is a new job name and is NOT yet a required
    status check** - adding it to the master ruleset is a separate, deliberate act.
- **`claude-code-review.yml`** - automated PR review. The job ends with a gate,
  `bin/check-review-posted.sh` (self-tested by `bin/test-check-review-posted.sh`, which runs
  first), asserting that a review from *this* run actually landed on the PR. Without it the check
  reports success when the action reviews nothing, which is indistinguishable from "reviewed, no
  findings" - it has happened twice here. **The gate fails on any PR that edits
  `claude-code-review.yml` itself**: the action refuses to run unless that file matches the default
  branch, so a PR cannot rewrite its own reviewer. That is the guard working. Get a real review
  with a `@claude review this` PR comment (which runs from `claude.yml`, unmodified, so it
  validates), or split the workflow edit into its own PR. **Do not disable the gate to get a green
  check.**
- **`chaos-pain.yml`** - on-demand seeded chaos hunts (`workflow_dispatch`, inputs `seed`/`reps`).
  See [`docs/testing.md`](testing.md).
- **`cancel-closed-pr-runs.yml`** - cancels a PR's in-flight runs when it closes, so a withdrawn PR
  stops occupying runners. Housekeeping only; gates nothing.
- **`dependency-audit.yml`** - "Dependency Audit", job `deps: whole-tree CVE scan`. Named against
  `deps: vulnerabilities` (`maven.yml`), which reviews only the dependencies a PR *changes*; this one
  scans the whole resolved tree. The **only** place `ossindex-maven-plugin` is switched on
  (`-Dossindex.skip=false`); it binds to `validate`, so enabling it globally would mean six-plus
  scans per PR from one account. Runs on every in-repo PR, on dispatch, and **weekly on a schedule**
  - the schedule catches what no PR can, an unchanged tree acquiring a new advisory. (The one
  deliberate exception to "there is no scheduled build" below; it re-runs no suite the gate already
  covers.) Skipped for fork PRs, which receive no secrets and would 401 forever.
  - **Findings fail it.** astubbs/parallel-consumer#281 retired the standing backlog into
    `excludeVulnerabilityIds` entries in the root pom, each carrying a stated retirement condition,
    so a finding that reaches the gate is by construction an advisory nobody has looked at.
    PR-time rather than the schedule alone because a solo maintainer will not read a scheduled
    alert - the PR gate is the only channel with reliable attention.
  - **Two different reds, and they stay distinguishable.** Exit 1: the scan could not be proven to
    have run, so the *check* is broken and nothing was learned about the tree. Exit 2: the scan ran
    and found something, so the *tree* needs triage. Separate headings in the job summary. When both
    are true at once, exit 1 wins.
  - **False positive, disputed, or no fixed version?** Add the id to `excludeVulnerabilityIds` in
    the root pom with a reason and a retirement condition. `bin/check-ossindex-audit.sh` honours
    that list - the exported report does *not* pre-filter by it - and states the suppression count
    on every run, green ones included, so the list stays reviewable. If the fix is already merged
    upstream and you are only waiting for a release, mark it `TEMPORARY-SINCE: YYYY-MM-DD` so it
    expires (see `repo-hygiene.yml` above) instead of quietly joining the standing list.
  - The did-it-actually-scan guard is not optional: the plugin has **no setting that makes an
    unreachable scanner fatal**, so against an expired token it prints a WARNING and reports
    `BUILD SUCCESS` - the exact silent-green defect this repo has already shipped once. Maven runs
    with `-Dossindex.fail=false` deliberately, so a findings-bearing run still reaches the guard
    instead of dying in the Maven step and taking the summary with it.
    `bin/test-check-ossindex-audit.sh` runs first.
- **`release.yml`** - the dispatch-triggered release. See [`docs/releasing.md`](releasing.md).
- **`.semaphore/`** - legacy Confluent internal CI/release pipelines, retained but inactive on the
  fork.

## Self-hosted lanes

Setup and operation: [`docs/self-hosted-runner.md`](self-hosted-runner.md). None of these gate
merging - they exist for speed and for work too heavy for a 2-core hosted runner. All are
**skipped for PRs from forks** (`head.repo.full_name == github.repository`), because a fork PR must
never run on our own hardware.

**`highcpu` is the only self-hosted label.** Declare labels in
[`.github/actionlint.yaml`](../.github/actionlint.yaml) or actionlint flags them.

- `pr-highcpu-fast-feedback.yml` ("highcpu") - on every in-repo PR plus dispatch. The lane that
  earns the hardware; it carries the Chaos Pain Suite check.
- `mutation-full-sweep.yml` - dispatch only: the whole-project PIT sweep
  (`bin/ci-mutation-test.sh -Dverbose=true -Dthreads=N`). The PR-scoped mutation job in `maven.yml`
  only covers classes changed against the base; this is its exhaustive counterpart.

**There is no scheduled build, deliberately.** Every suite worth re-running is already a required
check on each PR and runs again on every push to master, so a cron lane would only repeat covered
work. **Do not add a lane for suites the gate already covers.** The repo's single cron lane,
`dependency-audit.yml`, is not a counter-example: it runs no *suite*, and what it catches - a new
advisory published against an unchanged dependency tree - is a function of elapsed time, which no
PR-triggered check can ever see. That is the test to apply to any future scheduled lane: **does time
alone change the answer?**

**Before pinning a job to a self-hosted label, confirm a runner serves it** -
`gh api repos/astubbs/parallel-consumer/actions/runners` lists each runner's labels and online
status. A job pinned to a label nothing advertises does not fail; it queues until GitHub cancels
it, so the lane reports nothing at all and looks merely quiet.

**Beware: `performance` names two unrelated things.** It is the *test suite*
(`bin/performance-test.sh`, the required **Performance Tests** check, on every PR from `maven.yml`,
`ubuntu-latest`). It is **not** a runner label - the only self-hosted label is `highcpu`.
