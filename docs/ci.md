# CI: workflows, lanes, and reading a failed run

What each workflow does, which ones gate merging, and how to get at a failed job's log. Read this
when CI goes red or when you are adding or changing a workflow. The rules that apply to every
agent regardless of CI live in AGENTS.md; this is the reference behind them.

## Reading a failed job's log

`gh run view --log` refuses while *any* job in the run is still going ("logs will be available when
it is complete"), and `--log-failed` is often empty for a Maven job, because the failure text is
ordinary stdout rather than an `::error::` annotation. Neither means the log is unavailable. Fetch
the job directly - this works as soon as **that job** finishes, regardless of the rest of the run:

```bash
jid=$(gh run view <run-id> --json jobs --jq '.jobs[] | select(.name=="Integration Tests") | .databaseId')
gh api "repos/astubbs/parallel-consumer/actions/jobs/$jid/logs" > /tmp/job.log
```

Then grep it: `Tests run:`, `<<< FAILURE`, and for broker integration tests the
`=== AMBIENT PROBE AUTOPSY ===` block, which classifies contention-vs-bug before you start reading
stack traces (see [`docs/testing.md`](testing.md)).

## Workflows

- **`maven.yml`** - build and test on every push/PR. PRs run two tiers in parallel: split suites on
  default Kafka 3.9.1 (`bin/ci-unit-test.sh`, `bin/ci-integration-test.sh`,
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
- **`quarantine-lane.yml`** - runs the `@Quarantined` tests non-gating on every PR push, every push
  to master, and on dispatch. See [`docs/testing.md`](testing.md).
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
- **`release.yml`** - the dispatch-triggered release. See [`docs/releasing.md`](releasing.md).
- **`.semaphore/`** - legacy Confluent internal CI/release pipelines, retained but inactive on the
  fork.

## Self-hosted lanes

Setup and operation: [`docs/SELF_HOSTED_RUNNER.md`](SELF_HOSTED_RUNNER.md). None of these gate
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
work. **Do not add a lane for suites the gate already covers.**

**Before pinning a job to a self-hosted label, confirm a runner serves it** -
`gh api repos/astubbs/parallel-consumer/actions/runners` lists each runner's labels and online
status. A job pinned to a label nothing advertises does not fail; it queues until GitHub cancels
it, so the lane reports nothing at all and looks merely quiet.

**Beware: `performance` names two unrelated things.** It is the *test suite*
(`bin/performance-test.sh`, the required **Performance Tests** check, on every PR from `maven.yml`,
`ubuntu-latest`). It is **not** a runner label - the only self-hosted label is `highcpu`.
