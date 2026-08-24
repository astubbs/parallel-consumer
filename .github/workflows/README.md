# Workflows - one line each

What each file is, so you can pick the right one without opening every one of them. The detail lives in
[`docs/ci.md`](../../docs/ci.md); each workflow's own header carries the reasoning behind it.

**The three `claude*` files are the confusing ones**, because the names do not separate them and
`claude-code-review.yml` is the one that does **not** review. One judges, two review:

| File | Runs when | What it is |
|---|---|---|
| `claude-code-review.yml` | every PR push | **the gate** - judges, never reviews. Produces two independent required checks: `claude-review` (an automated review happened) and `review: human LGTM` (the owner said so) |
| `claude-code-review-dispatch.yml` | `workflow_dispatch` | **the dispatched reviewer** - packaged procedure, takes a `focus` steer, cannot open inline threads |
| `claude.yml` | an `@claude` comment | **the comment reviewer** - same tools, free-form, and the only route that can open inline review threads |

Ask by comment when you want findings that block a merge; dispatch when you want the packaged
procedure or a steer. Why, and the gate's exact contract: [`docs/ci.md`](../../docs/ci.md).

## All workflows

| File | One line |
|---|---|
| `cancel-closed-pr-runs.yml` | Cancels in-flight runs for a PR once it closes, so a dead PR stops burning runners. |
| `chaos-pain.yml` | On-demand seeded chaos hunts for the stall bugs, `workflow_dispatch` with `seed`/`reps`. Self-hosted high-CPU lane. |
| `check-dependencies.yml` | Blocks a stacked PR until the PR it says it `depends on #N` has merged. |
| `claude-code-review-dispatch.yml` | The dispatched code reviewer - the expensive half, run when somebody asks for it. |
| `claude-code-review.yml` | The review gate. Invokes no Claude and costs nothing. What satisfies it: [`docs/ci.md`](../../docs/ci.md), stated once there. |
| `claude.yml` | Answers `@claude` comments, and reviews when asked to - the only route that can raise inline review threads. |
| `copyright.yml` | Checks source headers against the fork policy on every push and PR. |
| `dependency-audit.yml` | Scans the **whole** resolved dependency tree for CVEs - the only place OSS Index is switched on. Per-PR, on demand, and weekly, because a new advisory needs no push to arrive. |
| `maven.yml` | The main build: unit, integration and performance suites, SpotBugs, duplicate detection, PIT, dependency scanning. |
| `mutation-full-sweep.yml` | The whole-repo PIT mutation sweep, on demand - too slow for a PR. Self-hosted high-CPU lane. |
| `pr-checklist.yml` | Makes the PR template binding: the checklist must be present and every box resolved. |
| `pr-highcpu-fast-feedback.yml` | Fast PR feedback on the self-hosted high-CPU runners, including the per-PR chaos tripwire. |
| `publish.yml` | Publishes to Maven Central on every push to `master`; the pom version decides snapshot or release. |
| `quarantine-lane.yml` | Runs the quarantined tests separately, so known-flaky tests neither block nor disappear. |
| `release.yml` | Cuts a release. `workflow_dispatch`, and deliberately the most dangerous button here. |
| `repo-hygiene.yml` | Small always-on repo checks - shell sigpipe traps, one pinned version per GitHub Action, and expiring the pom's temporary CVE exclusions. |

## One required check is not in this directory

**`CodeQL` is a required status check that no workflow file here produces**, so grepping the YAML
for a red check's name works for every context except that one - this paragraph is what the search
lands on instead. It is GitHub's code-scanning default setup,
configured in repository settings rather than in the tree; `gh api
repos/astubbs/parallel-consumer/code-scanning/default-setup` is what answers for it. Do not add a
`codeql-analysis.yml` to fix the asymmetry - the two setups are mutually exclusive, and what that
trade costs is in [`docs/ci.md`](../../docs/ci.md).

## Two conventions that will bite you

- **Job names are an API.** `claude-review`, `review: human LGTM`, `shell: sigpipe`, `workflows: action versions`,
  `Check PR Dependencies` and the `maven.yml` suites are required status checks matched **by
  name** in the master ruleset. Rename a job and the ruleset silently stops being satisfied by
  anything - it does not fail, it just never passes.
- **Most of these run PR-authored code.** A `pull_request` job checks out the PR, so anything it
  executes is whatever the PR says it is. That is why the review jobs hold no write scope, and why
  `actions: write` lives alone in a job that checks nothing out. See "The reviewer runs PR code"
  in [`docs/ci.md`](../../docs/ci.md) before adding a permission to any of them.
