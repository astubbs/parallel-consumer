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
  `grep -q` under `pipefail` - that construct can report failure *because* it matched, once the
  producer still has more than a pipe buffer left to write when `grep` exits, so it passes every
  small fixture and surfaces only in production. shellcheck does not detect it, and the full
  mechanism is in the script's own header and in
  [`solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`](solutions/workflow-issues/a-check-that-reports-success-without-having-run.md).
  `actions` runs `bin/check-action-versions.sh`, keeping every
  GitHub Action pinned to one version across all workflows. Self-tests run first. **Both are
  required status checks** (`shell: sigpipe`, `workflows: action versions`) - which is exactly why
  the job names are an API. They exist because the failures they catch are invisible rather than
  loud, and they gate precisely so those failures cannot be skimmed past.
- **`claude-code-review.yml`** - the **review gate**, not the reviewer. It runs on every PR push,
  invokes no Claude and costs nothing, and asserts one thing: that a review exists for the
  **current head**. It produces the required check `claude-review`, so the job name is an API here
  as it is in `repo-hygiene.yml`. See "The automated review" below.
- **`claude-code-review-dispatch.yml`** - the **reviewer**, `workflow_dispatch` only. It carries
  the packaged review procedure, the tool allowlist and the review instructions, and takes an
  optional `focus` steer. See "The automated review" below.
- **`claude.yml`** - the general `@claude` mention handler, and **not** the reviewer. A mention
  passes whatever was typed straight through as a free-form prompt: it has no `plugins:` line and
  never invokes the review command, so what it produces is an answer, not a review. **Be aware
  that the gate cannot tell the difference** - any fresh, finished `claude[bot]` comment
  satisfies it, so answering a `@claude` question on a PR turns `claude-review` green. See "What
  the gate proves" below.
- **`chaos-pain.yml`** - on-demand seeded chaos hunts (`workflow_dispatch`, inputs `seed`/`reps`).
  See [`docs/testing.md`](testing.md).
- **`cancel-closed-pr-runs.yml`** - cancels a PR's in-flight runs when it closes, so a withdrawn PR
  stops occupying runners. Housekeeping only; gates nothing.
- **`release.yml`** - the dispatch-triggered release. See [`docs/releasing.md`](releasing.md).
- **`.semaphore/`** - legacy Confluent internal CI/release pipelines, retained but inactive on the
  fork.

## The automated review

**The review does not run on push. Dispatch it when the PR is ready for review** - not on every
push, and not on work in progress:

```bash
gh workflow run claude-code-review-dispatch.yml -R astubbs/parallel-consumer --ref master \
  -f pr=<number> \
  -f focus="the guard's failure paths, not the docs"
```

It used to fire on every `pull_request` event, which spent a full review on every push,
overwhelmingly on branches that were not ready for one. That coupled "get CI feedback" to "spend a
review" tightly enough that people batched pushes to avoid it. The two are now split:

| | Runs | Cost | Produces |
|---|---|---|---|
| **Gate** (`claude-code-review.yml`) | every PR push | none - no Claude, no JDK, no build | the required check `claude-review` |
| **Reviewer** (`claude-code-review-dispatch.yml`) | when dispatched | a full review | the review itself |

**`--ref master` is required, not cosmetic.** It is what lets the reviewer review a PR that edits
the reviewer - see "Editing the reviewer" below. Dispatching from the PR's own branch reintroduces
the trap it avoids.

### Seed the review with `-f focus` when you have a steer

A review told where to look hardest is materially better than a bare one, and this is the half of
the old `@claude` mention route worth keeping. The steer is **appended** to the packaged review
procedure, never substituted for it, so it adds emphasis without narrowing the review or licensing
it to skip anything.

**Why not just mention `@claude review this`?** Because a mention passes whatever was typed through
as the entire prompt. The reviewer is not a free-form request - it is
`/code-review:code-review <repo>/pull/<n> --comment`, a packaged procedure invoked as a slash
command with `plugins: 'code-review@claude-code-plugins'`. Route a review through a mention and you
silently swap that procedure for an improvised one; the output still looks like a review, so
nothing tells you. **If you ever edit the reviewer, the `plugins:` input and the
`/code-review:code-review ... --comment` prompt must survive.**

### The `@claude review this` comment route, and the one thing it does better

Commenting `@claude review this` on a PR runs `claude.yml`. It holds **the same curated tool
allowlist as the dispatched reviewer**, so it can run this repo's check scripts and its test suites
rather than reading and guessing.

It was briefly granted nothing, and the gap showed: on `astubbs/parallel-consumer#288` the reviewer
said so itself - "tool permissions blocked Bash execution ... Everything above is from static
reading" - on a PR whose entire diff was check scripts and their tests. (An absent allowlist is not
permissive: Bash is simply not pre-approved, and CI has no interactive approver, so every script
call is refused.) The argument for leaving it that way was that comment text is
attacker-influencable where a dispatch is not. That is true and it is not the operative difference:
**both** routes run PR-authored code, and the protection was never that the trigger is trusted - it
is that the list is curated to this repo's own scripts, that the job holding it has no write grant,
and that it is withheld on fork heads. So the lists are the same, deliberately, and
[`.github/workflows/claude.yml`](../.github/workflows/claude.yml) says to keep them that way.

**On a fork PR this route answers but does not run anything.** Granting `./mvnw` and the `bin/`
scripts against a fork's checkout would put PR-controlled executables beside
`CLAUDE_CODE_OAUTH_TOKEN` - a fork could replace an allowlisted script and wait to be asked to run
the checks. That is the hazard the dispatched reviewer avoids by refusing fork heads outright; here
the reply still happens, with no execution tools, because a fork author asking a question is
legitimate. The job is also capped at 30 minutes, since a comment can start a test suite.

**This route can open real inline review threads, and the dispatched one cannot.** The action
installs its inline-comment server only for an *entity* event; `issue_comment` is one and
`workflow_dispatch` is not. Since unresolved review threads are what mechanically gate a merge here,
a blocking finding from this route actually blocks, where the dispatch route's can only ask a human
to act. **If you want findings that hold the merge, ask for the review by comment.**

The dispatch route keeps two advantages: `-f focus` for a steer, and the packaged
`/code-review:code-review` procedure rather than whatever the comment said.

Both routes now clear the gate themselves. Each has a `refresh-gate` job that re-runs
`claude-review` after a review that actually succeeded, because the gate only ever triggers on
`pull_request` and a check run keeps its last conclusion until something re-runs it. **Neither
refreshes it for a fork head** - see "A fork PR cannot turn the gate green" below, which the comment
route has to enforce explicitly because, unlike the dispatched reviewer, it will happily answer on a
fork PR.

**Escape hatch, if a review posts and `claude-review` stays red anyway:** re-run the gate's existing
run by hand. Re-running the *existing* run is what matters - it reports back into the same check
suite on the same commit, where a fresh run would attach its check to the default branch and never
satisfy the PR's required check.

```bash
gh run list -R astubbs/parallel-consumer --workflow claude-code-review.yml -c <the PR head SHA>
gh run rerun <run-id> --failed -R astubbs/parallel-consumer
```

### What the dispatch trigger costs

`claude-code-action` picks its behaviour from the **event**. On a pull-request event it runs in
*tag* mode - it opens a tracking comment as `claude[bot]`, installs an MCP server for inline review
comments, and installs another for reading CI. On anything else it runs in *agent* mode with none
of them, and `workflow_dispatch` is not a pull-request event.

It also cannot be made into one. Handing the action a synthesised `pull_request` payload via
`GITHUB_EVENT_NAME` / `GITHUB_EVENT_PATH` looks like it should work and does not: the runner writes
the `GITHUB_*` context variables into every step's environment *after* the step's own `env:` block,
so the override is discarded. Measured, not assumed - a step declaring both saw
`GITHUB_EVENT_NAME as this step sees it: 'workflow_dispatch'`. The payload file's contents are
writable; the event name is not, and the name alone picks the mode.

So the reviewer runs in agent mode, and three things are paid for explicitly rather than lost
quietly:

| Lost | Replaced by |
|---|---|
| the `claude[bot]` tracking comment the gate reads | a **mandatory** summary comment, required by the reviewer's system prompt as its last action |
| `mcp__github_inline_comment__create_inline_comment` | **nothing on this route** - ask by comment instead, see below |
| the CI-status MCP server | `gh run list` / `gh run view` grants plus `additional_permissions: actions: read`. Not `gh pr checks` - reading check runs needs a `checks` scope this token cannot be given |

**The inline-comment loss is real, and it is confined to this route.** An unresolved review thread
is the only thing that mechanically gates a merge here, and the *dispatched* reviewer can no longer
open one: the action installs its inline-comment server only for an entity event, and a
`workflow_dispatch` is not one. On a dispatch, blocking findings arrive as a marked section at the
top of the summary comment, with the reviewer instructed to say plainly that a human has to act on
them.

**The comment route does not have this problem** - `issue_comment` *is* an entity event, so
`claude.yml` grants the tool and gets real review threads. So the honest guidance is not "we lost
inline comments", it is **"ask by comment when you want findings that block, and by dispatch when
you want the packaged procedure or a `-f focus` steer"**. Closing the gap on the dispatch route
itself would need a trigger that carries a PR context; it is recorded in
[`docs/inflight/ci-review-agent.md`](inflight/ci-review-agent.md).

### Editing the reviewer cannot be tested before it merges

`claude-code-action` refuses to run unless the workflow file invoking it is **byte-identical to the
copy on the default branch**. That guard is what stops a PR rewriting its own reviewer, and it
applies to a dispatch just as it does to a `pull_request` run: `--ref <a branch that edits this
workflow>` gets

```
Skipping action due to workflow validation: The workflow file must exist and have identical
content to the version on the repository's default branch.
```

and the action exits. So a change to `claude-code-review-dispatch.yml` can only be exercised for
real **after** it is on master, dispatched `--ref master`. Plan for that: land the change, dispatch
once against a live PR, and read the run rather than assuming.

**A PR that edits the reviewer can still be reviewed, though - do not reach for a bypass.** The
refusal is about which workflow file *runs*, not about which code is *read*. A `--ref master`
dispatch runs master's copy, so validation passes, and the checkout step below fetches
`refs/pull/<n>/head` - so master's reviewer reads the PR's diff, including its edits to this
workflow, and posts the summary comment that turns `claude-review` green. What cannot happen before
the merge is exercising the PR's **new** reviewer behaviour; reviewing the PR is a different thing
and it works.

The one case where that is not true is a PR fixing a reviewer that is *itself* broken on master -
this PR being the example, since master's dispatched reviewer refuses every invocation. Then there
is nothing on master to dispatch and merging does need an admin bypass, the same cost
`astubbs/parallel-consumer#124` paid. That is a property of the bug being fixed, not of touching the
file.

The action **exits 0** when it refuses like this, so the reviewer job explicitly checks the
action's own `conclusion` output and fails when it is empty. Without that step the run is green,
eleven seconds long, and has reviewed nothing - which is
[the defect this repo keeps meeting](solutions/workflow-issues/a-check-that-reports-success-without-having-run.md).

### A red `claude-review` is the expected state on an unreviewed PR

It is **not** a fault to diagnose and **not** something to fix by editing the gate. It means what
it says: nothing has reviewed this head yet. The fix is to dispatch a review once the work is
actually ready. Three distinct reds, each saying which it is:

- **never reviewed** - nobody has asked. Normal for a new or in-progress PR.
- **reviewed, but not since `<time>`** - there is a review, but you have pushed since. See below.
- **the reviewer left unticked boxes** - it started and did not finish, so ask again.

### Strict on purpose: a review of commit N does not vouch for commit N+1

The gate requires a review **newer than the current head**, so a push after a review turns it red
again and wants a fresh one. The lenient alternative - any review on the PR, ever, satisfies the
gate - was rejected because it reports success for code nothing has looked at, which is exactly the
class named in
[`docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`](solutions/workflow-issues/a-check-that-reports-success-without-having-run.md).
A gate whose whole job is to stand in for a missing always-on reviewer must not itself be
satisfiable by a review of code that no longer exists.

**There is deliberately no skip word, label or "trivial change" escape**, because any such escape
is asserted by the same person who wants to use it - which makes it exactly as strong as not having
the gate. The honest escape already exists and is loud: merge with the required check red, which
leaves a permanent record that somebody chose to merge unreviewed. Re-requesting a review after a
one-line fix costs one command and a couple of minutes, so an escape would save little and cost the
guarantee.

### A fork PR cannot turn the gate green

This is the one case where merging with the check red is the *expected* route rather than a last
resort, so it is written down rather than left to be discovered.

Both review routes have to hold this line, and they hold it differently. The dispatched reviewer
refuses a fork head outright and never runs. The `@claude` comment route **does** answer on a fork
PR - that is useful, and it executes nothing - but it refuses to refresh `claude-review` afterwards.
Without that refusal a maintainer merely *asking* a question on a fork PR would turn its required
check green, since the gate cannot tell a review from any other finished `claude[bot]` reply. That
is the self-asserted escape rejected at the end of this section, arriving by accident instead of on
purpose.

The reviewer refuses any PR whose head is not in this repo, on purpose: `workflow_dispatch` hands
the job `CLAUDE_CODE_OAUTH_TOKEN`, and it then checks out `refs/pull/<pr>/head` and runs the repo's
build and test scripts from it - so reviewing a fork would execute untrusted code beside the
credential. The old `pull_request` trigger prevented that for free by withholding secrets from fork
PRs; on a dispatch the guard has to be explicit. Meanwhile the gate accepts only a `claude[bot]`
comment, so no amount of careful human reading satisfies it. The two facts together mean a fork PR
stays red.

Two honest options, in order:

1. **Push the same commits to a branch in this repo and open the PR from there.** The reviewer runs
   and the check can go green. This is the path worth offering a contributor whose change you want
   to land.
2. **Review it by hand and merge with `claude-review` red**, which leaves the same permanent record
   as any other unreviewed merge.

**Deliberately not offered: letting a maintainer declare the PR reviewed.** That is the
self-asserted escape rejected just above, wearing a different hat - the person who wants the merge
would also be the person certifying the review, which makes the gate exactly as strong as not
having one. The real fix is a reviewer that can run without the credential, which is a different
piece of work and is recorded in
[`docs/inflight/ci-review-agent.md`](inflight/ci-review-agent.md).

### The reviewer runs PR code, so it holds no write grant

The fork refusal answers *whose* code runs beside the credential. The second boundary answers *what
that code could reach*, and it is why the reviewer is **two jobs**.

The `review` job checks out the PR and executes its scripts - that is the point of letting the
reviewer run tests - so on a PR branch those scripts are whatever the PR says they are. It therefore
runs at `actions: read`, and its checkout sets `persist-credentials: false` so the job's token is
not written into `.git/config` beside the code it is about to run.

The one write grant the contract needs, `actions: write` to re-run the gate, lives in a second job,
`refresh-gate`, which **checks nothing out**. Held in `review` instead, that token could dispatch,
cancel or re-run any workflow in the repo - `release.yml` among them - from a filesystem a PR author
controls. A same-repository PR does mean a write-access author, but write access to a branch is not
the same thing as a token that can fire a release. **Do not merge the two jobs back together.**

### What the gate proves, and what it does not

`bin/check-review-posted.sh` (self-tested by `bin/test-check-review-posted.sh`, which runs first)
takes the PR's comments and requires **one** comment that is all three of: authored by the reviewer
bot per the GitHub API, created strictly after the head appeared, and free of unticked task-list
boxes. Four limits are worth knowing before reading a green tick as a review:

1. **It does not read the review, and never will** - a check that judges review quality is a
   check nobody can keep honest.
2. **It cannot tell a review from any other answer the bot gave.** Asking `@claude` a question on
   the PR produces a comment satisfying all three rules, which turns the check green. A known
   limit, not a distinction the gate makes.
3. **The head's arrival time is the SHA's, not this PR's.** Force-pushing onto a commit that
   already ran checks elsewhere carries that older timestamp, so a review of a previous head can
   postdate it.
4. **It runs from the PR's own checkout**, like every other `pull_request` check here, so it
   polices a tree that can edit it.

The remedy for 2 and 3 is one change, recorded in
[`docs/inflight/ci-review-agent.md`](inflight/ci-review-agent.md): have the reviewer record the
exact SHA it reviewed as a check run on that head, and gate on that rather than on a timestamp.

It is a guard against the action failing quietly, not against somebody who wants to get around
it. **Do not disable it to get a green check.**

The gate exists because `claude-code-action` exits 0 in several situations where it reviews
nothing, so a check wired straight to it certifies nothing while looking identical to "reviewed, no
findings". That is not specific to the reviewer - it is a named class in this repo, with the
reviewer's workflow-validation skip as one of its recorded instances:
[`a-check-that-reports-success-without-having-run.md`](solutions/workflow-issues/a-check-that-reports-success-without-having-run.md)
has the full roster and the guard-design rules. The reviewer-specific occurrences are listed in
`bin/check-review-posted.sh`'s header.

### Editing the reviewer

`claude-code-action` refuses to run when **the workflow file that invokes it** differs from the
default branch's copy, so a PR cannot rewrite its own reviewer. Two consequences:

- **`claude-code-review.yml` is no longer affected**, because it no longer invokes the action.
  Editing the gate used to make its own `claude-review` check unfixably red - re-running cannot
  help, since the input is the workflow diff itself - and that meant an admin ruleset bypass to
  merge. It does not any more.
- **The reviewer is not affected either, as long as it is dispatched `--ref master`.** A dispatch
  runs the workflow from the ref you name, so master's copy of
  `claude-code-review-dispatch.yml` does the reviewing while the PR branch supplies only the code
  under review. Reviewer and reviewed are then two different things, which is precisely what a
  `pull_request` run cannot arrange. Dispatching from the PR branch instead runs the PR's copy of
  the reviewer, which is the case the guard exists to refuse - so do not.
- **The flip side:** changes to the reviewer's grants, procedure or instructions do not take effect
  until they merge, so the PR that makes them is reviewed by the old configuration. Expect the
  reviewer to say it lacks a grant that PR adds.

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
work. **Do not add a lane for suites the gate already covers.**

**Before pinning a job to a self-hosted label, confirm a runner serves it** -
`gh api repos/astubbs/parallel-consumer/actions/runners` lists each runner's labels and online
status. A job pinned to a label nothing advertises does not fail; it queues until GitHub cancels
it, so the lane reports nothing at all and looks merely quiet.

**Beware: `performance` names two unrelated things.** It is the *test suite*
(`bin/performance-test.sh`, the required **Performance Tests** check, on every PR from `maven.yml`,
`ubuntu-latest`). It is **not** a runner label - the only self-hosted label is `highcpu`.
