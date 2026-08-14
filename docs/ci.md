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

**For a one-line-per-file index, see [`.github/workflows/README.md`](../.github/workflows/README.md)** -
it sits next to the files, so it is what you find when you are looking at them rather than at this
document. This section is the detail behind it.

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
  `rename` runs `bin/test-rename-packages.sh`, the self-test for the
  package-rename tool (`bin/rename-packages.sh`) - a tool run by hand once per branch, which is
  exactly the shape that rots unnoticed between the day it is written and the day the whole rename
  depends on it. `actions` runs `bin/check-action-versions.sh`, keeping every
  GitHub Action pinned to one version across all workflows. Self-tests run first. **`shell: sigpipe`
  and `workflows: action versions` are required status checks** - which is exactly why the job names
  are an API. They exist because the failures they catch are invisible rather than loud, and they
  gate precisely so those failures cannot be skimmed past. `tooling: package rename` is not in the
  ruleset yet: a required context no run produces blocks every PR whose base predates it, so it can
  only be added once the job is on master.
  - `cve-exclusions` runs `bin/check-cve-exclusions.sh`, which **expires temporary CVE
    exclusions**. Entries in the root pom's `excludeVulnerabilityIds` come in two kinds: *standing*
    (retiring them needs someone else to act, on no timetable we control) and *temporary* (the
    upstream fix is merged, and the entry exists only because no release carries it yet). A
    temporary entry is marked `TEMPORARY-SINCE: YYYY-MM-DD` in the comment above it, and the check
    fails once it is more than **90 days** old - one quarter, anchored on jackson-databind 2.18.x's
    observed patch cadence (median gap ~65 days), since the upstream patch line, not our own
    release cycle, is what actually retires these entries. Undated, unparseable and future-dated
    markers **fail** rather than reading as "not expired yet", and so does an id with no rationale
    comment, because an unclassified entry silently means standing-forever - the rule from
    [`solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`](solutions/workflow-issues/a-check-that-reports-success-without-having-run.md),
    applied to the audit's own escape hatch: ask what the check does when it cannot decide, and make
    that state distinguishable from pass. **Exit 3**, deliberately
    not 1 or 2: those belong to `bin/check-ossindex-audit.sh` (broken lane / real finding) and this
    is neither - the scan and the tree are fine, the bookkeeping has rotted. The red is always
    clearable in one reviewable line: retire the entry if the fix shipped, or re-date / reclassify
    it having re-checked upstream. It lives here rather than in the audit job because that job is
    skipped for fork PRs and dies early on a token expiry, so the list would go unwatched exactly
    when it matters most. **`deps: CVE exclusion expiry` is a new job name and is NOT yet a required
    status check** - adding it to the master ruleset is a separate, deliberate act.
### The three `claude*` workflows, and which is which

Their filenames do not distinguish them well - `claude-code-review.yml` is the one file that does
**not** review - so read this before editing any of them. One judges, two review:

| File | Runs when | What it is |
|---|---|---|
| `claude-code-review.yml` | every PR push | **the gate.** Judges; never reviews |
| `claude-code-review-dispatch.yml` | `workflow_dispatch` | **the dispatched reviewer** |
| `claude.yml` | an `@claude` comment | **the comment reviewer**, and the general mention handler |

- **`claude-code-review.yml`** - the **review gate**, not the reviewer. It runs on every PR push,
  invokes no Claude and costs nothing, and asserts two things - an automated review, and a human
  LGTM from the repo owner - stated once, under
  ["The gate asks..."](#the-gate-asks-has-this-pr-been-reviewed-not-was-every-commit-reviewed)
  below, and deliberately not repeated here. It produces the required check
  **`review: bot + human LGTM`**, so the job name is an API here as it is in `repo-hygiene.yml`.
  That check was called `claude-review` until the human half was added, and the rename is a
  ruleset migration rather than an edit: while it is in flight the file carries a second,
  transitional job still reporting the old name under the old contract, so that a required
  context is never left with nothing producing it. Its header says when to delete it.
- **`claude-code-review-dispatch.yml`** - the **dispatched reviewer**, `workflow_dispatch` only.
  It carries the packaged review procedure, the tool allowlist and the review instructions, and
  takes an optional `focus` steer. It cannot open inline review comments. See "The automated
  review" below.
- **`claude.yml`** - the `@claude` mention handler, and **also a reviewer**: it holds the same
  execution allowlist as the dispatched route and is the **only** route that can open inline
  review threads, so it is the one to use when you want findings that block a merge. What it does
  not carry is the packaged procedure - a mention passes whatever was typed straight through as a
  free-form prompt - so `@claude review this` gets you a review, and `@claude why is X slow?` gets
  you an answer. **The gate cannot tell those two apart**: any finished `claude[bot]` comment
  satisfies its automated half, so answering a `@claude` question on a PR clears that half. See
  "What the gate proves" below.
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
| **Gate** (`claude-code-review.yml`) | every PR push | none - no Claude, no JDK, no build | the required check `review: bot + human LGTM` |
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

**The PR's code is on disk here, even though the checkout step names no `ref:`** - and the second
half of that sentence is why it has been got wrong. `actions/checkout` does put master's tree down,
but `claude-code-action` then checks the PR out itself: in tag mode it fetches the head branch by
name and switches to it (`src/github/operations/branch.ts`, `setupBranch`). Visible in any comment
run's log, e.g. run
[`31550657594`](https://github.com/astubbs/parallel-consumer/actions/runs/31550657594) - `git
checkout --force -B master refs/remotes/origin/master` in "Checkout repository", then `Switched to a
new branch 'ci/ossindex-audit-job'` inside "Run Claude Code". So a `bin/` script run here is the
PR's. What does **not** come from the PR is the workflow file, and therefore the allowlist - see
"Editing the reviewer". Both routes grant `git rev-parse` so a reviewer can settle which tree it is
standing in by looking, rather than inferring it from the workflow and getting it backwards.

**On a fork PR this route answers but does not run anything** - and "answers" needs one
qualification. Granting `./mvnw` and the `bin/` scripts against a fork's checkout would put
PR-controlled executables beside `CLAUDE_CODE_OAUTH_TOKEN`: a fork could replace an allowlisted
script and wait to be asked to run the checks. That hazard is real on this route and not only on the
dispatched one, because the action fetches a *cross-repository* PR through
`pull/<n>/head:<branch>` - the same `setupBranch`, an explicit fork case - so a fork's files reach
the runner exactly as an in-repo branch's do. The dispatched reviewer avoids it by refusing fork
heads outright; here the *reply* still happens, with no execution tools.

The qualification: **only a maintainer gets that reply.** `claude-code-action` runs its own
collaborator-permission check and exits before Claude starts, so a fork author - or anyone else
without write access - commenting `@claude` gets nothing at all, on any PR. That is the action's
behaviour rather than something this workflow chooses, and it is why the trusted-commenter
carve-out is defence in depth rather than the only guard. The useful case on a fork PR is a
*maintainer* asking a question and getting a read-only answer.

The job is also capped at 30 minutes, since a comment can start a test suite.

**This route can open real inline review threads, and the dispatched one cannot.** The action
installs its inline-comment server only for an *entity* event; `issue_comment` is one and
`workflow_dispatch` is not. Since unresolved review threads are what mechanically gate a merge here,
a blocking finding from this route actually blocks, where the dispatch route's can only ask a human
to act. **If you want findings that hold the merge, ask for the review by comment.**

The dispatch route keeps two advantages: `-f focus` for a steer, and the packaged
`/code-review:code-review` procedure rather than whatever the comment said.

Both routes now clear the gate's automated half themselves. Each has a `refresh-gate` job that
re-runs the gate after a review that actually succeeded, because the gate only ever triggers on
`pull_request` and a check run keeps its last conclusion until something re-runs it. **Neither
refreshes it for a fork head** - see "A fork PR cannot turn the gate green" below, which the comment
route has to enforce explicitly because, unlike the dispatched reviewer, it will happily answer on a
fork PR.

**Escape hatch, if a review posts (or an LGTM lands) and the gate stays red anyway:** re-run the gate's existing
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
workflow, and posts the summary comment that clears the gate's automated half. What cannot happen before
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

### A red gate is the expected state on an unreviewed PR

It is **not** a fault to diagnose and **not** something to fix by editing the gate. It means what
it says: nobody has reviewed it yet. The fix is to ask for a review when the work is ready, and to
leave an LGTM when you are happy for it to land. Each red says which of them is outstanding, and
the two halves are worded so you never have to open the job to tell them apart:

- **never reviewed** - `claude[bot]` has not commented. Normal for a new or in-progress PR.
- **the reviewer left unticked boxes** - it started and did not finish, so ask again.
- **no human LGTM** - `astubbs` has not submitted a review saying LGTM. Every one of these starts
  `NO HUMAN LGTM ON THIS PR`, and there are three of them: he has not reviewed it at all, he has
  reviewed it and never said the word, or he wrote something the rule refuses (`lgtm?`, a
  negation, or an LGTM inside a code fence) - in which case the message says which.
- **the human half could not be scanned** - `Could not scan this PR's reviews for a human LGTM`,
  which deliberately does **not** start `NO HUMAN LGTM ON THIS PR`, because it is not one. The
  scan itself failed, so the checker refused to guess and treated the PR as unstamped. This one
  is a bug to fix rather than a review to do, and it is the only red here that means "the
  instrument broke" rather than "the work is outstanding".

Both halves are evaluated on every run, even when the other has already failed, so one look tells
you everything that is outstanding.

(A further red is possible and says so plainly: the PR's head is in a fork. See "A fork PR cannot
turn the gate green" below.)

**The human half will usually need one re-run.** Submitting a review raises no `pull_request`
event, so the gate does not re-evaluate itself when the LGTM lands - the same mechanism, and the
same fix, as the escape hatch above: re-run the gate's existing run. Adding a `pull_request_review`
trigger is the obvious fix and is **not wired**, deliberately: it changes when a required check
runs, which is its own decision. It used to be a *trap* as well - the jobs were guarded on
`github.event.sender.type`, and on that event the sender is the reviewer, so a bot-submitted review
would have *skipped* the job, which satisfies a required check - and the workflow header records
both that trap and its closure, since the guard now reads the PR author instead. Leaving the LGTM
*before* asking for the automated review avoids the re-run entirely, because the reviewer's
`refresh-gate` job then re-runs the gate with both halves already true.

### The gate asks "has this PR been reviewed?", not "was every commit reviewed?"

<!-- CANONICAL: the gate contract. Nowhere else states what satisfies the gate - everything else
     links here. If you change this paragraph, run bin/check-review-gate-contract.sh.
     The sub-contract of the human half - what counts as an LGTM - is canonical in
     bin/check-human-lgtm.sh, NOT here. Do not paste a copy of it back into this file. -->

**Two things satisfy it, and it needs both**, whenever either of them happened:

1. **Any finished `claude[bot]` review on the PR** - the automated half.
2. **A pull-request review submitted by `astubbs` whose body says LGTM** - the human half.

A review of the first commit therefore vouches for the twentieth, and so does an LGTM left on it.
That is a deliberate reversal of the rule this repo shipped first, and it is now applied to both
halves alike.

**What exactly counts as an LGTM is stated in `bin/check-human-lgtm.sh` and only there** - the
whole-word rule, the `?` and negation clauses, why a fenced or quoted LGTM does not stamp, why an
empty `APPROVED` does not count while a `DISMISSED` review does, and the survey of every owner LGTM
this repo has actually received that settled case-insensitivity. That file is the executable
version of the rule, so its prose sits beside the awk that implements it and the two cannot drift;
a copy here could, and did. An earlier draft of this section carried a seven-clause restatement of
it, with a summary of that survey that was wrong in every particular - wrong count, wrong PR range,
and wrong about the one capitalised `Lgtm` that makes case-insensitivity load-bearing rather than
merely kind. It was wrong in four files at once, because it had been pasted into four.

The two altitudes: **this section owns the gate contract** - what satisfies the gate, which is the
two numbered items above. **`bin/check-human-lgtm.sh` owns the matching rule** - what satisfies the
human half of it. Link, do not paraphrase, in either direction.

**Neither half proves the reviewer read anything**, and the human half proves less than the
automated one: it is a **memory aid**, not a control. The owner is both the person it asserts
about and the person who wants the merge, so it stops nobody - what it does is turn "have I read
this one myself yet?" from something to remember across a dozen open PRs into a red check that
names which half is outstanding. That is not the self-asserted escape rejected at the end of this
section, and the difference is the direction: an escape *removes* an obligation, and is worthless
when the person asserting it wants the merge; a self-asserted *requirement* only ever makes the
gate redder.

**This paragraph is the only statement of that contract.** It was restated in nine files at one
point, in nine slightly different sentences, and four of them were still describing the *previous*
contract weeks after it changed - caught one at a time by four separate review rounds. Every other
mention now links here instead of paraphrasing.

It is a real trade, so it is worth knowing why it was made rather than discovering the cost by
surprise. Strict is the stronger guarantee - a review of commit N genuinely does not vouch for
commit N+1 - and it was abandoned not because it was wrong but because of **what enforcing it
cost**, and because the per-commit coverage it protected already arrives from elsewhere: a
separate auto-reviewer reads every push. What freshness cost was the **timestamp machinery**: a
contested comparison between the contributor-controlled committer date and the server-side
check-suite time, same-second ties, an endpoint with undocumented ordering, and the reviewed-SHA
plumbing crossing job boundaries. All of that is gone.

**What it did not cost, despite an earlier draft here saying so, is the `actions: write` scope.**
Both `refresh-gate` jobs still hold it, because the gate is produced by a `pull_request` workflow
and a review landing after the last push raises no event to re-evaluate it - true under either
rule. That scope is a cost of *how the check is produced*, not of what it asks, so leniency could
never have paid it off; retiring it needs the reviewer to raise a check run on the reviewed SHA.
The distinction matters to anyone weighing a return to strictness: restoring it buys back the
guarantee at the price of the timestamp machinery alone, not of a new privilege escalation.

**The assumption that makes this safe is that the auto-review keeps happening.** If it ever stops,
per-commit coverage stops coming from anywhere and nothing announces it - the gate keeps passing,
because "a review exists on this PR" is still true. That trigger, and the archived strict
implementation, are recorded in
[`docs/inflight/parked-strict-review-gate-freshness.md`](inflight/parked-strict-review-gate-freshness.md).
Read it before re-proposing strictness; it is a considered trade, not an oversight.

**There is deliberately no skip word, label or "trivial change" escape**, because any such escape
is asserted by the same person who wants to use it - which makes it exactly as strong as not having
the gate. The honest escape already exists and is loud: merge with the required check red, which
leaves a permanent record that somebody chose to merge unreviewed.

### A fork PR cannot turn the gate green

This is the one case where merging with the check red is the *expected* route rather than a last
resort, so it is written down rather than left to be discovered.

**The gate itself refuses a fork head**, before it looks at any comment. That is the primary
guard and it is stated first because it became load-bearing when the freshness rule went: the
gate now asks only whether a finished `claude[bot]` comment exists on the PR, and the `@claude`
route answers on fork PRs quite happily - so without the refusal, a maintainer merely *asking a
question* on a fork PR would satisfy the required check on the next push. That is the
self-asserted escape rejected at the end of this section, arriving by accident rather than on
purpose.

Two further facts hold the same line from the other side. The **dispatched reviewer** refuses any
PR whose head is not in this repo: `workflow_dispatch` hands the job `CLAUDE_CODE_OAUTH_TOKEN`, and
it then checks out `refs/pull/<pr>/head` and runs the repo's build and test scripts from it - so
reviewing a fork would execute untrusted code beside the credential. The old `pull_request` trigger
prevented that for free by withholding secrets from fork PRs; on a dispatch the guard has to be
explicit. The **comment route** does answer on a fork PR - useful, and harmless once its execution
allowlist is withheld - but produces nothing the gate will accept.

So a fork PR stays red, from three directions.

Two honest options, in order:

1. **Push the same commits to a branch in this repo and open the PR from there.** The reviewer runs
   and the check can go green. This is the path worth offering a contributor whose change you want
   to land.
2. **Review it by hand and merge with the gate red**, which leaves the same permanent record
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
takes the PR's comments and requires **one** comment that is both: authored by the reviewer bot
per the GitHub API, and free of unticked task-list boxes. Three limits are worth knowing before
reading a green tick as a review:

1. **It does not read the review, and never will** - a check that judges review quality is a
   check nobody can keep honest.
2. **It cannot tell a review from any other answer the bot gave.** Asking `@claude` a question on
   the PR produces a comment satisfying both rules, which turns the check green. A known limit,
   not a distinction the gate makes - and a larger one now that age is not a rule, since any such
   answer counts for the life of the PR. This is why fork PRs are refused outright rather than
   left to the comment rules.
3. **It runs from the PR's own checkout**, like every other `pull_request` check here, so it
   polices a tree that can edit it.

The remedy for 2 is one change, recorded in
[`docs/inflight/ci-review-agent.md`](inflight/ci-review-agent.md): have the reviewer raise a check
run on the exact SHA it reviewed, and gate on that rather than on comment metadata. That would
also retire the `refresh-gate` jobs and the last `actions: write` in the review system.

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
  Editing the gate used to make its own required check unfixably red - re-running cannot
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
  reviewer to say it lacks a grant that PR adds. **The comment route is no exception and no
  escape** - GitHub runs an `issue_comment` workflow from the default branch too, so `@claude` on
  the PR exercises master's `claude.yml`. Both routes now say so in the reviewer's system prompt,
  because this has already been misdiagnosed once as an approval layer beyond `--allowedTools`
  ([`docs/inflight/ci-review-agent.md`](inflight/ci-review-agent.md)).
- **So new `bin/` guards are granted by pattern, not one at a time.** Both allowlists carry
  `Bash(bin/check-*.sh:*)` and `Bash(bin/test-check-*.sh:*)` (each in the bare and `./` spelling),
  which means **a new `bin/check-*.sh` needs no allowlist change at all** - it is covered the
  moment it exists on the default branch. The reasoning, and what the pattern deliberately
  accepts, is in
  [`claude-code-review-dispatch.yml`](../.github/workflows/claude-code-review-dispatch.yml).

#### What the allowlist is for, and what it is not for

Two boundaries meet here, and mistaking one for the other is how the list gets widened. *Whose* code
runs beside the credential is settled by the fork refusal; what the job's **token** can reach is
settled by splitting `review` from `refresh-gate`. Neither touches the third question: **what that
code can talk the reviewer into running.** An in-repo head does not make the diff, the PR body and
the comments trustworthy - they are still attacker-influencable text being fed to a model that can
call Bash. Keeping the grants to this repo's own read-only scripts, rather than `Bash(*)` or
`Bash(bin/*.sh:*)`, is the margin against injection-into-execution.

So: **grant a script when it is read-only and lets the reviewer check a claim rather than infer it.
Do not grant anything that writes, publishes, or reaches the network beyond `gh` reads.**

**The pattern grants turn that rule into a naming convention, and the convention is now the
boundary.** Nobody approves a new `bin/check-foo.sh`; naming it is what grants it. The two prefixes
were chosen to keep `deploy.sh`, `chaos-test.sh`, `soak-test.sh`, `performance-test.sh`,
`quarantined-test.sh` and `quarantine-lane-report.sh` outside the grant, so a script that writes must
not be named `check-*` or `test-check-*` - a misnamed one defeats the scoping silently and nothing
will flag it. ([`bin/AGENTS.md`](../bin/AGENTS.md) carries that one rule at the point someone would
break it.) `bin/test-check-docs-data.sh` is the single granted script that writes: a considered
exception, argued in the workflow comment, not a precedent.

#### Adding a grant that no pattern covers

For anything outside those two prefixes - the `ci-*-test.sh` wrappers, `./mvnw`, `actionlint`,
`bin/todo-index.sh`, the `node .github/scripts/*.test.js` entries - all three of these apply:

- **Both spellings.** Rules match the command as written, so `Bash(bin/foo.sh:*)` does **not** match
  `./bin/foo.sh`. A half-added grant is worse than none: the invocation fails in a way that reads
  like the script is broken, which is how the review on astubbs/parallel-consumer#108 logged ten
  permission denials, gave up, and still reported success.
- **Both files.** `claude-code-review-dispatch.yml` and `claude.yml` carry the same list and nothing
  mechanical keeps them in step.
- **An earlier PR than the script.** Per the bullet above, a grant never applies to the PR that adds
  it. That discipline is the one nobody keeps - four check scripts arrived with
  astubbs/parallel-consumer#279, all four granted in that same PR, and its reviewer could run none of
  them - which is exactly why the two `bin/` families became patterns instead.

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
