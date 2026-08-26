# CI: workflows, lanes, and reading a failed run

What each workflow does, which ones gate merging, and how to get at a failed job's log. Read this
when CI goes red or when you are adding or changing a workflow. The rules that apply to every
agent regardless of CI live in AGENTS.md; this is the reference behind them.

## Reading a failed job's log

**Every route here can hand you an incomplete log that looks complete, so the retrieval order is part
of the diagnosis, not a detail.**
[`docs/solutions/workflow-issues/gh-run-view-log-truncation.md`](solutions/workflow-issues/gh-run-view-log-truncation.md)
**owns the routes and the incidents**; what follows is the part you need before your first command.

**For a chaos or broker integration-test failure, go to the uploaded test-report artifact first.**
The counts are XML attributes and the `AMBIENT PROBE AUTOPSY` is captured inside `system-out`, so
neither depends on the console stream surviving:

```bash
gh run download <run-id> -R astubbs/parallel-consumer -n "highcpu-fast-feedback-reports-Chaos Pain Suite-<n>" -D /tmp/reports
# then parse /tmp/reports/**/failsafe-reports/TEST-*.xml - `errors`/`failures` are attributes
```

**For anything else, the run-logs archive**, which cannot truncate:

```bash
gh api repos/astubbs/parallel-consumer/actions/runs/<run-id>/logs > /tmp/logs.zip   # add /attempts/<n> for a re-run
unzip -p /tmp/logs.zip '*<job name>*.txt' > /tmp/job.log
```

Two routes that read as authoritative and are not:

- **`gh run view --job <id> --log` silently truncates.** It returned 990 lines of a ~5000-line chaos
  job on astubbs#357, cutting inside a *passing* test and then appending the post-job steps - so the
  file ends with cleanup and looks whole. Three separate sessions have filed a wrong diagnosis from
  it. Convenience only.
- **`gh api .../actions/jobs/<id>/logs` can exit 1 having written nothing**, with
  `the response contains terminal escape sequences; pass --allow-escape-sequences to output it anyway`
  on **stderr** - the one stream a `>` redirect does not capture, so you get an empty file and a job
  that appears to have no log. With the flag it still dies mid-stream on a large log
  (`read: operation timed out`), leaving a partial file with no marker.

**Check completeness before you diagnose, whichever route you used**: the log must end with a real
terminal marker - `Tests run:`, `BUILD SUCCESS`/`BUILD FAILURE`, or the post-job cleanup *of the step
you care about*. **A grep for a failure signature returning zero on a truncated log is a false
negative that reads exactly like a clean run**, and the more systematically you grep, the more
confident the wrong answer becomes.

Then grep: `Tests run:`, `<<< FAILURE`, and for broker integration tests the
`AMBIENT PROBE AUTOPSY` block, which classifies contention-vs-bug before you start reading
stack traces (see [`docs/testing.md`](testing.md)).

`gh run view --log` also refuses while *any* job in the run is still going ("logs will be available
when it is complete"), and `--log-failed` is often empty for a Maven job, because the failure text is
ordinary stdout rather than an `::error::` annotation. Neither means the log is unavailable.

## Workflows

**For a one-line-per-file index, see [`.github/workflows/README.md`](../.github/workflows/README.md)** -
it sits next to the files, so it is what you find when you are looking at them rather than at this
document. This section is the detail behind it.

- **`maven.yml`** - build and test on every push/PR. PRs run two tiers in parallel: split suites on
  the pom's default Kafka version (`bin/ci-unit-test.sh`, `bin/ci-integration-test.sh`,
  `bin/performance-test.sh`) for fast feedback, and an experimental Kafka 4.x compatibility check
  (`bin/ci-build.sh`). It also carries **`Chaos Pain Suite`**, the per-PR ambient tripwire,
  which moved here from the self-hosted box on 2026-08-26 - see
  ["Chaos does not need the self-hosted box"](#chaos-does-not-need-the-self-hosted-box). It is
  **gating**, like the suite it replaced: a chaos RED is a real finding. Also carries the seconds-fast Quarantine Audit job, SpotBugs, duplicate
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
  [`docs/releasing.md`](releasing.md)), the issue-reference gate (`issue-ref-gate.js`, see
  [`docs/issue-references.md`](issue-references.md)) and the file-reference gate
  (`file-ref-gate.js`, see [`docs/citations.md`](citations.md)), which fails a cited repo path that
  does not exist - whole tree, so a deletion that strands a citation fails the PR that made it. Each gate's logic is a unit-tested module and its self-test runs first, so a
  broken rule fails loudly rather than passing - or failing - every PR silently.
- **`check-dependencies.yml`** - "PR Dependency Check". Reads `depends on
  astubbs/parallel-consumer#N` lines from the PR body and blocks the child until every parent has
  merged. Produces the **required** check `Check PR Dependencies`, so a stacked PR cannot merge out
  of order.
- **`repo-hygiene.yml`** - cheap repo-wide static checks needing no broker, no Docker and no build.
  **ONE job, `repo: hygiene`, which DISCOVERS rather than enumerates**: it runs
  `bin/check-all.sh --with-tests`, globbing `bin/check-*.sh` and `bin/test-*.sh`. It was one job per
  concern until that shape cost more than it bought - a gate added to `bin/` ran NOWHERE until
  somebody remembered to name it here, and nothing went red, because a check that is not running
  looks exactly like a check that is passing. The job name is still an API: it is a required status
  check, and renaming it silently stops satisfying the ruleset.

  What the lane covers, and why each one is not obvious:

  - **`check-shell-sigpipe.sh`** fails any script piping into `grep -q` under `pipefail`. That
    construct can report failure *because* it matched, once the producer still has more than a pipe
    buffer left to write when `grep` exits - so it passes every small fixture and surfaces only in
    production. ShellCheck does not detect it. Full mechanism in the script header and in
    [`solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`](solutions/workflow-issues/a-check-that-reports-success-without-having-run.md).
  - **`check-shell-hazards.sh`** fails coreutils flags that mean different things on GNU and BSD -
    `stat -f` exits 1 on GNU while printing filesystem prose to stdout, `sed -i` takes its suffix
    attached on one and as the next argument on the other. Same class as the above, and it should
    eventually absorb it ([`inflight/ci-fold-sigpipe-into-shell-hazards.md`](inflight/ci-fold-sigpipe-into-shell-hazards.md)).
  - **`test-rename-packages.sh`** self-tests the package-rename tool - run by hand once per branch,
    exactly the shape that rots unnoticed between the day it is written and the day a whole rename
    depends on it.
  - **`check-action-versions.sh`** keeps every GitHub Action pinned to one version across workflows.
  - **`check-inflight-tags.sh`** validates every `docs/inflight/` note's tags against the closed sets
    in `bin/lib/inflight-tags.sh` ([`docs/inflight/AGENTS.md`](inflight/AGENTS.md) owns their
    meanings), failing the commit that mistyped a tag rather than leaving the next session to notice
    the index could not place a note.
  - **`check-test-log-config.sh`** pins the four library modules' `logback-test.xml` to the
    `pc.log.level`-driven harness and fails any logger committed at debug/trace - otherwise silent:
    the log floods, nothing goes red, and the volume alone has timed tests out.

  **A JDK is set up even though nothing here builds**: `bin/test-check-infer.sh` FAILS rather than
  skips when no JDK resolves under `CI`, so without it the lane would pass only because the runner
  image happens to ship one.

  **Adding a required check is ordered, and getting it wrong blocks every open PR.** A required
  context that no run produces leaves every PR pending until it merges master. So removals go first
  (they can never block anything) and additions wait until the job exists on master - which is why
  `shell: sigpipe` and `workflows: action versions` were dropped from the ruleset in the same change
  that deleted those jobs. **Neither name exists any more, and neither is required.** `repo: hygiene`,
  the single lane that replaced them (and the rest of `repo-hygiene.yml`'s old per-concern jobs), is
  **also absent from the required list** as of the last live check -
  `gh api repos/astubbs/parallel-consumer/rulesets/15055005` enumerates every required context by
  name and `repo: hygiene` is not among them. Whether that is the pending "addition" this paragraph
  describes, still waiting for its turn, or simply missed when the jobs were collapsed, is not
  settled here - confirm against the live ruleset rather than assuming either.
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

### `CodeQL` is a required check that no workflow file produces

**Every required status check in the master ruleset is produced by a workflow YAML in
`.github/workflows/` - except one.** `CodeQL` is required and no workflow file produces it, because
the scan is GitHub's **code-scanning default setup** - configured in repository settings, stored
outside the tree, and therefore invisible to every check-by-name convention the rest of this document
relies on. (Grepping the directory for `CodeQL` does now hit something: the note in its `README.md`,
which exists to catch exactly that search.) Two API calls are the only way to see what it is doing:

```bash
gh api repos/astubbs/parallel-consumer/code-scanning/default-setup
gh api "repos/astubbs/parallel-consumer/code-scanning/analyses?per_page=5"
```

It was enabled on 2026-07-24 over `actions`, `java-kotlin` and `python`, on the `default` query suite
and the `remote` threat model, weekly plus every push and PR. The analyses list is the half worth
reading: it shows one analysis **per language per PR head**, so a green `CodeQL` is an aggregate of
three, and it runs `build-mode: none` - no Maven build, so the scan costs nothing in `maven.yml`'s
lane and cannot be broken by a build failure there.

**Do not add an advanced CodeQL workflow to bring this back into the tree.** The two setups are
mutually exclusive: GitHub's own switch procedure is to disable default setup *first*, so a workflow
file added alongside it does not become a second opinion. Trading down is the real cost - a
hand-written matrix has to re-declare `actions` and `python`, which default setup covers for free,
and then be maintained. astubbs#1 was exactly that proposal, opened in 2021 against
`github/codeql-action@v1`; it was overtaken by default setup and closed by becoming this section.

### Which checks are required, and why several deliberately are not

**The required list is repository settings, not tree state**, so no PR can change it and nothing goes
red when it drifts. Read it rather than trusting any list written down here:

```bash
gh api repos/astubbs/parallel-consumer/rulesets --jq '.[] | "\(.id) \(.name)"'
gh api repos/astubbs/parallel-consumer/rulesets/<id> \
  --jq '.rules[] | select(.type=="required_status_checks") | .parameters.required_status_checks[].context'
```

**A required context that no run produces leaves every PR pending until it merges master**, so a new
check is only promoted once the job that emits it is already on master. The same ordering governs a
renamed job: the ruleset keeps the old name, which then blocks nothing visibly and passes never. That
is how a bare `spotbugs` context outlived the job that became `static: spotbugs` and sat required with
no producer until 2026-08-26. **A skip does not satisfy a required check either** - it waits - so a
job that can legitimately have nothing in scope should report success rather than skip before anyone
requires it.

**These are deliberately NOT required, and each would break something if promoted:**

| Check | Why not |
|---|---|
| `Mutation Tests (PIT, PR-scoped)` | **Requiring it would be vacuous.** The job is `continue-on-error: true`, so its check-run *conclusion* is success even when the step fails - the row reddens, and a required check reads the conclusion. The property worth gating is that the lane could not measure anything, which `bin/ci-mutation-test.sh` signals through its own exit codes rather than by finding survivors. Gating that means removing `continue-on-error` first, which is a code change, not a ruleset edit |
| `Performance (optional)` | The self-hosted lane is dispatch-only, so this context is never produced on a PR. Requiring it would block every PR permanently |
| `compat: kafka 4.x (experimental)` | Disabled with `if: false` |
| `full build (master)` | Push-only; never produced on a PR |
| `Analyze (actions)`, `Analyze (java-kotlin)`, `Analyze (python)` | The `CodeQL` aggregate above is already required and covers all three |

This table is the durable half of a note that has been retired: the three ruleset edits it tracked -
adding `Chaos Pain Suite` once it reached master, adding `static: infer`, and removing the orphaned
`spotbugs` - were made on 2026-08-26. The reasoning survives it, because the failure it prevents is
someone re-proposing one of the rows above and re-deriving why it does not work.

**`Chaos Pain Suite` was promoted without waiting for a bake-in period**, deliberately and against
the advice recorded at the time: it had been red for much of 2026-08-25 on a timing bound, and the
detector responsible was demoted to non-gating only the day after. The owner's call was that a red
chaos check is a real finding and will be fixed as one. Read a red there as a bug to investigate, not
as the gate misbehaving.

### The three `claude*` workflows, and which is which

Their filenames do not distinguish them well - `claude-code-review.yml` is the one file that does
**not** review - so read this before editing any of them. One judges, two review:

| File | Runs when | What it is |
|---|---|---|
| `claude-code-review.yml` | every PR push | **the gate.** Judges; never reviews |
| `claude-code-review-dispatch.yml` | `workflow_dispatch` | **the dispatched reviewer** |
| `claude.yml` | an `@claude` comment | **the comment reviewer**, and the general mention handler |

- **`claude-code-review.yml`** - the **review gate**, not the reviewer. It runs on every PR push,
  invokes no Claude and costs nothing, and asserts exactly one thing - stated once, under
  ["The gate asks..."](#the-gate-asks-has-this-pr-been-reviewed-not-was-every-commit-reviewed)
  below, and deliberately not repeated here. It produces the required check `claude-review`, so
  the job name is an API here as it is in `repo-hygiene.yml`.
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
  satisfies it, so answering a `@claude` question on a PR turns `claude-review` green. See "What
  the gate proves" below.
- **`chaos-pain.yml`** - on-demand seeded chaos hunts (`workflow_dispatch`, inputs `seed`/`reps`).
  See [`docs/testing.md`](testing.md). It declares no `concurrency` and queues on a runner like any
  other job - see ["The box decides its own concurrency"](#the-box-decides-its-own-concurrency).

### Chaos does not need the self-hosted box

**Measured head-to-head on the same commit (`d8beb162f`, 2026-08-26): hosted `ubuntu-latest`
13m59s, self-hosted `highcpu` 12m16s, both green.** The Chaos Pain Suite ran on the box on the
premise that it needed many real cores to provoke anything; 14% of wall-clock does not buy a shared
physical machine, and the sharing is what produced every scheduling problem the `highcpu` lane has
had. A hosted runner gives each job **its own VM**, so co-residency cannot occur there at all.

`bin/chaos-test.sh` needed no change to move, which is the tell that the premise was never
load-bearing: it passes no `forkCount` and no `-Dparallel-tests`, so the suite was never configured
to exploit the cores it was placed there for.

It now runs as `Chaos Pain Suite` in `maven.yml`, and it is **gating** - a chaos RED is a
real finding. Do not re-add it to the self-hosted lane: chaos would then run twice per PR, and the
second copy is the one that has to be scheduled against a finite box. On-demand seeded hunts stay in
`chaos-pain.yml`.

**Caveat carried forward, deliberately:** the move was settled on one head-to-head run, and the
hosted job's per-scenario test counts were not read (the job-log endpoint returned empty). Duration
rules out a zero-scenario run - that is build-only, ~2 minutes - but the standing rule still applies:
read the job's own `Chaos suite timing` summary and its zero-tests-selected warning before trusting a
green.

### The box decides its own concurrency

**No workflow caps how many jobs run on the highcpu box. How many run at once is the box's own
decision, made by how many runner processes it runs** - six today. Nothing in this repository asserts
a limit, and nothing should: a workflow file cannot know the machine's capacity, and the moment it
claims to, the claim rots silently the next time a runner is added or removed. The lever is on the
box, and it needs no change here.

**Nothing on the box is triggered by a pull request any more** - all three workflows that target
`highcpu` are `workflow_dispatch` only - so the scheduling question is now much smaller than it was.
What cancellation remains is the ordinary kind: a group keyed per-suite and per-ref supersedes an
older run of the same suite on the same ref. It is keyed per-suite rather than workflow-wide to avoid
a head-of-line stall, because one workflow-wide group makes a new run wait out the slowest-dying
maven JVM of the old one before anything starts.

#### Why a `concurrency` group is not a mutex

Worth stating because the repository tried it and the failure was expensive. Between 2026-08-25 and
2026-08-26 every job that could occupy the box - both per-PR suites, on-demand chaos, and the full
mutation sweep - shared one repo-wide `highcpu-box-exclusive` group with `cancel-in-progress: false`,
intended as a box mutex.

**A concurrency group deduplicates; it does not queue.** GitHub keeps one run in progress and **at
most one pending** per group, and DISCARDS anything that arrives behind that. With several branches
active, each new push therefore evicted whichever run was already waiting. Measured over the 50
minutes after it landed (2026-08-26, 01:03Z-01:53Z, 16 runs across 9 branches): **26 of 32 jobs never
executed a single step** - chaos 12 of 16 evicted while pending, Performance 14 of 16 - while five of
the six runners sat idle. A tripwire that runs on a quarter of pushes, chosen by whoever pushed last,
is worth less than one that occasionally shares a box.

The co-residency reds that motivated the mutex were `~154s lagStagnation` against a 150s bound - the
bound meeting the load rather than a defect - and that detector was demoted to non-gating in the same
pull request (`ProgressProbe.recordLagStagnation` now calls `observe` rather than recording a
violation). The problem was fixed in the instrument, where it belonged; see
[`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md).

**So: never reach for a `concurrency` group to protect a shared physical resource.** Express capacity
where capacity lives - the number of runners serving the label.

#### Reading a cancelled or absent chaos check

**A CANCELLED check is rendered as a FAILING one.** `gh pr checks` prints `conclusion=cancelled` as
`fail`, so a red `Performance (optional)` or `Chaos Pain Suite` may mean *it never ran* rather than
that something regressed - check `conclusion` before believing it. A cancelled chaos check means
**not measured**: neither a pass nor a failure.

The chaos job writes a `Chaos measurement provenance` block into its job summary naming the commit it
measured. If that commit is not the PR's head, the current code has not been through the suite -
normally because a newer push superseded the run, in which case that push has a run of its own.
Re-run on demand with `gh workflow run chaos-pain.yml -R astubbs/parallel-consumer`.

Whether six concurrent chaos suites is in fact too many for the box is now an open question about
runner count rather than about this workflow:
[`docs/inflight/ci-highcpu-box-concurrency-is-runner-count.md`](inflight/ci-highcpu-box-concurrency-is-runner-count.md).
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
| **Gate** (`claude-code-review.yml`) | every PR push | none - no Claude, no JDK, no build | the required check `claude-review` |
| **Reviewer** (`claude-code-review-dispatch.yml`) | when dispatched | a full review | the review itself |

**The second reviewer, Codex, is on request too - comment `@codex review` on the PR.** It reviewed
every push automatically until 2026-08-19, and that setting lives in the Codex account settings
rather than in this repo, so nothing here changes when it is toggled. Turning it off is why the
review gate's coverage gap is now accepted rather than covered - see
["The gate asks..."](#the-gate-asks-has-this-pr-been-reviewed-not-was-every-commit-reviewed).

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
<!-- file-refs: N/A - a path inside claude-code-action, not this repo -->

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
it says: nobody has asked for a review yet. The fix is to ask for one when the work is ready. Two
distinct reds, each saying which it is:

- **never reviewed** - `claude[bot]` has not commented. Normal for a new or in-progress PR.
- **the reviewer left unticked boxes** - it started and did not finish, so ask again.

(A third red is possible and says so plainly: the PR's head is in a fork. See "A fork PR cannot
turn the gate green" below.)

### The gate asks "has this PR been reviewed?", not "was every commit reviewed?"

<!-- CANONICAL: the gate contract. Nowhere else states what satisfies the gate - everything else
     links here. Before you change this paragraph, find the mentions that link to it:
     grep -rn "claude-review" . --exclude-dir=.git --exclude-dir=target -->

**Any finished `claude[bot]` review on the PR satisfies it**, whenever it was posted. A review of
the first commit therefore vouches for the twentieth, and that is a deliberate reversal of the
rule this repo shipped first.

**This paragraph is the only statement of that contract.** It was restated in nine files at one
point, in nine slightly different sentences, and four of them were still describing the *previous*
contract weeks after it changed - caught one at a time by four separate review rounds. Every other
mention now links here instead of paraphrasing.

It is a real trade, so it is worth knowing why it was made rather than discovering the cost by
surprise. Strict is the stronger guarantee - a review of commit N genuinely does not vouch for
commit N+1 - and it was abandoned not because it was wrong but because of **what enforcing it
cost**, and because the per-commit coverage it protected arrived from elsewhere at the time: a
separate auto-reviewer read every push. What freshness cost was the **timestamp machinery**: a
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

**The assumption that made leniency safe - that the auto-review keeps happening - expired on
2026-08-19, and the answer is to accept the gap rather than close it.** That auto-review was
switched off because a full review of every push spends more than the coverage is worth; it is now
on request only, like the dispatched reviewer above. So a PR reviewed at commit 1 can merge at
commit 20 with commits 2-20 read by nobody, and every check stays green.

**Do not answer that by making a push invalidate the Claude review.** That moves the per-push spend
onto the more expensive reviewer, which is the cost both splits were made to remove - the freshness
rule was parked and the reviewer was taken off `pull_request` for the same reason. The condition
that reopens the decision is the price of a review falling by roughly two orders of magnitude, not
the rediscovery that the gap exists. Ask for a review when a PR is ready, and ask again after a
push that changes what a reviewer already looked at; that judgement is deliberately a person's, not
a gate's. The reasoning and the archived strict implementation are in
[`docs/inflight/ci-strict-review-gate-freshness.md`](inflight/ci-strict-review-gate-freshness.md).
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


### The second required check: `review: human LGTM`

A separate job, and a separate required check, asserting one thing: the repository owner has left a
review whose body contains `lgtm` - any case, anywhere in the body, on any commit. `bin/check-human-lgtm.sh`
owns the rule and states it in full.

It is a **memory aid, not a security control.** The owner is both the subject of the assertion and the
person who wants the merge, so it stops nobody who wants not to be stopped. What it buys is that
"have I read this one myself yet?" becomes a red check instead of something to remember across a
dozen open PRs.

**Every PR, with no bot exemption** - unlike the automated half, which skips bot-raised PRs. The two
assert different things: a Dependabot PR does not need an *automated review*, but it is still a change
going in, and the requirement is that the owner reviews everything himself. Having no guard also means
there is no job to skip, and a skipped job would otherwise satisfy the required check having asserted
nothing.

It is deliberately a second job rather than a second step in `claude-review`, so the checks list says
*which* half is missing without opening anything - and so `claude-review`, a required check matched by
name in the master ruleset, did not have to be renamed. It is **not head-sensitive**, matching the
automated half: an LGTM on any commit counts for the whole PR, permanently.

## Self-hosted lanes

Setup and operation: [`docs/self-hosted-runner.md`](self-hosted-runner.md). None of these gate
merging - they exist for speed and for work too heavy for a 2-core hosted runner. All are
**skipped for PRs from forks** (`head.repo.full_name == github.repository`), because a fork PR must
never run on our own hardware.

**`highcpu` is the only self-hosted label.** Declare labels in
[`.github/actionlint.yaml`](../.github/actionlint.yaml) or actionlint flags them.

- `pr-highcpu-fast-feedback.yml` **was deleted on 2026-08-26**, and is named here because a lane
  that used to exist is exactly what someone greps for. Both suites it carried had hosted
  equivalents: chaos moved to the hosted gate (see
  ["Chaos does not need the self-hosted box"](#chaos-does-not-need-the-self-hosted-box)), and its
  `Performance (optional)` check ran the *same* `bin/performance-test.sh` as `maven.yml`'s
  **required** `Performance Tests` - a non-gating duplicate of a gating check. What remained was an
  on-demand benchmark nobody dispatched, so it was not worth a file. Read it at
  `git show 5ae0cbfe4:.github/workflows/pr-highcpu-fast-feedback.yml`.
- `mutation-full-sweep.yml` - **nightly plus dispatch**: the whole-project PIT sweep
  (`bin/ci-mutation-test.sh -Dverbose=true -Dthreads=N`). The PR-scoped mutation job in `maven.yml`
  only covers classes changed against the base; this is its exhaustive counterpart.

### A green mutation tick usually means "measured nothing" - read the exit code

`bin/ci-mutation-test.sh` answers in its exit code, and the script's own header owns the contract:
**0** scored mutants, **2** could not run (a scope regex matching nothing in the tree, or PIT
producing no statistics / zero mutants), **3** nothing in scope. Measured over the last 40
`maven.yml` PR runs: 40 passes, zero mutants scored - the lane is correctly narrow, not broken. Only
a **0** is evidence about test quality. `bin/test-ci-mutation-test.sh` guards the contract and runs
in the lane ahead of it. The scope, the exclusions and the ranked widening list are in
[`docs/inflight/ci-mutation-testing.md`](inflight/ci-mutation-testing.md); whether a skip should
render grey rather than green is an open decision in
[`docs/inflight/ci-mutation-lane-skip-reads-as-a-pass.md`](inflight/ci-mutation-lane-skip-reads-as-a-pass.md).

**There is almost no scheduled build, deliberately.** Every suite worth re-running is already a
required check on each PR and runs again on every push to master, so a cron lane would usually only
repeat covered work. **Do not add a lane for suites the gate already covers.** The test to apply to
any proposed scheduled lane is: **does time alone change the answer?**

Two lanes are scheduled, and they clear that bar in different ways:

- **`dependency-audit.yml`** passes the test outright. It runs no *suite*, and what it catches - a
  new advisory published against an unchanged dependency tree - is purely a function of elapsed
  time, which no PR-triggered check can ever see.
- **`mutation-full-sweep.yml`** (nightly, 2026-08-26) **fails the test on paper and is a deliberate
  exception.** A mutation score changes when the code changes, not when time passes, so the honest
  trigger is per-merge - and per-merge is unusable at this repository's merge rate. Measured over the
  last 60 master commits: up to 32 in a day, a **median gap of 0 minutes** (squash-merges arrive in
  bursts), and **83% of gaps shorter than the sweep's own 31m27s job-elapsed runtime** (job elapsed
  is the right clock here: it is how long a push has to arrive within to kill a running sweep; the
  often-quoted 21m55s is only the PIT phase, and n=1). Per-push therefore either
  piles dozens of concurrent sweeps onto one box, or - with a cancelling group - has four in five
  killed before they finish, which is precisely the never-completes failure the lane was rebuilt to
  escape. Since master moves every day, a nightly is in practice "after today's merges". The accepted
  cost is that a regression is attributed to a **date** rather than a merge, so `git log` over that
  day is the first step of triage.

The exception is written here rather than left to contradict the rule silently. Note what makes it
one: not that a schedule is convenient, but that the correct trigger was measured and found
unusable. The workflow's `on:` block carries the same reasoning from its side.

**Before pinning a job to a self-hosted label, confirm a runner serves it** -
`gh api repos/astubbs/parallel-consumer/actions/runners` lists each runner's labels and online
status. A job pinned to a label nothing advertises does not fail; it queues until GitHub cancels
it, so the lane reports nothing at all and looks merely quiet.

**Beware: `performance` names two unrelated things.** It is the *test suite*
(`bin/performance-test.sh`, the required **Performance Tests** check, on every PR from `maven.yml`,
`ubuntu-latest`). It is **not** a runner label - the only self-hosted label is `highcpu`.
