# Ruleset edits owed by the folds into `repo: hygiene`, `scan: repo` and `static: analysis`

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

Three standalone jobs were deleted because `repo: hygiene` already ran every gate they ran, via
`bin/check-all.sh --with-tests`'s glob, on every PR: `Copyright header check` (the whole of
`.github/workflows/copyright.yml`), and `.github/workflows/maven.yml`'s `quarantine: audit` and
`docs data: audit`. A fourth, `PR Checklist` (the whole of `.github/workflows/pr-checklist.yml`),
was deleted because its steps could run as the tail of the same job - see "Why hygiene hosts the
checklist" below. Three more, `maven.yml`'s no-build scanners `dups: clones`, `dups: similarity` and
`deps: vulnerabilities`, became steps of one new job, `scan: repo` - see "The scanner fold" below. Two more, `maven.yml`'s
build-dependent static analysers `static: infer` and `static: spotbugs`, became steps of one new job,
`static: analysis` - see "The static-analysis fold" below.
All nine old names are still **required status-check contexts in the master ruleset**, and the ruleset is repository settings, not tree state - no PR can change it
([`docs/ci.md`](../ci.md), "The required list is repository settings, not tree state").
<!-- file-refs: N/A - copyright.yml and pr-checklist.yml are named as the files this work deleted; the record of each is its deleting commit, `git log --diff-filter=D -- .github/workflows/copyright.yml .github/workflows/pr-checklist.yml` -->

## The edit

Remove these nine contexts from the master ruleset's `required_status_checks`:

- `Copyright header check`
- `quarantine: audit`
- `docs data: audit`
- `PR Checklist`
- `dups: clones`
- `dups: similarity`
- `deps: vulnerabilities`
- `static: infer`
- `static: spotbugs`

Add two:

- `scan: repo`
- `static: analysis`

`repo: hygiene` needs no add - it is already in the required list (verified live 2026-09-07 with
`gh api repos/astubbs/parallel-consumer/rules/branches/master`; that command is the answer, not this
sentence). `scan: repo` and `static: analysis` are new, so they are not.

## When: at the merge of the deleting PR, not before and not after

A required context nothing produces leaves every PR **pending** - it never fails, it never passes.

- **Before the merge** the four jobs still run on every other open PR, so dropping the contexts
  early only widens the window in which a broken header, a drifted registry or an unresolved
  checklist could merge on a PR opened before the fold.
- **After the merge** no run produces them, so every PR in the repository pends until somebody edits
  the ruleset. That is the `spotbugs` incident again ([`docs/ci.md`](../ci.md), "Which checks are
  required").

So: edit the ruleset in the same sitting as the merge. Only one merge is exposed either way - the
deleting PR's own checks list shows the nine contexts as expected-but-missing until the ruleset
drops them, which is the intended tell that the edit is still owed, not a fault in that PR.

**The add has the opposite ordering.** A new required context that no master run has produced
leaves every PR pending ([`docs/ci.md`](../ci.md), "Which checks are required": a check is promoted
only once the job that emits it is already on master). Both `scan: repo` and `static: analysis` are
`pull_request`-only, so strictly neither ever runs *on* master; the condition that matters is that
the job definition is on master, so every PR opened afterwards produces the context. Sequence at the
merge, then:

1. Merge the deleting PR.
2. In the same sitting, remove the nine old contexts.
3. Add `scan: repo` and `static: analysis` only after the merge has landed and a PR run has produced
   each context - the first PR to rebase onto the merged master shows both in its checks list; that
   is the evidence. Until then the three scanners and the two analysers gate nothing, which is a
   window measured in one PR's CI run, and is preferable to every PR pending on a context nothing
   yet produces.

## Why hygiene hosts the checklist, and not another job

The pairing was decided against the other per-PR candidates, and the reasons are the constraints the
fold had to keep:

- **`Check PR Dependencies`** (`check-dependencies.yml`) stays its own job. Its action needs
  `checks: write`, and it runs on the `closed` trigger so that a parent merging unblocks its
  children. Putting PR-authored `github-script` steps in a job that holds `checks: write` would
  widen a write grant the repo forbids handing to PR code.
- **The two review gates** (`claude-review`, `review: human LGTM`) stay because each produces a
  required check with its own semantics, and a fold would collapse two verdicts into one tick.
- **`repo: hygiene` fits**: both jobs checked out the PR's tree with read-only scope, both ran on
  every PR push, and three of the checklist's named self-test steps (`bin/test-check-issue-refs.sh`,
  `bin/test-check-file-refs.sh`, `bin/test-todo-index.sh`) were already swept by hygiene's glob. What
  the checklist carried that hygiene did not is now explicit in `repo-hygiene.yml`, each with its
  reason at the point it appears: the `edited` pull_request type (the gates read the PR body), a
  concurrency group keyed on the PR number with a SHA fallback (so master pushes never cancel each
  other), `pull-requests: read` (the job's first token use - the shell sweep is still given no
  `GH_TOKEN`), `persist-credentials: false` on the checkout, and `!cancelled()` on every folded step
  so the job reports every verdict rather than stopping at the first red.

## What was checked before deleting, so nobody re-derives it

- The log of the latest successful `Repo Hygiene` run on master showed `check-copyright-headers.sh`,
  `check-quarantine-registry.sh`, `check-quarantine-owners.sh` and `check-docs-data.sh` all `ok`
  (exit 0), and all four self-tests `ok`, with zero CANNOT - the gates were genuinely running there,
  not skipping. `bin/inflight.mjs` cannot answer this; the run log is the only record.
- `copyright.yml` also ran on `push` to master. `repo-hygiene.yml` runs on push to master too, so
  that cadence is kept.
- The quarantine **owner** check needs an authenticated `gh`; `repo: hygiene`'s shell sweep is given
  no token, so there it degrades to advisory. The authenticated run is `quarantine-lane.yml`'s
  fail-fast step, with `github.token`, on every PR push and every master push, under the required
  `tests` check - so an orphaned or overdue owner claim still reds a required check.
- `check-all.sh` used to exit 0 on a CANNOT (exit 2) as long as something else ran. The hygiene job
  now runs `--strict`, under which a CANNOT fails the sweep, and asserts PyYAML and shellcheck are
  present in named steps - so the folded gates cannot silently turn into skips on an image change.
- For the checklist fold: the three dropped self-test steps were confirmed against
  `ls bin/test-*.sh` - each file exists and matches the `bin/test-*.sh` glob `check-all.sh
  --with-tests` iterates, so each already ran in this job. `bin/todo-index.sh --check` does not match
  `bin/check-*`, and cannot be added to the glob because run bare it regenerates the index
  (`docs/inflight/ci-hygiene-gaps.md`), so it stays a named step. Nothing in the sweep consumes the
  checkout credential: the only network `git` is `check-quarantine-owners.sh`'s anonymous fetch of the
  public origin URL into a scratch git dir, and `gh` reads `GH_TOKEN`.

## The scanner fold

`dups: clones`, `dups: similarity` and `deps: vulnerabilities` each checked out the tree without
building it, ran one third-party action, and posted a PR comment - about a minute of work apiece in
three job slots. They are now three steps of `scan: repo` in `maven.yml`, and the constraints the
fold kept are stated as comments on the job itself: `!cancelled()` on every scanner step so the
first red never hides the others, no verdict step and no `continue-on-error` because the job's own
conclusion aggregates the step results, one `fetch-depth: 0` checkout because both duplication
tools compare against the base branch, the clones job's `contents: read` + `pull-requests: write`
grant restated on the merged job and nothing wider, and dependency review first so the cheapest
signal lands first in the log. **The three old job names live on as the step names**, so a red step
reads in the log the way the red check used to, and the references across `docs/inflight/` to what
`dups: clones` or `dups: similarity` found stay accurate - only the check name changed.

Not folded into `repo: hygiene`: hygiene runs PR-authored scripts under a read-only token by design,
and all three scanners need `pull-requests: write` to post. Keeping the write grant in a job that
runs only pinned third-party actions is the same reviewer-isolation line `docs/ci.md` draws for
`Check PR Dependencies`.

## The static-analysis fold

`static: infer` and `static: spotbugs` were the two build-dependent analysers: both `needs:
prepare-deps`, both restored the same Maven cache with a byte-identical `actions/cache/restore` step,
and between them they held two runner slots for work that fits comfortably inside one. They are now
two steps of `static: analysis` in `maven.yml`, and the constraints the fold kept are stated as
comments on the job itself: **Infer first** because it is the cheaper signal by a wide margin, so it
lands in the log before SpotBugs' compile-and-analyse; `!cancelled()` on every analysis step - the
Infer toolchain steps included, so a failed toolchain download cannot hide SpotBugs either - with no
verdict step and no new `continue-on-error`, because the job's own conclusion aggregates the steps;
**one** Maven cache restore where there were two, with the Infer toolchain cache left as its own
steps ahead of the Infer run because it is keyed on the toolchain version, not the pom hash; and
`timeout-minutes: 30` where the two jobs held 15 and 10, sized so the sum plus a toolchain download
on a cache miss has headroom without a hung tool sitting for an hour.

The sum still fits under the critical path: `Unit Tests` is the longest lane, and Infer plus SpotBugs
comes in below it, so the batch does not become the job everything else waits on.

Permissions are the union and nothing wider. Neither job declared job-level permissions, so both
inherited maven.yml's workflow-wide grant; the merged job declares `contents: read`,
`pull-requests: write` (the SpotBugs sticky comment) and `checks: write` (the SpotBugs annotation
action), which drops the workflow's `actions: read` that neither analyser used. The
`continue-on-error: true` on the SpotBugs summary-comment step is untouched - a comment write must
not fail the lane, and that reasoning is recorded at the step.

**The two old job names live on as the step names**, so a red step reads in the log the way the red
check used to, and the prose in `docs/solutions/` and `docs/inflight/` that says what
`static: spotbugs` found stays accurate - only the check name changed.

**One out-of-scope repair is owed**: `bin/check-pr-analysis-surfaces.sh` filters check runs by name
with `test("Mutation|spotbugs|racerd|CVE|Quarantine")`. That regex reads the CHECK name, not the step
names, so once the ruleset carries `static: analysis` the script stops matching the analysis lane and
silently reports one surface fewer. The fix is one alternation:
`test("Mutation|spotbugs|racerd|static: analysis|CVE|Quarantine")`.

This note tracks only the owed edits. Once the live ruleset lists `scan: repo` and
`static: analysis` and none of the nine old contexts, nothing here is both true and unowned elsewhere
- the reasoning is in [`docs/ci.md`](../ci.md), the `repo-hygiene.yml` header and the `scan: repo`
and `static: analysis` jobs' own comments.
