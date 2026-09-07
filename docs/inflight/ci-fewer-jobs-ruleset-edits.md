# Ruleset edits owed by the folds into `repo: hygiene` and `scan: repo`

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

Three standalone jobs were deleted because `repo: hygiene` already ran every gate they ran, via
`bin/check-all.sh --with-tests`'s glob, on every PR: `Copyright header check` (the whole of
`.github/workflows/copyright.yml`), and `.github/workflows/maven.yml`'s `quarantine: audit` and
`docs data: audit`. A fourth, `PR Checklist` (the whole of `.github/workflows/pr-checklist.yml`),
was deleted because its steps could run as the tail of the same job - see "Why hygiene hosts the
checklist" below. Three more, `maven.yml`'s no-build scanners `dups: clones`, `dups: similarity` and
`deps: vulnerabilities`, became steps of one new job, `scan: repo` - see "The scanner fold" below.
All seven old names are still **required status-check contexts in the master ruleset**, and the ruleset is repository settings, not tree state - no PR can change it
([`docs/ci.md`](../ci.md), "The required list is repository settings, not tree state").
<!-- file-refs: N/A - copyright.yml and pr-checklist.yml are named as the files this work deleted; the record of each is its deleting commit, `git log --diff-filter=D -- .github/workflows/copyright.yml .github/workflows/pr-checklist.yml` -->

## The edit

Remove these seven contexts from the master ruleset's `required_status_checks`:

- `Copyright header check`
- `quarantine: audit`
- `docs data: audit`
- `PR Checklist`
- `dups: clones`
- `dups: similarity`
- `deps: vulnerabilities`

Add one:

- `scan: repo`

`repo: hygiene` needs no add - it is already in the required list (verified live 2026-09-07 with
`gh api repos/astubbs/parallel-consumer/rules/branches/master`; that command is the answer, not this
sentence). `scan: repo` is new, so it is not.

## When: at the merge of the deleting PR, not before and not after

A required context nothing produces leaves every PR **pending** - it never fails, it never passes.

- **Before the merge** the four jobs still run on every other open PR, so dropping the contexts
  early only widens the window in which a broken header, a drifted registry or an unresolved
  checklist could merge on a PR opened before the fold.
- **After the merge** no run produces them, so every PR in the repository pends until somebody edits
  the ruleset. That is the `spotbugs` incident again ([`docs/ci.md`](../ci.md), "Which checks are
  required").

So: edit the ruleset in the same sitting as the merge. Only one merge is exposed either way - the
deleting PR's own checks list shows the seven contexts as expected-but-missing until the ruleset
drops them, which is the intended tell that the edit is still owed, not a fault in that PR.

**The add has the opposite ordering.** A new required context that no master run has produced
leaves every PR pending ([`docs/ci.md`](../ci.md), "Which checks are required": a check is promoted
only once the job that emits it is already on master). `scan: repo` is `pull_request`-only, so
strictly it never runs *on* master; the condition that matters is that the job definition is on
master, so every PR opened afterwards produces the context. Sequence at the merge, then:

1. Merge the deleting PR.
2. In the same sitting, remove the seven old contexts.
3. Add `scan: repo` only after the merge has landed and a PR run has produced the context - the
   first PR to rebase onto the merged master shows `scan: repo` in its checks list; that is the
   evidence. Until then the three scanners gate nothing, which is a window measured in one PR's
   CI run, and is preferable to every PR pending on a context nothing yet produces.

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

This note tracks only the owed edits. Once the live ruleset lists `scan: repo` and none of the
seven old contexts, nothing here is both true and unowned elsewhere - the reasoning is in
[`docs/ci.md`](../ci.md), the `repo-hygiene.yml` header and the `scan: repo` job's own comments.
