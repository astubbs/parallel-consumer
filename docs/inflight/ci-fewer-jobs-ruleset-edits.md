# Ruleset edits owed by the fold of four gate jobs into `repo: hygiene`

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

Three standalone jobs were deleted because `repo: hygiene` already ran every gate they ran, via
`bin/check-all.sh --with-tests`'s glob, on every PR: `Copyright header check` (the whole of
`.github/workflows/copyright.yml`), and `.github/workflows/maven.yml`'s `quarantine: audit` and
`docs data: audit`. A fourth, `PR Checklist` (the whole of `.github/workflows/pr-checklist.yml`),
was deleted because its steps could run as the tail of the same job - see "Why hygiene hosts the
checklist" below. All four names are still **required status-check contexts in the master
ruleset**, and the ruleset is repository settings, not tree state - no PR can change it
([`docs/ci.md`](../ci.md), "The required list is repository settings, not tree state").
<!-- file-refs: N/A - copyright.yml and pr-checklist.yml are named as the files this work deleted; the record of each is its deleting commit, `git log --diff-filter=D -- .github/workflows/copyright.yml .github/workflows/pr-checklist.yml` -->

## The edit

Remove these four contexts from the master ruleset's `required_status_checks`:

- `Copyright header check`
- `quarantine: audit`
- `docs data: audit`
- `PR Checklist`

Nothing to add: `repo: hygiene` is already in the required list (verified live 2026-09-07 with
`gh api repos/astubbs/parallel-consumer/rules/branches/master`; that command is the answer, not this
sentence).

## When: at the merge of the deleting PR, not before and not after

A required context nothing produces leaves every PR **pending** - it never fails, it never passes.

- **Before the merge** the four jobs still run on every other open PR, so dropping the contexts
  early only widens the window in which a broken header, a drifted registry or an unresolved
  checklist could merge on a PR opened before the fold.
- **After the merge** no run produces them, so every PR in the repository pends until somebody edits
  the ruleset. That is the `spotbugs` incident again ([`docs/ci.md`](../ci.md), "Which checks are
  required").

So: edit the ruleset in the same sitting as the merge. Only one merge is exposed either way - the
deleting PR's own checks list shows the four contexts as expected-but-missing until the ruleset
drops them, which is the intended tell that the edit is still owed, not a fault in that PR.

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

This note tracks only the owed edit. Once the live ruleset no longer lists the four contexts,
nothing here is both true and unowned elsewhere - the reasoning is in [`docs/ci.md`](../ci.md) and
the `repo-hygiene.yml` header.
