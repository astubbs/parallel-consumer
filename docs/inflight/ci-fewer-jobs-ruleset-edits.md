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
`static: analysis` - see "The static-analysis fold" below. A tenth, `dependency-audit.yml`'s
`deps: whole-tree CVE scan`, became a fourth step of `scan: repo` - see "The CVE fold" below; that
workflow keeps its `schedule` and `workflow_dispatch` triggers and is not deleted. An eleventh,
`maven.yml`'s `Mutation Tests (PIT, PR-scoped)`, became the fifth and last step of `scan: repo` -
and is the one of these eleven folds that owes **no** ruleset edit at all; see "The PIT fold owes no
ruleset edit" below, which exists so a reader diffing the checks list does not go looking for the
entry that is deliberately absent.
All ten old names are still **required status-check contexts in the master ruleset**, and the ruleset is repository settings, not tree state - no PR can change it
([`docs/ci.md`](../ci.md), "The required list is repository settings, not tree state").
<!-- file-refs: N/A - copyright.yml and pr-checklist.yml are named as the files this work deleted; the record of each is its deleting commit, `git log --diff-filter=D -- .github/workflows/copyright.yml .github/workflows/pr-checklist.yml` -->

## The edit

Remove these ten contexts from the master ruleset's `required_status_checks`:

- `Copyright header check`
- `quarantine: audit`
- `docs data: audit`
- `PR Checklist`
- `dups: clones`
- `dups: similarity`
- `deps: vulnerabilities`
- `static: infer`
- `static: spotbugs`
- `deps: whole-tree CVE scan`

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
deleting PR's own checks list shows the ten contexts as expected-but-missing until the ruleset
drops them, which is the intended tell that the edit is still owed, not a fault in that PR.

**The add has the opposite ordering.** A new required context that no master run has produced
leaves every PR pending ([`docs/ci.md`](../ci.md), "Which checks are required": a check is promoted
only once the job that emits it is already on master). Both `scan: repo` and `static: analysis` are
`pull_request`-only, so strictly neither ever runs *on* master; the condition that matters is that
the job definition is on master, so every PR opened afterwards produces the context. Sequence at the
merge, then:

1. Merge the deleting PR.
2. In the same sitting, remove the ten old contexts.
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

**A name-matching consumer had to be repaired, and it is the class to check on every fold**:
`bin/check-pr-analysis-surfaces.sh` filters check runs by name. That regex reads the CHECK name, not
the step names, so a fold silently drops a surface from its listing - nothing fails, the row just
stops appearing. It read `test("Mutation|spotbugs|racerd|CVE|Quarantine")`, in which `racerd` had
already been dead since the job became `static: infer`; it now names the live checks,
`test("Mutation|static: analysis|scan: repo|CVE|Quarantine")`, and carries a comment saying to
re-read it whenever a job is renamed or folded.

## The PIT fold owes no ruleset edit - and that is the point of this section

`Mutation Tests (PIT, PR-scoped)` is the eleventh check the `ci-fewer-jobs` folds remove from a PR's
checks list, and the only one whose name is **not** on the removal list above. It was never a required
context: [`docs/ci.md`](../ci.md)'s "These are deliberately NOT required" table has always carried a
row for it, because the job was `continue-on-error: true` and a required check reads the *conclusion*
- which `continue-on-error` makes success even when the step fails. Requiring it would have gated
nothing. So there is nothing to remove, and nothing to add.

Written down because the absence is indistinguishable from an oversight. Somebody comparing a PR's
checks list before and after the merge sees eleven rows go and ten names accounted for here; without
this paragraph the eleventh reads as a missed edit, and the natural repair - adding
`Mutation Tests (PIT, PR-scoped)` to a ruleset that never had it - fails with a context nothing has
ever produced.

What the fold had to carry:

- **`continue-on-error: true` moved from the JOB to each of the two steps.** `scan: repo` **is** a
  required check, so folding an advisory lane into it bare would have promoted a mutation verdict to
  a merge gate by accident - the single most consequential thing this fold could have got wrong. On
  the steps, the semantics are identical to before: a red step renders red in the log and the step
  list, and cannot fail the check. Every exit-code branch of the run step (`0` scored, `3` nothing in
  scope, `2` broken lane -> `exit 1`, anything else -> `exit "$rc"`) is copied unchanged, `set +e`
  handling included.
- **A step-level `timeout-minutes: 20`,** where the job held 30. PIT is ~11s when nothing is in scope
  and up to half an hour when something is; the job bound is now 45 and is only a backstop. The step
  bound is the load-bearing one: it stops a mutating run leaving the scanner and CVE results waiting,
  and a timed-out *step* still lets the job finish and report, where the old timed-out *job* reported
  nothing at all (the sighting in [`test-untracked-ci-flakes.md`](test-untracked-ci-flakes.md)).
- **The JDK and the Maven cache restore came out from behind the credentials guard.** They were
  guarded because the CVE block was the only thing in the job that built; PIT runs on every PR
  including fork and Dependabot ones, so leaving them guarded would have handed the mutation lane a
  cold repository on exactly the PRs that get no CVE scan. Every step that talks to OSS Index is
  still guarded, and `server-id: ossindex` is inert when those steps skip.
- **`fetch-depth: 0` was already there** for the two duplication tools, which is what the mutation
  lane needs to diff against the PR base; it must not be narrowed, because an unresolvable base ref
  makes `bin/ci-mutation-test.sh` fall back to the *full* glob.
- **The self-test still runs first,** `continue-on-error` like the run step - it also runs
  non-advisory inside `repo: hygiene`'s `bin/check-all.sh --with-tests` sweep, which is required, so
  nothing is lost by not gating on it twice here.

**The name-matching consumer needs a second repair, and it is in `bin/` rather than in a
workflow**: `bin/check-pr-analysis-surfaces.sh` lists check runs through
`test("Mutation|static: analysis|scan: repo|CVE|Quarantine")`. After this fold no check run is named
`Mutation Tests (PIT, PR-scoped)`, so the `Mutation` alternative matches nothing - it is `racerd`
again, one fold later. Nothing breaks: `scan: repo` is already in the pattern and is now the row
that carries the PIT job summary, so the surface is still listed, under the batched name. The fix is
to drop the now-dead `Mutation` alternative, leaving
`test("static: analysis|scan: repo|CVE|Quarantine")`, and to say in the comment above it that the
PIT summary now hangs off `scan: repo`.

## The CVE fold

`deps: whole-tree CVE scan` was the whole of `dependency-audit.yml`'s `ossindex` job, and that
workflow's `pull_request` trigger was its only PR-time producer. The trigger is gone and the job is
now a fourth step of `scan: repo`; the workflow itself is **not deleted** - it keeps `schedule` and
`workflow_dispatch`, which is the half no PR can cover (an unchanged tree acquiring a new advisory).
So the name `deps: whole-tree CVE scan` still exists in the tree, as both a job in that workflow and
a step in this one, and is still on the removal list above: no PR run produces it any more, and a
required context nothing produces on a PR leaves every PR pending.

What the fold had to carry:

- **The credentials guard, verbatim and with its semantics unchanged.** The standalone job's `if:`
  skipped for fork PRs and for Dependabot-actor runs, because in both cases `secrets.*` resolves
  empty and the preflight would red every time by design. A job-level `if:` cannot skip four steps
  of a job that has other work to do, so the same expression is now the job-level env var
  `CVE_SCAN_CREDENTIALS_PRESENT` and every CVE step reads
  `!cancelled() && env.CVE_SCAN_CREDENTIALS_PRESENT == 'true'`. Skip, not warn and not red: those
  PRs get no CVE steps at all, exactly as they got no job before. A *present-but-empty* secret is
  still red, from the preflight - that case is a mistake, not a design.
- **The setup the scanners did not need**: `needs: prepare-deps`, `actions/setup-java` with
  `server-id: ossindex` (the plugin's `authId`, read from the generated settings.xml at Maven
  runtime), and the restore-only Maven cache step. All three sit with the CVE steps at the end of
  the job and behind the same guard, so a fork PR pays none of it.
- **Last, not first.** It is the only step here that builds, so the cheap signals still land first
  in the log, and the `dups: clones` comment about `target/` not being on disk stays true - the
  Maven step is below it, not above.
- **`timeout-minutes: 25`** where the scanners held 10 and the audit job held 20.
- **The exposure the standalone job avoided, and what contains it here.** That job deliberately held
  `contents: read` only, because it runs PR-authored build code in the same job as the OSS Index
  token. `scan: repo` also holds `pull-requests: write`, for the three tools that post comments.
  What contains it is the guard itself: the CVE steps run only for a branch in *this* repository,
  pushed by somebody who already has write access, so the build code is not attacker-supplied the
  way a fork PR's would be. The OSS Index secrets stay in `env:` on the two steps that need them and
  are not visible to the pinned third-party actions.
- **The steps are now duplicated between two files.** `dependency-audit.yml` still owns the
  reasoning - what the lane covers, why findings gate, the two different reds, the
  did-it-actually-scan guard - and `maven.yml` points at it rather than restating it. The commands
  themselves are copied, because sharing them needs a composite action and a third file. Nothing
  checks that the copies agree; both file headers say so.

This note tracks only the owed edits. Once the live ruleset lists `scan: repo` and
`static: analysis` and none of the ten old contexts, nothing here is both true and unowned elsewhere
- the reasoning is in [`docs/ci.md`](../ci.md), the `repo-hygiene.yml` and `dependency-audit.yml`
headers, and the `scan: repo` and `static: analysis` jobs' own comments.
