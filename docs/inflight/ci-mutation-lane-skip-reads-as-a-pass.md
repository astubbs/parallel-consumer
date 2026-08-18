# The mutation lane's skip renders as a green tick - decision pending

**Open decision, not a bug.** The lane behaves exactly as designed and says so in its job summary.
What is undecided is whether *the checks list* should keep showing a pass when the lane graded
nothing. Raised off astubbs#296, whose only main-code file is
`internal/AbstractParallelEoSStreamProcessor.java`: the lane went green in ~10s having asserted
nothing about it.

The scope decision itself - why `offsets.` only, and the order in which to re-widen - is owned by
[`ci-mutation-testing.md`](ci-mutation-testing.md) and
[`docs/plans/2026-08-03-002-mutation-testing-plan.md`](../plans/2026-08-03-002-mutation-testing-plan.md).
This file owns only the visibility question.

## Recommendation

**Make the skip render as a *skipped* (grey) check rather than a green one; leave
`PIT_DECIDABLE_PACKAGES` alone.** It is the only option that puts the truth where the reader already
looks, and on this repo's shape it is cheaper than the status quo, not dearer.

## What actually fires, and how often

Three exit paths in `bin/ci-mutation-test.sh`; two of them are skips and both exit 0.

| Path | Log line | Last 100 merged PRs | Since the lane landed (astubbs#111) |
|---|---|---|---|
| Nothing to mutate | `no core main-source classes changed` | 86 | 49 |
| Nothing **decidable** to mutate | `changed classes are all outside the decidable packages` | 8 | 5 |
| Actually scored mutants | - | 6 | 4 |

So a green tick means "did no work" on **94 of 100** PRs. The path that prompted this - changed core
main code, declined as undecidable - is **8 of the 14** PRs that touched core main code at all, i.e.
when the lane had something it *could* have looked at, it declined more often than not.

Method (re-run it rather than trusting the table): `gh pr list -R astubbs/parallel-consumer --state
merged --limit 100 --json number,files`, then classify each PR by whether any path matches
`^parallel-consumer-core/src/main/java/.*\.java$` and, of those, whether any matches
`/parallelconsumer/offsets/`. Path-based, so it spans the `io.confluent` -> `bz.stub` rename;
deletions are not filtered out, where the script's `--diff-filter=d` would.

**Red is honest, green is not.** A genuine failure surfaces as a red check row - the job-level
`continue-on-error: true      # advisory - never gates merge` in `.github/workflows/maven.yml` stops
the *workflow run* failing, but the check run's conclusion is still `failure` (confirmed against a
run where the mutation step failed). Only the green side is ambiguous.

## Why the scope is right, and should not move

Every declined class across those 8 PRs was in `internal.` (12) or `state.` (2) - precisely the
argument in the plan's `is close to the worst possible target`: mutants to locks, loop conditions and
timeouts hang rather than dying, and the covering tests are timing-based, so a survivor cannot be
told from a race that did not happen. astubbs#296 is the textbook case, a shutdown race in the
concurrency core. **Widening to `state.` would convert honest silence into unfalsifiable survivors**,
and the plan already fixes the price of admission: a completed `state.` sweep, measured, not argued.

One nuance the package filter cannot express: `internal/Documentation.java` (astubbs#289) is pure
constants, where mutants *would* be decidable and also worthless. The filter is a proxy for
"hang-prone concurrency" and errs toward silence on a couple of harmless classes. Not worth fixing.

## Options considered

- **Leave as-is.** Defensible: the lane never gates, the job summary already says
  `This green tick is not evidence about test quality`, and the public limitation is disclosed in
  `docs/data/testing-evidence.yaml` (`a skipped or narrow run is not a project-wide coverage claim`).
  Rejected because the repo's recurring failure mode is false-green, and a row that is green 94% of
  the time while meaning nothing is the purest instance of it on the board.
- **Widen the decidable set.** Rejected above - it trades a truthful skip for noise.
- **Post a second check run with `conclusion: neutral`.** Rejected: needs `checks: write` plumbing,
  and the plan already established (in removing the duplicate PIT lanes) that an extra tick trains
  people to skim the list, which is how a real red gets missed.
- **Skip the job instead of passing it.** Split the decision into a cheap `mutation-scope` job that
  calls the existing script in a scope-only mode, then guard the real job with `if:` so the row
  renders grey. The check name can carry the reason via `${{ needs... }}` interpolation - the pattern
  `.github/workflows/pr-highcpu-fast-feedback.yml` already uses for
  `format('{0} (optional)', matrix.suite-name)`.

## Why the recommended option is cost-negative

On the 94% skip path the job today pays `actions/setup-java` **and** a Maven cache restore before the
script gets to decide there is nothing to do. Moving the decision in front of those steps deletes
that work from almost every PR. The scope job still needs the `fetch-depth: 0` checkout the diff
depends on; deriving the changed-file list from the PR API instead would drop even that, but would
fork the decidable regex away from `bin/ci-mutation-test.sh`, which must stay its sole owner.

Both skips collapse to one grey row, which is correct - "we graded nothing" is the reader's answer
either way, and the summary still distinguishes them.

## Do this first, regardless of the decision above

The only always-loaded agent-facing warning -
`Confirm the mutation lane scored mutants rather than trusting its tick` in `AGENTS.md` - is the last
bullet of the temporary `IN FLIGHT: rename your branch BEFORE you merge master` section, which that
same file instructs to **delete** once no open branch predates the rename. Deleting the section
deletes the warning. Its wording is rename-specific too ("when its package regex is stale"), which is
narrower than the case that actually fires. Re-home it, or the visibility gap widens on its own.

## Who decides

Antony. The measurement and the reasoning are here; nothing is blocked on further investigation.
