# Skipping CI on docs-only PRs - investigated, parked

<!-- inflight-priority: low -->

**Recommendation: do not do it.** Investigated 2026-08-16/17 and answered, with nothing changed.
Parked here rather than dropped so the next person asking "why does a markdown-only PR run the whole
suite?" gets the answer instead of re-deriving it.

## The prize is smaller than it looks

Measured on astubbs/parallel-consumer#282, a docs-only PR: Integration 7 min, Unit 6 min, Performance
3 min, everything else about a minute each, plus the self-hosted `highcpu` lane at about 6 min.
Roughly **16 parallel hosted-runner minutes, about 8 minutes wall clock**. The 60-minute job timeouts
suggest a far bigger saving than actually exists.

## The trap that makes the obvious approach fatal

Master is protected by a ruleset carrying **21 required checks**, ten of them from `maven.yml` alone.

**A workflow skipped by `paths-ignore` never reports its checks.** They sit at *"Expected, waiting for
status to be reported"* indefinitely, and the PR can never merge. There are no path filters anywhere
in `.github/workflows/` today, so this is not a pattern the repo has already solved somewhere.

## The mechanism that would work, and is unproven

A `changes` gate job plus job-level `if:` on each dependent job. A job skipped by `if:` **does** report
as skipped, which satisfies a required check where `paths-ignore` does not.

**That claim was not verified.** Prove it on a throwaway PR against a protected branch before trusting
it. The test matrix is not an obstacle - all three suites are one `test` job with
`name: "${{ matrix.name }}"`.

## The hard part is the filter, not the YAML

It has to be an allowlist, because several "docs" paths are load-bearing:

- [`docs/quarantined-tests.md`](../quarantined-tests.md) is validated against Java annotations by the
  `quarantine: audit` check.
- [`docs/todo-index.md`](../todo-index.md) is generated, and `--check` fails when it is stale.
- `AGENTS.md`, `.github/` and `bin/` are neither code nor docs, and changing them can change what CI
  itself does.

## Why it is parked rather than queued

It manufactures green checks whose meaning is *"we did not look"* - across ten required checks at
once. That is the same class of defect
[a check that reports success without having run](../solutions/workflow-issues/a-check-that-reports-success-without-having-run.md)
documents, and that `AGENTS.md` already warns about for `bin/ci-mutation-test.sh`'s
"nothing to mutate, skipping". Trading that for eight minutes of wall clock is a bad exchange on a
repo whose recurring failure mode is exactly false-green.

The two places it might pay are the self-hosted `highcpu` lane and the billed `claude-review` - and
the latter is a policy question about when review is worth paying for, not a path filter.

## Restarting this

If someone wants it anyway: prove the `if:`-skip-satisfies-required-check claim first, build the
allowlist from the three exceptions above, and start with the `highcpu` lane alone, where the cost is
real and the check is not one of the 21 required.
