# The coverage comparison is still not like-for-like

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`codecov/project` compared a PR total against a base total built from a **disjoint set of flags**.
The master `build` job uploaded everything under one `default` flag; a PR runs the `test` matrix and
uploads `unit`, `integration`, `performance`, `lincheck`, `chaos`, and never `default`.
<!-- post-merge: checked-begin -->
Fixed on the master side in astubbs/parallel-consumer#400: that job now uploads `unit` and
`integration` separately, matching the two halves the pom's `report` and `report-integration`
executions produce.
<!-- post-merge: checked-end -->

**Observed before the fix**, on a PR touching zero Java: Files 90 -> 90, Lines 4822 -> 4822,
Branches 463 -> 463, reported **-3.53%**. Nothing had changed; the two sides counted different things.

## What is still wrong, and why it was not fixed in the same change

**The base now has `{unit, integration}` and a PR has `{unit, integration, performance, lincheck,
chaos}`.** The asymmetry is smaller and it now errs SAFE - a PR's total can only be higher, so the
threshold no longer fires spuriously - but it has become a false-negative channel: a genuine drop in
unit or integration coverage can be masked by coverage that `performance`, `lincheck` or `chaos`
contributes and the base has no equivalent for.

That is the trade this repository normally refuses, so it is written down rather than left implied.
It was not fixed in the same change because the fix is a **status-check policy** decision, not a
workflow one, and it adds check contexts to every PR:

- **Per-flag project statuses** in `codecov.yml` - `unit` compares against `unit`, `integration`
  against `integration` - so each side is measured the same way and the extra PR-only flags stop
  contaminating the total. This is the complete fix.
- Or drop `performance`, `lincheck` and `chaos` from coverage upload entirely, leaving one pair on
  both sides. Simpler, and loses whatever those suites uniquely cover.

## The fix cannot be verified on the PR that makes it

`build` is `push`-only, so the new flags do not exist until this lands on master and that job runs.
Until then every PR still compares against a `default`-flagged base and `codecov/project` stays red -
including the PR carrying the fix. **A red `codecov/project` on the first PRs after this merges is
the expected state, not a regression**, and it clears once master has re-uploaded under the new flags.

## Delete when

Per-flag statuses are configured (or the extra suites stop uploading), and a PR after that change has
shown `codecov/project` comparing like-for-like.
