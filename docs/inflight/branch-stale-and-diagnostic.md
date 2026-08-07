# Branches safe to delete, and one to salvage

**Diagnostic-only, investigations landed:** `debug/committedoffset-firstpoll-stall` and
`debug/chaos-w4-red-commit-response-stall` (astubbs#80 and astubbs#100, both with write-ups in `docs/solutions/`).

**Superseded:** `cherry-pick/893-offset-reset`, `cherry-pick/905-max-shard-metric`, `upstream-pr-893`,
`upstream-pr-905`, `pr-909-temp`, `bugs/859-pcmetrics-leak-v2` - all folded into astubbs#57. Plus
`refactor/test-hardening`, `ci/reenable-parallel-tests` and `backup/*`.

**Do not delete `master-confluent`** (pinned at pre-rebrand `7f290122`): it is ruleset-protected and
is the base of astubbs#29 and astubbs#31 - retarget those first. `dev-cc` is pinned at the same commit.

**Salvage or abandon: `astubbs/orca`** - CI/tooling (Claude review + PR-assistant workflows,
PR-dependency check, CI matrix tweaks) from before master grew its own versions of most of it. Badly
diverged; take whatever is still novel, then drop it.
