# Branches safe to delete, and one to salvage

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->
<!-- inflight-vetted: 2026-09-07 - checked every branch named here against `git ls-remote origin`. Still present, so the deletions are still outstanding: both `debug/` branches, `cherry-pick/905-max-shard-metric`, `bugs/859-pcmetrics-leak-v2`, `cherry-pick/893-offset-reset`, `ci/reenable-parallel-tests`, `refactor/test-hardening` (still holding the unsalvaged restore-to-1M commit), the ten `backup/*` refs, and the protected `master-confluent`. Already gone and struck out above: `upstream-pr-905`, `pr-909-temp`, `upstream-pr-893`, `dev-cc` and `astubbs/orca` -->


**Diagnostic-only, investigations landed:** `debug/committedoffset-firstpoll-stall` and
`debug/chaos-w4-red-commit-response-stall` (astubbs#80 and astubbs#100, both with write-ups in `docs/solutions/`).

<!-- post-merge: checked-begin -->
**Superseded:** `cherry-pick/905-max-shard-metric` and `bugs/859-pcmetrics-leak-v2` - folded into
astubbs#57, along with `upstream-pr-905` and `pr-909-temp`, which have since been deleted.
`cherry-pick/893-offset-reset` was folded in there too until 2026-08-24, when the confluentinc#893
cherry-pick was split out to astubbs#337, so its work is that PR's now; its sibling `upstream-pr-893`
is likewise already deleted. Plus `ci/reenable-parallel-tests` and `backup/*`.
<!-- post-merge: checked-end -->

**`refactor/test-hardening` - superseded now, but it was not when this list first said so.** It held
the only copy of a 455-line audit of disabled, kneecapped and weakened tests, committed "not yet
triaged" - and neither this entry nor its `docs/refactoring.md` line mentioned it, so it sat on the
delete list with unique content on it. That audit is now absorbed into
[`docs/test-hardening/inactive-tests-audit-2026-08-08.md`](../test-hardening/inactive-tests-audit-2026-08-08.md),
with the two reasons its own git history refutes corrected.

Of its other two commits, the **OOM diagnostics are now salvaged** to
[`docs/test-hardening/large-volume-in-memory-tests-oom-diagnostics-2026-04-22.md`](../test-hardening/large-volume-in-memory-tests-oom-diagnostics-2026-04-22.md) -
they are the only measured evidence that `LargeVolumeInMemoryTests` at 1M exhausts the heap in the
close path, so they had to survive the branch. The **restore-to-1M commit is still unsalvaged**, and
should not simply be cherry-picked: those same diagnostics show it OOMs as written. Scope for doing
it properly is in [`docs/refactoring.md`](../refactoring.md). Take that commit before deleting the
branch.

<!-- post-merge: checked -->
**Do not delete `master-confluent`** (pinned at pre-rebrand `7f290122`): it is ruleset-protected.
<!-- post-merge: checked -->
It was the base of astubbs#29 and astubbs#31; both have since been retargeted onto `master`, so the
retarget-first caveat no longer applies to them - check for any newer PR still based on it before
deleting. `dev-cc` has since been deleted.

**`astubbs/orca` is no longer on the remote** - `git ls-remote origin | grep -i orca` returns
nothing, so if its CI/tooling (Claude review + PR-assistant workflows, PR-dependency check, CI matrix
tweaks) held anything master never grew its own version of, it is not reachable from here. Nothing
recorded whether the salvage pass happened before it went; do not assume it did.
