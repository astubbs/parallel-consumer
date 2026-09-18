# The master ruleset has no bypass, so the next `release.yml` run cannot push its release commits

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->
<!-- inflight-state: deferred - decide before the next release is cut; nothing waits on it until then -->

`release.yml` runs `maven-release-plugin`'s `release:prepare`, which commits the version bump, tags
`vX`, and **pushes both commits to `master`** through `RELEASE_PAT`. That push only succeeds when
the master ruleset lets the token's owner bypass the pull-request rule - [`docs/releasing.md`](../releasing.md)
states it: *the "Repository admin" role must be in the master ruleset's bypass list*.

## What happened on 0.6.0.0 (2026-09-17)

- The bypass was set to "pull request only", which does not cover a direct push. The first dispatch
  failed for a different reason (the pom's `<scm>` was SSH; astubbs#505), the second only ran because
  the owner flipped the bypass to **Always** for the day.
- After the tag the owner **removed the bypass entirely**. A separate "All branches" ruleset that
  required a status on branch creation was found unsatisfiable and disabled at the same time.
- `RELEASE_PAT` is still set. With no bypass, `release:prepare`'s push is refused, so the workflow
  as it stands cannot cut 0.6.0.1 or 0.7.0.0.

## The decision

Two ways to make the next release possible, and the owner has not chosen:

1. **Restore the bypass for the release day only**, as on 0.6.0.0. Cheapest; a manual step the
   release runbook has to name, and a window in which an admin push to master is unguarded.
2. **Invert the flow**: a PR sets the release version (the two `release:prepare` commits become an
   ordinary reviewed PR through the normal gates), and a publish workflow deploys from the tag on a
   green master. No bypass, no PAT with write scope. This is the design the owner sketched on
   2026-09-17 and explicitly declined to fold into astubbs#509 ("fix the bug in the process, not
   change the process"), so it is a candidate, not a plan.

Either way `docs/releasing.md`'s secrets paragraph and `release.yml`'s header comment describe the
old state and change with the decision.

## Delete when

The decision is made and recorded in `docs/releasing.md`, and a release has been cut under it.
