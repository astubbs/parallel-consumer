# Web dashboard (astubbs#268): surfaces it must expose when it lands

<!-- inflight-type: register -->

Register of instance state the embedded web dashboard needs to show, accumulated by work that
lands before the GUI does. astubbs#268 is the GUI PR; each entry names the state, where it comes
from, and why the GUI is the right place to see it. When the GUI lands, its PR works this list and
deletes this file (or shrinks it to what remains).

- **Commit-failure seam state** (astubbs#317, plan:
  `docs/plans/2026-08-24-001-feat-commit-failure-seam-plan.md`): whether commits are currently
  failing, consecutive exhausted budgets, time since the last successful commit, the active
  decision (continuing vs shutting down) and the configured policy. The seam's plan requires these
  as metrics; the GUI renders them - a `CONTINUE`-ing instance looks healthy from outside while
  committing nothing, which is exactly the state a dashboard exists to make visible. The meters
  landed as (all `subsystem=committer`): `pc.commit.failure.exhaustions` (counter),
  `pc.commit.failure.consecutive.exhaustions` (gauge), `pc.commit.time.since.last.success` (gauge,
  seconds), and `pc.commit.failure.seam.state` (gauge: 0 HEALTHY, 1 FAILING_CONTINUING,
  2 FAILING_PAUSED). The active decision and configured policy are not meters - the GUI reads those
  from the options and the logs. Feature
  rationale: `docs/inflight/core-commit-failure-seam.md`.
