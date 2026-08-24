---
title: A closure state is a rendering choice, not a triage - verify it, and audit without a window
date: 2026-08-12
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: high
root_cause: inadequate_documentation
resolution_type: workflow_improvement
applies_when:
  - Trusting an issue tracker's closed/completed state as evidence something was done
  - An issue body says "fixed by PR #N" and you are about to act on that
  - Any monitoring keyed on "updated since X" - sweeps, feeds, dashboards
  - Looking for bulk events (sweeps, purges) in a history that contains bots
symptoms:
  - Dozens of issues closed the same day, all marked COMPLETED, none with a fix
  - A windowed sweep that has never once surfaced a known-missing cohort
  - An issue's "fixed by" PR turns out to have been closed unmerged
tags:
  - upstream
  - false-negative
  - sweep
  - issue-tracker
  - bots
  - audit
---

# A closure state is a rendering choice, not a triage - verify it, and audit without a window

## Context

In 2023 upstream ran two bulk clearouts before going quiet: 35 unmerged PRs closed "Closing -
Stale." and 28 issues closed "Closing Issue" - every issue marked **COMPLETED** rather than "not
planned". Nobody noticed for three years. The full factual record lives in the `THE 2023
ADMINISTRATIVE SWEEPS` section header of `src/docs/development/upstream-map.yaml` (PR
astubbs#258); this doc carries only the transferable lessons.

The cohort was invisible for two independent, compounding reasons - and each is a general failure
class, not a quirk of this repo.

## Guidance

**1. A closure state reason is UI, not evidence.** GitHub renders `completed` as resolved, so a
bulk clearout marked COMPLETED reads as 28 things getting done. A real triage leaves a residue: a
linked merged PR, or a closing comment that describes a fix. No residue means the state tells you
how someone clicked a button, nothing more.

**2. Follow "fixed by #N" to the merge state.** Four of the 28 issue bodies claimed they were
implemented or solved by a named PR - and every one of those PRs had itself been closed
**unmerged** in the other sweep. A claim of a fix that cites a PR is only as good as that PR's
merge bit, which takes one API call to check.

**3. A windowed search is structurally blind to everything before its window.** The sweep tool
searched `updated:>=last_swept`, so an item last touched in 2023 could *never* appear, no matter
how many times the sweep ran. Any watcher keyed on recency needs a complementary no-window audit
that asks the completeness question directly ("which closed items are unaccounted for?") - here
that became `upstream-sweep.sh --audit`, which rediscovers both sweeps from scratch. This is the
instrument-that-could-have-said-yes principle
([negative-results-need-an-instrument-that-could-have-said-yes.md](negative-results-need-an-instrument-that-could-have-said-yes.md))
applied to monitoring: a sweep that cannot possibly surface a cohort returns the same "nothing new"
as a healthy one.

**4. Filter bots before hunting bulk events.** Four of the six bulk PR-closure days were dependabot
self-closing superseded bumps - batches that look identical to an administrative sweep and, left
in, buried the two real ones. Filtering them also surfaced two *human* PRs hiding inside bot
batches that would otherwise never have been reviewed.

## Where the pieces live now

- Facts and cohort policy: `upstream-map.yaml`, `sweep-2023-*` entries and their section header.
- Tooling and its blind spots: `docs/upstream.md`, "Checking upstream for new activity".
- Open follow-up work: `docs/inflight/upstream-coverage-completeness.md` and siblings.
