---
title: "Two routes to the same reviewer, measured - the mention route posts, the dispatch route can finish and post nothing"
date: 2026-08-17
category: workflow-issues
module: tooling
problem_type: workflow_issue
component: development_workflow
severity: medium
root_cause: config_error
resolution_type: documentation
status: "Measured, not fixed. The dispatch route's silent no-post has no guard; `claude-review` stays green on whatever comment was posted last, however stale."
applies_when:
  - Choosing how to request the automated reviewer on a PR
  - A review run reports success and no review appears on the PR
  - Reading a green `claude-review` check and treating it as evidence this head was reviewed
  - Deciding whether to build a progress announce for the dispatch route
symptoms:
  - A review workflow runs for minutes, concludes success, and posts no comment
  - The required review check is green while the only review comment predates the current head
  - Two review entry points behave differently and the difference is undocumented
tags:
  - ci
  - review-automation
  - silent-failure
  - claude-review
  - false-negative
---

# Two routes to the same reviewer, measured

There are two ways to ask the automated reviewer for a review, and they are not equivalent. Both were
exercised on astubbs/parallel-consumer#296 on 2026-08-14. This records what each actually did, because
the difference decides which one to use and the reviewer-notes file previously said the comment route
was untested.

## What was measured

| | Dispatch route | Mention route |
|---|---|---|
| Trigger | `workflow_dispatch` on `claude-code-review-dispatch.yml` | `@claude` in a PR comment, `claude.yml` |
| Runs | [`31771994565`](https://github.com/astubbs/parallel-consumer/actions/runs/31771994565), [`31774560811`](https://github.com/astubbs/parallel-consumer/actions/runs/31774560811) | [`31773366411`](https://github.com/astubbs/parallel-consumer/actions/runs/31773366411), [`31775979614`](https://github.com/astubbs/parallel-consumer/actions/runs/31775979614) |
| Posted a comment | **First run yes, second run no** | **Yes, both** |
| Progress announce | none - `track_progress` is hard-set `false` | posts a sticky comment at start, rewrites it at the end |

**The dispatch route can finish successfully and post nothing.** Run `31774560811` ran for
**8 minutes 42 seconds**, its `Run Claude Code Review` step concluded `success`, and the workflow's own
`Refuse to report success for a review that did not run` guard passed - yet no comment appeared on the
PR. Its prompt is explicit that this must not happen: *"your LAST action must be to post exactly one
comment... A review that finds problems and posts no comment leaves the PR unmergeable and is
indistinguishable from no review at all."* It happened anyway, on the run that had the most to say.

**The mention route posts at both ends.** The `claude[bot]` comment on astubbs/parallel-consumer#297
was `created_at` 9 seconds into the run and `updated_at` at the end - so the sticky comment *is* the
progress announce, posted first and rewritten into the finished review. That is exactly the announce
the dispatch route lost when `track_progress` had to be hard-set `false`.

## Why it matters more than "one route is nicer"

`claude-review` is a required check satisfied by an issue comment from the reviewer **and by nothing
else**. It does not know which head that comment reviewed. So a dispatch run that finishes and posts
nothing leaves the check green on an **older** comment - and on astubbs#296 that stale comment was
headed **Blocking**, describing a defect in code that had since been deleted. A reader arriving at the
PR sees an unresolved blocker against code that is not there, and the gate says everything is fine.

This is [a check that reports success without having run](a-check-that-reports-success-without-having-run.md)
in its review-automation form: the failure is not that the check goes red, it is that it stays green
while nothing looked at this head.

## What to do

- **Prefer the mention route** for requesting a review. It posts, and it announces.
- **After a dispatch run, check a comment actually appeared**, and that its head SHA matches the PR's.
  Do not read the green check as the answer.
- If the dispatch route is kept, the announce work `docs/inflight/ci-review-agent.md` proposes
  (a `gh pr comment` step) is largely what the mention route already does for free.

## Still unexercised

Whether either route can open an **inline review thread** is untested. Both measured runs found
nothing blocking on the head they reviewed, so neither had occasion to open one; that says nothing
either way. The dispatch route's prompt states outright it cannot ("YOU CANNOT OPEN INLINE REVIEW
THREADS ON THIS RUN"), and the claim that the mention route can is read off the action's
entity-event handling, not off an observed thread. Deciding it needs a PR with a real blocking
finding.
