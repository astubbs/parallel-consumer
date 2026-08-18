---
title: "gh run view --job --log silently truncates long CI logs - use the run-logs archive endpoints for diagnosis"
date: 2026-08-18
category: workflow-issues
module: CI/GitHub Actions
problem_type: workflow_issue
component: development_workflow
severity: high
applies_when:
  - "Debugging GitHub Actions job failures with long or large logs"
  - "`gh run view --job <id> --log` returns truncated output from multi-thousand-line logs"
  - "Troubleshooting misattributed test failures due to mid-run truncation point"
root_cause: inadequate_documentation
resolution_type: documentation_update
tags: [gh-cli, github-actions, log-truncation, ci-debugging, tool-limitation]
---

# gh run view --job --log silently truncates long CI logs - use the run-logs archive endpoints for diagnosis

## Context

On 2026-08-18, a handoff document for this repo (astubbs/parallel-consumer) was written from a CI
chaos-suite log fetched with `gh run view --job <id> --log`. That command returned exactly 1654
lines of what was actually a 5948-line job log - no warning, no truncation marker, no non-zero exit.
The cut landed inside a **passing** test's phase: the cooperative revoke-under-work chaos test,
whose log is full of expected, handled `RebalanceInProgressException` churn during a deliberate
rebalance storm. Read on its own, that churn looks exactly like a failure. The handoff attributed
the job's failure to that test and circulated its seed. The real failure was a different test with a
different seed - `ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger`, killed by `ProgressProbe`
on a fleet-wide `NO_PROGRESS` stall - while the cited test had actually finished with
`probe violations=[]`. The error was caught only because a second session re-pulled the log through a
different route before writing the ledger entry. The full account, including both seeds and the job
run link, is recorded as the fifth sighting in
[`docs/inflight/bug-857-family.md`](../../inflight/bug-857-family.md), under the heading
**"Correction worth recording: a truncated log misattributed this sighting before it was written."**

**It happened again the same day, to someone who had this document available and did not read it**,
which is the most useful thing recorded here. Diagnosing a `Chaos Pain Suite` failure on astubbs#296
([run 32104058992](https://github.com/astubbs/parallel-consumer/actions/runs/32104058992)), the
job-log route returned 2207 lines of a 6266-line log; the tail carried the verdict, the autopsy and
all three scenario seeds. The conclusion written from it - "the seed is lost, this sighting can never
be replayed" - was published to a ledger entry on astubbs#29 and had to be corrected an hour later.
The seed, `4709156528562690268`, had never been anywhere but the part of the log that was cut. Same
command, same failure mode, same class of wrong claim, two days after this file was written to
prevent exactly it. **Prior-art checks are the fix, not more documentation** - the entry was written
without running the greps `AGENTS.md` requires before investigating anything.

This is a sibling of an earlier trap in the same file's fourth sighting: there, GitHub truncated the
log **stream** itself server-side, so neither `--log` nor `--log-failed` contained the
`=== AMBIENT PROBE AUTOPSY ===`  block at all, and the autopsy had to be recovered from the uploaded
test-report artifact (see that entry's `**Retrieval note`). Two different truncation mechanisms,
same failure mode: a log that looks complete but isn't, feeding a diagnosis that inherits the gap.

## Guidance

**Before diagnosing anything from a fetched CI log, verify it is complete - then, if it might not
be, fetch it from a route that cannot truncate.**

Three retrieval routes exist for this repo's CI logs, in order of completeness:

1. **Uploaded test-report artifact** (failsafe/surefire XML, e.g.
   `highcpu-fast-feedback-reports-Chaos Pain Suite-*`) - survives even server-side log-stream
   truncation, because the autopsy and diagnostic output are captured as `system-out` inside the XML
   itself, not read back off the console stream. Most complete; go here first for a chaos or broker
   integration-test failure.
2. **Run-logs archive endpoints** - a zip of the complete per-job logs for the run:
   ```bash
   gh api repos/<owner>/<repo>/actions/runs/<run-id>/logs > logs.zip
   ```
   For a job that was re-run, the default view only shows the latest attempt; an earlier attempt's
   failure is otherwise invisible and needs:
   ```bash
   gh api repos/<owner>/<repo>/actions/runs/<run-id>/attempts/<n>/logs > logs.zip
   ```
   Unzip and read the per-job `.txt` files. These downloads can be slow and large for a long
   job - use a generous timeout or background the fetch rather than letting it appear to hang.
3. **`gh api repos/<owner>/<repo>/actions/jobs/<job-id>/logs`** - the job-level route
   [`docs/ci.md`](../../ci.md) recommends. It can exit **1 having written zero bytes to stdout**,
   with `the response contains terminal escape sequences; pass --allow-escape-sequences to output it
   anyway` on **stderr**. Redirect stdout to a file and you get an empty file and a job that appears
   to have no log at all. It is the only route here that does signal - and it signals into the one
   stream a `>` redirect does not capture.
4. **`gh run view --job <id> --log`** - convenience only. It is fine for a short job or a quick
   skim, but it is **not diagnostic-grade for a long job**: it silently returned 1654 of 5948 lines
   in this incident, with no indication anything was cut.

**Verification habit, before trusting any fetched log enough to diagnose from it:** check that it
ends with the job's real terminal lines - a surefire/failsafe summary (`Tests run:`), a
`BUILD SUCCESS`/`BUILD FAILURE` line, or a post-job/cleanup step. A log that just stops mid-phase,
with no closing marker, is truncated, and any diagnosis built on it inherits the cut. A suspiciously
round or repeated line count across two independent fetches (as happened here - two sessions both
got exactly 1654 lines) is another tell that the cut is systematic, not incidental.

**Corollary: never parse test totals out of console text.** When the tail is missing, no `Tests run:`
line survives, so every parser downstream reports zero tests and zero failures - which is the shape
of a suite that was *skipped*, not one that failed. A grep for a failure signature returning **0** on
a truncated log is a false negative, and it reads exactly like a clean run; the more systematically
you grep, the more confident the wrong answer gets. Parse the failsafe/surefire XML from the uploaded
artifact instead, where the counts are attributes and the autopsy is captured inside `system-out`.

## Why This Matters

A truncated log doesn't fail loudly - it produces a plausible-looking, wrong diagnosis. In this
incident the wrong route named the wrong test and circulated the wrong replay seed in a document
other agents would act on: replaying that seed can never reproduce a failure, because it belongs to
the run's passing control arm. Every minute spent chasing the misattributed test, and every downgrade
of trust in the real failure's actual seed, is cost created by treating an incomplete log as
complete. Because the failure mode is silent (no error, no non-zero exit, no truncation marker), the
only defense is the terminal-marker check performed before the diagnosis, not after something looks
wrong.

## When to Apply

- Fetching any CI job log longer than a screen or two, especially chaos-suite, integration-test, or
  otherwise long-running jobs.
- Before writing any handoff, inflight note, or ledger entry that names a failing test, cites a seed,
  or attributes a CI failure to a specific phase of a job.
- When a job was re-run and you need the failing attempt's log, not the latest (successful) attempt.
- When `docs/testing.md`'s ambient-probe section is the next thing you'd check for a broker
  integration-test failure. It used to state that every such failure **log** includes the
  `=== AMBIENT PROBE AUTOPSY ===` block; both truncation incidents in this repo (fourth and fifth sightings
  of `docs/inflight/bug-857-family.md`) showed that needed a scope correction rather than a
  retraction - the autopsy is reliably **emitted** on failure, but the console **log** you fetch it
  from is not a reliable place to find it; the artifact and archive routes above are. **That
  correction has landed**: the section now says "emits" and defers the retrieval routes to this
  document. What remains is to keep the two consistent if either moves.

## Examples

**Wrong fetch (what produced the misattribution):**

```bash
gh run view --job 95579861648 --log > job.log
wc -l job.log
# 1654 job.log   <- looks like a complete log; nothing here says it was cut
tail -5 job.log
# ...RebalanceInProgressException: Failed to ...
# ...RebalanceInProgressException: Failed to ...
# (no "Tests run:", no BUILD SUCCESS/FAILURE, no cleanup step - this is the tell)
```

Diagnosing from this file reads as: the cooperative revoke-under-work chaos test is failing, spam of
`RebalanceInProgressException`. That test in fact passed with `probe violations=[]` on seed
`4087023100803854645` - the tail of the file lands mid-churn inside its passing run, not at a
failure.

**Right fetch (what recovered the real failure):**

```bash
gh api repos/astubbs/parallel-consumer/actions/runs/32093367999/logs > logs.zip
unzip -l logs.zip | grep -i chaos
unzip -p logs.zip '*Chaos Pain Suite*.txt' > job-full.log
wc -l job-full.log
# 5948 job-full.log
tail -5 job-full.log
# Tests run: N, Failures: 1, Errors: 0, Skipped: 0
# BUILD FAILURE
```

The full log's terminal lines confirm completeness and locate the actual failure:
`ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger`, killed by `ProgressProbe` for
`NO_PROGRESS: fleet consumed count stuck at 98150/100000 for 30s`, replay seed
`3086917415748208232` - a different test and a different seed than the one the truncated fetch
implicated.

## Related

- [`docs/inflight/bug-857-family.md`](../../inflight/bug-857-family.md) - fourth sighting
  (`**Retrieval note`, server-side stream truncation, artifact recovery) and fifth sighting
  (`**Correction worth recording`, this incident, archive-zip recovery, both seeds).
- [`docs/testing.md`](../../testing.md) - ambient-probe section (`=== AMBIENT PROBE AUTOPSY ===`), which
  now carries the corrected "emits" wording and points here for the retrieval routes rather than
  restating them.
- [`docs/ci.md`](../../ci.md) - "Reading a failed job's log", the topic doc for CI log retrieval;
  currently documents the job-level `actions/jobs/$jid/logs` API route only, not the run-level
  archive-zip endpoints or the terminal-marker completeness check this incident established.
