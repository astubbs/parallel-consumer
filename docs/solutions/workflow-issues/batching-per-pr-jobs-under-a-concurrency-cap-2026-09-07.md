---
title: Batching per-PR jobs under a concurrency cap - what worked, what was refuted, and the traps
date: 2026-09-07
category: workflow-issues
module: ci
problem_type: workflow_issue
component: development_workflow
severity: medium
root_cause: resource_contention
resolution_type: workflow_improvement
applies_when:
  - Sharding a CI suite and finding the wall-clock win smaller than expected
  - Proposing to merge, split, rename or delete a GitHub Actions job
  - Deciding whether a required status check may be added or removed
  - Reading a green duplication or coverage report as evidence about a change
  - Scoring a CI change on an average or median step duration
symptoms:
  - Big sharded suites sit queued while one-minute jobs run
  - A PR push launches more jobs than the account's concurrent-job ceiling
  - Job queue delay dominates job duration for small checks
tags:
  - github-actions
  - concurrency-cap
  - required-status-checks
  - job-batching
  - measurement
  - refuted-hypotheses
  - bimodal-durations
---

# Batching per-PR jobs under a concurrency cap

## The problem, measured before it was theorised

Sharding the heavy suites made each one faster and the crowding worse. On the free plan a
GitHub account gets **20 concurrent jobs**, shared across every repository and every open PR.
A single push here launched more than that on its own, so a second PR pushing at the same time
put roughly triple the ceiling into 20 slots.

Measured over eight pushes before any change, from `gh api .../actions/runs/<id>/jobs`:

- The **small no-build jobs queued longer by median than the big ones**, because the big ones
  wait on the cache job and enter the queue later. Queue delay dominated their runtime.
- The critical path was the unit suite, and it sat behind the cache job's own queue wait.

Reproduce the current shape rather than trusting a number written here:

```bash
gh api repos/astubbs/parallel-consumer/commits/<sha>/check-runs --jq '.check_runs[].name'
```

## What the metric had to be, and why not wall clock

The lever is the **count of jobs a `pull_request` event starts**, which is a property of the
workflow YAML. Queue delay is GitHub's scheduler responding to that count *plus every other
repository on the account*, so timing a CI run per experiment measures other people's load.
The harness therefore parsed the workflow files, expanded matrices, and excluded `if: false`
and close-only triggers - no CI run in the loop at all. One real run confirms at the end.

**A static harness must model discovery-by-glob or it lies.** The first measurement reported
four checks dropped, because the fingerprint could not see gates that `bin/check-all.sh`
finds by globbing `bin/check-*.sh`. Any job invoking that script runs the whole set minus its
declared exception lists.

## The folds that worked

Deleting three jobs outright, then batching in four rounds, took the per-push job count down by
about a third. The pattern that made each one cheap:

- **Name each step after the job it replaces.** The log then reads as the checks list used to,
  and prose elsewhere saying "`dups: clones` found X on PR N" stays literally true - only the
  *check* name changed. This shrinks the citation sweep to references that say "job" or
  "required check".
- **Guard every folded step with `if: ${{ !cancelled() }}`.** A red first step then never hides
  the ones behind it, and the job conclusion aggregates them. No verdict step is needed; the
  platform already does that job.

## The fold that did not work: a median hid a bimodal worst case

One of the four rounds put the PR-scoped PIT mutation lane in as the last steps of `scan: repo`,
a **required** check. That was wrong, and astubbs/parallel-consumer#463 extracted it back into its
own job. The other folds in this run were fine and remain fine.

**The methodological half is the valuable one: the harness scored each candidate fold on MEDIAN
step duration, and PIT's distribution is bimodal.** It takes about 11 seconds when no in-scope class
changed - the common case, and therefore the median - or up to about 19-20 minutes when it actually
mutates. Nothing lands in between. A median made a 20-minute worst case look like a rounding error.
For any step whose duration is bimodal, or long-tailed, the median is the wrong statistic: score the
fold on the tail it can actually produce.

**The design half:** batching is safe for work that is fast, bounded and gating. A slow, bimodal,
deliberately non-gating lane is the one shape that must not be folded into a required job, because
**a job emits one check run and that check does not report until the whole job finishes**. The
mutation lane is `continue-on-error` precisely so its outcome cannot block a merge - folding it into
a required check let it block the merge with its runtime instead, which the flag does nothing about.
Measured on job `101622402881`: every other step of `scan: repo` was finished 3m29s in, and PIT held
the required context for the remaining ~17 minutes.

**The comment written above the folded block claimed "Every check above has already reported by the
time this starts."** It was false, and its falseness is the mistake in one line: the *steps* had
finished and the duplication tools had posted their own PR comments, so the lane looked reported -
but the *check* had not reported and structurally could not. A step completing and a check reporting
are different events. Do not reason about check latency from what the log shows finishing.

## The pairing test, learned by getting it wrong

A fold predicted at two jobs delivered one. **Trigger types and permission scopes are part of a
job's identity.** Before pairing two jobs, diff their `on.pull_request.types` and their
`permissions`. Any of these vetoes the fold or forces the union onto the host:

- a write scope one side needs and the other must not have beside PR-authored code;
- a trigger only one side needs (`closed`, `edited`);
- a job-level `if:` that cannot express "skip seven of these ten steps" - that becomes a
  job-level env var each step reads, and the guard's behaviour must be carried over verbatim.

**A guard written for one purpose silently starves another.** The JDK and cache steps sat behind
a CVE-credentials condition, and the mutation lane runs on fork and Dependabot PRs where that
condition is false - so it would have run with no JDK.

## The trap that costs the most: names are an API

A required status check is matched by **job name**, and the ruleset is repository settings that
no PR can change or test.

- **A skipped required check pends forever.** Giving a batched job a `needs:` edge means a
  transient cache failure *skips* it, which wedges the PR permanently. Name a status function
  (`if: ${{ !cancelled() && ... }}`) so the job runs and reports instead.
- **Removals and additions have opposite orderings.** Remove an orphaned context in the same
  sitting as the merge; add a new one only once a PR has produced it. Backwards, every PR in
  the repository pends on a check nothing emits.
- **A rename silently breaks name-matching consumers**, even when step names are preserved.
  `bin/check-pr-analysis-surfaces.sh` filters check runs through a name regex; one alternative
  in it had already been dead since an earlier rename, dropping a surface from its listing with
  nothing to say so. Grep `bin/` and `.claude/hooks/` for the check name on every fold.

## Refuted, and worth more than the successes

- **Removing the cache-warming job.** It looked like a free job and the only lever that also cut
  wall clock. It is the recorded fix for
  [the Azure west-US Maven Central timeout](../build-errors/maven-central-timeout-azure-west-regions-2026-04-21.md),
  where each uncached artifact hangs for four minutes, and a later PR added a step to it that an
  open branch depends on. **The process failure is the finding**: the hypothesis survived two
  rounds of review because the prior-art sweep searched "workflow", "runner queue", "job count"
  and "concurrency" - none of which reach a document filed under build errors and titled for a
  Maven timeout. The owning document was one grep away, on the mechanism word `prepare-deps`.
  A real-CI control arm would also have been the *wrong* instrument: the incident is
  region-dependent, so a green run measures the region you drew.
- **The target itself was wrong.** It was set as the cap minus three CodeQL analyze jobs.
  Default setup runs **four**. Fitting under the cap was never reachable at the stated target,
  and is not a batching problem at all: both the Python and JavaScript analyses have real source
  here, so closing the gap is a coverage decision or a plan decision.
- **"The reviewer failed because the prompt was too large."** Refuted by comparison: a PR whose
  body and comments were the same size reviewed successfully minutes earlier.

## Two things a job-count harness structurally cannot see

- **Dependency-chain latency.** It tracks the longest job, not how long a job waits for its
  `needs:` edge. A fold that adds an edge looks free and is not.
- **Whether a local sweep is the same sweep CI runs.** `bin/check-all.sh` defaults to gates only;
  CI runs `--with-tests`. Two failures reached CI because the pre-push check ran the shorter one.
  The self-tests are where a fold's environment changes surface - a job-level env var set for a
  gate also reaches that gate's self-test, whose fixtures may depend on its absence.

## Also established

- The duplication engines cannot see repeated workflow YAML. One never reads workflows at all;
  the other does and finds clones elsewhere, but not within a file whose repeated block is
  broken up by differing comments. Recorded with the measurement in `docs/refactoring.md`.
- A coverage flag can report a large drop with no code change, comparing against a base snapshot
  that disagrees with the default branch. Merging the base in made it vanish.
