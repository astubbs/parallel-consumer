---
title: "The run that had to retract an instruction was the one run gated silent"
date: 2026-09-02
category: workflow-issues
module: build-system
problem_type: workflow_issue
component: development_workflow
severity: medium
status: "SOLVED - the quarantine lane writes a distinct lane-emptied report and its two reporting steps are gated on the emptiness check having RUN rather than on the lane being non-empty."
applies_when:
  - A CI comment tells a reader to delete or change something, and a later run must withdraw that
  - Adding an `if:` that skips a reporting step when there is "nothing to report"
  - A code path exists for a transition that no fixture can reach
  - Deciding whether a report should update in place or post a fresh comment
tags:
  - ci
  - reporting
  - stale-comment
  - guard-design
  - unreachable-code
  - quarantine-lane
---

# The run that had to retract an instruction was the one run gated silent

## Context

The quarantine lane posts a sticky PR comment classifying every `@Quarantined` test. One row is an
instruction rather than an observation:

> 🚨✅ **PASSED - ACTION REQUIRED** - fix landed → delete annotation + registry entry

A PR that obeys it, and happens to be removing the **last** entry, left that comment live on itself
forever - still demanding an action it had already carried out.

Nothing corrected it because both halves of the pipeline treated an empty registry as *nothing to
say*:

- `.github/workflows/quarantine-lane.yml` set `found=false` and gated every later step, the one that
  posts included, on `steps.any.outputs.found == 'true'`.
- `bin/quarantine-lane-report.sh` returned before writing a report file at all
  (`Quarantine lane empty - nothing to report`).

So the run that should have retracted the instruction was the one run guaranteed not to speak.

**The tell was reachability, not a bug report.** `.github/scripts/quarantine-report-comment.js`'s
`renderDelta` already had a branch rendering `left the lane` for a row that disappeared - correct,
tested, and for the *last* row unreachable, because a lane with no rows produced no report to carry
the delta. Nobody complained; a correctness review cross-read the module's branches against the
workflow's step gating and found a behaviour the wiring could not reach.

## The class

**A reporter gated on the condition it exists to announce the absence of.** It reads as obviously
correct at the `if:` - why run a report when there is nothing to report? - and the answer is that
"nothing" is itself the news, and is the *only* state in which an earlier report becomes a lie.

It is adjacent to but distinct from
[`a-check-that-reports-success-without-having-run.md`](a-check-that-reports-success-without-having-run.md).
That class is about a **verdict that never reaches anything** - the check goes green having measured
nothing. This one measures fine and is *muted at exactly the transition that matters*: the previous
run's output is still on screen, and the run that would correct it has been skipped.

**The transferable test:** for any step you are about to gate on "there is something to report", ask
what the **previous** run of it said, and whether skipping this one leaves that standing. If the
report is ever an instruction, an alert, or a claim about current state, the empty case is a report,
not a silence.

And the second half, cheaper to apply than to remember: **when you find a branch no test can reach,
do not delete it - find out why the wiring cannot get there.** The `left the lane` branch was written
by somebody who correctly understood the domain, and its unreachability was the defect, not the
branch.

## What was done

- The producer writes a **distinct lane-emptied report** with a payload carrying `"status":"empty"`
  and no outcomes, so the reader renders every remaining row as `left the lane`.
- The two reporting steps are gated on `steps.any.outcome == 'success'` - the emptiness check having
  RUN - instead of on its result. The execution steps stay gated on the result, because there really
  is nothing to run.
- The retraction **posts a fresh comment and retires the old one**, the same treatment every other
  outcome change gets. Silently editing it in place would withdraw, without notification, an
  instruction that notified people when it was issued - the failure
  astubbs/parallel-consumer#409 removed, landing on the one PR where the instruction had already
  been obeyed. The retired copy's heading says `[superseded - the quarantine lane is now empty]`, so
  a reader arriving at the merged PR and landing on the old comment is told it is stale rather than
  sent to delete something that is gone.
- It **stays silent when there is nothing to retract** (`postWhenAbsent: false` in
  `.github/scripts/sticky-report-comment.js`). An empty lane is the healthy steady state; announcing
  it on every PR is the fifteen-comments problem in a new costume. The body is a correction, and a
  correction with nothing to correct is noise.

## Why the tests missed it, and what was added

Every suite on both sides assumed **at least one registry entry** - the Java script test, the
JavaScript reader test, and the end-to-end case that runs the real reporter through the real reader.
The one transition none of them could express was the one that broke.

`.github/scripts/quarantine-report-comment.test.js` now drives the real script into an emptied
registry (annotation and entry both deleted, as the PR would) and asserts the retraction, the
silence, and the steady state. It also asserts the workflow's `if:` lines as text, because nothing in
this repo executes a workflow condition and the seam between a correct module and a gate that skips
it is where this defect lived.
