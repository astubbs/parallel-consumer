# Handoff: the overnight torture harness, and the state of the confluentinc#857 work behind it

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

For an agent picking this up with no context. Written 2026-08-28 at the end of a long session; the
detail behind every claim is in the notes cited, not repeated here.

## What you are inheriting

`bin/torture-overnight.sh` on branch `feats/overnight-torture-harness`. An MVP spike, not finished
work. Run it overnight on the highcpu rig:

    bin/torture-overnight.sh 8 30      # 8 hours, 30-minute cycles

It rotates chaos scenarios against commit modes, gives each cycle a hard wall-clock budget, **takes a
thread dump before killing anything that overruns**, and packages every cycle's logs, failsafe
reports and siloed log streams into a tarball with a `SUMMARY.md`. The morning review should need the
summary and the `dumps/` directory, nothing else.

**The dump-before-kill is the point of the whole design.** A hang with no stack is a rumour; the six
thread dumps that identified the revoke deadlock are the only reason it stopped being a signature and
became a mechanism.

## What it is hunting, and why those

The AB-BA revoke deadlock is **fixed and verified** - see below. What remains unaccounted for:

- **The unbounded revoke wait in transactional mode.** Carries astubbs#44 / confluentinc#803, the
  only issue upstream ever labelled a verified bug. The chaos suite barely exercises
  `PERIODIC_TRANSACTIONAL_PRODUCER`, so the rotation weights it deliberately.
  See `bug-857-transactional-revoke-wait.md` - its design decision is explicitly unsettled, so **do
  not write a fix before settling it with Antony**.
- **Commit-response timeouts**, reported in the field twice and never reproduced -
  `bug-177-commit-response-timeout-unreproduced.md`.
- **Silent data skip.** confluentinc#875 describes an offset never delivered, lag growing, and a
  restart making it reappear. That is not a liveness failure and no liveness detector will see it.
  Cycles must assert delivery COMPLETENESS, not just progress.

## State of the investigation - what is settled, so you do not redo it

- **The deadlock is verified.** A/B on `Rebalance857CommitSyncDeadlockProbeIT`, one term changed:
  control failed every repetition, the fix failed none and logged the contended decline throughout.
  ~240 repetitions per arm. `test-857-deadlock-ab-soak-harness.md`.
- **The async `NO_PROGRESS` line is a TIMING PROXY, not a defect.** Six firings, six drains.
  `test-857-churn-storm-async-stalls.md`.
- **The `CLASS2_STALL` line was demoted the same way** on 2026-08-25. Roughly half the family ledger
  is superseded; every sighting now carries a STATUS line saying which.
- **Six more family defects landed than astubbs#119's status counts** - astubbs#346, astubbs#345,
  astubbs#373, astubbs#336, astubbs#344 and astubbs#349. The issue's `## Fork status` needs rewriting, not appending to.
- **`largeNumberOfInstances` does not reproduce here** - 19 green across three scales. But that is
  evidence about an M2 desktop, not about the code.

## The one habit that mattered most

**Five times this week a measurement error, not the system, was the answer.** The reproducer was
inverted; a probe's window never opened; a test was reshaped between the claim and the measurement; a
grep was narrower than the question; a classifier labelled drained runs flat. Every one produced a
confident wrong answer and every one cost a single command to catch.

So: **before believing any result, check what actually ran.** Did the test execute? Did the code path
fire? Is the file you grepped the file that holds the answer? A green run is evidence only about the
thing that ran.

## Known gaps in this harness - fix before trusting a long run

- `-Dchaos.commitMode` is **assumed** to be a real property. **Verify it is honoured** before reading
  any transactional result; if it is not, every cycle has been running the default mode and the
  transactional hunt has not happened. This is exactly the class of error listed above.
- Cycle budget is wall-clock only. A cycle that finishes fast still burns its slot; consider
  proceeding immediately instead of waiting.
- No completeness assertion yet for the data-skip hunt. The scenarios assert their own SLOs; nothing
  yet checks "every produced offset was delivered exactly once" independently.
  `kafka-verifiable-producer` is the cheap way in, and it is INDEPENDENT of PC's own accounting -
  which matters, because this codebase keeps finding bugs in that accounting.
- Not containerised. A desktop passes everything; the constrained rig is where these defects live.
  `test-pc-soak-harness-architecture.md` has the design, including what to reuse rather than build.

## Where things sit

Branch `feats/overnight-torture-harness`, cut from master, one commit, no PR yet.

The 857 work lives on `bugs/857-paused-consumption-multi-consumers-bug` (astubbs#29). It is NOT
mergeable yet, and for reasons unrelated to code: its records must be migrated out of
`pr-29-857-deadlock-and-what-the-measuring-taught.md` before that note is deleted on merge, and its
title and body are stale in four ways. `docs/merge-checklist.md` owns the rest.
