# The chaos autopsy can report `violations (0)` on a run a violation killed

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`AGENTS.md` tells every agent to **read the `=== AMBIENT PROBE AUTOPSY ===` block before diagnosing
a broker integration test failure by hand**. On at least one chaos arm that instruction returns the
opposite of the truth.

## The observation

<!-- post-merge: checked-begin -->
`Chaos Pain Suite`, `ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger`, seen on astubbs#348
([job 97659446984](https://github.com/astubbs/parallel-consumer/actions/runs/32800216920/job/97659446984)).
The test died of a probe violation, and the autopsy printed:

    failure: TerminalFailureException: probe violation during run
    chaos seed: 2575991864395313898
    violations (0):
      (none crossed the chaos-calibrated bounds - see peaks/frozen detail below)

The run summary from `ChaosScenarioBase#settleRun`, in the same log, disagrees:

    probe violations=[NO_PROGRESS: fleet consumed count stuck at 98804/100000 for 30s (bound 30s)]

So the violation that failed the test is **absent from the autopsy's own violation list**.
<!-- post-merge: checked-end -->

## Why it matters more than a cosmetic mismatch

A reader following the documented procedure sees `violations (0)` and the accompanying gloss *"none
crossed the chaos-calibrated bounds"*, and reasonably concludes the probe saw nothing - i.e. that
the failure is elsewhere, or that the test is simply flaky. The truth is that a **fleet-level**
detector fired on a hard bound. That is worse than no autopsy: it is a confident wrong answer at the
exact moment the procedure says to trust it.

It also silently weakens every entry that cites a clean autopsy as evidence. `bug-857-family.md`
repeatedly distinguishes *"the probe genuinely fired"* from *"probe clean"*, and
`test-load-tightness-flakes.md` cites *"probe clean ... the fault is likely in the test itself"* as
support for a test-side diagnosis. If `violations (0)` can mean "a fleet violation fired and was not
listed", those readings need re-checking rather than trusting.

## Likely shape, stated as a hypothesis not a finding

The autopsy appears to enumerate **partition/ambient-scoped** violations - it prints `peaks:` and
`frozen partitions:` immediately after - while `NO_PROGRESS` is a **fleet-scoped** detector on
`ProgressProbe`. The two `CLASS2_STALL` and `ZOMBIE_MEMBER` autopsies observed on the same branch
the same night both listed their violations correctly, and both are partition/group scoped, which
is consistent. Confirm against `ProgressProbe` and the autopsy writer before fixing.

## Delete when

The autopsy lists every violation the run recorded, including fleet-scoped ones - or, failing that,
stops claiming `none crossed the chaos-calibrated bounds` when it has not looked at all of them.
