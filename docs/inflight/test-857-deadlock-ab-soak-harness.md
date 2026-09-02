# The A/B soak harness for the confluentinc#857 revoke deadlock, and what it measured

<!-- inflight-type: register -->

Consulted rather than completed: this is how to re-run the deadlock A/B, and what it returned on
2026-08-27/28. Kept because the setup is easy to get wrong in ways that produce a confident wrong
answer, and this repo produced two of them in one day before it produced the right one.

## The instrument, and why not chaos seeds

`Rebalance857CommitSyncDeadlockProbeIT` forces the overlap by construction: a dwell inside
`onPartitionsRevoked` against a much shorter commit interval, with enough slow-processing backlog
that there is always something to commit. It is `@RepeatedTest`, so one invocation is many
repetitions.

**Replaying captured chaos seeds does not work and should not be attempted again.** Six seeds exist
where a thread dump caught the poll thread BLOCKED on the `commitCommand` monitor, and replaying one
on a laptop opened the window zero times - on the eager scenario as well as the cooperative one. The
chaos suite finds this defect by luck; the probe finds it by construction.

## Two settings that silently destroy the experiment

- **Do not pass `-Pci`.** `surefire.forkCount` is 1 by default and `1C` under that profile, and
  forking one broker per fork removes the window entirely - which is how the suite stayed green
  while the deadlock sat untouched. The pom warns separately that `-DforkCount` is ignored; only
  `-Dsurefire.forkCount` is read.
- **Read the run log, not the failsafe `.txt`.** That file is a few lines of summary carrying no log
  output, so grepping it for the decline line returns zero regardless of what happened.

## The arms

Two worktrees identical but for one term in `tryCommitOffsetsOnRevoke()`: `tryLock()` (FIXED) or a
blocking `lock()` (CONTROL, the pre-fix AB-BA shape). Branch
`experiment/857-deadlock-control-arm-do-not-merge` carries the control; it is knowingly defective and
should be deleted once nobody needs the arm.

Alternate the arms rather than running all of one then all of the other, so neither sits in
systematically different box conditions - the same discipline the 2026-08-18 soak used.

## What it returned

Twelve invocations per arm, alternating, each invocation many repetitions:

- **CONTROL failed every repetition**, with commit-response timeouts, and logged no declines.
- **FIXED failed none**, and logged the contended-decline line in every invocation - the window
  opened throughout.

**The decline count is the load-bearing half.** A green FIXED arm on its own is indistinguishable
from a probe that never opened the window. Zero declines on the CONTROL arm is equally correct: a
blocking revoke never declines, it deadlocks.

This reproduces the 2026-08-18 result independently, on different hardware.

## Known defect in the harness

The window gate counts `declines == 0` as NO-WINDOW, which is right for the FIXED arm and **wrong for
the CONTROL arm**, where the window-evidence is the failures and timeouts instead. It mislabelled
every control invocation. Harmless where the result is unambiguous; fix it before reusing the script
for anything marginal, because a gate that misreads one arm is how a marginal result gets read
backwards.

## The overnight CI runs, read

Three failures across both arms, and they separate cleanly:

- **Both fix-branch failures are `ChaosChurnStormIT`**, the `PERIODIC_CONSUMER_ASYNCHRONOUS` line
  nothing in the family explains - one with `NO_PROGRESS` on random seed `9086872209853284830`, the
  other on random seed `6078190770998307147`. **Neither can be this deadlock**: the AB-BA cycle
  cannot close in that mode, and the probe A/B rules it out independently. They are fresh sightings
  of the open fourth mechanism, found by random-seed hunting rather than replay.
- **The one control-branch failure is `ChaosRevokeUnderWorkCooperativeDrainIT` on seed
  `2867310537409227917`** - which is the second of the six BLOCKED-on-monitor captures. The same six captured seeds all
  passed on the fix branch. That is a chaos-level A/B pointing the same way as the probe, on the
  scenario family where the cycle lives.

**Stated as corroboration, not proof.** Some control-arm runs were cancelled rather than completed,
so this is not a clean six-versus-six; and a single chaos failure is not a rate. The probe A/B is the
evidence. This is a second, independent needle pointing the same way.

## Reading these artefacts is harder than it should be

`gh run view --log-failed` returns truncated debug noise for these runs - the failure mode
`docs/solutions/workflow-issues/gh-run-view-log-truncation.md` owns. The verdicts are in the run's
uploaded reports artefact instead.

**And the artefact carries several same-named XMLs per test class**, because `CHAOS_REPS > 1` makes
every rep write `TEST-<class>.xml` again; `bin/chaos-test.sh` notes this. So a grep that picks "the"
report picks an arbitrary rep, and two greps of the same artefact can disagree - which happened while
reading these. Whoever does the chaos sharding should fix the collision at the same time, since
sharding multiplies it.

## Still open

The overnight CI chaos runs on both arms produced a small number of failures, on the fix branch as
well as the control. They cannot be this deadlock - the probe result rules that out - so they are
either the unexplained async line, a calibration overshoot, or something new. Unread at time of
writing; `gh run view --log-failed` returns truncated noise for them, which is the failure mode
`docs/solutions/workflow-issues/gh-run-view-log-truncation.md` already owns.

## 2026-09-02: the runner's known defect is fixed, and it was worse than "needs fixing"

The header carried `KNOWN DEFECT: the declines==0 window gate is right for the FIXED arm and WRONG
for the CONTROL arm`. It is now implemented per arm, which is what that line asked for - but the gate
had never been implemented at all, in either arm. It was a comment above a `printf`.

**Why the uniform version would have inverted the result.** A blocking revoke never reaches the
decline; it deadlocks. So on the control arm `declines == 0` is what a SUCCESSFUL reproduction looks
like, and scoring both arms by declines discards every real control observation while keeping the
empty ones. The arm that proves the defect exists would have been the arm scored as "no data".

Now: `FIXED` needs `declines > 0`; `CONTROL` needs a failure or a timeout; either arm reports
`DID-NOT-RUN` when the failsafe report says no test executed, so a build that ran nothing can never
be counted as a clean run.

**Three other things were wrong with the runner, none of them recorded anywhere.**

- **It could only work on one machine.** Both trees and the JDK were hardcoded absolute paths, one of
  them pointing at a worktree on `experiment/857-deadlock-control-arm-do-not-merge` - a branch whose
  name says it is temporary. Anywhere else it measured nothing and said so only through an empty
  tally. Both trees are now required arguments, validated for an `mvnw`, and `JAVA_HOME` must be set.
- **It counted with `$(grep -c ... || echo 0)`**, which captures `0\n0` on no match, because `grep -c`
  prints `0` AND exits 1. The declines and timeout columns - the two the experiment reads - would have
  been corrupted in exactly the no-match case. `bin/lib/chaos-experiment-common.sh` has carried
  `pc_count_matches` and a note about this trap the whole time; this script did not use it.
- **It re-implemented `pc_failsafe_stats` inline** rather than sourcing the shared helper, so it also
  missed `pc_classify_failsafe_stats` and had no notion of a run that executed no test.

**Other instances of the count defect: one candidate, checked and dismissed.**
`bin/test-check-quarantine-registry.sh` matches the pattern and is a **red control** - it reproduces
the broken form deliberately so two tests can prove the real implementation differs. It was "fixed"
during this sweep and its own tests went red, which is the control working. It now carries a comment
saying so, because the function name alone does not.
