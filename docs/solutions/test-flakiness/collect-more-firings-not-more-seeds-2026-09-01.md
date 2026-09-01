---
title: "When a probe line fires intermittently, collect more FIRINGS - not more seeds"
date: 2026-09-01
category: test-flakiness
module: parallel-consumer-core
problem_type: test_failure
component: testing_framework
severity: medium
symptoms:
  - "A chaos probe line fires on some runs and not others, and the question is whether it is a real defect or a timing artifact"
  - "A seed hunt for a second reproducer returns nothing in N attempts and the question stays open"
  - "A diagnostic run reports 'engaged' rather than an answer"
root_cause: instrument_choice
resolution_type: method
status: "Answered 2026-08-28: the backlog DRAINED on all six firings collected, which demoted the asynchronous no-progress line to a timing proxy. The two runners built to ask it were retired on astubbs/parallel-consumer#381."
applies_when:
  - "Deciding whether an intermittent probe violation is a defect or a timing proxy"
  - "Choosing between hunting a new seed and collecting more firings of a known one"
  - "Writing an unattended experiment runner whose stopping condition you have to pick"
tags:
  - chaos-testing
  - experiment-design
  - stopping-conditions
  - confluentinc-857
---

# Collect more firings, not more seeds

## The question, and how it was settled

The confluentinc#857 chaos suite has an **asynchronous no-progress line** that fires intermittently.
The open question was whether it marks a real wedge or is a timing proxy - a bound crossed by a busy
fleet that would have finished anyway.

It was settled by watching the backlog after each firing: **the backlog drained on all six firings
collected** (2026-08-28), which demotes the line to a timing proxy rather than a distinct defect.
`ChaosChurnStormIT`'s `Calibration status` javadoc is the standing record - grep that class for
`already answered, do not re-derive`.

**The discriminator, for anyone re-reading a future firing:** a run whose backlog *drains* reproduces
a known result and is not news. A run that stays **flat**, or advances and then stops short of the
backlog, is the finding worth reporting.

## The method worth keeping - two instrument choices that were both wrong first

**1. More firings beat more seeds.** The first attempt to firm up a single-firing result hunted for a
second *seed* and found none in eight tries, leaving the question exactly where it started. That was
the wrong instrument: the known seed already fires most runs, so the cheap axis was **repetitions of
the known trigger**, collecting each firing's recovery trajectory. A seed hunt answers "can this
happen elsewhere"; a firing count answers "what does it do when it happens", and only the second
question was open.

**2. A stopping condition must be the ANSWER, never the engagement.** An earlier run stopped as soon
as the recovery diagnostic switched on and reported no answer at all. The diagnostic engaging proves
the *wiring*, not the outcome - and because the seed reproduces on most runs but not all, a clean run
is ordinary luck rather than evidence. The stopping condition has to be a violation with its
trajectory captured.

## The trap that voids a cross-tree comparison

The recovery diagnostic was lifted into `ChaosScenarioBase` at a known point. On any tree that
predates the lift, `-Dchaos.diagnoseStallRecovery=true` is **accepted and does nothing**: those runs
fail with `errors=1` in the failsafe report but emit no telemetry, and a grep for violations reads
that silence as "did not fire". Comparing an old tree against a new one therefore needs the lift
backported first - otherwise the old arm looks clean because it was never instrumented.

That is the same shape as every other silent-no-op this repo has paid for. The general rule is
written up under `docs/solutions/best-practices/` as *silence from an instrument that could not have
spoken is not evidence* - deliberately named rather than linked, because it arrives with
astubbs/parallel-consumer#29 and is not on this branch yet.

## Why this document exists at all

The two unattended runners that carried this method - a hunt for the drain/flat answer and a
confirmation runner for the demotion - were **retired** when the question was answered, under
`bin/AGENTS.md`'s rule that an experiment whose question is settled has its method written up here
and its script deleted. This is that write-up.

The retirement was prompted by a code review pointing out the contradiction rather than by anyone
revisiting the scripts: the branch's own calibration record said "answered, do not re-derive" while
the runners still advertised the question as open and dispatchable. **An instrument that outlives its
question does not look retired - it looks live**, and the next person spends a chaos run re-deriving
a known result.

To ask it again anyway - a second tree, or many more firings - the recipe is:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dit.test=ChaosChurnStormIT \
      -Dchaos.diagnoseStallRecovery=true -Dfailsafe.failIfNoSpecifiedTests=false

and read the trajectory after the violation: climbing consumption means drained, flat means wedged.
