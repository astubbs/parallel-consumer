---
title: "A capability absent by accident reads as absent by decision"
date: 2026-09-01
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "A test suite, tool, or subsystem does not cover a case and nobody has ever asked why"
  - "A surface looks parameterised - a mode, a flag, a strategy - and you are about to assume the unused values were considered"
  - "Deciding whether a gap in coverage is a decision someone made or a capability nobody built"
  - "A defect lives in the exact configuration your test suite happens not to exercise"
tags:
  - coverage-gaps
  - false-consensus
  - test-infrastructure
  - parameterisation
  - chaos-testing
---

## Context

The chaos suite in `parallel-consumer-core` builds fleets of consumer instances through
`ManagedPCInstance` and runs them through scripted rebalance scenarios such as
`AbstractRevokeUnderWorkScenario`. The fleet builder has always been parameterised by commit mode -
`config.commitMode` is threaded straight through to the `ParallelConsumerOptions` builder - so every
`CommitMode` value looked equally reachable from the outside.

It was not. `PERIODIC_TRANSACTIONAL_PRODUCER` requires a producer to be supplied to the options
builder, and `ManagedPCInstance` never wired one for any mode - so any scenario requesting
transactional mode would have failed at construction, not run and passed. Independently,
`AbstractRevokeUnderWorkScenario` had the mode written inline at its one builder call site as
`CommitMode.PERIODIC_CONSUMER_SYNC`, with no accessor at all
rather than exposing it as an overridable hook. Two separate closures, on two separate axes, both
landed on the same gap: nobody had ever asked this suite to run the one mode that needed a producer.

The cost was not hypothetical. `astubbs/parallel-consumer#44` (mirroring
`confluentinc/parallel-consumer#803`) is an issue carrying upstream's verified-bug label, and it
is in transactional mode. The unbounded revoke wait tracked in
`docs/inflight/bug-857-transactional-revoke-wait.md` is in the same mode. The chaos suite exists
specifically to hunt this defect family under rebalance pressure, and for months it could not enter
the one mode where the family's only externally-confirmed member lives.

## Guidance

When something in a codebase looks parameterised, don't take the parameter's existence as proof the
range is reachable - check that at least one caller actually exercises each value. A parameter fed by
exactly one live value at every call site is a constant wearing a config key.

The diagnostic question is counterfactual, not archaeological: **if you requested the missing value
today, what happens?** If the honest answer is "it fails at construction" or "the field it needs was
never wired," the absence is a gap, not a decision - regardless of how deliberate the surrounding code
looks. Contrast that with an absence that has a decision attached to it: a comment, a rejected
alternative recorded somewhere, a note explaining why the excluded case doesn't apply. A real decision
leaves a trace. An accidental one leaves nothing, and the surrounding structure - options builders,
enums, config plumbing - fills that silence with the appearance of having been considered.

When you close a gap like this, widen the capability at the narrowest point that reaches it. Here that
meant: wire the producer only inside the `if (config.commitMode == PERIODIC_TRANSACTIONAL_PRODUCER)`
branch of `ManagedPCInstance`, and turn that inline literal into an overridable `commitMode()` that
still defaults to the original value. Every existing scenario - eager and cooperative, sync and async
commit - is byte-for-byte unchanged, because none of them takes the new branch or overrides the new
hook. The new scenario, `ChaosRevokeUnderWorkTransactionalIT`, varies exactly one term from the
existing eager arm (`ChaosRevokeUnderWorkIT`): the commit mode, not the assignor, not the timing, not
the scenario shape. That is what makes the new arm a controlled addition instead of a parallel suite
that has to be recalibrated from scratch - the existing arms' calibration (seed protocol, timing
thresholds, probe tuning) is the expensive part of this suite and is worth protecting.

Closing the reachability gap is not the same as proving the detector works. `ChaosRevokeUnderWorkTransactionalIT`'s
own javadoc records its first run as green in 144 seconds with `probe violations=[]`, and states
explicitly that this proves only that the scenario *runs* in this mode - the fleet starts, the
conductor churns it, the probe reports. It does not establish whether the scenario goes red on an
unfixed tree, red only under a particular timing, or stays green because this scenario family's
revoke pressure isn't shaped right to trip the wait. See
`docs/solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md`
for the general form of that caution: a clean result from an instrument that has never been shown
capable of firing is not evidence of health, it's an unarmed detector. The next step for anyone
picking this up is to make the scenario fail on the pre-fix composition before trusting a green run
from it anywhere.

## Why This Matters

An accidental gap in test or scenario coverage is worse than a documented one, because it doesn't
trigger anyone's "should we look at this?" reflex. A missing test that everyone can see is missing
gets flagged in review. A missing test that *looks* like a parameterised, deliberately-scoped
exclusion doesn't - it reads as settled, so nobody re-opens it, and the confidence that "the suite
covers this" becomes false without anyone asserting it. The gap here specifically excluded the suite
from the one mode containing the family's only upstream-verified defect and an already-known unbounded
wait - meaning the coverage looked complete exactly where it mattered least to be wrong.

## When to Apply

- Auditing test suites, fixture builders, or scenario matrices where a dimension (mode, flag,
  backend, region) is threaded through config but you haven't verified every value is actually
  constructible end to end.
- Reviewing a PR that adds a new enum value or config option to an existing parameterised system -
  check whether the plumbing that consumes it was updated everywhere the plumbing that reads it
  exists, or whether only the read side moved.
- Investigating "why don't we test X" questions. Before accepting "we decided not to" as the answer,
  try to construct X today and see whether it actually fails at setup.
- Any coverage gap that lines up with a known defect family - if the gap and the bug are in the same
  mode/branch/configuration, that's a strong signal the gap is what let the bug go unverified by your
  own tooling, not a coincidence of scope.

## Examples

- `ManagedPCInstance` (`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/utils/ManagedPCInstance.java`):
  `config.commitMode` was passed to `ParallelConsumerOptions` for every mode, but a producer was
  wired only conditionally - `if (config.commitMode == CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)` -
  added specifically to close this gap without touching any other mode's construction path.
- `AbstractRevokeUnderWorkScenario`: the mode was previously an inline literal with no accessor;
  `commitMode()` now exists and defaults to
  `CommitMode.PERIODIC_CONSUMER_SYNC`; now an overridable protected method with that same value as
  its default, so every pre-existing subclass is unaffected.
- `ChaosRevokeUnderWorkTransactionalIT` is the first scenario to override it, returning
  `CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER`, and its javadoc is explicit that the scenario's first
  green run demonstrates reachability only, not health - the calibration status is recorded as
  "UNCALIBRATED" in the class doc itself.
- The cost of the gap: `astubbs/parallel-consumer#44` (confluentinc/parallel-consumer#803), upstream's
  labelled a verified bug upstream, and `docs/inflight/bug-857-transactional-revoke-wait.md`,
  both live only in the mode the suite could not reach.
- Related pattern, cite rather than duplicate:
  `docs/solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md`.

## Where this sits among its siblings

This repo already documents three failures whose common shape is *an absence that reads as health*,
and this is the fourth face of it. They are kept apart because the question each one makes you ask is
different, and collapsing them would lose that:

- [`an-inert-analysis-config-reads-as-a-clean-codebase.md`](../workflow-issues/an-inert-analysis-config-reads-as-a-clean-codebase.md)
  \- the CONFIG is present and valid and never reaches the run. Ask: did the tool read what I wrote?
- [`a-check-that-reports-success-without-having-run.md`](../workflow-issues/a-check-that-reports-success-without-having-run.md)
  \- the CHECK reports success without executing. Ask: did this run at all?
- [`negative-results-need-an-instrument-that-could-have-said-yes.md`](../workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md)
  \- the SEARCH returns nothing and the nothing is believed. Ask: could this query have found it?
- [`silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md`](silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md)
  \- the DETECTOR is quiet because it could not fire. Ask: could this have gone red?

This one is upstream of all four, because there is no output to distrust. **Nothing ran, nothing
reported, and nothing was configured wrongly - the capability was never built, and its absence was
read as a choice.** The question it makes you ask is: *would this even work if I asked for it today?*

If a fifth face of this shape turns up, that is the point to consolidate the family into one doc with
four sections rather than add to the row.
