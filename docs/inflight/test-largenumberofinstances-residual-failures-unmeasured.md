# `largeNumberOfInstances`: the claim that the residual failures are Kafka's has never been measured

## The claim

`MultiInstanceRebalanceTest.largeNumberOfInstances` documented its acceptance as *80%+ pass rate
(currently ~90%)*, and its javadoc attributes the residual failures to the broker rather than to PC:

*(Update 2026-08-18: the test has since been split - the rate is now explicitly the profile's
measured OUTPUT rather than an acceptance gate, and a deterministic correctness twin,
`scriptedChurnRoundsCompleteWithoutStall`, gates in the integration lane. The aggressive profile's
parameters and churn are unchanged, deliberately, so the residual rate this note is about remains
the baseline to measure against.)*

> the remaining ~10% failure is the Kafka consumer group protocol under extreme membership churn
> (`assignedPartitions=0`), not a PC bug

## Why that needs settling before the rate is tuned

**It is asserted, never measured.** No experiment separates "the group coordinator cannot converge at
this churn rate" from "PC has a defect that only appears at this churn rate", and the two produce the
same visible outcome: instances alive, assignment empty, no progress.

That matters more than it looks, because the obvious response to a flaky stress test is to back the
parameters off until it passes - and if any part of the residual is PC's, backing off **hides a real
defect** rather than removing a confound. That is precisely the shape that let the confluentinc#857
deadlock survive four months: astubbs/parallel-consumer#68 gave every test an uncontended broker, the
suite went green, and the defect was untouched.

## What would settle it

A control arm. Same churn against a plain `KafkaConsumer` group with no PC in the path, or PC
instrumented to distinguish "coordinator never assigned us partitions" from "we were assigned and
made no progress". If the bare consumer group fails at the same rate, the claim holds and the
parameters are simply past what Kafka converges at. If it does not, the residual is ours.

Until then the javadoc should say the claim is unverified rather than state it as fact - and it now
does.

**Update 2026-08-18: the capacity profiles now take a scale factor**, `-Dperf.scale=<n>`, which makes
a cheaper version of that control arm available before anyone builds a bare-consumer harness. If the
residual is the coordinator failing to converge at this churn rate, the failure rate should move with
scale; if it is a PC defect, a defect does not care how many partitions there are. That is weaker
than the bare-consumer arm and does not replace it - a rate that moves with scale is consistent with
both a coordinator limit and a load-sensitive PC bug - but a rate that does NOT move with scale is
hard to explain as "Kafka cannot converge at this size", and it costs one flag rather than a new
harness. Correctness profiles deliberately cannot read the factor, and a guard enforces it
(`onlyCapacityProfilesMayScale`).

## Related

- `docs/solutions/workflow-issues/prove-the-problem-exists-before-writing-the-fix.md`
- `docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md` -
  the astubbs/parallel-consumer#68 precedent, where an infrastructure change made a suite green
  without fixing anything
