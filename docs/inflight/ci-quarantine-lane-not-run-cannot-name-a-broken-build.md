# The quarantine lane's "not run" row cannot say the lane's build broke

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

`bin/quarantine-lane-report.sh` classifies a quarantined test as `NOT_RUN` when no surefire or
failsafe XML names it, and the row reads *"⚪ not run - report missing - check the lane job"*. **That
row is produced identically whether the test was not selected, its module's build failed, or the
whole maven run died before the test phase** - and the reader it sends to the lane job finds a GREEN
tick, because the run step is `continue-on-error` by design.

## The worked case, which is where this was found

On astubbs/parallel-consumer#29, the first quarantine-lane run after
`MultiInstanceRebalanceTest.largeNumberOfInstances` was quarantined reported it as `NOT_RUN`. The
report was correct: that run's maven invocation ended `BUILD FAILURE` inside a minute, before the
integration tests, so no failsafe XML for it existed. `ProducerManagerTest` was still classified
`FAILED`, because core's surefire reports *had* been written - so "were there any reports at all"
would not have discriminated either.

The lane job was green throughout, the comment said "check the lane job", and the next push produced
a working build and quietly rewrote the same comment to say "passed (flapper)". Nothing recorded that
the row had ever said anything else. That half is fixed - the report now posts a fresh comment when
an outcome changes, and stamps every comment with the commit and run it describes - but the row
itself still cannot name a broken build.

## Why this is a decision rather than a fix

The obvious signal does not work. `steps.run.outcome` is `failure` on almost every run of this lane
*by design*: the quarantined tests are expected to fail, and `continue-on-error` is what keeps that
from gating. So it cannot separate "a quarantined test failed" from "the build did not compile".

"No reports at all" does not work either, as the case above shows: a partial build writes some.

What would work is a signal the reporter does not currently have - the maven run's own exit status
distinguished from the test outcomes, or a per-module reached-the-test-phase marker. Choosing which
is a design call, and it belongs with whoever next changes the lane.

## Not to be confused with

This is not about which tests the lane selects, and not about the quarantine on
`MultiInstanceRebalanceTest.largeNumberOfInstances` itself - both belong to
astubbs/parallel-consumer#29. The registry entry and the annotation are correct; the reporting of an
absent report is what is underspecified.
