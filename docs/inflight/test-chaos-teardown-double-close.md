# Chaos teardown can still double-close a PC, and can orphan one after the fleet is "settled"

The double-start race inside `ChaosConductor`'s draw loop is fixed (astubbs/parallel-consumer#292:
single-flight `start()`, `stopRequested` abort, `closePending` CAS). Three independent reviewers on
that PR then found the *same failure class* reachable through the teardown path, which #292 does not
touch. Recorded here rather than folded into that PR: these are pre-existing defects in
`ChaosScenarioBase`/`ChaosConductor`, and fixing them changes teardown behaviour, so they want their
own change and their own broker-backed validation.

All line numbers are as of `d35f4629`; grep the named methods rather than trusting them.

## 1. `settleFleet` bypasses the one-closer-per-PC guard

`ChaosScenarioBase.settleFleet` (grep `let any stopAsync background close finish first`) waits up to
15s on `pc.isClosePending()`, then calls `pc.getParallelConsumer().close()` **regardless of whether
that wait timed out**. It reaches past `ManagedPCInstance` to the raw PC, so the `closePending` CAS
never sees it - it is a check-then-act, not a lock.

`stopAsync()`'s own javadoc says a real close takes 30-40s. So a close still running at 15s puts the
main test thread inside `close()` while `pc-close-N` is also inside it: two threads, one
`KafkaConsumer`, which is exactly the `ConcurrentModificationException` #292 fixed for the
conductor's own draws. The per-instance `try/catch` swallows it to a WARN, but the resulting
unclassified failure cause can then trip `assertScenarioSlos` - so this teardown race can fail a
scenario on its own.

## 2. `STOP_DRAIN` never engages `closePending` at all

`ChaosConductor.doStopDrain` (grep `chaos-drain-`) calls
`victim.getParallelConsumer().closeDrainFirst()` on its own thread, never going through
`ManagedPCInstance.stopAsync()`. `closePending` is therefore never set for a drain stop, so
`settleFleet`'s wait above does not even wait - it reads `false` immediately and proceeds to close.
`joinDrainers` gives *all* outstanding drainers one shared 60s budget and logs
`still running after 60s join budget - teardown may race them`, conceding the same window.

## 3. A `run()` merely queued at teardown can orphan a live PC

`settleFleet` makes one linear pass over the fleet, reading `pc.getParallelConsumer()` at that
instant. It never consults `startInFlight`, and it never calls `stop()`/`stopAsync()`, so it never
sets `stopRequested`. A `RESTART`/`JOIN_NEW` whose `run()` is still queued on the work-stealing pool
therefore sees `stopRequested == false` when it finally executes - after the sweep - and builds a
brand-new `KafkaConsumer` that nothing will ever close. `pcExecutor.shutdownNow()` only interrupts
what is running at that moment.

This is the orphaned-group-member failure #292 fixes, relocated to the teardown boundary. Note #292's
"residual 3" understates it: the consequence is an unclosed, still-polling PC, not a stranded flag.

## 4. `settleRun` has no `try`/`finally`

`settleRun` runs `conductor.stop()`, `probe.stop()`, `producerThread.join(10_000)`,
`settleFleet(...)`, `pcExecutor.shutdownNow()` as five bare sequential statements, and propagates
`InterruptedException` from the `join`. An interrupt there (a test-level timeout, say) skips both
`settleFleet` and the executor shutdown, leaking consumer-group members and pool threads for the rest
of the forked JVM - which failsafe shares across IT classes, so it strands resources for unrelated
scenarios too.

## What this means for triage

If a chaos IT throws `ConcurrentModificationException` again after #292 lands, do **not** read it as
that fix having failed. Look first at whether a drain exceeded the 60s shared join budget, or a
`stopAsync()` close exceeded `settleFleet`'s 15s wait. Both reproduce the original signature through
paths #292 deliberately left alone.

## Related

- `bug-857-family.md` - the `ZOMBIE_MEMBER`/`REBALANCE_BLOCKED` signature these produce is the same
  one that file tracks. #292 confirmed one of its sightings was the harness, not the product; these
  paths are how the harness could still produce it.
