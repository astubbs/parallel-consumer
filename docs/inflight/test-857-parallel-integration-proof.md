# Gated on astubbs#29: proving thread-parallel integration tests are safe again

The deferred "Step 2" experiment. Separated from the confluentinc#857 sighting records because it is
a *work item with a trigger*, not an observation: it cannot start until astubbs#29 lands.

**Gated on astubbs#29: proving thread-parallel integration tests are safe again.** astubbs#68 made the integration
suite reliable by *forking* per broker (`forkCount=4`), which sidesteps the deadlock rather than
proving it gone - the contended `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` failure it was hiding is
the real confluentinc#857 bug. The deferred "Step 2" is to re-run with `-Dparallel-tests=true` on a
shared broker **after astubbs#29 lands** and see whether it stays green. One probe on the highcpu runner
hinted it might (forked unit suite green with threads enabled; the integration red was the separate
`PartitionStateCommittedOffsetIT` flake, since fixed by astubbs#80), but one green run is not proof. Forking
stays the default regardless: fork×threads measured no faster than fork alone, because forking already
saturates the cores.

**Scope caveat added 2026-08-18.** This experiment can only prove something about the mode astubbs#29
fixes. `RebalanceEoSDeadlockTest` runs `PERIODIC_TRANSACTIONAL_PRODUCER`, where the AB-BA cycle
cannot occur - so it is the wrong probe for astubbs#29's fix, and green does not mean the deadlock is
gone. Fix the test's mode and its latch target first, or the experiment measures nothing. See
`bug-857-family.md`.
