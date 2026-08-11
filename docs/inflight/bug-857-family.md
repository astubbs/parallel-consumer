# The confluentinc#857 family - what is still open

Three distinct defects sit behind upstream's one "paused consumption after rebalance" symptom.

**Landed:** astubbs#100 (a mid-rebalance commit threw `RebalanceInProgressException`, which nothing caught,
permanently killing the broker-poll thread) and astubbs#80 (a draining consumer never called
`consumer.poll()` - ~10kHz busy-spin plus a rebalance-unresponsive member zombie-holding its
assignment). Write-ups in `docs/solutions/test-flakiness/`.

**Still open: the original deadlock, in astubbs#29** - `synchronized(commitCommand)` between the poll thread
(`onPartitionsRevoked`) and the control thread (`commitOffsetsThatAreReady`), replaced there with
`ReentrantLock.tryLock()`. A sibling of the two landed fixes, not a duplicate: astubbs#29/#31 were verified
*not* to fix the drain defect, and the uber-branch experiment showed the astubbs#80 stack composes cleanly
with both. Live confirmation the deadlock is still present: `RebalanceEoSDeadlockTest` failed once
under the 20-run stress hunt (see `test-load-tightness-flakes.md`, where it is explicitly *not* a
member). astubbs#29 needs a rebase and a retarget first - see `pr-blockers-and-collisions.md`.

**Second live confirmation, 2026-08-11: the chaos probe caught the stall directly.**
`ChaosRevokeUnderWorkIT.revokeUnderWorkStaysProtocolHonest` (the **eager** variant) was killed
fail-fast by `ProgressProbe` with five simultaneous violations, on
[job 93666671951](https://github.com/astubbs/parallel-consumer/actions/runs/31454939035/job/93666671951):

```
CLASS2_STALL/LAG_STAGNATION: partition ...-44 lag=2324, committed offset stagnant at 817 for 154s
(bound 150s) - protocol-invisible stall: group STABLE + heartbeats flowing, yet this partition's
backlog is going nowhere
```

Partitions 28/34/38/44/46/57, lags 2041-2757, all stagnant at ~154s. **Replay seed
`4734674029169027864`**, which is the part no command can recover once the log expires:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=4734674029169027864

This is a stronger datapoint than the `RebalanceEoSDeadlockTest` sighting above, and worth reading
before assuming the two landed fixes closed the symptom. The probe's own vacuity caveat does not
apply - 154s against a 150s bound with thousands of records of lag is a detector that genuinely
fired, not a `probe clean` with nothing to see. The **cooperative** variant passed in the same run
(`probe violations=[]`), so whatever remains is eager-protocol-specific, which is where astubbs#29's
`onPartitionsRevoked` / `commitOffsetsThatAreReady` contention lives. Seen on astubbs#224, a
docs-and-CI-scripts branch that touches no product code, so the branch is not a suspect; the chaos
suite randomises its seed per run, so other branches passing the same day only means their seeds
did not draw this interleaving.

**Gated on astubbs#29: proving thread-parallel integration tests are safe again.** astubbs#68 made the integration
suite reliable by *forking* per broker (`forkCount=4`), which sidesteps the deadlock rather than
proving it gone - the contended `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` failure it was hiding is
the real confluentinc#857 bug. The deferred "Step 2" is to re-run with `-Dparallel-tests=true` on a
shared broker **after astubbs#29 lands** and see whether it stays green. One probe on the highcpu runner
hinted it might (forked unit suite green with threads enabled; the integration red was the separate
`PartitionStateCommittedOffsetIT` flake, since fixed by astubbs#80), but one green run is not proof. Forking
stays the default regardless: fork×threads measured no faster than fork alone, because forking already
saturates the cores.
