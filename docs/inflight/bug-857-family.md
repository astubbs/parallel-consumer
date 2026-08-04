# Upstream #857 family - what is still open

Three distinct defects sit behind upstream's one "paused consumption after rebalance" symptom.

**Landed:** #100 (a mid-rebalance commit threw `RebalanceInProgressException`, which nothing caught,
permanently killing the broker-poll thread) and #80 (a draining consumer never called
`consumer.poll()` - ~10kHz busy-spin plus a rebalance-unresponsive member zombie-holding its
assignment). Write-ups in `docs/solutions/test-flakiness/`.

**Still open: the original deadlock, in #29** - `synchronized(commitCommand)` between the poll thread
(`onPartitionsRevoked`) and the control thread (`commitOffsetsThatAreReady`), replaced there with
`ReentrantLock.tryLock()`. A sibling of the two landed fixes, not a duplicate: #29/#31 were verified
*not* to fix the drain defect, and the uber-branch experiment showed the #80 stack composes cleanly
with both. Live confirmation the deadlock is still present: `RebalanceEoSDeadlockTest` failed once
under the 20-run stress hunt (see `test-load-tightness-flakes.md`, where it is explicitly *not* a
member). #29 needs a rebase and a retarget first - see `pr-blockers-and-collisions.md`.

**Gated on #29: proving thread-parallel integration tests are safe again.** #68 made the integration
suite reliable by *forking* per broker (`forkCount=4`), which sidesteps the deadlock rather than
proving it gone - the contended `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` failure it was hiding is
the real upstream #857 bug. The deferred "Step 2" is to re-run with `-Dparallel-tests=true` on a
shared broker **after #29 lands** and see whether it stays green. One probe on the highcpu runner
hinted it might (forked unit suite green with threads enabled; the integration red was the separate
`PartitionStateCommittedOffsetIT` flake, since fixed by #80), but one green run is not proof. Forking
stays the default regardless: fork×threads measured no faster than fork alone, because forking already
saturates the cores.
