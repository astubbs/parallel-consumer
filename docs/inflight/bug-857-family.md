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
with both. astubbs#29 needs a rebase and a retarget first - see `pr-blockers-and-collisions.md`.

## Commit mode decides which defect can explain a sighting

Added 2026-08-18, after a re-read found the mode recorded for **none** of the six sightings this file
used to hold - which let a transactional-mode failure be logged as "live confirmation" of a cycle
that cannot occur in that mode. Mode is the discriminator:

| Mode | astubbs#29's AB-BA cycle | Transactional revoke wait |
|---|---|---|
| `PERIODIC_CONSUMER_SYNC` | **can close** - `commitAndWait()` blocks on the response queue | no - gated on transaction mode |
| `PERIODIC_CONSUMER_ASYNCHRONOUS` | no - `commit()` falls through to `requestCommitInternal()` and never blocks | no |
| `PERIODIC_TRANSACTIONAL_PRODUCER` | no - `ConsumerOffsetCommitter` is never constructed | **yes** - `:418-419` spins unbounded |

**Record the commit mode with every future sighting.** It is one line and it is what makes a sighting
decidable.

## astubbs#29's own reproducer cannot currently settle anything

`RebalanceEoSDeadlockTest` fails 5/5 in CI on that branch for two reasons independent of the
deadlock, either of which alone is sufficient:

- **Wrong mode** - it runs `PERIODIC_TRANSACTIONAL_PRODUCER`, where the cycle cannot occur.
- **Unreachable latch** - it counts a latch by overriding `commitOffsetsThatAreReady()`, but the
  revoke path on that branch calls the private `tryCommitOffsetsOnRevoke()` instead. It would fail
  against a perfect fix.

The `tryLock()` contended arm has never been observed executing: its INFO skip-log appears **zero**
times in the 741,161-line CI log of the run meant to prove it.

## Cluster decomposition and the A/B result

astubbs#29's production diff is four independent changes in one April commit. The decomposition, the
2026-08-18 A/B soak that measured the deadlock fix (master 20/20 fail vs branch 0/20), and the order
to take them are in
`docs/plans/2026-08-18-002-fix-857-revoke-path-cluster-decomposition-plan.md`.

## The sightings, split out by mode

One file per item, per this directory's rules:

- `test-857-revoke-under-work-sightings.md` - `PERIODIC_CONSUMER_SYNC`; the only sightings
  mode-compatible with astubbs#29.
- `test-857-churn-storm-async-stalls.md` - `PERIODIC_CONSUMER_ASYNCHRONOUS`; **unexplained by every
  known defect**, and the strongest single-arm evidence the family holds.
- `bug-857-transactional-revoke-wait.md` - `PERIODIC_TRANSACTIONAL_PRODUCER`; a separate open defect
  with a user report (astubbs#44, confluentinc#803), not astubbs#29's.
- `test-857-parallel-integration-proof.md` - the Step-2 experiment, gated on astubbs#29 landing.

**Not one of the five captured seeds has ever been replayed**, on any box, contended or not. Each
sighting names the replay as its deciding experiment. That is the family's cheapest open work.

## Deleting these files

Per sighting, when its seed has been replayed and the result explained - not on a release, and not on
astubbs#29 merging. astubbs#29 landing resolves at most the two consumer-sync sightings, and only if
their replays confirm.
