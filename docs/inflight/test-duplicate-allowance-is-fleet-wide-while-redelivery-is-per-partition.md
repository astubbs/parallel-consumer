# The ledger's duplicate allowance is fleet-wide while redelivery is per-partition

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-state: open - found by a defect-class sweep, no red control attempted -->

<!-- post-merge: checked - astubbs#491 is cited as the merged PR that made this sweep, which stays true and stays findable after it lands -->
Found by the same-class sweep on astubbs#491, which closed the commit half of
[`test-per-shard-liveness-has-no-gate.md`](test-per-shard-liveness-has-no-gate.md) and then went
looking for **other detectors scoped to the fleet or the instance while the failure they watch is
scoped to a partition or a shard**. This is the one candidate that survived.

`ProgressProbe#ledger` counts duplicates across the whole run - `allConsumedKeysWithDuplicates.size()`
minus the unique set - and compares that one number to `disturbanceCount x perDisturbanceAllowance`,
capped at half of everything produced. Both sides are fleet-wide, so **one partition redelivering its
entire content repeatedly is masked by an allowance sized for the whole fleet**: with twenty
disturbances the raw allowance is 100,000 records, and no single partition's redelivery storm can
reach it on a 250,000-record run. The per-disturbance sizing is right for what it was written for (an
in-flight batch plus a commit interval, per instance, per disturbance); what it cannot do is notice
that the whole allowance was spent in one place.

<!-- post-merge: checked - same citation, same reason as above -->
**This is not the same defect as the one astubbs#491 fixed, and it is a weaker case.** The commit gap
was a liveness property with a named mechanism; this is a *bound* whose granularity does not match its
subject, and nothing has yet shown a run where it matters. What makes it worth a note rather than a
`refactoring.md` line is that the fix is a judgement call - a per-partition allowance needs a
per-partition disturbance count, which the conductor does not track - and that it should not be
attempted without a red control, for exactly the reason the note above records.

## Ruled out by the same sweep, with why

- **`NO_PROGRESS`** - fleet-wide by construction and correctly so; the per-instance case is
  `INSTANCE_STALL`'s and the per-partition case is now
  `UNCOMMITTED_COMPLETIONS/COMMIT_NOT_LANDING`'s.
- **`ZOMBIE_MEMBER/REBALANCE_BLOCKED`** - group-scoped, watching a group-scoped observable. A member
  blocking the join shows up in the group's own state, so the granularity matches the failure.
- **`DRAIN_OVERDUE`** - keyed by instance id, which is the granularity of the disturbance it bounds.
- **`KeyOrderLedger`** - already per key, partition, epoch and incarnation window; finer than the
  failure it watches.
- **`LEDGER_LOSS`** - per key, so a single partition losing records fails it regardless of the rest of
  the fleet. Only the DUPLICATE half of that ledger has the mismatch.
- **The instance-wide load gate** (`isSufficientlyLoaded` / queue-depth counting) is the same class in
  MAIN code rather than in the probes, and it is already owned elsewhere -
  `bug-119-load-gate-counts-blocked-work-as-available.md`. Not re-opened here.

## Delete when

Either a red control shows a run whose partition-scoped duplicate storm passes this ledger, and a
per-partition allowance replaces the fleet-wide one - or the judgement is recorded that a fleet-wide
duplicate bound is what the suite wants, in `docs/testing.md`, so the mismatch stops being folklore.
