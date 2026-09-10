# Exact continuous offset encoding - what is provisional on today's approximation

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->

[astubbs#237](https://github.com/astubbs/parallel-consumer/issues/237) (confluentinc#53) asks for
the offset map to be encoded continuously, so a partition's commit-metadata payload is a known size
at all times rather than a budgeted estimate. The issue carries the fork status, the prior upstream
branch and the evidence bar for doing it; do not restate them here.

**What this note carries is the list of decisions taken against the approximation that stands in
for it, so that landing exact encoding is also the trigger to revisit them.** The approximation is
the pressure threshold: a partition stops taking work at a fixed fraction of the metadata cap
(`PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT`), and the margin below the cap is
the price of not knowing the exact size.

## Decisions that assume the approximation

- **The UX modernisation plan's export fraction (R27 of
  `docs/plans/2026-09-09-002-feat-ux-modernisation-plan.md`).** Parked records are exported to a dead-letter destination when a partition's
  payload reaches a declared fraction of the cap. The default is sixty percent and a declared
  fraction at or above the pressure threshold is refused at definition time, both chosen only
  because the threshold is seventy-five percent and a fraction above it can never be reached. The
  owner's original number was eighty percent, "when offset capacity nears its end". With exact
  encoding the margin shrinks or disappears, the threshold can rise, and the default should be
  re-derived from what "nears its end" means against a precise size - flagged for revisiting by the
  owner on 2026-09-10 during that plan's third review round.
- **Whether the threshold becomes a per-instance setting.** The same plan records raising the
  threshold per instance as a small-tier engine change that could keep a higher default. Exact
  encoding makes that question moot in one direction or the other; decide it there, not twice.

## Where the number lives

`grep -rn 'USED_PAYLOAD_THRESHOLD_MULTIPLIER' --include=*.java .` finds the constant and the one
place it is read; the plan's Dependencies section cites the same constant. When exact encoding
lands, update both entries above and this note in the same PR, or `git rm` the note if nothing
provisional remains.
