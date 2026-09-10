# Draft response to astubbs#237 (confluentinc#53) - a decision now waits on it

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->

**Not posted.** Post only on explicit instruction, and delete this file when it is posted - never when
the PR that wrote it lands.

**What is already said on the issue, and must not be repeated:** that the threshold approximation
(confluentinc#47) is what shipped, that the 0.75 multiplier is its price, and that the evidence to
justify exact encoding would be a workload where the margin measurably costs throughput.

---

The UX modernisation requirements (the plan in `docs/plans/2026-09-09-002-feat-ux-modernisation-plan.md`)
add a second reason to want this beyond throughput. Park in place keeps an exhausted record incomplete in
the offset map and exports it to a dead-letter topic only when a partition's payload reaches a declared
fraction of the metadata cap. That fraction has to sit below the pressure threshold, or the partition
stops taking work before the export ever fires, so the default is seventy percent, five points below, and anything at or above
the threshold is refused at definition time. Both numbers are chosen against today's estimate, not
against the size the payload really has.

With exact encoding the margin below the cap shrinks, the threshold can rise, and the export default
should be re-derived from the precise size. `docs/inflight/core-237-continuous-offset-encoding.md` lists
what is provisional on the approximation, so landing this issue is the trigger to revisit it.
