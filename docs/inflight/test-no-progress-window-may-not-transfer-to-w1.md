# The 30s fleet-progress window may not transfer to W1 either - a possible third instance

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

Two chaos detectors have now been found asserting a timing bound that the scenario's own disturbances
legitimately cross - `CLASS2_STALL` (demoted to an observation) and `REBALANCE_DWELL` (disarmed in
W5). **`NO_PROGRESS` is the open candidate for a third, and it has already fired.**

## The evidence, which is not mine - and it is no longer a single sighting

**Every one is `ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger`, every one at exactly the
bound, every one in the tail.** The first pair were recorded independently, by different pieces of
work, neither looking for this. The rest come from an unattended overnight soak that was not looking
for it either:

| Source | Observation | Seed |
|---|---|---|
| astubbs/parallel-consumer#348, in `test-chaos-autopsy-omits-fleet-violations.md` | `fleet consumed count stuck at 98804/100000 for 30s (bound 30s)` | not recorded |
| `bug-857-family.md`'s fourteenth sighting (from astubbs/parallel-consumer#347) | `NO_PROGRESS, fleet stuck at 97896/100000` - 30s against a 30s bound | **`1521825993857670757`** |
| Torture soak 2026-08-29, cycle 51 (`bin/torture-overnight.sh`) | `fleet consumed count stuck at 97386/100000 for 30s (bound 30s)` | **`87978223167568`** |
| Torture soak 2026-08-29, cycle 166 | `fleet consumed count stuck at 97297/100000 for 30s (bound 30s)` | **`106062481479157`** |

**The soak gives this line its first RATE, and its first control arm.** `NO_PROGRESS` killed
roughly one `ChaosChurnStormIT` cycle in twenty, on an otherwise idle desktop, while the other four
chaos scenarios went the whole night without a single failure of any kind. So this is not ambient
load - it is specific to W1. The per-scenario tallies are in that run's `SUMMARY.md`
(`~/pc-soak-runs/`, machine-local); `grep 'END ' tally.tsv` reproduces them.

**Both soak firings sit further past `TAIL_SLACK` than the originals** - the outstanding counts are
in the table above, against a slack of 500. If the reading is "the guard for exactly this case is set
too tight", the soak widens the gap it has to cover rather than narrowing it.

**Neither soak firing can settle the question, and it is worth being exact about why.** The run did
not pass `-Dchaos.diagnoseStallRecovery`, so both aborted at the violation and neither recorded
whether the fleet recovered. They are seeds, not answers. Replaying them with the diagnostic engaged
is now the cheapest experiment on this line - each reproduces in about two and a half minutes.

**Not one of the four was looking for this, which is why they are worth something.** astubbs#348's
note is about a **reporting** defect - the ambient autopsy printed `violations (0)` for its run,
because fleet-scoped detectors cannot be re-derived by the ambient probe, which has no consumed-count
supplier. The fourteenth sighting records its occurrence as one arm among several on a branch it was
clearing of suspicion. The two soak firings are incidental output of an unattended rotation. This
note exists so the calibration question is not lost between them.

## Why it looks like the same class

- **W4 widens this window and W1 does not.** `AbstractRevokeUnderWorkScenario` calls
  `withNoProgressWindow(Duration.ofSeconds(60))`, because "storm-phase rebalances can legitimately
  pause much of the fleet for up to the eviction horizon (all of it, under the eager assignor)".
  `ChaosChurnStormIT` runs continuous churn against the 30s default.
- **It fired in the tail, just past the slack.** 98804 of 100000 consumed leaves 1196 outstanding,
  against a `TAIL_SLACK` of 500 - so the guard that exists for exactly this case missed it by ~700
  records. A tail of heavy-tailed records legitimately sleeping in flight is the shape that guard
  describes.
- **The bound was crossed by nothing** - 30s against a 30s bound. The same "bound meeting the load"
  reading the other two instances turned out to have.

## Why it is NOT yet called a third instance

Nobody has replayed that seed, and no control arm exists. The alternative reading is a genuine
fleet-wide stall, which is exactly what this detector is for and would be the most interesting
outcome in the whole family. **Do not demote or widen it on the argument above** - that argument is
pattern-matching, and the same reasoning applied to `CLASS2_STALL` took a replay to settle.

## What would settle it

Replay `1521825993857670757` with the fleet allowed to continue past detection, and read whether
consumption resumes. Drains -> calibration, and W1 wants the same widening W4 has. Stays flat ->
this is the fleet-level stall the family has been hunting, and it is a much better lead than any
`CLASS2_STALL` seed in [`bug-857-family.md`](bug-857-family.md).

**Three seeds are now available for that experiment, not one** - the original plus `87978223167568`
and `106062481479157` from the soak. That matters because
[`test-857-churn-storm-async-stalls.md`](test-857-churn-storm-async-stalls.md)'s "ANSWERED" section
rests on a single firing of a single seed and says so outright: *"a second firing, ideally on a
different seed, is what would put it beyond argument."* Two different seeds is what these are for.

A repeat soak now answers this without anyone reading a log: `bin/torture-overnight.sh` engages the
recovery diagnostic on every cycle and prints a drain verdict per cycle.
[`test-857-churn-storm-async-stalls.md`](test-857-churn-storm-async-stalls.md) **owns that
mechanism** - what it records, and the instrument defect its first version repeated.

## Related

- astubbs/parallel-consumer#348 carries the reporting half and the seed
- [`bug-857-family.md`](bug-857-family.md) - the 2026-08-25 entry, for how the other two instances
  were settled and the replay method that settled them
- [`test-chaos-phase2.md`](test-chaos-phase2.md) - the roster this belongs to once resolved
