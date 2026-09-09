# The 30s fleet-progress window may not transfer to W1 either - a possible third instance

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-vetted: 2026-09-09 - the calibration asymmetry is still unchanged in the tree (ProgressProbe's NO_PROGRESS window is still 30s and TAIL_SLACK still 500, AbstractRevokeUnderWorkScenario still calls withNoProgressWindow(Duration.ofSeconds(60)), ChaosChurnStormIT still takes the default), and the 2026-09-09 row below is new evidence. PROPOSED partly-true, for the owner: "What would settle it" and "Nobody has replayed that seed, and no control arm exists" are out of date, and this note never took delivery of the answer. The deciding experiment HAS been run with the diagnostic engaged, on two seeds that are not in this table, and both DRAINED - test-857-churn-storm-async-stalls.md's "CONFIRMED, 2026-08-28" section replayed 9086872209853284830 six times and drained six for six, and its "Sighting, 2026-09-08" section replayed 5650361238717170909 from 93487 to 101070/100000 at an outstanding count of 6513 against a TAIL_SLACK of 500, larger than every row here. That second sighting says outright that this note owns the question; the pointer was written and never followed. The 2026-09-09 row is a third drain-side observation and the first on the gate's own configuration, but it is the weak form - no diagnostic, and churn stopped 10s after the firing. Owner-gated because the impact is misdirection: the demote-or-widen call this now supports is not an agent's to make. What the note says against it still stands as written - it forbids acting on the pattern-matching argument, and these are replays, not that argument -->

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
| PR lane, hosted runner, 2026-09-02 - astubbs/parallel-consumer#414 at `810a8b3ac` (a workflow-only branch: no Java differs from master) | `fleet consumed count stuck at 95209/100000 for 30s (bound 30s)` | **`2512758007437016849`** <!-- post-merge: checked - a PR number and a sha are durable; the row reads the same after the merge --> |
| PR lane, hosted runner, 2026-09-05 - astubbs/parallel-consumer#446 at `e46c4458e` (a docs-only branch: one markdown file differs from master, and the previous push had passed this lane) | `fleet consumed count stuck at 97992/100000 for 30s (bound 30s)`; run summary `consumed=98899`, so the fleet was still short of the total when the run was killed, and the diagnostic was not engaged | **`8064312734196519950`** <!-- post-merge: checked - a PR number and a sha are durable; the row reads the same after the merge --> |
| PR lane, hosted runner, 2026-09-07 - astubbs/parallel-consumer#459 at `ebd5e9a82` (changes only the instance-stall detector; the fleet-level path is untouched, and the same test passed on the branch's previous head an hour earlier and on a dozen other branches that hour - `bin/inflight.mjs codecov test churnStormMeetsSlosAndBalancesLedger`) | `fleet consumed count stuck at 97614/100000 for 30s (bound 30s)`, 50s into the run, [job 101613435463](https://github.com/astubbs/parallel-consumer/actions/runs/34079778694/job/101613435463) | **`8637977624689145046`** |
| PR lane, hosted runner, 2026-09-09 - astubbs/parallel-consumer#495 at `f631c221` (a docs-only branch: one README paragraph and its generator template, no Java, no pom, no workflow), `Chaos Pain Suite 4/4`, [job 102305607928](https://github.com/astubbs/parallel-consumer/actions/runs/34300208784/job/102305607928). The same test on the same lane over that hour is `bin/inflight.mjs codecov test churnStormMeetsSlosAndBalancesLedger`, which also carries the arms that hold every merge of 2026-09-09 and passed | `fleet consumed count stuck at 96632/100000 for 30s (bound 30s)`, 43s into the run - **and then it kept consuming**: the settle summary reads `consumed=99569`, so the outstanding count fell from 3368 at the firing to 431, *inside* the `TAIL_SLACK` of 500. Weaker than the replays, in two named ways - the recovery diagnostic was not engaged, so the counter compared is the scenario's ledger rather than the probe's own fleet count; and the conductor's churn phase ended 10s after the firing, so this is recovery-once-churn-stops, not the in-churn drain `-Dchaos.diagnoseStallRecovery` measures. The shard was re-run once on the identical commit and PASSED, which separates *always red* from *not always red* and establishes nothing about the rate - the suite reseeds per run, so the re-run did not replay this seed | **`3717713223451201639`** |
<!-- file-refs: N/A - the harness moved to branch test/overnight-torture-harness-v2; named here as the instrument that produced these runs, not as a file in this tree -->

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

**Not one of these was looking for this, which is why they are worth something.** astubbs#348's
note is about a **reporting** defect - the ambient autopsy printed `violations (0)` for its run,
because fleet-scoped detectors cannot be re-derived by the ambient probe, which has no consumed-count
supplier. The fourteenth sighting records its occurrence as one arm among several on a branch it was
clearing of suspicion. The two soak firings are incidental output of an unattended rotation. This
note exists so the calibration question is not lost between them.

**The 2026-09-02 firing adds two things and settles nothing.** It is the first on a hosted GitHub
runner since the soak, and it fired on a branch whose only change from master is workflow YAML - the
cleanest branch-independence control this line has had, since nothing in the product differed. Its
outstanding count at the kill is the largest recorded here, several times the slack rather than a
little past it. And the ambient autopsy for that run printed `violations (0)` with the fleet-level
`NO_PROGRESS` line sitting a hundred lines above it in the same log - the reporting defect
[`test-chaos-autopsy-omits-fleet-violations.md`](test-chaos-autopsy-omits-fleet-violations.md)
records, still live. The seed is in the table; the diagnostic was not engaged, so whether the fleet
drained is unknown, as for every row above it.

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

**That experiment no longer rests on a single seed - every row in the table above is one**, and the
table is where the set lives, so a sentence here does not restate it (an earlier version of this
paragraph counted them, and the count went stale the next time a row was added). That matters because
[`test-857-churn-storm-async-stalls.md`](test-857-churn-storm-async-stalls.md)'s "ANSWERED" section
rests on a single firing of a single seed and says so outright: *"a second firing, ideally on a
different seed, is what would put it beyond argument."* Different seeds are what these are for.

A repeat soak now answers this without anyone reading a log: `bin/torture-overnight.sh` engages the
recovery diagnostic on every cycle and prints a drain verdict per cycle.
[`test-857-churn-storm-async-stalls.md`](test-857-churn-storm-async-stalls.md) **owns that
mechanism** - what it records, and the instrument defect its first version repeated.
<!-- file-refs: N/A - the harness moved to branch test/overnight-torture-harness-v2; it is named here as the instrument these runs used, not as a file in this tree -->

## Related

- astubbs/parallel-consumer#348 carries the reporting half and the seed
- [`bug-857-family.md`](bug-857-family.md) - the 2026-08-25 entry, for how the other two instances
  were settled and the replay method that settled them
- [`test-chaos-phase2.md`](test-chaos-phase2.md) - the roster this belongs to once resolved
