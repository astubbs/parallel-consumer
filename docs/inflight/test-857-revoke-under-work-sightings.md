# `ChaosRevokeUnderWork*` sightings - the two that are mode-compatible with astubbs#29

**Commit mode: `PERIODIC_CONSUMER_SYNC`** - inherited from `AbstractRevokeUnderWorkScenario`, which
both the eager (`ChaosRevokeUnderWorkIT`) and cooperative
(`ChaosRevokeUnderWorkCooperativeIT`) variants extend. Verified in source.

That makes these **the only sightings in the family whose mode permits astubbs#29's AB-BA cycle to
close** - the cycle's second edge lives in `ConsumerOffsetCommitter`, constructed only for the
consumer-commit modes, and among those only the *sync* arm blocks. The scenario's own javadoc says
the mode was chosen to maximise revoke-path vs commit-path lock contention.

Mode-compatible is not the same as attributed. **Two seeds have now been replayed, and the result
is bad for astubbs#29: the fix does not close this stall** - see "The first replays" below. Of five
sightings four carry a reproducer; two of those four have been run. The third sighting has no probe
verdict at all; the fourth's seed was recorded as lost and then recovered, see its own correction. Compare `test-857-churn-storm-async-stalls.md`, whose
sightings are in a mode where the cycle cannot close.

## The first replays, 2026-08-18 - both seeds reproduce on BOTH arms

**Headline: the astubbs#29 deadlock fix does not close this failure.** 16 local runs of
`ChaosRevokeUnderWorkIT` (eager, `PERIODIC_CONSUMER_SYNC`), arms interleaved, chaos test sources
byte-identical across arms so only main code differs:

- DEFECT = `origin/master` @ `438b09d9b`; FIXED = this branch @ `b8a335b05`.

| Cell | Attempts | Reproduced | Violations per run |
|---|---|---|---|
| Seed A `4734674029169027864` x DEFECT | 3 | 3 | 25, 31, 32 |
| Seed A `4734674029169027864` x FIXED | 3 | **3** | 42, 44, 35 |
| Seed B `4709156528562690268` x DEFECT | 3 | 2 | 2, **pass**, 2 |
| Seed B `4709156528562690268` x FIXED | 3 | **3** | 5, 1, 6 |
| Fresh random seed x DEFECT (control) | 2 | 0 | clean, clean |
| Fresh random seed x FIXED (control) | 2 | **1** | clean, **`CLASS2_STALL`** |

Every failure carried an explicit `CLASS2_STALL/LAG_STAGNATION` verdict and every pass an explicit
`probe violations=[]`; no verdict-less run. Seed B reproduced its CI sighting **to the digit** -
partition 22, lag 3010, committed offset stagnant at 173, matching job 95609956596's autopsy.

**Why this is the strong direction.** The documented asymmetry says reproduction is strong evidence
and non-reproduction weak - and reproduction is what happened, 11 of 12 seeded attempts, including
**6 of 6 on the arm carrying the fix**. The single seeded pass (DEFECT, seed B) is the expected
behaviour of a seed that fixes the scenario but not the interleaving, not a contradiction.

**The control is what makes this more than a replay artefact.** Fresh random seeds passed 3 of 4 in
the same wall-clock window, so the recorded seeds are genuinely enriched (11/12 vs 1/4) rather than
the box simply being unable to run the scenario. But the one control failure is itself a finding: a
**fresh** seed drew the same stall on the FIXED arm, confirming the fixed code still reaches
`CLASS2_STALL` with no replay machinery involved at all.

**What this does NOT establish.** That the residual stall is astubbs#29's AB-BA cycle. It
establishes only that the fix does not remove the stall. The cleanest failing run (FIXED, seed B,
one violation) is the one to attribute from: the storm ended at 51:38.997 and the probe fired at
53:18.953, so partition 22 sat committed-frozen for **100s of the quiet phase** - group STABLE,
heartbeats flowing - against the scenario's own ~40s worst-case legitimate recovery arithmetic. A
genuine anomaly by the test's own calibration, mechanism unattributed. **Next experiment: a
FIXED-arm failing run with periodic thread dumps.**

**Do the two seeds behave the same? Same class, materially different severity.** Both give
`CLASS2_STALL/LAG_STAGNATION` at ~154s - and that ~154s is bound-150s plus probe poll cadence, an
artefact of the detector, **not a defect fingerprint**. Seed A fails violently (25-44 frozen
partitions), seed B narrowly (1-6, matching its CI sighting's 4), consistently across both arms.
Recurring per-partition fingerprints (partition 22 lag=3010 stagnant@173; partition 21 lag=2974
stagnant@91) appear under **both** seeds, **both** arms and the fresh-seed control, because record
placement is workload-determined (`HEAVY_EVERY=2000`, 250k records, 80 partitions). **Do not read
fingerprint identity as seed identity.**

**Confounds, recorded rather than resolved:**

- This box exports `-XX:ActiveProcessorCount=8` (of 32 cores), so every test JVM saw 8 processors.
  Likely inflates severity - seed A's 25-44 violations against CI's 5-6 - though the controls
  passing under the same cap argue it does not manufacture the stall.
- Concurrent agent load ranged 1.98-11.62 (1-min), recorded per run. Outcome did not track it:
  runs failed at load ~2, and the sole seeded pass happened at ~6.8.
- **Invocation deviated from the replay command above**: `-Dit.test=ChaosRevokeUnderWorkIT` ran the
  eager scenario alone, where CI runs the whole chaos group in one JVM. Sequencing effects from
  sibling chaos tests were not replayed.

**Evidence**, outside the repo because it is 96MB compressed: `/home/astubbs/pc/evidence/857-replay-2026-08-18/`
- `results.log` (the ledger), `autopsy-blocks.txt` (226 extracted verdicts), `reports/` (gzipped
  failsafe XMLs), and the driver scripts so the experiment can be re-run.

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

**Third sighting, 2026-08-12 - the COOPERATIVE variant went red, and this one is NOT confirmed as
the stall.** `ChaosRevokeUnderWorkCooperativeIT.revokeUnderWorkStaysProtocolHonestWithCooperativeAssignor`
failed on [job 93954713987](https://github.com/astubbs/parallel-consumer/actions/runs/31544745175/job/93954713987).
**Replay seed `3986919097693415295`** - captured because it is the part no command can recover once
the log expires:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=3986919097693415295

**Read this as consistent with the family, not as a member of it.** The second sighting above was a
clean `ProgressProbe` kill - explicit `CLASS2_STALL`/`LAG_STAGNATION` violations, named partitions,
lag numbers, a 154s stagnation against a 150s bound. This one has **no probe verdict that could be
extracted from the log at all**. What it shows is a shutdown-path symptom:

```
Thread execution pool termination await timeout (PT10S)! Were any processing jobs dead locked
(test latch locks?) or otherwise stuck? Forcing shutdown of workers.
```

That is compatible with work wedged behind the open deadlock, and equally compatible with an
ordinary chaos `STOP_NO_DRAIN` racing a slow shutdown. **Nothing here distinguishes the two**, and
the inference should not be made without doing so.

**What would settle it**, for whoever picks this up: replay the seed above and look for a
`ProgressProbe` verdict. A `CLASS2_STALL`/`LAG_STAGNATION` violation alongside the termination
timeout makes it the family; `probe violations=[]` with the timeout still present makes it a
shutdown-path issue of its own, and a third thing to chase rather than evidence about astubbs#29.
Check the surefire XML rather than the console log - the console tail here carried no verdict either
way, which is why this entry cannot resolve it.

**Why it matters even unconfirmed: it is the cooperative sibling, one assignor apart.** The second
sighting records that the cooperative variant *passed in that same run* (`probe violations=[]`), and
reasons from that to "whatever remains is eager-protocol-specific". A cooperative red is the first
datapoint pointing the other way - and it is **not** enough to retire that inference, precisely
because it is unconfirmed as a stall. If the replay comes back with a probe violation, the
eager-specific reading needs revisiting; if it comes back clean, it stands untouched. No cooperative
sighting had been recorded before this one.

**Do not re-diagnose astubbs#100 from this log.** It is full of
`org.apache.kafka.common.errors.RebalanceInProgressException` lines. Those are the **landed** fix
working - `ConsumerOffsetCommitter.commitDeferringOnRebalance()` catching and deferring - not the old
unhandled path that killed the broker-poll thread. Grepping the log for that exception and
concluding the poll thread died again would re-open something already closed.

**Branch context:** seen on astubbs#287, a review-gate/CI branch whose only Java change is a
one-line test annotation, so the branch is not a suspect. As with the second sighting, the chaos
suite randomises its seed per run, so other branches passing the same day only means their seeds did
not draw this interleaving.

**Third candidate explanation for the third sighting: a killed runner.** It is the family's only
verdict-less red, and `ci-disabled-jobs-and-runner-load.md` records the highcpu lane killing jobs
mid-step with no verdict (re-confirmed 2026-08-17). The entry above offers two explanations; this is
a third, and it is the one most consistent with "no probe verdict could be extracted from the log at
all".


**Fourth sighting, 2026-08-18 - the second live confirmation reproduced, same test, same bound,
seven weeks later.** `ChaosRevokeUnderWorkIT.revokeUnderWorkStaysProtocolHonest` - the **eager**
variant again - killed by `ProgressProbe` on
[job 95609956596](https://github.com/astubbs/parallel-consumer/actions/runs/32104058992), at head
`151d86202` on astubbs#296. **Commit mode `PERIODIC_CONSUMER_SYNC`**, verified in source at
`AbstractRevokeUnderWorkScenario`'s `.commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)`:

```
CLASS2_STALL/LAG_STAGNATION: partition ChaosRevokeUnderWorkIT-w4-1440515387-22 lag=3010 with
committed offset stagnant at 173 for 154s (bound 150s) - protocol-invisible stall:
group STABLE + heartbeats flowing, yet this partition's backlog is going nowhere
```

Three further frozen partitions (stagnant 20s, 25s, 119s); peaks `rebalanceDwell=9908ms`,
`lagStagnation=154358ms`.

**This is the second live confirmation's signature, not merely its family.** Same test, same eager
arm, same `CLASS2_STALL/LAG_STAGNATION`, and the same 154s against a 150s bound. Two independent
occurrences seven weeks apart, in the only mode where astubbs#29's cycle can close, is the strongest
evidence this file holds that the second sighting was not a one-off interleaving.

**The control arm is unusually strong, and needs no replay to hold.** The same lane passed on the two
immediately preceding heads of that branch, `87152f7b4` and `a1db0f109`. The diff from `a1db0f109` to
`151d86202` contains **zero non-comment Java lines** - comments, javadoc and markdown only, so the
bytecode is identical. The same executable passed twice and failed once, which excludes the branch
arithmetically rather than by the usual "that branch touches no product code" argument. astubbs#296
hardens work submission against an already-closed worker pool and is unrelated to the revoke path.

**Replay seed `4709156528562690268`** - the eager `w4` scenario, `cooperative=false`:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=4709156528562690268

**Correction, same day: this entry first said the seed was lost, and that was wrong.** It was
recorded that way because `gh run view --job <id> --log` returned a log ending at 05:47 for a job
that ran to 05:52, and the seed line was past the cut. The seed was never gone - it is in the
archived attempt, reachable by a different route:

    gh api repos/astubbs/parallel-consumer/actions/runs/32104058992/attempts/1/logs

That returns **6266 lines** against the 2207 the job-log route gave, and carries both autopsy blocks,
`BUILD FAILURE`, and all three scenario seeds. **The lesson is about retrieval, not about logging:**
"the log is truncated" was a statement about the route used, and the fix was to ask for the attempt
rather than the job. Two traps sit here together - a job-log read is truncated, *and* after a re-run
the same job id resolves to the latest attempt's window, so a re-run silently changes what that route
returns. Prefer `/attempts/<n>/logs` whenever the answer matters.

**Still worth doing at the source:** the seed belongs inside the `AMBIENT PROBE AUTOPSY` block, which
travels in the uploaded artifact. Recovering it took a second retrieval route and knowing to try it;
in the block it would have arrived with the violation.

**It does, however, confirm the third sighting's own advice.** That entry says to check the surefire
XML rather than the console log. Here the console returned **zero** matches for
`AMBIENT PROBE AUTOPSY` and parsed as `tests=0 failures=0 errors=0` - indistinguishable from a clean
run - while the artifact's failsafe XML carried the violation, the autopsy and the frozen-partition
table above. The advice was written from a case where the log carried no verdict either way; this is
a case where it carried a false *negative*, which is worse, and the same check caught it.


**Fifth sighting, 2026-08-18 - the same signature a third time, ten times as wide, and the first one
whose seed arrived with the violation.** `ChaosRevokeUnderWorkIT.revokeUnderWorkStaysProtocolHonest`
- the **eager** variant again - killed by `ProgressProbe` on
[job 95879300043](https://github.com/astubbs/parallel-consumer/actions/runs/32189088516), at head
`53d4b8bbe` on astubbs#296, sixteen hours after the fourth and on the same branch. **Commit mode
`PERIODIC_CONSUMER_SYNC`**, verified in source at `AbstractRevokeUnderWorkScenario`'s
`.commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)`:

```
=== AMBIENT PROBE AUTOPSY (test failed): revokeUnderWorkStaysProtocolHonest() ===
failure: TerminalFailureException: probe violation
chaos seed: 9082185140923636480
violations (37):
  - CLASS2_STALL/LAG_STAGNATION: partition ChaosRevokeUnderWorkIT-w4-2065844453-3 lag=3042 with
    committed offset stagnant at 65 for 154s (bound 150s) - protocol-invisible stall:
    group STABLE + heartbeats flowing, yet this partition's backlog is going nowhere
```

All 37 violations are `CLASS2_STALL/LAG_STAGNATION`, and every one of them reads 154s against the
150s bound; lags run 587-3121. The autopsy's frozen-partition table lists 52 entries; peaks
`rebalanceDwell=7685ms`, `lagStagnation=154313ms`.

**Replay seed `9082185140923636480`** - the eager `w4` scenario, `cooperative=false`:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=9082185140923636480

**37 simultaneous violations is the widest instance the family holds - and that is a fact about
breadth, not about severity trending.** The second sighting named six partitions, the fourth four.
This one stalls 37 of the topic's 80, with 52 in the frozen table: most of the assignment, not a
corner of it. It is still **one seed**. Three numbers - 6, 4, 37 - are three draws from a randomised
conductor, so nothing here says the defect is worsening, and reading a trend into them would be
reading noise. What is not noise is that the *stagnation figure is identical*: 154s against a 150s
bound, exactly as in the second and the fourth. Breadth varies with the interleaving; the bound is
where the probe fires. That the widest and the narrowest instances stop at the same number is what
makes these one signature rather than three separate incidents.

**The seed arrived inside the autopsy for the first time, and that is machinery rather than luck.**
The fourth sighting above spends a paragraph recovering its seed by a second retrieval route, and
closes by saying the seed belongs inside the `AMBIENT PROBE AUTOPSY` block. It now is - astubbs#296
commit `742b9821d`, *"test(chaos) confluentinc#857: carry the replay seed inside the autopsy
block"*, which threads a `ChaosSeed` through to the `chaos seed: ` and `chaos replay: ` lines in
`AmbientProbeExtension`. The autopsy travels in the failsafe XML that is uploaded as an artifact, so
the seed now sits where console truncation cannot reach it. This is the first sighting captured
after that change and the first whose seed needed no second route: it was read straight out of the
block above. Those anchors resolve on astubbs#296's branch, not on this one - neither `ChaosSeed`
nor the `chaos seed: ` line exists here yet.

**The branch is not a suspect, and the control arm is stronger than the fourth sighting's.**
astubbs#296 hardens work submission against an already-closed worker pool, which is not the revoke
path. Beyond that argument, `git log --oneline 151d86202..53d4b8bbe` is six commits - five
documentation, and `742b9821d`, which is test-harness only. The range's diff touches `docs/`,
`docs/testing.md`, three chaos test classes, `AmbientProbeExtension` and its unit test: **no
main-source file at all.** So the eager arm has now failed, on first attempt, at two of this
branch's three heads - `151d86202` (the fourth sighting) and `53d4b8bbe` (this one) - with no
production code differing between them. n=3 heads is too small to quote as a rate, but it is the
opposite of a branch-specific explanation.

**The re-run went green, and it settles nothing, because it drew a different seed.** Run
32189088516 was re-run after this failure; attempt 2's `Chaos Pain Suite` passed. Its eager scenario
ran `seed=7455570399125658252`, not `9082185140923636480` - the suite draws fresh per run, so the
re-run is a different experiment, not a retraction of this one. **The only result that would retract
it is replaying `9082185140923636480` and watching it pass.** Read the run's overall conclusion with
that in mind: `gh run list` now reports run 32189088516 as `success`, and reports the fourth
sighting's run 32104058992 as `success` too - that one was also re-run into green. Both failures
live in **attempt 1**, reachable only as `/attempts/1/logs`, exactly as the fourth sighting's
correction warns.

**The cooperative arm passed in the same attempt, as it did in the two earlier eager reds.** Here
`ChaosRevokeUnderWorkCooperativeIT` ran clean on seed `8367488744993533060`, and `ChaosChurnStormIT`
on `7750587486126758074`. Checking attempt 1 of the fourth sighting's run shows the same shape: the
eager arm errored, the cooperative arm passed. All three confirmed eager stalls therefore sit beside
a green cooperative arm in the same run, which is the second sighting's "eager-protocol-specific"
reading holding across three independent occurrences. The third sighting is still the only
cooperative red, and still unconfirmed as a stall, so it still does not overturn that reading.
