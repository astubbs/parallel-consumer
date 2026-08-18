# `ChaosRevokeUnderWork*` sightings - the two that are mode-compatible with astubbs#29

**Commit mode: `PERIODIC_CONSUMER_SYNC`** - inherited from `AbstractRevokeUnderWorkScenario`, which
both the eager (`ChaosRevokeUnderWorkIT`) and cooperative
(`ChaosRevokeUnderWorkCooperativeIT`) variants extend. Verified in source.

That makes these **the only sightings in the family whose mode permits astubbs#29's AB-BA cycle to
close** - the cycle's second edge lives in `ConsumerOffsetCommitter`, constructed only for the
consumer-commit modes, and among those only the *sync* arm blocks. The scenario's own javadoc says
the mode was chosen to maximise revoke-path vs commit-path lock contention.

Mode-compatible is not the same as attributed. **No seed here has ever been replayed**, the third
sighting has no probe verdict at all, and the fourth has no seed to replay - so of four sightings,
two carry a reproducer and neither has been run. Compare `test-857-churn-storm-async-stalls.md`, whose
sightings are in a mode where the cycle cannot close.

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
