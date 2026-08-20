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

**Fourth sighting, 2026-08-12 - the `ZOMBIE_MEMBER` arm, and the probe genuinely fired.**
`ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger` was killed fail-fast by `ProgressProbe` on
[job 94014375262](https://github.com/astubbs/parallel-consumer/actions/runs/31564815332/job/94014375262),
on astubbs#289 at head `976f88c65`. The failing wait is the `await()` in `ChaosChurnStormIT` aliased
`all messages consumed under churn`, whose `failFast` is `probe violation during run`:

```
ZOMBIE_MEMBER/REBALANCE_BLOCKED: group 'group-1-1929174831' dwelling in PreparingRebalance for 15s
(bound 15s) - a member is not answering the rebalance (protocol-unresponsive)
peaks: rebalanceDwell=15426ms lagStagnation=99137ms
```

23 frozen partitions, stagnant 96-101s, lag from 56 to 1132. **Replay seed `7731567379755737438`** -
the part no command can recover once the log and artifact expire:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=7731567379755737438

**The two sibling seeds from the same run are the control arm, not a footnote.** Both passed:
`ChaosRevokeUnderWorkIT` on `642714983109785585` and `ChaosRevokeUnderWorkCooperativeIT` on
`374908783265204320`. Same run, same runner, same broker image - so whatever this was, it is not an
ambient property of the machine that hour, and the two revoke-under-work scenarios did not draw it.

**The probe was not vacuous, and that needs saying so nobody dismisses it.** The rule elsewhere is
that a *clean* probe is only evidence when its detectors could have fired; the converse holds here.
`rebalanceDwell` reached 15426ms against a 15s bound, with 23 partitions stalled around 100s and
four-figure lag on several - a detector that fired on its own terms, not a short window that tripped
a threshold by accident.

**Which arm, and what it is not.** This is the `ZOMBIE_MEMBER`/`REBALANCE_BLOCKED` signature - a
member not answering the rebalance - and not the `CLASS2_STALL`/`LAG_STAGNATION` signature of the
second sighting. The zombie-member defect is recorded above as **landed** via astubbs#80, and the
family's original deadlock (astubbs#29) is still open. **Do not read this entry as identifying
either.** It records a signature and a replayable seed; which defect it belongs to, if any, is what
the replay is for.

**Branch context: astubbs#289 is not a suspect, and here is why rather than an assertion.** It
changes documentation, logback *test* configuration, `<repositories>` blocks in three poms,
`CODEOWNERS`, deleted upstream CI files, one added unit test, and exactly one line of main source -
a `String` constant holding a documentation URL. Nothing on the rebalance, poll, commit or shutdown
path. As with the second and third sightings, the chaos suite randomises its seed per run, so other
branches passing the same day only means their seeds did not draw this interleaving.

**Retrieval note - the autopsy was NOT in the CI log.** GitHub truncated the job's log stream partway
through the run, so neither `gh run view --log` nor `--log-failed` contained the
`=== AMBIENT PROBE AUTOPSY ===` block, and the check-run annotations carried only
`Process completed with exit code 1`. The autopsy and all three seeds came from the **uploaded test
report artifact** (`highcpu-fast-feedback-reports-Chaos Pain Suite-*`), inside the failsafe XML for
the failing class, where the block is embedded in the captured system-out. Go there first for a
chaos failure; the console is not reliable for this and will look like the verdict simply does not
exist. This generalises beyond the entry - it belongs in the ambient-probe section of
`docs/testing.md`, which currently states that every broker integration test failure *log* includes
the block, and that file was owned by another branch when this was written.

**Fifth sighting, 2026-08-18 - the fleet-level `NO_PROGRESS` arm, twice in one night (this entry
and the sixth below share a signature).** `ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger`
was killed fail-fast by `ProgressProbe` on
[job 95579861648](https://github.com/astubbs/parallel-consumer/actions/runs/32093367999/job/95579861648),
2026-08-18T02:52Z, on the astubbs#310 branch (docs+hooks only; since merged, branch deleted - the
run record survives):

```
NO_PROGRESS: fleet consumed count stuck at 98150/100000 for 30s (bound 30s)
```

Autopsy: 26 frozen partitions, committed stagnant 50-55s with live lag (56-1132); peaks
`rebalanceDwell=6644ms`, `lagStagnation=55152ms`. The fleet-level detector fired on its own terms -
98150 of 100000 consumed, then nothing for the full 30s bound - so the vacuity caveat does not
apply. **Replay seed `3086917415748208232`** - the part no command can recover once the log
expires:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=3086917415748208232

**Control arm from the same run:** both revoke-under-work variants PASSED with
`probe violations=[]` - cooperative on seed `4087023100803854645`, eager on `334227014609238766`.
Same runner, same broker, same hour.

**Correction worth recording: a truncated log misattributed this sighting before it was written.**
The `gh run view --job <id> --log` route returned 1654 lines and cut off inside the cooperative
revoke phase, whose (expected, non-fatal) `RebalanceInProgressException` churn then read as the
failure - a handoff circulated citing the cooperative test and seed `4087023100803854645`, which is
in fact the PASSING control arm. Do not replay that seed expecting a failure. The full 5948-line
log came from the run-logs archive
(`gh api repos/.../actions/runs/<run-id>/logs`, or `.../attempts/<n>/logs` for a re-run's earlier
attempt) - a second retrieval route alongside the fourth sighting's report-artifact note, and the
console-log truncation trap is the same one recorded there.

**Sixth sighting, 2026-08-18 - same test, same arm, four hours earlier, different branch.**
`ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger`, killed by the same fleet detector on
[job 95584026682's run, attempt at head d96375053](https://github.com/astubbs/parallel-consumer/actions/runs/32078875110)
(2026-08-17T23:04Z), on astubbs#308 - a docs-only branch (ideation/strategy/ledger files, zero
product code), so the branch is not a suspect:

```
NO_PROGRESS: fleet consumed count stuck at 95382/100000 for 30s (bound 30s)
```

Autopsy: 28 frozen partitions; peaks `rebalanceDwell=3345ms`, `lagStagnation=82777ms`; run settled
at consumed=95667. **Replay seed `8603691233664838594`**:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=8603691233664838594

**Control arm, same run:** cooperative passed on seed `6926127865194591503`, eager on
`5980280513720170608`. The suite re-ran green on the same branch at head `bb5799df0` five hours
later - consistent with every prior sighting: seed-dependent, not branch-dependent.

**What two same-night `NO_PROGRESS` kills add up to.** This arm differs from the second sighting's
`CLASS2_STALL/LAG_STAGNATION` (per-partition stagnation, eager revoke) and the fourth's
`ZOMBIE_MEMBER/REBALANCE_BLOCKED` (member unresponsive to rebalance): here the *fleet-wide*
consumed count freezes outright with the group ostensibly live, under churn-storm weights
(`STOP_DRAIN`-heavy, high `joinAfterDrainBias`) rather than revoke-under-work. Both kills landed on
`ChaosChurnStormIT` - as did the fourth - so the churn-storm scenario is now the family's most
productive trap. Two replayable seeds with the same signature within four hours is the strongest
single-arm evidence the ledger holds; whichever defect they belong to, the replays are now the
cheapest next experiment the family has.

**Seventh sighting, 2026-08-17 - the eager `CLASS2_STALL` again, and a DIFFERENT arm from the
fleet `NO_PROGRESS` pair above, and it reproduces the second sighting
almost exactly.** `ChaosRevokeUnderWorkIT.revokeUnderWorkStaysProtocolHonest` killed fail-fast by
`ProgressProbe` on
[job 95331078881](https://github.com/astubbs/parallel-consumer/actions/runs/32011246250/job/95331078881),
on astubbs#293 at head `b94e85d64`:

```
CLASS2_STALL/LAG_STAGNATION: partition ChaosRevokeUnderWorkIT-w4-204589915-25 lag=2885 with
committed offset stagnant at 296 for 154s (bound 150s) - protocol-invisible
```

Two violations, eight frozen partitions (lag 79-1676, stagnant 12-23s), `peaks:
rebalanceDwell=10990ms lagStagnation=154235ms`. **Replay seed `1870799285619636118`**:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=1870799285619636118

**Why this one is worth more than another tally mark: it is the same variant and the same signature
as the second sighting**, four days apart on an unrelated branch - eager assignor, `CLASS2_STALL`/
`LAG_STAGNATION`, and 154s of stagnation against the same 150s bound. The second sighting stands as
the family's strongest datapoint partly because it was single; it is not any more.

**Control arm, from the same run:** both siblings passed - `ChaosRevokeUnderWorkCooperativeIT` on
seed `4957387373444835170` and `ChaosChurnStormIT` on `9069097373343684126`. Same runner, same
broker image, same minute, so this is not an ambient property of the machine, and the cooperative
variant again did not draw it.

**Branch context: astubbs#293 is not a suspect.** It adds a proxy sidecar and eleven language
clients; nothing in it touches the rebalance, poll, commit or shutdown path in
`parallel-consumer-core`, and the same lane was green on the previous head of the same branch two
runs earlier with different seeds.

**One correction to the retrieval note above: this time the autopsy WAS in the console log**, read
straight out of `gh api .../actions/jobs/<id>/logs`. The truncation that hid it on 2026-08-12 is
intermittent, not the rule - go to the artifact when the console lacks the block, not instead of
looking.

**Gated on astubbs#29: proving thread-parallel integration tests are safe again.** astubbs#68 made the integration
suite reliable by *forking* per broker (`forkCount=4`), which sidesteps the deadlock rather than
proving it gone - the contended `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` failure it was hiding is
the real confluentinc#857 bug. The deferred "Step 2" is to re-run with `-Dparallel-tests=true` on a
shared broker **after astubbs#29 lands** and see whether it stays green. One probe on the highcpu runner
hinted it might (forked unit suite green with threads enabled; the integration red was the separate
`PartitionStateCommittedOffsetIT` flake, since fixed by astubbs#80), but one green run is not proof. Forking
stays the default regardless: fork×threads measured no faster than fork alone, because forking already
saturates the cores.


## A technique with a named target here (2026-08-21)

A competitor's TLA+ verification reports finding, by exhaustive state exploration, **a race between
offset commit and partition revocation** - a commit tick executing inside a revoked window before
revocation completed, producing silent duplicates under specific rebalance interleavings. That is the
shape of this family, found by construction rather than by seeded replay.
[`next-formal-verification-and-correctness-methods.md`](next-formal-verification-and-correctness-methods.md)
argues the case and scopes it: model the commit-advancement and drain/revoke paths only, not the
whole system.
