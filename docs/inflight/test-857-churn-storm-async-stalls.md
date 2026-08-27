# `ChaosChurnStormIT` stalls - three sightings no known defect explains

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->

**Commit mode: `PERIODIC_CONSUMER_ASYNCHRONOUS`** (`ChaosChurnStormIT`, verified in source). This is
why the file exists separately, and it is the most important fact in it:

<!-- post-merge: checked -->
- The **astubbs#29 AB-BA cycle cannot close here.** In `PERIODIC_CONSUMER_ASYNCHRONOUS`,
  `ConsumerOffsetCommitter.commit()` falls through to `requestCommitInternal()` and never blocks, so
  there is no second edge.
- The **transactional revoke wait cannot run here** - it is gated on
  `options.isUsingTransactionCommitMode()`. See `bug-857-transactional-revoke-wait.md`.
- **astubbs#100 and astubbs#80 are landed.**

So unless one of the landed fixes regressed or is incomplete, **all three sightings below are
unexplained by every known member of the confluentinc#857 family.** That is either a fourth defect or
something outside the product. Nobody has said this out loud before, because the ledger these
entries came from never recorded commit modes.

**Confounds to discharge before calling it a product defect:**

- The chaos harness's own open teardown races - see `test-chaos-teardown-double-close.md`.
  `STOP_DRAIN` never sets `closePending` and `settleFleet` bypasses the one-closer guard, and
  churn-storm is `STOP_DRAIN`-heavy.
- Ambient load on the shared self-hosted highcpu runner - see
  `ci-disabled-jobs-and-runner-load.md`. Every detector that fired here is a *stall/stagnation*
  detector, and box saturation could starve consumers past a 30s bound.

**None of the three seeds below has ever been replayed.** The replays are the cheapest experiment
available and would discharge both confounds at once.

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
<!-- post-merge: checked -->
family's original deadlock is astubbs#29's. **Do not read this entry as identifying
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

## A REPLAYABLE seed, 2026-08-28 - the first this line has ever had

Seed `9086872209853284830`, `ChaosChurnStormIT`, found by random-seed hunting in CI rather than by
replaying anything. Replayed locally on two trees, twice each:

- **fix branch: failed both times**; **master: passed, then failed.** Three of four runs reproduced.
- Every failure is the same shape - `NO_PROGRESS`, the fleet reaching roughly 95% of its target and
  stopping.
- A second seed found in the same sweep, `6078190770998307147`, did **not** reproduce in any of four
  runs. Recorded because it is the control that makes the first one mean something: seeds from this
  line are mostly one-offs, and this is the one that is not.

**It fires on master, so it is not branch-caused.** An n=1 reading of the first two runs suggested
otherwise and was wrong - the master arm failed on its second attempt. That inference had already
been ruled out for this line before: these sightings have appeared on branches carrying no Java at
all, and astubbs#373 was tested against this arm on a mode-matching hypothesis and fired anyway on
its own head.

**Why this matters more than another sighting.** Every entry above names replay as its deciding
experiment and none had ever been run - *"Not one of the five captured seeds has ever been
replayed"*. This line now has a handle: a seed that reproduces on demand, on a laptop, in minutes,
on unmodified master. That is what the deadlock needed and did not have until a purpose-built probe
was written, and it is the difference between a ledger of sightings and a defect somebody can chase.

**RUN, 2026-08-28: astubbs#344 is NOT the explanation.** Seed `9086872209853284830` replayed either
side of the encoder fix - `1cebce8fd` (pre) and current master (post) - failed on **both arms, every
run**. The prediction recorded before the run said this was the likely outcome, because the post arm
IS master and master already reproduced; the mode match was suggestive and wrong. Recorded as a
refuted hypothesis rather than deleted, so nobody re-derives it from the same mode coincidence.

Completed at three reps per arm: the pre arm failed most runs, the post arm failed all of them. So
master is no better than the tree before the fix, and at this sample size the direction between them
is not a finding.

**A third of the failures went red with `NO_PROGRESS` not firing at all** - one on each arm. That is
the more interesting result of this experiment, and it is a separate question: either this seed
produces a second signature, or the detector is missing occurrences it should catch. **It matters
because the demotion of the Class 2 bound left `INSTANCE_STALL` and this detector carrying the
gating liveness claim.** A detector whose silence cannot be trusted is worse than one that is absent,
because the suite reports green on its say-so. Establish which before reading any future silence from
it as evidence.

**The superseded reasoning, kept because the mode coincidence will occur to the next reader too.** astubbs#344 fixed an offset-encoder
double-read that is live in `PERIODIC_CONSUMER_ASYNCHRONOUS` and only there - the same mode as every
entry in this file - whose consequence is offsets marked complete while still incomplete. Replay this
seed either side of astubbs#344. If that is the mechanism, the pre arm reproduces and the post arm
does not. **The post arm is current master, which already reproduces** - so on the face of it
astubbs#344 is NOT the explanation, and the test is whether the pre arm reproduces harder. Say so
here either way.

Replay: `-Dchaos.seed=9086872209853284830 -Dit.test=ChaosChurnStormIT` with the chaos group included.
