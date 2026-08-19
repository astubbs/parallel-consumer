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

**Answered, 2026-08-13: this one was the test harness, not the product.** astubbs#292 (squash
`40575362`) read this exact job log and found instance 34 drawing
`STOP_NO_DRAIN -> RESTART -> STOP_NO_DRAIN -> RESTART` in ~4s, then running **twice** 138ms apart on
two pool workers, each building its own consumer:

```
05:00:42.192  Runner-34 [worker-10]  Running consumer instance 34   -> KafkaConsumer@60186851
05:00:42.330  Runner-34 [worker-5]   WARN previous PC did not close within 10s, proceeding anyway
05:00:42.330  Runner-34 [worker-5]   Running consumer instance 34   -> KafkaConsumer@5b669f02
```

Both joined `group-1-1929174831`; the probe fired four seconds later. Two PCs for one logical
instance, one of them orphaned and closed by nobody - a group member that answers no rebalance. That
is the double-start race astubbs#292 fixed, so this sighting is **retired from the family**: it was
never evidence about astubbs#29 or astubbs#80. The replay above is no longer worth spending.

**Branch context: astubbs#289 is not a suspect, and here is why rather than an assertion.** It
changes documentation, logback *test* configuration, `<repositories>` blocks in three poms,
`CODEOWNERS`, deleted upstream CI files, one added unit test, and exactly one line of main source -
a `String` constant holding a documentation URL. Nothing on the rebalance, poll, commit or shutdown
path. As with the second and third sightings, the chaos suite randomises its seed per run, so other
branches passing the same day only means their seeds did not draw this interleaving.

**Retrieval note - the autopsy was NOT in the CI log.** GitHub truncated the job's log stream partway
through the run, so neither `gh run view --log` nor `--log-failed` contained the
`AMBIENT PROBE AUTOPSY` block, and the check-run annotations carried only
`Process completed with exit code 1`. The autopsy and all three seeds came from the **uploaded test
report artifact** (`highcpu-fast-feedback-reports-Chaos Pain Suite-*`), inside the failsafe XML for
the failing class, where the block is embedded in the captured system-out. Go there first for a
chaos failure; the console is not reliable for this and will look like the verdict simply does not
exist. This generalises beyond the entry - it belongs in the ambient-probe section of
`docs/testing.md`, which currently states that every broker integration test failure *log* includes
the block, and that file was owned by another branch when this was written.

**Fifth sighting, 2026-08-13 - the same `ZOMBIE_MEMBER` signature, with astubbs#292 present and no
double-start anywhere in the log.** This is the one that matters, because it is what the fourth
sighting's attribution does *not* cover.
`ChaosChurnStormIT` on [job 94333179847](https://github.com/astubbs/parallel-consumer/actions/runs/31663523870/job/94333179847),
at head `8fc2a244`. **Replay seed `8724638006462097730`**:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=8724638006462097730

The harness was ruled out by counting, not by assertion - **0** concurrent double-runs across 41
`run()` invocations over 16 instances, **0** guard engagements (so the race window was never even
entered), and **0** teardown indicators: no drain exceeded its 60s join budget, no `settleFleet`
close error, no `ConcurrentModificationException`. The chaos timeline around the violation is
healthy - every `STOP_DRAIN` paired with a `DRAIN_COMPLETE`, restarts proceeding, PCs logging
"Close complete". A member simply stopped answering the rebalance with no orphaned PC involved.

**The triage rule this changes.** The third sighting says nothing distinguishes a product stall from
"an ordinary chaos `STOP_NO_DRAIN` racing a slow shutdown". That is now half answered - one cause
identified and removed, at least one remaining - and the check is cheap:

> A `ZOMBIE_MEMBER` violation is no longer evidence of the harness double-start; that is fixed and
> guarded. Grep the log for two `Running consumer instance N` lines on different pool workers within
> ~2s. Present, it is the harness and astubbs#292's guards regressed. **Absent, it is the other
> cause, and this file is where it belongs.**

astubbs#29 and confluentinc#857 remain open, and none of this attributes anything to either.

**Corroboration that the guards do engage.** The chaos lane on astubbs#296 at head `347ceae90`
(`ChaosRevokeUnderWorkCooperativeIT`, `ChaosRevokeUnderWorkIT`) came back with **14** guard
engagements - `start aborted - stopped while this start was queued` / `async stop skipped - close
already in progress` - and zero double-runs, zero `ZOMBIE_MEMBER`, zero
`ConcurrentModificationException`. So on that run the window was entered fourteen times and closed
every time. Absence of the signature there is weak evidence on its own (it is drawn per seed); the
engagement count is the part worth keeping.

**Also open, recorded here so it is not lost.** A `CLASS2_STALL`/`LAG_STAGNATION` fired locally on
the merged tree - `ChaosRevokeUnderWorkCooperativeIT`, lag stagnant 154s against a 150s bound, group
STABLE with heartbeats flowing - and did **not** reproduce on a re-run of the same scenario. That is
the second sighting's signature rather than the zombie arm, still intermittent, still open. It also
weakens the second sighting's "whatever remains is eager-protocol-specific" reading further, on the
cooperative side, without settling it.

**Sixth sighting, 2026-08-13 - the eager `CLASS2_STALL` again, and the first time both arms fired in
one run.** `ChaosRevokeUnderWorkIT.revokeUnderWorkStaysProtocolHonest` (the **eager** variant) was
killed by `ProgressProbe` on
[job 94355941379](https://github.com/astubbs/parallel-consumer/actions/runs/31671178444/job/94355941379),
on astubbs#296 at head `e7956798c`. **Replay seed `4317404402494426241`**:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=4317404402494426241

The autopsy in full - **9 violations**, one zombie and eight stalls, all on topic
`ChaosRevokeUnderWorkIT-w4-255551928`:

```
=== AMBIENT PROBE AUTOPSY (test failed): revokeUnderWorkStaysProtocolHonest() ===
failure: TerminalFailureException: probe violation
violations (9):
  - ZOMBIE_MEMBER/REBALANCE_BLOCKED: group 'group-1-1258752376' dwelling in PreparingRebalance
    for 15s (bound 15s) - a member is not answering the rebalance (protocol-unresponsive)
  - CLASS2_STALL/LAG_STAGNATION: partition -43 lag=2438 committed stagnant at 669  for 154s (bound 150s)
  - CLASS2_STALL/LAG_STAGNATION: partition -10 lag=2255 committed stagnant at 818  for 154s
  - CLASS2_STALL/LAG_STAGNATION: partition -14 lag=2414 committed stagnant at 740  for 154s
  - CLASS2_STALL/LAG_STAGNATION: partition -47 lag=1885 committed stagnant at 1273 for 154s
  - CLASS2_STALL/LAG_STAGNATION: partition  -8 lag=2197 committed stagnant at 937  for 154s
  - CLASS2_STALL/LAG_STAGNATION: partition -41 lag=591  committed stagnant at 2435 for 154s
  - CLASS2_STALL/LAG_STAGNATION: partition -12 lag=2320 committed stagnant at 749  for 154s
  - CLASS2_STALL/LAG_STAGNATION: partition -16 lag=496  committed stagnant at 2688 for 154s
peaks: rebalanceDwell=15444ms lagStagnation=154099ms
frozen partitions (committed stagnant >= 10s with lag >= 1):
  - -10: committed=818  end=3073 lag=2255 stagnant=17s
  - -12: committed=749  end=3069 lag=2320 stagnant=17s
  - -14: committed=740  end=3154 lag=2414 stagnant=17s
  - -16: committed=2688 end=3184 lag=496  stagnant=17s
  - -27: committed=2846 end=3132 lag=286  stagnant=12s
  - -29: committed=2465 end=3045 lag=580  stagnant=12s
```

**Read the two numbers together.** Every stall violation reports 154s of stagnation, but the frozen
list reports 12-17s. They are different clocks, not a contradiction: the violation's 154s is the
probe's stagnation timer against its 150s bound, while `stagnant=` is how long that partition had
been frozen at the moment the autopsy was taken. Six partitions were still frozen at the end; eight
had breached the bound during the run.

**Also worth noting: partitions -41 and -16 stalled with committed offsets *ahead* of the others**
(2435 and 2688 against 669-937) and correspondingly small lag. So this is not one wedged consumer
falling behind from the start - partitions that had progressed furthest froze too.

**This is the second sighting's signature, reproduced.** Same scenario, same eager variant, same
154s-against-150s stagnation, same "group STABLE + heartbeats flowing" framing. The second sighting
stood alone for two days; it no longer does.

**The control arm holds, and strengthens the eager-specific reading.** `ChaosRevokeUnderWorkCooperativeIT`
and `ChaosChurnStormIT` both **passed in this same run** - same runner, same broker image, same
minute. That is exactly the pattern the second sighting reported (cooperative green while eager
died), now seen twice, which is the strongest evidence yet that whatever remains is
eager-protocol-specific - where astubbs#29's `onPartitionsRevoked` / `commitOffsetsThatAreReady`
contention lives.

**Both arms in one run is new.** Previous entries treat `CLASS2_STALL` and `ZOMBIE_MEMBER` as
separate arms seen in separate runs. Here the zombie violation fired alongside eight stall
violations in the same autopsy. Whether that makes them one defect or two coincident ones is not
settled by this entry - but "these are unrelated signatures" can no longer be assumed for free. The
ordering is the thread to pull: a member that stops answering the rebalance and partitions whose
committed offsets stop moving are exactly what one wedged commit path would produce at both ends.

**Not quarantined, deliberately.** `Chaos Pain Suite` is **not** in master's required-checks ruleset
- it is the advisory highcpu lane - so this red blocks no merge, and quarantine exists to move a red
out of the *gating* suites. Quarantining would also block the 0.6.0.0 release under registry rule 5
in order to silence something that is not gating, and it would switch off the detector that is
producing this file's evidence. Each red here is a sighting plus a seed that dies with its artifact;
that is the asset, not the noise. Re-read this if the reds get tiresome enough to reconsider.

**The harness is excluded by the fifth sighting's own rule, applied and counted.** Across 38 `run()`
invocations over 14 instances there were **0** concurrent double-runs (two `Running consumer instance
N` lines on different pool workers within 2s), with 10 guard engagements showing the guards
themselves active. Not the astubbs#292 double-start.

**Branch context: astubbs#296 is not a suspect, and this run proves it unusually cleanly** - the
commit under test is **documentation-only** (this very file), and the same branch's *previous* head
ran the same suite green. Its product change is a shutdown-path guard; the log contains zero
`rejected from ThreadPoolExecutor` and zero of that guard's `Worker pool is shut down` warnings, so
it never even engaged.

**Retrieval note, again, and it caught a careful reader out.** The console log for this job was
**truncated mid-run** - grepping it returned zero for every probe signature, which reads exactly like
a clean run. All twelve stall violations, the zombie violation, the autopsy and the seed came from
the **uploaded artifact** (`highcpu-fast-feedback-reports-Chaos Pain Suite-*`), inside the failsafe
XML's captured system-out. The fourth sighting already recorded this; it is repeated here because
the trap was walked into anyway, one screen after reading the warning. **A zero from the console is
not a zero.**

**Gated on astubbs#29: proving thread-parallel integration tests are safe again.** astubbs#68 made the integration
suite reliable by *forking* per broker (`forkCount=4`), which sidesteps the deadlock rather than
proving it gone - the contended `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` failure it was hiding is
the real confluentinc#857 bug. The deferred "Step 2" is to re-run with `-Dparallel-tests=true` on a
shared broker **after astubbs#29 lands** and see whether it stays green. One probe on the highcpu runner
hinted it might (forked unit suite green with threads enabled; the integration red was the separate
`PartitionStateCommittedOffsetIT` flake, since fixed by astubbs#80), but one green run is not proof. Forking
stays the default regardless: fork×threads measured no faster than fork alone, because forking already
saturates the cores.

---

**Seventh sighting, 2026-08-14 - local, not CI, and the first one bisected against a clean
baseline.** `PCMetricsTest.metricsRegisterBinding` failed a full multi-module unit run with
`expected: 205.0 but was: 203.0 within 2 minutes` - two records that never completed. That is the
family's signature, not a new one, and it appeared while landing astubbs#296's work-return path -
since removed - whose whole subject was work being taken and not accounted for. So it had to be either the strongest evidence yet or a flake, and
guessing was not acceptable.

**Bisected, not reasoned about.** A detached worktree at the branch's last documentation-only commit
gave a genuine pre-change baseline:

| Run | Result |
|---|---|
| `PCMetricsTest` isolated, with the change | pass x2, 10s |
| `PCMetricsTest` isolated, baseline | pass, 10s |
| All modules, baseline | BUILD SUCCESS |
| All modules, with the change | **FAILURE** (137s), then BUILD SUCCESS on re-run |

**This is evidence the un-quarantine was premature, and the first write-up of it said otherwise.**
`PCMetricsTest` was un-quarantined by astubbs#265 on 2026-08-13; this failure is from the day after,
and astubbs#304 has since removed its row from `test-untracked-ci-flakes.md` on the grounds that the
ledger named a quarantine no longer in force.

The first version of this entry dismissed the sighting as "load, not the test". That is the exact
move [`AGENTS.md`](../../AGENTS.md) warns against - *"a test failing under concurrent load may be
exposing a real main-code bug that only manifests under stress"* - and it is self-serving here,
because the load was partly self-inflicted by concurrent Maven runs. **"Only fails under load" is
what a flaky test is.** The suite runs under load in CI too.

What the bisect does establish is narrower than the dismissal claimed: the failure is not attributable
to astubbs#296, because it reproduces on a clean baseline worktree and the branch's changes cannot
reach a running instance. It says nothing about whether the test is fit for the gating lane.

**What would settle it**: a full-suite run on a CI runner, not a developer box, repeated enough times
to put a number on the rate. Until someone does that, treat the test as un-quarantined on the strength
of astubbs#265's fix and one contrary observation, rather than as proven stable.

**Flake.** The failing run took 137s where the passing ones take 10s, which is load, not logic. Two
other timing-sensitive tests each failed exactly once across the same session's ~10 heavy runs and
passed on repeat: `ParallelEoSStreamProcessorTest.processInKeyOrder` and
`ParallelEoSStreamProcessorTest.executorThreadsInterruptedOnShutdownTimeout`. Three different tests,
one failure each, none reproducible - that is a saturated box, not three regressions.

**What this adds to the family.** The signature reproduces *locally*, off the chaos suite, on an
ordinary unit run. Every prior sighting came from CI, which made the seed-randomised chaos harness
the natural suspect. This one had no chaos harness involved at all, which weakens any reading that
ties the family specifically to that suite.

**The method is the reusable part.** Neither "it passed on retry" nor "it failed once" is evidence
on its own. A worktree at the pre-change commit, the same command on both, is cheap and it is what
turns "probably a flake" into an answer. Do that before attributing - or dismissing - anything with
this signature, especially on a branch whose subject matter would explain it.

**Eighth sighting, 2026-08-17 - the `ZOMBIE_MEMBER` arm again, and its seed replays clean.**
`ChaosRevokeUnderWorkCooperativeIT.revokeUnderWorkStaysProtocolHonestWithCooperativeAssignor` on
[job 95308176649](https://github.com/astubbs/parallel-consumer/actions/runs/32002566427/job/95308176649),
on astubbs#204 at head `85a3646d8`. Same arm as the fourth sighting, on the revoke-under-work
cooperative scenario, and the failing assertion is the scenario SLO rather than a probe fail-fast -
the probe violation rides along in the autopsy:

```
no instance may end the run with an unclassified failure cause
but was: [instance 7: RuntimeException: Error from poll control thread:
          Timeout waiting for commit response PT10S to request CommitRequest(id=608fa144-...)]
ZOMBIE_MEMBER/REBALANCE_BLOCKED: group 'group-1-122304053' dwelling in PreparingRebalance for 15s
(bound 15s) - a member is not answering the rebalance (protocol-unresponsive)
peaks: rebalanceDwell=15465ms lagStagnation=85032ms
```

Two frozen partitions, stagnant 73-90s, lag 145 and 979. **Replay seed `2966043432903644461`**:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=2966043432903644461

**Replayed, and it does not reproduce.** On the same head, uncontended, all three chaos scenarios
passed - `ChaosRevokeUnderWorkIT` 135.2s, `ChaosChurnStormIT` 82.8s,
`ChaosRevokeUnderWorkCooperativeIT` 86.0s. That is the AGENTS.md diagnostic for separating contention
from a concurrency bug, and it lands on the contention side. It rules out a deterministic defect
reachable from that seed, and nothing else: a passing replay cannot exclude an interleaving only a
loaded box reaches.

**Branch context - astubbs#204 rhymes with this failure, which is why it needs naming rather than a
one-line dismissal.** That PR changes the commit path: `ConsumerManager.commitSync`'s retry budget
becomes per-call instead of per-attempt, so PC gives up where it previously hung, and the failure
cause here *is* a commit-response timeout. Against it being the cause: the same lane failed on this
same branch at `26d2f195`, before that change existed, and
[`ci-disabled-jobs-and-runner-load.md`](ci-disabled-jobs-and-runner-load.md) records the same window
killing the `highcpu` lane on three other branches with the no-verdict signature. Unresolved rather
than cleared - a later sighting on a branch that does not touch the commit path would settle it.

**Ninth sighting, 2026-08-18 - the fleet-level `NO_PROGRESS` arm, twice in one night (this entry
and the ninth below share a signature).** `ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger`
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

**Tenth sighting, 2026-08-18 - same test, same arm, four hours earlier, different branch.**
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
