# The confluentinc#857 family - what is still open

<!-- inflight-type: bug -->
<!-- inflight-labels: concurrency -->
<!-- inflight-impact: stall -->


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

**READ BEFORE RESUMING astubbs#29, and before reading any `CLASS2_STALL` entry below.** On
2026-08-25 the discriminating replay this file had been asking for since the twelfth sighting was
finally run, and it establishes that **the chaos suite's `CLASS2_STALL` reds are a timing proxy, not
sightings of this family** - both nominated seeds fired the bound and then drained completely. The
full evidence is the last section of this file; three consequences bind anyone picking astubbs#29 up:

- **Do not treat a `CLASS2_STALL` red as evidence for or against astubbs#29.** It measures how long a
  committed offset stays pinned, which one incomplete record does legitimately. The detector that
  would be real evidence is `INSTANCE_STALL/NO_WORK_COMPLETED`, which watches completions and has
  never fired.
- **A prediction recorded before the fact, so landing astubbs#29 tests it rather than merely
  following it:** land astubbs#29 and the rest of the backlog, re-run the chaos suite on a loaded
  box, and the Class 2 findings **continue at roughly the same rate**, because they are the bound
  meeting the load and no deadlock fix touches that. **If they instead drop off, this reading is
  wrong** - say so loudly here, because the whole 2026-08-25 section then needs revisiting.
- **The reproducer named for this deadlock runs in the one mode the deadlock cannot reach.**
  `RebalanceEoSDeadlockTest` configures `PERIODIC_TRANSACTIONAL_PRODUCER`, while
  [`revoke-path-commit-deadlock-between-poll-and-control-threads.md`](../solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md)
  states the AB-BA cycle is *"Only reachable in PERIODIC_CONSUMER_SYNC - the reproducer test runs a
  transactional mode where this cycle cannot occur"*. That sentence is in the solutions doc and the
  test still has the name, so the two disagree in the open. **Whichever is right matters to
  astubbs#29:** if the doc is right, the test's name promises coverage it does not have, and this
  file's "live confirmation the deadlock is still present: `RebalanceEoSDeadlockTest` failed once
  under the 20-run stress hunt" was a *different* failure being read as this one. If the doc is too
  narrow, the mode restriction it rests on needs revising. The cheap resolution is a sibling test at
  `PERIODIC_CONSUMER_SYNC` with the same rebalance shape - deliberately not added here, because on a
  branch with no fix it would land as a knowingly-red test and that is
  [`docs/quarantined-tests.md`](../quarantined-tests.md)'s decision to make, not a side effect of a
  chaos-suite change.
- **The old sequencing advice - "land the backlog, then re-run" - was sound when written and no
  longer applies to `CLASS2_STALL` specifically.** It still applies to any signature this file
  records that is *not* the lag bound. (It lived in a chaos-lane note since deleted; the reasoning
  that superseded it for the lag bound is
  [`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md).)

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

**SUPERSEDED 2026-08-19 - this sighting is a test defect, and does not belong to the family.**
<!-- post-merge: checked-begin -->
The entry above asks for "a full-suite run on a CI runner, repeated enough times to put a number on
the rate". A mechanism settles it instead:
[`bug-pcmetrics-committed-offset-vs-completion-count.md`](bug-pcmetrics-committed-offset-vs-completion-count.md)
**owns the diagnosis** - the assertion compares a contiguous commit offset to an out-of-order
completion counter under `UNORDERED`, and the gap is permanent, not slow. That explains every
observation here without invoking a stall: failing only under load (concurrency is what produces
out-of-order completion), passing in isolation, and both observed gaps - the 2 records here
(`205.0` vs `203.0`) and the 7 seen later on astubbs/parallel-consumer#322. That citation records
where a gap was OBSERVED, so it reads the same once that PR has landed.
<!-- post-merge: checked-end -->

**Do not count this as a family sighting.** It was recorded as "the family's signature" on the
strength of a shortfall under load, which the family shares with any test that races. Leaving it here
inflates the ledger with a defect that has nothing to do with the revoke path - the same contamination
this file already records once, when a transactional-mode failure was logged as confirmation of a
cycle impossible in that mode. Kept rather than deleted so the reasoning that led here is visible.

The original assessment follows, and is retained deliberately.

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

**Eleventh sighting, 2026-08-19 - the eager `CLASS2_STALL` again, and the first seed in this family
that replays RED on demand, with a master control arm proving it is not branch-caused.**
`ChaosRevokeUnderWorkIT.revokeUnderWorkStaysProtocolHonest` killed by `ProgressProbe` on
[job 95962195275](https://github.com/astubbs/parallel-consumer/actions/runs/32217696441), at head
`2b8b89183` on astubbs#204. Seven `CLASS2_STALL/LAG_STAGNATION` violations, lags 587-3010, all
stagnant 154s against the 150s bound; `peaks: rebalanceDwell=9949ms lagStagnation=154627ms`.

**Replay seed `6825864417772979246`**:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=6825864417772979246

**What makes this one different: it reproduced, twice, off CI.** Every earlier `CLASS2_STALL` entry
records a seed that nobody has since replayed red - the eighth sighting's seed explicitly "replays
clean". This one does not:

| Arm | Head | Verdict | Violations | `lagStagnation` |
|---|---|---|---|---|
| CI | `2b8b89183` (astubbs#204) | RED | 7 | 154627ms |
| Local replay | `2b8b89183` (astubbs#204) | RED | 5 | 154429ms |
| **Local control** | **`5c377ec04` (master)** | **RED** | **10** | **154387ms** |

**A RED here does NOT mean every `CLASS2_STALL` is a family occurrence.**
[`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md)
records one with this same ~154s signature whose seed replays **green** on an uncontended box,
peaking at 121.3s - self-hosted runner contention, from `Performance` and `Chaos Pain Suite` sharing
the box. That note owns the discriminator; the short version is that ~154s is what a crossed 150s
bound looks like regardless of cause, so only an uncontended replay of the seed separates the two.
This arm earns its place precisely because it replayed RED.

**The master arm is the point.** The same seed fails on plain master with no astubbs#204 code in the
tree, so this is the family's own defect and not something that PR introduced. Two further checks
agree: astubbs#204's tree at the passing run `45fd8f6f9` and the failing run `6b2d91370` have the
*identical* tree SHA `e939d33ca` (the run in between was a history-only re-cut), so content cannot
explain the pass/fail difference; and none of astubbs#204's new failure paths appear anywhere in the
replay output - no `OffsetCommitBudgetExceededException`, no poller-death handoff. The commit budget
was never exhausted and the poll thread never died, so the stall happens with every commit
apparently healthy.

**Control arm, same CI run:** the two sibling chaos tests passed with `probe violations=[]` -
cooperative on seed `2550023127530143017`, churn storm on `783220268230054177`.

**Same seed, different violation *count* (5 / 7 / 10).** The seed fixes the chaos schedule, not the
topic name (`ChaosRevokeUnderWorkIT-w4-<random>` differs every run) nor which partitions land on
which member. So it reproduces the *signature* reliably and the exact frozen-partition set not at
all. Treat a replay as red/green evidence, never as a per-partition fingerprint to diff.

**Why this is the cheapest next experiment the family has.** A seed that replays red locally in ~3
minutes on master turns every prior "is it the product or the harness" question into something
bisectable. The obvious next step is to bisect it against the family's landed fixes, and to attach a
thread dump at the moment `lagStagnation` crosses ~150s - the probe already knows when that is.

**Two traps for whoever picks this up.** The retrieval note above still holds and cost real time
here: GitHub served a log with *no* verdict in it at all - no `Tests run:`, no `BUILD`, no autopsy -
for a step that had genuinely failed a test, and `--log-failed` showed only ordinary INFO. The
verdict was in the uploaded failsafe XML artifact, as documented. Separately, narrowing the replay
with `-Dit.test=ChaosRevokeUnderWorkIT` needs `-Dfailsafe.failIfNoSpecifiedTests=false`, because
`-am` puts the parent module in the reactor and its own failsafe execution then fails the build with
"No tests matching pattern"; dropping `-am` instead fails the enforcer's `ReactorModuleConvergence`.
The unnarrowed command above has neither problem.

**Twelfth sighting, 2026-08-20 - three `Chaos Pain Suite` reds inside twenty minutes across two
branches, and the first one is on the DRAIN control arm.** Four scenarios fired between 01:05 and
01:21Z. All the numbers below come from the uploaded failsafe artifacts, per the retrieval note
above, not from the console.

| Time (Z) | Branch | Head | Run | Arm that fired | Violations | `lagStagnation` peak | Seed |
|---|---|---|---|---|---|---|---|
| 01:05 | `test/chaos-instrumentation` (astubbs#325) | `9698012d9` | [32319767471](https://github.com/astubbs/parallel-consumer/actions/runs/32319767471/job/96279438153) | `ChaosKeyOrderIT` | 1 `ZOMBIE_MEMBER` | 44074ms (dwell 15452ms) | `1055565754928226840` |
| 01:05 | as above | as above | as above | `ChaosRevokeUnderWorkDrainIT` | 12 `CLASS2_STALL` | 154321ms | `3426636341371267227` |
| 01:12 | `split/docs-ledger-and-plans` (astubbs#323) | `c748ca667` | [32320198063](https://github.com/astubbs/parallel-consumer/actions/runs/32320198063) | `ChaosChurnStormIT` | 23 `CLASS2_STALL` | 154064ms | `4156620401101833712` |
| 01:12 | as above | as above | as above | `ChaosRevokeUnderWorkIT` | 3 `CLASS2_STALL` | 154360ms | `1680705091015468437` |
| 01:21 | `test/chaos-instrumentation` (astubbs#325) | `4b7ffc6e6` | [32320705139](https://github.com/astubbs/parallel-consumer/actions/runs/32320705139/job/96282172263) | `ChaosRevokeUnderWorkDrainIT` | 34 `CLASS2_STALL` | 154239ms | `4044221734199516240` |

Every `CLASS2_STALL` line is the familiar one - 154s of stagnation against the 150s bound, group
STABLE with heartbeats flowing. Replay any of them with:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=<seed>

**The four stagnation peaks land within 300ms of each other** (154064-154360ms), which is the same
constant the eleventh sighting measured across CI, a local replay and a local master control
(154627 / 154429 / 154387ms). That is corroboration of a known characteristic of the signature, and
it is **not** evidence about the machine: the eleventh sighting produced ~154.4s from a single
uncontended local run on plain master. Its warning applies here too - the same seed there yielded
5, 7 and 10 violations, so the count is not a per-partition fingerprint and the 34-vs-12 difference
between the two drain-arm runs says nothing on its own.

**astubbs#323's head is the cleanest not-PR-introduced control this file has recorded.** Against the
merge base, `c748ca667` changes exactly three lines of main code and they are all inside
`// TODO(refactor):` comments - a `docs/inflight/` filename repointed after a rename, in
`ConsumerManager` and `ConsumerOffsetCommitter` - plus one `@Quarantined` annotation on
`PCMetricsTest`, which the chaos lane does not run. No executable main-code change exists on that
branch to reach a broker with. Prior entries argued branch innocence from "docs and one constant";
this one is a comment-only diff, and it agrees with the eleventh sighting's master arm rather than
proposing anything new.

**The new observation is the drain control arm.** `ChaosRevokeUnderWorkDrainIT` arrives with
astubbs#325 and exists precisely to test the leading explanation for this signature: that the
watermark pinning is redelivery of heavy work abandoned by a non-draining stop. Its own javadoc
pre-declares both outcomes, and this is the second one - *"Drain arm ALSO red - the abandoned-work
explanation is wrong and something else pins the watermark."*

**Do not cash that in yet, for two reasons.** The arm is brand new and has four CI exposures in
total: green at 21:17 on 2026-08-19 (run 32303069710, seed `754504705742948700`, all seven scenarios
green), then red three times tonight on three different seeds (01:05, 01:21 and the 01:41 run
below). One green and three reds is a rate worth noticing, not a calibration.
More importantly, a fail-fast red proves the *bound* was crossed, not that the backlog never drained
- which is exactly the critique in
`test-class2-probe-asserts-timing-not-correctness.md` - deleted once its critique was settled, so
read it as this entry read it with
`git show 77beb4f31:docs/inflight/test-class2-probe-asserts-timing-not-correctness.md` and grep
`What the proxy costs, measured` - where seed `4734674029169027864` trips this bound 53 times on the
eager arm and still drains completely. Until one of tonight's drain seeds is replayed with
`-Dchaos.diagnoseStallRecovery=true`, "the drain arm is also red" and "the drain arm is also slow"
are the same observation.

**What would settle it, and it is cheap:** replay `4044221734199516240` (34 violations, the larger
sample) with `-Dchaos.diagnoseStallRecovery=true` and read whether the backlog drains. Drains ->
this is the timing-proxy problem the probe note owns, and the abandoned-work explanation survives
untouched. Does not drain -> the drain arm has done its job and the family has its first real lead
on a Class 2 mechanism.

**Every cooperative arm passed, in all three runs.** `ChaosRevokeUnderWorkCooperativeIT` was green
each time (seed `4001744813866842738` in the astubbs#323 run, alongside the two eager reds), and
`ChaosRevokeUnderWorkCooperativeDrainIT` green in both astubbs#325 runs. Each arm that fired
`CLASS2_STALL` is an eager one. That is consistent with the second and sixth sightings'
eager-protocol-specific reading and adds a third occasion to it - though the note recorded above the
fifth sighting, of a `CLASS2_STALL` on the *cooperative* variant locally, still stands against
treating it as settled.

**Two sibling tests failed together in one run for the first time in this arm.** In the astubbs#323
run, `ChaosChurnStormIT` and `ChaosRevokeUnderWorkIT` both fired `CLASS2_STALL` while the cooperative
sibling passed. Earlier entries record the siblings as a *passing* control arm within the run (fourth,
ninth, tenth, eleventh sightings all name two green siblings). Recorded as an observation; two of
three arms drawing the signature on one broker in one run is not by itself evidence of anything the
per-arm entries do not already say, but "the siblings always pass" is no longer true.

**astubbs#325 is not a suspect for its own two reds, and the second one has a specific check.** That
branch adds test-integration code only. The 01:21 run is at `4b7ffc6e6`, whose only change over
`9698012d9` moves the drain arms' identical action-mix map into
`AbstractRevokeUnderWorkScenario#drainOnlyChaosWeights()`; the run's own log confirms the mix is
unchanged - `weights={STOP_DRAIN=3, RESTART=3, JOIN_NEW=2} tick=PT0.3S..PT1S joinAfterDrainBias=0.0`
- and the same arm had already fired at 01:05, before that commit existed.

**A fourth red the same night, on a documentation-only commit.** Run
[32321963226](https://github.com/astubbs/parallel-consumer/actions/runs/32321963226/job/96285796525)
at 01:41Z, head `202da9d0a` on astubbs#325 - which differs from `4b7ffc6e6` by two markdown files and
nothing else. Three arms fired, all eager: `ChaosChurnStormIT` 23 `CLASS2_STALL` plus 2
`ZOMBIE_MEMBER` (peaks `rebalanceDwell=15602ms lagStagnation=151882ms`, seed `989468380938115993`),
`ChaosRevokeUnderWorkDrainIT` 43 `CLASS2_STALL` (`lagStagnation=152433ms`, seed
`6334815371835997501`), `ChaosRevokeUnderWorkIT` 4 `CLASS2_STALL` (`lagStagnation=154211ms`, seed
`2553818384688673562`). Both cooperative arms passed again, on seeds `7815585942040470933` and
`2549365579181485261`.

**Two things this adds.** A commit whose entire diff is prose cannot be a suspect, which is the same
argument astubbs#323's head makes and this time on the branch that owns the chaos code. And the two
`ZOMBIE_MEMBER` violations arrive **inside** a `CLASS2_STALL` autopsy again - the sixth sighting was
the first time both arms fired in one run and said "these are unrelated signatures can no longer be
assumed for free"; this is the third such run in the ledger, so that caution has now outlived being
a one-off.

**And then the next commit ran green, which is the tightest control this file has.** Head
`41c8cda26` differs from the red `202da9d0a` by **one markdown file** - the paragraph above - and
[run 32323037970](https://github.com/astubbs/parallel-consumer/actions/runs/32323037970/job/96288770167)
passed all seven chaos scenarios. Two adjacent commits, a prose-only diff between them, red then
green: whatever draws this signature is drawn per seed, and no tree-content explanation survives that
pair. Every prior entry asserts seed-dependence from branch subject matter; this one measures it.

**Thirteenth sighting, 2026-08-20 - the first chaos run with the new detectors, and none of them
fired.** `Chaos Pain Suite` on astubbs/parallel-consumer#325's merged head `283202eb5`
([run 32334089543](https://github.com/astubbs/parallel-consumer/actions/runs/32334089543)), 8 of 9
chaos ITs green, `ChaosRevokeUnderWorkDrainIT.revokeUnderDrainingStopsStaysProtocolHonest` red at
173s. **21 violations, all one kind**: `CLASS2_STALL/LAG_STAGNATION`, committed offset stagnant
~153s against the 150s bound, group STABLE and heartbeats flowing.
`peaks: rebalanceDwell=9935ms lagStagnation=153984ms`.

**Seed `2801529966526445415`** - recorded because the artifact expires and, by this ledger's own
argument, the seed is the asset:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=2801529966526445415

**What makes this entry worth more than another tally mark: zero `INSTANCE_STALL`, zero
`LEDGER_KEY_ORDER`, zero `LEDGER_KEY_CONCURRENCY`.** This was the first full chaos storm run against
the ordering ledger and the stall probe astubbs#325 added, and neither produced a single finding. A
new detector's most likely failure is crying wolf; on this evidence they do not. The red is entirely
the pre-existing `CLASS2_STALL` timing bound, whose value and gating that PR deliberately left alone.

**Contention is a hypothesis here, not the finding.** `Performance (optional)` ran 05:02:53-05:05:42
on the same self-hosted box, overlapping the chaos job's opening minutes, and this ledger already
records that pairing as a prior cause. But the discriminator is an uncontended replay of *this* seed,
and per the table in
[`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md)
the GREEN side needs two or three replays before it settles anything. **Nobody has replayed it.**
Recorded as unresolved.

<!-- post-merge: checked-begin -->
**Fourteenth sighting, 2026-08-25 - THREE reds on a docs-and-comments branch, fresh seeds each time,
and the contention explanation does NOT fit.** `Chaos Pain Suite` failed at three consecutive heads
while astubbs#347 was in review - `22826761a`, `5f9fa4088` (run `32803999735`) and `7753777c3` (run
`32805270339`) - having passed at `257c4173a`, an earlier head of the same work. Three of the four
chaos runs on that branch were red. The harness randomises its seeds per run, so these are three
independent seed sets rather than one bad seed replaying. The first two runs' arms are timing bounds
tripped by margins of 2.7% and 0%:
<!-- post-merge: checked-end -->

| Test | Probe | Margin | Seed |
|---|---|---|---|
| `ChaosRevokeUnderWorkDrainIT.revokeUnderDrainingStopsStaysProtocolHonest` | `CLASS2_STALL/LAG_STAGNATION` on 4 partitions | 154s against a 150s bound | `3334891073975887762` |
| `ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger` | `NO_PROGRESS`, fleet stuck at 97896/100000 | 30s against a 30s bound | `1521825993857670757` |

**The branch cannot be the cause, and that is unusually easy to state here.** The diff from the head
that passed to the head that failed is six markdown files plus a comment and one `iterations` value
in a `@Tag("lincheck")` class - **no `src/main` file anywhere in the PR**, no integration test, no
chaos test, no pom change in that range, and the `lincheck` tag is excluded from the chaos suite's
group filter. So this is another data point for the eleventh sighting's finding that the reds are
seed-dependent rather than branch-dependent, from about as inert a branch as could produce one.

**What is new: the runner-contention hypothesis is ruled out on timing, not merely unreplayed.** The
thirteenth sighting names `Performance (optional)` overlapping the chaos job as the prior cause. It
ran here too - but on `highcpu-2`, finishing **03:10:36**, while the first failing test did not start
until **03:15:05** and the second failed at 03:19:37 on `highcpu-6`. There is no overlap, so whatever
produced these two, it was not that pairing.

**The third run repeated `CLASS2_STALL` on the same test and added a second arm.**
`ChaosRevokeUnderWorkDrainIT` tripped `CLASS2_STALL` again on seed `7325551558538345707`, and
`ChaosKeyOrderIT.perKeyOrderSurvivesChurn` tripped `ZOMBIE_MEMBER` on seed `4984003374538738324` -
the arm the fourth and eighth sightings describe. So the pattern is not one arm on one seed: it is
this suite going red at a high rate right now, against a branch that provably cannot influence it.
**That points at the box or at master, and it is worth someone's attention independent of this PR** -
`Integration Tests` on the same box also failed in this window with the Kafka broker container
exiting 126 (it never started), and passed on a straight re-run.

**Still unresolved, and by the same missing step as every prior entry: nobody has replayed the
seeds.** Recorded rather than diagnosed. The `CLASS2_STALL` arm is the one
`test-class2-probe-asserts-timing-not-correctness.md` argues is asserting the wrong property (that
note was deleted once its critique was settled - read it as this entry read it with
`git show 77beb4f31:docs/inflight/test-class2-probe-asserts-timing-not-correctness.md`) - the probe's own message says the bound "is a TIMING
measurement, not a correctness verdict" - and a `-Dchaos.diagnoseStallRecovery=true` replay of
`3334891073975887762` is what would say whether the backlog drains. `Chaos Pain Suite` is **not** in
master's required-checks ruleset, so neither red blocked the PR.
<!-- post-merge: checked-begin -->
**Fifteenth sighting, 2026-08-24 - the eager `CLASS2_STALL` again, on a branch that added no reactor
code at all.** Numbered after the fourteenth although it happened a day *earlier*: both were written
while their PRs were open and astubbs#347 merged first. The numbers are recording order, not
chronology, and renumbering a merged entry would break every citation of it.
<!-- post-merge: checked-end -->

`Chaos Pain Suite` on the jcstress probe module's head `37376d89d`
([job 97629465695](https://github.com/astubbs/parallel-consumer/actions/runs/32789987174/job/97629465695)),
8 of 9 chaos ITs green, `ChaosRevokeUnderWorkIT.revokeUnderWorkStaysProtocolHonest` - the **eager**
variant - red. **One violation**: `CLASS2_STALL/LAG_STAGNATION`, partition
`ChaosRevokeUnderWorkIT-w4-1541869711-77` lag=2699, committed offset stagnant at 396 for 153s
against the 150s bound, group STABLE. `peaks: rebalanceDwell=7735ms lagStagnation=153984ms`, and 20
further partitions frozen 19-36s. The run still consumed 241403 records.

**Seed `8497120797726003675`** - recorded because the artifact expires and the seed is the asset:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=8497120797726003675

<!-- post-merge: checked-begin -->
**The branch was not a suspect, and this one is unusually clean on that point.** The change under
test added the
`jcstress-poc` module, which has no `<parent>` and no entry in the root `<modules>`, so no reactor
build - the chaos lane included - ever compiles it; the rest of that diff is `.gitignore` and two
markdown files. Nothing in the change is reachable from the code under test.
<!-- post-merge: checked-end -->

**Contention is a weaker hypothesis here than in the thirteenth sighting, and is recorded as
unresolved either way.** `Performance (optional)` did share the self-hosted box (`highcpu-4` against
the chaos job's `highcpu-1`) but ran 23:38:46-23:41:49, while the failing scenario started at
23:41:20 and its 153s stagnation window opened around 23:41:28 - so the overlap covers only the
window's first ~20 seconds, not the storm phase the way
[`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md)
records it. As there, the discriminator is an uncontended replay of this seed, and **nobody has
replayed it**.

**Counted as a tally mark, not as evidence of the stall.** By
`test-class2-probe-asserts-timing-not-correctness.md` (deleted once settled; read it at
`git show 77beb4f31:docs/inflight/test-class2-probe-asserts-timing-not-correctness.md`, and its
conclusions now live in
[`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md))
this bound is a timing measurement inside a correctness suite, and the probe's own message now says
so; one violation at 153s against 150s, on a run that consumed 241403 records, is the marginal case
that critique predicts rather than a protocol-invisible wedge.

**The very next run of the same branch went red on the OTHER arm** - not a new numbered sighting,
because the signature is already in the twelfth sighting's table, but the seed is worth keeping.
`ChaosKeyOrderIT.perKeyOrderSurvivesChurn`
([job 97655734973](https://github.com/astubbs/parallel-consumer/actions/runs/32798888723/job/97655734973)),
two `ZOMBIE_MEMBER/REBALANCE_BLOCKED` violations, `rebalanceDwell=15449ms` - within 3ms of the
15452ms already recorded for this same test in that table. **Seed `1296229998219100811`.**

Two things about it are worth more than the tally. **The key-order detectors did not fire**: zero
`LEDGER_KEY_ORDER`, zero `LEDGER_KEY_CONCURRENCY` on the test whose whole purpose is per-key
ordering under churn, so ordering held and the red is the rebalance arm only. And **two consecutive
heads of one branch drew different arms** - `CLASS2_STALL` then `ZOMBIE_MEMBER` - which is the same
pairing the sixth sighting called new when it saw both in one run. `Performance (optional)` again
shared the box (`highcpu-6` against the chaos job's `highcpu-2`), overlapping 01:51:35-01:53:52 of a
job that ran to 02:04:34. Neither seed has been replayed uncontended.

**And the third consecutive head drew a THIRD arm** - `ChaosChurnStormIT.churnStormMeetsSlosAndBalancesLedger`
([job 97659446984](https://github.com/astubbs/parallel-consumer/actions/runs/32800216920/job/97659446984)),
`NO_PROGRESS: fleet consumed count stuck at 98804/100000 for 30s (bound 30s)`, the arm the ninth and
tenth sightings record. **Seed `2575991864395313898`.** So three chaos runs on one branch in one
night went red on three *different* detectors - `CLASS2_STALL`, then `ZOMBIE_MEMBER`, then
`NO_PROGRESS` - on a branch whose entire delta is markdown plus a module no reactor build compiles.

**A fourth run made it four for four, and drew two arms at once.**
`ChaosRevokeUnderWorkDrainIT.revokeUnderDrainingStopsStaysProtocolHonest` with **5**
`CLASS2_STALL/LAG_STAGNATION` violations (stagnant 154s against the 150s bound, **seed
`3602363451667827241`**) and `ChaosKeyOrderIT.perKeyOrderSurvivesChurn` with one
`ZOMBIE_MEMBER/REBALANCE_BLOCKED`, `rebalanceDwell=15436ms` (**seed `3452960246289915619`**) - a
third `ChaosKeyOrderIT` dwell within 20ms of the other two on file. Four for four is a statement
about the suite's current pass rate on master, not about any of the seeds; **no PR should be read as
having broken the chaos lane on this evidence, and no green chaos run in this window should be read
as clearing one.**

**Across all four runs the ordering detectors stayed silent** - zero `LEDGER_KEY_ORDER`, zero
`LEDGER_KEY_CONCURRENCY`, including twice on the test whose entire purpose is per-key ordering under
churn. Every red is a liveness/timing detector. That is the load-bearing negative here: the suite is
noisy about progress bounds and has said nothing about correctness.

**Read that third autopsy with care**: it printed `violations (0): (none crossed the
chaos-calibrated bounds)` on a run its own summary records as killed by the `NO_PROGRESS` violation
above. That is a defect in the autopsy rather than in this family, and it has its own note -
[`test-chaos-autopsy-omits-fleet-violations.md`](test-chaos-autopsy-omits-fleet-violations.md).
Entries in this ledger that lean on a clean autopsy are worth re-checking against it.
<!-- post-merge: checked-end -->

<!-- post-merge: checked-begin -->
**Sixteenth sighting, 2026-08-25 - the drain arm again, and a SECOND same-day timing rule-out of
contention.** `Chaos Pain Suite` on astubbs/parallel-consumer#353's head `c1f423e4a`
([run 32807910210](https://github.com/astubbs/parallel-consumer/actions/runs/32807910210/job/97681493424)),
<!-- post-merge: checked-end -->
`ChaosRevokeUnderWorkDrainIT.revokeUnderDrainingStopsStaysProtocolHonest` red at 172s. **4
`CLASS2_STALL/LAG_STAGNATION` in the autopsy** (3 fired live), committed offsets stagnant 154s
against the 150s bound, group STABLE and heartbeats flowing.
`peaks: rebalanceDwell=13178ms lagStagnation=154263ms` - the ~154s constant again, inside the 300ms
band the twelfth sighting measured across four arms (154064-154360ms).

**Seed `6037000644302969438`** - recorded before the log expires:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=6037000644302969438

Two things this entry adds. **The branch is not a suspect, again**:
<!-- post-merge: checked-begin -->
at the observed head `c1f423e4a`, astubbs#353 carried only
<!-- post-merge: checked-end -->
`.claude/hooks/pre-commit-gate.sh`, `bin/test-check-agent-hooks.sh` and `docs/agent-harness.md` -
no Java at all - the same not-PR-introduced control the fourteenth and fifteenth sightings each
recorded. **And it corroborates the fourteenth sighting's timing rule-out of runner contention**:
`Performance (optional)` finished at 04:14:18Z and the failing drain arm did not start until
04:17:05Z, three minutes after the box went quiet - the second drain-arm red in one day where the
contention pairing provably was not present. The discriminator remains an uncontended replay, and
nobody has replayed this seed either. Recorded as unresolved.

**The same branch's next run drew TWO arms at once** - not a new numbered sighting, the signature
is already this entry's, but the seeds are the asset.
[Run 32863352692](https://github.com/astubbs/parallel-consumer/actions/runs/32863352692/job/97852502402):
`ChaosRevokeUnderWorkIT.revokeUnderWorkStaysProtocolHonest` (**eager**, seed
`5501517460666962649`, `lagStagnation=154115ms`) and
`ChaosRevokeUnderWorkDrainIT.revokeUnderDrainingStopsStaysProtocolHonest` (seed
`7370431147468591204`, `lagStagnation=154524ms`), 47 `CLASS2_STALL/LAG_STAGNATION` violations
between them, every peak inside the familiar ~154s band. And a THIRD same-day timing rule-out of
contention: `Performance (optional)` ended 15:06:08Z, the eager arm started 15:08:36Z and the
drain arm 15:11:28Z. The head under test differs from this entry's by hook-script and markdown
commits only.

## 2026-08-25: the discriminator was finally run, and it closes the `CLASS2_STALL` line of this file

**Every `CLASS2_STALL` entry above is a timing measurement, not a family sighting.** The twelfth
sighting named the experiment that would decide it - *"replay `4044221734199516240` (34 violations,
the larger sample) with `-Dchaos.diagnoseStallRecovery=true` and read whether the backlog drains"* -
and pre-declared both readings. It has now been run, on that seed and on the eleventh sighting's, and
the answer is the one that entry called the timing-proxy side.

| Seed | Arm | Head | Observations fired | Outcome |
|---|---|---|---|---|
| `6825864417772979246` (eleventh sighting - the seed with the plain-master control arm) | `ChaosRevokeUnderWorkIT` | `da91f3f61` (master) | 2 | **drained**: `consumed=251326/250000 started=251326 inFlight=0`, full key coverage, 33s after the bound was crossed |
| `4044221734199516240` (twelfth sighting's own nominated seed) | `ChaosRevokeUnderWorkDrainIT` | `da91f3f61` (master) | 46 | **drained**: `consumed=251726/250000 inFlight=0`, full key coverage |

**Both ran on a CONTENDED developer box, which biases toward "did not drain".** They drained anyway.
That asymmetry is what makes a local run worth the minutes here, and it is the reverse of the usual
caution in [`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md):
that note's "a GREEN replay needs two or three" rule governs an *absent* violation, where contention
and a quiet schedule are indistinguishable. These runs are not absences - the bound was crossed, 2 and
46 times, and the run finished anyway. A fired-and-drained replay is positive evidence, and one is
enough for the same reason one RED replay was.

**The eleventh sighting's master arm does not survive as evidence of a defect, and it is the entry
that most needs re-reading.** Its argument was that the seed replays RED on plain master, so "this is
the family's own defect and not something that PR introduced". The first half still holds - the
schedule really does cross the bound on master, deterministically. The second half does not follow:
crossing a timing bound on master means master is slow on that schedule, not that master is wedged.

**The ~154s constant was never corroboration, and several entries above read it as such.** The sixth,
eleventh and twelfth sightings all remark on peaks landing within a few hundred milliseconds of each
other across runs, branches and machines. That is arithmetic, not signal: the probe samples every 5s
and the scenario fail-fasts on the first violation, so the peak is always bound plus detection latency
and can carry no severity information at all. The note above already said "~154s is a signature, not a
diagnosis"; this is the same point, now with the mechanism rather than the caution.

**What this does NOT close.** The confluentinc#857 wedge is real and `bugs/857-paused-consumption-multi-consumers-bug`
(astubbs#29) still fixes a real deadlock. What is now established is narrower and more useful: **the
chaos suite has never reproduced it.** That agrees with
[`test-chaos-phase2.md`](test-chaos-phase2.md)'s own long-standing assessment - *"a 9-seed sweep found
0 hits"* - and disagrees with the accumulating shape of this file, which read fourteen timing
crossings as fourteen sightings.

**The detector that would be a real sighting exists, and has never fired.**
`INSTANCE_STALL/NO_WORK_COMPLETED` (astubbs#325) watches COMPLETIONS, so it cannot fire on
slow-but-progressing - it is the wedge signature exactly. The thirteenth sighting is its first full
exposure: 21 violations in one storm run, every one of them the Class 2 timing bound, zero
`INSTANCE_STALL`. **Honest limit: "it has never cried wolf" and "it has never had a wolf to catch"
are not yet distinguishable** - the detector is days old. That is an argument for watching it, not
for keeping a bound that cries constantly.

**A second limit, and it is a correction to the paragraph above rather than a footnote to it.**
`INSTANCE_STALL` is per-INSTANCE, so it does NOT cover everything the demoted bound was watching. One
partition's committed offset freezing while the owning instance's other shards keep completing fires
nothing that gates - `INSTANCE_STALL` is re-armed by any returned work result, and the ledger counts
records processed rather than offsets durably committed. **So the demotion reduced per-shard liveness
coverage; it did not relocate it**, and an earlier version of this entry said otherwise. Tracked, with
the correlated gate that would close it and the red control that gate must have first, in
[`test-per-shard-liveness-has-no-gate.md`](test-per-shard-liveness-has-no-gate.md).
<!-- post-merge: checked-begin -->
It was raised by the cross-model adversarial reviewer on astubbs#354, the PR that demoted the bound;
three in-process reviewers on that same diff missed it, which is the clearest argument this file
records for keeping a cross-model pass.
<!-- post-merge: checked-end -->

**A falsifiable prediction, recorded before the fact.** Land astubbs#29 and the rest of the backlog,
re-run the chaos suite on a loaded box, and the `CLASS2_STALL` findings continue at roughly the same
rate - because they are the bound meeting the load, and neither the deadlock fix nor the sequencing
argument in the contention note touches that. If they instead drop off, this entry is wrong and
should be marked so loudly.

**Consequences already applied:** the bound now reports instead of gating (`Class2ObservationIT`
guards the routing), the diagnostic mode's quiet cap no longer silently exceeds the scenario
`@Timeout` - which is why nobody had run this experiment in the five days since two documents called
it cheap - and the chaos lane no longer runs several suites at once on one box.

### Correction to the section above: `INSTANCE_STALL` has now fired, once

<!-- post-merge: checked-begin -->
The section above says the detector that would be a real sighting "has never fired", and states the
honest limit that never-cried-wolf and never-had-a-wolf are not yet distinguishable. One of those two
has moved.

**On [run 32812259117](https://github.com/astubbs/parallel-consumer/actions/runs/32812259117),
`ChaosChurnStormIT` fired `INSTANCE_STALL/NO_WORK_COMPLETED` at `t=+154670ms`** - *instance 70 holds
work (queued=0, outForProcessing=43) but has returned no work result for 150s (bound 150s) at 27914
results returned*. Seed `7852140587594987229`. The 23 violations the autopsy then listed were all
Class 2, so the interesting one is absent from the autopsy list and only in the run log - which is
worth knowing for whoever reads the next one.

**This does not make it a wedge, and the section above is why.** One firing, on a box also running
`Performance`, with no replay: `INSTANCE_STALL` is re-armed by any returned work result, so an
instance that is merely very slow can trip it. What it does change is the evidential position - the
detector can fire, which was previously untested either way, so "watch it" is now a live instruction
rather than a hope. The replay that would settle it is the same one this file already prescribes,
against that seed.

**Corroboration for the prediction, not a test of it.** Three consecutive chaos runs on
astubbs/parallel-consumer#357 - a branch changing agent hooks, shell gates and documentation and no
product code - went red on four distinct tests across five distinct seeds, every violation Class 2,
counts falling 23, 17, 3, 2 (seeds `7852140587594987229`, `567232329738342203`,
`5843692436386966698`, `4651058060322796970`). Two partitions grazing the bound is what a timing
proxy meeting load looks like. It is **not** the registered prediction, which asks for a re-run after
astubbs#29 and the backlog land; recorded here so nobody later reads it as one.

One of those runs took the **cooperative** arm down with an explicit probe verdict, which retires the
second sighting's "eager-protocol-specific" reading for Class 2 specifically - a small thing now that
Class 2 is a speed measurement, but it was an open question above.
<!-- post-merge: checked-end -->

## 2026-08-25, INTEGRATION lane: the commit-response timeout, on the deadlock's exact preconditions

<!-- post-merge: checked-begin -->

**A sighting from the ordinary `Integration Tests` lane rather than the chaos suite**, on
astubbs#354's head `d1184338b`
([run 32824349833, job 97729240493](https://github.com/astubbs/parallel-consumer/actions/runs/32824349833/job/97729240493),
08:03-08:04Z). One error in 155 tests - not a cascade, and not the infrastructure failure that killed
the previous run on the same PR (recorded at the end of this entry so the two are not conflated).

    MultiInstanceRebalanceTest.consumeWithMultipleInstancesPeriodicConsumerSync(ProcessingOrder)[1]
    expected: 1000  commit: PERIODIC_CONSUMER_SYNC  order: UNORDERED  max poll: 500

    Terminal failure in one or more of the PCs:
    Error from poll control thread: Timeout waiting for commit response PT10S to request
    ConsumerOffsetCommitter.CommitRequest(id=e7c10671-c1cb-4ab8-a57b-905070e7b5b4,
    requestedAtMs=1787644975926) - the broker poll thread is the only producer of commit
    responses, and it has not died with an exception, so it is not answering: it is blocked
    or slower than the configured offsetCommitTimeout.

**Why this earns an entry rather than a retry.** It lands on all three preconditions
[`revoke-path-commit-deadlock-between-poll-and-control-threads.md`](../solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md)
states for the AB-BA cycle - **multiple consumers**, **a rebalance**, and **`PERIODIC_CONSUMER_SYNC`**,
which that document says is the only mode where the cycle is reachable at all. The exception's own
text supplies the other half: the poll thread *has not died*, so it is blocked - which is the poll
thread parked in `onPartitionsRevoked` on `synchronized (commitCommand)` while the control thread
waits on the `commitAndWait()` only that thread can service.

**Not proven, and the missing evidence is nameable.** There is no thread dump, so "parked on the
commitCommand monitor" is inferred from the preconditions plus the exception's own reasoning, not
observed. The honest alternative is a broker slow enough to miss a 10s commit deadline under CI load,
which produces the identical message - [`bug-offset-commit-timeout-does-two-jobs.md`](bug-offset-commit-timeout-does-two-jobs.md)
is about exactly that ambiguity. **The discriminator is a thread dump at the moment of the timeout**,
and nothing in this lane captures one; that is the cheapest thing to add before the next occurrence.

**astubbs#354 is not a suspect.** Its diff is chaos-suite test infrastructure, CI workflow and
documentation. `MultiInstanceRebalanceTest` is not a chaos scenario, never constructs a
`ProgressProbe` in chaos mode, and none of the classes on the failing path
(`ConsumerOffsetCommitter`, `BrokerPollSystem`, the poll thread) are touched by that branch. The
eight most recent `CI` runs on master were all green, which fits the probabilistic behaviour this
file already attributes to the root-cause stall rather than a regression.

**What it adds, stated carefully.** Every prior entry here is a chaos-suite finding, and the
2026-08-25 discriminator entry concluded those were overwhelmingly the Class 2 *timing* proxy. This
is the other shape: a hard commit-response timeout, a terminal PC failure, consumption stopped - from
a suite with no calibrated bounds to cross. It is a **candidate** occurrence of the product deadlock
astubbs#29 fixes, arriving from the lane nobody was watching for it. If it holds up, that is a better
argument for landing astubbs#29 than any chaos seed in this file. Treat it as one datum: the
preconditions match and the message fits; a thread dump would settle it.

**The previous run on the same PR, so it is not mistaken for a recurrence of this.** Head `e668d8ca`
failed the same lane for an unrelated INFRASTRUCTURE reason: the Kafka container exited with code 126
and timed out waiting for `[KafkaServer id=N] started` on `confluentinc/cp-kafka:7.9.0`, after which
every remaining class failed instantly with `NoClassDefFoundError: Could not initialize class
BrokerIntegrationTest` - one dead static initialiser cascading through the fork. Different signature,
different cause: two consecutive reds on one PR that are not the same failure.

This sighting was observed on astubbs#354, a chaos-suite change, and is recorded here because the
evidence for a CI failure dies with its logs. That PR is not its cause - it reads the same after the
merge, when astubbs#354 is a landed commit rather than an open PR.
<!-- post-merge: checked-end -->

## 2026-08-25, the first chaos RED under the demotion - and it is NOT Class 2

<!-- post-merge: checked-begin -->
`Chaos Pain Suite` on astubbs#354's head `f7d0ad0e4`
([job 97734555500](https://github.com/astubbs/parallel-consumer/actions/runs/32826167590/job/97734555500)),
the first full run with `CLASS2_STALL` demoted to a non-gating observation. **Seed
`1838980910098175839`.**

    ChaosKeyOrderIT
    ZOMBIE_MEMBER/REBALANCE_BLOCKED: group 'group-1-275607478' dwelling in
    PreparingRebalance for 15s (bound 15s) - a member is not answering the rebalance

**Six of the seven scenarios logged `probe violations=[], non-gating observations=[]`** - not merely
no violations, but **no Class 2 observations either**, on a run of 250k records per scenario. The
demotion is therefore not hiding a flood on this seed; there was nothing to hide.

**The one failure is a different detector, and it still gates.** `ZOMBIE_MEMBER/REBALANCE_BLOCKED` is
the Class 1 rebalance-dwell probe, which this branch does not touch: a member not answering the
rebalance is protocol-VISIBLE, bounded at 15s against a measured healthy peak of ~6.7s, and it is a
correctness claim rather than a timing proxy. This is the suite behaving as the demotion intended -
quiet about speed, still red about a member that will not answer.

**Why it belongs in this file.** `ZOMBIE_MEMBER` is one of the family's own signatures (the fourth,
fifth and twelfth sightings all carry it). Recorded with its seed because the seed is the asset and
the artifact expires.

**DIAGNOSED, and it is calibration - the same defect class as the Class 2 finding, one detector
over.** The seed replays RED deterministically, including on plain master:

| Arm | Head | Verdict | `rebalanceDwell` peak |
|---|---|---|---|
| CI | `f7d0ad0e4` (astubbs#354) | RED | 15411ms |
| Local replay | `f7d0ad0e4` (astubbs#354) | RED | same violation |
| **Local control** | **`cf0007df1` (plain master)** | **RED** | **15658ms** |

So it is **master-state, not PR-state**, and astubbs#354 is excluded three ways: the control arm
above, `ChaosKeyOrderIT` being untouched by that branch, and that scenario extending
`ChaosScenarioBase` rather than the scenario class the branch modified.

**The mechanism, from the run's own log.** The suite's user function dwells NON-interruptibly by
design (`ChaosScenarioBase#newInstance`, "sleep-until-deadline"; its javadoc explains that letting
PC's close interrupt it would cap every drain and stall at seconds and shrink the windows the probes
discriminate on). So when a chaos action closes an instance mid-dwell, the close cannot complete -
`Thread execution pool termination await timeout (PT10S)!` then `is user function swallowing
interrupted exception?`, 25 seconds before the probe fired. A member stuck in that close is not
answering the rebalance, and the group dwells past the 15s bound.

**The calibration gap is one line wide.** `AbstractRevokeUnderWorkScenario` calls
`disableRebalanceDwellViolation()` precisely because its disturbances legitimately block rebalances
(there, blocked rebalances self-resolve by eviction). `ChaosKeyOrderIT` runs the same disturbance
shapes - stop and restart against an uninterruptible heavy tail, under KEY ordering where dwells
chain - and leaves that violation ARMED. All three crossings measured 15.4-15.7s against a 15000ms
bound: **2.7-4.4% over**, which is the bound meeting the load, not a member that is wedged.

**Not a quarantine candidate.** Quarantine is for a known-red test whose fix is pending; here the
diagnosis is complete and the fix is a choice between disabling the dwell violation for this scenario
(matching W4) or widening its bound for the disturbance shape it actually runs. Recorded rather than
applied, because it is a scenario calibration change and astubbs#354 is a different change.
<!-- post-merge: checked-end -->

## 2026-08-25, CHAOS lane: the same commit-response timeout, from a second lane

<!-- post-merge: checked-begin -->
**The section above records the commit-response timeout arriving from `Integration Tests`. It also
arrived, the same day, from the chaos suite** - `ChaosRevokeUnderWorkIT.revokeUnderWorkStaysProtocolHonest`
on astubbs#351's head `ec2c54181`
([run 32801846128](https://github.com/astubbs/parallel-consumer/actions/runs/32801846128)). Two lanes,
two suites, one signature, neither of them the Class 2 timing proxy.

**The assertion that failed is a correctness one**, not `CLASS2_STALL`:

```
AssertionErrorWithFacts: no instance may end the run with an unclassified failure cause
expected to be empty
but was: [instance 42: java.lang.RuntimeException: Error from poll control thread:
  Timeout waiting for commit response PT10S to request ConsumerOffsetCommitter.CommitRequest(...)
  - the broker poll thread is the only producer of commit responses, and it has not died with an
  exception, so it is not answering: it is blocked or slower than the configured offsetCommitTimeout]
```

Alongside it, **1 `ZOMBIE_MEMBER/REBALANCE_BLOCKED`**: group `group-1-604121656` dwelling in
`PreparingRebalance` for 15s against the 15s bound - the arm the section below diagnoses as
calibration on `ChaosKeyOrderIT`, here on a different scenario.

**Seed `8584935079849032188`:**

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=8584935079849032188

**Why a second lane matters more than a second seed.** The discriminator section above closes the
`CLASS2_STALL` line precisely because that bound is a timing measurement that a loaded box crosses on
its own. This failure is not that bound and has no calibrated threshold behind it: a commit request
that times out after 10s, from a broker-poll thread the runtime has positively established is *alive
and not throwing*, is a thread that is blocked - and the error text reaches that conclusion by
elimination rather than inferring it from a watermark. The Integration-lane sighting landed on all
three preconditions for the AB-BA cycle; this one arrives from a suite that deliberately disturbs
rebalances, which is the same cycle's entry condition reached by a different route.

**Do NOT read either as identifying the open deadlock.** The alternative reading is identical in both
lanes - a poll thread merely slower than `offsetCommitTimeout`, the ambiguity
[`bug-offset-commit-timeout-does-two-jobs.md`](bug-offset-commit-timeout-does-two-jobs.md) owns - and
nothing in either capture separates them. The fifth sighting's rule applies: do not promote a
signature to a mechanism without the step that distinguishes them.

**The discriminator is the same one, and it is now wanted twice over**: replay with a thread dump of
the broker-poll thread at the moment the commit request times out. Parked on the `commitCommand`
monitor -> the family's original deadlock has a reproduction at last, from two independent lanes.
Running, or waiting on the broker -> it is a timeout to tune, and belongs with the timing-proxy
critique rather than here.

**Two `CLASS2_STALL` seeds from the same branch are recorded here and nowhere else, closed on
arrival.** Heads `d4b6923d0` and `bff3927b1` drew the drain arm at 154s and 153s against the 150s
bound - seeds `2445946824654755330` and `8791285396374198974`. They were written up as sightings
before the discriminator ran; they are not. Both are the arithmetic the section above explains, both
ran while `Performance (optional)` overlapped the chaos job on the shared box, and the bound they
crossed no longer gates. Kept as seeds only, so the pair is not rediscovered and re-argued.
<!-- post-merge: checked-end -->

## 2026-08-26: the pre-declared discriminator fired - the poll thread was parked on the `commitCommand` monitor

**The CHAOS-lane section above named the experiment that would decide this family's last open item -
*"replay with a thread dump of the broker-poll thread at the moment the commit request times out.
Parked on the `commitCommand` monitor -> the family's original deadlock has a reproduction at last"*.
That dump now exists, it was taken automatically at the timeout, and it says the poll thread was
blocked on a monitor.** `Chaos Pain Suite`,
`ChaosRevokeUnderWorkKeyOrderIT.perKeyOrderSurvivesRevokeUnderWork`, the same correctness SLO both
earlier lanes tripped - `no instance may end the run with an unclassified failure cause`
([run 32933049885](https://github.com/astubbs/parallel-consumer/actions/runs/32933049885)):

```
instance 65: RuntimeException: Error from poll control thread: Timeout waiting for commit response
PT10S ... POLL THREAD AT TIMEOUT: BLOCKED - the poll thread is waiting to acquire a monitor, so this
is contention or a lock-ordering defect, NOT a slow broker. Lock:
java.util.concurrent.atomic.AtomicBoolean@6b27bb8f, held by: pc-control-PC-65.
Top frames: [...commitOffsetsThatAreReady(AbstractParallelEoSStreamProcessor.java:1585),
             ...onPartitionsRevoked(AbstractParallelEoSStreamProcessor.java:548),
             ConsumerRebalanceListenerInvoker.invokePartitionsRevoked, ... ConsumerManager.poll]
```

**The frames resolve, at the head under test, to the exact AB-BA pair astubbs#29 replaces.** Frame
`:1585` is not merely inside `commitOffsetsThatAreReady` - it *is* that method's
`synchronized (commitCommand)` line, so the poll thread was stopped on the monitor acquisition
itself; frame `:548` is the `commitOffsetsThatAreReady()` call inside `onPartitionsRevoked`. And the
`AtomicBoolean` in `Lock:` has exactly one candidate: every `synchronized` block in
`AbstractParallelEoSStreamProcessor` takes `commitCommand`, and it is the only `AtomicBoolean` among
them. The holder, `pc-control-PC-65`, is the control thread, which reaches the same method from the
control loop and then blocks *inside* `committer.retrieveOffsetsAndCommit()` - the call the monitor
is held across. The only producer of commit responses is therefore the thread waiting for the
monitor the waiter holds, and `PT10S` is what breaks the cycle rather than anything resolving it.

**This is the step the two 2026-08-25 sections said they lacked.** Both closed with *"do NOT read
either as identifying the open deadlock"*, because a poll thread merely slower than
`offsetCommitTimeout` - the ambiguity
[`bug-offset-commit-timeout-does-two-jobs.md`](bug-offset-commit-timeout-does-two-jobs.md) owns -
produces an identical error text. That alternative requires the poll thread to be RUNNABLE, or
waiting on a socket. It was neither: it was `BLOCKED` on a named monitor with a named holder. For
this capture the ambiguity is resolved, and the reading is the one those sections pre-declared as
the deadlock side - which is why it counts for more than a third occurrence of a signature would.

**Seed `7728704565782280867`** - recorded before the log expires:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=7728704565782280867

<!-- post-merge: checked-begin -->
**Neither the branch nor the box is a suspect.** At the observed head `b75beb40d` the branch carried
a `.claude/hooks/` script, `bin/` shell and markdown only - no Java, no pom, no workflow - so it
could not reach the chaos engine, the same not-PR-introduced control the fourteenth, fifteenth and
sixteenth sightings each recorded. Nor was it the shared box: other PRs' chaos jobs in the same
window completed green, and the non-successes among them were `cancelled` by the `box-exclusive`
queue rather than failed, so this red is not master-state.
<!-- post-merge: checked-end -->

**What is wanted next is no longer a discriminator but a verification.** astubbs#29 replaces this
`synchronized` with `ReentrantLock.tryLock()`; until now it had a unit reproduction
(`RebalanceEoSDeadlockTest`) and no fleet-level one. Replaying this seed with that stack applied, and
reading whether the poll thread is still found `BLOCKED` on the same monitor, is the experiment that
would let astubbs#29 land on evidence rather than on argument. **Recorded as one observation, not a
rate:** the seed has not been replayed, and nothing here says how often the cycle closes.

## 2026-08-26, second capture: the same BLOCKED-on-monitor discriminator, on the COOPERATIVE arm

**The section above closed with "Recorded as one observation, not a rate". This is a second
independent capture of the same discriminator, and the value of it is that almost nothing is shared
with the first: different scenario, different assignor arm, different seed, different PR, hours
apart.** `Chaos Pain Suite`,
`ChaosRevokeUnderWorkCooperativeIT.revokeUnderWorkStaysProtocolHonestWithCooperativeAssignor`, the
same correctness SLO - `no instance may end the run with an unclassified failure cause`
([run 32948482633](https://github.com/astubbs/parallel-consumer/actions/runs/32948482633)):

```
instance 14: RuntimeException: Error from poll control thread: Timeout waiting for commit response
PT10S ... POLL THREAD AT TIMEOUT: BLOCKED - the poll thread is waiting to acquire a monitor, so this
is contention or a lock-ordering defect, NOT a slow broker. Lock:
java.util.concurrent.atomic.AtomicBoolean@215bf355, held by: pc-control-PC-14.
Top frames: [...commitOffsetsThatAreReady(AbstractParallelEoSStreamProcessor.java:1585),
             ...onPartitionsRevoked(AbstractParallelEoSStreamProcessor.java:548),
             ConsumerRebalanceListenerInvoker.invokePartitionsRevoked, ... ConsumerManager.poll]
```

**Frame for frame, monitor type for monitor type, holder for holder, it is the capture above.** Same
`:1585` (`commitOffsetsThatAreReady`, the `synchronized (commitCommand)` acquisition itself), same
`:548` (the call inside `onPartitionsRevoked`), same `AtomicBoolean` monitor, and the holder is again
the control thread for the same instance (`pc-control-PC-14` against `instance 14`). The reasoning
that identified the AB-BA pair there applies here unchanged and is not repeated.

**What this adds is that the cycle is not a property of the key-order arm.** The first capture was
`ChaosRevokeUnderWorkKeyOrderIT` under the eager assignor; this one is the **cooperative** assignor
in the plain revoke-under-work scenario. Both reach the deadlock through
`onPartitionsRevoked`, which is the part that matters: the revocation callback is the entry point
regardless of which assignor scheduled the revocation, so the pair is reachable across the arm split
rather than by one scenario's particular interleaving.

**Two captures is still not a rate, and this does not claim one.** Neither seed has been replayed.
What it does do is remove "seen once" as a reason to wait before running the verification the
section above asks for.

**The same-run control arm is unusually good here, so it is worth stating.** Every sibling scenario
in the same JVM, on the same runner, against the same broker image, passed: `ChaosChurnStormIT`,
`ChaosKeyOrderIT`, `ChaosRevokeUnderWorkIT`, `ChaosRevokeUnderWorkDrainIT`,
`ChaosRevokeUnderWorkKeyOrderIT`, and - the sharpest of them - `ChaosRevokeUnderWorkCooperativeDrainIT`,
which is the *same* cooperative assignor differing only in stop-mode. So the failure is not an
ambient property of that machine in that hour, and not the cooperative assignor by itself either.

Also present, and non-gating: one `CLASS2_STALL/LAG_STAGNATION` observation, which since 2026-08-25
does not fail the run. It is noted only so nobody re-reads this capture as a Class 2 sighting.
Peaks were `rebalanceDwell=15445ms`, `lagStagnation=154189ms`, and two
`ZOMBIE_MEMBER/REBALANCE_BLOCKED` violations fired on the 15s rebalance-dwell bound.

**Seed `2867310537409227917`** - recorded before the log expires:

    ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
      -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=2867310537409227917

<!-- post-merge: checked-begin -->
**Neither the PR nor the box is a suspect.** astubbs/parallel-consumer#364 changed
`.claude/hooks/` shell, `bin/` shell and markdown only - no Java, no pom, no workflow - so it cannot
reach the chaos engine, the same not-PR-introduced control the earlier sightings each recorded. Nor
was it the shared box: three other chaos jobs completed green inside the same half hour, and the
non-successes among the rest were `cancelled` by the `box-exclusive` queue rather than failed.
<!-- post-merge: checked-end -->

**One retrieval note, because it nearly produced a wrong reading.** The run-logs archive downloaded
short the first time - a `.zip` with no central directory - and `gh api` exited **0** both times, so
the only thing separating the truncated download from the complete one was `unzip -t`. This is the
`gh run view` truncation class arriving by a different route, so the remedy in
[`docs/ci.md`](../ci.md) needs its own completeness check: test the archive before believing a grep
that came back quiet.

<!-- post-merge: checked-begin -->
## 2026-08-26, third capture: the same arm again, and the first intermittency datum

**The section above ends "Two captures is still not a rate", and this does not make one either -
but it is the first repeat of the SAME scenario on the SAME arm, which the two before it were not.**
`Chaos Pain Suite`,
`ChaosRevokeUnderWorkCooperativeIT.revokeUnderWorkStaysProtocolHonestWithCooperativeAssignor`,
`chaos.seed=3649400609451361367`
([run 32965577251](https://github.com/astubbs/parallel-consumer/actions/runs/32965577251)), on
astubbs/parallel-consumer#345's head `fa49683c0`. Same `Timeout waiting for commit response PT10S`,
same ambient-probe verdict that the poll thread is BLOCKED on a monitor rather than waiting on a
broker, same `AtomicBoolean` `commitCommand` held by the instance's own `pc-control` thread, reached
through `onPartitionsRevoked`. The frame-for-frame identity argument in the capture above applies
unchanged and is not repeated here.

**What is new is the pair of adjacent heads.** The next head on that branch, `bc177988a` - a merge
of master carrying no main-code change of its own - ran the same suite and **passed**. So the two
outcomes sit one commit apart on one branch, which is the closest thing this file has to a direct
intermittency observation: previous captures were each on a different branch, so none of them could
separate "this tree provokes it" from "this run happened to hit it". This pair says the trigger is
the schedule, not the tree.

**It also says the sighting is easy to lose.** Nothing on astubbs/parallel-consumer#345 records the
failure any more - the branch is green, the red belongs to a head no longer at the tip, and the PR
merges without anyone meeting it. That is the argument for writing captures down here as they
happen rather than when somebody decides the rate matters.

**Not this PR's defect, and not fixed by it.** astubbs/parallel-consumer#345 changes
`ShardManager.removeWorkFromShardFor` and touches no locking and no
`AbstractParallelEoSStreamProcessor` line; the cycle is between the poll thread and `pc-control`
over `commitCommand`. Neither seed here nor in the two captures above has been replayed.

<!-- post-merge: checked-end -->

## Delete when

The `CLASS2_STALL` entries above are superseded by this section and kept only as the record of how a
timing proxy accumulated fourteen sightings.
<!-- post-merge: checked-begin -->
The sixteenth sighting above was written on
astubbs/parallel-consumer#353 before this section existed and merged in after it, so it is one more
of the same crossings rather than an exception to them; the counts here are left as they were
written rather than silently re-derived.
<!-- post-merge: checked-end --> This file may be retired once astubbs#29 lands and the
remaining open item - the original deadlock - has its own solutions write-up.

