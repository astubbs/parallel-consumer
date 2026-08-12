# Investigation handoff: Chaos Pain Suite W4 is reproducibly RED in CI

**Written:** 2026-08-01 (task: "merge master into PR astubbs#57", the chaos RED was found incidentally while
watching that PR's CI). **Updated the same day** after the investigation ran to completion.

> ## ✅ RESOLVED - root cause confirmed, reproduced locally, fixed
>
> **Root cause: `RebalanceInProgressException` was not handled on the commit path, and killed the
> broker-poll thread permanently.** See section 0 for the confirmed chain. The load-starvation
> hypothesis that this report originally led with (H0/H1 below) is **falsified** - the failure
> reproduces on an idle developer machine. Sections 4.1-9 are preserved as the investigation record;
> where they disagree with section 0, section 0 wins.
>
> **Fix:** branch `fix/commit-rebalance-in-progress-kills-poll-thread` (off `master` `192d32bc`) -
> one `catch` in **`ConsumerOffsetCommitter.commitDeferringOnRebalance()`**, plus
> `MockConsumerRebalanceInProgressTest`, a broker-free unit reproducer that fails in
> ~10s on unfixed code.
>
> **Not** in `ConsumerManager.commitSync()`, which is where it looks like it belongs and where the
> first attempt put it. That placement is *wrong* and section 0.5 explains why: it lets
> `retrieveOffsetsAndCommit()` reach `onOffsetCommitSuccess()` and mark offsets clean that never
> reached the broker. The layer is the whole point of the fix.
>
> **No open PR fixed this.** PR astubbs#80 is the nearest relative (same confluentinc#857 family, also a close/commit
> path bug) and was verified *present* in a CI run that still went red - see section 0.4.

**Ledger entry:** `docs/inflight.md`. This report is the long form of that entry.

> **Pointer repair (the ledger moved, the claims did not).** The single file `docs/inflight.md` was
> split into the directory [`docs/inflight/`](../inflight/) on 2026-08-04 and deleted in `0de96fc`
> (`git log --diff-filter=D -- docs/inflight.md`). Read it as it stood while this report was written
> with `git show 0de96fc^:docs/inflight.md`. The live successors to this entry are
> [`docs/inflight/test-chaos-phase2.md`](../inflight/test-chaos-phase2.md) (Class 2 hunt status) and
> [`docs/inflight/bug-857-family.md`](../inflight/bug-857-family.md) (the confluentinc#857 family,
> including the RED-side occurrence this report predicted). Every `docs/inflight.md` named below is
> that pre-split single file, as it was on the dates given.

---

## 0. CONFIRMED root cause (added after the investigation completed)

### 0.1 The chain

An unhandled `RebalanceInProgressException` on the commit path permanently kills the broker-poll
thread, and everything else follows from that:

1. The control thread wants to commit, guarded by `!isRebalanceInProgress.get()`
   (`shouldTryCommitNow` in `AbstractParallelEoSStreamProcessor#maybeAcquireCommitLock`) - a best-effort
   pre-check that races.
2. Under `PERIODIC_CONSUMER_SYNC` the control thread is not the commit owner (the broker-poll thread
   claims ownership in `BrokerPollSystem.java`'s `isResponsibleForCommits()`), so it calls
   `ConsumerOffsetCommitter.commitAndWait()`, enqueues a request, and blocks.
3. The broker-poll thread services it: `BrokerPollSystem.java`'s `maybeDoCommit()` →
   `retrieveOffsetsAndCommit()` → `ConsumerManager.commitSync()` → `consumer.commitSync()`.
4. A rebalance is underway, so Kafka throws `RebalanceInProgressException`. `commitSync()`'s
   exception ladder (`ConsumerManager#commitSync` - the `CommitFailedException` branch is no longer at
   HEAD, see
   `git log -S'CommitFailedException' -- parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ConsumerManager.java`)
   handles `CommitFailedException`, `TimeoutException` and `SaslAuthenticationException` - **but not
   this one**, its closest sibling.
5. It escapes into `BrokerPollSystem.controlLoop()`'s `catch (Exception e)`, which logs
   `"Unknown error"` and **rethrows** - the broker-poll thread dies for good.
6. That thread is the only producer of commit responses. The blocked control thread waits the full
   `offsetCommitTimeout` and throws `Timeout waiting for commit response`.
7. The control thread dies, PC self-closes with a failure cause, and the close-path commit fails too
   (the poll thread is gone). The chaos sweep flags it in `ChaosScenarioBase.java`, at the
   `"no instance may end the run with an unclassified failure cause"` assertion.

One retriable protocol condition therefore kills the consumer. In production this is the
"confluentinc#857 locks forever until manual restart" symptom.

### 0.2 Verified locally, which falsifies the load hypothesis

Replaying seed `8254214163208094917` at `192d32bc` on a developer machine (12 cores, load ~5.5, not a
starved CI runner) reproduced the failure exactly: `ChaosRevokeUnderWorkCooperativeIT` failed in
132.5s with the same assertion, and the log shows the sequence above with the 10-second gap between
the poll thread's death (`27:00.187`) and the control thread's timeout (`27:10.190`).

`MockConsumerRebalanceInProgressTest` then reduced it to a **broker-free unit test that
fails in ~10s** - a `MockConsumer` whose `commitSync` throws `RebalanceInProgressException`
reproduces the whole chain, including the misleading timeout.

### 0.3 Why it correlates with astubbs#87 (the correlation was real, the inference was wrong)

Section 4.1's correlation stands as data, but the causal reading was wrong. astubbs#87 did not introduce the
bug - it *exposed* it. Cooperative rebalancing keeps members processing and **committing during**
rebalances by design, which is precisely the window where `commitSync` meets an in-progress rebalance.
Eager rebalancing stops the world instead, so the window is much narrower - which is why the coop arm
fails in every run containing it while the eager arm fails intermittently. The bug itself is
long-standing and inherited from upstream: the exception ladder arrived with `29795bf5` (upstream
confluentinc#819), and no fork commit touched it.

### 0.4 No open PR fixes it (checked)

PR astubbs#80 is the nearest relative - same confluentinc#857 family, also a close/commit path bug - but it does not
touch this exception. Verified two ways: `RebalanceInProgressException` appears nowhere in main code
on either branch, and the failing CI run `30607535421` was PR astubbs#80's own branch head (`c28c0e09`) with
its fix demonstrably present (`shutdownRequested` fully removed there; still present 8× on master) -
and it still went red on **both** arms.

### 0.5 A trap worth knowing about (first fix attempt was wrong)

The obvious fix - catching `RebalanceInProgressException` inside `ConsumerManager.commitSync()`
alongside its siblings - stops the crash but is **incorrect**, and the unit test caught it.
`AbstractOffsetCommitter.retrieveOffsetsAndCommit()` calls `onOffsetCommitSuccess()` unconditionally
once `commitOffsets()` returns normally, so swallowing the exception down there makes PC record a
commit that never reached the broker: the partitions go clean, nothing is retried, and the test's
commit counter stalls at 2 instead of climbing.

The fix therefore lives one layer up, in `ConsumerOffsetCommitter`, where the exception can abort
`retrieveOffsetsAndCommit()` **before** the success marking (so offsets stay dirty and really are
re-committed next cycle) while still (a) not escaping to kill the poll thread and (b) sending the
commit response anyway, so the waiter is released immediately instead of hanging for
`offsetCommitTimeout`.

> **Note for whoever touches this next:** the pre-existing `CommitFailedException` handler in
> `ConsumerManager#commitSync` (no longer at HEAD:
> `git log -S'CommitFailedException' -- parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ConsumerManager.java`)
> has the *same* latent flaw - it breaks out of the commit and lets
> `onOffsetCommitSuccess()` mark the offsets clean, despite its comment saying the poller will "seek
> commit later". Worth a follow-up; not changed here to keep this fix reviewable.

---

## 1. My vantage point - read this first

Everything below was observed from this exact repo state. If your `master` has moved, re-check the
timeline claims before trusting them.

| What | Value |
|---|---|
| Repo | `/Users/astubbs/github/parallel-consumer` (fork `astubbs/parallel-consumer`) |
| Branch | `master`, clean except untracked dirs + my `docs/inflight.md` edit |
| `master` HEAD | `192d32bc555bae99e2fc7b8e0bb4da2f1e4a8cc5` - "feat(chaos): cooperative-sticky variant of the W4 revoke-under-work scenario (astubbs#87)" |
| `origin/master` | identical (`192d32bc`) - master was not ahead/behind |
| Worktree | `.claude/worktrees/dev-cc` on `fix/859-metrics-leak-plus-cherrypicks` (PR astubbs#57), at `4cdb5265` |

Relevant master history (newest first):

```
192d32bc  astubbs#87  chaos: cooperative-sticky W4 variant   <- master HEAD; also REFACTORED the eager W4 test
8cc543a3  astubbs#86  ambient flight-recorder probe on every broker IT
df065823  astubbs#85  chaos: W4 revoke-under-work (Phase 2)  <- introduced W4 (eager)
9ccf2ec4  astubbs#83  Chaos Pain Suite (Phase 1, W1)
```

**What I changed:** only `docs/inflight.md` on `master` (uncommitted), plus PR astubbs#57's own branch (a
master merge + copyright headers - unrelated to chaos). **I changed no chaos, test, or main code.**
**What I did NOT do:** I never ran the chaos suite locally. Every observation below is from CI logs
via `gh`. That gap is the single most important thing for you to close first.

### Where to start: the A/B pair

**Start at `192d32bc` (master HEAD, = astubbs#87). Diff it against `8cc543a3` (= astubbs#86).** That two-commit
window is the entire delta between the last known-GREEN chaos run and the first RED one, and it
isolates the change that correlates perfectly with the failure (section 4.1).

| role | sha | what it is |
|---|---|---|
| **first RED / start here** | `192d32bc` | astubbs#87, master HEAD - coop variant + eager-driver extraction |
| **last known GREEN** | `8cc543a3` | astubbs#86 - the ambient-probe merge; eager W4 passed here in 161.8s |
| (exact green tree in CI) | `40d3d9fbd` | branch head of `feats/chaos-ambient-probe` for the 05:02 green run |
| W4 origin (eager introduced) | `df065823` | astubbs#85 - useful as a second baseline, but *not* where the red starts |
| chaos Phase 1 (W1 only) | `9ccf2ec4` | astubbs#83 - predates all W4 code |

A note on your hunch: you are right that W4 (`df065823`, astubbs#85) landed directly after the Phase 1 chaos
suite (`9ccf2ec4`, astubbs#83) - the order on master is astubbs#83 → astubbs#85 → astubbs#86 → astubbs#87. But `df065823` is **not** the
right bisect origin, because W4 *passed* after it landed: the 05:02 green run contained astubbs#85 and ran
the eager W4 scenario successfully. The red begins one commit later, at astubbs#87.

---

## 2. The finding in one paragraph

The Chaos Pain Suite job on the `highcpu` self-hosted lane has failed on **every** run since
2026-07-31 ~05:24, across five unrelated branches, while passing as recently as 05:02 that same
morning. Both W4 arms fail - eager *and* cooperative - so this is **not** the cooperative variant's
problem. The dominant signature is fleet instances dying on
`InternalRuntimeException: Timeout waiting for commit response`, which the scenario cannot classify
and therefore reports as an SLO breach.

The failure correlates perfectly with commit `192d32bc` (astubbs#87): 3/3 runs containing it fail this way,
0/3 without it do, and the *same eager test* passed before it and fails after (section 4.1). My
leading explanation is still **not a PC bug**: astubbs#87 added a second full-fleet scenario to the same CI
job, roughly doubling its footprint on a self-hosted box that the lane already oversubscribes, and the
chaos suite's timing-based SLOs are the canary. That is a hypothesis with a clear falsification test
(section 7, step 1) - please run it before touching any code.

---

## 3. Which test exposes it

Primary, and the one to start with:

```
parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/
  chaostests/ChaosRevokeUnderWorkCooperativeIT.java   ::revokeUnderWorkStaysProtocolHonestWithCooperativeAssignor
  chaostests/ChaosRevokeUnderWorkIT.java              (eager arm - fails too)
  chaostests/AbstractRevokeUnderWorkScenario.java     (shared driver; logs "=== CHAOS", then asserts)
  chaostests/ChaosScenarioBase.java                   (assertScenarioSlos - the assertion that fails)
```

Note both concrete ITs and the abstract driver were **last touched by `192d32bc` (astubbs#87)**, which
extracted the eager test into `AbstractRevokeUnderWorkScenario` and added the cooperative subclass.
So astubbs#87 is the last commit to touch every file in the failing path, and the failure correlates with it
exactly (section 4.1) - which is why it is the starting SHA. Section 6 explains why I still think the
mechanism is load rather than a broken refactor.

The assertion that fires, in `ChaosScenarioBase.java`:

```java
assertWithMessage("no instance may end the run with an unclassified failure cause (replay: %s)", replayCmd)
        .that(unexpectedFailures).isEmpty();
```

It sweeps `conductor.getFleet()`, reads each `ParallelConsumer.getFailureCause()`, and flags anything
`ManagedPCInstance.isExpectedCloseException()` does not recognise.

---

## 4. Evidence: five red runs, five different branches

All on workflow `highcpu` (`.github/workflows/pr-highcpu-fast-feedback.yml`), job "Chaos Pain Suite".

| time (UTC 2026-07-31) | run id | branch | result |
|---|---|---|---|
| 04:13 | 30603994869-era | `feats/chaos-pain-suite` | **green** (W1 only, no W4) |
| 04:22 | 30603994869 | `feats/chaos-ambient-probe` | **green** |
| 04:39 | 30604723974 | `feats/chaos-w4-revoke-under-work` | red - **mode B** (942s) |
| 05:02 | 30605690556 | `feats/chaos-ambient-probe` | **green** - ran `ChaosRevokeUnderWorkIT`, passed in **161.8s** |
| 05:24 | 30606726847 | `feats/chaos-w4-cooperative` | red - mode B (1042s) |
| 05:41 | 30607480136 | `fix/brokerpoller-backpressure-vacuous-await` | red - **mode A**, coop, seed `5666019246240209747` |
| 05:42 | 30607535421 | `fix/flaky-partitionstate-committedoffset-it` | red - mode A, **both** eager + coop |
| 06:02 | 30608468815 | `fix/859-metrics-leak-plus-cherrypicks` (PR astubbs#57) | red - mode A, coop, seed `8254214163208094917` |

**The 05:02 green run is the load-bearing data point:** it executed the eager W4 scenario to
completion in 161.8s with zero violations, on the same box, from a branch that already contained astubbs#85's
W4. So W4 is not inherently broken. Something changed between 05:02 and 05:24 - and what visibly
changed is the *arrival rate of concurrent runs* (four branches pushing within 40 minutes, each
spawning six concurrent jobs including two PIT sweeps).

### Two distinct red modes

**Mode A - the interesting one** (05:41, 05:42, 06:02). Test runs ~120-180s, then
`Failures: 1` on the unclassified-failure-cause assertion. Multiple fleet instances die with:

```
InternalRuntimeException: Timeout waiting for commit response PT30S to request
  ConsumerOffsetCommitter.CommitRequest(id=..., requestedAtMs=...)
```
often immediately after `org.apache.kafka.common.errors.RebalanceInProgressException`. In run
30607535421 I saw instances 7 and 10 die; in 30606726847, PC-0, PC-2 and PC-4. The exception kills the
`pc-control` thread, PC self-closes, `ManagedPCInstance` reports "died from unexpected error".

**Mode B - plainly infrastructural** (04:39, 05:24). `Errors: 1`, 942-1042s elapsed, and the cause is
that Testcontainers could not start a broker at all:

```
ContainerLaunchException: Timed out waiting for log output matching '.*\[KafkaServer id=\d+\] started.*'
  for image confluentinc/cp-kafka:7.9.0
```

Mode B is unambiguous proof the box was too starved to boot a Kafka container within the timeout.

### 4.1 The decisive correlation: mode A appears only where astubbs#87 is present

I checked, for each run's head SHA, whether `192d32bc` (astubbs#87) is an ancestor
(`git merge-base --is-ancestor`):

| run | branch | contains astubbs#87? | outcome |
|---|---|---|---|
| 30604723974 | `feats/chaos-w4-revoke-under-work` (`7273b0bf0`) | no | mode B (infra) |
| 30606726847 | `feats/chaos-w4-cooperative` (`55d95f524`) | no | mode B (infra) |
| 30605690556 | `feats/chaos-ambient-probe` (`40d3d9fbd`) | no | **GREEN** (eager W4, 161.8s) |
| 30607480136 | `fix/brokerpoller-backpressure-vacuous-await` (`3d8aeeb97`) | **YES** | **mode A** |
| 30607535421 | `fix/flaky-partitionstate-committedoffset-it` (`c28c0e093`) | **YES** | **mode A** (both arms) |
| 30608468815 | `fix/859-metrics-leak-plus-cherrypicks` (`4cdb52651`) | **YES** | **mode A** |

**3/3 runs containing astubbs#87 show mode A; 0/3 runs without it do.** astubbs#87 merged at 05:38 UTC and every
mode-A failure is timestamped after it. Even more directly: **the same eager test
(`ChaosRevokeUnderWorkIT`) passed pre-astubbs#87 and fails post-astubbs#87** - a clean same-test A/B across that
commit. This is why the starting SHA is astubbs#87, not astubbs#85.

Caveat before you over-read it: the two pre-astubbs#87 runs failed mode B, so they never got far enough to
tell us what they *would* have done. The clean comparison is green-`40d3d9fbd` vs red-`4cdb52651`.

---

## 5. Why the failure is "unclassified" (mechanism, not cause)

`ManagedPCInstance.isExpectedCloseException()` (`ManagedPCInstance.java`) is a **whitelist** that
walks the cause chain for `InterruptedException`, `WakeupException`, `DisconnectException`,
`ClosedChannelException`, `java.util.concurrent.TimeoutException`.

The thrown object is `io.confluent.parallelconsumer.internal.InternalRuntimeException`, constructed in
`ConsumerOffsetCommitter.commitAndWait()` via `InternalRuntimeException.msg("Timeout waiting for
commit response ...")` - **a bare exception with no cause chain**. Nothing in the whitelist can ever
match it, so a commit-response timeout is by
construction "unexpected". That is why the test reports an SLO breach rather than a tolerated close.

This is mechanism, not root cause: the honest question is whether an instance dying from a commit
timeout *should* be tolerated. I lean no - it is real PC behaviour worth surfacing - so please do not
"fix" this by adding `InternalRuntimeException` to the whitelist. That would blind the tripwire.

---

## 6. Where I think the issue is coming from - ranked

> **Ranking note.** I first ranked lane-starvation (H1) alone at the top. The section 4.1 correlation
> changed that: the failure tracks astubbs#87 too precisely to be pure environment. My current best
> explanation is **H0 below, which unifies H1 and H3** - astubbs#87 did not introduce a *bug* so much as it
> doubled the chaos job's own footprint on an already-oversubscribed box. H1 and H3 are kept as the
> pure forms, because the falsification test in section 7 distinguishes all three.

### H0 (leading, unifying): astubbs#87 doubled the chaos job's load on a box that was already at its limit

astubbs#87 added a second full-fleet scenario to the same job. Before it, the chaos job ran one
revoke-under-work scenario (green run: `Tests run: 1`, 161.8s). After it, the job runs **two** -
eager *and* cooperative - each standing up its own broker + PC fleet (post-astubbs#87 runs report
`Tests run: 2`, with the coop arm alone taking 123-149s). That roughly doubles the chaos job's
runtime and its concurrent resource draw.

Now combine with H1's lane structure: that doubled job overlaps two PIT mutation sweeps *and* other
branches' jobs on one machine. The result is a job that used to fit inside the box's spare capacity
and now does not. Both the perfect astubbs#87 correlation and the starvation evidence are satisfied by this,
without requiring the refactor to be incorrect - and I read the eager-arm diff in astubbs#87 as a faithful
extraction (imports moved, javadoc rewritten, driver hoisted to `AbstractRevokeUnderWorkScenario`),
which is why I do not lead with "the refactor broke it".

If H0 holds, the fix is scheduling/isolation, not test logic - and note that it will keep getting
worse as W-scenarios are added, since each new arm lands in the same job.

### H1 (pure form): the highcpu lane starves its own chaos SLOs

`.github/workflows/pr-highcpu-fast-feedback.yml` sets concurrency **per suite and per ref**:

```yaml
concurrency:
  group: highcpu-${{ matrix.suite-name }}-${{ github.head_ref || github.ref }}
```

Consequences: (a) all six suites - Unit, Integration, Performance, **Mutation (PIT, scoped)**,
**Mutation (PIT, full)**, Chaos - run *concurrently* on one box; (b) because the group includes the
ref, **different branches do not supersede each other**, so N concurrent PRs multiply the load by N.
`timeout-minutes: 60`, so a wedged PIT sweep occupies cores for a long time.

Corroboration, all from the same runs:
- In **every** red run, the CPU-hungry mutation job(s) also failed.
- In PR astubbs#57's run, three jobs (Unit, both Mutations) died with *"The self-hosted runner lost
  communication with the server"* - the box did not merely slow down, it fell over.
- Mode B shows a Kafka container failing to boot inside its wait window.
- The workflow already concedes timing noise for Performance ("timing is noisy under concurrency -
  that's fine") - but chaos is deliberately **not** `continue-on-error`, so the same noise renders as
  a red finding.

Under this hypothesis the chaos suite is behaving correctly *as a detector* and the environment is out
of spec. The commit-response wait is `options.offsetCommitTimeout` = `PT10S` in these runs; a 10s
budget on a box that cannot boot a broker in 15 minutes will trip regardless of PC's correctness.

### H2: a genuine commit-response stall under revoke-heavy load

This is exactly the confluentinc#857-family behaviour the Class 2 probe was built to catch, and `docs/inflight.md`
records the probe as "a calibrated TRIPWIRE - RED-side awaiting a real-world/CI occurrence" (`git
show 0de96fc^:docs/inflight.md`, grep `calibrated TRIPWIRE`; the successor carries the same stance -
grep `calibrated tripwire` in [`docs/inflight/test-chaos-phase2.md`](../inflight/test-chaos-phase2.md)). If the
load hypotheses are falsified, H2 is the live explanation and this is PC's first RED-side hit - a
significantly more valuable outcome than a CI-scheduling fix. Treat it as a PC bug then, not a test
bug. Note the astubbs#87 correlation does not rule H2 out: if the second scenario simply makes the fleet
spend more wall-clock under revoke pressure, a pre-existing PC stall would surface more often without
astubbs#87 having caused it.

### H3 (pure form): a test-side behaviour change in astubbs#87's refactor

astubbs#87 rewrote the eager test into `AbstractRevokeUnderWorkScenario`, so a behaviour change there would
hit both arms - which matches the symptom, and the section 4.1 correlation is consistent with it. What
argues against it being a straightforward bug: I read the eager-arm diff and it looks like a faithful
extraction (imports moved, javadoc rewritten, driver hoisted; the seed/replay block hoisted to
`ChaosScenarioBase`). I have **not** diffed the extracted driver line-by-line against astubbs#85's original
inline version, though - if step 1 says the failure reproduces on an idle box at `192d32bc` but not at
`8cc543a3`, that diff is the very next thing to read, paying particular attention to any timing,
fleet-size, `maxConcurrency`, commit-mode or consumer-property default that changed in the move.

---

## 7. Original next-steps plan (all now executed - see section 0)

> Kept for the record. Step 1 was run and came back **red on an idle box**, which sent the
> investigation down the H2 branch and found the root cause in section 0. Steps 2 and 3 are therefore
> superseded; the "decide the classification question" item in step 4 is still open and now lives in
> the follow-ups list in `docs/inflight.md`.
>
> **Pointer repair.** That list is the section `Open follow-ups from the W4 revoke-under-work
> investigation` - `git show 0de96fc^:docs/inflight.md` and grep the heading. It did **not** carry
> into `docs/inflight/` when the file was split, so the history above is the record of it; the
> nearest live note is [`docs/inflight/test-chaos-phase2.md`](../inflight/test-chaos-phase2.md).
> Whether the classification item is still open today is not a question this repair answers.

1. **Replay a captured seed on an idle machine, at `192d32bc`.** This one step separates every
   hypothesis. Both of these failed in CI:
   ```
   git checkout 192d32bc
   ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true -Dincluded.groups=chaos -Dchaos.seed=8254214163208094917
   ./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true -Dincluded.groups=chaos -Dchaos.seed=5666019246240209747
   ```
   - **Green on an idle box** ⇒ load-driven (H0/H1). Confirm by re-running under synthetic CPU load.
   - **Red on an idle box** ⇒ go to step 1b; you have a deterministic reproducer, which is the best
     possible outcome here.
1b. **If red at `192d32bc`, run the same eager scenario at `8cc543a3`** (last known green). Red there
   too ⇒ H2, a real PC stall that astubbs#87 merely surfaced. Green there ⇒ H3, and the astubbs#87 driver diff is
   your target (section 6, H3). Note the coop seed cannot replay at `8cc543a3` - that test does not
   exist yet - so use the eager arm for this comparison.
2. **If load-driven (H0/H1):** fix the lane, not the test. Options: put chaos and the mutation suites
   in a *shared* concurrency group so they cannot co-run; drop the ref from the group so runs
   supersede across branches; move mutation off the highcpu box; or give chaos its own runner label.
   Under H0 specifically, also consider splitting the arms into separate matrix entries so one job
   does not carry every W-scenario as the roster grows. **Do not widen the chaos SLOs to quiet
   CI** - that de-calibrates the tripwire, which is the entire point of the suite (the ledger already
   notes W4's legit stagnation peaks sit only ~1.25x under the 150s Class 2 bound, so there is little
   headroom to give away).
3. **If H2:** chase the commit path. `ConsumerOffsetCommitter.commitAndWait()` waits on
   `commitResponseQueue.poll(commitTimeout)`; the response is produced by the broker-poll/control
   path. Interaction with `RebalanceInProgressException` immediately prior is the obvious thread to
   pull.
4. **Either way, decide the classification question** (section 5) deliberately: prefer a dedicated
   "commit timeout" classification with its own explicit SLO over silently whitelisting
   `InternalRuntimeException`.

---

## 8. Unrelated real bug I found while reading the throw site

Small, independent of everything above, worth its own tiny PR + unit test:

`ConsumerOffsetCommitter.commitAndWait()` waits on `commitTimeout`
(= `options.getOffsetCommitTimeout()`, `PT10S` in these runs) but the exception message interpolates
`AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT` (`PT30S`):

```java
Duration timeout = AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT;
CommitResponse take = commitResponseQueue.poll(commitTimeout.toMillis(), MILLISECONDS);
if (take == null)
    throw InternalRuntimeException.msg("Timeout waiting for commit response {} to request {}", timeout, commitRequest);
```

So every such error misreports how long it actually waited by 3x and ignores the user's configured
timeout. Fix: interpolate `commitTimeout`. **Caveat for you:** this means the "PT30S" in every log
line quoted in this report actually elapsed **10s**, not 30s. Keep that in mind when reasoning about
timing.

---

## 9. Caveats on this report

- I never ran the suite locally; all evidence is CI logs read through `gh`. Section 4's timeline is
  solid (job ids and timestamps are checkable), section 6's ranking is inference.
- I did not check whether the highcpu box has other tenants, nor its core count / current load, nor
  whether anything changed on the machine itself around 05:24. If you have host access, `dmesg`, OOM
  logs, and the runner's own diagnostic logs around 06:39-07:25 UTC would settle H1 immediately.
- Job logs expire. If you need the raw logs, pull them now:
  `gh api repos/astubbs/parallel-consumer/actions/jobs/<job-id>/logs`. Chaos job ids: `91085772286`
  (PR astubbs#57), `91074548322` (04:39 mode B), `91077334343` (05:02 green).
- Chaos is non-gating, so none of this blocks PR astubbs#57 or any other PR today. It does mean the suite is
  currently crying wolf on every PR, which will erode trust in it fast - that is the real cost of
  leaving it unresolved.
