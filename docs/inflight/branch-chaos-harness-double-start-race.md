---
artifact_contract: "ce-handoff/v1"
created_at: "2026-08-12T14:16:08Z"
title: "Chaos harness double-start race: diagnosed and fixed, never compiled"
summary: "The unexplained ChaosRevokeUnderWorkCooperativeIT failure was traced to a ManagedPCInstance double-submission in the test harness and fixed on a pushed branch, but the code has never been compiled or run because the authoring machine has no JDK."
keywords: ["chaos", "ChaosRevokeUnderWorkCooperativeIT", "ManagedPCInstance", "ChaosConductor", "ConcurrentModificationException", "ZOMBIE_MEMBER", "double-start", "unverified", "no-jdk"]
cwd: "/home/astubbs/git/parallel-consumer/.claude/worktrees/fix+broker-poll-swallowed-cause"
resume_focus: "Get the pushed fix compiled and the new ManagedPCInstanceLifecycleIT run - a PR is the only CI route, since maven.yml does not fire on feature-branch pushes."
repository: "astubbs/parallel-consumer"
repo_root_sha: "713c7468d5ecda55a2190d38e698cda017e91259"
branch: "fix/chaos-harness-double-start-race"
head: "aba47280d94142685b773e0d22d7457511aab624"
worktree_path: "/home/astubbs/git/parallel-consumer/.claude/worktrees/fix+broker-poll-swallowed-cause"
---

# `fix/chaos-harness-double-start-race` - diagnosed and fixed, never compiled

Session handoff, committed here so it travels with the branch rather than sitting in a machine-local
temp directory. Delete this file in the PR that lands the branch (per the rule above in
[`AGENTS.md`](AGENTS.md) - work a PR resolves is tracked by that PR).

## Objective and where it landed

This session resumed an earlier handoff whose goal was to decide whether one unexplained chaos
failure was a real main-code defect or a harness artifact. **That question is now answered with
evidence: it is a test-harness lifecycle defect.** The fix is written and pushed. The open item is
purely verification — the code has never been compiled.

The user's latest stated intent: *"just for now i will migrate this work to another machine soon."*
The pushed branch is the transport for that; see **What travels and what does not**.

## The answer (settled, with evidence)

`ChaosConductor.doStopNoDrain` marks an instance `STOPPED` the moment `stopAsync()` returns — the
close runs on a background thread — and `doRestart` then calls `ManagedPCInstance.start()`, which
submitted unconditionally. `run()` begins by waiting up to 10s for the previous PC to close and then
proceeds regardless. Instance 2 drew `STOP_NO_DRAIN → RESTART → STOP_NO_DRAIN → RESTART` in ~1.8s, so
the first restart's `run()` was still parked in that wait loop when the second was submitted.

Proof in the CI log — the same instance running twice, same millisecond, two pool workers:

```
24:03.414 Runner-2 [ForkJoinPool-1-worker-7] Running consumer instance 2
24:03.414 Runner-2 [ForkJoinPool-1-worker-9] Running consumer instance 2
```

One defect produces every symptom in the original failure:

- The two `run()`s race on the `parallelConsumer` field, so one PC is **orphaned** — a group member
  nobody ever closes → `ZOMBIE_MEMBER/REBALANCE_BLOCKED` and the frozen partitions.
- `stopAsync()` ran twice against one PC (`pc-close-2` logged "Close complete." twice) → two threads
  inside one `KafkaConsumer` while its poll thread was still polling →
  `ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access` at
  `ConsumerManager.updateCache()` (`consumer.groupMetadata()`).

The two PC objects had **different** consumers (`@2e171dcd`, `@67280a5a`, from the options line's
identity hashes), which is how the "restart reuses a consumer" idea was ruled out.

**Main code is not implicated.** `AbstractParallelEoSStreamProcessor` already refuses a second start
per object — grep `Invalid state - you cannot call the poll` (`state != State.UNUSED` →
`IllegalStateException`). The harness simply built two objects.

### Two corrections to the previous handoff

Both were load-bearing in that document and are wrong; do not re-derive from them.

1. **The root cause was never swallowed.** `BrokerPollSystem.controlLoop` logs it — grep
   `"Unknown error"` — and the rethrown wrapper carries the full `Caused by` chain, which
   `AbstractParallelEoSStreamProcessor` also logs (grep `Error from poll control thread`, the
   `log.error(..., e)` call). Only the *assertion message* drops it, because
   `ChaosScenarioBase.assertScenarioSlos` stringifies the exception (grep
   `no instance may end the run with an unclassified failure cause`, ~10 lines above it).
2. **`probe violations=[]` vs the two-violation autopsy is not a bug.** They are two different
   `ProgressProbe` instances with different windows: the scenario probe is stopped inside
   `settleRun` *before* `settleFleet` (grep `Run summary: consumed=`), while the ambient probe from
   `AmbientProbeExtension` (grep `ambientObserver`) keeps sampling through teardown — which is when
   the dwell occurred.

## Current state

**Complete:** diagnosis; implementation; commit `aba47280` on `fix/chaos-harness-double-start-race`,
pushed to `origin` (remote sha matches local). Working tree clean.

**Not done — verification. Nothing has been compiled or executed, at all.** The authoring machine has
no `java`, no `javac`, no `docker`, no `podman` (all checked). The commit message states this in its
final paragraph. `ce-code-review` was also not run, so this is not a shipped change.

**No CI has run.** `.github/workflows/maven.yml` triggers on `push` to `master` and on
`pull_request` only, so the feature-branch push matched nothing; `gh run list --branch
fix/chaos-harness-double-start-race` was empty. A PR is the only route to CI. The chaos suite itself
(`.github/workflows/chaos-pain.yml`) is `workflow_dispatch`-only and would not run on a PR anyway —
but the new test is deliberately untagged, so it runs in the default integration build a PR triggers.

**Pending decision, never answered:** the user was asked whether to open a draft PR to get CI to
compile and run this, and invoked the handoff instead. That question is still open.

## What changed (all test-harness; zero main-code changes)

In `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/`:

- `utils/ManagedPCInstance.java` — grep `startInFlight` for the single-flight guard `start()` claims
  and `run()`'s `finally` releases; `start()` now returns `boolean` (false = refused). grep
  `stopRequested` for the abort a queued `run()` performs so it cannot orphan a PC. `closePending` is
  now an `AtomicBoolean` CAS'd in `stopAsync()` so one PC never gets two closers, with an explicit
  `isClosePending()` kept because `ChaosScenarioBase` polls it (grep `isClosePending`). Also a
  package-private `setParallelConsumerForTest` hook added for the no-broker test.
- `chaostests/ChaosConductor.java` — `doRestart` honours a refused start and leaves the instance
  `STOPPED` and redrawable instead of recording a disturbance that never happened; class javadoc
  states the new contract.
- `utils/ManagedPCInstanceLifecycleIT.java` (new) — three cases: second start refused while the first
  is queued; a stop while queued aborts the start and releases the guard; a second `stopAsync` starts
  no second closer. No broker, untagged, modelled on `chaostests/ChaosConductorPlanIT.java`.

### Where this unverified code is most likely to break

Reading is not compiling. One compile error was already caught and fixed on re-read (a missing
`AtomicBoolean` import), which is evidence the read pass is fallible. Unproven specifically:

- `mock(ParallelEoSStreamProcessor.class)` in the third test — the class is public and non-final and
  `close()` is not final, and mockito 5.23 is on the classpath (`pom.xml`, grep `mockito.version`),
  but the mock was never actually constructed.
- The `RecordingExecutor extends AbstractExecutorService` stub, and the assumption that
  `submit(Runnable)` routes through `execute` so `runAll()` runs the task body.
- Lombok interplay: class-level `@Getter` now sees an `AtomicBoolean closePending` alongside the
  hand-written `isClosePending()`.
- Timing in the third test (a 200ms sleep to let a wrongly-spawned second closer appear) is the kind
  of thing that behaves differently on a loaded CI box.

## Constraints that still bind

- `AGENTS.md` (Testing): do not loosen a test to make a stress failure go away. Fixing the harness's
  lifecycle bug is not loosening; raising the 15s `REBALANCE_DWELL_BOUND` would be, and remains the
  tempting wrong move since the failure hit that bound exactly.
- The `highcpu` chaos lane is non-gating, so nothing forces this forward and nothing will report if it
  recurs.
- A green chaos rerun proves nothing: each run draws a new seed unless `-Dchaos.seed` is passed.
- Repo convention: record a known defect as a note under `docs/inflight/` (see
  `docs/inflight/AGENTS.md`), not as a comment in a test. Nothing was filed this session — arguably
  nothing needs to be, now that the defect is fixed rather than merely known.

## Reproducing the evidence (the log route that worked)

`gh run view --job 94054545979 --log` and `gh api .../jobs/94054545979/logs` both failed — the former
with `stream error: stream ID 1; CANCEL`, the latter hung past 120s. What worked was the whole
attempt-1 archive (4.7 MB zip → a 110 MB `1_Chaos Pain Suite.txt`):

```bash
curl -sL -H "Authorization: Bearer $(gh auth token)" \
  "https://api.github.com/repos/astubbs/parallel-consumer/actions/runs/31578047933/attempts/1/logs" \
  -o attempt1.zip
```

`unzip` is not installed on that machine; `python3 -c "import zipfile; ..."` extracted it. Inside,
`grep -n -A45 "Unknown error"` lands directly on the root-cause stack (one hit, log line 1608), and
lines ~1575-1650 hold the full interleaving quoted above. Note the run's *conclusion is `success`*
because the failed attempt was rerun — attempt 1 is where the failure lives.

The extracted log is **machine-local** at
`/tmp/claude-1000/-home-astubbs-git-parallel-consumer/daaff26e-3636-4c41-ad6d-acacbbee27a4/scratchpad/a1/`
and will not survive the move; the curl above re-fetches it as long as GitHub retains the archive.

## What travels and what does not

- **Travels:** `origin/fix/chaos-harness-double-start-race` at `aba47280`. That is the whole change.
- **Does not travel:** this handoff (managed `/tmp`, OS-managed, machine-local — copy it or re-publish
  it somewhere the receiving machine can see before relying on it), the extracted CI log, and the
  worktree at `.claude/worktrees/fix+broker-poll-swallowed-cause` (machine-local; it holds no
  uncommitted work, so nothing is lost by discarding it — nothing here has been torn down).

## Plausible next steps

One path, in order: open a PR for the branch so the integration lane compiles it and runs
`ManagedPCInstanceLifecycleIT`; fix whatever the compiler and that test surface; then run
`ce-code-review` before treating it as shippable. On a machine with a JDK the first two steps collapse
into a local `./mvnw -pl parallel-consumer-core -am test-compile` plus running that one IT, which is
much faster feedback than a CI round trip.

Optionally, and independently of the above: replay the original scenario with
`-Dchaos.seed=8291601231857558952` (needs Docker, heavy, `-Pci -pl parallel-consumer-core -am verify
-DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups=`) to confirm the seed that exposed the race
now passes. This is confirmation, not diagnosis — the mechanism is already established from the log,
and the seed only replays the chaos draw sequence, not the thread interleaving, so a pass is
suggestive rather than conclusive.

Superseded: `chaos-revoke-under-work-cooperative-failure.md` in this same directory — the handoff this
session resumed. Its two central claims are corrected above.
