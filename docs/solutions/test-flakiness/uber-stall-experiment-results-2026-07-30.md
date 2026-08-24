---
title: "Uber-branch stall experiment results: drain fix validated by direct test; astubbs#29+astubbs#31 composition broken vs current master"
date: 2026-07-30
category: test-flakiness
module: parallel-consumer-core
problem_type: experiment_report
component: internal / poll-control / close
status: COMPLETE
related:
  - "docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md (the investigation this experiment concludes)"
  - "PR astubbs#80 - drain fix + guards (the 'fix' arm delta)"
  - "PR astubbs#29 (confluentinc#857) / PR astubbs#31 (confluentinc#909) - the merged in-flight fixes"
  - "docs/plans/2026-07-30-001-feat-chaos-pain-suite-design-plan.md (chaos suite design, branch docs/chaos-pain-suite-design)"
archive_refs:
  - "experiment/stall-uber-nofix - commit A (defect) + astubbs#29 + astubbs#31"
  - "experiment/stall-uber-fix - commit B (fix) + astubbs#29 + astubbs#31"
tags: [experiment, silent-stall, "857", "909", drain, composition]
---

# Uber-branch stall experiment: results

> Extracted from `origin/docs/uber-stall-experiment-results` @bd82668b3, `docs/solutions/test-flakiness/uber-stall-experiment-results-2026-07-30.md`.

> Concludes the experiment proposed in the silent-stall report (recommendation 5): merge the three
> non-conflicting stall fixes (astubbs#29, astubbs#31, PR astubbs#80's drain fix) and measure. Everything below is
> reproducible from the two pushed archive refs (never merge candidates) named in the frontmatter.

## Design recap

Two arms, identical except for **one commit** (the drain fix):

```
commit A (2eca71fa) defect present ─┐                 ┌─ experiment/stall-uber-nofix
                                    ├ + merge astubbs#29 + astubbs#31┤
commit B (60661ea9) fix present  ───┘                 └─ experiment/stall-uber-fix
```

Alternating runs on one box (12-core Mac, Docker, runs strictly sequential). Workloads: (a) astubbs#29's chaos
harness `MultiInstanceRebalanceTest#largeNumberOfInstances` (12 PCs / 80 partitions / 500k msgs / chaos
monkey), (b) the `forkCount=16` full-core-IT stress recipe (1.33 forks/core - the investigation's proven
stall reproducer), (c) post-hoc: the purpose-built `DrainingMemberRebalanceIT` (PR astubbs#80).

## Results

### Chaos axis: uninformative (8/8 pass)

| arm | runs | result |
|---|---|---|
| nofix | 4 | 4 PASS |
| fix | 4 | 4 PASS |

The box did not reproduce astubbs#29's reported 10-20% residual at these parameters; phase stopped early - an
axis that cannot discriminate between two zeros. (astubbs#29's residual numbers may need its exact box/params.)

### fork16 axis: polluted by deterministic composition regressions - but salvageable

All 6 runs (3 per arm) FAILED - however the failure decomposition tells two different stories:

**Constant on BOTH arms, every run (= composition regressions, NOT stalls, NOT fix-related):**

| failing surface | detail | determinism check (sequential, uncontended) |
|---|---|---|
| `CloseAndOpenOffsetTest` | close→reopen redelivery broken | **9 of 14 cases ERROR** - worse than under load; fully deterministic |
| `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` | ALL 5 params fail (~250s) | all 5 fail sequentially too |
| `RebalanceTest.rebalanceCompletesForCommitModeVariations[5]` | 1 case | (not isolated separately) |
| `ManagedPCInstanceLifecycleTest.rapidToggle...` | astubbs#29's own new test, flaky under fork16 (rotating params, both arms) | - |

**Roaming silent stalls (the class under investigation):** nofix run 3 stalled `KafkaSanityTests` (68s);
fix run 2 stalled `PartitionStateCommittedOffsetIT.committedOffsetRemoved[1]` (167s). **~1 per 3 runs on
each arm - no measurable drain-fix differential in this sample.**

### Attribution runs (single-PR arms, the two broken test classes, sequential)

| arm | `CloseAndOpenOffsetTest` | `RebalanceEoSDeadlockTest.noDeadlockOnRevoke` |
|---|---|---|
| base(+fix) + **astubbs#29 only** (= archive ref `experiment/stall-uber-fix`@`340bf75f`) | **9/14 ERROR** (72s) | **5/5 FAIL** (212s) |
| base(+fix) + **astubbs#31 only** (`experiment/attr-31`) | **14/14 pass** (34s) | **5/5 pass** (76s) |

**Verdict: astubbs#29 owns 100% of the composition breakage; astubbs#31 is fully exonerated.** The failures implicate
astubbs#29's close/commit-path changes against the current base (close→reopen offset continuity; revoke-commit
correctness - its `tryLock` change is the prime suspect for the latter). All remediation lands where a
rebase was already mandatory.

### The direct instrument: `DrainingMemberRebalanceIT` (decisive)

Where the statistical axes were noisy or uninformative, the purpose-built test discriminated perfectly:

| arm | verdict | detail |
|---|---|---|
| PR astubbs#80 branch (fix) | **GREEN** (12.5s) | B (joined mid-drain) consumed 200 while A drained; A closed promptly; ledger A=1100, B=200, **duplicates=100 = exactly the parked in-flight tail** (maxConcurrency) - the handover honoured A's commits |
| nofix arm (defect) | **RED** (51s) | group frozen while A drained (B starved); the mid-drain liveness window could never be established - B only progressed after the worker-pool force-interrupt bailed A out |

This is the planted-bug calibration the chaos-suite design demands, executed: the test fails on the
known-defective composition and passes on the fixed one, while also verifying the drain's *purpose*
(partitions held → in-flight finished → committed → duplicates bounded; eager release would have shown
duplicates ≈ A's full record count).

### highcpu runner datapoint (independent, one run)

On PR astubbs#80's branch (fix present), the highcpu runner's Integration job still reddened on
`committedOffsetRemoved[1]` while Unit + Performance passed - consistent with the report's position that
this test's fresh-PC first-poll stall is a **distinct, likely broker-side mechanism**, not the zombie
drain.

## Conclusions

1. **The drain fix is validated by direct measurement** (RED→GREEN on both the unit guard and the
   integration guard, against the planted defect), **not** by the statistical axes - which were
   respectively uninformative (chaos) and polluted (fork16).
2. **The prediction "zombie-drain explains astubbs#29's 10-20% residual" is NOT confirmed** - no differential in
   the roaming stalls was measurable here. The drain fix stands on its code-confirmed mechanism and
   guards; `committedOffsetRemoved`'s mechanism remains open and broker-side-suspected.
3. **Headline: astubbs#29 (+possibly astubbs#31) does NOT compose with current master.** Deterministic, load-independent
   breakage of close→reopen offset redelivery (9/14 encodings) and of the revoke-commit deadlock guard
   (5/5 params). astubbs#29 cannot land as-is; see merge strategy.

## Merge strategy for the three PRs (recommendation)

1. **astubbs#75** (highcpu runner CI) first - astubbs#80 stacks on it.
2. **astubbs#80** (drain fix + guards) next - self-contained, green, small; landing early forces astubbs#29's rebase to
   account for the new drain semantics (correct, it owns that choreography).
3. **astubbs#31** (confluentinc#909, 11 lines) - **unblocked by attribution** (solo arm fully green): trivial rebase onto
   post-astubbs#80 master and land. No remediation needed.
4. **astubbs#29** (confluentinc#857) - **never as-is** (stale April base, 1,586 lines, proven composition breakage). Rebase
   onto post-astubbs#80 master and split into slices: (a) ThreadConfinedConsumer + CME close-race fix, (b)
   pausedForThrottling reset on assignment, (c) counter adjustments, (d) test harness (ManagedPCInstance +
   chaos test - the Chaos Pain Suite Phase 1 dependency), (e) investigation docs. The two deterministic
   regressions are the rebase's **acceptance criteria** (each reproducible sequentially in ~5 min).
   - The `experiment/stall-uber-fix` merge commit (`340bf75f`) is a **conflict cheat-sheet**: the
     astubbs#29-vs-astubbs#80 `ConsumerManager`/pom conflicts are already resolved correctly there.
   - The archive refs are the **rebase test bench**: every slice can be checked against the exact failing
     tests before its PR opens.

## Caveats

- Small samples throughout (4+4 chaos, 3+3 fork16); one box; the arms carry astubbs#29's verbose DEBUG logback.
- Chaos phase was stopped deliberately at 4 pairs (user call: axis could not discriminate).
- fork16 binary verdicts were unusable; the per-test decomposition above is the salvage.
- `ManagedPCInstanceLifecycleTest` flakiness under fork16 is astubbs#29's own test under load - not triaged here.

  > **Citation repair, 2026-08-20:** `ManagedPCInstanceLifecycleTest` was DELETED in astubbs/parallel-consumer#325. A mutation matrix over the four `ManagedPCInstance` guards showed it killed none of them, while `ManagedPCInstanceLifecycleIT` - which now lives at `integrationTests/ManagedPCInstanceLifecycleIT.java` - killed both real mutants deterministically, without a broker, in a tenth of the time. The claim above is left unchanged: the flakiness it records was real when measured.

## Addendum (2026-07-30, post nudge-race fix): full-stack composition check

After the nudge-race harness fix landed on PR astubbs#80, its full tip (drain fix + nudge fix + all guards and
diagnostics) was merged into `experiment/stall-uber-fix` (ref updated to `c72c923c`): **zero conflicts**
(including `BrokerIntegrationTest`, which both astubbs#29 and astubbs#80 modify), and **all four guards green** on the
combined composition - `BrokerPollSystemDrainTest`, `PartitionStateCommittedOffsetIT` 7/7,
`LatestResetTailNudgeIT`, `DrainingMemberRebalanceIT` (ledger byte-identical to the standalone branch:
A=1100, B=200, duplicates=100 = the parked tail). Conclusion: the PR astubbs#80 fix stack is composition-safe
with astubbs#29+astubbs#31; the deterministic astubbs#29 regressions (`CloseAndOpenOffsetTest`, `RebalanceEoSDeadlockTest`)
remain, unchanged, as the astubbs#29 rebase acceptance criteria. The 20-run acceptance hunt on PR astubbs#80's own
branch separately measured committedOffsetRemoved 0/20, 15/20 fully clean, zero stall-class failures
(catalogued in inflight's load-tightness flake family).
